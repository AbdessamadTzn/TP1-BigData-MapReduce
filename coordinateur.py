# ==============================
# FICHIER 1 : coordinateur.py
# ==============================

import socket
import threading
import json
from collections import defaultdict
import queue
import time
from concurrent.futures import ThreadPoolExecutor
import os

# ----------- CONFIGURATION -----------
HOST = '0.0.0.0'  # écoute toutes les IP
PORT = 5000
NB_WORKERS = 2
CHUNK_SIZE = 5 * 1024 * 1024  # Augmenté à 5MB par segment
MAX_RETRIES = 3  # Nombre maximum de tentatives pour un segment
SEGMENT_FILE = 'yelp_academic_dataset_review.json'
SOCKET_TIMEOUT = 300  # Augmenté à 5 minutes
MAX_CONCURRENT_WORKERS = 4  # Nombre maximum de workers simultanés

# Files d'attente pour la gestion des segments et résultats
segment_queue = queue.Queue()
results_queue = queue.Queue()
active_threads = []
failed_segments = []

# ----------- FONCTION MAP SIMPLIFIÉE -----------
def clean_text(text):
    return ''.join(c.lower() if c.isalnum() or c.isspace() else ' ' for c in text)

def map_function(segment):
    pizza_count = 0
    for line in segment.strip().splitlines():
        try:
            review = json.loads(line)
            if review.get("stars", 0) == 5 and "pizza" in review.get("text", "").lower():
                pizza_count += 1
        except json.JSONDecodeError:
            continue
    return {"pizza_5stars": pizza_count}

# ----------- FONCTION REDUCE -----------
def reduce_function(results_list):
    final_result = defaultdict(int)
    for result in results_list:
        for word, count in result.items():
            final_result[word] += count
    return dict(final_result)

# ----------- FONCTIONS UTILITAIRES -----------
def receive_data(sock, expected_size):
    """Reçoit des données avec gestion de la taille"""
    data = b""
    while len(data) < expected_size:
        chunk = sock.recv(min(8192, expected_size - len(data)))
        if not chunk:
            raise ConnectionError("Connection perdue pendant la réception")
        data += chunk
        if len(data) % (1024*1024) == 0:  # Log tous les 1MB
            print(f"[INFO] Reçu: {len(data)/1024/1024:.1f}MB / {expected_size/1024/1024:.1f}MB")
    return data

def send_data(sock, data):
    """Envoie des données avec un en-tête de taille"""
    try:
        # Convertir les données en JSON si nécessaire
        if isinstance(data, dict):
            data = json.dumps(data)
        elif not isinstance(data, str):
            data = str(data)
            
        # Envoyer la taille en premier
        size = len(data.encode('utf-8'))
        sock.sendall(str(size).encode('utf-8') + b"\n")
        
        # Envoyer les données par morceaux
        data_bytes = data.encode('utf-8')
        total_sent = 0
        while total_sent < len(data_bytes):
            sent = sock.send(data_bytes[total_sent:total_sent + 8192])
            if sent == 0:
                raise RuntimeError("Connexion perdue")
            total_sent += sent
            
            # Afficher la progression tous les 1MB
            if total_sent % (1024 * 1024) == 0:
                print(f"[INFO] Envoi: {total_sent / (1024 * 1024):.1f}MB / {len(data_bytes) / (1024 * 1024):.1f}MB")
                
        print(f"[INFO] Envoi terminé: {total_sent / (1024 * 1024):.1f}MB")
        return True
    except Exception as e:
        print(f"[ERREUR] Échec de l'envoi: {str(e)}")
        return False

# ----------- GESTION D'UN WORKER -----------
def handle_worker(conn, addr):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            segment = segment_queue.get_nowait()
            print(f"[INFO] Envoi segment de {len(segment)/1024:.1f}KB à {addr} (tentative {retries + 1})")
        except queue.Empty:
            print(f"[INFO] Aucun segment à attribuer pour {addr}")
            conn.close()
            return

        try:
            # Envoi du segment
            if not send_data(conn, {"segment": segment}):
                raise RuntimeError("Échec de l'envoi du segment")
            
            # Réception du résultat
            try:
                # Lire la taille
                size_str = receive_data(conn, 1024).decode('utf-8').strip()
                if not size_str:
                    raise RuntimeError("Taille de réponse invalide")
                    
                size = int(size_str)
                if size <= 0:
                    raise RuntimeError(f"Taille de réponse invalide: {size}")
                
                # Lire le résultat
                result_data = receive_data(conn, size).decode('utf-8').strip()
                if not result_data:
                    raise RuntimeError("Données de réponse vides")
                
                # Parser le JSON
                try:
                    result = json.loads(result_data)
                    if not isinstance(result, dict) or 'result' not in result:
                        raise ValueError("Format de réponse invalide")
                    results_queue.put(result['result'])
                    print(f"[INFO] Résultat reçu de {addr}")
                    return  # Succès, on sort de la fonction
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"Erreur de parsing JSON: {str(e)}")
                    
            except Exception as e:
                raise RuntimeError(f"Erreur lors de la réception du résultat: {str(e)}")
                
        except ConnectionError as e:
            print(f"[ERREUR] Erreur de connexion avec {addr}: {e}")
            retries += 1
            if retries >= MAX_RETRIES:
                print(f"[ERREUR] Abandon du segment après {MAX_RETRIES} tentatives")
                failed_segments.append(segment)
            else:
                segment_queue.put(segment)  # On remet le segment dans la queue pour réessayer
            time.sleep(2)  # Attendre plus longtemps avant de réessayer
        except Exception as e:
            print(f"[ERREUR] Worker {addr} a échoué (tentative {retries + 1}): {e}")
            retries += 1
            if retries >= MAX_RETRIES:
                print(f"[ERREUR] Abandon du segment après {MAX_RETRIES} tentatives")
                failed_segments.append(segment)
            else:
                segment_queue.put(segment)  # On remet le segment dans la queue pour réessayer
            time.sleep(2)  # Attendre plus longtemps avant de réessayer

    conn.close()

# ----------- DÉCOUPAGE DU FICHIER -----------
def split_file(filename):
    segments = []
    current_segment = []
    current_size = 0
    
    print(f"[INFO] Découpage du fichier en segments de {CHUNK_SIZE/1024/1024:.1f}MB...")
    
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line_size = len(line.encode('utf-8'))
            if current_size + line_size > CHUNK_SIZE and current_segment:
                segments.append(''.join(current_segment))
                current_segment = []
                current_size = 0
            current_segment.append(line)
            current_size += line_size
            
    if current_segment:
        segments.append(''.join(current_segment))
    
    print(f"[INFO] Fichier découpé en {len(segments)} segments")
    return segments

# ----------- GESTION DES RÉSULTATS -----------
def process_results():
    results = []
    while len(results) < NB_WORKERS:
        try:
            result = results_queue.get(timeout=1)
            results.append(result)
        except queue.Empty:
            continue
    return results

# ----------- SERVEUR PRINCIPAL -----------
def start_server():
    # Découpage du fichier en segments plus grands
    segments = split_file(SEGMENT_FILE)
    for seg in segments:
        segment_queue.put(seg)
    
    total_segments = segment_queue.qsize()
    print(f"[INFO] {total_segments} segments créés et mis en file d'attente")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(MAX_CONCURRENT_WORKERS)  # Augmenté pour plus de parallélisme
    print(f"[INFO] Serveur en écoute sur {HOST}:{PORT}")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
        while not segment_queue.empty():
            try:
                conn, addr = server.accept()
                conn.settimeout(SOCKET_TIMEOUT)
                thread = executor.submit(handle_worker, conn, addr)
                active_threads.append(thread)
                print(f"[INFO] {segment_queue.qsize()}/{total_segments} segments restants")
            except Exception as e:
                print(f"[ERREUR] Erreur lors de l'acceptation d'une connexion: {e}")
                continue

    print("[INFO] Attente de la fin du traitement des workers...")
    for thread in active_threads:
        try:
            thread.result()
        except Exception as e:
            print(f"[ERREUR] Erreur lors de l'attente d'un thread: {e}")

    results = []
    while not results_queue.empty():
        try:
            result = results_queue.get(timeout=1)
            results.append(result)
        except queue.Empty:
            continue
        except Exception as e:
            print(f"[ERREUR] Erreur lors de la récupération d'un résultat: {e}")

    if not results:
        print("[ERREUR] Aucun résultat n'a été collecté!")
        return

    final = reduce_function(results)
    print("[INFO] Traitement terminé, écriture des résultats...")
    
    with open("resultat_final.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)
    print("[INFO] Résultat final écrit dans resultat_final.json")

def process_segment(segment, worker_address, retry_count=0):
    """Traite un segment avec un worker"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(300)  # 5 minutes timeout
            sock.connect(worker_address)
            
            # Envoi du segment
            if not send_data(sock, segment):
                raise RuntimeError("Échec de l'envoi du segment")
            
            # Réception du résultat
            try:
                # Lire la taille
                size_str = receive_data(sock, 1024).decode('utf-8').strip()
                if not size_str:
                    raise RuntimeError("Taille de réponse invalide")
                    
                size = int(size_str)
                if size <= 0:
                    raise RuntimeError(f"Taille de réponse invalide: {size}")
                
                # Lire le résultat
                result_data = receive_data(sock, size).decode('utf-8').strip()
                if not result_data:
                    raise RuntimeError("Données de réponse vides")
                
                # Parser le JSON
                try:
                    result = json.loads(result_data)
                    if not isinstance(result, dict) or 'result' not in result:
                        raise ValueError("Format de réponse invalide")
                    return result['result']
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"Erreur de parsing JSON: {str(e)}")
                    
            except Exception as e:
                raise RuntimeError(f"Erreur lors de la réception du résultat: {str(e)}")
                
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"[INFO] Nouvelle tentative pour le worker {worker_address} (tentative {retry_count + 1}/{MAX_RETRIES})")
            time.sleep(1)  # Attendre 1 seconde avant de réessayer
            return process_segment(segment, worker_address, retry_count + 1)
        else:
            print(f"[ERREUR] Worker {worker_address} a échoué après {MAX_RETRIES} tentatives: {str(e)}")
            return None

if __name__ == '__main__':
    start_server()