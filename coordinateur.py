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
CHUNK_SIZE = 1 * 1024 * 1024  # Réduit à 1MB par segment
MAX_RETRIES = 3  # Nombre maximum de tentatives pour un segment
SEGMENT_FILE = 'yelp_academic_dataset_review.json'

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

# ----------- GESTION D'UN WORKER -----------
def handle_worker(conn, addr):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            segment = segment_queue.get_nowait()
            print(f"[INFO] Envoi segment de {len(segment)/1024/1024:.1f}MB à {addr} (tentative {retries + 1})")
        except queue.Empty:
            print(f"[INFO] Aucun segment à attribuer pour {addr}")
            conn.close()
            return

        try:
            # Envoi du segment avec compression
            segment_data = json.dumps({"segment": segment}).encode('utf-8')
            size = len(segment_data)
            conn.sendall(str(size).encode() + b'\n')
            
            # Envoi par petits morceaux de 8KB
            for i in range(0, size, 8192):
                chunk = segment_data[i:i + 8192]
                conn.sendall(chunk)
                if i % (1024*1024) == 0:  # Log tous les 1MB
                    print(f"[INFO] Envoi à {addr}: {i/1024/1024:.1f}MB / {size/1024/1024:.1f}MB")

            # Réception du résultat
            conn.settimeout(300)  # 5 minutes timeout
            size = int(conn.recv(1024).decode().strip())
            data = b""
            while len(data) < size:
                chunk = conn.recv(min(8192, size - len(data)))
                if not chunk:
                    raise ConnectionError("Connection perdue")
                data += chunk
                if len(data) % (1024*1024) == 0:
                    print(f"[INFO] Reçu de {addr}: {len(data)/1024/1024:.1f}MB / {size/1024/1024:.1f}MB")

            response = json.loads(data.decode())
            results_queue.put(response['result'])
            print(f"[INFO] Résultat reçu de {addr}")
            return  # Succès, on sort de la fonction

        except Exception as e:
            print(f"[ERREUR] Worker {addr} a échoué (tentative {retries + 1}): {e}")
            retries += 1
            if retries >= MAX_RETRIES:
                print(f"[ERREUR] Abandon du segment après {MAX_RETRIES} tentatives")
                failed_segments.append(segment)
            else:
                segment_queue.put(segment)  # On remet le segment dans la queue pour réessayer
            time.sleep(1)  # Attendre un peu avant de réessayer

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
    # Découpage du fichier en segments plus petits
    segments = split_file(SEGMENT_FILE)
    for seg in segments:
        segment_queue.put(seg)
    
    total_segments = segment_queue.qsize()
    print(f"[INFO] {total_segments} segments créés et mis en file d'attente")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(NB_WORKERS)
    print(f"[INFO] Serveur en écoute sur {HOST}:{PORT}")

    with ThreadPoolExecutor(max_workers=NB_WORKERS) as executor:
        while not segment_queue.empty():
            conn, addr = server.accept()
            thread = executor.submit(handle_worker, conn, addr)
            active_threads.append(thread)
            print(f"[INFO] {segment_queue.qsize()}/{total_segments} segments restants")

    print("[INFO] Attente de la fin du traitement des workers...")
    for thread in active_threads:
        thread.result()

    results = []
    while not results_queue.empty():
        results.append(results_queue.get())

    final = reduce_function(results)
    print("[INFO] Traitement terminé, écriture des résultats...")
    
    with open("resultat_final.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)
    print("[INFO] Résultat final écrit dans resultat_final.json")

if __name__ == '__main__':
    start_server()