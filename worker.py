# ==============================
# FICHIER 2 : worker.py
# ==============================

import socket
import json
from collections import defaultdict
import time

# ----------- CONFIGURATION -----------
HOST = '172.20.10.2'  # IP du coordinateur 
PORT = 5000
BUFFER_SIZE = 8192  # Taille du buffer pour la réception/envoi

# ----------- FONCTION MAP -----------
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

def receive_data(sock, expected_size):
    """Reçoit des données avec gestion de la taille"""
    data = b""
    while len(data) < expected_size:
        chunk = sock.recv(min(BUFFER_SIZE, expected_size - len(data)))
        if not chunk:
            raise ConnectionError("Connection perdue pendant la réception")
        data += chunk
        if len(data) % (1024*1024) == 0:  # Log tous les 1MB
            print(f"[INFO] Reçu: {len(data)/1024/1024:.1f}MB / {expected_size/1024/1024:.1f}MB")
    return data

def receive_message(sock):
    """Reçoit un message complet (taille + données)"""
    # Lire la taille
    size_str = receive_data(sock, 1024).decode('utf-8').strip()
    if not size_str:
        raise RuntimeError("Taille de données invalide")
        
    try:
        size = int(size_str)
    except ValueError:
        raise RuntimeError(f"Taille invalide: {size_str}")
        
    if size <= 0:
        raise RuntimeError(f"Taille de données invalide: {size}")
    
    # Lire les données
    data = receive_data(sock, size)
    return data.decode('utf-8').strip()

def send_data(sock, data):
    """Envoie des données avec un en-tête de taille"""
    try:
        # Convertir les données en JSON si nécessaire
        if isinstance(data, dict):
            data = json.dumps(data)
        elif not isinstance(data, str):
            data = str(data)
            
        # Ajouter un marqueur de fin
        data = data + "\n"
        
        # Envoyer la taille en premier
        size = len(data.encode('utf-8'))
        sock.sendall(str(size).encode('utf-8') + b"\n")
        
        # Envoyer les données par morceaux
        data_bytes = data.encode('utf-8')
        total_sent = 0
        while total_sent < len(data_bytes):
            sent = sock.send(data_bytes[total_sent:total_sent + BUFFER_SIZE])
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

# ----------- CLIENT -----------
def worker():
    while True:  # Boucle continue pour traiter plusieurs segments
        sock = None
        try:
            # Connexion au coordinateur
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(300)  # 5 minutes timeout
            sock.connect((HOST, PORT))
            print(f"[INFO] Connecté au coordinateur {HOST}:{PORT}")
            
            # Réception du message
            print("[INFO] Réception du segment...")
            message = receive_message(sock)
            
            # Décodage et extraction du segment
            try:
                received = json.loads(message)
                if not isinstance(received, dict) or 'segment' not in received:
                    raise ValueError("Format de données invalide")
                    
                segment = received['segment']
                segment_size = len(segment)
                print(f"[INFO] Segment reçu ({segment_size/1024/1024:.1f}MB)")
                
                # Traitement du segment
                print("[INFO] Traitement du segment...")
                result = map_function(segment)
                print("[INFO] Traitement terminé")
                
                # Envoi du résultat
                print("[INFO] Envoi du résultat...")
                if not send_data(sock, {"result": result}):
                    raise RuntimeError("Échec de l'envoi du résultat")
                print("[INFO] Résultat envoyé")
                
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Erreur de décodage JSON: {e}")
            
        except ConnectionRefusedError:
            print("[INFO] Plus de segments à traiter, arrêt du worker")
            break
        except socket.timeout:
            print("[ERREUR] Timeout de la connexion")
            time.sleep(1)
        except ConnectionError as e:
            print(f"[ERREUR] Erreur de connexion: {e}")
            time.sleep(1)
        except Exception as e:
            print(f"[ERREUR] Erreur lors du traitement : {e}")
            time.sleep(1)  # Attendre un peu avant de réessayer
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass

if __name__ == '__main__':
    while True:
        try:
            worker()
        except Exception as e:
            print(f"[ERREUR] Erreur fatale du worker: {e}")
            time.sleep(5)  # Attendre plus longtemps en cas d'erreur fatale
