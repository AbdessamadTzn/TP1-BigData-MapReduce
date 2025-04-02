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
    data = b""
    while len(data) < expected_size:
        chunk = sock.recv(min(BUFFER_SIZE, expected_size - len(data)))
        if not chunk:
            raise ConnectionError("Connection perdue pendant la réception")
        data += chunk
        if len(data) % (1024*1024) == 0:  # Log tous les 1MB
            print(f"[INFO] Reçu: {len(data)/1024/1024:.1f}MB / {expected_size/1024/1024:.1f}MB")
    return data

def send_data(sock, data):
    size = len(data)
    sock.sendall(str(size).encode() + b'\n')
    
    # Envoi par petits morceaux
    for i in range(0, size, BUFFER_SIZE):
        chunk = data[i:i + BUFFER_SIZE]
        sock.sendall(chunk)
        if i % (1024*1024) == 0:  # Log tous les 1MB
            print(f"[INFO] Envoyé: {i/1024/1024:.1f}MB / {size/1024/1024:.1f}MB")

# ----------- CLIENT -----------
def worker():
    while True:  # Boucle continue pour traiter plusieurs segments
        try:
            # Connexion au coordinateur
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, PORT))
            print(f"[INFO] Connecté au coordinateur {HOST}:{PORT}")
            
            # Réception de la taille des données
            size = int(sock.recv(1024).decode().strip())
            print(f"[INFO] Taille du segment à recevoir: {size/1024/1024:.1f}MB")
            
            # Réception du segment
            print("[INFO] Réception du segment...")
            data = receive_data(sock, size)
            
            # Décodage et extraction du segment
            received = json.loads(data.decode())
            segment = received['segment']
            segment_size = len(segment)
            print(f"[INFO] Segment reçu ({segment_size/1024/1024:.1f}MB)")
            
            # Traitement du segment
            print("[INFO] Traitement du segment...")
            result = map_function(segment)
            print("[INFO] Traitement terminé")
            
            # Envoi du résultat
            result_data = json.dumps({"result": result}).encode()
            print("[INFO] Envoi du résultat...")
            send_data(sock, result_data)
            print("[INFO] Résultat envoyé")
            
        except ConnectionRefusedError:
            print("[INFO] Plus de segments à traiter, arrêt du worker")
            break
        except Exception as e:
            print(f"[ERREUR] Erreur lors du traitement : {e}")
            time.sleep(1)  # Attendre un peu avant de réessayer
        finally:
            sock.close()

if __name__ == '__main__':
    worker()
