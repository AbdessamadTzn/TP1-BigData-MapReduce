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
            
            # Réception du segment
            data = b""
            print("[INFO] Réception du segment...")
            while len(data) < size:
                chunk = sock.recv(min(4096, size - len(data)))
                if not chunk:
                    raise ConnectionError("Connection perdue")
                data += chunk
                if len(data) % (1024*1024) == 0:  # Log tous les 1MB
                    print(f"[INFO] Reçu {len(data)/1024/1024:.1f}MB / {size/1024/1024:.1f}MB")
            
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
            size = len(result_data)
            sock.sendall(str(size).encode() + b'\n')
            sock.sendall(result_data)
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
