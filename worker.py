# ==============================
# FICHIER 2 : worker.py
# ==============================

import socket
import json
from collections import defaultdict
import time
import threading

# ----------- CONFIGURATION -----------
HOST = '172.20.10.2'  # IP du coordinateur 
PORT = 5000

# ----------- FONCTION MAP -----------
def clean_text(text):
    return ''.join(c.lower() if c.isalnum() or c.isspace() else ' ' for c in text)

def map_function(segment):
    import json
    from collections import defaultdict

    pizza_count = 0
    for line in segment.strip().splitlines():
        try:
            review = json.loads(line)
            text = review.get("text", "").lower()
            stars = review.get("stars", 0)
            if stars == 5 and "pizza" in text:
                pizza_count += 1
        except json.JSONDecodeError:
            continue

    return {"pizza_5stars": pizza_count}

# ----------- CLIENT -----------
def worker():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
        print(f"[INFO] Connecté au coordinateur {HOST}:{PORT}")
        
        # Réception des données
        data = b""
        while not data.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("Connexion perdue avec le coordinateur")
            data += chunk
            
        received = json.loads(data.decode())
        segment = received['segment']
        
        # Traitement des données
        print("[INFO] Traitement du segment...")
        result = map_function(segment)
        
        # Envoi du résultat
        msg = json.dumps({"result": result}) + '\n'
        sock.sendall(msg.encode())
        print("[INFO] Résultat envoyé au coordinateur")
        
    except Exception as e:
        print(f"[ERREUR] Erreur lors du traitement : {e}")
    finally:
        sock.close()

if __name__ == '__main__':
    worker()
