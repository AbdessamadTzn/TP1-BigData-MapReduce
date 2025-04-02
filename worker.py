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
def clean_text(text):
    return ''.join(c.lower() if c.isalnum() or c.isspace() else ' ' for c in text)

def map_function(segment):
    word_count = defaultdict(int)
    words = clean_text(segment).split()
    for word in words:
        word_count[word] += 1
    return dict(word_count)

# ----------- CLIENT -----------
def worker():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #time.sleep(10)  # ralentir volontairement de 10 secondes
    sock.connect((HOST, PORT))
    data = b""
    while not data.endswith(b"\n"):
        data += sock.recv(4096)
    received = json.loads(data.decode())
    segment = received['segment']
    # Ajoute cette ligne ici 👇 pour simuler un worker lent
    time.sleep(10)
    msg = json.dumps({"result": result}) + '\n'
    sock.sendall(msg.encode())
    sock.close()

if __name__ == '__main__':
    worker()
