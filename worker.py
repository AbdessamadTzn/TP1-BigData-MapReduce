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
    #time.sleep(10)  # ralentir volontairement de 10 secondes
    sock.connect((HOST, PORT))
    data = b""
    while not data.endswith(b"\n"):
        data += sock.recv(4096)
    received = json.loads(data.decode())
    segment = received['segment']
    # Ajoute cette ligne ici 👇 pour simuler un worker lent
    result = map_function(segment)
    time.sleep(10)
    msg = json.dumps({"result": result}) + '\n'
    sock.sendall(msg.encode())
    sock.close()

if __name__ == '__main__':
    worker()
