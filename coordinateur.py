# ==============================
# FICHIER 1 : coordinateur.py
# ==============================

import socket
import threading
import json
from collections import defaultdict
import queue
import time

segment_queue = queue.Queue()


# ----------- CONFIGURATION -----------
HOST = '0.0.0.0'  # écoute toutes les IP
PORT = 5000
NB_WORKERS = 2
SEGMENT_FILE = 'grand_texte.txt'


results = []  # résultats partiels reçus

# ----------- FONCTION MAP SIMPLIFIÉE -----------
def clean_text(text):
    return ''.join(c.lower() if c.isalnum() or c.isspace() else ' ' for c in text)

def map_function(segment):
    word_count = defaultdict(int)
    words = clean_text(segment).split()
    for word in words:
        word_count[word] += 1
    return word_count

# ----------- FONCTION REDUCE -----------
def reduce_function(results_list):
    final_result = defaultdict(int)
    for result in results_list:
        for word, count in result.items():
            final_result[word] += count
    return dict(final_result)

# ----------- GESTION D'UN WORKER -----------
def handle_worker(conn, addr):
    try:
        segment = segment_queue.get_nowait()
    except queue.Empty:
        print(f"[INFO] Aucun segment à attribuer pour {addr}")
        conn.close()
        return

    try:
        print(f"[INFO] Envoi segment à {addr}")
        segment_data = json.dumps({"segment": segment}) + '\n'
        conn.sendall(segment_data.encode())
        conn.settimeout(10)  # 10 secondes max pour réponse

        data = b""
        while not data.endswith(b"\n"):
            chunk = conn.recv(4096)
            if not chunk:
                raise ConnectionError("Connexion interrompue")
            data += chunk

        response = json.loads(data.decode())
        results.append(response['result'])
        print(f"[INFO] Résultat reçu de {addr}")

    except (ConnectionError, socket.timeout, json.JSONDecodeError) as e:
        print(f"[ERREUR] Worker {addr} a échoué : {e}")
        segment_queue.put(segment)  # remettre le segment dans la file

    finally:
        conn.close()


# ----------- DÉCOUPAGE DU FICHIER -----------
def split_file(filename, n):
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    size = len(lines) // n
    return [''.join(lines[i*size:(i+1)*size]) for i in range(n)]

# ----------- SERVEUR PRINCIPAL -----------
def start_server():
    segments = split_file(SEGMENT_FILE, NB_WORKERS)
    for seg in segments:
        segment_queue.put(seg)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(NB_WORKERS)
    print(f"[INFO] Serveur en écoute sur {HOST}:{PORT}")

    while not segment_queue.empty():
        conn, addr = server.accept()
        threading.Thread(target=handle_worker, args=(conn, addr)).start()

    # attendre un peu que tous les threads finissent
    time.sleep(3)

    final = reduce_function(results)
    with open("resultat_final.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)
    print("[INFO] Résultat final écrit dans resultat_final.json")


if __name__ == '__main__':
    start_server()