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

# ----------- CONFIGURATION -----------
HOST = '0.0.0.0'  # écoute toutes les IP
PORT = 5000
NB_WORKERS = 2
SEGMENT_FILE = 'yelp_academic_dataset_review.json'

# File d'attente pour les segments
segment_queue = queue.Queue()
# File d'attente pour les résultats
results_queue = queue.Queue()
# Liste pour stocker les threads actifs
active_threads = []

# ----------- FONCTION MAP SIMPLIFIÉE -----------
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
                raise ConnectionError("Le worker a fermé la connexion sans envoyer de données.")
            data += chunk

        response = json.loads(data.decode())
        results_queue.put(response['result'])
        print(f"[INFO] Résultat reçu de {addr}")

    except (ConnectionError, socket.timeout, json.JSONDecodeError) as e:
        print(f"[ERREUR] Worker {addr} a échoué : {e}")
        segment_queue.put(segment)  # Réassigner le segment

    finally:
        conn.close()

# ----------- DÉCOUPAGE DU FICHIER -----------
def split_file(filename, n):
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    size = len(lines) // n
    segments = [''.join(lines[i*size:(i+1)*size]) for i in range(n)]

    if len(lines) % n != 0:
        segments[-1] += ''.join(lines[n*size:])

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
    segments = split_file(SEGMENT_FILE, NB_WORKERS)
    for seg in segments:
        segment_queue.put(seg)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(NB_WORKERS)
    print(f"[INFO] Serveur en écoute sur {HOST}:{PORT}")

    # Créer un pool de threads pour gérer les connexions
    with ThreadPoolExecutor(max_workers=NB_WORKERS) as executor:
        while not segment_queue.empty():
            conn, addr = server.accept()
            thread = executor.submit(handle_worker, conn, addr)
            active_threads.append(thread)

    # Attendre que tous les threads soient terminés
    for thread in active_threads:
        thread.result()

    # Récupérer et traiter les résultats
    results = process_results()
    final = reduce_function(results)
    
    with open("resultat_final.json", "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)
    print("[INFO] Résultat final écrit dans resultat_final.json")

if __name__ == '__main__':
    start_server()