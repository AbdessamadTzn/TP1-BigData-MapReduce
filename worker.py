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
BUFFER_SIZE = 8192  # Augmenté à 8KB pour plus de performance
SOCKET_TIMEOUT = 300  # Augmenté à 5 minutes

# ----------- FONCTION MAP -----------
def map_function(segment):
    pizza_count = 0
    # Utilisation d'un buffer pour le traitement
    buffer = []
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
        try:
            chunk = sock.recv(min(BUFFER_SIZE, expected_size - len(data)))
            if not chunk:
                raise ConnectionError("Connection perdue pendant la réception")
            data += chunk
            if len(data) % (5*1024*1024) == 0:  # Log tous les 5MB
                print(f"[INFO] Reçu: {len(data)/1024/1024:.1f}MB / {expected_size/1024/1024:.1f}MB")
        except socket.timeout:
            raise ConnectionError("Timeout pendant la réception")
    return data

def receive_message(sock):
    """Reçoit un message complet (taille + données)"""
    try:
        # Lire la taille
        size_data = receive_data(sock, 1024).decode('utf-8').strip()
        if not size_data:
            raise RuntimeError("Taille de données invalide")
        
        # Séparer la taille du reste des données
        try:
            size_str, rest = size_data.split('\n', 1)
            size = int(size_str)
        except (ValueError, IndexError):
            raise RuntimeError(f"Format de taille invalide: {size_data}")
            
        if size <= 0:
            raise RuntimeError(f"Taille de données invalide: {size}")
        
        # Lire le reste des données
        remaining_size = size - len(rest.encode('utf-8'))
        if remaining_size > 0:
            additional_data = receive_data(sock, remaining_size)
            data = rest + additional_data.decode('utf-8')
        else:
            data = rest
        
        return data.strip()
    except socket.timeout:
        raise ConnectionError("Timeout pendant la réception du message")
    except Exception as e:
        raise RuntimeError(f"Erreur lors de la réception du message: {str(e)}")

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
            try:
                sent = sock.send(data_bytes[total_sent:total_sent + BUFFER_SIZE])
                if sent == 0:
                    raise RuntimeError("Connexion perdue")
                total_sent += sent
                
                # Afficher la progression tous les 5MB
                if total_sent % (5 * 1024 * 1024) == 0:
                    print(f"[INFO] Envoi: {total_sent / (1024 * 1024):.1f}MB / {len(data_bytes) / (1024 * 1024):.1f}MB")
            except socket.timeout:
                raise ConnectionError("Timeout pendant l'envoi")
                
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
            sock.settimeout(SOCKET_TIMEOUT)
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
            time.sleep(5)  # Attendre plus longtemps en cas de timeout
        except ConnectionError as e:
            print(f"[ERREUR] Erreur de connexion: {e}")
            time.sleep(5)  # Attendre plus longtemps en cas d'erreur de connexion
        except Exception as e:
            print(f"[ERREUR] Erreur lors du traitement : {e}")
            time.sleep(5)  # Attendre plus longtemps en cas d'erreur
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
            time.sleep(10)  # Attendre plus longtemps en cas d'erreur fatale
