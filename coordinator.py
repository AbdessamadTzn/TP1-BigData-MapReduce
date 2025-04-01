"""
Coordinator node for the MapReduce implementation
"""
import socket
import json
import logging
import threading
from typing import List, Dict
import os
from config import *
from collections import defaultdict
import time

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT
)
logger = logging.getLogger('Coordinator')

class Coordinator:
    def __init__(self, host: str = HOST, port: int = PORT):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.results: List[Dict] = []
        self.active_workers = 0
        self.lock = threading.Lock()
        self.chunks: List[str] = []
        self.processed_chunks = set()
        self.running = True

    def start(self):
        """Start the coordinator server"""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        logger.info(f"Coordinator listening on {self.host}:{self.port}")

    def split_file(self, file_path: str) -> List[str]:
        """Split the input file into chunks"""
        chunks = []
        with open(file_path, 'r', encoding='utf-8') as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
        return chunks

    def handle_worker(self, client_socket: socket.socket, address: tuple):
        """Handle individual worker connections"""
        try:
            # Receive the result from the worker
            data = client_socket.recv(4096).decode('utf-8')
            if data:
                result = json.loads(data)
                with self.lock:
                    self.results.append(result)
                    self.active_workers -= 1
                logger.info(f"Received result from {address}")
        except Exception as e:
            logger.error(f"Error handling worker {address}: {e}")
            # Redistribute the chunk if there was an error
            self.redistribute_chunk()
        finally:
            client_socket.close()

    def redistribute_chunk(self):
        """Redistribute a chunk that failed to process"""
        with self.lock:
            # Find an unprocessed chunk
            for i, chunk in enumerate(self.chunks):
                if i not in self.processed_chunks:
                    self.processed_chunks.add(i)
                    return chunk
        return None

    def distribute_tasks(self, file_path: str):
        """Distribute tasks to workers"""
        self.chunks = self.split_file(file_path)
        logger.info(f"Split file into {len(self.chunks)} chunks")

        # Wait for workers to connect and distribute tasks
        while self.running and len(self.processed_chunks) < len(self.chunks):
            try:
                client_socket, address = self.server_socket.accept()
                self.active_workers += 1
                logger.info(f"New worker connected from {address}")

                # Send chunk to worker
                chunk = self.redistribute_chunk()
                if chunk:
                    client_socket.send(json.dumps({"chunk": chunk}).encode('utf-8'))

                    # Handle worker in a separate thread
                    thread = threading.Thread(
                        target=self.handle_worker,
                        args=(client_socket, address)
                    )
                    thread.start()
                else:
                    client_socket.close()
                    self.active_workers -= 1

            except Exception as e:
                logger.error(f"Error distributing tasks: {e}")
                self.active_workers -= 1

    def reduce(self) -> Dict:
        """Aggregate results from all workers"""
        final_result = defaultdict(int)
        for result in self.results:
            for word, count in result.items():
                final_result[word] += count
        return dict(final_result)

    def run(self, file_path: str):
        """Main execution method"""
        try:
            self.start()
            self.distribute_tasks(file_path)
            
            # Wait for all chunks to be processed
            while len(self.processed_chunks) < len(self.chunks):
                pass

            # Perform reduce operation
            final_result = self.reduce()
            
            # Save results
            with open('results.json', 'w') as f:
                json.dump(final_result, f, indent=4)
            logger.info("Results saved to results.json")

        except Exception as e:
            logger.error(f"Error in coordinator: {e}")
        finally:
            self.running = False
            self.server_socket.close()

def reduce_function(partial_results):
    final_count = defaultdict(int)
    for result in partial_results:
        for word, count in result.items():
            final_count[word] += count
    return dict(final_count)

def coordinator(file_path):
    # Lecture du fichier et découpage en segments
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    segments = [text[i:i+CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)]
    
    # Initialisation du serveur
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"Coordinateur démarré sur {HOST}:{PORT}")
    
    # Gestion des résultats
    results = []
    segments_to_process = list(range(len(segments)))
    lock = threading.Lock()
    
    def handle_client(client_socket, addr):
        try:
            with lock:
                if not segments_to_process:
                    return
                segment_id = segments_to_process.pop(0)
            
            # Envoi du segment
            msg = json.dumps({
                "segment_id": segment_id,
                "segment": segments[segment_id]
            }) + '\n'
            client_socket.sendall(msg.encode())
            
            # Réception du résultat
            data = b""
            while not data.endswith(b"\n"):
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
            
            if data:
                result = json.loads(data.decode())["result"]
                with lock:
                    results.append(result)
                print(f"Résultat reçu du worker {addr}")
            
        except Exception as e:
            print(f"Erreur avec le worker {addr}: {e}")
            with lock:
                segments_to_process.append(segment_id)
        finally:
            client_socket.close()
    
    # Boucle principale
    try:
        while segments_to_process:
            client_socket, addr = server.accept()
            print(f"Connexion de {addr}")
            thread = threading.Thread(target=handle_client, args=(client_socket, addr))
            thread.start()
        
        # Attendre que tous les segments soient traités
        while len(results) < len(segments):
            time.sleep(1)
        
        # Phase de reduce
        final_result = reduce_function(results)
        
        # Sauvegarde du résultat
        with open('result.json', 'w') as f:
            json.dump(final_result, f, indent=2)
        print("Résultat final sauvegardé dans result.json")
        
    except KeyboardInterrupt:
        print("Arrêt du coordinateur")
    finally:
        server.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python coordinator.py <input_file>")
        sys.exit(1)
    
    coordinator = Coordinator()
    coordinator.run(sys.argv[1]) 