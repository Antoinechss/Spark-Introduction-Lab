# producer.py
# ------------------------------------------------------------
# Générateur de flux : serveur socket qui envoie des ventes
# Format envoyé (CSV) :
#   id,event_time,produit,quantite,prix_unitaire,region
# ------------------------------------------------------------
import socket
import random
import time
from datetime import datetime

HOST = "localhost"
PORT = 9999

produits = [
    ("Ordinateur", 850.0),
    ("Souris", 25.5),
    ("Clavier", 45.0),
    ("Écran", 220.0),
    ("Casque", 35.0),
]
regions = ["Nord", "Sud", "Est", "Ouest"]

# 1. Créer le serveur TCP
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((HOST, PORT))
server.listen(1)

print(f" Serveur socket en écoute sur {HOST}:{PORT}")
print(" En attente de la connexion de Spark...")

# 2. Attendre que Spark se connecte
conn, addr = server.accept()
print(f" Spark connecté depuis {addr}")
print(" Envoi de ventes toutes les 0.5s (Ctrl+C pour arrêter)\n")

# 3. Envoyer des ventes en continu
i = 1
try:
    while True:
        produit, prix = random.choice(produits)
        ligne = (
            f"{i},"
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},"
            f"{produit},"
            f"{random.randint(1, 10)},"
            f"{prix},"
            f"{random.choice(regions)}\n"
        )
        conn.sendall(ligne.encode("utf-8"))
        print(f"  → {ligne.strip()}")
        i += 1
        time.sleep(0.5)
except KeyboardInterrupt:
    print("\n Arrêt du producer.")
finally:
    conn.close()
    server.close()