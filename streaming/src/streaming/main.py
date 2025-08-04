import os
import time
import redis

LOG_DIR = os.getenv('LOG_PATH', '/home/hluser/hl/data/node_fills/hourly')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_STREAM = 'node_fills:ALL'   # Change si tu veux

def get_latest_file():
    """Retourne le chemin du fichier de log de l'heure courante (le + récent)."""
    days = sorted(os.listdir(LOG_DIR))
    if not days:
        return None
    last_day = days[-1]
    last_day_dir = os.path.join(LOG_DIR, last_day)
    hours = sorted(os.listdir(last_day_dir))
    if not hours:
        return None
    last_hour = hours[-1]
    return os.path.join(last_day_dir, last_hour)

def tail_log_file(file_path, rds):
    """Lit en temps réel les nouvelles lignes du fichier file_path et pousse sur Redis."""
    print(f"Tailing live: {file_path}")
    with open(file_path, 'r') as f:
        f.seek(0, os.SEEK_END)  # Va à la fin, pour ne lire que le nouveau
        while True:
            where = f.tell()
            line = f.readline()
            if not line:
                time.sleep(0.2)
                # Option : détecter si on a changé d’heure (rotation)
                if not os.path.exists(file_path):
                    print("Fichier disparu (rotation ?). On quitte.")
                    break
                else:
                    continue
            rds.xadd(REDIS_STREAM, {'data': line.strip()})

def main():
    rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    while True:
        latest = get_latest_file()
        if latest and os.path.exists(latest):
            tail_log_file(latest, rds)
        else:
            print("Aucun fichier de log trouvé, attente...")
            time.sleep(2)

if __name__ == '__main__':
    main()
