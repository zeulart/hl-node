import os
import time
import redis
import inotify.adapters

LOG_DIR = os.getenv('LOG_PATH', '/home/hluser/hl/data/node_fills/hourly')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_STREAM = 'node_fills:ALL'   # Change si tu veux

def get_latest_file():
    """Retourne le chemin du fichier de log de l'heure courante (le + récent)."""
    days = sorted(os.listdir(LOG_DIR))
    if not days:
        return None
    
    # Essayer d'abord le dernier jour, puis l'avant-dernier si vide
    for day_candidate in reversed(days):
        day_dir = os.path.join(LOG_DIR, day_candidate)
        try:
            hours = sorted(os.listdir(day_dir))
            if hours:  # Si le répertoire du jour a des fichiers horaires
                last_hour = hours[-1]
                file_path = os.path.join(day_dir, last_hour)
                if os.path.exists(file_path) and os.path.getsize(file_path) >= 0:
                    return file_path
        except (OSError, IOError) as e:
            print(f"Erreur accès répertoire {day_dir}: {e}")
            continue
    
    return None

def tail_log_file(file_path, rds, watcher=None):
    """Lit en temps réel les nouvelles lignes du fichier file_path et pousse sur Redis."""
    print(f"Tailing live: {file_path}")
    last_check_time = time.time()
    last_day_check = time.time()
    
    with open(file_path, 'r') as f:
        f.seek(0, os.SEEK_END)  # Va à la fin, pour ne lire que le nouveau
        while True:
            where = f.tell()
            line = f.readline()
            if not line:
                time.sleep(0.2)
                
                current_time = time.time()
                
                # Vérifier périodiquement s'il y a un nouveau fichier plus récent
                if current_time - last_check_time > 10:  # Vérifier chaque 10 secondes
                    latest = get_latest_file()
                    if latest and latest != file_path:
                        print(f"Nouveau fichier détecté: {latest}")
                        return latest  # Retourner le nouveau fichier
                    last_check_time = current_time
                
                # Vérifier périodiquement les nouveaux répertoires de jour
                if current_time - last_day_check > 60:  # Vérifier chaque minute
                    check_and_add_new_day_watches(watcher)
                    last_day_check = current_time
                
                # Vérifier si le fichier existe toujours
                if not os.path.exists(file_path):
                    print("Fichier disparu (rotation ?). On quitte.")
                    break
                else:
                    continue
            rds.xadd(REDIS_STREAM, {'data': line.strip()})
    
    return None  # Pas de nouveau fichier

def setup_directory_watcher():
    """Configure inotify watcher pour détecter les nouveaux fichiers et répertoires."""
    try:
        i = inotify.adapters.Inotify()
        
        # Watch the base log directory for new date directories (changement de jour)
        if os.path.exists(LOG_DIR):
            i.add_watch(LOG_DIR)
            print(f"inotify: Surveillance du répertoire racine {LOG_DIR}")
            
            # Watch existing date directories for new hour files
            for day_dir in os.listdir(LOG_DIR):
                day_path = os.path.join(LOG_DIR, day_dir)
                if os.path.isdir(day_path):
                    i.add_watch(day_path)
                    print(f"inotify: Surveillance du répertoire jour {day_path}")
        
        return i
    except Exception as e:
        print(f"Erreur inotify setup: {e}")
        return None

def check_and_add_new_day_watches(watcher):
    """Ajoute la surveillance des nouveaux répertoires de jour détectés."""
    if not watcher:
        return
    
    try:
        # Vérifier s'il y a de nouveaux répertoires de jour à surveiller
        current_days = set()
        if os.path.exists(LOG_DIR):
            for day_dir in os.listdir(LOG_DIR):
                day_path = os.path.join(LOG_DIR, day_dir)
                if os.path.isdir(day_path):
                    current_days.add(day_path)
        
        # Ajouter surveillance pour nouveaux répertoires (simple, sans tracking complexe)
        for day_path in current_days:
            try:
                watcher.add_watch(day_path)
            except:
                pass  # Ignore si déjà surveillé
                
    except Exception as e:
        print(f"Erreur vérification nouveaux jours: {e}")

def main():
    rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    current_file = None
    
    # Setup inotify watcher pour détecter les nouveaux fichiers plus rapidement
    try:
        watcher = setup_directory_watcher()
        if watcher:
            print("inotify watcher configuré")
        else:
            print("Fallback sur polling simple")
    except Exception as e:
        print(f"Erreur inotify: {e}, fallback sur polling")
        watcher = None
    
    while True:
        latest = get_latest_file()
        if latest and os.path.exists(latest):
            if latest != current_file:
                print(f"Switch vers nouveau fichier: {latest}")
                current_file = latest
            
            # Tail le fichier, qui peut retourner un nouveau fichier
            next_file = tail_log_file(current_file, rds, watcher)
            if next_file:
                current_file = next_file
        else:
            print("Aucun fichier de log trouvé, attente...")
            time.sleep(2)

if __name__ == '__main__':
    main()