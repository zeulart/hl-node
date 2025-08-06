import os
import sys
import time
import signal
import logging
import redis
import inotify.adapters

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Configuration
LOG_DIR = os.getenv('LOG_PATH', '/home/hluser/hl/data/node_fills/hourly')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_STREAM = 'node_fills:ALL'

# Global shutdown flag
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

def get_latest_file():
    """Find the most recent log file"""
    try:
        if not os.path.exists(LOG_DIR):
            logger.error(f"Log directory does not exist: {LOG_DIR}")
            return None
            
        days = sorted(os.listdir(LOG_DIR))
        if not days:
            logger.warning(f"No date directories found in {LOG_DIR}")
            return None
        
        # Try latest day first, then previous days if empty
        for day_candidate in reversed(days):
            day_dir = os.path.join(LOG_DIR, day_candidate)
            try:
                hours = sorted(os.listdir(day_dir))
                if hours:  # If day directory has hourly files
                    last_hour = hours[-1]
                    file_path = os.path.join(day_dir, last_hour)
                    if os.path.exists(file_path) and os.path.getsize(file_path) >= 0:
                        logger.debug(f"Found latest log file: {file_path}")
                        return file_path
            except (OSError, IOError) as e:
                logger.warning(f"Error accessing directory {day_dir}: {e}")
                continue
        
        logger.error("No valid log files found in any date directory")
        return None
        
    except Exception as e:
        logger.error(f"Unexpected error in get_latest_file: {e}")
        return None

def tail_log_file(file_path, rds, watcher=None):
    """Tail log file in real-time and push new lines to Redis"""
    logger.info(f"Starting to tail log file: {file_path}")
    last_check_time = time.time()
    last_day_check = time.time()
    lines_processed = 0
    
    try:
        with open(file_path, 'r') as f:
            f.seek(0, os.SEEK_END)  # Go to end to read only new content
            logger.debug(f"Positioned at end of file: {file_path}")
            
            while not shutdown_requested:
                line = f.readline()
                if not line:
                    time.sleep(0.2)
                    current_time = time.time()
                    
                    # Check for newer file periodically
                    if current_time - last_check_time > 10:
                        try:
                            latest = get_latest_file()
                            if latest and latest != file_path:
                                logger.info(f"Newer file detected, switching to: {latest}")
                                return latest
                            last_check_time = current_time
                        except Exception as e:
                            logger.warning(f"Error checking for latest file: {e}")
                    
                    # Check for new day directories periodically
                    if current_time - last_day_check > 60:
                        try:
                            check_and_add_new_day_watches(watcher)
                            last_day_check = current_time
                        except Exception as e:
                            logger.warning(f"Error updating directory watches: {e}")
                    
                    # Check if file still exists
                    if not os.path.exists(file_path):
                        logger.warning(f"File disappeared (rotation?): {file_path}")
                        break
                    
                    continue
                
                # Process the line
                try:
                    stripped_line = line.strip()
                    if stripped_line:  # Only process non-empty lines
                        rds.xadd(REDIS_STREAM, {'data': stripped_line})
                        lines_processed += 1
                        
                        if lines_processed % 1000 == 0:
                            logger.debug(f"Processed {lines_processed} lines from {file_path}")
                            
                except redis.RedisError as e:
                    logger.error(f"Redis error while adding to stream: {e}")
                    # Depending on strategy, might want to reconnect or exit
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error processing line: {e}")
                    continue
                    
    except FileNotFoundError:
        logger.error(f"Log file not found: {file_path}")
    except PermissionError:
        logger.error(f"Permission denied accessing file: {file_path}")
    except Exception as e:
        logger.error(f"Unexpected error in tail_log_file: {e}")
    
    logger.info(f"Finished tailing {file_path}, processed {lines_processed} lines")
    return None

def setup_directory_watcher():
    """Configure inotify watcher to detect new files and directories"""
    try:
        i = inotify.adapters.Inotify()
        
        # Watch the base log directory for new date directories
        if os.path.exists(LOG_DIR):
            i.add_watch(LOG_DIR)
            logger.info(f"inotify: Watching root directory {LOG_DIR}")
            
            # Watch existing date directories for new hour files
            for day_dir in os.listdir(LOG_DIR):
                day_path = os.path.join(LOG_DIR, day_dir)
                if os.path.isdir(day_path):
                    try:
                        i.add_watch(day_path)
                        logger.debug(f"inotify: Watching day directory {day_path}")
                    except Exception as e:
                        logger.warning(f"Failed to watch directory {day_path}: {e}")
        else:
            logger.error(f"Log directory does not exist: {LOG_DIR}")
            return None
        
        return i
    except Exception as e:
        logger.error(f"Error setting up inotify watcher: {e}")
        return None

def check_and_add_new_day_watches(watcher):
    """Add watches for newly detected day directories"""
    if not watcher:
        logger.debug("No watcher provided, skipping day watch update")
        return
    
    try:
        # Check for new day directories to watch
        current_days = set()
        if os.path.exists(LOG_DIR):
            for day_dir in os.listdir(LOG_DIR):
                day_path = os.path.join(LOG_DIR, day_dir)
                if os.path.isdir(day_path):
                    current_days.add(day_path)
        
        # Add watches for new directories (simple approach without complex tracking)
        for day_path in current_days:
            try:
                watcher.add_watch(day_path)
                logger.debug(f"Added/refreshed watch for directory: {day_path}")
            except Exception as e:
                # This is expected for already-watched directories
                logger.debug(f"Watch already exists for {day_path}: {e}")
                
    except Exception as e:
        logger.warning(f"Error checking for new day directories: {e}")

def main():
    """Main streaming service loop"""
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Starting Hyperliquid log streaming service")
    logger.info(f"Log directory: {LOG_DIR}")
    logger.info(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"Redis stream: {REDIS_STREAM}")
    
    # Setup Redis connection with error handling
    try:
        rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        # Test connection
        rds.ping()
        logger.info("Redis connection established successfully")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error connecting to Redis: {e}")
        return 1
    
    current_file = None
    
    # Setup inotify watcher for faster file detection
    try:
        watcher = setup_directory_watcher()
        if watcher:
            logger.info("inotify watcher configured successfully")
        else:
            logger.warning("inotify watcher failed, using simple polling fallback")
    except Exception as e:
        logger.warning(f"inotify setup error: {e}, falling back to simple polling")
        watcher = None
    
    # Main processing loop
    try:
        while not shutdown_requested:
            latest = get_latest_file()
            if latest and os.path.exists(latest):
                if latest != current_file:
                    logger.info(f"Switching to new file: {latest}")
                    current_file = latest
                
                # Tail the file, which may return a newer file
                next_file = tail_log_file(current_file, rds, watcher)
                if next_file and not shutdown_requested:
                    current_file = next_file
            else:
                logger.warning("No log files found, waiting...")
                time.sleep(2)
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")
        return 1
    finally:
        logger.info("Cleaning up...")
        try:
            rds.close()
            logger.info("Redis connection closed")
        except:
            pass
            
    logger.info("Hyperliquid log streaming service stopped")
    return 0

if __name__ == '__main__':
    sys.exit(main())