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

# Timeout configurations for consistent behavior
INOTIFY_TIMEOUT_SHORT = 0.5  # Quick event checks during active processing
INOTIFY_TIMEOUT_MEDIUM = 2.0  # Regular event checks during idle periods
INOTIFY_CHECK_INTERVAL = 2  # How often to check for rotation events

# Global shutdown flag
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

def get_candidate_files(max_files=200):
    """Get potential log files for analysis with memory limits"""
    candidates = []
    try:
        if not os.path.exists(LOG_DIR):
            logger.error(f"Log directory does not exist: {LOG_DIR}")
            return candidates
            
        days = sorted(os.listdir(LOG_DIR))
        if not days:
            logger.warning(f"No date directories found in {LOG_DIR}")
            return candidates
        
        # Look at the last 2 days to handle day transitions
        for day_candidate in reversed(days[-2:]):
            day_dir = os.path.join(LOG_DIR, day_candidate)
            try:
                hours = sorted(os.listdir(day_dir))
                for hour_file in hours:
                    # Memory protection: limit number of candidate files
                    if len(candidates) >= max_files:
                        logger.warning(f"Reached maximum candidate files limit ({max_files}), stopping scan")
                        break
                        
                    file_path = os.path.join(day_dir, hour_file)
                    if os.path.isfile(file_path) and os.path.getsize(file_path) >= 0:
                        candidates.append(file_path)
                        
                # Break outer loop if we hit the limit
                if len(candidates) >= max_files:
                    break
                    
            except (OSError, IOError) as e:
                logger.warning(f"Error accessing directory {day_dir}: {e}")
                continue
        
        # Sort candidates by modification time (newest first) and take most recent
        if len(candidates) > max_files // 2:  # Keep most recent half if we have many
            candidates.sort(key=lambda f: os.path.getmtime(f) if os.path.exists(f) else 0, reverse=True)
            candidates = candidates[:max_files // 2]
            logger.debug(f"Trimmed to {len(candidates)} most recent candidate files")
        
        logger.debug(f"Found {len(candidates)} candidate files")
        return candidates
        
    except Exception as e:
        logger.error(f"Unexpected error getting candidate files: {e}")
        return candidates

def find_active_file_at_startup():
    """Find actively growing log file using size-based detection"""
    logger.info("Starting smart file detection at startup")
    
    candidates = get_candidate_files()
    if not candidates:
        logger.error("No candidate log files found")
        return None
    
    logger.info(f"Analyzing {len(candidates)} candidate files for growth")
    
    # Take initial size measurements
    initial_stats = {}
    for file_path in candidates:
        try:
            if os.path.exists(file_path):
                stat = os.stat(file_path)
                initial_stats[file_path] = {
                    'size': stat.st_size,
                    'mtime': stat.st_mtime
                }
        except Exception as e:
            logger.warning(f"Failed to stat file {file_path}: {e}")
    
    if not initial_stats:
        logger.error("No files available for analysis")
        return None
    
    logger.debug("Waiting 5 seconds to measure file growth...")
    time.sleep(5)
    
    # Measure growth
    grown_files = []
    for file_path, initial in initial_stats.items():
        try:
            if os.path.exists(file_path):
                current_size = os.path.getsize(file_path)
                growth = current_size - initial['size']
                if growth > 0:
                    grown_files.append((file_path, growth))
                    logger.debug(f"File {file_path} grew by {growth} bytes")
        except Exception as e:
            logger.warning(f"Failed to measure growth for {file_path}: {e}")
    
    if grown_files:
        # Return file with most growth
        active_file, growth = max(grown_files, key=lambda x: x[1])
        logger.info(f"Selected actively growing file: {active_file} (grew {growth} bytes)")
        return active_file
    else:
        # Fallback to most recent file by modification time
        most_recent = max(initial_stats.items(), key=lambda x: x[1]['mtime'])
        logger.info(f"No growing files detected, using most recent: {most_recent[0]}")
        return most_recent[0]

def process_inotify_events(watcher, timeout_s=INOTIFY_TIMEOUT_MEDIUM):
    """Process inotify events for file rotation detection"""
    if not watcher:
        return None
        
    try:
        for event in watcher.event_gen(yield_nones=False, timeout_s=timeout_s):
            if event:
                (_, type_names, path, filename) = event
                
                # Skip directory events
                if 'IN_ISDIR' in type_names:
                    continue
                
                full_path = os.path.join(path, filename)
                
                # New hourly file created
                if 'IN_CREATE' in type_names:
                    logger.info(f"New file detected via inotify: {full_path}")
                    return full_path
                    
                # File moved into directory (common in log rotation)
                if 'IN_MOVED_TO' in type_names:
                    logger.info(f"File moved in via inotify: {full_path}")
                    return full_path
                    
        return None
        
    except Exception as e:
        logger.warning(f"Error processing inotify events: {e}")
        return None

def wait_for_new_file_via_inotify(watcher, max_wait_time=30):
    """Wait for a new log file to be created via inotify events"""
    logger.info(f"Waiting for new file via inotify (max {max_wait_time}s)")
    
    start_time = time.time()
    event_count = 0
    max_events = max_wait_time * 2  # Prevent infinite loops on bad events
    
    while (not shutdown_requested and 
           (time.time() - start_time) < max_wait_time and 
           event_count < max_events):
        
        new_file = process_inotify_events(watcher, timeout_s=INOTIFY_TIMEOUT_MEDIUM)
        event_count += 1
        
        if new_file and os.path.isfile(new_file):
            logger.info(f"Found new file via inotify: {new_file}")
            return new_file
        
        # Small delay to prevent tight loops on rapid invalid events
        if event_count % 10 == 0:
            time.sleep(0.1)
    
    # Timeout or max events reached, fallback to candidate search
    if event_count >= max_events:
        logger.warning(f"Max inotify events ({max_events}) reached, falling back to file search")
    else:
        logger.warning("inotify timeout reached, falling back to file search")
        
    try:
        candidates = get_candidate_files()
        if candidates:
            most_recent = max(candidates, key=lambda f: os.path.getmtime(f) if os.path.exists(f) else 0)
            logger.info(f"Fallback: using most recent file {most_recent}")
            return most_recent
    except Exception as e:
        logger.error(f"Error in fallback file search: {e}")
    
    return None

def tail_log_file_with_inotify(file_path, rds, watcher=None):
    """Tail log file in real-time using inotify for rotation detection"""
    logger.info(f"Starting to tail log file: {file_path}")
    last_day_check = time.time()
    lines_processed = 0
    last_inotify_check = time.time()
    
    try:
        with open(file_path, 'r') as f:
            f.seek(0, os.SEEK_END)  # Go to end to read only new content
            logger.debug(f"Positioned at end of file: {file_path}")
            
            while not shutdown_requested:
                line = f.readline()
                if not line:
                    time.sleep(0.2)
                    current_time = time.time()
                    
                    # Check for file rotation via inotify (every N seconds when idle)
                    if current_time - last_inotify_check > INOTIFY_CHECK_INTERVAL:
                        try:
                            new_file = process_inotify_events(watcher, timeout_s=INOTIFY_TIMEOUT_SHORT)
                            if new_file and new_file != file_path and os.path.isfile(new_file):
                                # Verify the new file is actually newer (handle race conditions)
                                try:
                                    new_mtime = os.path.getmtime(new_file)
                                    current_mtime = os.path.getmtime(file_path)
                                    if new_mtime > current_mtime:
                                        logger.info(f"inotify detected newer file: {new_file}")
                                        return new_file
                                except (OSError, IOError) as e:
                                    # Race condition: file disappeared during mtime check
                                    logger.warning(f"Race condition during file comparison: {e}")
                                    # If we can't compare times, trust inotify and switch anyway
                                    logger.info(f"Trusting inotify event, switching to: {new_file}")
                                    return new_file
                            last_inotify_check = current_time
                        except Exception as e:
                            logger.warning(f"Error checking inotify events: {e}")
                    
                    # Check for new day directories periodically (keep this for day transitions)
                    if current_time - last_day_check > 60:
                        try:
                            check_and_add_new_day_watches(watcher)
                            last_day_check = current_time
                        except Exception as e:
                            logger.warning(f"Error updating directory watches: {e}")
                    
                    # Check if current file still exists
                    if not os.path.exists(file_path):
                        logger.warning(f"Current file disappeared: {file_path}")
                        # Use inotify to find replacement file
                        new_file = wait_for_new_file_via_inotify(watcher, max_wait_time=10)
                        if new_file:
                            logger.info(f"Found replacement file via inotify: {new_file}")
                            return new_file
                        else:
                            logger.error("No replacement file found, stopping")
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
        logger.error(f"Unexpected error in tail_log_file_with_inotify: {e}")
    
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
    
    # Smart startup file detection
    current_file = find_active_file_at_startup()
    if not current_file:
        logger.error("Failed to find any log file at startup")
        return 1
    
    # Main processing loop with inotify-based approach
    try:
        while not shutdown_requested:
            if current_file and os.path.exists(current_file):
                logger.info(f"Processing file: {current_file}")
                
                # Tail the file using inotify for rotation detection
                next_file = tail_log_file_with_inotify(current_file, rds, watcher)
                if next_file and not shutdown_requested:
                    current_file = next_file
                else:
                    # Current file finished, wait for new file via inotify
                    logger.info("Current file finished, waiting for new file...")
                    current_file = wait_for_new_file_via_inotify(watcher, max_wait_time=60)
                    
                    if not current_file:
                        logger.warning("No new files detected via inotify, using fallback detection")
                        candidates = get_candidate_files()
                        if candidates:
                            current_file = max(candidates, key=os.path.getmtime)
                            logger.info(f"Fallback: selected {current_file}")
                        else:
                            logger.warning("No candidate files found, waiting...")
                            time.sleep(5)
            else:
                logger.warning("No current file available, searching for new files...")
                current_file = wait_for_new_file_via_inotify(watcher, max_wait_time=30)
                if not current_file:
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
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")
        
        # Cleanup inotify watcher
        try:
            if watcher:
                # The inotify watcher should be closed automatically when it goes out of scope
                # but we can try to clean up explicitly if there's a close method
                if hasattr(watcher, 'close'):
                    watcher.close()
                elif hasattr(watcher, '_inotify') and hasattr(watcher._inotify, 'close'):
                    watcher._inotify.close()
                logger.info("inotify watcher cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up inotify watcher: {e}")
            
    logger.info("Hyperliquid log streaming service stopped")
    return 0

if __name__ == '__main__':
    sys.exit(main())