# Added: os for file checks
import os
import time
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from cli.trade_ingester import ingest_periodic
from cli.trade_ingester import Aggregator, aggregate_process
from cli.activity_monitor import ActivityMonitor
import queue

class NewFileHandler(FileSystemEventHandler):
    def __init__(self, executor, launcher):
        self.executor = executor
        self.launch_ingest = launcher

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        print(f"New file detected: {file_path}. Submitting to thread pool.")
        try:
            self.launch_ingest(file_path)
        except Exception as e:
            print(f"Error: Failed to submit ingest task for {file_path}: {e}")

def listen(path):
    print(f"Listening for new files in {path} (including existing growing files)...")
    # A with statement ensures the thread pool is properly shut down
    with ThreadPoolExecutor(max_workers=10) as executor:
        aggregator = Aggregator(bucket_size_sec=30)
        q = queue.Queue()
        activity_monitor = ActivityMonitor(buffer_size=120)

        active_files: set[str] = set()

        def launch_ingest(file_path: str):
            """Submit ingest job for *file_path* if not already active."""
            if file_path in active_files:
                return

            def ingest_with_agg():
                ingest_periodic(
                    file_path,
                    process=lambda lines: aggregate_process(lines, aggregator, q),
                )

            print(f"Submitting ingest for existing file: {file_path}")
            executor.submit(ingest_with_agg)
            active_files.add(file_path)

        # ---------------------------------------------------------
        # Detect currently growing file(s) before starting observer
        # ---------------------------------------------------------

        def detect_growing_files(base_path: str, delay: float = 1.0):
            """Return list of files that increase in size over *delay* seconds."""
            print(f"Scanning {base_path} for growing files...")
            print(f"Recursively walking directory tree...")
            snapshot = {}
            for root, _, files in os.walk(base_path):
                print(f"  Checking directory: {root}")
                for fn in files:
                    fp = os.path.join(root, fn)
                    try:
                        size = os.path.getsize(fp)
                        snapshot[fp] = size
                        print(f"  Found file: {fp} (size: {size} bytes)")
                    except OSError:
                        print(f"    Error accessing: {fp}")

            if not snapshot:
                print("  No candidate files found.")
                return []

            print(f"Waiting {delay}s to detect file growth...")
            time.sleep(delay)

            growing = []
            for fp, old_size in snapshot.items():
                try:
                    new_size = os.path.getsize(fp)
                    if new_size > old_size:
                        print(f"  Growing file detected: {fp} ({old_size} -> {new_size} bytes)")
                        growing.append(fp)
                    else:
                        print(f"  No growth: {fp} (still {old_size} bytes)")
                except OSError:
                    print(f"    Error re-checking: {fp}")
            
            if not growing:
                print("  No growing files detected.")
            
            return growing

        def find_most_recent_log(base_path: str):
            """Find the most recently modified log file as fallback."""
            print(f"Looking for most recent log file in {base_path}...")
            print(f"Recursively walking directory tree...")
            latest_file = None
            latest_time = 0
            
            for root, _, files in os.walk(base_path):
                print(f"  Checking directory: {root}")
                for fn in files:
                    fp = os.path.join(root, fn)
                    try:
                        mtime = os.path.getmtime(fp)
                        if mtime > latest_time:
                            latest_time = mtime
                            latest_file = fp
                        print(f"    Found log file: {fp} (mtime: {mtime})")
                    except OSError:
                        print(f"    Error accessing: {fp}")
            
            if latest_file:
                print(f"  Most recent log file: {latest_file}")
            else:
                print("  No log files found.")
            
            return latest_file

        for growing_file in detect_growing_files(path, delay=3.0):
            launch_ingest(growing_file)
        
        # Fallback: if no growing files, ingest the most recent one
        if not active_files:
            recent_file = find_most_recent_log(path)
            if recent_file:
                print("No growing files detected, falling back to most recent log file.")
                launch_ingest(recent_file)

        # Spawn activity monitor consumer thread
        executor.submit(activity_monitor.consume_from_queue, q)

        event_handler = NewFileHandler(executor, launch_ingest)
        observer = Observer()
        # Using recursive=True to watch for files in subdirectories like .../hourly/YYYYMMDD/HH
        observer.schedule(event_handler, path, recursive=True)
        observer.start()
        print("Listener started. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutdown signal received. Stopping observer and shutting down threads...")
            observer.stop()
        observer.join()
        print("Observer stopped. All tasks complete.")