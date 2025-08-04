import os
import asyncio
from typing import Dict, Set, Callable, Optional
from pathlib import Path
import structlog

logger = structlog.get_logger(__name__)

try:
    import inotify.adapters
    INOTIFY_AVAILABLE = True
except ImportError:
    INOTIFY_AVAILABLE = False
    logger.warning("inotify not available, falling back to polling")


class FileWatcher:
    def __init__(self, poll_interval: float = 0.1):
        self.poll_interval = poll_interval
        self.watched_files: Dict[str, float] = {}  # path -> last_modified
        self.callbacks: Dict[str, Callable[[str], None]] = {}
        self.running = False
        self.use_inotify = INOTIFY_AVAILABLE
        
    def add_watch(self, file_path: str, callback: Callable[[str], None]):
        path = str(Path(file_path).resolve())
        self.watched_files[path] = 0.0  # Will be updated on first check
        self.callbacks[path] = callback
        logger.info("Added file watch", path=path, use_inotify=self.use_inotify)
    
    def remove_watch(self, file_path: str):
        path = str(Path(file_path).resolve())
        self.watched_files.pop(path, None)
        self.callbacks.pop(path, None)
        logger.info("Removed file watch", path=path)
    
    async def start_watching(self):
        if self.use_inotify:
            await self._watch_with_inotify()
        else:
            await self._watch_with_polling()
    
    async def _watch_with_polling(self):
        logger.info("Starting file watching with polling", interval=self.poll_interval)
        self.running = True
        
        while self.running:
            try:
                for file_path in list(self.watched_files.keys()):
                    await self._check_file_polling(file_path)
                    
                await asyncio.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error("Error in polling loop", error=str(e))
                await asyncio.sleep(1.0)
    
    async def _check_file_polling(self, file_path: str):
        try:
            if not os.path.exists(file_path):
                return
                
            stat_info = os.stat(file_path)
            current_mtime = stat_info.st_mtime
            last_mtime = self.watched_files.get(file_path, 0.0)
            
            if current_mtime > last_mtime:
                self.watched_files[file_path] = current_mtime
                callback = self.callbacks.get(file_path)
                if callback:
                    try:
                        callback(file_path)
                    except Exception as e:
                        logger.error("Error in file callback", path=file_path, error=str(e))
                        
        except Exception as e:
            logger.error("Error checking file", path=file_path, error=str(e))
    
    async def _watch_with_inotify(self):
        logger.info("Starting file watching with inotify")
        self.running = True
        
        # Create inotify adapter for watched directories
        watched_dirs = set()
        for file_path in self.watched_files.keys():
            dir_path = str(Path(file_path).parent)
            watched_dirs.add(dir_path)
        
        if not watched_dirs:
            logger.warning("No directories to watch")
            return
            
        i = inotify.adapters.Inotify()
        
        try:
            # Add watches for directories
            for dir_path in watched_dirs:
                if os.path.exists(dir_path):
                    i.add_watch(dir_path, mask=inotify.constants.IN_MODIFY | inotify.constants.IN_MOVED_TO)
                    logger.info("Added inotify watch", directory=dir_path)
            
            # Event loop
            while self.running:
                # Get events with timeout
                events = i.event_gen(yield_nones=False, timeout_s=1)
                
                for event in events:
                    if not self.running:
                        break
                        
                    (_, type_names, path, filename) = event
                    
                    if filename:
                        full_path = os.path.join(path, filename)
                        await self._handle_inotify_event(full_path, type_names)
                        
        except Exception as e:
            logger.error("Error in inotify loop", error=str(e))
        finally:
            try:
                for dir_path in watched_dirs:
                    i.remove_watch(dir_path)
            except:
                pass
    
    async def _handle_inotify_event(self, file_path: str, event_types: list):
        # Check if this file is in our watch list
        if file_path not in self.watched_files:
            return
            
        # Handle modify events
        if 'IN_MODIFY' in event_types or 'IN_MOVED_TO' in event_types:
            callback = self.callbacks.get(file_path)
            if callback:
                try:
                    callback(file_path)
                except Exception as e:
                    logger.error("Error in inotify callback", path=file_path, error=str(e))
    
    def stop(self):
        self.running = False
        logger.info("Stopping file watcher")


class DirectoryWatcher:
    def __init__(self, base_path: str, pattern: str = "*.log"):
        self.base_path = Path(base_path)
        self.pattern = pattern
        self.file_watchers: Dict[str, FileWatcher] = {}
        self.running = False
        
    async def start_watching_directory(self, callback: Callable[[str], None]):
        logger.info("Starting directory watch", path=str(self.base_path), pattern=self.pattern)
        self.running = True
        
        while self.running:
            try:
                # Discover only the most recent file matching pattern
                current_files = set()
                if self.base_path.exists():
                    newest_file = None
                    newest_mtime = 0
                    
                    for file_path in self.base_path.rglob(self.pattern):
                        if file_path.is_file():
                            mtime = file_path.stat().st_mtime
                            if mtime > newest_mtime:
                                newest_mtime = mtime
                                newest_file = file_path
                    
                    if newest_file:
                        current_files.add(str(newest_file))
                        logger.info("Found newest log file", path=str(newest_file), mtime=newest_mtime)
                
                # Add watchers for new files
                existing_files = set(self.file_watchers.keys())
                new_files = current_files - existing_files
                
                for file_path in new_files:
                    watcher = FileWatcher()
                    watcher.add_watch(file_path, callback)
                    self.file_watchers[file_path] = watcher
                    
                    # Start watching in background
                    asyncio.create_task(watcher.start_watching())
                    logger.info("Started watching new file", path=file_path)
                
                # Remove watchers for deleted files
                deleted_files = existing_files - current_files
                for file_path in deleted_files:
                    watcher = self.file_watchers.pop(file_path, None)
                    if watcher:
                        watcher.stop()
                        logger.info("Stopped watching deleted file", path=file_path)
                
                await asyncio.sleep(5.0)  # Check for new files every 5 seconds
                
            except Exception as e:
                logger.error("Error in directory watching", error=str(e))
                await asyncio.sleep(10.0)
    
    def stop(self):
        self.running = False
        for watcher in self.file_watchers.values():
            watcher.stop()
        self.file_watchers.clear()
        logger.info("Stopped directory watcher")