import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Set
from pathlib import Path
import structlog

from ..core.log_reader import MemoryMappedLogReader, LogReaderPool
from ..core.file_watcher import FileWatcher, DirectoryWatcher
from ..core.redis_client import RedisStreamsClient

logger = structlog.get_logger(__name__)


class BaseProducer(ABC):
    def __init__(
        self,
        name: str,
        redis_client: RedisStreamsClient,
        config: Dict[str, Any]
    ):
        self.name = name
        self.redis_client = redis_client
        self.config = config
        
        # Core components
        self.log_reader_pool = LogReaderPool()
        self.file_watcher: Optional[FileWatcher] = None
        self.directory_watcher: Optional[DirectoryWatcher] = None
        
        # Runtime state
        self.running = False
        self.coins: Set[str] = set(config.get("coins", []))
        self.stream_prefix = config.get("stream_prefix", self.name)
        self.max_retention = config.get("max_retention", 100000)
        self.batch_size = config.get("batch_size", 1000)
        self.poll_interval_ms = config.get("poll_interval_ms", 10)
        
        # Performance tracking
        self.stats = {
            "lines_processed": 0,
            "messages_sent": 0,
            "errors": 0,
            "start_time": None,
            "last_activity": None,
            "files_watched": 0
        }
    
    @abstractmethod
    async def setup(self) -> bool:
        """Initialize producer-specific resources"""
        pass
    
    @abstractmethod
    async def teardown(self):
        """Cleanup producer-specific resources"""
        pass
    
    @abstractmethod
    async def parse_line(self, raw_line: bytes) -> Optional[Dict[str, Any]]:
        """Parse a raw log line and extract metadata"""
        pass
    
    @abstractmethod
    def extract_coin_from_line(self, raw_line: bytes) -> Optional[str]:
        """Fast extraction of coin symbol from raw line"""
        pass
    
    @abstractmethod
    def get_log_file_pattern(self) -> str:
        """Get glob pattern for log files"""
        pass
    
    async def start(self) -> bool:
        """Start the producer"""
        try:
            logger.info("Starting producer", name=self.name)
            
            # Setup producer-specific resources
            if not await self.setup():
                logger.error("Failed to setup producer", name=self.name)
                return False
            
            # Initialize file monitoring
            if not await self._initialize_file_monitoring():
                logger.error("Failed to initialize file monitoring", name=self.name)
                return False
            
            self.running = True
            self.stats["start_time"] = time.time()
            
            # Start main processing loop
            asyncio.create_task(self._processing_loop())
            
            logger.info("Producer started successfully", name=self.name)
            return True
            
        except Exception as e:
            logger.error("Failed to start producer", name=self.name, error=str(e))
            return False
    
    async def stop(self):
        """Stop the producer"""
        logger.info("Stopping producer", name=self.name)
        
        self.running = False
        
        # Stop file watching
        if self.file_watcher:
            self.file_watcher.stop()
        
        if self.directory_watcher:
            self.directory_watcher.stop()
        
        # Close log readers
        await self.log_reader_pool.close_all()
        
        # Producer-specific cleanup
        await self.teardown()
        
        logger.info("Producer stopped", name=self.name)
    
    async def _initialize_file_monitoring(self) -> bool:
        """Initialize file monitoring based on configuration"""
        log_path = self.config.get("log_path")
        if not log_path:
            logger.error("No log_path configured", name=self.name)
            return False
        
        base_path = Path(log_path)
        
        if not base_path.exists():
            logger.warning("Log path does not exist", path=str(base_path))
            # Continue anyway - path might be created later
        
        # Setup directory watcher for new files
        pattern = self.get_log_file_pattern()
        self.directory_watcher = DirectoryWatcher(str(base_path), pattern)
        
        # Start directory watching
        asyncio.create_task(
            self.directory_watcher.start_watching_directory(self._on_file_discovered)
        )
        
        logger.info(
            "Initialized file monitoring",
            name=self.name,
            path=str(base_path),
            pattern=pattern
        )
        return True
    
    async def _on_file_discovered(self, file_path: str):
        """Handle new file discovery"""
        try:
            # Create unique key for this file
            file_key = f"{self.name}:{file_path}"
            
            # Add log reader for this file
            success = await self.log_reader_pool.add_reader(file_key, file_path)
            if success:
                self.stats["files_watched"] += 1
                logger.info("Added log file to monitoring", name=self.name, file=file_path)
            else:
                logger.warning("Failed to add log file", name=self.name, file=file_path)
                
        except Exception as e:
            logger.error("Error handling file discovery", name=self.name, file=file_path, error=str(e))
    
    async def _processing_loop(self):
        """Main processing loop"""
        logger.info("Starting processing loop", name=self.name)
        
        while self.running:
            try:
                # Process all active log readers
                total_processed = 0
                
                for reader_key, reader in list(self.log_reader_pool.readers.items()):
                    processed = await self._process_reader(reader_key, reader)
                    total_processed += processed
                
                # Update activity timestamp if we processed anything
                if total_processed > 0:
                    self.stats["last_activity"] = time.time()
                    self.stats["lines_processed"] += total_processed
                
                # Brief sleep to prevent CPU spinning
                await asyncio.sleep(self.poll_interval_ms / 1000.0)
                
            except Exception as e:
                logger.error("Error in processing loop", name=self.name, error=str(e))
                self.stats["errors"] += 1
                await asyncio.sleep(1.0)  # Longer sleep on error
    
    async def _process_reader(self, reader_key: str, reader: MemoryMappedLogReader) -> int:
        """Process a single log reader"""
        try:
            # Check for log rotation
            if reader.detect_rotation():
                logger.info("Log rotation detected", name=self.name, reader=reader_key)
                await self._handle_log_rotation(reader_key, reader)
                return 0
            
            # Check for new data
            if not reader.has_new_data():
                return 0
            
            # Refresh memory map if file grew
            if not await reader.refresh_mmap():
                logger.warning("Failed to refresh memory map", name=self.name, reader=reader_key)
                return 0
            
            # Read new lines in batches
            lines = await reader.read_lines_batch(self.batch_size)
            if not lines:
                return 0
            
            # Process and publish lines
            await self._process_and_publish_lines(lines)
            
            return len(lines)
            
        except Exception as e:
            logger.error("Error processing reader", name=self.name, reader=reader_key, error=str(e))
            self.stats["errors"] += 1
            return 0
    
    async def _handle_log_rotation(self, reader_key: str, reader: MemoryMappedLogReader):
        """Handle log file rotation"""
        try:
            # Close old reader
            await reader.close()
            
            # Try to reopen with the same path
            success = await reader.open()
            if not success:
                # Remove reader if we can't reopen
                await self.log_reader_pool.remove_reader(reader_key)
                logger.warning("Removed reader after rotation failure", name=self.name, reader=reader_key)
            else:
                logger.info("Successfully reopened after rotation", name=self.name, reader=reader_key)
                
        except Exception as e:
            logger.error("Error handling log rotation", name=self.name, reader=reader_key, error=str(e))
    
    async def _process_and_publish_lines(self, lines: List[bytes]):
        """Process and publish lines to Redis"""
        # Group lines by coin for efficient publishing
        coin_lines: Dict[str, List[bytes]] = {}
        other_lines: List[bytes] = []
        
        for line in lines:
            coin = self.extract_coin_from_line(line)
            
            if coin and (not self.coins or coin in self.coins):
                if coin not in coin_lines:
                    coin_lines[coin] = []
                coin_lines[coin].append(line)
            else:
                other_lines.append(line)
        
        # Publish coin-specific streams
        for coin, coin_lines_list in coin_lines.items():
            await self._publish_coin_batch(coin, coin_lines_list)
        
        # Publish other lines to generic stream if configured
        if other_lines and self.config.get("publish_other_coins", False):
            await self._publish_coin_batch("OTHER", other_lines)
    
    async def _publish_coin_batch(self, coin: str, lines: List[bytes]):
        """Publish a batch of lines for a specific coin"""
        try:
            results = await self.redis_client.publish_trade_batch(
                coin=coin,
                raw_data_list=lines,
                stream_prefix=self.stream_prefix,
                maxlen=self.max_retention
            )
            
            # Count successful publishes
            successful = sum(1 for r in results if r is not None)
            self.stats["messages_sent"] += successful
            
            if successful < len(lines):
                logger.warning(
                    "Some messages failed to publish",
                    name=self.name,
                    coin=coin,
                    successful=successful,
                    total=len(lines)
                )
                
        except Exception as e:
            logger.error("Error publishing coin batch", name=self.name, coin=coin, error=str(e))
            self.stats["errors"] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        stats = self.stats.copy()
        stats.update({
            "name": self.name,
            "running": self.running,
            "coins": list(self.coins),
            "stream_prefix": self.stream_prefix,
            "config": self.config
        })
        
        # Add reader stats
        stats["readers"] = self.log_reader_pool.get_all_stats()
        
        return stats
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        try:
            # Check if we're running
            if not self.running:
                return {"status": "stopped", "producer": self.name}
            
            # Check Redis connectivity
            redis_health = await self.redis_client.health_check()
            if redis_health["status"] != "healthy":
                return {
                    "status": "unhealthy",
                    "producer": self.name,
                    "reason": "redis_unhealthy",
                    "redis_status": redis_health
                }
            
            # Check recent activity
            last_activity = self.stats.get("last_activity")
            if last_activity and (time.time() - last_activity) > 300:  # 5 minutes
                return {
                    "status": "stale",
                    "producer": self.name,
                    "last_activity": last_activity,
                    "stats": self.get_stats()
                }
            
            return {
                "status": "healthy",
                "producer": self.name,
                "stats": self.get_stats()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "producer": self.name,
                "error": str(e)
            }