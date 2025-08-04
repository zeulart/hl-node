import os
import sys
import signal
import asyncio
import logging
from typing import Dict, List, Optional
import structlog

from .config import get_config
from .core.redis_client import RedisStreamsClient
from .producers.node_fills_producer import NodeFillsProducer


class StreamingService:
    def __init__(self):
        self.config = get_config()
        self.redis_client: Optional[RedisStreamsClient] = None
        self.producers: Dict[str, any] = {}
        self.running = False
        self.shutdown_event = asyncio.Event()
        
        # Setup logging
        self._setup_logging()
        
        self.logger = structlog.get_logger(__name__)
    
    def _setup_logging(self):
        """Setup structured logging"""
        logging_config = self.config.get_logging_config()
        log_level = logging_config.get("level", "INFO")
        
        # Configure standard logging
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=logging_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    async def initialize(self) -> bool:
        """Initialize the streaming service"""
        try:
            self.logger.info("Initializing streaming service")
            
            # Validate configuration
            if not self.config.validate():
                self.logger.error("Configuration validation failed")
                return False
            
            # Initialize Redis client
            if not await self._initialize_redis():
                self.logger.error("Failed to initialize Redis client")
                return False
            
            # Initialize producers
            if not await self._initialize_producers():
                self.logger.error("Failed to initialize producers")
                return False
            
            # Set CPU affinity if configured
            self._set_cpu_affinity()
            
            self.logger.info("Streaming service initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error("Failed to initialize streaming service", error=str(e))
            return False
    
    async def _initialize_redis(self) -> bool:
        """Initialize Redis client"""
        try:
            redis_config = self.config.get_redis_config()
            
            self.redis_client = RedisStreamsClient(
                host=redis_config.get("host", "localhost"),
                port=redis_config.get("port", 6379),
                pool_size=redis_config.get("pool_size", 10),
                max_connections=redis_config.get("max_connections", 20),
                decode_responses=redis_config.get("decode_responses", False)
            )
            
            # Test connection
            if not await self.redis_client.connect():
                self.logger.error("Failed to connect to Redis")
                return False
            
            # Perform health check
            health = await self.redis_client.health_check()
            if health["status"] != "healthy":
                self.logger.error("Redis health check failed", health=health)
                return False
            
            self.logger.info("Redis client initialized", health=health)
            return True
            
        except Exception as e:
            self.logger.error("Error initializing Redis client", error=str(e))
            return False
    
    async def _initialize_producers(self) -> bool:
        """Initialize all enabled producers"""
        try:
            enabled_producers = self.config.get_enabled_producers()
            
            if not enabled_producers:
                self.logger.warning("No producers enabled")
                return True
            
            self.logger.info("Initializing producers", producers=list(enabled_producers.keys()))
            
            # Initialize each producer
            for name, config in enabled_producers.items():
                producer = await self._create_producer(name, config)
                if producer:
                    self.producers[name] = producer
                    self.logger.info("Producer initialized", name=name)
                else:
                    self.logger.error("Failed to initialize producer", name=name)
                    return False
            
            self.logger.info("All producers initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error("Error initializing producers", error=str(e))
            return False
    
    async def _create_producer(self, name: str, config: Dict):
        """Create a producer instance"""
        try:
            if name == "node_fills":
                producer = NodeFillsProducer(self.redis_client, config)
            else:
                self.logger.error("Unknown producer type", name=name)
                return None
            
            # Start the producer
            if await producer.start():
                return producer
            else:
                self.logger.error("Failed to start producer", name=name)
                return None
                
        except Exception as e:
            self.logger.error("Error creating producer", name=name, error=str(e))
            return None
    
    def _set_cpu_affinity(self):
        """Set CPU affinity if configured"""
        try:
            cpu_affinity = self.config.get("performance.cpu_affinity")
            if cpu_affinity and hasattr(os, 'sched_setaffinity'):
                os.sched_setaffinity(0, cpu_affinity)
                self.logger.info("Set CPU affinity", cores=cpu_affinity)
        except Exception as e:
            self.logger.warning("Failed to set CPU affinity", error=str(e))
    
    async def start(self):
        """Start the streaming service"""
        try:
            self.logger.info("Starting streaming service")
            
            if not await self.initialize():
                self.logger.error("Service initialization failed")
                return False
            
            # Setup signal handlers
            self._setup_signal_handlers()
            
            self.running = True
            self.logger.info("Streaming service started successfully")
            
            # Start monitoring task
            asyncio.create_task(self._monitoring_loop())
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
            return True
            
        except Exception as e:
            self.logger.error("Error starting streaming service", error=str(e))
            return False
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Shutdown the streaming service"""
        self.logger.info("Shutting down streaming service")
        
        self.running = False
        
        # Stop all producers
        for name, producer in self.producers.items():
            try:
                await producer.stop()
                self.logger.info("Producer stopped", name=name)
            except Exception as e:
                self.logger.error("Error stopping producer", name=name, error=str(e))
        
        # Disconnect Redis
        if self.redis_client:
            try:
                await self.redis_client.disconnect()
                self.logger.info("Redis client disconnected")
            except Exception as e:
                self.logger.error("Error disconnecting Redis", error=str(e))
        
        self.logger.info("Streaming service shutdown complete")
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info("Received shutdown signal", signal=signum)
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Add SIGUSR1 for stats dump
        def stats_handler(signum, frame):
            asyncio.create_task(self._dump_stats())
        
        signal.signal(signal.SIGUSR1, stats_handler)
    
    async def _monitoring_loop(self):
        """Background monitoring loop"""
        self.logger.info("Starting monitoring loop")
        
        while self.running:
            try:
                # Perform health checks
                await self._perform_health_checks()
                
                # Log stats periodically
                await self._log_periodic_stats()
                
                # Sleep for monitoring interval
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error("Error in monitoring loop", error=str(e))
                await asyncio.sleep(30)  # Shorter sleep on error
    
    async def _perform_health_checks(self):
        """Perform health checks on all components"""
        try:
            # Check Redis health
            redis_health = await self.redis_client.health_check()
            if redis_health["status"] != "healthy":
                self.logger.warning("Redis health check failed", health=redis_health)
            
            # Check producer health
            for name, producer in self.producers.items():
                producer_health = await producer.health_check()
                if producer_health["status"] not in ["healthy", "stale"]:
                    self.logger.warning("Producer health check failed", name=name, health=producer_health)
                    
        except Exception as e:
            self.logger.error("Error performing health checks", error=str(e))
    
    async def _log_periodic_stats(self):
        """Log periodic statistics"""
        try:
            # Redis stats
            redis_stats = self.redis_client.get_stats()
            self.logger.info("Redis stats", **redis_stats)
            
            # Producer stats
            for name, producer in self.producers.items():
                producer_stats = producer.get_stats()
                self.logger.info("Producer stats", name=name, **producer_stats)
                
        except Exception as e:
            self.logger.error("Error logging stats", error=str(e))
    
    async def _dump_stats(self):
        """Dump detailed statistics"""
        self.logger.info("=== STREAMING SERVICE STATS ===")
        
        # Service stats
        self.logger.info("Service status", running=self.running, producers=list(self.producers.keys()))
        
        # Redis stats
        if self.redis_client:
            redis_health = await self.redis_client.health_check()
            self.logger.info("Redis health", **redis_health)
        
        # Producer stats
        for name, producer in self.producers.items():
            producer_health = await producer.health_check()
            self.logger.info("Producer health", name=name, **producer_health)
        
        self.logger.info("=== END STATS ===")


async def main():
    """Main entry point"""
    service = StreamingService()
    
    try:
        success = await service.start()
        if success:
            print("Streaming service completed successfully")
            sys.exit(0)
        else:
            print("Streaming service failed to start")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nReceived interrupt signal")
        sys.exit(0)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())