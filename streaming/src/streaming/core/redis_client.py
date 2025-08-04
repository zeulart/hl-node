import asyncio
import time
from typing import Dict, List, Optional, Any
import redis.asyncio as redis
from redis.asyncio import Redis, ConnectionPool
import structlog

logger = structlog.get_logger(__name__)


class RedisStreamsClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        pool_size: int = 10,
        max_connections: int = 20,
        decode_responses: bool = False,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.max_connections = max_connections
        self.decode_responses = decode_responses
        self.kwargs = kwargs
        
        self.connection_pool: Optional[ConnectionPool] = None
        self.redis_client: Optional[Redis] = None
        self.is_connected = False
        
        # Performance metrics
        self.stats = {
            "messages_sent": 0,
            "bytes_sent": 0,
            "errors": 0,
            "last_error_time": None,
            "connection_time": None
        }
    
    async def connect(self) -> bool:
        try:
            # Create connection pool
            self.connection_pool = ConnectionPool.from_url(
                f"redis://{self.host}:{self.port}",
                max_connections=self.max_connections,
                decode_responses=self.decode_responses,
                socket_keepalive=True,
                socket_keepalive_options={},
                retry_on_timeout=True,
                **self.kwargs
            )
            
            # Create Redis client
            self.redis_client = Redis(connection_pool=self.connection_pool)
            
            # Test connection
            await self.redis_client.ping()
            
            self.is_connected = True
            self.stats["connection_time"] = time.time()
            
            logger.info(
                "Connected to Redis",
                host=self.host,
                port=self.port,
                pool_size=self.pool_size,
                max_connections=self.max_connections
            )
            return True
            
        except Exception as e:
            logger.error("Failed to connect to Redis", error=str(e))
            await self.disconnect()
            return False
    
    async def disconnect(self):
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
            
        if self.connection_pool:
            await self.connection_pool.disconnect()
            self.connection_pool = None
            
        self.is_connected = False
        logger.info("Disconnected from Redis")
    
    async def ensure_connected(self) -> bool:
        if not self.is_connected:
            return await self.connect()
        
        # Test connection health
        try:
            await self.redis_client.ping()
            return True
        except Exception as e:
            logger.warning("Redis health check failed, reconnecting", error=str(e))
            await self.disconnect()
            return await self.connect()
    
    async def xadd_single(
        self,
        stream_name: str,
        fields: Dict[str, Any],
        message_id: str = "*",
        maxlen: Optional[int] = None,
        approximate: bool = True
    ) -> Optional[str]:
        try:
            if not await self.ensure_connected():
                return None
            
            # Add message to stream
            result = await self.redis_client.xadd(
                stream_name,
                fields,
                id=message_id,
                maxlen=maxlen,
                approximate=approximate
            )
            
            # Update stats
            self.stats["messages_sent"] += 1
            message_size = sum(len(str(k)) + len(str(v)) for k, v in fields.items())
            self.stats["bytes_sent"] += message_size
            
            return result
            
        except Exception as e:
            self.stats["errors"] += 1
            self.stats["last_error_time"] = time.time()
            logger.error("Failed to add message to stream", stream=stream_name, error=str(e))
            return None
    
    async def xadd_batch(
        self,
        stream_name: str,
        messages: List[Dict[str, Any]],
        maxlen: Optional[int] = None,
        approximate: bool = True
    ) -> List[Optional[str]]:
        try:
            if not await self.ensure_connected():
                return [None] * len(messages)
            
            # Use pipeline for batch operations
            pipe = self.redis_client.pipeline()
            
            for fields in messages:
                pipe.xadd(
                    stream_name,
                    fields,
                    maxlen=maxlen,
                    approximate=approximate
                )
            
            # Execute pipeline
            results = await pipe.execute()
            
            # Update stats
            self.stats["messages_sent"] += len(messages)
            total_size = sum(
                sum(len(str(k)) + len(str(v)) for k, v in msg.items())
                for msg in messages
            )
            self.stats["bytes_sent"] += total_size
            
            return results
            
        except Exception as e:
            self.stats["errors"] += 1
            self.stats["last_error_time"] = time.time()
            logger.error("Failed to add batch to stream", stream=stream_name, error=str(e))
            return [None] * len(messages)
    
    async def publish_trade_data(
        self,
        coin: str,
        raw_data: bytes,
        stream_prefix: str = "node_fills",
        maxlen: int = 100000
    ) -> Optional[str]:
        stream_name = f"{stream_prefix}:{coin}"
        
        fields = {
            "data": raw_data,
            "timestamp": int(time.time_ns()),
            "size": len(raw_data)
        }
        
        return await self.xadd_single(
            stream_name,
            fields,
            maxlen=maxlen,
            approximate=True
        )
    
    async def publish_trade_batch(
        self,
        coin: str,
        raw_data_list: List[bytes],
        stream_prefix: str = "node_fills",
        maxlen: int = 100000
    ) -> List[Optional[str]]:
        if not raw_data_list:
            return []
            
        stream_name = f"{stream_prefix}:{coin}"
        
        messages = []
        current_time_ns = int(time.time_ns())
        
        for i, raw_data in enumerate(raw_data_list):
            fields = {
                "data": raw_data,
                "timestamp": current_time_ns + i,  # Ensure unique timestamps
                "size": len(raw_data)
            }
            messages.append(fields)
        
        return await self.xadd_batch(
            stream_name,
            messages,
            maxlen=maxlen,
            approximate=True
        )
    
    async def create_consumer_group(
        self,
        stream_name: str,
        group_name: str,
        start_id: str = "0",
        mkstream: bool = True
    ) -> bool:
        try:
            if not await self.ensure_connected():
                return False
                
            await self.redis_client.xgroup_create(
                stream_name,
                group_name,
                id=start_id,
                mkstream=mkstream
            )
            
            logger.info("Created consumer group", stream=stream_name, group=group_name)
            return True
            
        except Exception as e:
            if "BUSYGROUP" in str(e):
                # Group already exists
                logger.debug("Consumer group already exists", stream=stream_name, group=group_name)
                return True
            else:
                logger.error("Failed to create consumer group", stream=stream_name, group=group_name, error=str(e))
                return False
    
    async def get_stream_info(self, stream_name: str) -> Optional[Dict]:
        try:
            if not await self.ensure_connected():
                return None
                
            info = await self.redis_client.xinfo_stream(stream_name)
            return info
            
        except Exception as e:
            logger.error("Failed to get stream info", stream=stream_name, error=str(e))
            return None
    
    async def trim_stream(self, stream_name: str, maxlen: int, approximate: bool = True) -> bool:
        try:
            if not await self.ensure_connected():
                return False
                
            await self.redis_client.xtrim(stream_name, maxlen=maxlen, approximate=approximate)
            logger.debug("Trimmed stream", stream=stream_name, maxlen=maxlen)
            return True
            
        except Exception as e:
            logger.error("Failed to trim stream", stream=stream_name, error=str(e))
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        stats = self.stats.copy()
        stats.update({
            "is_connected": self.is_connected,
            "host": self.host,
            "port": self.port,
            "pool_size": self.pool_size,
            "max_connections": self.max_connections
        })
        return stats
    
    async def health_check(self) -> Dict[str, Any]:
        try:
            if not self.is_connected:
                return {"status": "disconnected", "error": "Not connected"}
            
            # Ping test
            start_time = time.time()
            await self.redis_client.ping()
            ping_time = (time.time() - start_time) * 1000  # ms
            
            # Memory info
            memory_info = await self.redis_client.info("memory")
            
            return {
                "status": "healthy",
                "ping_time_ms": round(ping_time, 2),
                "memory_used": memory_info.get("used_memory_human", "unknown"),
                "stats": self.get_stats()
            }
            
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}


class RedisStreamsPool:
    def __init__(self):
        self.clients: Dict[str, RedisStreamsClient] = {}
        
    async def add_client(self, name: str, client: RedisStreamsClient) -> bool:
        success = await client.connect()
        if success:
            self.clients[name] = client
            logger.info("Added Redis client to pool", name=name)
        return success
    
    async def remove_client(self, name: str):
        if name in self.clients:
            await self.clients[name].disconnect()
            del self.clients[name]
            logger.info("Removed Redis client from pool", name=name)
    
    def get_client(self, name: str) -> Optional[RedisStreamsClient]:
        return self.clients.get(name)
    
    async def disconnect_all(self):
        for client in self.clients.values():
            await client.disconnect()
        self.clients.clear()
        logger.info("Disconnected all Redis clients")
    
    async def health_check_all(self) -> Dict[str, Any]:
        results = {}
        for name, client in self.clients.items():
            results[name] = await client.health_check()
        return results