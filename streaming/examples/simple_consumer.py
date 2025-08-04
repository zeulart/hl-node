#!/usr/bin/env python3
"""
Simple Redis Streams consumer example

This demonstrates how to consume trade data from the streaming system.
"""

import asyncio
import json
import signal
import sys
from typing import Dict, Any
import aioredis
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class SimpleTradeConsumer:
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = None
        self.running = False
        self.shutdown_event = asyncio.Event()
        
        # Track consumer stats
        self.stats = {
            "messages_processed": 0,
            "parse_errors": 0,
            "start_time": None
        }
    
    async def connect(self) -> bool:
        """Connect to Redis"""
        try:
            self.redis_client = aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}",
                decode_responses=False  # Keep as bytes for consistency
            )
            
            # Test connection
            await self.redis_client.ping()
            logger.info("Connected to Redis", host=self.redis_host, port=self.redis_port)
            return True
            
        except Exception as e:
            logger.error("Failed to connect to Redis", error=str(e))
            return False
    
    async def consume_single_stream(self, stream_name: str, last_id: str = "$"):
        """Consume from a single stream"""
        logger.info("Starting single stream consumer", stream=stream_name)
        
        while self.running:
            try:
                # Read from stream (blocking with timeout)
                messages = await self.redis_client.xread(
                    {stream_name: last_id},
                    block=1000,  # 1 second timeout
                    count=100    # Read up to 100 messages
                )
                
                if not messages:
                    continue
                
                # Process messages
                for stream, msgs in messages:
                    for msg_id, fields in msgs:
                        await self._process_message(stream.decode(), msg_id.decode(), fields)
                        last_id = msg_id.decode()
                
            except Exception as e:
                logger.error("Error in single stream consumer", stream=stream_name, error=str(e))
                await asyncio.sleep(5)
    
    async def consume_with_consumer_group(
        self, 
        stream_name: str, 
        group_name: str = "demo_consumers",
        consumer_name: str = "consumer1"
    ):
        """Consume using Redis consumer groups"""
        logger.info("Starting consumer group consumer", 
                   stream=stream_name, group=group_name, consumer=consumer_name)
        
        # Create consumer group if it doesn't exist
        try:
            await self.redis_client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
            logger.info("Created consumer group", stream=stream_name, group=group_name)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.error("Failed to create consumer group", error=str(e))
                return
        
        while self.running:
            try:
                # Read from consumer group
                messages = await self.redis_client.xreadgroup(
                    group_name,
                    consumer_name,
                    {stream_name: ">"},  # Read unprocessed messages
                    block=1000,
                    count=50
                )
                
                if not messages:
                    continue
                
                # Process and acknowledge messages
                for stream, msgs in messages:
                    stream_str = stream.decode()
                    processed_ids = []
                    
                    for msg_id, fields in msgs:
                        msg_id_str = msg_id.decode()
                        
                        try:
                            await self._process_message(stream_str, msg_id_str, fields)
                            processed_ids.append(msg_id_str)
                        except Exception as e:
                            logger.error("Error processing message", 
                                       stream=stream_str, msg_id=msg_id_str, error=str(e))
                    
                    # Acknowledge processed messages
                    if processed_ids:
                        await self.redis_client.xack(stream_str, group_name, *processed_ids)
                        logger.debug("Acknowledged messages", 
                                   stream=stream_str, count=len(processed_ids))
                
            except Exception as e:
                logger.error("Error in consumer group consumer", error=str(e))
                await asyncio.sleep(5)
    
    async def _process_message(self, stream: str, msg_id: str, fields: Dict[bytes, bytes]):
        """Process a single message"""
        try:
            # Extract data
            raw_data = fields.get(b"data")
            timestamp = fields.get(b"timestamp")
            size = fields.get(b"size")
            
            if not raw_data:
                logger.warning("Message missing data field", stream=stream, msg_id=msg_id)
                return
            
            # Parse trade data
            trade = self._parse_trade_data(raw_data)
            if not trade:
                self.stats["parse_errors"] += 1
                return
            
            # Process the trade
            await self._handle_trade(stream, trade, msg_id, timestamp, size)
            
            self.stats["messages_processed"] += 1
            
        except Exception as e:
            logger.error("Error processing message", stream=stream, msg_id=msg_id, error=str(e))
            self.stats["parse_errors"] += 1
    
    def _parse_trade_data(self, raw_data: bytes) -> Dict[str, Any]:
        """Parse raw trade data JSON"""
        try:
            # Decode and parse JSON
            data = json.loads(raw_data.decode('utf-8'))
            
            # Validate Hyperliquid format: [wallet_address, trade_data]
            if not isinstance(data, list) or len(data) < 2:
                return None
            
            wallet_address = data[0]
            trade_data = data[1]
            
            if not isinstance(trade_data, dict):
                return None
            
            return {
                "wallet": wallet_address,
                "coin": trade_data.get("coin"),
                "time": trade_data.get("time"),
                "price": float(trade_data.get("px", 0)),
                "size": float(trade_data.get("sz", 0)),
                "side": trade_data.get("side"),  # 'A' = Ask/Sell, 'B' = Bid/Buy
                "direction": trade_data.get("dir"),
                "crossed": trade_data.get("crossed"),
                "order_id": trade_data.get("oid"),
                "trade_id": trade_data.get("tid")
            }
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.debug("Failed to parse trade data", error=str(e))
            return None
    
    async def _handle_trade(
        self, 
        stream: str, 
        trade: Dict[str, Any], 
        msg_id: str,
        timestamp: bytes,
        size: bytes
    ):
        """Handle a parsed trade"""
        # Extract coin from stream name (e.g., "node_fills:BTC" -> "BTC")
        coin = stream.split(":")[-1] if ":" in stream else "UNKNOWN"
        
        # Simple processing: log interesting trades
        if trade["size"] > 10.0:  # Large trades
            logger.info("Large trade detected",
                       coin=coin,
                       price=trade["price"],
                       size=trade["size"],
                       side=trade["side"],
                       msg_id=msg_id)
        
        # You can add your own processing logic here:
        # - Send to another system
        # - Store in database  
        # - Trigger alerts
        # - Calculate features
        # - Feed to ML models
        # etc.
    
    async def start_consumer(self, streams: list[str], use_consumer_group: bool = True):
        """Start consuming from specified streams"""
        import time
        
        self.stats["start_time"] = time.time()
        
        if not await self.connect():
            logger.error("Failed to connect to Redis")
            return False
        
        self.running = True
        
        # Setup signal handlers
        def signal_handler(signum, frame):
            logger.info("Received shutdown signal", signal=signum)
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start consumers for each stream
        tasks = []
        for stream in streams:
            if use_consumer_group:
                task = asyncio.create_task(
                    self.consume_with_consumer_group(stream)
                )
            else:
                task = asyncio.create_task(
                    self.consume_single_stream(stream)
                )
            tasks.append(task)
        
        # Start stats reporting
        stats_task = asyncio.create_task(self._stats_reporter())
        tasks.append(stats_task)
        
        logger.info("Consumer started", streams=streams, use_consumer_group=use_consumer_group)
        
        # Wait for shutdown
        await self.shutdown_event.wait()
        
        # Cancel all tasks
        self.running = False
        for task in tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("Consumer stopped", stats=self.stats)
        return True
    
    async def _stats_reporter(self):
        """Report stats periodically"""
        while self.running:
            try:
                await asyncio.sleep(30)  # Report every 30 seconds
                
                if self.stats["messages_processed"] > 0:
                    logger.info("Consumer stats", **self.stats)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in stats reporter", error=str(e))


async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple Redis Streams trade consumer")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--streams", nargs="+", 
                       default=["node_fills:BTC", "node_fills:ETH", "node_fills:HYPE"],
                       help="Streams to consume from")
    parser.add_argument("--no-consumer-group", action="store_true",
                       help="Use simple consumer instead of consumer group")
    
    args = parser.parse_args()
    
    consumer = SimpleTradeConsumer(args.redis_host, args.redis_port)
    
    try:
        success = await consumer.start_consumer(
            streams=args.streams,
            use_consumer_group=not args.no_consumer_group
        )
        
        if success:
            print("Consumer completed successfully")
            sys.exit(0)
        else:
            print("Consumer failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nReceived interrupt signal")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())