#!/usr/bin/env python3
"""
Hyperliquid Redis Stream Consumer (Production Base Template)
============================================================
Production-grade consumer with comprehensive logging, error handling,
and performance monitoring. Serves as the foundation for all consumer
implementations.

Features:
- Structured logging with configurable levels
- Robust error handling with exponential backoff
- Hourly stream rotation detection (UTC-based)
- Real-time performance metrics
- Graceful shutdown handling
- Comprehensive configuration management
"""

import logging
import os
import signal
import sys
import time
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, Optional
import redis


class ConsumerConfig:
    """Centralized configuration management with validation"""
    
    def __init__(self):
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_timeout = int(os.getenv("REDIS_TIMEOUT", "5"))
        self.redis_password = os.getenv("REDIS_PASSWORD")
        self.redis_password_file = os.getenv("REDIS_PASSWORD_FILE")
        self.redis_username = os.getenv("REDIS_USERNAME", "streaming_consumer")
        self.stream_pattern = os.getenv("STREAM_PATTERN", "node_fills:*")
        self.stats_interval = int(os.getenv("STATS_INTERVAL", "1"))
        self.read_count = int(os.getenv("READ_COUNT", "50"))
        self.read_timeout = int(os.getenv("READ_TIMEOUT", "1000"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "5"))
        self.retry_delay = float(os.getenv("RETRY_DELAY", "1.0"))
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        
        self._validate()
    
    def _validate(self):
        """Validate configuration values"""
        if self.redis_port < 1 or self.redis_port > 65535:
            raise ValueError(f"Invalid Redis port: {self.redis_port}")
        if self.stats_interval < 1:
            raise ValueError(f"Stats interval must be >= 1: {self.stats_interval}")
        if self.max_retries < 1:
            raise ValueError(f"Max retries must be >= 1: {self.max_retries}")
        if self.retry_delay < 0:
            raise ValueError(f"Retry delay must be >= 0: {self.retry_delay}")
    
    def get_redis_password(self) -> Optional[str]:
        """Get Redis password from file or environment variable"""
        if self.redis_password_file:
            try:
                with open(self.redis_password_file, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except (FileNotFoundError, PermissionError) as e:
                # Don't log password details for security
                pass
        return self.redis_password

    def summary(self) -> str:
        """Return configuration summary for logging"""
        auth_status = "with auth" if (self.redis_password or self.redis_password_file) else "no auth"
        return (
            f"Redis={self.redis_host}:{self.redis_port} ({auth_status}), "
            f"User={self.redis_username}, "
            f"Pattern={self.stream_pattern}, "
            f"Stats={self.stats_interval}s, "
            f"Retries={self.max_retries}, "
            f"LogLevel={self.log_level}"
        )


class PerformanceMetrics:
    """Track and report consumer performance metrics"""
    
    def __init__(self, stats_interval: int):
        self.stats_interval = stats_interval
        self.reset()
        self.start_time = time.time()
        self.total_trades = 0
        self.total_errors = 0
        self.connection_uptime = 0
    
    def reset(self):
        """Reset interval-based counters"""
        self.trade_count = 0
        self.total_latency = 0.0
        self.latency_samples = 0
        self.parse_errors = 0
        self.last_stats_time = time.time()
    
    def add_trade(self, latency_ms: Optional[float] = None):
        """Record a successful trade"""
        self.trade_count += 1
        self.total_trades += 1
        if latency_ms is not None and latency_ms > 0:
            self.total_latency += latency_ms / 1000  # Convert to seconds
            self.latency_samples += 1
    
    def add_error(self, error_type: str = "parse"):
        """Record an error"""
        if error_type == "parse":
            self.parse_errors += 1
        self.total_errors += 1
    
    def should_report(self) -> bool:
        """Check if it's time to report stats"""
        return (time.time() - self.last_stats_time) >= self.stats_interval
    
    def get_stats(self) -> Dict:
        """Calculate and return current statistics"""
        now = time.time()
        elapsed = now - self.last_stats_time
        
        trades_per_sec = self.trade_count / elapsed if elapsed > 0 else 0
        avg_latency_ms = 0.0
        if self.latency_samples > 0:
            avg_latency_ms = (self.total_latency / self.latency_samples) * 1000
        
        error_rate = (self.parse_errors / self.trade_count * 100) if self.trade_count > 0 else 0
        uptime = now - self.start_time
        
        return {
            "trades_per_sec": trades_per_sec,
            "avg_latency_ms": avg_latency_ms,
            "error_rate_pct": error_rate,
            "parse_errors": self.parse_errors,
            "uptime_seconds": uptime,
            "total_trades": self.total_trades,
            "total_errors": self.total_errors
        }


class RedisStreamConsumer:
    """Production Redis stream consumer with comprehensive error handling"""
    
    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.logger = self._setup_logger()
        self.metrics = PerformanceMetrics(config.stats_interval)
        self.redis_client: Optional[redis.Redis] = None
        self.stream_positions: Dict[str, str] = {}
        self.current_hour = self._get_current_hour_utc()
        self.shutdown_requested = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info(f"Consumer initialized: {config.summary()}")
    
    def _setup_logger(self) -> logging.Logger:
        """Configure structured logging"""
        logger = logging.getLogger("hyperliquid.consumer")
        logger.setLevel(getattr(logging, self.config.log_level, logging.INFO))
        
        # Only add handler if not already present
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.warning(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
    
    def _get_current_hour_utc(self) -> str:
        """Get current UTC hour as YYYYMMDDHH"""
        return datetime.now(timezone.utc).strftime('%Y%m%d%H')
    
    def _connect_redis(self) -> redis.Redis:
        """Connect to Redis with retry logic"""
        for attempt in range(1, self.config.max_retries + 1):
            try:
                password = self.config.get_redis_password()
                client = redis.Redis(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    password=password,
                    username=self.config.redis_username,
                    decode_responses=True,
                    socket_timeout=self.config.redis_timeout,
                    socket_connect_timeout=self.config.redis_timeout,
                    retry_on_timeout=True
                )
                client.ping()  # Test connection
                self.logger.info(f"Connected to Redis at {self.config.redis_host}:{self.config.redis_port}")
                return client
                
            except redis.ConnectionError as e:
                if attempt == self.config.max_retries:
                    self.logger.critical(f"Failed to connect to Redis after {attempt} attempts: {e}")
                    raise
                
                delay = self.config.retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                self.logger.warning(f"Redis connection failed (attempt {attempt}/{self.config.max_retries}): {e}")
                self.logger.info(f"Retrying in {delay:.1f}s...")
                time.sleep(delay)
        
        raise redis.ConnectionError("Max retries exceeded")
    
    def _get_expected_streams(self) -> Set[str]:
        """Infer current streams from UTC time instead of scanning Redis"""
        now_utc = datetime.now(timezone.utc)
        current_hour = now_utc.strftime('%Y%m%d:%H')
        
        expected_streams = {
            f"node_fills:{current_hour}"   # Only hourly streams
        }
        
        # Include previous hour during transition period (first 5 minutes of new hour)
        if now_utc.minute < 5:
            prev_hour = (now_utc - timedelta(hours=1)).strftime('%Y%m%d:%H')
            expected_streams.add(f"node_fills:{prev_hour}")
        
        self.logger.debug(f"Expected {len(expected_streams)} streams: {sorted(expected_streams)}")
        return expected_streams
    
    def _handle_stream_rotation(self):
        """Check for hourly rotation and update stream list"""
        new_hour = self._get_current_hour_utc()
        if new_hour != self.current_hour:
            self.logger.info(f"Hour rotation detected: {new_hour[:8]} {new_hour[8:]}:00 UTC")
            self.current_hour = new_hour
            
            # Get expected streams based on current UTC time
            expected_streams = self._get_expected_streams()
            
            # Add new expected streams
            new_streams = expected_streams - set(self.stream_positions.keys())
            for stream in new_streams:
                self.stream_positions[stream] = '$'  # Start from latest
                self.logger.info(f"Added expected stream: {stream}")
            
            # Remove old streams (those not in expected set)
            old_streams = set(self.stream_positions.keys()) - expected_streams
            for stream in old_streams:
                self.logger.info(f"Removed old stream: {stream}")
                del self.stream_positions[stream]
            
            if new_streams or old_streams:
                self.logger.info(f"Stream rotation complete: {len(self.stream_positions)} active streams")
    
    def _process_messages(self, messages):
        """Process Redis stream messages"""
        now_utc = datetime.now(timezone.utc).timestamp()
        
        for stream_name, msgs in messages:
            for msg_id, fields in msgs:
                raw_data = fields.get('data', '')
                
                try:
                    # Parse trade data
                    trade_data = json.loads(raw_data)
                    trade = trade_data[1] if len(trade_data) > 1 else {}
                    
                    # Calculate latency
                    trade_time = trade.get('time', 0)
                    latency_ms = None
                    if trade_time > 0:
                        latency_ms = (now_utc - (trade_time / 1000)) * 1000
                    
                    self.metrics.add_trade(latency_ms)
                    
                except (json.JSONDecodeError, IndexError, KeyError) as e:
                    self.metrics.add_error("parse")
                    self.logger.debug(f"Parse error in {stream_name}: {e}")
                
                # Update stream position
                self.stream_positions[stream_name] = msg_id
    
    def _report_stats(self):
        """Report performance statistics"""
        stats = self.metrics.get_stats()
        
        self.logger.info(
            f"Performance: {stats['trades_per_sec']:6.1f} trades/sec | "
            f"{stats['avg_latency_ms']:6.0f}ms avg latency | "
            f"{stats['error_rate_pct']:4.1f}% errors | "
            f"{len(self.stream_positions)} streams"
        )
        
        # Debug-level detailed stats
        self.logger.debug(
            f"Detailed stats: uptime={stats['uptime_seconds']:.0f}s, "
            f"total_trades={stats['total_trades']}, "
            f"total_errors={stats['total_errors']}, "
            f"parse_errors={stats['parse_errors']}"
        )
        
        self.metrics.reset()
    
    def run(self):
        """Main consumer loop"""
        try:
            # Connect to Redis
            self.redis_client = self._connect_redis()
            
            # Initial stream setup using expected streams
            expected_streams = self._get_expected_streams()
            for stream in expected_streams:
                self.stream_positions[stream] = '$'  # Start from latest
                self.logger.info(f"Monitoring expected stream: {stream}")
            
            if not self.stream_positions:
                self.logger.warning("No expected streams configured")
            
            self.logger.info("Consumer started successfully")
            
            # Main processing loop
            while not self.shutdown_requested:
                try:
                    # Handle hourly rotation
                    self._handle_stream_rotation()
                    
                    # Skip if no streams available
                    if not self.stream_positions:
                        self.logger.warning("No active streams, waiting...")
                        time.sleep(5)
                        continue
                    
                    # Read from streams with graceful handling of missing streams
                    try:
                        messages = self.redis_client.xread(
                            self.stream_positions,
                            block=self.config.read_timeout,
                            count=self.config.read_count
                        )
                        
                        if messages:
                            self._process_messages(messages)
                            
                    except redis.ResponseError as e:
                        # Handle missing streams gracefully
                        if "NOGROUP" in str(e) or "does not exist" in str(e).lower():
                            self.logger.debug(f"Expected stream not yet available: {e}")
                            # Remove non-existent streams temporarily
                            self._cleanup_missing_streams()
                        else:
                            raise
                    
                    # Report stats if needed
                    if self.metrics.should_report():
                        self._report_stats()
                
                except redis.ConnectionError as e:
                    self.logger.error(f"Redis connection lost: {e}")
                    self.logger.info("Attempting to reconnect...")
                    self.redis_client = self._connect_redis()
                
                except redis.TimeoutError:
                    # Normal timeout, continue
                    if self.metrics.should_report():
                        self._report_stats()
                
                except Exception as e:
                    self.logger.error(f"Unexpected error in main loop: {e}")
                    time.sleep(1)  # Brief pause before continuing
        
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.critical(f"Fatal error: {e}")
            return 1
        finally:
            self._cleanup()
        
        return 0
    
    def _cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up resources...")
        
        if self.redis_client:
            try:
                self.redis_client.close()
                self.logger.info("Redis connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing Redis connection: {e}")
        
        # Final stats report
        if self.metrics.trade_count > 0 or self.metrics.parse_errors > 0:
            self._report_stats()
        
        final_stats = self.metrics.get_stats()
        self.logger.info(
            f"Session summary: {final_stats['total_trades']} trades processed, "
            f"uptime {final_stats['uptime_seconds']:.0f}s, "
            f"{final_stats['total_errors']} total errors"
        )
        
        self.logger.info("Consumer shutdown complete")
    
    def _cleanup_missing_streams(self):
        """Remove streams that don't exist from position tracking"""
        streams_to_check = list(self.stream_positions.keys())
        for stream in streams_to_check:
            try:
                # Quick check if stream exists
                self.redis_client.xlen(stream)
            except redis.ResponseError:
                # Stream doesn't exist, remove from tracking
                self.logger.debug(f"Temporarily removing non-existent stream: {stream}")
                del self.stream_positions[stream]


def main():
    """Main entry point"""
    try:
        config = ConsumerConfig()
        consumer = RedisStreamConsumer(config)
        return consumer.run()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Fatal initialization error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())