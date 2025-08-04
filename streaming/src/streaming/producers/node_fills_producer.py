import re
import json
from typing import Dict, Optional, Any
import structlog

from .base_producer import BaseProducer

logger = structlog.get_logger(__name__)


class NodeFillsProducer(BaseProducer):
    def __init__(self, redis_client, config: Dict[str, Any]):
        super().__init__("node_fills", redis_client, config)
        
        # Pre-compile regex for fast coin extraction
        # Hyperliquid format: [wallet_address, {"coin": "BTC", ...}]
        self.coin_pattern = re.compile(rb'"coin"\s*:\s*"([^"]+)"')
        
        # Track parsing stats
        self.parse_stats = {
            "valid_lines": 0,
            "invalid_lines": 0,
            "extraction_failures": 0
        }
    
    async def setup(self) -> bool:
        """Initialize node_fills specific resources"""
        try:
            logger.info("Setting up node_fills producer", coins=list(self.coins))
            
            # Create consumer groups for each coin if needed
            for coin in self.coins:
                stream_name = f"{self.stream_prefix}:{coin}"
                await self.redis_client.create_consumer_group(
                    stream_name,
                    "processors",
                    start_id="0",
                    mkstream=True
                )
            
            logger.info("Node fills producer setup complete")
            return True
            
        except Exception as e:
            logger.error("Failed to setup node_fills producer", error=str(e))
            return False
    
    async def teardown(self):
        """Cleanup node_fills specific resources"""
        logger.info("Tearing down node_fills producer")
        # No specific cleanup needed for node_fills
    
    async def parse_line(self, raw_line: bytes) -> Optional[Dict[str, Any]]:
        """Parse a node_fills log line"""
        try:
            # Decode and parse JSON
            line_str = raw_line.decode('utf-8', errors='replace')
            data = json.loads(line_str)
            
            # Validate format: [wallet_address, trade_data]
            if not isinstance(data, list) or len(data) < 2:
                self.parse_stats["invalid_lines"] += 1
                return None
            
            wallet_address = data[0]
            trade_data = data[1]
            
            if not isinstance(trade_data, dict):
                self.parse_stats["invalid_lines"] += 1
                return None
            
            # Extract key fields
            parsed = {
                "wallet": wallet_address,
                "coin": trade_data.get("coin"),
                "time": trade_data.get("time"),
                "px": trade_data.get("px"),
                "sz": trade_data.get("sz"),
                "side": trade_data.get("side"),
                "dir": trade_data.get("dir"),
                "crossed": trade_data.get("crossed"),
                "oid": trade_data.get("oid"),
                "tid": trade_data.get("tid")
            }
            
            self.parse_stats["valid_lines"] += 1
            return parsed
            
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
            self.parse_stats["invalid_lines"] += 1
            logger.debug("Failed to parse line", error=str(e), line_preview=raw_line[:100])
            return None
        except Exception as e:
            self.parse_stats["invalid_lines"] += 1
            logger.error("Unexpected error parsing line", error=str(e))
            return None
    
    def extract_coin_from_line(self, raw_line: bytes) -> Optional[str]:
        """Fast extraction of coin symbol from raw line using regex"""
        try:
            match = self.coin_pattern.search(raw_line)
            if match:
                coin = match.group(1).decode('utf-8')
                return coin
            else:
                self.parse_stats["extraction_failures"] += 1
                return None
                
        except Exception as e:
            self.parse_stats["extraction_failures"] += 1
            logger.debug("Failed to extract coin", error=str(e))
            return None
    
    def get_log_file_pattern(self) -> str:
        """Get glob pattern for node_fills log files"""
        # Hyperliquid node_fills pattern: YYYYMMDD/HH (hourly files)
        return "*"  # Match all files in subdirectories
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics including parsing stats"""
        stats = super().get_stats()
        stats["parse_stats"] = self.parse_stats.copy()
        
        # Calculate parsing efficiency
        total_lines = self.parse_stats["valid_lines"] + self.parse_stats["invalid_lines"]
        if total_lines > 0:
            stats["parse_efficiency"] = {
                "success_rate": round(self.parse_stats["valid_lines"] / total_lines * 100, 2),
                "extraction_success_rate": round(
                    (total_lines - self.parse_stats["extraction_failures"]) / total_lines * 100, 2
                ) if total_lines > 0 else 0
            }
        
        return stats