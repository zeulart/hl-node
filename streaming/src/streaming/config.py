import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import structlog

logger = structlog.get_logger(__name__)


class Config:
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._find_config_file()
        self.data: Dict[str, Any] = {}
        self._load_config()
        self._apply_environment_overrides()
    
    def _find_config_file(self) -> str:
        """Find configuration file in standard locations"""
        possible_paths = [
            os.getenv("STREAMING_CONFIG_PATH"),
            "/app/config/streaming.yaml",
            "./config/streaming.yaml",
            "../config/streaming.yaml"
        ]
        
        for path in possible_paths:
            if path and Path(path).exists():
                return path
        
        # Default fallback
        return "/app/config/streaming.yaml"
    
    def _load_config(self):
        """Load configuration from YAML file"""
        try:
            config_file = Path(self.config_path)
            
            if not config_file.exists():
                logger.warning("Config file not found, using defaults", path=self.config_path)
                self._load_defaults()
                return
            
            with open(config_file, 'r') as f:
                self.data = yaml.safe_load(f) or {}
            
            logger.info("Loaded configuration", path=self.config_path)
            
        except Exception as e:
            logger.error("Failed to load config file", path=self.config_path, error=str(e))
            self._load_defaults()
    
    def _load_defaults(self):
        """Load default configuration"""
        self.data = {
            "redis": {
                "host": "localhost",
                "port": 6379,
                "pool_size": 10,
                "max_connections": 20,
                "decode_responses": False
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "producers": {
                "node_fills": {
                    "enabled": True,
                    "log_path": "/home/hluser/hl/data/node_fills/hourly",
                    "coins": ["BTC", "ETH", "HYPE"],
                    "stream_prefix": "node_fills",
                    "max_retention": 100000,
                    "batch_size": 1000,
                    "poll_interval_ms": 10
                }
            },
            "performance": {
                "mmap_buffer_size": 1048576,
                "read_buffer_size": 65536,
                "max_batch_size": 1000,
                "cpu_affinity": None
            }
        }
    
    def _apply_environment_overrides(self):
        """Apply environment variable overrides"""
        # Redis overrides
        if os.getenv("REDIS_HOST"):
            self.data["redis"]["host"] = os.getenv("REDIS_HOST")
        if os.getenv("REDIS_PORT"):
            self.data["redis"]["port"] = int(os.getenv("REDIS_PORT"))
        
        # Log path override
        if os.getenv("LOG_PATH"):
            for producer_config in self.data["producers"].values():
                if "log_path" in producer_config:
                    producer_config["log_path"] = os.getenv("LOG_PATH")
        
        # Logging level override
        if os.getenv("LOG_LEVEL"):
            self.data["logging"]["level"] = os.getenv("LOG_LEVEL")
        
        # Coins override
        if os.getenv("COINS"):
            coins = [coin.strip() for coin in os.getenv("COINS").split(",")]
            for producer_config in self.data["producers"].values():
                if "coins" in producer_config:
                    producer_config["coins"] = coins
        
        logger.info("Applied environment overrides")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with dot notation support"""
        keys = key.split(".")
        value = self.data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_redis_config(self) -> Dict[str, Any]:
        """Get Redis configuration"""
        return self.data.get("redis", {})
    
    def get_producer_config(self, producer_name: str) -> Dict[str, Any]:
        """Get producer-specific configuration"""
        return self.data.get("producers", {}).get(producer_name, {})
    
    def get_enabled_producers(self) -> Dict[str, Dict[str, Any]]:
        """Get all enabled producer configurations"""
        enabled = {}
        
        for name, config in self.data.get("producers", {}).items():
            if config.get("enabled", False):
                enabled[name] = config
        
        return enabled
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.data.get("logging", {})
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance configuration"""
        return self.data.get("performance", {})
    
    def validate(self) -> bool:
        """Validate configuration"""
        try:
            # Check required sections
            required_sections = ["redis", "producers"]
            for section in required_sections:
                if section not in self.data:
                    logger.error("Missing required config section", section=section)
                    return False
            
            # Validate Redis config
            redis_config = self.data["redis"]
            if not isinstance(redis_config.get("host"), str):
                logger.error("Redis host must be a string")
                return False
            
            if not isinstance(redis_config.get("port"), int):
                logger.error("Redis port must be an integer")
                return False
            
            # Validate producers
            producers = self.data["producers"]
            if not isinstance(producers, dict):
                logger.error("Producers must be a dictionary")
                return False
            
            for name, config in producers.items():
                if not self._validate_producer_config(name, config):
                    return False
            
            logger.info("Configuration validation passed")
            return True
            
        except Exception as e:
            logger.error("Configuration validation failed", error=str(e))
            return False
    
    def _validate_producer_config(self, name: str, config: Dict[str, Any]) -> bool:
        """Validate individual producer configuration"""
        try:
            if not isinstance(config, dict):
                logger.error("Producer config must be a dictionary", producer=name)
                return False
            
            # Check required fields for enabled producers
            if config.get("enabled", False):
                required_fields = ["log_path", "stream_prefix"]
                for field in required_fields:
                    if field not in config:
                        logger.error("Missing required producer field", producer=name, field=field)
                        return False
                
                # Validate coins if specified
                coins = config.get("coins")
                if coins is not None and not isinstance(coins, list):
                    logger.error("Producer coins must be a list", producer=name)
                    return False
                
                # Validate numeric fields
                numeric_fields = ["max_retention", "batch_size", "poll_interval_ms"]
                for field in numeric_fields:
                    value = config.get(field)
                    if value is not None and not isinstance(value, int):
                        logger.error("Producer field must be integer", producer=name, field=field)
                        return False
            
            return True
            
        except Exception as e:
            logger.error("Producer config validation failed", producer=name, error=str(e))
            return False
    
    def __str__(self) -> str:
        """String representation (without sensitive data)"""
        safe_data = self.data.copy()
        
        # Remove or mask sensitive information if any
        if "redis" in safe_data and "password" in safe_data["redis"]:
            safe_data["redis"]["password"] = "***"
        
        return f"Config(file={self.config_path}, data_keys={list(safe_data.keys())})"
    
    def dump(self) -> str:
        """Dump configuration as YAML string"""
        return yaml.dump(self.data, default_flow_style=False)


# Global config instance
_config_instance: Optional[Config] = None


def get_config(config_path: Optional[str] = None) -> Config:
    """Get global configuration instance"""
    global _config_instance
    
    if _config_instance is None or config_path:
        _config_instance = Config(config_path)
    
    return _config_instance


def reload_config(config_path: Optional[str] = None):
    """Reload configuration"""
    global _config_instance
    _config_instance = Config(config_path)
    logger.info("Configuration reloaded")