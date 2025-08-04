import time
import psutil
import asyncio
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class PerformanceMetrics:
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_sent_mb: float
    network_recv_mb: float
    open_files: int
    tcp_connections: int


class PerformanceMonitor:
    def __init__(self, interval: float = 60.0):
        self.interval = interval
        self.running = False
        self.metrics_history: list[PerformanceMetrics] = []
        self.max_history = 60  # Keep last 60 measurements (1 hour if interval=60s)
        
        # Baseline measurements for calculating deltas
        self._last_disk_io = None
        self._last_network_io = None
        self._process = psutil.Process()
    
    async def start_monitoring(self):
        """Start performance monitoring loop"""
        self.running = True
        logger.info("Starting performance monitoring", interval=self.interval)
        
        while self.running:
            try:
                metrics = await self._collect_metrics()
                if metrics:
                    self._store_metrics(metrics)
                    
                await asyncio.sleep(self.interval)
                
            except Exception as e:
                logger.error("Error in performance monitoring", error=str(e))
                await asyncio.sleep(self.interval)
    
    def stop_monitoring(self):
        """Stop performance monitoring"""
        self.running = False
        logger.info("Stopped performance monitoring")
    
    async def _collect_metrics(self) -> Optional[PerformanceMetrics]:
        """Collect current performance metrics"""
        try:
            # Get current timestamp
            timestamp = time.time()
            
            # CPU and memory metrics
            cpu_percent = self._process.cpu_percent()
            memory_info = self._process.memory_info()
            memory_percent = self._process.memory_percent()
            memory_used_mb = memory_info.rss / 1024 / 1024
            
            # Disk I/O metrics
            disk_io = self._process.io_counters()
            if self._last_disk_io:
                disk_read_bytes = disk_io.read_bytes - self._last_disk_io.read_bytes
                disk_write_bytes = disk_io.write_bytes - self._last_disk_io.write_bytes
            else:
                disk_read_bytes = disk_write_bytes = 0
            
            disk_io_read_mb = disk_read_bytes / 1024 / 1024
            disk_io_write_mb = disk_write_bytes / 1024 / 1024
            self._last_disk_io = disk_io
            
            # Network I/O metrics (system-wide)
            network_io = psutil.net_io_counters()
            if self._last_network_io:
                net_sent_bytes = network_io.bytes_sent - self._last_network_io.bytes_sent
                net_recv_bytes = network_io.bytes_recv - self._last_network_io.bytes_recv
            else:
                net_sent_bytes = net_recv_bytes = 0
            
            network_sent_mb = net_sent_bytes / 1024 / 1024
            network_recv_mb = net_recv_bytes / 1024 / 1024
            self._last_network_io = network_io
            
            # File and connection counts
            try:
                open_files = len(self._process.open_files())
            except (psutil.AccessDenied, OSError):
                open_files = 0
            
            try:
                tcp_connections = len([c for c in self._process.connections() if c.type == psutil.SOCK_STREAM])
            except (psutil.AccessDenied, OSError):
                tcp_connections = 0
            
            return PerformanceMetrics(
                timestamp=timestamp,
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_used_mb=memory_used_mb,
                disk_io_read_mb=disk_io_read_mb,
                disk_io_write_mb=disk_io_write_mb,
                network_sent_mb=network_sent_mb,
                network_recv_mb=network_recv_mb,
                open_files=open_files,
                tcp_connections=tcp_connections
            )
            
        except Exception as e:
            logger.error("Error collecting performance metrics", error=str(e))
            return None
    
    def _store_metrics(self, metrics: PerformanceMetrics):
        """Store metrics in history"""
        self.metrics_history.append(metrics)
        
        # Trim history if too long
        if len(self.metrics_history) > self.max_history:
            self.metrics_history = self.metrics_history[-self.max_history:]
        
        # Log significant changes
        self._check_performance_alerts(metrics)
    
    def _check_performance_alerts(self, metrics: PerformanceMetrics):
        """Check for performance alerts"""
        # High CPU usage alert
        if metrics.cpu_percent > 80:
            logger.warning("High CPU usage detected", cpu_percent=metrics.cpu_percent)
        
        # High memory usage alert
        if metrics.memory_percent > 80:
            logger.warning("High memory usage detected", memory_percent=metrics.memory_percent)
        
        # High disk I/O alert
        if metrics.disk_io_write_mb > 100:  # More than 100MB/min write
            logger.warning("High disk write activity", disk_write_mb=metrics.disk_io_write_mb)
        
        # Many open files alert
        if metrics.open_files > 1000:
            logger.warning("High number of open files", open_files=metrics.open_files)
    
    def get_current_metrics(self) -> Optional[Dict[str, Any]]:
        """Get current performance metrics"""
        if not self.metrics_history:
            return None
        
        latest = self.metrics_history[-1]
        return asdict(latest)
    
    def get_average_metrics(self, minutes: int = 5) -> Optional[Dict[str, Any]]:
        """Get average metrics over the last N minutes"""
        if not self.metrics_history:
            return None
        
        cutoff_time = time.time() - (minutes * 60)
        recent_metrics = [m for m in self.metrics_history if m.timestamp >= cutoff_time]
        
        if not recent_metrics:
            return None
        
        # Calculate averages
        avg_metrics = {
            "period_minutes": minutes,
            "sample_count": len(recent_metrics),
            "avg_cpu_percent": sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics),
            "avg_memory_percent": sum(m.memory_percent for m in recent_metrics) / len(recent_metrics),
            "avg_memory_used_mb": sum(m.memory_used_mb for m in recent_metrics) / len(recent_metrics),
            "total_disk_read_mb": sum(m.disk_io_read_mb for m in recent_metrics),
            "total_disk_write_mb": sum(m.disk_io_write_mb for m in recent_metrics),
            "total_network_sent_mb": sum(m.network_sent_mb for m in recent_metrics),
            "total_network_recv_mb": sum(m.network_recv_mb for m in recent_metrics),
            "avg_open_files": sum(m.open_files for m in recent_metrics) / len(recent_metrics),
            "avg_tcp_connections": sum(m.tcp_connections for m in recent_metrics) / len(recent_metrics)
        }
        
        return avg_metrics
    
    def get_peak_metrics(self, minutes: int = 60) -> Optional[Dict[str, Any]]:
        """Get peak metrics over the last N minutes"""
        if not self.metrics_history:
            return None
        
        cutoff_time = time.time() - (minutes * 60)
        recent_metrics = [m for m in self.metrics_history if m.timestamp >= cutoff_time]
        
        if not recent_metrics:
            return None
        
        peak_metrics = {
            "period_minutes": minutes,
            "sample_count": len(recent_metrics),
            "peak_cpu_percent": max(m.cpu_percent for m in recent_metrics),
            "peak_memory_percent": max(m.memory_percent for m in recent_metrics),
            "peak_memory_used_mb": max(m.memory_used_mb for m in recent_metrics),
            "peak_disk_read_mb": max(m.disk_io_read_mb for m in recent_metrics),
            "peak_disk_write_mb": max(m.disk_io_write_mb for m in recent_metrics),
            "peak_network_sent_mb": max(m.network_sent_mb for m in recent_metrics),
            "peak_network_recv_mb": max(m.network_recv_mb for m in recent_metrics),
            "peak_open_files": max(m.open_files for m in recent_metrics),
            "peak_tcp_connections": max(m.tcp_connections for m in recent_metrics)
        }
        
        return peak_metrics
    
    def export_metrics(self, filename: str):
        """Export metrics history to file"""
        try:
            import json
            
            with open(filename, 'w') as f:
                json.dump([asdict(m) for m in self.metrics_history], f, indent=2)
            
            logger.info("Exported metrics to file", filename=filename, samples=len(self.metrics_history))
            
        except Exception as e:
            logger.error("Failed to export metrics", filename=filename, error=str(e))