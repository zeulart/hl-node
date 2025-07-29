import queue
from collections import deque, defaultdict
from typing import Dict, Any


class ActivityMonitor:
    """Monitors aggregated trade metrics to detect activity surges and shifts."""
    
    def __init__(self, buffer_size: int = 120):
        """Initialize with rolling buffer size (default: 120 = 1 hour at 30s intervals)."""
        self.buffer_size = buffer_size
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=buffer_size))
    
    def detect_tape_speed_shift(self, coin: str, current_speed: float) -> bool:
        """Detect if tape speed has significantly increased compared to full history."""
        history = self.metrics_history[coin]
        if len(history) < 10:  # Need at least 10 data points
            return False
        
        # Calculate average tape speed over full history
        recent_speeds = [m["metrics"]["tape_speed"] for m in list(history)]
        avg_speed = sum(recent_speeds) / len(recent_speeds)
        
        # Detect significant increase (2x or more)
        if current_speed > avg_speed * 2.0 and current_speed > 1.0:  # At least 1 trade/sec
            return True
        return False
    
    def process_metrics(self, metrics: Dict[str, Any]) -> None:
        """Process a single metrics record and detect activity patterns."""
        coin = metrics["coin"]
        current_speed = metrics["metrics"]["tape_speed"]
        
        # Store in rolling buffer
        self.metrics_history[coin].append(metrics)
        
        # Check for tape speed shift
        if self.detect_tape_speed_shift(coin, current_speed):
            print(f"🚀 ACTIVITY SURGE DETECTED: {coin} - Current speed: {current_speed:.2f} trades/sec")
            print(f"   Bucket: {metrics['bucket_start']}, Volume: {metrics['metrics']['volume']:.2f}")
            print(f"   Volume Delta: {metrics['metrics']['volume_delta']:.2f}, Cum Delta: {metrics['cum_volume_delta']:.2f}")
        # else:
            # Normal logging (less verbose)
            # print(f"{coin}: speed={current_speed:.2f} t/s, vol={metrics['metrics']['volume']:.1f}, delta={metrics['metrics']['volume_delta']:.1f}")
    
    def consume_from_queue(self, q: queue.Queue) -> None:
        """Continuously consume metrics from queue and process them."""
        while True:
            try:
                metrics = q.get(timeout=5)
                self.process_metrics(metrics)
            except queue.Empty:
                continue
    
    def get_history(self, coin: str) -> list:
        """Get full history for a specific coin."""
        return list(self.metrics_history[coin])
    
    def get_recent_activity_summary(self, coin: str, last_n: int = 10) -> Dict[str, float]:
        """Get summary statistics for recent activity of a coin."""
        history = self.metrics_history[coin]
        if len(history) < last_n:
            return {}
        
        recent = list(history)[-last_n:]
        speeds = [m["metrics"]["tape_speed"] for m in recent]
        volumes = [m["metrics"]["volume"] for m in recent]
        deltas = [m["metrics"]["volume_delta"] for m in recent]
        
        return {
            "avg_speed": sum(speeds) / len(speeds),
            "max_speed": max(speeds),
            "avg_volume": sum(volumes) / len(volumes),
            "total_volume_delta": sum(deltas),
        } 