from dataclasses import dataclass, asdict
import time

@dataclass
class WSSMetrics:
    messages_received: int = 0
    messages_per_second: float = 0.0
    last_message_age_seconds: float = 0.0
    reconnection_count: int = 0
    current_subscriptions: int = 0
    mode: str = "disconnected"  # "wss", "polling", "fallback"
    last_message_time: float = 0.0
    
    _STATUS_FILE = "/tmp/pm_wss_status.json"

    def to_dict(self) -> dict:
        """Export metrics for dashboard."""
        now = time.time()
        if self.last_message_time > 0:
            self.last_message_age_seconds = now - self.last_message_time
        return asdict(self)

    def record_message(self):
        """Record a received message."""
        self.messages_received += 1
        self.last_message_time = time.time()
        
    def save(self):
        """Save metrics to disk for dashboard visibility."""
        try:
            import json
            with open(self._STATUS_FILE, "w") as f:
                json.dump(self.to_dict(), f)
        except Exception:
            pass

    @classmethod
    def load(cls) -> 'WSSMetrics':
        """Load metrics from disk (for dashboard)."""
        metrics = cls()
        try:
            import json
            import os
            if os.path.exists(cls._STATUS_FILE):
                with open(cls._STATUS_FILE, "r") as f:
                    data = json.load(f)
                    # Safe load
                    for k, v in data.items():
                        if hasattr(metrics, k):
                            setattr(metrics, k, v)
        except Exception:
            pass
        return metrics

# Singleton metrics instance
_metrics = WSSMetrics()

def get_wss_metrics() -> WSSMetrics:
    return _metrics
