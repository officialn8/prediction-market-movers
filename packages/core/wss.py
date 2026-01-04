from dataclasses import dataclass, asdict
import time
from typing import Optional

@dataclass
class WSSMetrics:
    messages_received: int = 0
    messages_per_second: float = 0.0
    last_message_age_seconds: float = 0.0
    reconnection_count: int = 0
    current_subscriptions: int = 0
    mode: str = "disconnected"  # "wss", "polling", "disconnected"
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
        """Save metrics to disk AND database for dashboard visibility."""
        # Save to file (for local/same-container access)
        try:
            import json
            with open(self._STATUS_FILE, "w") as f:
                json.dump(self.to_dict(), f)
        except Exception:
            pass
        
        # Save to database (for cross-container access)
        try:
            from packages.core.storage.db import get_db_pool
            db = get_db_pool()
            
            # Use a simple key-value approach in a settings/status table
            # If table doesn't exist, we'll just skip DB save
            data = self.to_dict()
            import json
            json_data = json.dumps(data)
            
            # Try to upsert into a simple status table
            db.execute("""
                INSERT INTO system_status (key, value, updated_at)
                VALUES ('wss_metrics', %s, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
            """, (json_data,))
        except Exception:
            # Table might not exist - that's OK
            pass

    @classmethod
    def load(cls) -> 'WSSMetrics':
        """Load metrics from database first, then file fallback."""
        metrics = cls()
        
        # Try database first (works across containers)
        try:
            from packages.core.storage.db import get_db_pool
            db = get_db_pool()
            
            result = db.execute("""
                SELECT value, updated_at FROM system_status WHERE key = 'wss_metrics'
            """, fetch=True)
            
            if result and result[0]['value']:
                import json
                data = json.loads(result[0]['value'])
                for k, v in data.items():
                    if hasattr(metrics, k):
                        setattr(metrics, k, v)
                
                # Check if status is stale (no update in 60 seconds = disconnected)
                updated_at = result[0]['updated_at']
                if updated_at:
                    from datetime import datetime, timezone
                    now = datetime.now(timezone.utc)
                    if updated_at.tzinfo is None:
                        updated_at = updated_at.replace(tzinfo=timezone.utc)
                    age = (now - updated_at).total_seconds()
                    
                    if age > 60:
                        # Status is stale, mark as disconnected
                        metrics.mode = "disconnected"
                    
                return metrics
        except Exception:
            pass
        
        # Fallback to file-based (for local development)
        try:
            import json
            import os
            if os.path.exists(cls._STATUS_FILE):
                with open(cls._STATUS_FILE, "r") as f:
                    data = json.load(f)
                    for k, v in data.items():
                        if hasattr(metrics, k):
                            setattr(metrics, k, v)
        except Exception:
            pass
        
        return metrics
    
    @classmethod
    def load_with_activity_check(cls) -> 'WSSMetrics':
        """
        Load metrics and verify with actual database activity.
        More reliable than just checking saved status.
        """
        metrics = cls.load()
        
        # Double-check by looking at actual recent snapshot activity
        try:
            from packages.core.storage.db import get_db_pool
            db = get_db_pool()
            
            # Check for snapshots in last 30 seconds
            result = db.execute("""
                SELECT COUNT(*) as cnt, MAX(ts) as latest
                FROM snapshots 
                WHERE ts > NOW() - INTERVAL '30 seconds'
            """, fetch=True)
            
            if result and result[0]['cnt'] > 0:
                # Data is flowing - we're connected
                if metrics.mode == "disconnected":
                    metrics.mode = "polling"  # At minimum, data is coming in
                
                # Update last message time based on actual data
                latest = result[0]['latest']
                if latest:
                    from datetime import timezone
                    if latest.tzinfo is None:
                        latest = latest.replace(tzinfo=timezone.utc)
                    metrics.last_message_time = latest.timestamp()
            else:
                # No recent data - disconnected
                metrics.mode = "disconnected"
                
        except Exception:
            pass
        
        return metrics


# Singleton metrics instance
_metrics = WSSMetrics()

def get_wss_metrics() -> WSSMetrics:
    return _metrics
