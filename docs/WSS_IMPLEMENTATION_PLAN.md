# Polymarket WebSocket (WSS) Implementation Plan

## Executive Summary

This plan details adding real-time WebSocket support to the prediction-market-movers project using Polymarket's CLOB WSS API. The implementation uses a **hybrid approach**: WebSocket for real-time price streaming, REST APIs for market metadata sync.

**Key Benefit**: Reduce price update latency from ~30 seconds to <100ms.

**Good News**: The MARKET channel (price updates) does **NOT require authentication** per the WSS docs. Only the USER channel needs API credentials.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         HYBRID ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   REST APIs (Existing)                 WSS (New)                        │
│   ├─ Market metadata sync              ├─ Real-time price updates       │
│   │  (Gamma API, every 15 min)         ├─ Order book changes            │
│   ├─ Initial token discovery           └─ Instant mover detection       │
│   └─ Fallback when WSS disconnected                                     │
│                                                                          │
│   ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐   │
│   │ PolymarketWSS   │───▶│ Price Handler    │───▶│ Database        │   │
│   │ (New Adapter)   │    │ (Batch Insert)   │    │ (Snapshots)     │   │
│   └─────────────────┘    └──────────────────┘    └─────────────────┘   │
│          │                                                ▲              │
│          │ Reconnect                                      │              │
│          ▼                                                │              │
│   ┌─────────────────┐                            ┌────────┴────────┐   │
│   │ REST Fallback   │───────────────────────────▶│ Existing Sync   │   │
│   │ (Existing)      │                            │ (Backup)        │   │
│   └─────────────────┘                            └─────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: WebSocket Client Foundation
**Files to create/modify:**

#### 1.1 New File: `apps/collector/adapters/polymarket_wss.py`

Core WebSocket client with:

```python
# Key components:
class PolymarketWebSocket:
    """
    Real-time WebSocket client for Polymarket CLOB API.

    Subscribes to MARKET channel for price updates.
    No authentication required for market data.
    """

    WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    async def connect(self, asset_ids: list[str]) -> None:
        """
        Connect and subscribe to MARKET channel.

        Subscribe message format:
        {
            "assets_ids": ["token_id_1", "token_id_2", ...],
            "type": "MARKET"
        }
        """

    async def subscribe_assets(self, asset_ids: list[str]) -> None:
        """
        Subscribe to additional assets after connection.

        Message format:
        {
            "assets_ids": ["new_token_id"],
            "operation": "subscribe"
        }
        """

    async def unsubscribe_assets(self, asset_ids: list[str]) -> None:
        """
        Unsubscribe from assets.

        Message format:
        {
            "assets_ids": ["token_id"],
            "operation": "unsubscribe"
        }
        """

    async def listen(self) -> AsyncIterator[WSSMessage]:
        """
        Yield parsed messages from WebSocket.
        Handles reconnection on disconnect.
        """
```

**Message Types to Handle:**
- `price_change` - Token price updated
- `book_update` - Order book change (bid/ask)
- `trade` - Trade executed (for volume tracking)
- `heartbeat` - Connection keepalive

#### 1.2 New File: `apps/collector/adapters/wss_messages.py`

Message parsing and type definitions:

```python
from dataclasses import dataclass
from typing import Literal, Optional
from datetime import datetime

@dataclass
class PriceUpdate:
    """Parsed price update from WSS."""
    token_id: str
    price: float
    timestamp: datetime

@dataclass
class BookUpdate:
    """Order book update with spread calculation."""
    token_id: str
    best_bid: float
    best_ask: float
    spread: float
    timestamp: datetime

@dataclass
class TradeEvent:
    """Trade execution event."""
    token_id: str
    price: float
    size: float
    side: Literal["BUY", "SELL"]
    timestamp: datetime

def parse_wss_message(raw: dict) -> Optional[PriceUpdate | BookUpdate | TradeEvent]:
    """Parse raw WSS message into typed event."""
```

---

### Phase 2: Integration with Collector

#### 2.1 Modify: `apps/collector/main.py`

Add new WSS loop alongside existing polling:

```python
async def run_polymarket_wss(shutdown: Shutdown) -> None:
    """
    Run WebSocket-based real-time sync.
    Falls back to polling on disconnect.
    """
    from apps.collector.jobs.polymarket_wss_sync import run_wss_loop

    logger.info("Starting Polymarket WSS real-time sync")
    await run_wss_loop(shutdown)


async def _amain() -> None:
    # ... existing code ...

    # Check if WSS mode is enabled
    use_wss = os.getenv("POLYMARKET_USE_WSS", "false").lower() == "true"

    if mode == "polymarket":
        if use_wss:
            logger.info("Mode: polymarket (WSS real-time)")
            await run_polymarket_wss(shutdown)
        else:
            logger.info("Mode: polymarket (polling)")
            await run_polymarket(shutdown, every_seconds=interval)
```

#### 2.2 New File: `apps/collector/jobs/polymarket_wss_sync.py`

WebSocket sync job orchestration:

```python
"""
WebSocket-based real-time sync for Polymarket.

Architecture:
1. Initial sync via REST to discover all tokens
2. Subscribe to MARKET channel for real-time updates
3. Batch price updates for efficient DB writes
4. Fallback to REST polling on disconnect
"""

class WSSyncState:
    """Extended state for WSS mode."""
    connected: bool = False
    last_message_time: Optional[float] = None
    subscribed_tokens: set[str] = field(default_factory=set)
    pending_updates: list[PriceUpdate] = field(default_factory=list)

async def run_wss_loop(shutdown: Shutdown) -> None:
    """
    Main WSS loop with:
    - Initial REST sync to get token list
    - WSS connection and subscription
    - Message handling with batched DB writes
    - Automatic reconnection
    - Fallback to polling on prolonged disconnect
    """

async def handle_price_update(update: PriceUpdate) -> None:
    """
    Process price update:
    1. Add to pending batch
    2. Trigger mover detection if significant change
    3. Flush batch if size/time threshold met
    """

async def flush_price_batch() -> None:
    """
    Batch insert pending updates to database.
    More efficient than individual inserts.
    """
```

---

### Phase 3: Settings and Configuration

#### 3.1 Modify: `packages/core/settings.py`

Add WSS-related settings:

```python
class Settings(BaseSettings):
    # ... existing fields ...

    # WebSocket Settings
    polymarket_use_wss: bool = Field(
        default=False,
        description="Enable WebSocket for real-time updates"
    )
    wss_reconnect_delay: float = Field(
        default=5.0,
        description="Seconds to wait before reconnecting"
    )
    wss_max_reconnect_attempts: int = Field(
        default=10,
        description="Max reconnection attempts before fallback"
    )
    wss_batch_size: int = Field(
        default=100,
        description="Batch size for DB writes"
    )
    wss_batch_interval: float = Field(
        default=1.0,
        description="Max seconds between batch flushes"
    )
    wss_fallback_to_polling: bool = Field(
        default=True,
        description="Fall back to REST polling on WSS failure"
    )
```

---

### Phase 4: Enhanced Mover Detection

#### 4.1 Modify: `apps/collector/jobs/movers_cache.py`

Enable real-time mover detection:

```python
# Add instant mover check triggered by WSS updates
async def check_instant_mover(
    token_id: UUID,
    old_price: float,
    new_price: float,
    threshold: float = 0.05  # 5% change
) -> Optional[MoverAlert]:
    """
    Check if price change qualifies as instant mover.
    Called directly from WSS handler for sub-second detection.
    """
    change_pct = abs(new_price - old_price) / old_price if old_price > 0 else 0

    if change_pct >= threshold:
        return MoverAlert(
            token_id=token_id,
            old_price=old_price,
            new_price=new_price,
            change_pct=change_pct,
            detected_at=datetime.utcnow()
        )
    return None
```

---

### Phase 5: Graceful Degradation

#### 5.1 Fallback Strategy

```python
class ConnectionManager:
    """
    Manages WSS connection with fallback to polling.
    """

    def __init__(self):
        self.mode: Literal["wss", "polling", "fallback"] = "wss"
        self.consecutive_failures: int = 0
        self.last_successful_message: Optional[float] = None

    async def on_disconnect(self) -> None:
        """
        Handle disconnect:
        1. Increment failure counter
        2. Attempt reconnect with exponential backoff
        3. Switch to polling after max attempts
        """
        self.consecutive_failures += 1

        if self.consecutive_failures >= settings.wss_max_reconnect_attempts:
            logger.warning("WSS max reconnects reached, falling back to polling")
            self.mode = "fallback"
            await self.start_fallback_polling()
        else:
            delay = min(2 ** self.consecutive_failures, 60)  # Max 60s
            await asyncio.sleep(delay)
            await self.reconnect()

    async def on_successful_message(self) -> None:
        """Reset failure counter on successful message."""
        self.consecutive_failures = 0
        self.last_successful_message = time.time()

        # Switch back to WSS if in fallback mode
        if self.mode == "fallback":
            logger.info("WSS recovered, switching back from polling")
            self.mode = "wss"
```

---

### Phase 6: Monitoring and Observability

#### 6.1 New File: `apps/collector/adapters/wss_metrics.py`

```python
"""
WSS connection metrics for monitoring.
"""

@dataclass
class WSSMetrics:
    messages_received: int = 0
    messages_per_second: float = 0.0
    last_message_age_seconds: float = 0.0
    reconnection_count: int = 0
    current_subscriptions: int = 0
    mode: str = "disconnected"  # "wss", "polling", "fallback"

    def to_dict(self) -> dict:
        """Export metrics for dashboard."""
        return asdict(self)

# Singleton metrics instance
_metrics = WSSMetrics()

def get_wss_metrics() -> WSSMetrics:
    return _metrics
```

#### 6.2 Dashboard Integration

Add WSS status to dashboard home page:

```python
# In apps/dashboard/pages/home.py

def render_wss_status():
    """Show WSS connection status and metrics."""
    metrics = get_wss_metrics()

    col1, col2, col3 = st.columns(3)

    with col1:
        status_color = "green" if metrics.mode == "wss" else "orange"
        st.metric("Connection Mode", metrics.mode.upper())

    with col2:
        st.metric("Messages/sec", f"{metrics.messages_per_second:.1f}")

    with col3:
        st.metric("Last Update", f"{metrics.last_message_age_seconds:.1f}s ago")
```

---

## File Summary

| File | Action | Description |
|------|--------|-------------|
| `apps/collector/adapters/polymarket_wss.py` | **Create** | Core WebSocket client |
| `apps/collector/adapters/wss_messages.py` | **Create** | Message parsing and types |
| `apps/collector/adapters/wss_metrics.py` | **Create** | Connection metrics |
| `apps/collector/jobs/polymarket_wss_sync.py` | **Create** | WSS sync job orchestration |
| `apps/collector/main.py` | **Modify** | Add WSS mode and loop |
| `packages/core/settings.py` | **Modify** | Add WSS configuration |
| `apps/collector/jobs/movers_cache.py` | **Modify** | Add instant mover detection |
| `apps/dashboard/pages/home.py` | **Modify** | Add WSS status display |
| `requirements.txt` | **Modify** | Add `websockets>=12.0` (optional, aiohttp already supports WSS) |

---

## Implementation Order

```
Step 1: Settings (Phase 3)
   └── Add configuration options first

Step 2: Message Types (Phase 1.2)
   └── Define data structures

Step 3: WebSocket Client (Phase 1.1)
   └── Core WSS functionality

Step 4: Sync Job (Phase 2.2)
   └── Orchestration logic

Step 5: Main Integration (Phase 2.1)
   └── Wire into collector

Step 6: Fallback Logic (Phase 5)
   └── Graceful degradation

Step 7: Mover Detection (Phase 4)
   └── Real-time alerts

Step 8: Monitoring (Phase 6)
   └── Dashboard integration
```

---

## Testing Strategy

### Unit Tests

```python
# tests/test_polymarket_wss.py

async def test_subscribe_message_format():
    """Verify subscription message matches API spec."""

async def test_parse_price_update():
    """Test parsing real WSS price message."""

async def test_reconnection_backoff():
    """Test exponential backoff on disconnect."""

async def test_batch_flush_on_size():
    """Test batch flushes when size threshold met."""

async def test_batch_flush_on_time():
    """Test batch flushes when time threshold met."""
```

### Integration Tests

```python
async def test_wss_to_database_flow():
    """Test full flow: WSS message → parsed → DB insert."""

async def test_fallback_to_polling():
    """Test automatic fallback when WSS fails."""

async def test_recovery_from_fallback():
    """Test switching back to WSS after recovery."""
```

---

## Environment Variables

```bash
# .env additions
POLYMARKET_USE_WSS=true
WSS_RECONNECT_DELAY=5.0
WSS_MAX_RECONNECT_ATTEMPTS=10
WSS_BATCH_SIZE=100
WSS_BATCH_INTERVAL=1.0
WSS_FALLBACK_TO_POLLING=true
```

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| WSS endpoint unavailable | Automatic fallback to REST polling |
| Message format changes | Defensive parsing with unknown field handling |
| Rate limiting on subscribe | Batch subscriptions, respect limits |
| Memory growth from buffering | Bounded queue with overflow handling |
| Connection drops | Exponential backoff reconnection |

---

## Success Metrics

| Metric | Current (Polling) | Target (WSS) |
|--------|------------------|--------------|
| Price update latency | ~30 seconds | <500ms |
| Mover detection time | 30-60 seconds | <1 second |
| API calls per minute | ~2 calls | 0 (streaming) |
| Bandwidth usage | Higher (full fetches) | Lower (deltas only) |

---

## Next Steps

1. Review and approve this plan
2. Begin implementation with Phase 1 (WebSocket client)
3. Test with a small subset of tokens before full rollout
4. Monitor metrics during gradual rollout
