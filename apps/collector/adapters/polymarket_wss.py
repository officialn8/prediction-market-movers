import asyncio
import json
import logging
import time
from typing import AsyncIterator, Optional, Union
import websockets
from websockets.exceptions import ConnectionClosed


from apps.collector.adapters.wss_messages import (
    parse_wss_message, 
    PriceUpdate, 
    BookUpdate, 
    TradeEvent,
    SpreadUpdate,
    MarketResolved,
    NewMarket,
)
from packages.core.wss import get_wss_metrics

logger = logging.getLogger(__name__)

# Type alias for all events we can yield
WSSEvent = Union[PriceUpdate, TradeEvent, SpreadUpdate, BookUpdate, MarketResolved, NewMarket]


class PolymarketWebSocket:
    """
    Real-time WebSocket client for Polymarket CLOB API.
    
    Subscribes to MARKET channel for price updates.
    No authentication required for market data.
    
    Features:
    - price_change: Best bid/ask updates (default)
    - last_trade_price: Trade events WITH SIZE for real-time volume
    - best_bid_ask: Spread data (requires custom_feature_enabled)
    - new_market: Market creation events (requires custom_feature_enabled)
    - market_resolved: Resolution events (requires custom_feature_enabled)
    """
    
    WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    def __init__(self, enable_custom_features: bool = True):
        """
        Initialize Polymarket WebSocket client.
        
        Args:
            enable_custom_features: Enable advanced messages (spread, new markets, 
                                    resolutions). Highly recommended for accuracy!
        """
        self._metrics = get_wss_metrics()
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._subscribed_assets: set[str] = set()
        self._keepalive_task: Optional[asyncio.Task] = None
        self._enable_custom_features = enable_custom_features
    
    async def connect(self, asset_ids: list[str]) -> None:
        """Connect and subscribe to MARKET channel."""
        logger.info(f"Connecting to Polymarket WSS: {self.WSS_URL}")
        
        # Cancel any existing keepalive task from previous connection
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None
        
        # Clear subscribed assets for fresh subscription
        self._subscribed_assets.clear()
        
        try:
            # Polymarket use application-level PING/PONG, so we disable protocol pings
            self._websocket = await websockets.connect(
                self.WSS_URL, 
                ping_interval=None,
                open_timeout=20 # Increase timeout
            )
            self._metrics.mode = "wss"
            logger.info("Connected to Polymarket WSS")
            
            # Start keepalive task and track reference
            self._keepalive_task = asyncio.create_task(self._keepalive())
            
            if asset_ids:
                await self.subscribe_assets(asset_ids)
                
        except Exception as e:
            logger.error(f"Failed to connect to WSS: {e}")
            self._metrics.mode = "disconnected"
            raise

    async def _keepalive(self):
        """Send application-level PING every 10s."""
        while True:
            # Check if websocket is locally tracked as open or just rely on try/except
            if not self._websocket:
                break
                
            try:
                await asyncio.sleep(10)
                # Just try to send. If closed, it will raise.
                if self._websocket:
                    await self._websocket.send("PING")
                    logger.debug("Sent WSS PING")
            except Exception:
                # Any error (AttributeError, ConnectionClosed, etc) -> stop keepalive
                break

    async def subscribe_assets(self, asset_ids: list[str]) -> None:
        """
        Subscribe to assets with optional custom features.
        
        When custom_feature_enabled=true, we get:
        - best_bid_ask: Spread updates
        - new_market: Market creation events
        - market_resolved: Resolution events
        
        These are critical for accuracy!
        """
        if not self._websocket:
            raise RuntimeError("WebSocket not connected")
            
        # Polymarket WSS subscription format:
        # - First subscription establishes channel: {"assets_ids": [...], "type": "market"}
        # - Additional assets use: {"assets_ids": [...], "operation": "subscribe"}
        # - custom_feature_enabled unlocks advanced message types!
        
        if not asset_ids:
            return
            
        # Split into small chunks and rate limit to avoid "INVALID OPERATION"
        chunk_size = 20
        total_chunks = (len(asset_ids) + chunk_size - 1) // chunk_size
        
        features_note = " (custom_features=ON)" if self._enable_custom_features else ""
        logger.info(f"Subscribing to {len(asset_ids)} assets in {total_chunks} chunks{features_note}")
        
        is_first_subscription = len(self._subscribed_assets) == 0
        
        for chunk_idx, i in enumerate(range(0, len(asset_ids), chunk_size)):
            chunk = [str(a) for a in asset_ids[i:i + chunk_size]]
            
            if is_first_subscription and chunk_idx == 0:
                # First subscription establishes the market channel
                message = {
                    "assets_ids": chunk,
                    "type": "market"
                }
                # Enable custom features for spread, new markets, and resolutions
                if self._enable_custom_features:
                    message["custom_feature_enabled"] = True
                logger.debug(f"Sending initial subscription (type=market, custom={self._enable_custom_features})")
            else:
                # Subsequent subscriptions use operation: subscribe
                message = {
                    "assets_ids": chunk,
                    "operation": "subscribe"
                }
                # Custom features should persist from initial subscription,
                # but include it anyway for safety
                if self._enable_custom_features:
                    message["custom_feature_enabled"] = True
                
            await self._websocket.send(json.dumps(message))
            
            # Rate limit: 200ms delay between chunks to be safe
            await asyncio.sleep(0.2)
            
        self._subscribed_assets.update(asset_ids)
        self._metrics.current_subscriptions = len(self._subscribed_assets)
        logger.info(f"Successfully subscribed to {len(asset_ids)} assets")

    async def listen(self) -> AsyncIterator[WSSEvent]:
        """
        Yield parsed messages from WebSocket.
        
        Now yields ALL event types (not just PriceUpdate):
        - PriceUpdate: Price changes
        - TradeEvent: Individual trades WITH SIZE (for volume!)
        - SpreadUpdate: Bid/ask spreads (requires custom_feature_enabled)
        - BookUpdate: Full order book snapshots
        - MarketResolved: Resolution events
        - NewMarket: New market creation
        """
        if not self._websocket:
            logger.error("listen() called but websocket is None - connection lost")
            raise ConnectionError("WebSocket not connected")

        # Track non-JSON messages to avoid log spam
        invalid_op_count = 0
        # Track event types seen for logging
        event_types_seen: set[str] = set()

        try:
            async for message in self._websocket:
                # Handle application-level PONG (text message)
                if isinstance(message, str) and "PONG" in message:
                     logger.debug(f"Received WSS PONG/Control: {message}")
                     continue
                
                # Handle known non-JSON responses from Polymarket
                if isinstance(message, str):
                    msg_upper = message.strip().upper()
                    if msg_upper == "INVALID OPERATION":
                        invalid_op_count += 1
                        # Only log periodically to avoid spam
                        if invalid_op_count == 1:
                            logger.warning("Received 'INVALID OPERATION' from WSS (may indicate subscription format issue)")
                        elif invalid_op_count % 100 == 0:
                            logger.warning(f"Received {invalid_op_count} 'INVALID OPERATION' responses from WSS")
                        continue
                    elif msg_upper in ("OK", "SUBSCRIBED", "UNSUBSCRIBED"):
                        # These are acknowledgment messages, safe to ignore
                        logger.debug(f"WSS acknowledgment: {message}")
                        continue
                    
                try:
                    data = json.loads(message)
                    
                    # Log summary of invalid ops if we had any and now getting valid data
                    if invalid_op_count > 0:
                        logger.info(f"WSS recovered - received {invalid_op_count} 'INVALID OPERATION' responses during subscription")
                        invalid_op_count = 0
                    
                    # Polymarket WSS returns a list of events
                    # OR a single event (depending on message type, but safe to treat all as potential lists)
                    events_to_process = []
                    if isinstance(data, list):
                        events_to_process = data
                    elif isinstance(data, dict):
                        events_to_process = [data]
                        
                    for event_raw in events_to_process:
                        self._metrics.record_message()
                        
                        # Track event types for debugging
                        event_type = event_raw.get("event_type", "unknown")
                        if event_type not in event_types_seen:
                            event_types_seen.add(event_type)
                            logger.info(f"New WSS event type seen: {event_type}")
                        
                        # parse_wss_message returns a LIST of typed updates
                        updates = parse_wss_message(event_raw)
                        for update in updates:
                            # Yield ALL event types for maximum data capture
                            yield update
                        
                except json.JSONDecodeError:
                    sample = message[:200] if isinstance(message, str) else str(message)[:200]
                    logger.warning(f"Received unexpected non-JSON from WSS: {sample!r}")
                except Exception as e:
                    logger.error(f"Error parsing WSS message: {e}")
                    
        except ConnectionClosed:
            logger.warning("WSS connection closed")
            self._metrics.mode = "disconnected"
            raise
        except Exception as e:
            logger.error(f"WSS loop error: {e}")
            self._metrics.mode = "disconnected"
            raise
    
    async def close(self):
        # Cancel keepalive task
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None
        
        if self._websocket:
            try:
                await self._websocket.close()
            except Exception:
                pass  # Ignore close errors
            self._websocket = None
            self._metrics.mode = "disconnected"
