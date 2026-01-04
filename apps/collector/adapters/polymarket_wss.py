import asyncio
import json
import logging
import time
from typing import AsyncIterator, Optional
import websockets
from websockets.exceptions import ConnectionClosed


from apps.collector.adapters.wss_messages import parse_wss_message, PriceUpdate, BookUpdate, TradeEvent
from packages.core.wss import get_wss_metrics

logger = logging.getLogger(__name__)

class PolymarketWebSocket:
    """
    Real-time WebSocket client for Polymarket CLOB API.
    
    Subscribes to MARKET channel for price updates.
    No authentication required for market data.
    """
    
    WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    def __init__(self):
        self._metrics = get_wss_metrics()
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._subscribed_assets: set[str] = set()
    
    async def connect(self, asset_ids: list[str]) -> None:
        """Connect and subscribe to MARKET channel."""
        logger.info(f"Connecting to Polymarket WSS: {self.WSS_URL}")
        try:
            # Polymarket use application-level PING/PONG, so we disable protocol pings
            self._websocket = await websockets.connect(
                self.WSS_URL, 
                ping_interval=None,
                open_timeout=20 # Increase timeout
            )
            self._metrics.mode = "wss"
            logger.info("Connected to Polymarket WSS")
            
            # Start keepalive task
            asyncio.create_task(self._keepalive())
            
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
        """Subscribe to assets."""
        if not self._websocket:
            raise RuntimeError("WebSocket not connected")
            
        # Polymarket WSS expects batched subscriptions
        # Format for CLOB WSS:
        # {
        #     "assets_ids": ["..."],
        #     "type": "MARKET"
        # }
        
        if not asset_ids:
            return
            
        # Split into small chunks and rate limit to avoid "INVALID OPERATION"
        chunk_size = 20
        total_chunks = (len(asset_ids) + chunk_size - 1) // chunk_size
        
        logger.info(f"Subscribing to {len(asset_ids)} assets in {total_chunks} chunks (size={chunk_size})")
        
        for i in range(0, len(asset_ids), chunk_size):
            chunk = [str(a) for a in asset_ids[i:i + chunk_size]]
            message = {
                "assets_ids": chunk,
                "type": "market"
            }
            
            if i == 0:
                logger.debug(f"Sending first subscription chunk: {json.dumps(message)[:200]}...")
                
            await self._websocket.send(json.dumps(message))
            
            # Rate limit: 200ms delay between chunks to be safe
            await asyncio.sleep(0.2)
            
        self._subscribed_assets.update(asset_ids)
        self._metrics.current_subscriptions = len(self._subscribed_assets)
        logger.info(f"Successfully subscribed to {len(asset_ids)} assets")

    async def listen(self) -> AsyncIterator[PriceUpdate]:
        """Yield parsed messages from WebSocket."""
        if not self._websocket:
            return

        try:
            async for message in self._websocket:
                # Handle application-level PONG (text message)
                if isinstance(message, str) and "PONG" in message:
                     logger.debug(f"Received WSS PONG/Control: {message}")
                     continue
                    
                try:
                    data = json.loads(message)
                    
                    # Polymarket WSS returns a list of events
                    # OR a single event (depending on message type, but safe to treat all as potential lists)
                    events_to_process = []
                    if isinstance(data, list):
                        events_to_process = data
                    elif isinstance(data, dict):
                        events_to_process = [data]
                        
                    for event_raw in events_to_process:
                        self._metrics.record_message()
                        # parse_wss_message now returns a LIST of updates
                        updates = parse_wss_message(event_raw)
                        for update in updates:
                            if isinstance(update, PriceUpdate):
                                yield update
                        
                except json.JSONDecodeError:
                    sample = message[:200] if isinstance(message, str) else str(message)[:200]
                    logger.warning(f"Received invalid JSON from WSS: {sample!r}")
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
        if self._websocket:
            await self._websocket.close()
            self._metrics.mode = "disconnected"
