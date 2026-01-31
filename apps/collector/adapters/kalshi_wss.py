"""
Kalshi WebSocket adapter for real-time market data.

This is the KEY accuracy improvement - sub-millisecond latency instead of
5-15 minute REST polling delay!

WebSocket URL: wss://trading-api.kalshi.com/trade-api/ws/v2
Requires API key authentication (unlike REST public endpoints).

Channels:
- orderbook_delta: Real-time order book changes
- trade: Individual trades with price and size

References:
- kalshi-rs: https://docs.rs/kalshi-rs/latest/kalshi_rs/
- kalshi-mdp: https://github.com/rothcharlie1/kalshi-mdp (sub-ms Redis sink)
"""

import asyncio
import json
import logging
import time
import hmac
import hashlib
import base64
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import AsyncIterator, Optional, Union
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


# ============================================================================
# Event Types
# ============================================================================

@dataclass
class KalshiOrderbookDelta:
    """
    Order book update from Kalshi WebSocket.
    
    Contains bid/ask changes with price and size at each level.
    """
    ticker: str
    market_ticker: str
    seq: int  # Sequence number for ordering
    yes_bids: list[dict]  # [{price, size}, ...]
    yes_asks: list[dict]
    no_bids: list[dict]
    no_asks: list[dict]
    timestamp: datetime


@dataclass  
class KalshiTrade:
    """
    Trade event from Kalshi WebSocket.
    
    Contains trade details including SIZE for volume calculation.
    """
    ticker: str
    trade_id: str
    price: int  # Cents (1-99)
    count: int  # Number of contracts
    taker_side: str  # "yes" or "no"
    timestamp: datetime
    
    @property
    def price_decimal(self) -> float:
        """Price as decimal (0-1)."""
        return self.price / 100
    
    @property
    def notional_value(self) -> float:
        """Trade notional value in dollars."""
        return self.count * self.price / 100


@dataclass
class KalshiSubscribed:
    """Subscription confirmation."""
    channel: str
    tickers: list[str]


@dataclass
class KalshiError:
    """Error from WebSocket."""
    code: int
    message: str


KalshiEvent = Union[KalshiOrderbookDelta, KalshiTrade, KalshiSubscribed, KalshiError]


# ============================================================================
# Authentication
# ============================================================================

def generate_kalshi_signature(
    api_key: str,
    private_key_pem: str,
    timestamp_ms: int,
    method: str = "GET",
    path: str = "/trade-api/ws/v2"
) -> str:
    """
    Generate Kalshi API signature for WebSocket authentication.
    
    Kalshi uses RSA-PSS with SHA256 (salt length 32) for signatures.
    Message format: timestamp + method + path
    
    Args:
        api_key: API key ID
        private_key_pem: RSA private key in PEM format
        timestamp_ms: Unix timestamp in milliseconds
        method: HTTP method (GET for WebSocket)
        path: API path
        
    Returns:
        Base64-encoded signature
    """
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend
        
        # Load private key
        private_key = serialization.load_pem_private_key(
            private_key_pem.encode() if isinstance(private_key_pem, str) else private_key_pem,
            password=None,
            backend=default_backend()
        )
        
        # Create message to sign: timestamp + method + path
        # Kalshi requires this exact format
        message = f"{timestamp_ms}{method}{path}"
        
        # Sign with RSA-PSS (NOT PKCS1v15!)
        # Kalshi requires PSS padding with SHA256 and salt length 32
        signature = private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=32  # Kalshi requires salt length 32
            ),
            hashes.SHA256()
        )
        
        return base64.b64encode(signature).decode()
        
    except ImportError:
        logger.error("cryptography package required for Kalshi WebSocket auth. Install with: pip install cryptography")
        raise
    except Exception as e:
        logger.error(f"Failed to generate Kalshi signature: {e}")
        raise


# ============================================================================
# WebSocket Client
# ============================================================================

class KalshiWebSocket:
    """
    Real-time WebSocket client for Kalshi Trading API.
    
    Provides sub-millisecond market data updates vs 5-15 minute REST polling!
    
    Requires API key authentication. Get keys at:
    https://kalshi.com/account/api-keys
    
    Usage:
        wss = KalshiWebSocket(api_key="...", private_key_pem="...")
        await wss.connect()
        await wss.subscribe_orderbook(["KXHIGHNY-25JAN31-T42"])
        await wss.subscribe_trades(["KXHIGHNY-25JAN31-T42"])
        
        async for event in wss.listen():
            if isinstance(event, KalshiTrade):
                print(f"Trade: {event.ticker} @ {event.price}c x {event.count}")
            elif isinstance(event, KalshiOrderbookDelta):
                print(f"Book: {event.ticker} seq={event.seq}")
    """
    
    # Correct URL from kalshi-rs: api.elections.kalshi.com (NOT trading-api.kalshi.com)
    WSS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        private_key_pem: Optional[str] = None,
        private_key_path: Optional[str] = None,
    ):
        """
        Initialize Kalshi WebSocket client.
        
        Args:
            api_key: Kalshi API key ID
            private_key_pem: RSA private key as PEM string
            private_key_path: Path to RSA private key file (alternative to private_key_pem)
        """
        self.api_key = api_key
        self._private_key_pem = private_key_pem
        
        # Load private key from file if path provided
        if private_key_path and not private_key_pem:
            with open(private_key_path, 'r') as f:
                self._private_key_pem = f.read()
        
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._subscribed_orderbook: set[str] = set()
        self._subscribed_trades: set[str] = set()
        self._keepalive_task: Optional[asyncio.Task] = None
        self._message_id = 0
        self._connected = False
    
    def _next_message_id(self) -> int:
        """Generate unique message ID."""
        self._message_id += 1
        return self._message_id
    
    async def connect(self) -> None:
        """
        Connect to Kalshi WebSocket with authentication.
        
        Raises:
            ValueError: If API credentials not provided
            ConnectionError: If connection fails
        """
        if not self.api_key or not self._private_key_pem:
            raise ValueError(
                "Kalshi WebSocket requires API key and private key. "
                "Get API keys at https://kalshi.com/account/api-keys"
            )
        
        logger.info(f"Connecting to Kalshi WSS: {self.WSS_URL}")
        
        # Cancel any existing keepalive
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
        
        try:
            # Generate authentication headers
            timestamp_ms = int(time.time() * 1000)
            signature = generate_kalshi_signature(
                self.api_key,
                self._private_key_pem,
                timestamp_ms
            )
            
            # Connect with auth headers
            # websockets 15.x uses additional_headers instead of extra_headers
            headers = [
                ("KALSHI-ACCESS-KEY", self.api_key),
                ("KALSHI-ACCESS-SIGNATURE", signature),
                ("KALSHI-ACCESS-TIMESTAMP", str(timestamp_ms)),
            ]
            
            self._websocket = await websockets.connect(
                self.WSS_URL,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            )
            
            self._connected = True
            logger.info("Connected to Kalshi WSS (authenticated)")
            
            # Start keepalive
            self._keepalive_task = asyncio.create_task(self._keepalive())
            
        except Exception as e:
            logger.error(f"Failed to connect to Kalshi WSS: {e}")
            self._connected = False
            raise ConnectionError(f"Kalshi WSS connection failed: {e}")
    
    async def _keepalive(self) -> None:
        """Send periodic ping to keep connection alive."""
        while self._connected and self._websocket:
            try:
                await asyncio.sleep(15)
                # WebSocket library handles ping/pong, but we can send a command
                # to verify the connection is responsive
                if self._websocket:
                    # Kalshi doesn't have explicit ping, connection just stays alive
                    pass
            except Exception:
                break
    
    async def subscribe_orderbook(self, tickers: list[str]) -> None:
        """
        Subscribe to order book updates for given market tickers.
        
        Args:
            tickers: List of market tickers (e.g., ["KXHIGHNY-25JAN31-T42"])
        """
        if not self._websocket:
            raise RuntimeError("WebSocket not connected")
        
        if not tickers:
            return
        
        new_tickers = [t for t in tickers if t not in self._subscribed_orderbook]
        if not new_tickers:
            return
        
        message = {
            "id": self._next_message_id(),
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": new_tickers
            }
        }
        
        await self._websocket.send(json.dumps(message))
        self._subscribed_orderbook.update(new_tickers)
        logger.info(f"Subscribed to Kalshi orderbook for {len(new_tickers)} markets")
    
    async def subscribe_trades(self, tickers: list[str]) -> None:
        """
        Subscribe to trade events for given market tickers.
        
        Trade events contain SIZE which is essential for volume calculation!
        
        Args:
            tickers: List of market tickers (e.g., ["KXHIGHNY-25JAN31-T42"])
        """
        if not self._websocket:
            raise RuntimeError("WebSocket not connected")
        
        if not tickers:
            return
        
        new_tickers = [t for t in tickers if t not in self._subscribed_trades]
        if not new_tickers:
            return
        
        message = {
            "id": self._next_message_id(),
            "cmd": "subscribe",
            "params": {
                "channels": ["trade"],
                "market_tickers": new_tickers
            }
        }
        
        await self._websocket.send(json.dumps(message))
        self._subscribed_trades.update(new_tickers)
        logger.info(f"Subscribed to Kalshi trades for {len(new_tickers)} markets")
    
    async def subscribe_all(self, tickers: list[str]) -> None:
        """Subscribe to both orderbook and trades for given tickers."""
        await self.subscribe_orderbook(tickers)
        await self.subscribe_trades(tickers)
    
    async def unsubscribe(self, channel: str, tickers: list[str]) -> None:
        """Unsubscribe from a channel for given tickers."""
        if not self._websocket:
            return
        
        message = {
            "id": self._next_message_id(),
            "cmd": "unsubscribe",
            "params": {
                "channels": [channel],
                "market_tickers": tickers
            }
        }
        
        await self._websocket.send(json.dumps(message))
        
        if channel == "orderbook_delta":
            self._subscribed_orderbook -= set(tickers)
        elif channel == "trade":
            self._subscribed_trades -= set(tickers)
    
    def _parse_message(self, data: dict) -> Optional[KalshiEvent]:
        """Parse raw WebSocket message into typed event."""
        msg_type = data.get("type")
        
        # Subscription confirmation
        if msg_type == "subscribed":
            return KalshiSubscribed(
                channel=data.get("msg", {}).get("channel", ""),
                tickers=data.get("msg", {}).get("market_tickers", [])
            )
        
        # Error
        if msg_type == "error":
            return KalshiError(
                code=data.get("msg", {}).get("code", 0),
                message=data.get("msg", {}).get("message", "Unknown error")
            )
        
        # Order book delta
        if msg_type == "orderbook_delta":
            try:
                msg = data.get("msg", {})
                return KalshiOrderbookDelta(
                    ticker=msg.get("market_ticker", ""),
                    market_ticker=msg.get("market_ticker", ""),
                    seq=msg.get("seq", 0),
                    yes_bids=msg.get("yes", {}).get("bids", []),
                    yes_asks=msg.get("yes", {}).get("asks", []),
                    no_bids=msg.get("no", {}).get("bids", []),
                    no_asks=msg.get("no", {}).get("asks", []),
                    timestamp=datetime.now(timezone.utc)
                )
            except Exception as e:
                logger.warning(f"Failed to parse orderbook_delta: {e}")
                return None
        
        # Trade
        if msg_type == "trade":
            try:
                msg = data.get("msg", {})
                return KalshiTrade(
                    ticker=msg.get("market_ticker", ""),
                    trade_id=msg.get("trade_id", ""),
                    price=msg.get("yes_price", 0),  # Price in cents
                    count=msg.get("count", 0),
                    taker_side=msg.get("taker_side", ""),
                    timestamp=datetime.fromisoformat(
                        msg.get("created_time", "").replace("Z", "+00:00")
                    ) if msg.get("created_time") else datetime.now(timezone.utc)
                )
            except Exception as e:
                logger.warning(f"Failed to parse trade: {e}")
                return None
        
        # Unknown message type
        if msg_type:
            logger.debug(f"Unknown Kalshi message type: {msg_type}")
        
        return None
    
    async def listen(self) -> AsyncIterator[KalshiEvent]:
        """
        Yield parsed events from WebSocket.
        
        Yields:
            KalshiOrderbookDelta: Order book changes
            KalshiTrade: Trade events (with SIZE for volume!)
            KalshiSubscribed: Subscription confirmations
            KalshiError: Error messages
        """
        if not self._websocket:
            raise ConnectionError("WebSocket not connected")
        
        try:
            async for message in self._websocket:
                try:
                    data = json.loads(message)
                    event = self._parse_message(data)
                    if event:
                        yield event
                        
                except json.JSONDecodeError:
                    logger.warning(f"Non-JSON message from Kalshi: {message[:100]}")
                except Exception as e:
                    logger.error(f"Error parsing Kalshi message: {e}")
                    
        except ConnectionClosed:
            logger.warning("Kalshi WSS connection closed")
            self._connected = False
            raise
        except Exception as e:
            logger.error(f"Kalshi WSS error: {e}")
            self._connected = False
            raise
    
    async def close(self) -> None:
        """Close WebSocket connection."""
        self._connected = False
        
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
        
        if self._websocket:
            try:
                await self._websocket.close()
            except Exception:
                pass
            self._websocket = None
        
        self._subscribed_orderbook.clear()
        self._subscribed_trades.clear()
        logger.info("Kalshi WSS connection closed")


# ============================================================================
# Convenience Functions
# ============================================================================

async def test_kalshi_wss(api_key: str, private_key_path: str) -> None:
    """
    Test Kalshi WebSocket connection and subscriptions.
    
    Usage:
        asyncio.run(test_kalshi_wss("your-api-key", "kalshi_private.pem"))
    """
    wss = KalshiWebSocket(api_key=api_key, private_key_path=private_key_path)
    
    try:
        await wss.connect()
        print("✅ Connected to Kalshi WSS")
        
        # Subscribe to a popular market (will need to find active tickers)
        # For testing, we'll just listen for any messages
        print("Listening for messages (Ctrl+C to stop)...")
        
        async for event in wss.listen():
            print(f"Event: {event}")
            
    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        await wss.close()


if __name__ == "__main__":
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) < 3:
        print("Usage: python kalshi_wss.py <api_key> <private_key_path>")
        print("Get API keys at: https://kalshi.com/account/api-keys")
        sys.exit(1)
    
    api_key = sys.argv[1]
    key_path = sys.argv[2]
    
    asyncio.run(test_kalshi_wss(api_key, key_path))
