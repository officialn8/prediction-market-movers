"""
Kalshi WebSocket sync job - real-time market data.

This provides sub-millisecond latency vs 5-15 minute REST polling!
Requires API key authentication.

Reference: apps/collector/adapters/kalshi_wss.py
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from packages.core.settings import settings
from packages.core.storage.db import get_db_pool
from apps.collector.adapters.kalshi_wss import (
    KalshiWebSocket, 
    KalshiTrade, 
    KalshiOrderbookDelta,
    KalshiSubscribed,
    KalshiError,
)
from apps.collector.adapters.kalshi import KalshiAdapter
from apps.collector.jobs.kalshi_sync import sync_markets, get_sync_state

logger = logging.getLogger(__name__)


class KalshiWSSSync:
    """
    Real-time Kalshi WebSocket sync handler.
    
    Connects to Kalshi WSS, subscribes to markets, and processes
    trade/orderbook events to update snapshots in real-time.
    """
    
    def __init__(self):
        self.wss: Optional[KalshiWebSocket] = None
        self.ticker_to_token_id: Dict[str, str] = {}
        self.price_cache: Dict[str, float] = {}  # ticker -> last price
        self.volume_accumulator: Dict[str, float] = {}  # ticker -> accumulated volume
        self._last_flush = time.time()
        self._messages_received = 0
        self._trades_received = 0
    
    async def initialize(self) -> bool:
        """
        Initialize WSS connection with authentication.
        
        Returns True if successful, False otherwise.
        """
        api_key = settings.kalshi_api_key
        private_key_path = settings.kalshi_private_key_path
        private_key = settings.kalshi_private_key
        
        if not api_key:
            logger.warning("Kalshi API key not configured - WSS disabled")
            return False
        
        if not private_key_path and not private_key:
            logger.warning("Kalshi private key not configured - WSS disabled")
            return False
        
        try:
            self.wss = KalshiWebSocket(
                api_key=api_key,
                private_key_path=private_key_path,
                private_key_pem=private_key,
            )
            
            await self.wss.connect()
            logger.info("✅ Kalshi WSS connected!")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect Kalshi WSS: {e}")
            return False
    
    async def load_markets_and_subscribe(self) -> int:
        """
        Load market tickers from DB and subscribe to WSS.
        
        Returns number of markets subscribed to.
        """
        if not self.wss:
            return 0
        
        db = get_db_pool()
        
        # Get all Kalshi markets with their token IDs
        rows = db.execute("""
            SELECT mt.token_id, mt.source_token_id, m.source_id
            FROM markets m
            JOIN market_tokens mt ON mt.market_id = m.market_id
            WHERE m.source = 'kalshi' AND m.status = 'active'
        """, fetch=True) or []
        
        self.ticker_to_token_id = {}
        tickers = []
        
        for row in rows:
            ticker = row.get("source_id") or row.get("source_token_id")
            if ticker:
                self.ticker_to_token_id[ticker] = str(row["token_id"])
                tickers.append(ticker)
        
        if not tickers:
            logger.warning("No Kalshi markets found to subscribe to")
            return 0
        
        # Subscribe to trades (most important - contains price and size)
        await self.wss.subscribe_trades(tickers)
        
        # Optionally subscribe to orderbook for spread data
        # await self.wss.subscribe_orderbook(tickers)
        
        logger.info(f"Subscribed to {len(tickers)} Kalshi markets via WSS")
        return len(tickers)
    
    async def process_event(self, event) -> None:
        """Process a single WSS event."""
        self._messages_received += 1
        
        if isinstance(event, KalshiTrade):
            await self._handle_trade(event)
        elif isinstance(event, KalshiOrderbookDelta):
            await self._handle_orderbook(event)
        elif isinstance(event, KalshiSubscribed):
            logger.debug(f"Subscribed to {event.channel}: {len(event.tickers)} tickers")
        elif isinstance(event, KalshiError):
            logger.error(f"Kalshi WSS error: {event.code} - {event.message}")
    
    async def _handle_trade(self, trade: KalshiTrade) -> None:
        """Handle trade event - update price and accumulate volume."""
        self._trades_received += 1
        
        ticker = trade.ticker
        token_id = self.ticker_to_token_id.get(ticker)
        
        if not token_id:
            return
        
        # Update price cache
        price = trade.price_decimal
        self.price_cache[ticker] = price
        
        # Accumulate volume (notional value)
        volume = trade.notional_value
        if ticker not in self.volume_accumulator:
            self.volume_accumulator[ticker] = 0
        self.volume_accumulator[ticker] += volume
        
        logger.debug(f"Trade: {ticker} @ {trade.price}¢ x {trade.count}")
    
    async def _handle_orderbook(self, book: KalshiOrderbookDelta) -> None:
        """Handle orderbook delta - extract best bid/ask."""
        ticker = book.ticker
        token_id = self.ticker_to_token_id.get(ticker)
        
        if not token_id:
            return
        
        # Extract best bid/ask from the delta
        best_bid = None
        best_ask = None
        
        if book.yes_bids:
            best_bid = max(b.get("price", 0) for b in book.yes_bids) / 100
        if book.yes_asks:
            best_ask = min(a.get("price", 100) for a in book.yes_asks) / 100
        
        # Calculate mid price if we have both
        if best_bid and best_ask:
            mid_price = (best_bid + best_ask) / 2
            self.price_cache[ticker] = mid_price
    
    async def flush_snapshots(self) -> int:
        """Flush accumulated data to database."""
        if not self.price_cache:
            return 0
        
        db = get_db_pool()
        now = datetime.now(timezone.utc)
        inserted = 0
        
        for ticker, price in self.price_cache.items():
            token_id = self.ticker_to_token_id.get(ticker)
            if not token_id:
                continue
            
            try:
                volume = self.volume_accumulator.get(ticker)
                
                db.execute("""
                    INSERT INTO snapshots (ts, token_id, price, volume_24h)
                    VALUES (%s, %s, %s, %s)
                """, (now, token_id, price, volume))
                
                inserted += 1
            except Exception as e:
                logger.debug(f"Failed to insert snapshot for {ticker}: {e}")
        
        # Clear accumulators
        self.volume_accumulator.clear()
        self._last_flush = time.time()
        
        if inserted > 0:
            logger.debug(f"Flushed {inserted} Kalshi snapshots")
        
        return inserted
    
    async def close(self) -> None:
        """Close WSS connection."""
        if self.wss:
            await self.wss.close()
            self.wss = None


async def run_kalshi_wss_loop(shutdown) -> None:
    """
    Main Kalshi WSS loop.
    
    Args:
        shutdown: Shutdown signal object with .is_set property
    """
    logger.info("Starting Kalshi WSS sync loop...")
    
    # Initial REST sync to populate markets
    logger.info("Performing initial Kalshi REST sync...")
    adapter = KalshiAdapter()
    try:
        sync_markets(adapter)
    finally:
        adapter.close()
    
    handler = KalshiWSSSync()
    
    consecutive_failures = 0
    max_failures = 5
    
    while not shutdown.is_set:
        try:
            # Connect
            connected = await handler.initialize()
            if not connected:
                logger.warning("Kalshi WSS not available - falling back to REST polling")
                await _fallback_to_polling(shutdown)
                return
            
            # Subscribe to markets
            count = await handler.load_markets_and_subscribe()
            if count == 0:
                logger.warning("No markets to subscribe - waiting...")
                await asyncio.sleep(60)
                continue
            
            consecutive_failures = 0
            last_health_log = time.time()
            
            # Main message loop
            async for event in handler.wss.listen():
                if shutdown.is_set:
                    break
                
                await handler.process_event(event)
                
                # Periodic flush (every 2 seconds)
                if time.time() - handler._last_flush > 2.0:
                    await handler.flush_snapshots()
                
                # Health logging (every 60 seconds)
                if time.time() - last_health_log > 60:
                    logger.info(
                        f"Kalshi WSS Health: {handler._messages_received} msgs, "
                        f"{handler._trades_received} trades"
                    )
                    last_health_log = time.time()
                    handler._messages_received = 0
                    handler._trades_received = 0
                    
        except Exception as e:
            logger.error(f"Kalshi WSS error: {e}")
            consecutive_failures += 1
            
            if consecutive_failures >= max_failures:
                logger.error(f"Max Kalshi WSS failures ({max_failures}) - falling back to REST")
                await _fallback_to_polling(shutdown)
                return
            
            await asyncio.sleep(5 * consecutive_failures)  # Exponential backoff
            
        finally:
            await handler.close()
    
    logger.info("Kalshi WSS loop stopped")


async def _fallback_to_polling(shutdown, interval: int = 60) -> None:
    """Fallback to REST polling when WSS unavailable."""
    from apps.collector.jobs.kalshi_sync import sync_once
    
    logger.info(f"Kalshi REST polling mode (interval={interval}s)")
    
    while not shutdown.is_set:
        try:
            await sync_once()
        except Exception as e:
            logger.error(f"Kalshi REST sync error: {e}")
        
        try:
            await asyncio.wait_for(
                asyncio.create_task(asyncio.sleep(interval)),
                timeout=interval
            )
        except asyncio.TimeoutError:
            pass
        
        if shutdown.is_set:
            break


if __name__ == "__main__":
    import os
    
    logging.basicConfig(level=logging.INFO)
    
    class MockShutdown:
        is_set = False
    
    asyncio.run(run_kalshi_wss_loop(MockShutdown()))
