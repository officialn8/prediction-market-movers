"""
Kalshi WebSocket sync job - real-time market data.

This provides sub-millisecond latency vs 5-15 minute REST polling.
Requires API key authentication.

Reference: apps/collector/adapters/kalshi_wss.py
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Optional, Set

from apps.collector.adapters.kalshi import KalshiAdapter
from apps.collector.adapters.kalshi_wss import (
    KalshiError,
    KalshiOrderbookDelta,
    KalshiSubscribed,
    KalshiTrade,
    KalshiWebSocket,
)
from apps.collector.jobs.snapshot_gate import should_write_kalshi_snapshot
from apps.collector.jobs.kalshi_sync import sync_markets
from apps.collector.jobs.movers_cache import broadcast_mover_alert, check_instant_mover
from packages.core.settings import settings
from packages.core.storage.db import get_db_pool
from packages.core.storage.queries import MarketQueries

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
        self.price_cache: Dict[str, float] = {}  # ticker -> latest observed price
        self.volume_accumulator: Dict[str, float] = {}  # ticker -> accumulated batch volume
        self.dirty_tickers: Set[str] = set()  # only these are considered for writes

        # Last persisted state used by write-gate/dedupe
        self.last_written_price: Dict[str, float] = {}
        self.last_written_ts: Dict[str, float] = {}

        self._last_flush = time.time()
        self._last_status_update = time.time()
        self._last_storage_metrics = 0.0
        self._messages_received = 0
        self._trades_received = 0
        self._latest_latency_ms = 0.0
        self._instant_mover_last_ts: Dict[str, float] = {}
        self._alert_tasks: set[asyncio.Task] = set()

        # Per-minute snapshot write counters
        self._counter_window_start = time.time()
        self._inserted_since_window = 0
        self._skipped_since_window = 0

    def _track_alert_task(self, task: asyncio.Task) -> None:
        self._alert_tasks.add(task)

        def _on_done(done_task: asyncio.Task) -> None:
            self._alert_tasks.discard(done_task)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc:
                logger.warning(f"Instant mover alert task failed: {exc}")

        task.add_done_callback(_on_done)

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
            logger.info("✅ Kalshi WSS connected")
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

        rows = db.execute(
            """
            SELECT mt.token_id,
                   mt.source_token_id,
                   m.source_id,
                   (
                        SELECT s.price
                        FROM snapshots s
                        WHERE s.token_id = mt.token_id
                        ORDER BY s.ts DESC
                        LIMIT 1
                   ) AS price
            FROM markets m
            JOIN market_tokens mt ON mt.market_id = m.market_id
            WHERE m.source = 'kalshi' AND m.status = 'active'
            """,
            fetch=True,
        ) or []

        self.ticker_to_token_id = {}
        self.dirty_tickers.clear()
        tickers: list[str] = []

        for row in rows:
            ticker = row.get("source_id") or row.get("source_token_id")
            if not ticker:
                continue

            self.ticker_to_token_id[ticker] = str(row["token_id"])
            tickers.append(ticker)

            if row.get("price") is not None:
                price = float(row["price"])
                self.price_cache[ticker] = price
                self.last_written_price[ticker] = price

        if not tickers:
            logger.warning("No Kalshi markets found to subscribe to")
            return 0

        await self.wss.subscribe_trades(tickers)
        logger.info(f"Subscribed to {len(tickers)} Kalshi markets via WSS")
        return len(tickers)

    async def process_event(self, event) -> None:
        """Process a single WSS event."""
        self._messages_received += 1

        if hasattr(event, "timestamp") and event.timestamp:
            try:
                now_ts = time.time()
                latency = (now_ts - float(event.timestamp)) * 1000
                self._latest_latency_ms = max(0.0, latency)
            except (ValueError, TypeError):
                pass

        if isinstance(event, KalshiTrade):
            await self._handle_trade(event)
        elif isinstance(event, KalshiOrderbookDelta):
            await self._handle_orderbook(event)
        elif isinstance(event, KalshiSubscribed):
            logger.debug(f"Subscribed to {event.channel}: {len(event.tickers)} tickers")
        elif isinstance(event, KalshiError):
            logger.error(f"Kalshi WSS error: {event.code} - {event.message}")

        if time.time() - self._last_status_update > 5.0:
            await self._update_system_status()

    def _fetch_storage_sizes(self) -> dict:
        db = get_db_pool()
        rows = db.execute(
            """
            SELECT
                c.relname AS table_name,
                pg_total_relation_size(c.oid) AS bytes,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS size_pretty
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
              AND c.relname = ANY(%s)
            ORDER BY bytes DESC
            """,
            (
                [
                    "snapshots",
                    "ohlc_1m",
                    "ohlc_1h",
                    "movers_cache",
                    "alerts",
                    "volume_spikes",
                    "trade_volumes",
                    "volume_hourly",
                ],
            ),
            fetch=True,
        ) or []

        db_rows = db.execute(
            """
            SELECT
                pg_database_size(current_database()) AS db_size_bytes,
                pg_size_pretty(pg_database_size(current_database())) AS db_size_pretty
            """,
            fetch=True,
        ) or [{}]

        return {
            "tables": {
                r["table_name"]: {
                    "bytes": int(r["bytes"]),
                    "pretty": r["size_pretty"],
                }
                for r in rows
            },
            "db_size_bytes": int(db_rows[0].get("db_size_bytes") or 0),
            "db_size_pretty": db_rows[0].get("db_size_pretty") or "unknown",
        }

    async def _update_system_status(self):
        """Update the system_status table with current metrics."""
        try:
            now_ts = time.time()
            elapsed = max(now_ts - self._counter_window_start, 1e-6)
            per_min_scale = 60.0 / elapsed

            status_data = {
                "connected": True,
                "latency_ms": round(self._latest_latency_ms, 2),
                "messages_received": self._messages_received,
                "trades_received": self._trades_received,
                "snapshot_inserted_window": self._inserted_since_window,
                "snapshot_skipped_window": self._skipped_since_window,
                "snapshot_inserted_per_min": round(self._inserted_since_window * per_min_scale, 2),
                "snapshot_skipped_per_min": round(self._skipped_since_window * per_min_scale, 2),
                "last_updated": now_ts,
            }

            if now_ts - self._last_storage_metrics >= settings.storage_metrics_interval_seconds:
                storage_metrics = await asyncio.to_thread(self._fetch_storage_sizes)
                status_data["storage"] = storage_metrics
                self._last_storage_metrics = now_ts

            db = get_db_pool()
            db.execute(
                """
                INSERT INTO system_status (key, value, updated_at)
                VALUES ('kalshi_wss', %s, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                (json.dumps(status_data),),
            )

            self._last_status_update = now_ts
            if elapsed >= 60.0:
                self._counter_window_start = now_ts
                self._inserted_since_window = 0
                self._skipped_since_window = 0
        except Exception as e:
            logger.warning(f"Failed to update system status: {e}")

    async def _handle_trade(self, trade: KalshiTrade) -> None:
        """Handle trade event - update price and accumulate volume."""
        self._trades_received += 1

        ticker = trade.ticker
        token_id = self.ticker_to_token_id.get(ticker)
        if not token_id:
            return

        price = trade.price_decimal
        volume = trade.notional_value
        old_price = self.price_cache.get(ticker)

        if old_price is not None:
            now_ts = time.time()
            last_alert_ts = self._instant_mover_last_ts.get(token_id)
            if (
                last_alert_ts is None
                or (now_ts - last_alert_ts) >= settings.instant_mover_debounce_seconds
            ):
                mover = await check_instant_mover(
                    token_id,
                    old_price,
                    price,
                    volume=volume,
                )
                if mover:
                    logger.info(f"Instant Mover Detected: {ticker} {old_price:.4f} -> {price:.4f}")
                    self._instant_mover_last_ts[token_id] = now_ts
                    task = asyncio.create_task(broadcast_mover_alert(mover))
                    self._track_alert_task(task)

        self.price_cache[ticker] = price

        volume_decimal = Decimal(str(volume)).quantize(Decimal("0.01"))
        if volume_decimal > 0:
            db = get_db_pool()
            try:
                db.execute(
                    "SELECT public.accumulate_trade_volume(%s::uuid, %s::numeric, %s::timestamptz)",
                    (token_id, volume_decimal, trade.timestamp),
                )
            except Exception as e:
                logger.warning(f"Failed to accumulate Kalshi trade volume: {e}")

        self.volume_accumulator[ticker] = self.volume_accumulator.get(ticker, 0.0) + volume
        self.dirty_tickers.add(ticker)

        logger.debug(f"Trade: {ticker} @ {trade.price}¢ x {trade.count}")

    async def _handle_orderbook(self, book: KalshiOrderbookDelta) -> None:
        """Handle orderbook delta - extract best bid/ask and mark dirty on mid-price change."""
        ticker = book.ticker
        if ticker not in self.ticker_to_token_id:
            return

        best_bid = None
        best_ask = None
        if book.yes_bids:
            best_bid = max(b.get("price", 0) for b in book.yes_bids) / 100
        if book.yes_asks:
            best_ask = min(a.get("price", 100) for a in book.yes_asks) / 100

        if best_bid is None or best_ask is None:
            return

        mid_price = (best_bid + best_ask) / 2
        prior = self.price_cache.get(ticker)
        self.price_cache[ticker] = mid_price

        if prior is None or abs(prior - mid_price) >= 1e-9:
            self.dirty_tickers.add(ticker)

    async def flush_snapshots(self) -> int:
        """Flush only changed tickers to database."""
        if not self.dirty_tickers:
            return 0

        now = datetime.now(timezone.utc)
        now_ts = time.time()
        snapshots = []
        skipped = 0
        attempted = 0

        for ticker in list(self.dirty_tickers):
            token_id = self.ticker_to_token_id.get(ticker)
            price = self.price_cache.get(ticker)
            if not token_id or price is None:
                continue

            attempted += 1
            volume = self.volume_accumulator.get(ticker)
            should_write = should_write_kalshi_snapshot(
                last_price=self.last_written_price.get(ticker),
                last_written_ts=self.last_written_ts.get(ticker),
                new_price=price,
                batch_volume=volume,
                now_ts=now_ts,
                min_interval_seconds=settings.snapshot_min_write_interval_seconds,
                force_delta_pp=settings.snapshot_force_write_delta_pp,
            )
            if not should_write:
                skipped += 1
                continue

            snapshots.append(
                {
                    "token_id": token_id,
                    "price": price,
                    "volume_24h": None,
                    "spread": None,
                    "ts": now,
                }
            )
            self.last_written_price[ticker] = price
            self.last_written_ts[ticker] = now_ts

        inserted = 0
        if snapshots:
            inserted = MarketQueries.insert_snapshots_batch(snapshots)

        self._inserted_since_window += inserted
        self._skipped_since_window += skipped

        self.dirty_tickers.clear()
        self.volume_accumulator.clear()
        self._last_flush = now_ts

        if attempted > 0:
            logger.debug(
                "Kalshi snapshot flush: "
                f"attempted={attempted} inserted={inserted} skipped={skipped}"
            )

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
    logger.info("Starting Kalshi WSS sync loop")

    logger.info("Performing initial Kalshi REST sync")
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
            connected = await handler.initialize()
            if not connected:
                logger.warning("Kalshi WSS not available - falling back to REST polling")
                await _fallback_to_polling(shutdown)
                return

            count = await handler.load_markets_and_subscribe()
            if count == 0:
                logger.warning("No markets to subscribe - waiting")
                await asyncio.sleep(60)
                continue

            consecutive_failures = 0
            last_health_log = time.time()

            async for event in handler.wss.listen():
                if shutdown.is_set:
                    break

                await handler.process_event(event)

                if time.time() - handler._last_flush > 2.0:
                    await handler.flush_snapshots()

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

            await asyncio.sleep(5 * consecutive_failures)

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
            await asyncio.wait_for(asyncio.create_task(asyncio.sleep(interval)), timeout=interval)
        except asyncio.TimeoutError:
            pass

        if shutdown.is_set:
            break


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class MockShutdown:
        is_set = False

    asyncio.run(run_kalshi_wss_loop(MockShutdown()))
