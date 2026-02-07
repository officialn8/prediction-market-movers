import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from apps.collector.adapters.polymarket_wss import PolymarketWebSocket
from apps.collector.adapters.wss_messages import (
    MarketResolved,
    NewMarket,
    PriceUpdate,
    SpreadUpdate,
    TradeEvent,
)
from apps.collector.jobs.movers_cache import broadcast_mover_alert, check_instant_mover
from apps.collector.jobs.polymarket_sync import (
    get_sync_state,
    should_sync_full_metadata,
    sync_markets,
    sync_markets_and_prices,
)
from apps.collector.jobs.snapshot_gate import should_write_polymarket_snapshot
from packages.core.settings import settings
from packages.core.storage.db import get_db_pool
from packages.core.storage.queries import MarketQueries

logger = logging.getLogger(__name__)

# Health logging interval
HEALTH_LOG_INTERVAL = 60  # seconds


class Shutdown:
    """Simple shutdown signal carrier."""

    def __init__(self):
        self.is_set = False


def _to_epoch_seconds(ts: Optional[datetime]) -> Optional[float]:
    if ts is None:
        return None
    try:
        return ts.timestamp()
    except Exception:
        return None


def _load_active_asset_state(db_pool):
    """Load active Polymarket tokens plus last persisted state."""
    rows = db_pool.execute(
        """
        SELECT
            mt.token_id AS db_token_id,
            mt.source_token_id,
            ls.price,
            ls.ts,
            ls.spread
        FROM markets m
        JOIN market_tokens mt ON m.market_id = mt.market_id
        LEFT JOIN LATERAL (
            SELECT s.price, s.ts, s.spread
            FROM snapshots s
            WHERE s.token_id = mt.token_id
            ORDER BY s.ts DESC
            LIMIT 1
        ) ls ON true
        WHERE m.status = 'active'
          AND mt.source_token_id IS NOT NULL
          AND m.source = 'polymarket'
        """,
        fetch=True,
    ) or []

    source_to_db_token: dict[str, str] = {}
    price_map: dict[str, float] = {}
    last_written_price: dict[str, float] = {}
    last_written_ts: dict[str, float] = {}
    last_written_spread: dict[str, float] = {}

    for row in rows:
        source_id = row["source_token_id"]
        db_id = str(row["db_token_id"])
        source_to_db_token[source_id] = db_id

        if row.get("price") is not None:
            price = float(row["price"])
            price_map[source_id] = price
            last_written_price[source_id] = price

        last_ts = _to_epoch_seconds(row.get("ts"))
        if last_ts is not None:
            last_written_ts[source_id] = last_ts

        if row.get("spread") is not None:
            last_written_spread[source_id] = float(row["spread"])

    return source_to_db_token, price_map, last_written_price, last_written_ts, last_written_spread


def _sync_polymarket_markets_once() -> None:
    """Run metadata sync from the WSS path, including periodic full refreshes."""
    from apps.collector.adapters.polymarket import get_polymarket_adapter

    adapter = get_polymarket_adapter()
    try:
        if settings.polymarket_full_metadata_sync_enabled and should_sync_full_metadata():
            logger.info(
                "Running full Polymarket metadata+volume refresh from WSS path "
                "(max_markets=%s)",
                settings.polymarket_full_metadata_max_markets,
            )
            sync_markets_and_prices(
                adapter,
                max_markets=settings.polymarket_full_metadata_max_markets,
            )
            get_sync_state().last_full_metadata_sync = time.time()
        else:
            sync_markets(adapter)
    finally:
        adapter.close()


def _normalize_resolved_outcome(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"YES", "Y", "TRUE", "1"}:
        return "YES"
    if text in {"NO", "N", "FALSE", "0"}:
        return "NO"
    return None


def _mark_market_resolved(db_pool, market_source_id: str, outcome: Optional[str]) -> None:
    """Mark a market resolved so it can be dropped on subscription refresh."""
    normalized_outcome = _normalize_resolved_outcome(outcome)
    db_pool.execute(
        """
        UPDATE markets
        SET status = 'resolved',
            resolved_outcome = COALESCE(%s, resolved_outcome),
            resolved_at = COALESCE(resolved_at, NOW()),
            updated_at = NOW()
        WHERE source = 'polymarket'
          AND source_id = %s
        """,
        (normalized_outcome, market_source_id),
    )
    logger.info(
        "Marked market %s as resolved (outcome=%s)",
        market_source_id,
        normalized_outcome or outcome,
    )


def _fetch_storage_sizes(db_pool) -> dict:
    rows = db_pool.execute(
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

    db_rows = db_pool.execute(
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


def _upsert_polymarket_status(db_pool, status_data: dict) -> None:
    """Write Polymarket WSS health/status snapshot for dashboard visibility."""
    db_pool.execute(
        """
        INSERT INTO system_status (key, value, updated_at)
        VALUES ('polymarket_wss', %s, NOW())
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW()
        """,
        (json.dumps(status_data),),
    )


def _effective_subscription_refresh_seconds(asset_count: int) -> int:
    """
    Compute a safe refresh cadence based on subscription bootstrap time.

    Large universes take minutes to subscribe; refreshing faster than that
    causes constant reconnect churn and stale status telemetry.
    """
    subscription_chunks = max(1, (asset_count + 19) // 20)
    estimated_subscribe_seconds = subscription_chunks * 0.2
    return max(
        settings.polymarket_subscription_refresh_seconds,
        int(estimated_subscribe_seconds + 120),
    )


async def _emit_subscription_bootstrap_status(
    db_pool,
    client: PolymarketWebSocket,
    asset_count: int,
    refresh_interval_seconds: int,
    start_ts: float,
) -> None:
    """
    Keep polymarket_wss status fresh while connect()+subscribe is in flight.

    Large subscription universes can take minutes before the first event loop
    heartbeat; emit a lightweight status row so dashboard staleness reflects
    active bootstrap instead of a stale historical row.
    """
    while True:
        now_ts = time.time()
        status_data = {
            "connected": False,
            "latency_ms": 0.0,
            "messages_received": 0,
            "snapshot_inserted_window": 0,
            "snapshot_skipped_window": 0,
            "snapshot_inserted_per_min": 0.0,
            "snapshot_skipped_per_min": 0.0,
            "subscription_count": client._metrics.current_subscriptions,
            "subscription_target": asset_count,
            "refresh_interval_seconds": refresh_interval_seconds,
            "state": "subscribing",
            "bootstrap_elapsed_seconds": round(max(0.0, now_ts - start_ts), 1),
            "last_updated": now_ts,
        }
        try:
            _upsert_polymarket_status(db_pool, status_data)
        except Exception as e:
            logger.debug("Failed to update bootstrap status: %s", e)
        await asyncio.sleep(5.0)


async def run_wss_loop(shutdown: Shutdown) -> None:
    """
    Main WSS loop with:
    - Initial REST sync to get token list
    - WSS connection and subscription
    - Message handling with batched DB writes
    - Automatic reconnection
    - Fallback to polling on prolonged disconnect
    """

    logger.info("Performing initial REST sync before WSS")
    await asyncio.to_thread(_sync_polymarket_markets_once)

    db_pool = get_db_pool()
    client = PolymarketWebSocket(enable_custom_features=True)

    consecutive_failures = 0
    last_storage_metrics = 0.0

    while not shutdown.is_set:
        (
            source_to_db_token,
            price_map,
            last_written_price,
            last_written_ts,
            last_written_spread,
        ) = await asyncio.to_thread(_load_active_asset_state, db_pool)

        # Prioritize assets with the most recent persisted activity first so
        # the first subscription chunk is more likely to emit live events.
        asset_ids = sorted(
            source_to_db_token.keys(),
            key=lambda token: last_written_ts.get(token, 0.0),
            reverse=True,
        )
        if not asset_ids:
            logger.warning("No active Polymarket assets available; retrying in 30s")
            await asyncio.sleep(30)
            continue

        logger.info(f"Loaded {len(asset_ids)} assets for WSS subscription")

        pending_updates: list[PriceUpdate] = []
        pending_trades: list[TradeEvent] = []
        pending_spreads: list[SpreadUpdate] = []
        volume_accumulator: dict[str, float] = defaultdict(float)

        last_batch_flush = time.time()
        last_status_flush = time.time()
        latest_latency_ms = 0.0
        last_health_log = time.time()
        messages_since_last_health = 0
        instant_mover_last_ts: dict[str, float] = {}
        alert_tasks: set[asyncio.Task] = set()

        # Per-minute write counters
        counter_window_start = time.time()
        inserted_since_window = 0
        skipped_since_window = 0

        needs_subscription_refresh = False
        last_subscription_refresh = time.time()
        refresh_reason: Optional[str] = None

        # Refreshing 30k+ subscriptions takes minutes; avoid refresh cadence shorter
        # than the bootstrap + stabilization time.
        effective_refresh_seconds = _effective_subscription_refresh_seconds(len(asset_ids))

        def _track_alert_task(task: asyncio.Task) -> None:
            alert_tasks.add(task)

            def _on_done(done_task: asyncio.Task) -> None:
                alert_tasks.discard(done_task)
                try:
                    exc = done_task.exception()
                except asyncio.CancelledError:
                    return
                if exc:
                    logger.warning(f"Instant mover alert task failed: {exc}")

            task.add_done_callback(_on_done)

        try:
            bootstrap_start_ts = time.time()
            bootstrap_status_task = asyncio.create_task(
                _emit_subscription_bootstrap_status(
                    db_pool,
                    client,
                    len(asset_ids),
                    effective_refresh_seconds,
                    bootstrap_start_ts,
                )
            )
            try:
                await client.connect(asset_ids)
            finally:
                if bootstrap_status_task and not bootstrap_status_task.done():
                    bootstrap_status_task.cancel()
                    try:
                        await bootstrap_status_task
                    except asyncio.CancelledError:
                        pass
            last_subscription_refresh = time.time()
            last_status_flush = 0.0
            consecutive_failures = 0
            logger.info(
                f"WSS connected, starting message loop "
                f"(watchdog={settings.wss_watchdog_timeout}s, "
                f"refresh={effective_refresh_seconds}s)"
            )
            try:
                _upsert_polymarket_status(
                    db_pool,
                    {
                        "connected": True,
                        "latency_ms": 0.0,
                        "messages_received": 0,
                        "snapshot_inserted_window": inserted_since_window,
                        "snapshot_skipped_window": skipped_since_window,
                        "snapshot_inserted_per_min": 0.0,
                        "snapshot_skipped_per_min": 0.0,
                        "subscription_count": client._metrics.current_subscriptions,
                        "subscription_target": len(asset_ids),
                        "refresh_interval_seconds": effective_refresh_seconds,
                        "state": (
                            "subscribing"
                            if client.is_subscription_in_progress
                            else "streaming"
                        ),
                        "last_updated": time.time(),
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to write initial Polymarket WSS status: {e}")

            listen_iter = client.listen().__aiter__()
            while not shutdown.is_set:
                try:
                    event = await asyncio.wait_for(
                        listen_iter.__anext__(),
                        timeout=settings.wss_watchdog_timeout,
                    )
                except StopAsyncIteration:
                    logger.warning("WSS listen() iterator exhausted")
                    break
                except asyncio.TimeoutError:
                    subscription_error = client.pop_subscription_error()
                    if subscription_error:
                        raise ConnectionError(
                            f"Subscription bootstrap failed: {subscription_error}"
                        )

                    if client.is_subscription_in_progress:
                        logger.warning(
                            "Watchdog timeout during subscription bootstrap "
                            "(%s/%s assets subscribed); continuing",
                            client._metrics.current_subscriptions,
                            client.subscription_target,
                        )
                        continue

                    last_activity = client._metrics.last_message_time
                    if last_activity and (
                        (time.time() - last_activity) < settings.wss_watchdog_timeout
                    ):
                        logger.debug(
                            "Watchdog timeout ignored due recent socket activity "
                            "(idle=%.1fs)",
                            time.time() - last_activity,
                        )
                        continue

                    logger.error(
                        "WSS watchdog timeout: no messages received for "
                        f"{settings.wss_watchdog_timeout}s"
                    )
                    raise ConnectionError("Watchdog timeout - no messages received")

                now_ts = time.time()
                messages_since_last_health += 1

                if isinstance(event, PriceUpdate):
                    source_token_id = event.token_id

                    if source_token_id in price_map:
                        old_price = price_map[source_token_id]
                        db_token_id = source_to_db_token.get(source_token_id)
                        if db_token_id:
                            last_alert_ts = instant_mover_last_ts.get(db_token_id)
                            if (
                                last_alert_ts is None
                                or (now_ts - last_alert_ts)
                                >= settings.instant_mover_debounce_seconds
                            ):
                                mover = await check_instant_mover(
                                    db_token_id,
                                    old_price,
                                    event.price,
                                )
                                if mover:
                                    logger.info(
                                        "Instant Mover Detected: "
                                        f"{source_token_id} {old_price:.4f} -> {event.price:.4f}"
                                    )
                                    instant_mover_last_ts[db_token_id] = now_ts
                                    task = asyncio.create_task(broadcast_mover_alert(mover))
                                    _track_alert_task(task)

                    price_map[source_token_id] = event.price
                    pending_updates.append(event)

                elif isinstance(event, TradeEvent):
                    source_token_id = event.token_id
                    trade_volume = event.size * event.price
                    volume_accumulator[source_token_id] += trade_volume
                    trade_volume_decimal = Decimal(str(trade_volume)).quantize(Decimal("0.01"))
                    db_token_id = source_to_db_token.get(source_token_id)

                    # Trade events carry notional, so pass it to instant-mover scoring.
                    old_price = price_map.get(source_token_id)
                    if old_price is not None and db_token_id:
                        last_alert_ts = instant_mover_last_ts.get(db_token_id)
                        if (
                            last_alert_ts is None
                            or (now_ts - last_alert_ts)
                            >= settings.instant_mover_debounce_seconds
                        ):
                            mover = await check_instant_mover(
                                db_token_id,
                                old_price,
                                event.price,
                                volume=trade_volume,
                            )
                            if mover:
                                logger.info(
                                    "Instant Mover Detected (trade): "
                                    f"{source_token_id} {old_price:.4f} -> {event.price:.4f} "
                                    f"(vol=${trade_volume:.2f})"
                                )
                                instant_mover_last_ts[db_token_id] = now_ts
                                task = asyncio.create_task(broadcast_mover_alert(mover))
                                _track_alert_task(task)

                    price_map[source_token_id] = event.price

                    pending_trades.append(event)

                    if db_token_id and trade_volume_decimal > 0:
                        try:
                            db_pool.execute(
                                "SELECT public.accumulate_trade_volume(%s::uuid, %s::numeric, %s::timestamptz)",
                                (db_token_id, trade_volume_decimal, event.timestamp),
                            )
                        except Exception as e:
                            logger.warning(f"Failed to accumulate trade volume: {e}")

                elif isinstance(event, SpreadUpdate):
                    pending_spreads.append(event)

                elif isinstance(event, MarketResolved):
                    logger.info(f"Market Resolved: {event.market_id} -> {event.outcome}")
                    await asyncio.to_thread(
                        _mark_market_resolved,
                        db_pool,
                        event.market_id,
                        event.outcome,
                    )
                    # Defer reconnect to the periodic refresh window.
                    # Immediate reconnect on event bursts causes subscription churn.
                    refresh_reason = refresh_reason or "market_resolved"

                elif isinstance(event, NewMarket):
                    logger.info(f"New Market: {event.market_id} with {len(event.tokens)} tokens")
                    await asyncio.to_thread(_sync_polymarket_markets_once)
                    # Defer reconnect to the periodic refresh window.
                    # Immediate reconnect on event bursts causes subscription churn.
                    refresh_reason = refresh_reason or "new_market"

                if hasattr(event, "timestamp") and event.timestamp:
                    try:
                        msg_ts = event.timestamp.timestamp()
                        latest_latency_ms = max(0.0, (time.time() - msg_ts) * 1000)
                    except Exception:
                        latest_latency_ms = 0.0
                else:
                    latest_latency_ms = 0.0

                total_pending = len(pending_updates) + len(pending_trades) + len(pending_spreads)
                if (
                    total_pending >= settings.wss_batch_size
                    or (now_ts - last_batch_flush) >= settings.wss_batch_interval
                ):
                    inserted, skipped = await flush_price_batch(
                        updates=pending_updates,
                        source_to_db_token=source_to_db_token,
                        volume_accumulator=volume_accumulator,
                        spread_updates=pending_spreads,
                        last_written_price=last_written_price,
                        last_written_ts=last_written_ts,
                        last_written_spread=last_written_spread,
                    )
                    inserted_since_window += inserted
                    skipped_since_window += skipped

                    pending_updates.clear()
                    pending_trades.clear()
                    pending_spreads.clear()
                    volume_accumulator.clear()
                    last_batch_flush = now_ts
                    client._metrics.save()

                # Subscription refresh for closed/resolved/new markets.
                if (
                    now_ts - last_subscription_refresh
                    >= effective_refresh_seconds
                ):
                    needs_subscription_refresh = True
                    refresh_reason = refresh_reason or "periodic"

                if needs_subscription_refresh:
                    if pending_updates:
                        inserted, skipped = await flush_price_batch(
                            updates=pending_updates,
                            source_to_db_token=source_to_db_token,
                            volume_accumulator=volume_accumulator,
                            spread_updates=pending_spreads,
                            last_written_price=last_written_price,
                            last_written_ts=last_written_ts,
                            last_written_spread=last_written_spread,
                        )
                        inserted_since_window += inserted
                        skipped_since_window += skipped
                    logger.info(
                        "Refreshing Polymarket WSS subscription universe (reason=%s)",
                        refresh_reason or "unknown",
                    )
                    try:
                        _upsert_polymarket_status(
                            db_pool,
                            {
                                "connected": True,
                                "latency_ms": round(latest_latency_ms, 2),
                                "messages_received": messages_since_last_health,
                                "snapshot_inserted_window": inserted_since_window,
                                "snapshot_skipped_window": skipped_since_window,
                                "snapshot_inserted_per_min": 0.0,
                                "snapshot_skipped_per_min": 0.0,
                                "subscription_count": client._metrics.current_subscriptions,
                                "subscription_target": len(asset_ids),
                                "refresh_interval_seconds": effective_refresh_seconds,
                                "state": "refreshing",
                                "refresh_reason": refresh_reason,
                                "last_updated": now_ts,
                            },
                        )
                    except Exception as e:
                        logger.warning(f"Failed to update refresh status: {e}")
                    break

                if now_ts - last_status_flush > 5.0:
                    elapsed = max(now_ts - counter_window_start, 1e-6)
                    per_min_scale = 60.0 / elapsed

                    status_data = {
                        "connected": True,
                        "latency_ms": round(latest_latency_ms, 2),
                        "messages_received": messages_since_last_health,
                        "snapshot_inserted_window": inserted_since_window,
                        "snapshot_skipped_window": skipped_since_window,
                        "snapshot_inserted_per_min": round(inserted_since_window * per_min_scale, 2),
                        "snapshot_skipped_per_min": round(skipped_since_window * per_min_scale, 2),
                        "subscription_count": client._metrics.current_subscriptions,
                        "subscription_target": len(asset_ids),
                        "refresh_interval_seconds": effective_refresh_seconds,
                        "state": (
                            "subscribing"
                            if client.is_subscription_in_progress
                            else "streaming"
                        ),
                        "last_updated": now_ts,
                    }

                    if now_ts - last_storage_metrics >= settings.storage_metrics_interval_seconds:
                        status_data["storage"] = await asyncio.to_thread(_fetch_storage_sizes, db_pool)
                        last_storage_metrics = now_ts

                    try:
                        _upsert_polymarket_status(db_pool, status_data)
                        last_status_flush = now_ts
                    except Exception as e:
                        logger.warning(f"Failed to update system status: {e}")

                    if elapsed >= 60.0:
                        counter_window_start = now_ts
                        inserted_since_window = 0
                        skipped_since_window = 0

                if now_ts - last_health_log >= HEALTH_LOG_INTERVAL:
                    elapsed = now_ts - last_health_log
                    msgs_per_min = (messages_since_last_health / elapsed) * 60 if elapsed > 0 else 0
                    logger.info(
                        f"WSS Health: {messages_since_last_health} msgs in {elapsed:.0f}s "
                        f"({msgs_per_min:.1f}/min), subscriptions="
                        f"{client._metrics.current_subscriptions}/{len(asset_ids)}"
                    )
                    last_health_log = now_ts
                    messages_since_last_health = 0

            if needs_subscription_refresh:
                await client.close()
                await asyncio.sleep(1)
                continue

        except Exception as e:
            logger.error(f"WSS Loop Error: {e}")
            try:
                _upsert_polymarket_status(
                    db_pool,
                    {
                        "connected": False,
                        "latency_ms": 0.0,
                        "messages_received": 0,
                        "snapshot_inserted_window": 0,
                        "snapshot_skipped_window": 0,
                        "snapshot_inserted_per_min": 0.0,
                        "snapshot_skipped_per_min": 0.0,
                        "subscription_count": client._metrics.current_subscriptions,
                        "subscription_target": len(asset_ids),
                        "refresh_interval_seconds": effective_refresh_seconds,
                        "state": "error",
                        "last_error": str(e),
                        "last_updated": time.time(),
                    },
                )
            except Exception:
                pass
            consecutive_failures += 1

            if consecutive_failures >= settings.wss_max_reconnect_attempts:
                if settings.wss_fallback_to_polling:
                    logger.critical("Max WSS reconnects reached. Falling back to POLLING mode.")
                    await client.close()
                    from apps.collector.jobs.polymarket_sync import sync_once

                    logger.info(
                        f"Starting fallback polling (interval={settings.sync_interval_seconds}s)"
                    )
                    while not shutdown.is_set:
                        try:
                            await sync_once()
                            _upsert_polymarket_status(
                                db_pool,
                                {
                                    "connected": False,
                                    "latency_ms": 0.0,
                                    "messages_received": 0,
                                    "snapshot_inserted_window": 0,
                                    "snapshot_skipped_window": 0,
                                    "snapshot_inserted_per_min": 0.0,
                                    "snapshot_skipped_per_min": 0.0,
                                    "subscription_count": 0,
                                    "refresh_interval_seconds": effective_refresh_seconds,
                                    "state": "polling_fallback",
                                    "last_updated": time.time(),
                                },
                            )
                        except Exception as poll_err:
                            logger.exception(f"Fallback polling error: {poll_err}")
                        await asyncio.sleep(settings.sync_interval_seconds)
                    return
                else:
                    logger.critical(
                        "Max WSS reconnects reached and fallback disabled. Exiting."
                    )
                    break

            await asyncio.sleep(settings.wss_reconnect_delay)

        finally:
            await client.close()


async def flush_price_batch(
    updates: list[PriceUpdate],
    source_to_db_token: dict[str, str],
    volume_accumulator: Optional[dict[str, float]] = None,
    spread_updates: Optional[list[SpreadUpdate]] = None,
    last_written_price: Optional[dict[str, float]] = None,
    last_written_ts: Optional[dict[str, float]] = None,
    last_written_spread: Optional[dict[str, float]] = None,
) -> tuple[int, int]:
    """
    Batch insert pending updates to database with write gating.

    Returns:
        tuple(inserted_count, skipped_count)
    """
    if not updates:
        return 0, 0

    volume_accumulator = volume_accumulator or {}
    spread_updates = spread_updates or []
    last_written_price = last_written_price or {}
    last_written_ts = last_written_ts or {}
    last_written_spread = last_written_spread or {}

    spread_map: dict[str, float] = {}
    for spread_update in spread_updates:
        spread_map[spread_update.token_id] = spread_update.spread

    latest_map: dict[str, PriceUpdate] = {}
    for update in updates:
        latest_map[update.token_id] = update

    unique_updates = list(latest_map.values())
    if not unique_updates:
        return 0, 0

    now_dt = datetime.now(timezone.utc)
    now_ts = time.time()
    snapshots: list[dict] = []
    skipped = 0

    for update in unique_updates:
        source_token_id = update.token_id
        db_token_id = source_to_db_token.get(source_token_id)
        if not db_token_id:
            continue

        volume = volume_accumulator.get(source_token_id)
        spread = spread_map.get(source_token_id)

        should_write = should_write_polymarket_snapshot(
            last_price=last_written_price.get(source_token_id),
            last_written_ts=last_written_ts.get(source_token_id),
            new_price=update.price,
            batch_volume=volume,
            spread=spread,
            last_spread=last_written_spread.get(source_token_id),
            now_ts=now_ts,
            min_interval_seconds=settings.snapshot_min_write_interval_seconds,
            force_delta_pp=settings.snapshot_force_write_delta_pp,
        )

        if not should_write:
            skipped += 1
            continue

        snapshots.append(
            {
                "token_id": db_token_id,
                "price": update.price,
                "volume_24h": None,
                "spread": spread,
                "ts": now_dt,
            }
        )

        last_written_price[source_token_id] = update.price
        last_written_ts[source_token_id] = now_ts
        if spread is not None:
            last_written_spread[source_token_id] = spread

    inserted = 0
    if snapshots:
        inserted = MarketQueries.insert_snapshots_batch(snapshots)

    if inserted > 0 or skipped > 0:
        logger.debug(
            f"Polymarket flush: attempted={len(unique_updates)} inserted={inserted} skipped={skipped}"
        )

    return inserted, skipped
