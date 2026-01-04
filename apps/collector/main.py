"""
Prediction Market Collector - Main Entry Point

Runs background ingestion jobs that sync markets/snapshots into Postgres.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import Optional

from packages.core.settings import settings
from packages.core.storage import get_db_pool

logger = logging.getLogger("collector")


def run_migrations() -> None:
    """Run SQL migrations on startup."""
    import glob
    from packages.core.storage.db import get_db_pool
    
    db = get_db_pool()
    migration_files = sorted(glob.glob("migrations/*.sql"))
    
    if not migration_files:
        logger.info("No migrations found.")
        return

    logger.info(f"Found {len(migration_files)} migrations. Applying...")

    for mig in migration_files:
        try:
            with open(mig) as f:
                db.execute(f.read())
            logger.info(f"Applied migration: {mig}")
        except Exception as e:
            # Check for "already exists" type errors (DuplicateObject, etc.)
            # This is a basic catch-all for idempotency on dirty DBs
            err = str(e).lower()
            if "already exists" in err or "violates unique constraint" in err:
                logger.warning(f"Migration {mig} skipped (likely already applied): {e}")
            else:
                logger.error(f"Migration failed {mig}: {e}")
                # We stop on critical errors, but maybe we should let it try the constraint fix?
                # If 001 fails, we usually want to stop. But if it fails because it exists, we continue.
                # If 008 (the fix) runs, it needs to succeed.
                raise



def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class Shutdown:
    """Signal-driven shutdown flag."""
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    def request(self) -> None:
        self._stop.set()

    async def wait(self) -> None:
        await self._stop.wait()

    @property
    def is_set(self) -> bool:
        return self._stop.is_set()


async def run_simulated(shutdown: Shutdown, every_seconds: int = 15) -> None:
    """
    Runs the simulated data loop. Prefer keeping the loop here so SIGTERM works cleanly.
    """
    from apps.collector.jobs.simulated_sync import run_simulated_loop

    # If your run_simulated_loop doesn't support stop flags, run it in a thread
    # and rely on SIGTERM causing process exit. Better: refactor simulated_sync
    # to accept a stop flag. For now, keep it simple:
    try:
        run_simulated_loop(n_markets=30, every_seconds=every_seconds)
    except KeyboardInterrupt:
        logger.info("Simulated loop interrupted.")



async def run_polymarket_wss(shutdown: Shutdown) -> None:
    """
    Run WebSocket-based real-time sync.
    Falls back to polling on disconnect.
    """
    try:
        from apps.collector.jobs.polymarket_wss_sync import run_wss_loop
        logger.info("Starting Polymarket WSS real-time sync")
        await run_wss_loop(shutdown)
    except ImportError as e:
        logger.critical(f"Failed to import WSS module (missing dependencies?): {e}")
        raise
    except Exception as e:
        logger.critical(f"WSS Loop failed: {e}")
        raise


async def run_polymarket(shutdown: Shutdown, every_seconds: int = 30) -> None:
    """
    Run Polymarket-only sync loop.
    """
    from apps.collector.jobs.polymarket_sync import sync_once as poly_sync_once

    logger.info(f"Polymarket sync starting (interval={every_seconds}s)")
    
    # Check if WSS is enabled
    if settings.polymarket_use_wss:
        logger.info("Mode: polymarket (WSS real-time)")
        await run_polymarket_wss(shutdown)
        return

    logger.info("Mode: polymarket (POLLING)")
    while not shutdown.is_set:
        try:
            await poly_sync_once()
        except Exception:
            logger.exception("Error during Polymarket sync cycle")
        await asyncio.sleep(every_seconds)


async def run_live(shutdown: Shutdown, every_seconds: int = 30) -> None:
    """
    Run both Polymarket and Kalshi sync.
    """
    from apps.collector.jobs.polymarket_sync import sync_once as poly_sync_once
    from apps.collector.jobs.kalshi_sync import sync_once as kalshi_sync_once

    logger.info(f"Live sync starting (interval={every_seconds}s)")
    
    while not shutdown.is_set:
        try:
            await poly_sync_once()
            await kalshi_sync_once()
        except Exception:
            logger.exception("Error during live sync cycle")
        await asyncio.sleep(every_seconds)


async def run_alerts_loop(shutdown: Shutdown) -> None:
    """Background loop for checking alerts."""
    from apps.collector.jobs.alerts import run_alerts_check
    
    logger.info("Alerts loop starting (interval=60s)")
    while not shutdown.is_set:
        try:
            await run_alerts_check()
        except Exception:
            logger.exception("Error in alerts loop")
        
        # Wait for next check (interruptible sleep)
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=60)
            break
        except asyncio.TimeoutError:
            continue




async def run_movers_cache_loop(shutdown: Shutdown) -> None:
    """Background loop for updating movers cache."""
    from apps.collector.jobs.movers_cache import update_movers_cache
    
    logger.info("Movers cache loop starting (interval=300s)")
    while not shutdown.is_set:
        try:
            await update_movers_cache()
        except Exception:
            logger.exception("Error in movers cache loop")
        
        try:
            # Update every 5 minutes
            await asyncio.wait_for(shutdown.wait(), timeout=300)
            break
        except asyncio.TimeoutError:
            continue


async def run_rollups_loop(shutdown: Shutdown) -> None:
    """Background loop for OHLC rollups and retention."""
    from apps.collector.jobs.rollups import run_ohlc_rollups

    logger.info("Rollups loop starting (interval=60s)")
    while not shutdown.is_set:
        try:
            await run_ohlc_rollups()
        except Exception:
            logger.exception("Error in rollups loop")

        try:
            # Run every minute
            await asyncio.wait_for(shutdown.wait(), timeout=60)
            break
        except asyncio.TimeoutError:
            continue


async def run_user_alerts_loop(shutdown: Shutdown) -> None:
    """Background loop for checking user-defined custom alerts."""
    from apps.collector.jobs.user_alerts import check_user_alerts

    logger.info("User alerts loop starting (interval=30s)")
    while not shutdown.is_set:
        try:
            await check_user_alerts()
        except Exception:
            logger.exception("Error in user alerts loop")

        try:
            # Check every 30 seconds for responsive notifications
            await asyncio.wait_for(shutdown.wait(), timeout=30)
            break
        except asyncio.TimeoutError:
            continue


async def run_volume_spikes_loop(shutdown: Shutdown) -> None:
    """Background loop for detecting volume spikes (unusual activity)."""
    from apps.collector.jobs.volume_spikes import check_volume_spikes

    logger.info("Volume spikes loop starting (interval=120s)")
    while not shutdown.is_set:
        try:
            await check_volume_spikes()
        except Exception:
            logger.exception("Error in volume spikes loop")

        try:
            # Check every 2 minutes for volume anomalies
            await asyncio.wait_for(shutdown.wait(), timeout=120)
            break
        except asyncio.TimeoutError:
            continue


async def _amain() -> None:
    _configure_logging()
    logger.info("Starting collector…")

    # Initialize DB pool early (fails fast)
    db = get_db_pool()
    try:
        # Optional: run a tiny health query if your db wrapper supports it
        # db.execute("SELECT 1", fetch=True)
        pass
    except Exception:
        logger.exception("Database connectivity check failed.")
        sys.exit(1)

    # Run migrations
    try:
        run_migrations()
    except Exception:
        logger.exception("Failed to run migrations.")
        sys.exit(1)

    shutdown = Shutdown()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.request)
        except NotImplementedError:
            # Windows / some runtimes
            signal.signal(sig, lambda *_: shutdown.request())

    mode = os.getenv("COLLECTOR_MODE", "simulated").lower()
    interval = int(os.getenv("COLLECTOR_INTERVAL_SECONDS", "30"))

    # Start additional background tasks
    bg_tasks = []
    if mode != "simulated":
        bg_tasks.append(asyncio.create_task(run_alerts_loop(shutdown)))
        bg_tasks.append(asyncio.create_task(run_movers_cache_loop(shutdown)))
        bg_tasks.append(asyncio.create_task(run_rollups_loop(shutdown)))
        bg_tasks.append(asyncio.create_task(run_user_alerts_loop(shutdown)))
        bg_tasks.append(asyncio.create_task(run_volume_spikes_loop(shutdown)))

    try:
        if mode == "simulated":
            logger.info("Mode: simulated")
            await run_simulated(shutdown, every_seconds=interval)
        elif mode == "polymarket":
            logger.info("Mode: polymarket")
            await run_polymarket(shutdown, every_seconds=interval)
        else:
            logger.info("Mode: live (all sources)")
            await run_live(shutdown, every_seconds=interval)
            
    finally:
        logger.info("Shutting down…")
        
        # Wait for background tasks to finish
        for task in bg_tasks:
            try:
                task.cancel()
                await task
            except Exception:
                pass

        try:
            db.close()
        except Exception:
            logger.exception("Failed to close DB pool cleanly.")


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()



