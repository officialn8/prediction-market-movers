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


async def run_polymarket(shutdown: Shutdown, every_seconds: int = 30) -> None:
    """
    Run Polymarket-only sync loop.
    """
    from apps.collector.jobs.polymarket_sync import sync_once as poly_sync_once

    logger.info(f"Polymarket sync starting (interval={every_seconds}s)")
    
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

    shutdown = Shutdown()

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



