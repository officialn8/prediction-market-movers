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
    """
    Run SQL migrations on startup with proper tracking and transactional guarantees.
    
    Features:
    - Tracks applied migrations in schema_migrations table
    - Each migration runs in a transaction (rollback on failure)
    - Stops on first failure to prevent partial state
    - Idempotent: skips already-applied migrations
    """
    import glob
    import hashlib
    import os
    from packages.core.storage.db import get_db_pool
    
    db = get_db_pool()
    migration_files = sorted(glob.glob("migrations/*.sql"))
    
    if not migration_files:
        logger.info("No migrations found.")
        return

    # Bootstrap: Ensure schema_migrations table exists (special handling)
    # This must succeed before we can track anything
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_name TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                checksum TEXT
            )
        """)
    except Exception as e:
        logger.error(f"Failed to create schema_migrations table: {e}")
        raise

    # Get already-applied migrations
    applied = set()
    try:
        rows = db.execute("SELECT migration_name FROM schema_migrations", fetch=True)
        if rows:
            applied = {r["migration_name"] for r in rows}
    except Exception as e:
        logger.warning(f"Could not query schema_migrations (first run?): {e}")

    logger.info(f"Found {len(migration_files)} migrations, {len(applied)} already applied.")

    applied_count = 0
    for mig in migration_files:
        migration_name = os.path.basename(mig)
        
        # Skip if already applied
        if migration_name in applied:
            logger.debug(f"Skipping already-applied migration: {migration_name}")
            continue
        
        # Read migration content
        try:
            with open(mig) as f:
                sql_content = f.read()
        except Exception as e:
            logger.error(f"Failed to read migration file {mig}: {e}")
            raise
        
        # Compute checksum for tracking
        checksum = hashlib.sha256(sql_content.encode()).hexdigest()[:16]
        
        # Apply migration within a transaction
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Execute the migration
                    cur.execute(sql_content)
                    
                    # Record successful application
                    cur.execute(
                        """
                        INSERT INTO schema_migrations (migration_name, checksum)
                        VALUES (%s, %s)
                        ON CONFLICT (migration_name) DO NOTHING
                        """,
                        (migration_name, checksum)
                    )
                    
                # Commit the transaction (both migration and tracking record)
                conn.commit()
                
            logger.info(f"Applied migration: {migration_name}")
            applied_count += 1
            
        except Exception as e:
            err = str(e).lower()
            # Handle benign "already exists" errors (for legacy/dirty DBs)
            if "already exists" in err or "duplicate key" in err:
                logger.warning(f"Migration {migration_name} objects exist, marking as applied: {e}")
                # Still record it as applied so we don't retry
                try:
                    db.execute(
                        """
                        INSERT INTO schema_migrations (migration_name, checksum)
                        VALUES (%s, %s)
                        ON CONFLICT (migration_name) DO NOTHING
                        """,
                        (migration_name, checksum)
                    )
                except Exception:
                    pass  # Best effort
            else:
                # Transaction was rolled back, stop processing
                logger.error(f"Migration {migration_name} failed (rolled back): {e}")
                raise RuntimeError(f"Migration {migration_name} failed: {e}") from e
    
    if applied_count > 0:
        logger.info(f"Successfully applied {applied_count} new migration(s).")


def verify_schema_readiness() -> None:
    """
    Verify critical tables and functions exist before starting background tasks.
    Exits on missing schema elements to avoid noisy runtime failures.
    """
    db = get_db_pool()
    required_tables = [
        "markets",
        "market_tokens",
        "snapshots",
        "schema_migrations",
        "system_status",
        "volume_spikes",
        "user_alerts",
        "movers_cache",
        "ohlc_1m",
        "ohlc_5m",
        "ohlc_1h",
        "trade_volumes",
        "volume_hourly",
        "model_scoring_daily",
    ]

    missing_tables = []
    for table in required_tables:
        result = db.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            ) as exists
            """,
            (table,),
            fetch=True,
        )
        if not result or not result[0].get("exists"):
            missing_tables.append(table)

    func_result = db.execute(
        """
        SELECT EXISTS(
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = 'public' AND p.proname = 'accumulate_trade_volume'
        ) as exists
        """,
        fetch=True,
    )
    has_accumulate = bool(func_result and func_result[0].get("exists"))

    if missing_tables or not has_accumulate:
        if missing_tables:
            logger.error(f"Schema readiness check failed. Missing tables: {missing_tables}")
        if not has_accumulate:
            logger.error("Schema readiness check failed. Missing function: public.accumulate_trade_volume")
        sys.exit(1)



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
    Runs the simulated data loop with proper shutdown support.
    """
    from apps.collector.jobs.simulated_sync import seed_simulated_markets, write_simulated_snapshots

    logger.info(f"Starting simulated sync (interval={every_seconds}s)")
    
    # Seed markets once
    sim = seed_simulated_markets(n_markets=30)
    
    while not shutdown.is_set:
        try:
            inserted = write_simulated_snapshots(sim)
            logger.debug(f"[simulated] inserted_snapshots={inserted}")
        except Exception:
            logger.exception("Error in simulated sync cycle")
        
        # Interruptible sleep
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=every_seconds)
            break  # Shutdown triggered
        except asyncio.TimeoutError:
            continue  # Normal timeout, continue loop
    
    logger.info("Simulated loop stopped.")



async def run_polymarket_wss(shutdown: Shutdown) -> None:
    """
    Run WebSocket-based real-time sync.
    Falls back to polling if WSS fails to import or initialize.
    """
    try:
        from apps.collector.jobs.polymarket_wss_sync import run_wss_loop
        logger.info("Starting Polymarket WSS real-time sync")
        await run_wss_loop(shutdown)
    except ImportError as e:
        logger.warning(f"WSS module unavailable (missing dependencies?): {e}")
        logger.info("Falling back to polling mode")
        await _fallback_to_polling(shutdown)
    except Exception as e:
        logger.error(f"WSS Loop failed: {e}")
        logger.info("Falling back to polling mode")
        await _fallback_to_polling(shutdown)


async def _fallback_to_polling(shutdown: Shutdown, interval: int = 30) -> None:
    """
    Fallback polling loop when WSS is unavailable.
    """
    from apps.collector.jobs.polymarket_sync import sync_once as poly_sync_once
    
    logger.info(f"Running fallback polling (interval={interval}s)")
    while not shutdown.is_set:
        try:
            await poly_sync_once()
        except Exception:
            logger.exception("Error during fallback polling cycle")
        
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            continue


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
    
    Uses WebSocket for both when available:
    - Polymarket WSS: Always available (no auth needed)
    - Kalshi WSS: Requires API key (falls back to REST if not configured)
    """
    logger.info(f"Live sync starting (interval={every_seconds}s)")
    
    # Check if Kalshi WSS is enabled and configured
    kalshi_wss_enabled = (
        settings.kalshi_use_wss and 
        settings.kalshi_api_key and
        (settings.kalshi_private_key_path or settings.kalshi_private_key)
    )
    
    if kalshi_wss_enabled:
        logger.info("Mode: live (Polymarket WSS + Kalshi WSS)")
        # Run both WSS loops in parallel
        await asyncio.gather(
            run_polymarket_wss(shutdown),
            _run_kalshi_wss(shutdown),
            return_exceptions=True
        )
    elif settings.polymarket_use_wss:
        logger.info("Mode: live (Polymarket WSS + Kalshi REST)")
        # Run Polymarket WSS with Kalshi REST polling in background
        kalshi_task = asyncio.create_task(_run_kalshi_rest(shutdown, every_seconds))
        try:
            await run_polymarket_wss(shutdown)
        finally:
            kalshi_task.cancel()
    else:
        logger.info("Mode: live (REST polling for both)")
        from apps.collector.jobs.polymarket_sync import sync_once as poly_sync_once
        from apps.collector.jobs.kalshi_sync import sync_once as kalshi_sync_once
        
        while not shutdown.is_set:
            try:
                await poly_sync_once()
                await kalshi_sync_once()
            except Exception:
                logger.exception("Error during live sync cycle")
            await asyncio.sleep(every_seconds)


async def _run_kalshi_wss(shutdown: Shutdown) -> None:
    """Run Kalshi WebSocket sync loop."""
    try:
        from apps.collector.jobs.kalshi_wss_sync import run_kalshi_wss_loop
        await run_kalshi_wss_loop(shutdown)
    except ImportError as e:
        logger.warning(f"Kalshi WSS module unavailable: {e}")
        await _run_kalshi_rest(shutdown)
    except Exception as e:
        logger.error(f"Kalshi WSS failed: {e}")
        await _run_kalshi_rest(shutdown)


async def _run_kalshi_rest(shutdown: Shutdown, interval: int = 60) -> None:
    """Run Kalshi REST polling as fallback."""
    from apps.collector.jobs.kalshi_sync import sync_once as kalshi_sync_once
    
    logger.info(f"Kalshi REST polling (interval={interval}s)")
    while not shutdown.is_set:
        try:
            await kalshi_sync_once()
        except Exception:
            logger.exception("Kalshi REST sync error")
        
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            continue


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


async def run_retention_loop(shutdown: Shutdown) -> None:
    """Background loop for data retention with table-specific policies."""
    from apps.collector.jobs.retention import run_retention_cleanup

    interval_seconds = settings.retention_run_interval_seconds
    logger.info(f"Retention loop starting (interval={interval_seconds}s)")
    
    # Initial cleanup on startup
    try:
        await run_retention_cleanup()
    except Exception:
        logger.exception("Error in initial retention cleanup")
    
    while not shutdown.is_set:
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
        
        try:
            await run_retention_cleanup()
        except Exception:
            logger.exception("Error in retention cleanup loop")


async def run_market_stats_loop(shutdown: Shutdown) -> None:
    """Background loop for calculating market volatility stats (for Z-score normalization)."""
    from apps.collector.jobs.market_stats import update_market_stats

    # Run 30 min after startup (allow data to accumulate), then every 6 hours
    logger.info("Market stats loop starting (interval=6h, initial delay=30m)")
    
    # Initial delay to let market data accumulate
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=1800)  # 30 min
        return  # Shutdown requested during delay
    except asyncio.TimeoutError:
        pass
    
    # Initial calculation
    try:
        await update_market_stats()
    except Exception:
        logger.exception("Error in initial market stats calculation")
    
    while not shutdown.is_set:
        try:
            # Wait 6 hours before next update
            await asyncio.wait_for(shutdown.wait(), timeout=21600)
            break
        except asyncio.TimeoutError:
            pass
        
        try:
            await update_market_stats()
        except Exception:
            logger.exception("Error in market stats loop")


async def run_model_scoring_loop(shutdown: Shutdown) -> None:
    """Background loop for resolved-market scoring diagnostics."""
    from apps.collector.jobs.model_scoring import update_daily_model_scoring

    interval_seconds = settings.model_scoring_interval_seconds
    initial_delay_seconds = settings.model_scoring_initial_delay_seconds
    logger.info(
        "Model scoring loop starting "
        f"(interval={interval_seconds}s, initial_delay={initial_delay_seconds}s)"
    )

    if initial_delay_seconds > 0:
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=initial_delay_seconds)
            return
        except asyncio.TimeoutError:
            pass

    try:
        await update_daily_model_scoring()
    except Exception:
        logger.exception("Error in initial model scoring run")

    while not shutdown.is_set:
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass

        try:
            await update_daily_model_scoring()
        except Exception:
            logger.exception("Error in model scoring loop")


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

    # Verify schema readiness before starting loops
    try:
        verify_schema_readiness()
    except Exception:
        logger.exception("Schema readiness verification failed.")
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
        bg_tasks.append(asyncio.create_task(run_alerts_loop(shutdown), name="alerts"))
        bg_tasks.append(asyncio.create_task(run_movers_cache_loop(shutdown), name="movers_cache"))
        bg_tasks.append(asyncio.create_task(run_rollups_loop(shutdown), name="rollups"))
        bg_tasks.append(asyncio.create_task(run_user_alerts_loop(shutdown), name="user_alerts"))
        bg_tasks.append(asyncio.create_task(run_volume_spikes_loop(shutdown), name="volume_spikes"))
        bg_tasks.append(asyncio.create_task(run_retention_loop(shutdown), name="retention"))
        bg_tasks.append(asyncio.create_task(run_market_stats_loop(shutdown), name="market_stats"))
        bg_tasks.append(asyncio.create_task(run_model_scoring_loop(shutdown), name="model_scoring"))

    # Create the main sync task
    main_task: Optional[asyncio.Task] = None
    
    try:
        if mode == "simulated":
            logger.info("Mode: simulated")
            main_task = asyncio.create_task(run_simulated(shutdown, every_seconds=interval), name="main_simulated")
        elif mode == "polymarket":
            logger.info("Mode: polymarket")
            main_task = asyncio.create_task(run_polymarket(shutdown, every_seconds=interval), name="main_polymarket")
        else:
            logger.info("Mode: live (all sources)")
            main_task = asyncio.create_task(run_live(shutdown, every_seconds=interval), name="main_live")
        
        # Wait for either main task completion OR shutdown signal
        # This ensures background tasks get cancelled even if main task blocks
        all_tasks = [main_task] + bg_tasks if bg_tasks else [main_task]
        
        # Create a shutdown waiter task
        shutdown_waiter = asyncio.create_task(shutdown.wait(), name="shutdown_waiter")
        
        # Wait for first completion: either shutdown signal or main task failure
        done, pending = await asyncio.wait(
            [main_task, shutdown_waiter],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # If shutdown was triggered, request stop
        if shutdown_waiter in done:
            logger.info("Shutdown signal received")
        elif main_task in done:
            # Main task completed (likely error) - check for exception
            try:
                main_task.result()
            except Exception as e:
                logger.error(f"Main task failed: {e}")
            
    except asyncio.CancelledError:
        logger.info("Main coroutine cancelled")
    finally:
        logger.info("Shutting down…")
        
        # Signal shutdown to all tasks
        shutdown.request()
        
        # Cancel all tasks and wait with timeout
        all_to_cancel = ([main_task] if main_task else []) + bg_tasks
        
        for task in all_to_cancel:
            if task and not task.done():
                task.cancel()
        
        # Give tasks a chance to clean up (5 second timeout)
        if all_to_cancel:
            try:
                await asyncio.wait(
                    [t for t in all_to_cancel if t and not t.done()],
                    timeout=5.0
                )
            except Exception:
                pass
        
        # Suppress CancelledError from cancelled tasks
        for task in all_to_cancel:
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.debug(f"Task {task.get_name()} ended with: {e}")

        try:
            db.close()
        except Exception:
            logger.exception("Failed to close DB pool cleanly.")


def main() -> None:
    # HACK: If Railway Service Name is 'dashboard', hijack this process to run Streamlit.
    # This fixes a deployment issue where the dashboard service uses the collector Dockerfile/command.
    service_name = os.getenv("RAILWAY_SERVICE_NAME", "")
    if service_name == "dashboard":
        print(f"Service is '{service_name}'! Hijacking execution to run Streamlit...")
        port = os.getenv("PORT", "8501")
        # Replace current process with streamlit
        os.execvp(
            "streamlit",
            ["streamlit", "run", "apps/dashboard/app.py", f"--server.port={port}", "--server.address=0.0.0.0"]
        )

    asyncio.run(_amain())


if __name__ == "__main__":
    main()
