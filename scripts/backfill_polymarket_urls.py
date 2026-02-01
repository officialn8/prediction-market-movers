import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.collector.adapters.polymarket import PolymarketAdapter
from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries


def _backfill_for_active(adapter: PolymarketAdapter, active: bool, logger: logging.Logger) -> int:
    label = "active" if active else "closed"
    logger.info(f"Fetching {label} markets from Polymarket API...")
    markets = adapter.fetch_all_markets(max_markets=5000, active=active)
    logger.info(f"Fetched {len(markets)} {label} markets.")

    updated_count = 0
    for pm_market in markets:
        if not pm_market.url:
            continue

        MarketQueries.upsert_market(
            source="polymarket",
            source_id=pm_market.condition_id,
            title=pm_market.title,
            category=pm_market.category,
            end_date=pm_market.end_date,
            status="active" if pm_market.active and not pm_market.closed else "closed",
            url=pm_market.url,
        )
        updated_count += 1

        if updated_count % 200 == 0:
            logger.info(f"Updated {updated_count} {label} markets...")

    return updated_count


def main() -> None:
    """
    One-off backfill to rewrite Polymarket market URLs to canonical event URLs.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("backfill_polymarket_urls")

    logger.info("Starting Polymarket URL backfill...")

    db = get_db_pool()
    if not db.health_check():
        logger.error("Database not available")
        return

    adapter = PolymarketAdapter()
    try:
        active_updates = _backfill_for_active(adapter, True, logger)
        closed_updates = _backfill_for_active(adapter, False, logger)
        logger.info(
            f"Backfill complete. Updated {active_updates} active and "
            f"{closed_updates} closed markets."
        )
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
    finally:
        adapter.close()


if __name__ == "__main__":
    main()
