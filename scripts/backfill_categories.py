
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.collector.adapters.polymarket import PolymarketAdapter
from apps.collector.jobs.polymarket_sync import sync_markets
from packages.core.storage import get_db_pool

def main():
    """
    Backfill categories for all Polymarket markets.
    This effectively runs a full sync, which updates existing records with the latest
    category data from the API.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("backfill_categories")
    
    logger.info("Starting category backfill...")
    
    # Check DB connection
    db = get_db_pool()
    if not db.health_check():
        logger.error("Database not available")
        return

    adapter = PolymarketAdapter()
    
    try:
        # Fetch markets using the adapter directly (now uses /events under the hood)
        logger.info("Fetching markets from Polymarket API...")
        # Fetch active markets first
        markets = adapter.fetch_all_markets(max_markets=5000, active=True)
        logger.info(f"Fetched {len(markets)} markets.")
        
        updated_count = 0
        from packages.core.storage.queries import MarketQueries
        
        for pm_market in markets:
            if not pm_market.category:
                continue
                
            # We only care about updating the category for existing markets or upserting new ones
            # Upsert will handle both. We don't need to sync tokens/prices for this backfill.
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
            
            if updated_count % 100 == 0:
                logger.info(f"Updated {updated_count} markets...")

        logger.info(f"Successfully backfilled categories for {updated_count} markets.")
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
    finally:
        adapter.close()

if __name__ == "__main__":
    main()
