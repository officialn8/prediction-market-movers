"""
Kalshi sync job - fetches and stores market data.

TODO: Implement in Phase 3 (after Polymarket is working)
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def sync_once() -> None:
    """
    Run one Kalshi sync cycle (placeholder).
    
    TODO: Implement similar to polymarket_sync:
    - sync_markets() for market metadata
    - sync_prices() for price snapshots
    """
    # Silently skip for now - Kalshi implementation pending
    pass


async def sync_kalshi() -> int:
    """
    Legacy entry point - sync markets and prices from Kalshi.
    
    Returns:
        Number of markets synced
    """
    logger.debug("Kalshi sync: Not implemented yet")
    return 0
