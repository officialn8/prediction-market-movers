"""
Kalshi API adapter.

TODO: Implement in Phase 2

API Docs: https://trading-api.readme.io/reference/
"""

import logging
from typing import Optional

import httpx

from packages.core.models import KalshiMarketData
from packages.core.settings import settings

logger = logging.getLogger(__name__)

# Kalshi API endpoints
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiAdapter:
    """
    Adapter for Kalshi Trading API.
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        self.api_key = api_key or settings.kalshi_api_key
        self.api_secret = api_secret or settings.kalshi_api_secret
        self.client = httpx.AsyncClient(
            base_url=KALSHI_API_BASE,
            timeout=30.0,
        )
    
    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()
    
    async def get_markets(
        self,
        limit: int = 100,
        status: str = "open",
    ) -> list[KalshiMarketData]:
        """
        Fetch markets from Kalshi.
        
        TODO: Implement actual API call with authentication
        """
        logger.info("KalshiAdapter.get_markets: Not implemented")
        return []
    
    async def get_market(self, ticker: str) -> Optional[KalshiMarketData]:
        """
        Fetch a specific market by ticker.
        
        TODO: Implement actual API call
        """
        logger.info(f"KalshiAdapter.get_market({ticker}): Not implemented")
        return None

