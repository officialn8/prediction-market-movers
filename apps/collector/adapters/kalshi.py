"""
Kalshi API adapter.

API Base: https://api.elections.kalshi.com/trade-api/v2
(Despite 'elections' subdomain, serves ALL Kalshi markets)

No authentication required for public market data endpoints.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# Kalshi API endpoints
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Rate limiting
DEFAULT_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.25  # 250ms between requests (safer for Kalshi)
MAX_RETRIES = 3
BACKOFF_BASE = 2.0  # Exponential backoff base (2s, 4s, 8s)


@dataclass
class KalshiMarket:
    """Normalized market data from Kalshi."""
    ticker: str  # Market ticker (e.g., KXHIGHNY-25JAN31-T42)
    event_ticker: str  # Parent event
    title: str
    subtitle: str
    status: str  # active, closed, settled
    yes_bid: int  # Price in cents (1-99)
    yes_ask: int
    last_price: int
    volume: int
    volume_24h: int
    open_interest: int
    close_time: Optional[str]
    expiration_time: Optional[str]
    result: Optional[str]
    
    @property
    def url(self) -> str:
        """Generate Kalshi market URL."""
        # Extract series from ticker (first part before date)
        parts = self.ticker.split('-')
        if len(parts) >= 1:
            series = parts[0]
            return f"https://kalshi.com/markets/{series.lower()}"
        return "https://kalshi.com"
    
    @property
    def mid_price(self) -> float:
        """Calculate mid price as decimal (0-1)."""
        if self.yes_bid > 0 and self.yes_ask > 0 and self.yes_ask < 100:
            return ((self.yes_bid + self.yes_ask) / 2) / 100
        elif self.last_price > 0:
            return self.last_price / 100
        return 0.5  # Default to 50% if no price data
    
    @property
    def spread(self) -> Optional[float]:
        """Calculate bid-ask spread in cents."""
        if self.yes_bid > 0 and self.yes_ask > 0 and self.yes_ask < 100:
            return self.yes_ask - self.yes_bid
        return None


@dataclass
class KalshiEvent:
    """Event (group of related markets) from Kalshi."""
    ticker: str
    title: str
    category: str
    status: str
    markets: list[KalshiMarket]


class KalshiAdapter:
    """
    Adapter for Kalshi Trading API (public endpoints).
    
    Uses synchronous requests for simplicity since we run in background jobs.
    No authentication required for market data.
    """
    
    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PMM-Collector/1.0",
            "Accept": "application/json",
        })
        self._last_request_time = 0.0
    
    def _rate_limit(self) -> None:
        """Simple rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()
    
    def _get(self, endpoint: str, params: Optional[dict] = None) -> Any:
        """Make a GET request with rate limiting, retries, and exponential backoff."""
        url = f"{KALSHI_API_BASE}{endpoint}"
        
        for attempt in range(MAX_RETRIES + 1):
            self._rate_limit()
            
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                logger.error(f"Timeout fetching {url}")
                raise
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429 and attempt < MAX_RETRIES:
                    # Rate limited - exponential backoff
                    wait_time = BACKOFF_BASE ** (attempt + 1)
                    logger.warning(f"Rate limited (429), waiting {wait_time:.1f}s before retry {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(wait_time)
                    continue
                logger.error(f"HTTP error {e.response.status_code} fetching {url}: {e}")
                raise
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                raise
        
        # Should never reach here, but just in case
        raise requests.exceptions.HTTPError(f"Max retries exceeded for {url}")
    
    def get_exchange_status(self) -> dict:
        """Check if Kalshi exchange is operational."""
        return self._get("/exchange/status")
    
    def get_markets(
        self,
        limit: int = 200,
        status: str = "open",
        series_ticker: Optional[str] = None,
        event_ticker: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> tuple[list[KalshiMarket], Optional[str]]:
        """
        Fetch markets from Kalshi.
        
        Returns:
            Tuple of (markets, next_cursor)
        """
        params = {
            "limit": min(limit, 200),  # Max 200 per request
            "status": status,
        }
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if cursor:
            params["cursor"] = cursor
        
        data = self._get("/markets", params)
        
        markets = []
        for m in data.get("markets", []):
            try:
                market = KalshiMarket(
                    ticker=m.get("ticker", ""),
                    event_ticker=m.get("event_ticker", ""),
                    title=m.get("title", ""),
                    subtitle=m.get("subtitle", ""),
                    status=m.get("status", "unknown"),
                    yes_bid=m.get("yes_bid", 0) or 0,
                    yes_ask=m.get("yes_ask", 0) or 0,
                    last_price=m.get("last_price", 0) or 0,
                    volume=m.get("volume", 0) or 0,
                    volume_24h=m.get("volume_24h", 0) or 0,
                    open_interest=m.get("open_interest", 0) or 0,
                    close_time=m.get("close_time"),
                    expiration_time=m.get("expiration_time"),
                    result=m.get("result"),
                )
                markets.append(market)
            except Exception as e:
                logger.warning(f"Failed to parse market {m.get('ticker')}: {e}")
                continue
        
        next_cursor = data.get("cursor")
        return markets, next_cursor
    
    def get_all_markets(
        self,
        status: str = "open",
        max_markets: int = 5000,
    ) -> list[KalshiMarket]:
        """
        Fetch all markets with pagination.
        
        Args:
            status: Filter by status (open, closed, settled)
            max_markets: Maximum markets to fetch (safety limit)
        """
        all_markets = []
        cursor = None
        
        while len(all_markets) < max_markets:
            markets, cursor = self.get_markets(
                limit=200,
                status=status,
                cursor=cursor,
            )
            
            if not markets:
                break
            
            all_markets.extend(markets)
            logger.info(f"Fetched {len(all_markets)} Kalshi markets so far...")
            
            if not cursor:
                break
        
        logger.info(f"Total Kalshi markets fetched: {len(all_markets)}")
        return all_markets
    
    def get_market(self, ticker: str) -> Optional[KalshiMarket]:
        """Fetch a specific market by ticker."""
        try:
            data = self._get(f"/markets/{ticker}")
            m = data.get("market", {})
            
            return KalshiMarket(
                ticker=m.get("ticker", ""),
                event_ticker=m.get("event_ticker", ""),
                title=m.get("title", ""),
                subtitle=m.get("subtitle", ""),
                status=m.get("status", "unknown"),
                yes_bid=m.get("yes_bid", 0) or 0,
                yes_ask=m.get("yes_ask", 0) or 0,
                last_price=m.get("last_price", 0) or 0,
                volume=m.get("volume", 0) or 0,
                volume_24h=m.get("volume_24h", 0) or 0,
                open_interest=m.get("open_interest", 0) or 0,
                close_time=m.get("close_time"),
                expiration_time=m.get("expiration_time"),
                result=m.get("result"),
            )
        except Exception as e:
            logger.error(f"Failed to get market {ticker}: {e}")
            return None
    
    def get_orderbook(self, ticker: str) -> dict:
        """Get orderbook for a market."""
        return self._get(f"/markets/{ticker}/orderbook")
    
    def get_events(
        self,
        limit: int = 200,
        status: str = "open",
        with_nested_markets: bool = False,
        cursor: Optional[str] = None,
    ) -> tuple[list[dict], Optional[str]]:
        """
        Fetch events from Kalshi.
        
        Returns:
            Tuple of (events, next_cursor)
        """
        params = {
            "limit": min(limit, 200),
            "status": status,
            "with_nested_markets": with_nested_markets,
        }
        if cursor:
            params["cursor"] = cursor
        
        data = self._get("/events", params)
        events = data.get("events", [])
        next_cursor = data.get("cursor")
        
        return events, next_cursor
    
    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> tuple[list[dict], Optional[str]]:
        """
        Fetch recent trades.
        
        Returns:
            Tuple of (trades, next_cursor)
        """
        params = {"limit": min(limit, 1000)}
        if ticker:
            params["ticker"] = ticker
        if cursor:
            params["cursor"] = cursor
        
        data = self._get("/markets/trades", params)
        trades = data.get("trades", [])
        next_cursor = data.get("cursor")
        
        return trades, next_cursor
    
    def close(self) -> None:
        """Close the session."""
        self.session.close()


# Convenience function for quick testing
def fetch_kalshi_markets(limit: int = 50) -> list[KalshiMarket]:
    """Quick function to fetch Kalshi markets."""
    adapter = KalshiAdapter()
    try:
        markets, _ = adapter.get_markets(limit=limit, status="open")
        return markets
    finally:
        adapter.close()


if __name__ == "__main__":
    # Quick test
    logging.basicConfig(level=logging.INFO)
    
    adapter = KalshiAdapter()
    
    # Check exchange status
    status = adapter.get_exchange_status()
    print(f"Exchange status: {status}")
    
    # Fetch some markets
    markets, cursor = adapter.get_markets(limit=10, status="open")
    print(f"\nFound {len(markets)} markets:")
    for m in markets[:5]:
        print(f"  {m.ticker}: {m.title[:50]}... @ {m.mid_price:.2%}")
    
    adapter.close()
