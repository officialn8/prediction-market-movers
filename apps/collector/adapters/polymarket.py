"""
Polymarket API adapter using the Gamma API.

Endpoints:
- Markets: https://gamma-api.polymarket.com/markets
- Events: https://gamma-api.polymarket.com/events

The Gamma API is public and doesn't require authentication for read operations.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# Polymarket API endpoints
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# Rate limiting settings
DEFAULT_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.25  # 250ms between requests to be respectful


@dataclass
class PolymarketMarket:
    """Normalized market data from Polymarket."""
    condition_id: str
    question_id: str
    title: str
    slug: str
    category: Optional[str]
    end_date: Optional[str]
    active: bool
    closed: bool
    tokens: list[dict]  # List of {token_id, outcome, price}
    volume_24h: Optional[float]
    liquidity: Optional[float]

    @property
    def url(self) -> str:
        return f"https://polymarket.com/event/{self.slug}"

    @property
    def is_binary(self) -> bool:
        """Check if this is a simple YES/NO market."""
        return len(self.tokens) == 2


@dataclass
class TokenPrice:
    """Price data for a single token."""
    token_id: str
    price: float  # 0-1 probability
    spread: Optional[float] = None


class PolymarketAdapter:
    """
    Adapter for Polymarket Gamma API.
    
    Uses synchronous requests for simplicity since we run in a background job.
    """
    
    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        })
        self._last_request_time = 0.0
    
    def _rate_limit(self) -> None:
        """Simple rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()
    
    def _get(self, url: str, params: Optional[dict] = None) -> Any:
        """Make a GET request with rate limiting and error handling."""
        self._rate_limit()
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} fetching {url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching {url}: {e}")
            raise
    
    def _post_json(self, url: str, payload: Any) -> Any:
        """Make a POST request with JSON body (supports dict or list payloads)."""
        self._rate_limit()
        
        try:
            response = self.session.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout posting to {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} posting to {url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error posting to {url}: {e}")
            raise
    
    def fetch_markets(
        self,
        limit: int = 100,
        offset: int = 0,
        active: bool = True,
        closed: bool = False,
    ) -> list[PolymarketMarket]:
        """
        Fetch markets from the Gamma API.
        
        Args:
            limit: Max markets to fetch (API max is usually 100)
            offset: Pagination offset
            active: Include active markets
            closed: Include closed markets
            
        Returns:
            List of PolymarketMarket objects
        """
        url = f"{GAMMA_API_BASE}/markets"
        params = {
            "limit": limit,
            "offset": offset,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        
        logger.debug(f"Fetching markets: {params}")
        
        try:
            data = self._get(url, params)
        except Exception as e:
            logger.error(f"Failed to fetch markets: {e}")
            return []
        
        markets = []
        for item in data:
            try:
                market = self._parse_market(item)
                if market:
                    markets.append(market)
            except Exception as e:
                logger.warning(f"Failed to parse market {item.get('condition_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Fetched {len(markets)} markets from Polymarket")
        return markets
    
    def fetch_all_markets(
        self,
        max_markets: int = 500,
        active: bool = True,
    ) -> list[PolymarketMarket]:
        """
        Fetch all markets with pagination, utilizing the events endpoint for better metadata.
        
        Args:
            max_markets: Maximum total markets to fetch
            active: Only fetch active markets
            
        Returns:
            List of all fetched markets
        """
        return self.fetch_markets_via_events(limit=max_markets, active=active)

    
    def _parse_market(self, data: dict) -> Optional[PolymarketMarket]:
        """Parse raw API response into a PolymarketMarket object."""
        # Skip if no condition_id (required)
        condition_id = data.get("condition_id") or data.get("conditionId")
        if not condition_id:
            return None
        
        # Parse tokens
        tokens = []
        clob_token_ids = data.get("clobTokenIds") or data.get("clob_token_ids") or []
        outcomes = data.get("outcomes") or []
        outcome_prices = data.get("outcomePrices") or data.get("outcome_prices") or []

        # DEBUG: Log raw data for one item to check fields
        if not data.get("category") and not data.get("tags"):
             logger.info(f"DEBUG DATA: {data}")

        
        # Handle different response formats
        if isinstance(outcomes, str):
            # Sometimes outcomes is a JSON string like '["Yes", "No"]'
            import json
            try:
                outcomes = json.loads(outcomes)
            except:
                outcomes = ["Yes", "No"]
        
        if isinstance(outcome_prices, str):
            import json
            try:
                outcome_prices = json.loads(outcome_prices)
            except:
                outcome_prices = []
        
        if isinstance(clob_token_ids, str):
            import json
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except:
                clob_token_ids = []
        
        # Map outcomes to tokens
        for i, outcome in enumerate(outcomes):
            token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
            price = float(outcome_prices[i]) if i < len(outcome_prices) else 0.5
            
            if token_id:
                # Normalize outcome to YES/NO
                normalized_outcome = "YES" if outcome.lower() in ("yes", "true", "1") else "NO"
                tokens.append({
                    "token_id": token_id,
                    "outcome": normalized_outcome,
                    "price": price,
                })
        
        # Skip markets without tokens
        if not tokens:
            return None
        
        # Extract other fields
        question = data.get("question") or data.get("title") or "Unknown"
        question_id = data.get("question_id") or data.get("questionId") or condition_id
        slug = data.get("slug") or data.get("market_slug") or condition_id
        
        # Category/tags
        category = None
        tags = data.get("tags") or []
        if tags and isinstance(tags, list) and len(tags) > 0:
            category = tags[0].get("label") if isinstance(tags[0], dict) else str(tags[0])
        
        # Fallback to direct category field if tags didn't work
        if not category:
            category = data.get("category")

        # Fallback to events category
        if not category:
            events = data.get("events")
            if events and isinstance(events, list) and len(events) > 0:
                category = events[0].get("category")
                
        # Final Fallback: Heuristic based on title keywords
        if not category:
            title_lower = question.lower()
            if any(k in title_lower for k in ["trump", "biden", "election", "president", "senate", "house", "cabinet", "democrat", "republican", "gop"]):
                category = "Politics"
            elif any(k in title_lower for k in ["bitcoin", "ethereum", "crypto", "btc", "eth", "sol", "token", "nft", "coin"]):
                category = "Crypto"
            elif any(k in title_lower for k in ["nba", "nfl", "super bowl", "champion", "f1", "formula 1", "messi", "ronaldo", "league"]):
                category = "Sports"
            elif any(k in title_lower for k in ["fed ", "federal reserve", "yield", "rates", "inflation", "s&p", "stock", "ipo", "recession"]):
                category = "Finance"
            elif any(k in title_lower for k in ["movie", "grossing", "taylor swift", "kanye", "grammy", "oscar"]):
                category = "Culture"
            elif any(k in title_lower for k in ["ukraine", "russia", "israel", "gaza", "china", "invasion", "war ", "peace"]):
                category = "Geopolitics"

        # DEBUG: Log raw data for one item to check fields
        # if not data.get("category") and not data.get("tags"):
        #      logger.info(f"DEBUG DATA: {data}")
        
        # Volume and liquidity
        volume_24h = None
        liquidity = None
        try:
            volume_24h = float(data.get("volume24hr") or data.get("volume_24h") or 0)
        except (ValueError, TypeError):
            pass
        try:
            liquidity = float(data.get("liquidity") or 0)
        except (ValueError, TypeError):
            pass
        
        return PolymarketMarket(
            condition_id=condition_id,
            question_id=question_id,
            title=question,
            slug=slug,
            category=category,
            end_date=data.get("end_date_iso") or data.get("endDate"),
            active=data.get("active", True),
            closed=data.get("closed", False),
            tokens=tokens,
            volume_24h=volume_24h,
            liquidity=liquidity,
        )
    
    def fetch_prices_batch(self, token_ids: list[str]) -> dict[str, TokenPrice]:
        """
        Fetch current prices for multiple tokens.
        
        Uses the CLOB API's price endpoint for real-time prices.
        
        Args:
            token_ids: List of CLOB token IDs
            
        Returns:
            Dict mapping token_id -> TokenPrice
        """
        if not token_ids:
            return {}
        
        prices = {}
        
        # Fetch from CLOB API in batches
        batch_size = 50
        try:
            for i in range(0, len(token_ids), batch_size):
                batch = token_ids[i:i + batch_size]
                batch_prices = self._fetch_clob_prices(batch)
                prices.update(batch_prices)
        except Exception as e:
            logger.error(f"Error in batch price fetch loop: {e}")
            # Return partial results if we strictly need to, or just whatever we have.
            # We continue to return what we have.
            pass
        
        logger.info(f"Fetched prices for {len(prices)}/{len(token_ids)} tokens")
        return prices

    def fetch_price_single(self, token_id: str) -> Optional[TokenPrice]:
        """
        Fetch price for a single token (debugging/verification).
        """
        prices = self._fetch_clob_prices([token_id])
        return prices.get(token_id)
    
    def _fetch_clob_prices(self, token_ids: list[str], side: str = "BUY") -> dict[str, TokenPrice]:
        """Fetch prices from CLOB API using POST with correct payload format."""
        url = f"{CLOB_API_BASE}/prices"
        
        # CLOB API expects a LIST of objects with token_id and side
        # Format: [{"token_id": "123...", "side": "BUY"}, ...]
        payload = [{"token_id": tid, "side": side} for tid in token_ids]
        
        try:
            data = self._post_json(url, payload)
        except Exception as e:
            logger.warning(f"CLOB price fetch failed: {e}")
            return {}
        
        prices = {}
        
        # Response format is likely a list or dict mapping token_id -> price
        # Observed format: {"token_id": {"BUY": "0.55"}}
        if isinstance(data, dict):
            for token_id, val in data.items():
                try:
                    price_str = val
                    if isinstance(val, dict):
                        price_str = val.get(side)
                    
                    if price_str is None:
                        continue
                        
                    price = float(price_str)
                    price = max(0.0, min(1.0, price))
                    prices[token_id] = TokenPrice(token_id=token_id, price=price)
                except (ValueError, TypeError):
                    continue
        elif isinstance(data, list):
            # Handle list response format (older API versions sometimes did this)
            for item in data:
                if isinstance(item, dict):
                    token_id = item.get("token_id")
                    val = item.get("price")
                    
                    # It might be nested here too? Unlikely for list format but let's be safe
                    if isinstance(val, dict):
                        val = val.get(side)
                        
                    if token_id and val is not None:
                        try:
                            price = float(val)
                            price = max(0.0, min(1.0, price))
                            prices[token_id] = TokenPrice(token_id=token_id, price=price)
                        except (ValueError, TypeError):
                            continue
        
        return prices
    
    def fetch_orderbook(self, token_id: str) -> Optional[dict]:
        """
        Fetch orderbook for a token (to calculate spread).
        
        Returns:
            Dict with 'bids', 'asks', 'spread' if available
        """
        url = f"{CLOB_API_BASE}/book"
        params = {"token_id": token_id}
        
        try:
            data = self._get(url, params)
        except Exception:
            return None
        
        # Calculate spread from best bid/ask
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        
        spread = None
        if bids and asks:
            try:
                best_bid = float(bids[0].get("price", 0))
                best_ask = float(asks[0].get("price", 1))
                spread = best_ask - best_bid
            except (ValueError, TypeError, IndexError):
                pass
        
        return {
            "bids": bids,
            "asks": asks,
            "spread": spread,
        }
    
    
    def fetch_markets_via_events(self, limit: int = 100, active: bool = True) -> List[PolymarketMarket]:
        """
        Fetch markets by iterating through events, which is the recommended way
        to get all active markets and ensures better category/metadata coverage.
        """
        markets = []
        offset = 0
        page_size = 50  # Default page size for events
        
        while True:
            params = {
                "limit": page_size,
                "offset": offset,
                "closed": str(not active).lower(),
                "order": "id",
                "ascending": "false" # Newest first
            }
            
            try:
                # Use /events endpoint
                url = f"{GAMMA_API_BASE}/events"
                data = self._get(url, params)
                
                if not data:
                    break
                    
                # Process events
                for event in data:
                    event_category = event.get("category")
                    
                    # Each event has a 'markets' list
                    event_markets = event.get("markets", [])
                    for m_data in event_markets:
                        # Enrich market data with event metadata if needed
                        if not m_data.get("category") and event_category:
                            m_data["category"] = event_category
                        
                        # Add event tags to market tags if missing
                        if "tags" not in m_data and "tags" in event:
                             m_data["tags"] = event["tags"]
                             
                        # Parse
                        pm_market = self._parse_market(m_data)
                        if pm_market:
                            markets.append(pm_market)
                
                # Check if we reached the limit requested
                if len(markets) >= limit:
                    break
                
                if len(data) < page_size:
                    break
                    
                offset += page_size
                
            except Exception as e:
                logger.error(f"Error fetching events page at offset {offset}: {e}")
                break
                
        return markets[:limit]

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()


# Module-level singleton for convenience
_adapter: Optional[PolymarketAdapter] = None


def get_polymarket_adapter() -> PolymarketAdapter:
    """Get or create the Polymarket adapter singleton."""
    global _adapter
    if _adapter is None:
        _adapter = PolymarketAdapter()
    return _adapter
