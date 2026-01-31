from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional, Any, Union
import logging

logger = logging.getLogger(__name__)


@dataclass
class PriceUpdate:
    """Parsed price update from WSS."""
    token_id: str
    price: float
    timestamp: datetime


@dataclass
class BookUpdate:
    """Order book update with spread calculation."""
    token_id: str
    best_bid: float
    best_ask: float
    spread: float
    timestamp: datetime


@dataclass
class TradeEvent:
    """
    Trade execution event from last_trade_price message.
    
    Contains SIZE for real-time volume calculation!
    This is the key to accurate volume without REST polling.
    """
    token_id: str
    price: float
    size: float
    side: Literal["BUY", "SELL"]
    fee_rate_bps: Optional[int]  # Basis points fee
    timestamp: datetime


@dataclass
class SpreadUpdate:
    """
    Spread update from best_bid_ask message.
    
    Requires custom_feature_enabled=true in subscription.
    """
    token_id: str
    best_bid: float
    best_ask: float
    spread: float
    timestamp: datetime


@dataclass
class MarketResolved:
    """
    Market resolution event.
    
    Requires custom_feature_enabled=true in subscription.
    """
    market_id: str
    outcome: str  # "YES" or "NO"
    winning_token_id: str
    timestamp: datetime


@dataclass
class NewMarket:
    """
    New market creation event.
    
    Requires custom_feature_enabled=true in subscription.
    """
    market_id: str
    condition_id: str
    tokens: list[dict]  # [{token_id, outcome}]
    timestamp: datetime


def parse_wss_message(raw: dict[str, Any]) -> list[Union[PriceUpdate, BookUpdate, TradeEvent, SpreadUpdate, MarketResolved, NewMarket]]:
    """
    Parse raw WSS message into typed events.
    
    Polymarket WSS message types:
    - price_change: Best bid/ask price updates
    - last_trade_price: Individual trades with SIZE (for volume!)
    - book: Full order book snapshot
    - best_bid_ask: Spread updates (requires custom_feature_enabled)
    - new_market: Market creation (requires custom_feature_enabled)
    - market_resolved: Resolution events (requires custom_feature_enabled)
    
    The `last_trade_price` message is KEY - it contains trade size
    which allows real-time volume calculation without REST polling!
    """
    event_type = raw.get("event_type")
    
    # =========================================================================
    # PRICE_CHANGE - Best bid/ask updates
    # =========================================================================
    if event_type == "price_change" and "price_changes" in raw:
        updates = []
        try:
            ts = datetime.fromtimestamp(int(raw["timestamp"]) / 1000)
            for change in raw["price_changes"]:
                updates.append(PriceUpdate(
                    token_id=change["asset_id"],
                    price=float(change["price"]),
                    timestamp=ts
                ))
            return updates
        except (KeyError, ValueError) as e:
            logger.debug(f"Failed to parse price_change: {e}")
            return []

    # Old format fallback
    if event_type == "price_change" and "asset_id" in raw:
        try:
            return [PriceUpdate(
                token_id=raw["asset_id"],
                price=float(raw["price"]),
                timestamp=datetime.fromtimestamp(int(raw["timestamp"]) / 1000)
            )]
        except (KeyError, ValueError):
            return []

    # =========================================================================
    # LAST_TRADE_PRICE - Individual trades WITH SIZE (for volume calculation!)
    # =========================================================================
    if event_type == "last_trade_price":
        try:
            ts = datetime.fromtimestamp(int(raw["timestamp"]) / 1000)
            
            # Can contain multiple trades in "last_trade_prices" array
            if "last_trade_prices" in raw:
                trades = []
                for trade in raw["last_trade_prices"]:
                    trades.append(TradeEvent(
                        token_id=trade["asset_id"],
                        price=float(trade["price"]),
                        size=float(trade.get("size", 0)),
                        side=trade.get("side", "BUY").upper(),
                        fee_rate_bps=trade.get("fee_rate_bps"),
                        timestamp=ts
                    ))
                return trades
            
            # Single trade format
            return [TradeEvent(
                token_id=raw["asset_id"],
                price=float(raw["price"]),
                size=float(raw.get("size", 0)),
                side=raw.get("side", "BUY").upper(),
                fee_rate_bps=raw.get("fee_rate_bps"),
                timestamp=datetime.fromtimestamp(int(raw["timestamp"]) / 1000)
            )]
        except (KeyError, ValueError) as e:
            logger.debug(f"Failed to parse last_trade_price: {e}")
            return []

    # =========================================================================
    # BEST_BID_ASK - Spread updates (requires custom_feature_enabled)
    # =========================================================================
    if event_type == "best_bid_ask":
        try:
            ts = datetime.fromtimestamp(int(raw["timestamp"]) / 1000)
            
            if "changes" in raw:
                updates = []
                for change in raw["changes"]:
                    best_bid = float(change.get("best_bid", 0))
                    best_ask = float(change.get("best_ask", 0))
                    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0
                    
                    updates.append(SpreadUpdate(
                        token_id=change["asset_id"],
                        best_bid=best_bid,
                        best_ask=best_ask,
                        spread=spread,
                        timestamp=ts
                    ))
                return updates
        except (KeyError, ValueError) as e:
            logger.debug(f"Failed to parse best_bid_ask: {e}")
            return []

    # =========================================================================
    # BOOK - Full order book snapshot
    # =========================================================================
    if event_type == "book":
        try:
            bids = raw.get("bids", [])
            asks = raw.get("asks", [])
            
            best_bid = float(bids[0]["price"]) if bids else 0
            best_ask = float(asks[0]["price"]) if asks else 0
            spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0
            
            return [BookUpdate(
                token_id=raw.get("asset_id", raw.get("market", "")),
                best_bid=best_bid,
                best_ask=best_ask,
                spread=spread,
                timestamp=datetime.fromtimestamp(int(raw.get("timestamp", 0)) / 1000)
            )]
        except (KeyError, ValueError, IndexError) as e:
            logger.debug(f"Failed to parse book: {e}")
            return []

    # =========================================================================
    # NEW_MARKET - Market creation (requires custom_feature_enabled)
    # =========================================================================
    if event_type == "new_market":
        try:
            return [NewMarket(
                market_id=raw.get("market_id", raw.get("market", "")),
                condition_id=raw.get("condition_id", ""),
                tokens=raw.get("tokens", []),
                timestamp=datetime.fromtimestamp(int(raw.get("timestamp", 0)) / 1000)
            )]
        except (KeyError, ValueError) as e:
            logger.debug(f"Failed to parse new_market: {e}")
            return []

    # =========================================================================
    # MARKET_RESOLVED - Resolution events (requires custom_feature_enabled)
    # =========================================================================
    if event_type == "market_resolved":
        try:
            return [MarketResolved(
                market_id=raw.get("market_id", raw.get("market", "")),
                outcome=raw.get("outcome", ""),
                winning_token_id=raw.get("winning_token_id", ""),
                timestamp=datetime.fromtimestamp(int(raw.get("timestamp", 0)) / 1000)
            )]
        except (KeyError, ValueError) as e:
            logger.debug(f"Failed to parse market_resolved: {e}")
            return []

    # Unknown event type - log for discovery
    if event_type:
        logger.debug(f"Unknown WSS event_type: {event_type}")
    
    return []

