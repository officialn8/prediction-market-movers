from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional, Any, Union

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
    """Trade execution event."""
    token_id: str
    price: float
    size: float
    side: Literal["BUY", "SELL"]
    timestamp: datetime

def parse_wss_message(raw: dict[str, Any]) -> list[Union[PriceUpdate, BookUpdate, TradeEvent]]:
    """
    Parse raw WSS message into typed event.
    
    Example input:
    [
        {
            "event_type": "price_change",
            "asset_id": "...",
            "price": "0.55",
            "timestamp": 1234567890
        }
    ]
    Note: The WSS actually returns a list of events usually.
    """
    # This function expects a SINGLE event dictionary, so the loop should happen in the client.
    
    event_type = raw.get("event_type")
    
    # New format: "price_changes": [{"asset_id": "...", "price": "..."}]
    if event_type == "price_change" and "price_changes" in raw:
        updates = []
        try:
            for change in raw["price_changes"]:
                updates.append(PriceUpdate(
                    token_id=change["asset_id"],
                    price=float(change["price"]),
                    timestamp=datetime.fromtimestamp(int(raw["timestamp"]) / 1000)
                ))
            return updates
        except (KeyError, ValueError):
            return []

    # Old format / other events fallback (optional, if they still support it)
    if event_type == "price_change" and "asset_id" in raw:
         try:
            return [PriceUpdate(
                token_id=raw["asset_id"],
                price=float(raw["price"]),
                timestamp=datetime.fromtimestamp(int(raw["timestamp"]) / 1000)
            )]
         except (KeyError, ValueError):
            return []

    if event_type == "book":
        # Handle book updates if we subscribe to them
        # For now we are primarily focused on price_change for movers
        pass

    return []

