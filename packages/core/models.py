"""
Pydantic models for Market, Token, and Snapshot data.
Used for validation and serialization throughout the application.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class MarketSource(str, Enum):
    """Supported prediction market sources."""
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"


class MarketStatus(str, Enum):
    """Market status options."""
    ACTIVE = "active"
    CLOSED = "closed"
    RESOLVED = "resolved"


class TokenOutcome(str, Enum):
    """Token outcome types."""
    YES = "YES"
    NO = "NO"


# =============================================================================
# Base Models
# =============================================================================

class MarketBase(BaseModel):
    """Base market model for creation/updates."""
    source: MarketSource
    source_id: str = Field(..., min_length=1, max_length=255)
    title: str = Field(..., min_length=1)
    category: Optional[str] = Field(default=None, max_length=128)
    status: MarketStatus = MarketStatus.ACTIVE
    url: Optional[str] = None


class Market(MarketBase):
    """Full market model with database fields."""
    market_id: UUID
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class MarketTokenBase(BaseModel):
    """Base token model."""
    market_id: UUID
    outcome: TokenOutcome
    symbol: Optional[str] = Field(default=None, max_length=128)
    source_token_id: Optional[str] = Field(default=None, max_length=255)


class MarketToken(MarketTokenBase):
    """Full token model with database fields."""
    token_id: UUID
    created_at: datetime
    
    class Config:
        from_attributes = True


class SnapshotBase(BaseModel):
    """Base snapshot model for inserts."""
    token_id: UUID
    price: Decimal = Field(..., ge=0, le=1)
    volume_24h: Optional[Decimal] = Field(default=None, ge=0)
    spread: Optional[Decimal] = Field(default=None, ge=0)
    
    @field_validator("price")
    @classmethod
    def validate_price(cls, v: Decimal) -> Decimal:
        """Ensure price is a valid probability (0-1)."""
        if v < 0 or v > 1:
            raise ValueError("Price must be between 0 and 1")
        return round(v, 6)


class Snapshot(SnapshotBase):
    """Full snapshot model with timestamp."""
    ts: datetime
    
    class Config:
        from_attributes = True


# =============================================================================
# Aggregated / View Models
# =============================================================================

class TokenWithPrice(BaseModel):
    """Token with its latest price info."""
    token_id: UUID
    outcome: TokenOutcome
    symbol: Optional[str]
    latest_price: Optional[Decimal]
    latest_volume: Optional[Decimal]


class MarketWithTokens(Market):
    """Market with all its tokens and latest prices."""
    tokens: list[TokenWithPrice] = Field(default_factory=list)


class PriceMover(BaseModel):
    """Model for top movers display."""
    token_id: UUID
    market_id: UUID
    title: str
    source: MarketSource
    category: Optional[str]
    outcome: TokenOutcome
    latest_price: Decimal
    old_price: Decimal
    pct_change: Decimal
    latest_ts: datetime
    url: Optional[str] = None
    
    @property
    def price_direction(self) -> str:
        """Return 'up', 'down', or 'flat'."""
        if self.pct_change > 0:
            return "up"
        elif self.pct_change < 0:
            return "down"
        return "flat"
    
    @property
    def formatted_change(self) -> str:
        """Return formatted percentage points change string."""
        sign = "+" if self.pct_change > 0 else ""
        return f"{sign}{self.pct_change:.2f}pp"


# =============================================================================
# API Response Models (for adapters)
# =============================================================================

class PolymarketMarketData(BaseModel):
    """Model for Polymarket API response data."""
    condition_id: str
    question: str
    outcomes: list[str]
    tokens: list[dict]
    category: Optional[str] = None
    end_date_iso: Optional[str] = None
    active: bool = True
    
    def to_market_base(self) -> MarketBase:
        """Convert to canonical MarketBase model."""
        return MarketBase(
            source=MarketSource.POLYMARKET,
            source_id=self.condition_id,
            title=self.question,
            category=self.category,
            status=MarketStatus.ACTIVE if self.active else MarketStatus.CLOSED,
        )


class KalshiMarketData(BaseModel):
    """Model for Kalshi API response data."""
    ticker: str
    title: str
    category: Optional[str] = None
    status: str = "active"
    yes_price: Optional[Decimal] = None
    no_price: Optional[Decimal] = None
    volume_24h: Optional[Decimal] = None
    
    def to_market_base(self) -> MarketBase:
        """Convert to canonical MarketBase model."""
        status_map = {
            "active": MarketStatus.ACTIVE,
            "closed": MarketStatus.CLOSED,
            "settled": MarketStatus.RESOLVED,
        }
        return MarketBase(
            source=MarketSource.KALSHI,
            source_id=self.ticker,
            title=self.title,
            category=self.category,
            status=status_map.get(self.status.lower(), MarketStatus.ACTIVE),
        )


# =============================================================================
# Analytics & Alerts Models
# =============================================================================

class MoverCache(BaseModel):
    """Precomputed top mover record."""
    as_of_ts: datetime
    window_seconds: int
    token_id: UUID
    price_now: Decimal
    price_then: Decimal
    move_pp: Decimal
    abs_move_pp: Decimal
    rank: int
    quality_score: Optional[Decimal] = None
    
    class Config:
        from_attributes = True


class Alert(BaseModel):
    """Alert record for significant movements."""
    alert_id: UUID
    created_at: datetime
    token_id: UUID
    window_seconds: int
    move_pp: Decimal
    threshold_pp: Decimal
    reason: str
    alert_type: Optional[str] = None
    volume_spike_ratio: Optional[Decimal] = None
    acknowledged_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

