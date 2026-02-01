"""
Arbitrage router - Cross-platform arbitrage detection endpoints.
"""

from datetime import datetime
from typing import Optional, List
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from apps.api.routers.auth import get_current_user
from packages.core.storage.queries import ArbitrageQueries

router = APIRouter()


# ============================================================================
# Models
# ============================================================================

class ArbitrageOpportunityResponse(BaseModel):
    """Active arbitrage opportunity."""
    opportunity_id: str
    detected_at: datetime
    arbitrage_type: str  # 'YES_NO' or 'NO_YES'
    
    # Market info
    polymarket_title: str
    kalshi_title: str
    polymarket_url: Optional[str]
    kalshi_url: Optional[str]
    category: Optional[str]
    
    # Prices
    polymarket_yes_price: float
    polymarket_no_price: float
    kalshi_yes_price: float
    kalshi_no_price: float
    
    # Profitability
    total_cost: float
    profit_margin: float
    profit_percentage: float
    
    # Volume
    polymarket_volume_24h: Optional[float]
    kalshi_volume_24h: Optional[float]
    min_volume_24h: Optional[float]
    
    # Trade recommendation
    trade_recommendation: Optional[str]
    
    # Expiry
    expires_at: Optional[datetime]
    seconds_until_expiry: Optional[int]


class MarketPairResponse(BaseModel):
    """Market pair for arbitrage tracking."""
    pair_id: str
    polymarket_market_id: str
    kalshi_market_id: str
    matching_method: str
    similarity_score: Optional[float]
    notes: Optional[str]
    active: bool
    created_at: datetime
    updated_at: datetime


class CreatePairRequest(BaseModel):
    """Request to create a market pair."""
    polymarket_market_id: str = Field(..., description="UUID of Polymarket market")
    kalshi_market_id: str = Field(..., description="UUID of Kalshi market")
    matching_method: str = Field(default="manual", description="'manual', 'fuzzy', or 'exact'")
    similarity_score: Optional[float] = Field(None, ge=0, le=1)
    notes: Optional[str] = None


class SimilarMarketResponse(BaseModel):
    """Market with similarity score for fuzzy matching."""
    market_id: str
    source: str
    source_id: str
    title: str
    category: Optional[str]
    url: Optional[str]
    similarity_score: float


# ============================================================================
# Public Endpoints (rate limited in production)
# ============================================================================

@router.get("/opportunities", response_model=List[ArbitrageOpportunityResponse])
async def get_active_opportunities(
    min_profit_pct: float = Query(default=0.2, description="Minimum profit percentage"),
    min_volume: float = Query(default=100, description="Minimum 24h volume"),
    limit: int = Query(default=50, le=100),
    current_user: dict = Depends(get_current_user),
):
    """
    Get currently active arbitrage opportunities.
    
    Opportunities are detected when the combined cost of buying YES on one
    platform and NO on another platform is less than $1.
    """
    opportunities = ArbitrageQueries.get_active_opportunities(
        min_profit_pct=Decimal(str(min_profit_pct)),
        min_volume=Decimal(str(min_volume)),
        limit=limit,
    )
    
    return [
        ArbitrageOpportunityResponse(
            opportunity_id=str(o["opportunity_id"]),
            detected_at=o["detected_at"],
            arbitrage_type=o["arbitrage_type"],
            polymarket_title=o.get("polymarket_title", "Unknown"),
            kalshi_title=o.get("kalshi_title", "Unknown"),
            polymarket_url=o.get("polymarket_url"),
            kalshi_url=o.get("kalshi_url"),
            category=o.get("category"),
            polymarket_yes_price=float(o["polymarket_yes_price"]),
            polymarket_no_price=float(o["polymarket_no_price"]),
            kalshi_yes_price=float(o["kalshi_yes_price"]),
            kalshi_no_price=float(o["kalshi_no_price"]),
            total_cost=float(o["total_cost"]),
            profit_margin=float(o["profit_margin"]),
            profit_percentage=float(o["profit_percentage"]),
            polymarket_volume_24h=float(o["polymarket_volume_24h"]) if o.get("polymarket_volume_24h") else None,
            kalshi_volume_24h=float(o["kalshi_volume_24h"]) if o.get("kalshi_volume_24h") else None,
            min_volume_24h=float(o["min_volume_24h"]) if o.get("min_volume_24h") else None,
            trade_recommendation=o.get("trade_recommendation"),
            expires_at=o.get("expires_at"),
            seconds_until_expiry=o.get("seconds_until_expiry"),
        )
        for o in opportunities
    ]


@router.get("/history", response_model=List[ArbitrageOpportunityResponse])
async def get_opportunity_history(
    pair_id: Optional[str] = None,
    hours: int = Query(default=24, le=168),
    limit: int = Query(default=100, le=500),
    current_user: dict = Depends(get_current_user),
):
    """Get historical arbitrage opportunities."""
    history = ArbitrageQueries.get_opportunity_history(
        pair_id=UUID(pair_id) if pair_id else None,
        hours=hours,
        limit=limit,
    )
    
    return [
        ArbitrageOpportunityResponse(
            opportunity_id=str(o["opportunity_id"]),
            detected_at=o["detected_at"],
            arbitrage_type=o["arbitrage_type"],
            polymarket_title=o.get("polymarket_title", "Unknown"),
            kalshi_title=o.get("kalshi_title", "Unknown"),
            polymarket_url=o.get("polymarket_url"),
            kalshi_url=o.get("kalshi_url"),
            category=o.get("category"),
            polymarket_yes_price=float(o["polymarket_yes_price"]),
            polymarket_no_price=float(o["polymarket_no_price"]),
            kalshi_yes_price=float(o["kalshi_yes_price"]),
            kalshi_no_price=float(o["kalshi_no_price"]),
            total_cost=float(o["total_cost"]),
            profit_margin=float(o["profit_margin"]),
            profit_percentage=float(o["profit_percentage"]),
            polymarket_volume_24h=float(o["polymarket_volume_24h"]) if o.get("polymarket_volume_24h") else None,
            kalshi_volume_24h=float(o["kalshi_volume_24h"]) if o.get("kalshi_volume_24h") else None,
            min_volume_24h=float(o["min_volume_24h"]) if o.get("min_volume_24h") else None,
            trade_recommendation=o.get("trade_recommendation"),
            expires_at=o.get("expires_at"),
            seconds_until_expiry=o.get("seconds_until_expiry"),
        )
        for o in history
    ]


# ============================================================================
# Admin Endpoints (Pro+ users)
# ============================================================================

@router.get("/pairs", response_model=List[MarketPairResponse])
async def get_market_pairs(
    current_user: dict = Depends(get_current_user),
):
    """Get all configured market pairs."""
    pairs = ArbitrageQueries.get_active_pairs()
    
    return [
        MarketPairResponse(
            pair_id=str(p["pair_id"]),
            polymarket_market_id=str(p["polymarket_market_id"]),
            kalshi_market_id=str(p["kalshi_market_id"]),
            matching_method=p.get("matching_method", "manual"),
            similarity_score=float(p["similarity_score"]) if p.get("similarity_score") else None,
            notes=p.get("notes"),
            active=p.get("active", True),
            created_at=p.get("created_at", datetime.utcnow()),
            updated_at=p.get("updated_at", datetime.utcnow()),
        )
        for p in pairs
    ]


@router.post("/pairs", response_model=MarketPairResponse)
async def create_market_pair(
    request: CreatePairRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Create a new market pair for arbitrage tracking.
    
    Pairs a Polymarket market with a Kalshi market that covers the same event.
    """
    # TODO: Check user has Pro+ subscription
    
    try:
        pair = ArbitrageQueries.upsert_market_pair(
            polymarket_market_id=UUID(request.polymarket_market_id),
            kalshi_market_id=UUID(request.kalshi_market_id),
            matching_method=request.matching_method,
            similarity_score=Decimal(str(request.similarity_score)) if request.similarity_score else None,
            notes=request.notes,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return MarketPairResponse(
        pair_id=str(pair["pair_id"]),
        polymarket_market_id=str(pair["polymarket_market_id"]),
        kalshi_market_id=str(pair["kalshi_market_id"]),
        matching_method=pair["matching_method"],
        similarity_score=float(pair["similarity_score"]) if pair.get("similarity_score") else None,
        notes=pair.get("notes"),
        active=pair["active"],
        created_at=pair["created_at"],
        updated_at=pair["updated_at"],
    )


@router.get("/suggest", response_model=List[SimilarMarketResponse])
async def suggest_market_pairs(
    title: str = Query(..., description="Market title to match"),
    source: str = Query(..., description="Source to search (polymarket or kalshi)"),
    threshold: float = Query(default=0.85, ge=0, le=1),
    limit: int = Query(default=10, le=50),
    current_user: dict = Depends(get_current_user),
):
    """
    Find markets on a platform with similar titles.
    
    Use this to find potential matches for creating market pairs.
    Requires pg_trgm extension for fuzzy matching.
    """
    if source not in ("polymarket", "kalshi"):
        raise HTTPException(status_code=400, detail="Source must be 'polymarket' or 'kalshi'")
    
    matches = ArbitrageQueries.find_similar_markets(
        title=title,
        source=source,
        threshold=threshold,
        limit=limit,
    )
    
    return [
        SimilarMarketResponse(
            market_id=str(m["market_id"]),
            source=m["source"],
            source_id=m["source_id"],
            title=m["title"],
            category=m.get("category"),
            url=m.get("url"),
            similarity_score=float(m["similarity_score"]),
        )
        for m in matches
    ]
