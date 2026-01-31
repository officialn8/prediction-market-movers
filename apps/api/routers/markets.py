"""
Markets router - Public and authenticated market data endpoints.
"""

from datetime import datetime, timedelta
from typing import Optional, List
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from apps.api.routers.auth import get_current_user
from packages.core.storage import get_db_pool

router = APIRouter()


# ============================================================================
# Models
# ============================================================================

class MarketResponse(BaseModel):
    id: str
    source: str
    source_id: str
    title: str
    category: Optional[str]
    status: str
    url: Optional[str]
    current_price: Optional[float]
    price_change_1h: Optional[float]
    price_change_24h: Optional[float]
    volume_24h: Optional[float]


class MoverResponse(BaseModel):
    market_id: str
    token_id: str
    title: str
    outcome: str
    current_price: float
    price_change: float
    volume_24h: Optional[float]
    composite_score: float
    window: str


class PriceHistoryPoint(BaseModel):
    timestamp: datetime
    price: float
    volume: Optional[float]


# ============================================================================
# Public Endpoints (rate limited in production)
# ============================================================================

@router.get("/", response_model=List[MarketResponse])
async def list_markets(
    source: Optional[str] = None,
    category: Optional[str] = None,
    status: str = "active",
    limit: int = Query(default=50, le=100),
    offset: int = 0,
):
    """List markets with optional filters."""
    db = get_db_pool()
    
    conditions = ["m.status = %s"]
    params = [status]
    
    if source:
        conditions.append("m.source = %s")
        params.append(source)
    if category:
        conditions.append("m.category = %s")
        params.append(category)
    
    query = f"""
        SELECT 
            m.market_id as id,
            m.source,
            m.source_id,
            m.title,
            m.category,
            m.status,
            m.url,
            ls.price as current_price,
            ls.volume_24h
        FROM markets m
        LEFT JOIN LATERAL (
            SELECT s.price, s.volume_24h
            FROM snapshots s
            JOIN market_tokens mt ON s.token_id = mt.token_id
            WHERE mt.market_id = m.market_id AND mt.outcome = 'YES'
            ORDER BY s.ts DESC
            LIMIT 1
        ) ls ON true
        WHERE {' AND '.join(conditions)}
        ORDER BY m.updated_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    
    results = db.execute(query, tuple(params), fetch=True) or []
    
    return [
        MarketResponse(
            id=str(r["id"]),
            source=r["source"],
            source_id=r["source_id"],
            title=r["title"],
            category=r["category"],
            status=r["status"],
            url=r["url"],
            current_price=float(r["current_price"]) if r["current_price"] else None,
            price_change_1h=None,  # TODO: compute
            price_change_24h=None,
            volume_24h=float(r["volume_24h"]) if r["volume_24h"] else None,
        )
        for r in results
    ]


@router.get("/movers", response_model=List[MoverResponse])
async def get_top_movers(
    window: str = Query(default="1h", regex="^(1h|4h|24h)$"),
    direction: Optional[str] = Query(default=None, regex="^(up|down)$"),
    limit: int = Query(default=20, le=50),
):
    """Get top price movers."""
    db = get_db_pool()
    
    window_seconds = {"1h": 3600, "4h": 14400, "24h": 86400}[window]
    
    query = """
        SELECT 
            mc.market_id,
            mc.token_id,
            m.title,
            mt.outcome,
            mc.current_price,
            mc.move_pp as price_change,
            mc.volume_24h,
            mc.composite_score
        FROM movers_cache mc
        JOIN markets m ON mc.market_id = m.market_id
        JOIN market_tokens mt ON mc.token_id = mt.token_id
        WHERE mc.window_seconds = %s
    """
    params = [window_seconds]
    
    if direction == "up":
        query += " AND mc.move_pp > 0"
    elif direction == "down":
        query += " AND mc.move_pp < 0"
    
    query += " ORDER BY mc.composite_score DESC LIMIT %s"
    params.append(limit)
    
    results = db.execute(query, tuple(params), fetch=True) or []
    
    return [
        MoverResponse(
            market_id=str(r["market_id"]),
            token_id=str(r["token_id"]),
            title=r["title"],
            outcome=r["outcome"],
            current_price=float(r["current_price"]),
            price_change=float(r["price_change"]),
            volume_24h=float(r["volume_24h"]) if r["volume_24h"] else None,
            composite_score=float(r["composite_score"]),
            window=window,
        )
        for r in results
    ]


@router.get("/search")
async def search_markets(
    q: str = Query(..., min_length=2),
    limit: int = Query(default=20, le=50),
):
    """Search markets by title."""
    db = get_db_pool()
    
    results = db.execute(
        """
        SELECT market_id as id, source, title, category, status
        FROM markets
        WHERE title ILIKE %s AND status = 'active'
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (f"%{q}%", limit),
        fetch=True
    ) or []
    
    return results


@router.get("/{market_id}")
async def get_market(market_id: str):
    """Get detailed market info."""
    db = get_db_pool()
    
    market = db.execute(
        "SELECT * FROM markets WHERE market_id = %s",
        (market_id,),
        fetch=True
    )
    
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    
    market = market[0]
    
    # Get tokens with latest prices
    tokens = db.execute(
        """
        SELECT 
            mt.token_id,
            mt.outcome,
            mt.symbol,
            ls.price,
            ls.volume_24h,
            ls.ts as last_updated
        FROM market_tokens mt
        LEFT JOIN LATERAL (
            SELECT price, volume_24h, ts
            FROM snapshots
            WHERE token_id = mt.token_id
            ORDER BY ts DESC
            LIMIT 1
        ) ls ON true
        WHERE mt.market_id = %s
        """,
        (market_id,),
        fetch=True
    ) or []
    
    return {
        **market,
        "market_id": str(market["market_id"]),
        "tokens": [
            {
                "token_id": str(t["token_id"]),
                "outcome": t["outcome"],
                "symbol": t["symbol"],
                "price": float(t["price"]) if t["price"] else None,
                "volume_24h": float(t["volume_24h"]) if t["volume_24h"] else None,
                "last_updated": t["last_updated"],
            }
            for t in tokens
        ]
    }


@router.get("/{market_id}/history")
async def get_price_history(
    market_id: str,
    outcome: str = "YES",
    hours: int = Query(default=24, le=168),
):
    """Get price history for a market token."""
    db = get_db_pool()
    
    # Get token ID
    token = db.execute(
        """
        SELECT token_id FROM market_tokens
        WHERE market_id = %s AND outcome = %s
        """,
        (market_id, outcome),
        fetch=True
    )
    
    if not token:
        raise HTTPException(status_code=404, detail="Token not found")
    
    token_id = token[0]["token_id"]
    
    # Get price history
    history = db.execute(
        """
        SELECT ts as timestamp, price, volume_24h as volume
        FROM snapshots
        WHERE token_id = %s AND ts > NOW() - INTERVAL '%s hours'
        ORDER BY ts ASC
        """,
        (str(token_id), hours),
        fetch=True
    ) or []
    
    return [
        PriceHistoryPoint(
            timestamp=h["timestamp"],
            price=float(h["price"]),
            volume=float(h["volume"]) if h["volume"] else None,
        )
        for h in history
    ]
