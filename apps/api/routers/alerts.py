"""
Alerts router - User-defined price alerts.
"""

from datetime import datetime
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

class AlertCreate(BaseModel):
    market_id: str
    outcome: str = "YES"
    condition: str  # "above" or "below"
    threshold: float
    notify_email: bool = True
    notify_webhook: Optional[str] = None


class AlertResponse(BaseModel):
    id: str
    market_id: str
    market_title: str
    outcome: str
    condition: str
    threshold: float
    current_price: Optional[float]
    triggered: bool
    triggered_at: Optional[datetime]
    created_at: datetime


class AlertUpdate(BaseModel):
    threshold: Optional[float] = None
    notify_email: Optional[bool] = None
    notify_webhook: Optional[str] = None
    enabled: Optional[bool] = None


# ============================================================================
# Tier limits
# ============================================================================

TIER_LIMITS = {
    "free": 3,
    "pro": 25,
    "enterprise": 100,
}


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/", response_model=List[AlertResponse])
async def list_alerts(
    triggered: Optional[bool] = None,
    user: dict = Depends(get_current_user),
):
    """List user's alerts."""
    db = get_db_pool()
    
    query = """
        SELECT 
            ua.id,
            ua.market_id,
            m.title as market_title,
            ua.outcome,
            ua.condition,
            ua.threshold,
            ua.triggered,
            ua.triggered_at,
            ua.created_at,
            ls.price as current_price
        FROM user_alerts ua
        JOIN markets m ON ua.market_id = m.market_id
        LEFT JOIN market_tokens mt ON ua.market_id = mt.market_id AND ua.outcome = mt.outcome
        LEFT JOIN LATERAL (
            SELECT price FROM snapshots WHERE token_id = mt.token_id ORDER BY ts DESC LIMIT 1
        ) ls ON true
        WHERE ua.user_id = %s AND ua.enabled = true
    """
    params = [str(user["id"])]
    
    if triggered is not None:
        query += " AND ua.triggered = %s"
        params.append(triggered)
    
    query += " ORDER BY ua.created_at DESC"
    
    results = db.execute(query, tuple(params), fetch=True) or []
    
    return [
        AlertResponse(
            id=str(r["id"]),
            market_id=str(r["market_id"]),
            market_title=r["market_title"],
            outcome=r["outcome"],
            condition=r["condition"],
            threshold=float(r["threshold"]),
            current_price=float(r["current_price"]) if r["current_price"] else None,
            triggered=r["triggered"],
            triggered_at=r["triggered_at"],
            created_at=r["created_at"],
        )
        for r in results
    ]


@router.post("/", response_model=AlertResponse)
async def create_alert(
    data: AlertCreate,
    user: dict = Depends(get_current_user),
):
    """Create a new price alert."""
    db = get_db_pool()
    
    # Check tier limit
    limit = TIER_LIMITS.get(user["tier"], 3)
    count = db.execute(
        "SELECT COUNT(*) as cnt FROM user_alerts WHERE user_id = %s AND enabled = true",
        (str(user["id"]),),
        fetch=True
    )
    current_count = count[0]["cnt"] if count else 0
    
    if current_count >= limit:
        raise HTTPException(
            status_code=403,
            detail=f"Alert limit reached ({limit} for {user['tier']} tier). Upgrade to create more."
        )
    
    # Verify market exists
    market = db.execute(
        "SELECT title FROM markets WHERE market_id = %s",
        (data.market_id,),
        fetch=True
    )
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    
    # Validate condition
    if data.condition not in ("above", "below"):
        raise HTTPException(status_code=400, detail="Condition must be 'above' or 'below'")
    
    # Create alert
    result = db.execute(
        """
        INSERT INTO user_alerts (user_id, market_id, outcome, condition, threshold, notify_email, notify_webhook)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id, created_at
        """,
        (
            str(user["id"]),
            data.market_id,
            data.outcome,
            data.condition,
            data.threshold,
            data.notify_email,
            data.notify_webhook,
        ),
        fetch=True
    )
    
    return AlertResponse(
        id=str(result[0]["id"]),
        market_id=data.market_id,
        market_title=market[0]["title"],
        outcome=data.outcome,
        condition=data.condition,
        threshold=data.threshold,
        current_price=None,
        triggered=False,
        triggered_at=None,
        created_at=result[0]["created_at"],
    )


@router.patch("/{alert_id}", response_model=AlertResponse)
async def update_alert(
    alert_id: str,
    data: AlertUpdate,
    user: dict = Depends(get_current_user),
):
    """Update an alert."""
    db = get_db_pool()
    
    # Verify ownership
    alert = db.execute(
        "SELECT * FROM user_alerts WHERE id = %s AND user_id = %s",
        (alert_id, str(user["id"])),
        fetch=True
    )
    
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    # Build update
    updates = []
    params = []
    
    if data.threshold is not None:
        updates.append("threshold = %s")
        params.append(data.threshold)
    if data.notify_email is not None:
        updates.append("notify_email = %s")
        params.append(data.notify_email)
    if data.notify_webhook is not None:
        updates.append("notify_webhook = %s")
        params.append(data.notify_webhook)
    if data.enabled is not None:
        updates.append("enabled = %s")
        params.append(data.enabled)
    
    if updates:
        params.extend([alert_id, str(user["id"])])
        db.execute(
            f"UPDATE user_alerts SET {', '.join(updates)} WHERE id = %s AND user_id = %s",
            tuple(params)
        )
    
    # Return updated alert
    return await list_alerts(user=user)  # Simplified - in prod, fetch single


@router.delete("/{alert_id}")
async def delete_alert(
    alert_id: str,
    user: dict = Depends(get_current_user),
):
    """Delete an alert."""
    db = get_db_pool()
    
    result = db.execute(
        "DELETE FROM user_alerts WHERE id = %s AND user_id = %s RETURNING id",
        (alert_id, str(user["id"])),
        fetch=True
    )
    
    if not result:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    return {"deleted": True, "id": alert_id}
