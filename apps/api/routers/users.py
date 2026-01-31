"""
Users router - Profile and subscription management.
"""

import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr

from apps.api.routers.auth import get_current_user, hash_password
from packages.core.storage import get_db_pool

router = APIRouter()


# ============================================================================
# Models
# ============================================================================

class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None


class PasswordChange(BaseModel):
    current_password: str
    new_password: str


class SubscriptionResponse(BaseModel):
    tier: str
    status: str
    current_period_end: Optional[datetime]
    cancel_at_period_end: bool
    features: dict


# ============================================================================
# Tier Features
# ============================================================================

TIER_FEATURES = {
    "free": {
        "alerts": 3,
        "watchlist": 10,
        "api_access": False,
        "webhooks": False,
        "priority_support": False,
        "price": 0,
    },
    "pro": {
        "alerts": 25,
        "watchlist": 100,
        "api_access": True,
        "webhooks": True,
        "priority_support": False,
        "price": 9,
    },
    "enterprise": {
        "alerts": 100,
        "watchlist": "unlimited",
        "api_access": True,
        "webhooks": True,
        "priority_support": True,
        "price": 49,
    },
}


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/profile")
async def get_profile(user: dict = Depends(get_current_user)):
    """Get user profile."""
    return {
        "id": str(user["id"]),
        "email": user["email"],
        "name": user["name"],
        "tier": user["tier"],
        "created_at": user["created_at"],
        "features": TIER_FEATURES.get(user["tier"], TIER_FEATURES["free"]),
    }


@router.patch("/profile")
async def update_profile(
    data: ProfileUpdate,
    user: dict = Depends(get_current_user),
):
    """Update user profile."""
    db = get_db_pool()
    
    updates = []
    params = []
    
    if data.name is not None:
        updates.append("name = %s")
        params.append(data.name)
    
    if data.email is not None:
        # Check if email is taken
        existing = db.execute(
            "SELECT id FROM users WHERE email = %s AND id != %s",
            (data.email, str(user["id"])),
            fetch=True
        )
        if existing:
            raise HTTPException(status_code=400, detail="Email already in use")
        
        updates.append("email = %s")
        params.append(data.email)
    
    if updates:
        params.append(str(user["id"]))
        db.execute(
            f"UPDATE users SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s",
            tuple(params)
        )
    
    return {"updated": True}


@router.post("/change-password")
async def change_password(
    data: PasswordChange,
    user: dict = Depends(get_current_user),
):
    """Change user password."""
    from apps.api.routers.auth import verify_password
    
    if not verify_password(data.current_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    
    db = get_db_pool()
    new_hash = hash_password(data.new_password)
    
    db.execute(
        "UPDATE users SET password_hash = %s, updated_at = NOW() WHERE id = %s",
        (new_hash, str(user["id"]))
    )
    
    return {"updated": True}


@router.get("/subscription", response_model=SubscriptionResponse)
async def get_subscription(user: dict = Depends(get_current_user)):
    """Get subscription details."""
    db = get_db_pool()
    
    # Get subscription from DB if exists
    sub = db.execute(
        "SELECT * FROM subscriptions WHERE user_id = %s AND status = 'active'",
        (str(user["id"]),),
        fetch=True
    )
    
    if sub:
        sub = sub[0]
        return SubscriptionResponse(
            tier=user["tier"],
            status=sub["status"],
            current_period_end=sub["current_period_end"],
            cancel_at_period_end=sub.get("cancel_at_period_end", False),
            features=TIER_FEATURES.get(user["tier"], TIER_FEATURES["free"]),
        )
    
    # Free tier
    return SubscriptionResponse(
        tier="free",
        status="active",
        current_period_end=None,
        cancel_at_period_end=False,
        features=TIER_FEATURES["free"],
    )


@router.get("/api-key")
async def get_api_key(user: dict = Depends(get_current_user)):
    """Get or generate API key (pro+ only)."""
    if user["tier"] == "free":
        raise HTTPException(
            status_code=403,
            detail="API access requires Pro tier or higher"
        )
    
    db = get_db_pool()
    
    # Check for existing key
    existing = db.execute(
        "SELECT api_key, created_at FROM api_keys WHERE user_id = %s AND revoked = false",
        (str(user["id"]),),
        fetch=True
    )
    
    if existing:
        return {
            "api_key": existing[0]["api_key"],
            "created_at": existing[0]["created_at"],
        }
    
    # Generate new key
    import secrets
    api_key = f"pmm_{secrets.token_urlsafe(32)}"
    
    db.execute(
        "INSERT INTO api_keys (user_id, api_key) VALUES (%s, %s)",
        (str(user["id"]), api_key)
    )
    
    return {"api_key": api_key, "created_at": datetime.utcnow()}


@router.post("/api-key/regenerate")
async def regenerate_api_key(user: dict = Depends(get_current_user)):
    """Regenerate API key (revokes old one)."""
    if user["tier"] == "free":
        raise HTTPException(
            status_code=403,
            detail="API access requires Pro tier or higher"
        )
    
    db = get_db_pool()
    
    # Revoke existing
    db.execute(
        "UPDATE api_keys SET revoked = true, revoked_at = NOW() WHERE user_id = %s",
        (str(user["id"]),)
    )
    
    # Generate new
    import secrets
    api_key = f"pmm_{secrets.token_urlsafe(32)}"
    
    db.execute(
        "INSERT INTO api_keys (user_id, api_key) VALUES (%s, %s)",
        (str(user["id"]), api_key)
    )
    
    return {"api_key": api_key, "created_at": datetime.utcnow()}


@router.delete("/account")
async def delete_account(user: dict = Depends(get_current_user)):
    """Delete user account and all data."""
    db = get_db_pool()
    
    # Delete in order (foreign keys)
    db.execute("DELETE FROM api_keys WHERE user_id = %s", (str(user["id"]),))
    db.execute("DELETE FROM user_alerts WHERE user_id = %s", (str(user["id"]),))
    db.execute("DELETE FROM watchlist WHERE user_id = %s", (str(user["id"]),))
    db.execute("DELETE FROM subscriptions WHERE user_id = %s", (str(user["id"]),))
    db.execute("DELETE FROM users WHERE id = %s", (str(user["id"]),))
    
    return {"deleted": True}
