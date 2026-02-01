"""
Webhooks router - Polar webhook handling for subscriptions.
Polar acts as Merchant of Record (MoR) - handles taxes, payments globally.
"""

import os
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, HTTPException, Header, Depends
from pydantic import BaseModel

from apps.api.routers.auth import get_current_user
from packages.core.storage import get_db_pool

router = APIRouter()
logger = logging.getLogger(__name__)

POLAR_WEBHOOK_SECRET = os.getenv("POLAR_WEBHOOK_SECRET")  # None if not set
POLAR_ACCESS_TOKEN = os.getenv("POLAR_ACCESS_TOKEN", "")

# Product IDs from Polar dashboard
POLAR_PRODUCTS = {
    "pro": os.getenv("POLAR_PRO_PRODUCT_ID"),
    "enterprise": os.getenv("POLAR_ENTERPRISE_PRODUCT_ID"),
}


# ============================================================================
# Polar Webhook
# ============================================================================

@router.post("/polar")
async def polar_webhook(request: Request):
    # Fail fast if webhook secret not configured
    if not POLAR_WEBHOOK_SECRET:
        logger.error("POLAR_WEBHOOK_SECRET not configured")
        raise HTTPException(status_code=503, detail="Webhook not configured")
    """
    Handle Polar webhook events.
    
    Key events:
    - checkout.created: Checkout started
    - checkout.updated: Checkout updated (e.g., confirmed)
    - subscription.created: New subscription
    - subscription.updated: Plan change, renewal
    - subscription.canceled: Cancellation
    - subscription.revoked: Immediate revocation
    """
    from polar_sdk.webhooks import validate_event, WebhookVerificationError
    
    payload = await request.body()
    headers = dict(request.headers)
    
    try:
        event = validate_event(
            payload=payload,
            headers=headers,
            secret=POLAR_WEBHOOK_SECRET,
        )
    except WebhookVerificationError as e:
        logger.error(f"Webhook verification failed: {e}")
        raise HTTPException(status_code=403, detail="Invalid webhook signature")
    
    event_type = event.get("type", "")
    data = event.get("data", {})
    
    db = get_db_pool()
    
    # Handle events
    if event_type == "checkout.updated":
        # Checkout confirmed/completed
        if data.get("status") == "succeeded":
            await handle_checkout_completed(db, data)
    
    elif event_type == "subscription.created":
        await handle_subscription_created(db, data)
    
    elif event_type == "subscription.updated":
        await handle_subscription_updated(db, data)
    
    elif event_type == "subscription.canceled":
        await handle_subscription_canceled(db, data)
    
    elif event_type == "subscription.revoked":
        await handle_subscription_revoked(db, data)
    
    return {"received": True}


async def handle_checkout_completed(db, checkout):
    """Handle completed checkout - subscription should be created separately."""
    checkout_id = checkout.get("id")
    customer_id = checkout.get("customer_id")
    customer_email = checkout.get("customer_email")
    
    # Get metadata we passed during checkout creation
    metadata = checkout.get("metadata", {})
    user_id = metadata.get("user_id")
    
    if not user_id:
        logger.warning(f"Checkout {checkout_id} completed but no user_id in metadata")
        return
    
    # Update user's polar_customer_id
    db.execute(
        "UPDATE users SET polar_customer_id = %s WHERE id = %s",
        (customer_id, user_id)
    )
    
    logger.info(f"Checkout completed: user={user_id}, customer={customer_id}")


async def handle_subscription_created(db, subscription):
    """Handle new subscription."""
    polar_sub_id = subscription.get("id")
    customer_id = subscription.get("customer_id")
    product_id = subscription.get("product_id")
    status = subscription.get("status")  # active, trialing, past_due, canceled, unpaid
    
    # Map product to tier
    tier = _get_tier_from_product(product_id)
    
    # Find user by polar_customer_id
    user = db.execute(
        "SELECT id FROM users WHERE polar_customer_id = %s",
        (customer_id,),
        fetch=True
    )
    
    if not user:
        logger.error(f"No user found for polar_customer_id {customer_id}")
        return
    
    user_id = user[0]["id"]
    
    # Update user tier
    db.execute(
        "UPDATE users SET tier = %s WHERE id = %s",
        (tier, user_id)
    )
    
    # Create/update subscription record
    current_period_end = subscription.get("current_period_end")
    if current_period_end:
        current_period_end = datetime.fromisoformat(current_period_end.replace("Z", "+00:00"))
    
    db.execute(
        """
        INSERT INTO subscriptions (user_id, polar_subscription_id, polar_customer_id, tier, status, current_period_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            polar_subscription_id = EXCLUDED.polar_subscription_id,
            tier = EXCLUDED.tier,
            status = EXCLUDED.status,
            current_period_end = EXCLUDED.current_period_end,
            updated_at = NOW()
        """,
        (user_id, polar_sub_id, customer_id, tier, status, current_period_end)
    )
    
    logger.info(f"User {user_id} subscribed to {tier} (polar_sub={polar_sub_id})")


async def handle_subscription_updated(db, subscription):
    """Handle subscription changes (upgrade/downgrade/renewal)."""
    polar_sub_id = subscription.get("id")
    status = subscription.get("status")
    product_id = subscription.get("product_id")
    cancel_at_period_end = subscription.get("cancel_at_period_end", False)
    
    tier = _get_tier_from_product(product_id)
    
    current_period_end = subscription.get("current_period_end")
    if current_period_end:
        current_period_end = datetime.fromisoformat(current_period_end.replace("Z", "+00:00"))
    
    # Update subscription
    result = db.execute(
        """
        UPDATE subscriptions SET
            status = %s,
            tier = %s,
            cancel_at_period_end = %s,
            current_period_end = %s,
            updated_at = NOW()
        WHERE polar_subscription_id = %s
        RETURNING user_id
        """,
        (status, tier, cancel_at_period_end, current_period_end, polar_sub_id),
        fetch=True
    )
    
    # Also update user tier
    if result:
        user_id = result[0]["user_id"]
        db.execute("UPDATE users SET tier = %s WHERE id = %s", (tier, user_id))
    
    logger.info(f"Subscription {polar_sub_id} updated: status={status}, tier={tier}")


async def handle_subscription_canceled(db, subscription):
    """Handle subscription cancellation (at period end)."""
    polar_sub_id = subscription.get("id")
    
    # Mark as canceling (will downgrade at period end)
    db.execute(
        """
        UPDATE subscriptions SET
            cancel_at_period_end = true,
            updated_at = NOW()
        WHERE polar_subscription_id = %s
        """,
        (polar_sub_id,)
    )
    
    logger.info(f"Subscription {polar_sub_id} set to cancel at period end")


async def handle_subscription_revoked(db, subscription):
    """Handle immediate subscription revocation."""
    polar_sub_id = subscription.get("id")
    
    # Get user and downgrade immediately
    sub = db.execute(
        "SELECT user_id FROM subscriptions WHERE polar_subscription_id = %s",
        (polar_sub_id,),
        fetch=True
    )
    
    if sub:
        user_id = sub[0]["user_id"]
        
        # Downgrade to free
        db.execute("UPDATE users SET tier = 'free' WHERE id = %s", (user_id,))
        db.execute(
            "UPDATE subscriptions SET status = 'revoked', tier = 'free', updated_at = NOW() WHERE polar_subscription_id = %s",
            (polar_sub_id,)
        )
        
        logger.info(f"User {user_id} subscription revoked, downgraded to free")


def _get_tier_from_product(product_id: str) -> str:
    """Map Polar product ID to tier name."""
    for tier, pid in POLAR_PRODUCTS.items():
        if pid == product_id:
            return tier
    return "free"


# ============================================================================
# Checkout Session Creation
# ============================================================================

class CheckoutRequest(BaseModel):
    tier: str


@router.post("/create-checkout")
async def create_checkout_session(
    data: CheckoutRequest,
    user: dict = Depends(get_current_user),
):
    """Create Polar checkout session for subscription."""
    from polar_sdk import Polar
    
    tier = data.tier
    user_id = str(user["id"])
    user_email = user.get("email", "")
    
    if tier not in POLAR_PRODUCTS:
        raise HTTPException(status_code=400, detail=f"Invalid tier: {tier}")
    
    product_id = POLAR_PRODUCTS[tier]
    if not product_id:
        raise HTTPException(status_code=500, detail=f"Product ID not configured for tier: {tier}")
    
    frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
    
    try:
        with Polar(access_token=POLAR_ACCESS_TOKEN) as polar:
            checkout = polar.checkouts.create(request={
                "products": [product_id],
                "customer_email": user_email,
                "success_url": f"{frontend_url}/dashboard?checkout=success",
                "metadata": {
                    "user_id": user_id,
                    "tier": tier,
                },
            })
            
            return {"checkout_url": checkout.url}
    
    except Exception as e:
        logger.error(f"Failed to create checkout: {e}")
        raise HTTPException(status_code=500, detail="Failed to create checkout session")


# ============================================================================
# Customer Portal (manage subscription)
# ============================================================================

@router.post("/customer-portal")
async def create_customer_portal_session(
    user: dict = Depends(get_current_user),
):
    """Create Polar customer portal session for managing subscription."""
    from polar_sdk import Polar
    
    db = get_db_pool()
    
    # Get user's polar_customer_id
    polar_customer_id = user.get("polar_customer_id")
    if not polar_customer_id:
        raise HTTPException(status_code=400, detail="No active subscription found")
    
    frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
    
    try:
        with Polar(access_token=POLAR_ACCESS_TOKEN) as polar:
            # Polar uses customer portal via their dashboard URL
            # Redirect user to manage subscription
            portal_url = f"https://polar.sh/purchases/subscriptions"
            
            return {"portal_url": portal_url}
    
    except Exception as e:
        logger.error(f"Failed to create portal session: {e}")
        raise HTTPException(status_code=500, detail="Failed to create portal session")
