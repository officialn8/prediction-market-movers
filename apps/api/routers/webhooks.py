"""
Webhooks router - Stripe webhook handling for subscriptions.
"""

import os
import logging
from datetime import datetime

from fastapi import APIRouter, Request, HTTPException, Header

from packages.core.storage import get_db_pool

router = APIRouter()
logger = logging.getLogger(__name__)

STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")


# ============================================================================
# Stripe Webhook
# ============================================================================

@router.post("/stripe")
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(None, alias="Stripe-Signature"),
):
    """
    Handle Stripe webhook events.
    
    Events we care about:
    - checkout.session.completed: New subscription
    - customer.subscription.updated: Plan change, renewal
    - customer.subscription.deleted: Cancellation
    - invoice.payment_failed: Payment issue
    """
    import stripe
    
    stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
    
    payload = await request.body()
    
    # Verify webhook signature
    try:
        event = stripe.Webhook.construct_event(
            payload, stripe_signature, STRIPE_WEBHOOK_SECRET
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    
    db = get_db_pool()
    
    # Handle events
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        await handle_checkout_completed(db, session)
    
    elif event["type"] == "customer.subscription.updated":
        subscription = event["data"]["object"]
        await handle_subscription_updated(db, subscription)
    
    elif event["type"] == "customer.subscription.deleted":
        subscription = event["data"]["object"]
        await handle_subscription_deleted(db, subscription)
    
    elif event["type"] == "invoice.payment_failed":
        invoice = event["data"]["object"]
        await handle_payment_failed(db, invoice)
    
    return {"received": True}


async def handle_checkout_completed(db, session):
    """Handle new subscription from checkout."""
    user_id = session.get("client_reference_id")
    customer_id = session.get("customer")
    subscription_id = session.get("subscription")
    
    if not user_id:
        logger.error("No user_id in checkout session")
        return
    
    # Determine tier from price
    # You'd map price IDs to tiers in production
    tier = "pro"  # Default for now
    
    # Update user
    db.execute(
        "UPDATE users SET tier = %s, stripe_customer_id = %s WHERE id = %s",
        (tier, customer_id, user_id)
    )
    
    # Create subscription record
    db.execute(
        """
        INSERT INTO subscriptions (user_id, stripe_subscription_id, stripe_customer_id, tier, status)
        VALUES (%s, %s, %s, %s, 'active')
        ON CONFLICT (user_id) DO UPDATE SET
            stripe_subscription_id = EXCLUDED.stripe_subscription_id,
            tier = EXCLUDED.tier,
            status = 'active',
            updated_at = NOW()
        """,
        (user_id, subscription_id, customer_id, tier)
    )
    
    logger.info(f"User {user_id} subscribed to {tier}")


async def handle_subscription_updated(db, subscription):
    """Handle subscription changes (upgrade/downgrade/renewal)."""
    stripe_sub_id = subscription["id"]
    status = subscription["status"]
    cancel_at_period_end = subscription.get("cancel_at_period_end", False)
    current_period_end = datetime.fromtimestamp(subscription["current_period_end"])
    
    # Update subscription
    db.execute(
        """
        UPDATE subscriptions SET
            status = %s,
            cancel_at_period_end = %s,
            current_period_end = %s,
            updated_at = NOW()
        WHERE stripe_subscription_id = %s
        """,
        (status, cancel_at_period_end, current_period_end, stripe_sub_id)
    )
    
    logger.info(f"Subscription {stripe_sub_id} updated: status={status}")


async def handle_subscription_deleted(db, subscription):
    """Handle subscription cancellation."""
    stripe_sub_id = subscription["id"]
    
    # Get user and downgrade
    sub = db.execute(
        "SELECT user_id FROM subscriptions WHERE stripe_subscription_id = %s",
        (stripe_sub_id,),
        fetch=True
    )
    
    if sub:
        user_id = sub[0]["user_id"]
        
        # Downgrade to free
        db.execute("UPDATE users SET tier = 'free' WHERE id = %s", (user_id,))
        db.execute(
            "UPDATE subscriptions SET status = 'canceled', updated_at = NOW() WHERE stripe_subscription_id = %s",
            (stripe_sub_id,)
        )
        
        logger.info(f"User {user_id} subscription canceled, downgraded to free")


async def handle_payment_failed(db, invoice):
    """Handle failed payment."""
    customer_id = invoice.get("customer")
    
    # Find user
    user = db.execute(
        "SELECT id, email FROM users WHERE stripe_customer_id = %s",
        (customer_id,),
        fetch=True
    )
    
    if user:
        logger.warning(f"Payment failed for user {user[0]['id']} ({user[0]['email']})")
        # In production: send email notification


# ============================================================================
# Checkout Session Creation
# ============================================================================

@router.post("/create-checkout")
async def create_checkout_session(
    tier: str,
    user_id: str,
):
    """Create Stripe checkout session for subscription."""
    import stripe
    
    stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
    
    # Price IDs (set these in Stripe dashboard)
    PRICE_IDS = {
        "pro": os.getenv("STRIPE_PRO_PRICE_ID"),
        "enterprise": os.getenv("STRIPE_ENTERPRISE_PRICE_ID"),
    }
    
    if tier not in PRICE_IDS:
        raise HTTPException(status_code=400, detail="Invalid tier")
    
    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            line_items=[{"price": PRICE_IDS[tier], "quantity": 1}],
            success_url=os.getenv("FRONTEND_URL", "http://localhost:3000") + "/billing?success=true",
            cancel_url=os.getenv("FRONTEND_URL", "http://localhost:3000") + "/billing?canceled=true",
            client_reference_id=user_id,
        )
        
        return {"checkout_url": session.url}
    
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))
