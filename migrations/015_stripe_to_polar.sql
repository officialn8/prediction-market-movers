-- Migration: Stripe -> Polar billing
-- Polar acts as Merchant of Record (MoR)

-- Rename columns in users table
ALTER TABLE users 
    RENAME COLUMN stripe_customer_id TO polar_customer_id;

-- Drop and recreate index with new name
DROP INDEX IF EXISTS idx_users_stripe_customer;
CREATE INDEX IF NOT EXISTS idx_users_polar_customer ON users(polar_customer_id);

-- Rename columns in subscriptions table
ALTER TABLE subscriptions 
    RENAME COLUMN stripe_subscription_id TO polar_subscription_id;

ALTER TABLE subscriptions 
    RENAME COLUMN stripe_customer_id TO polar_customer_id;

-- Drop and recreate index with new name
DROP INDEX IF EXISTS idx_subscriptions_stripe;
CREATE INDEX IF NOT EXISTS idx_subscriptions_polar ON subscriptions(polar_subscription_id);
