-- Analytics and Alerts Schema

BEGIN;

-- =============================================================================
-- MOVERS CACHE TABLE
-- Stores precomputed top movers for fast dashboard loading
-- =============================================================================
CREATE TABLE IF NOT EXISTS movers_cache (
    as_of_ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_seconds  INTEGER NOT NULL, -- e.g., 300, 3600, 86400
    token_id        UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    price_now       DECIMAL(10, 6) NOT NULL,
    price_then      DECIMAL(10, 6) NOT NULL,
    move_pp         DECIMAL(10, 6) NOT NULL, -- Percentage points change
    abs_move_pp     DECIMAL(10, 6) NOT NULL, -- Absolute change
    rank            INTEGER NOT NULL,
    quality_score   DECIMAL(20, 6), -- Optional scoring metric
    
    PRIMARY KEY (as_of_ts, window_seconds, rank)
);

-- Indexes for efficient querying by window
CREATE INDEX IF NOT EXISTS idx_movers_cache_window_ts ON movers_cache(window_seconds, as_of_ts DESC);

-- =============================================================================
-- ALERTS TABLE
-- Stores triggered alerts for significant price movements
-- =============================================================================
CREATE TABLE IF NOT EXISTS alerts (
    alert_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    token_id        UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    window_seconds  INTEGER NOT NULL,
    move_pp         DECIMAL(10, 6) NOT NULL,
    threshold_pp    DECIMAL(10, 6) NOT NULL,
    reason          TEXT NOT NULL,
    acknowledged_at TIMESTAMPTZ
);

-- Index for querying unacknowledged alerts or recent alerts
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_ack ON alerts(acknowledged_at) WHERE acknowledged_at IS NULL;

COMMIT;
