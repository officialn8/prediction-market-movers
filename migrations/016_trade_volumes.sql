-- Migration: Add trade_volumes table for real-time volume accumulation from WSS
-- This fixes the "$0 vol" dashboard issue by accumulating TradeEvent.size from Polymarket WSS

BEGIN;

-- =============================================================================
-- TRADE_VOLUMES TABLE
-- Stores rolling window volume aggregates from WSS trade events
-- =============================================================================
CREATE TABLE IF NOT EXISTS trade_volumes (
    token_id        UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    window_seconds  INTEGER NOT NULL CHECK (window_seconds IN (300, 900, 3600, 86400)), -- 5m, 15m, 1h, 24h
    volume_total    DECIMAL(20, 2) NOT NULL DEFAULT 0, -- Total notional volume in USD
    trade_count     INTEGER NOT NULL DEFAULT 0, -- Number of trades in window
    first_trade_ts  TIMESTAMPTZ NOT NULL, -- First trade timestamp in window
    last_trade_ts   TIMESTAMPTZ NOT NULL, -- Last trade timestamp in window
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Primary key ensures one row per token per window
    PRIMARY KEY (token_id, window_seconds)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_trade_volumes_token ON trade_volumes(token_id);
CREATE INDEX IF NOT EXISTS idx_trade_volumes_window ON trade_volumes(window_seconds);
CREATE INDEX IF NOT EXISTS idx_trade_volumes_updated ON trade_volumes(updated_at);

-- =============================================================================
-- VOLUME ACCUMULATION FUNCTION
-- Aggregates trades into rolling windows and updates trade_volumes table
-- =============================================================================
CREATE OR REPLACE FUNCTION accumulate_trade_volume(
    p_token_id UUID,
    p_volume DECIMAL(20, 2),
    p_trade_ts TIMESTAMPTZ DEFAULT NOW()
) RETURNS VOID AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
BEGIN
    -- 5-minute window (300 seconds)
    INSERT INTO trade_volumes (token_id, window_seconds, volume_total, trade_count, first_trade_ts, last_trade_ts)
    VALUES (p_token_id, 300, p_volume, 1, p_trade_ts, p_trade_ts)
    ON CONFLICT (token_id, window_seconds) DO UPDATE SET
        volume_total = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 300 
            THEN p_volume -- Reset if window expired
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 300 
            THEN 1 -- Reset if window expired
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 300 
            THEN p_trade_ts -- Reset if window expired
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = p_trade_ts,
        updated_at = v_now;

    -- 15-minute window (900 seconds)  
    INSERT INTO trade_volumes (token_id, window_seconds, volume_total, trade_count, first_trade_ts, last_trade_ts)
    VALUES (p_token_id, 900, p_volume, 1, p_trade_ts, p_trade_ts)
    ON CONFLICT (token_id, window_seconds) DO UPDATE SET
        volume_total = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 900 
            THEN p_volume -- Reset if window expired
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 900 
            THEN 1 -- Reset if window expired
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 900 
            THEN p_trade_ts -- Reset if window expired
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = p_trade_ts,
        updated_at = v_now;

    -- 1-hour window (3600 seconds)
    INSERT INTO trade_volumes (token_id, window_seconds, volume_total, trade_count, first_trade_ts, last_trade_ts)
    VALUES (p_token_id, 3600, p_volume, 1, p_trade_ts, p_trade_ts)
    ON CONFLICT (token_id, window_seconds) DO UPDATE SET
        volume_total = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 3600 
            THEN p_volume -- Reset if window expired
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 3600 
            THEN 1 -- Reset if window expired
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 3600 
            THEN p_trade_ts -- Reset if window expired
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = p_trade_ts,
        updated_at = v_now;

    -- 24-hour window (86400 seconds)
    INSERT INTO trade_volumes (token_id, window_seconds, volume_total, trade_count, first_trade_ts, last_trade_ts)
    VALUES (p_token_id, 86400, p_volume, 1, p_trade_ts, p_trade_ts)
    ON CONFLICT (token_id, window_seconds) DO UPDATE SET
        volume_total = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 86400 
            THEN p_volume -- Reset if window expired
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 86400 
            THEN 1 -- Reset if window expired
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (v_now - trade_volumes.updated_at)) > 86400 
            THEN p_trade_ts -- Reset if window expired
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = p_trade_ts,
        updated_at = v_now;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VIEW: Latest volume with WSS preference
-- Returns most recent volume, preferring WSS trade volumes over Gamma API
-- =============================================================================
CREATE OR REPLACE VIEW v_latest_volumes AS
WITH wss_volumes AS (
    -- Get 24h WSS volume (most recent rolling window)
    SELECT 
        token_id,
        volume_total as wss_volume_24h,
        trade_count as wss_trade_count,
        updated_at as wss_updated_at
    FROM trade_volumes 
    WHERE window_seconds = 86400
),
gamma_volumes AS (
    -- Get latest Gamma API volume (fallback)
    SELECT DISTINCT ON (token_id)
        token_id,
        volume_24h as gamma_volume_24h,
        ts as gamma_updated_at
    FROM snapshots
    WHERE volume_24h IS NOT NULL
    ORDER BY token_id, ts DESC
)
SELECT
    mt.token_id,
    mt.market_id,
    mt.outcome,
    m.title,
    m.source,
    m.category,
    m.url,
    -- Prefer WSS volume over Gamma volume
    COALESCE(wv.wss_volume_24h, gv.gamma_volume_24h, 0) as volume_24h,
    COALESCE(wv.wss_volume_24h, gv.gamma_volume_24h) is not null as has_volume_data,
    CASE 
        WHEN wv.wss_volume_24h IS NOT NULL THEN 'wss'
        WHEN gv.gamma_volume_24h IS NOT NULL THEN 'gamma'
        ELSE 'none'
    END as volume_source,
    wv.wss_trade_count,
    wv.wss_updated_at,
    gv.gamma_updated_at
FROM market_tokens mt
JOIN markets m ON mt.market_id = m.market_id
LEFT JOIN wss_volumes wv ON mt.token_id = wv.token_id
LEFT JOIN gamma_volumes gv ON mt.token_id = gv.token_id
WHERE m.status = 'active';

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Indexes are automatically created on the underlying tables (trade_volumes, market_tokens, markets)
-- Views inherit indexes from their base tables, so no need to create separate view indexes

COMMIT;