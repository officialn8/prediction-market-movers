-- Migration: Fix trade volume rolling window logic and enrich latest volumes view

BEGIN;

-- =============================================================================
-- FIX: VOLUME ACCUMULATION FUNCTION
-- Reset windows based on first_trade_ts rather than updated_at
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
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 300
            THEN p_volume
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 300
            THEN 1
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 300
            THEN p_trade_ts
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = GREATEST(trade_volumes.last_trade_ts, p_trade_ts),
        updated_at = v_now;

    -- 15-minute window (900 seconds)  
    INSERT INTO trade_volumes (token_id, window_seconds, volume_total, trade_count, first_trade_ts, last_trade_ts)
    VALUES (p_token_id, 900, p_volume, 1, p_trade_ts, p_trade_ts)
    ON CONFLICT (token_id, window_seconds) DO UPDATE SET
        volume_total = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 900
            THEN p_volume
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 900
            THEN 1
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 900
            THEN p_trade_ts
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = GREATEST(trade_volumes.last_trade_ts, p_trade_ts),
        updated_at = v_now;

    -- 1-hour window (3600 seconds)
    INSERT INTO trade_volumes (token_id, window_seconds, volume_total, trade_count, first_trade_ts, last_trade_ts)
    VALUES (p_token_id, 3600, p_volume, 1, p_trade_ts, p_trade_ts)
    ON CONFLICT (token_id, window_seconds) DO UPDATE SET
        volume_total = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 3600
            THEN p_volume
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 3600
            THEN 1
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 3600
            THEN p_trade_ts
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = GREATEST(trade_volumes.last_trade_ts, p_trade_ts),
        updated_at = v_now;

    -- 24-hour window (86400 seconds)
    INSERT INTO trade_volumes (token_id, window_seconds, volume_total, trade_count, first_trade_ts, last_trade_ts)
    VALUES (p_token_id, 86400, p_volume, 1, p_trade_ts, p_trade_ts)
    ON CONFLICT (token_id, window_seconds) DO UPDATE SET
        volume_total = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 86400
            THEN p_volume
            ELSE trade_volumes.volume_total + p_volume
        END,
        trade_count = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 86400
            THEN 1
            ELSE trade_volumes.trade_count + 1
        END,
        first_trade_ts = CASE 
            WHEN EXTRACT(EPOCH FROM (p_trade_ts - trade_volumes.first_trade_ts)) >= 86400
            THEN p_trade_ts
            ELSE trade_volumes.first_trade_ts
        END,
        last_trade_ts = GREATEST(trade_volumes.last_trade_ts, p_trade_ts),
        updated_at = v_now;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- VIEW: Latest volume with WSS preference (add latest price)
-- =============================================================================
CREATE OR REPLACE VIEW v_latest_volumes AS
WITH wss_volumes AS (
    SELECT 
        token_id,
        volume_total as wss_volume_24h,
        trade_count as wss_trade_count,
        updated_at as wss_updated_at
    FROM trade_volumes 
    WHERE window_seconds = 86400
),
gamma_volumes AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        volume_24h as gamma_volume_24h,
        ts as gamma_updated_at
    FROM snapshots
    WHERE volume_24h IS NOT NULL
    ORDER BY token_id, ts DESC
),
latest_prices AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        price as latest_price,
        ts as price_updated_at
    FROM snapshots
    WHERE price IS NOT NULL
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
    COALESCE(wv.wss_volume_24h, gv.gamma_volume_24h, 0) as volume_24h,
    COALESCE(wv.wss_volume_24h, gv.gamma_volume_24h) is not null as has_volume_data,
    CASE 
        WHEN wv.wss_volume_24h IS NOT NULL THEN 'wss'
        WHEN gv.gamma_volume_24h IS NOT NULL THEN 'gamma'
        ELSE 'none'
    END as volume_source,
    wv.wss_trade_count,
    wv.wss_updated_at,
    gv.gamma_updated_at,
    lp.latest_price as price,
    lp.price_updated_at
FROM market_tokens mt
JOIN markets m ON mt.market_id = m.market_id
LEFT JOIN wss_volumes wv ON mt.token_id = wv.token_id
LEFT JOIN gamma_volumes gv ON mt.token_id = gv.token_id
LEFT JOIN latest_prices lp ON mt.token_id = lp.token_id
WHERE m.status = 'active';

COMMIT;
