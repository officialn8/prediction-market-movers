-- Volume semantics alignment:
-- - Add hourly trade volume table
-- - Extend trade accumulator to populate hourly table
-- - Rebuild volume baselines using trade-first daily totals with provider fallback
-- - Add freshness metadata to v_latest_volumes

BEGIN;

-- ============================================================================
-- Hourly trade volume buckets (canonical baseline input)
-- ============================================================================
CREATE TABLE IF NOT EXISTS volume_hourly (
    token_id UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    bucket_ts TIMESTAMPTZ NOT NULL,
    volume_notional DECIMAL(20, 2) NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token_id, bucket_ts)
);

CREATE INDEX IF NOT EXISTS idx_volume_hourly_bucket_ts ON volume_hourly(bucket_ts DESC);
CREATE INDEX IF NOT EXISTS idx_volume_hourly_token_bucket ON volume_hourly(token_id, bucket_ts DESC);

-- ============================================================================
-- Trade accumulator: keep rolling windows + hourly buckets
-- ============================================================================
CREATE OR REPLACE FUNCTION accumulate_trade_volume(
    p_token_id UUID,
    p_volume DECIMAL(20, 2),
    p_trade_ts TIMESTAMPTZ DEFAULT NOW()
) RETURNS VOID AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
    v_hour_bucket TIMESTAMPTZ := DATE_TRUNC('hour', p_trade_ts);
BEGIN
    IF p_volume IS NULL OR p_volume <= 0 THEN
        RETURN;
    END IF;

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

    -- Canonical hourly aggregation for baseline statistics.
    INSERT INTO volume_hourly (token_id, bucket_ts, volume_notional, trade_count, updated_at)
    VALUES (p_token_id, v_hour_bucket, p_volume, 1, v_now)
    ON CONFLICT (token_id, bucket_ts) DO UPDATE SET
        volume_notional = volume_hourly.volume_notional + EXCLUDED.volume_notional,
        trade_count = volume_hourly.trade_count + EXCLUDED.trade_count,
        updated_at = v_now;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Trade-first volume baselines with provider fallback
-- ============================================================================
CREATE OR REPLACE VIEW volume_averages AS
WITH trade_daily AS (
    SELECT
        token_id,
        DATE(bucket_ts) AS day,
        SUM(volume_notional) AS day_volume
    FROM volume_hourly
    WHERE bucket_ts >= DATE_TRUNC('day', NOW()) - INTERVAL '14 days'
      AND bucket_ts < DATE_TRUNC('day', NOW())
    GROUP BY token_id, DATE(bucket_ts)
),
provider_daily AS (
    SELECT
        token_id,
        day,
        day_volume
    FROM (
        SELECT DISTINCT ON (token_id, DATE(ts))
            token_id,
            DATE(ts) AS day,
            volume_24h AS day_volume,
            ts
        FROM snapshots
        WHERE ts >= DATE_TRUNC('day', NOW()) - INTERVAL '14 days'
          AND ts < DATE_TRUNC('day', NOW())
          AND volume_24h IS NOT NULL
          AND volume_24h > 0
        ORDER BY token_id, DATE(ts), ts DESC
    ) s
),
merged_daily AS (
    SELECT
        td.token_id,
        td.day,
        td.day_volume
    FROM trade_daily td
    UNION ALL
    SELECT
        pd.token_id,
        pd.day,
        pd.day_volume
    FROM provider_daily pd
    WHERE NOT EXISTS (
        SELECT 1
        FROM trade_daily td
        WHERE td.token_id = pd.token_id
          AND td.day = pd.day
    )
)
SELECT
    token_id,
    AVG(day_volume) AS avg_volume_7d,
    STDDEV(day_volume) AS stddev_volume_7d,
    MAX(day_volume) AS max_volume_7d,
    MIN(day_volume) AS min_volume_7d,
    COUNT(*) AS sample_count
FROM merged_daily
GROUP BY token_id
HAVING COUNT(*) >= 2;

-- ============================================================================
-- Latest volume view with freshness metadata
-- ============================================================================
CREATE OR REPLACE VIEW v_latest_volumes AS
WITH wss_volumes AS (
    SELECT
        token_id,
        volume_total AS wss_volume_24h,
        trade_count AS wss_trade_count,
        updated_at AS wss_updated_at
    FROM trade_volumes
    WHERE window_seconds = 86400
),
provider_volumes AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        volume_24h AS provider_volume_24h,
        ts AS provider_updated_at
    FROM snapshots
    WHERE volume_24h IS NOT NULL
    ORDER BY token_id, ts DESC
),
latest_prices AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        price AS latest_price,
        ts AS price_updated_at
    FROM snapshots
    WHERE price IS NOT NULL
    ORDER BY token_id, ts DESC
),
joined AS (
    SELECT
        mt.token_id,
        mt.market_id,
        mt.outcome,
        m.title,
        m.source,
        m.category,
        m.url,
        COALESCE(wv.wss_volume_24h, pv.provider_volume_24h, 0) AS volume_24h,
        COALESCE(wv.wss_volume_24h, pv.provider_volume_24h) IS NOT NULL AS has_volume_data,
        CASE
            WHEN wv.wss_volume_24h IS NOT NULL THEN 'wss'
            WHEN pv.provider_volume_24h IS NOT NULL THEN 'gamma'
            ELSE 'none'
        END AS volume_source,
        wv.wss_trade_count,
        wv.wss_updated_at,
        pv.provider_updated_at AS gamma_updated_at,
        lp.latest_price AS price,
        lp.price_updated_at,
        COALESCE(wv.wss_updated_at, pv.provider_updated_at) AS volume_as_of
    FROM market_tokens mt
    JOIN markets m ON mt.market_id = m.market_id
    LEFT JOIN wss_volumes wv ON mt.token_id = wv.token_id
    LEFT JOIN provider_volumes pv ON mt.token_id = pv.token_id
    LEFT JOIN latest_prices lp ON mt.token_id = lp.token_id
    WHERE m.status = 'active'
)
SELECT
    j.*,
    CASE
        WHEN j.volume_as_of IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (NOW() - j.volume_as_of))
    END AS volume_age_seconds,
    CASE
        WHEN j.volume_source = 'wss'
            THEN COALESCE(EXTRACT(EPOCH FROM (NOW() - j.volume_as_of)) <= 600, FALSE)
        WHEN j.volume_source = 'gamma'
            THEN COALESCE(EXTRACT(EPOCH FROM (NOW() - j.volume_as_of)) <= 7200, FALSE)
        ELSE FALSE
    END AS is_volume_fresh
FROM joined j;

COMMIT;
