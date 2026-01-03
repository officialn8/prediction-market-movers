-- Volume Spikes Detection Schema
-- Tracks unusual volume activity for early event detection

BEGIN;

-- =============================================================================
-- VOLUME SPIKES TABLE
-- Stores detected volume anomalies for alerting and analysis
-- =============================================================================
CREATE TABLE IF NOT EXISTS volume_spikes (
    spike_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    token_id        UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,

    -- Volume metrics
    current_volume  DECIMAL(20, 2) NOT NULL,      -- Current 24h volume
    avg_volume      DECIMAL(20, 2) NOT NULL,      -- Historical average volume
    spike_ratio     DECIMAL(10, 4) NOT NULL,      -- current / avg (e.g., 5.0 = 5x normal)

    -- Context
    current_price   DECIMAL(10, 6),               -- Price at time of spike
    price_change_1h DECIMAL(10, 4),               -- % change in last hour

    -- Classification
    severity        VARCHAR(20) NOT NULL DEFAULT 'medium', -- low, medium, high, extreme
    acknowledged_at TIMESTAMPTZ
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_volume_spikes_created ON volume_spikes(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_volume_spikes_token ON volume_spikes(token_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_volume_spikes_severity ON volume_spikes(severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_volume_spikes_unack ON volume_spikes(acknowledged_at) WHERE acknowledged_at IS NULL;

-- =============================================================================
-- ADD ALERT_TYPE TO ALERTS TABLE
-- Distinguishes between price movement alerts and volume spike alerts
-- =============================================================================
DO $$
BEGIN
    -- Add alert_type column if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'alerts' AND column_name = 'alert_type'
    ) THEN
        ALTER TABLE alerts ADD COLUMN alert_type VARCHAR(20) DEFAULT 'price_move';
    END IF;

    -- Add volume_spike_ratio column for context
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'alerts' AND column_name = 'volume_spike_ratio'
    ) THEN
        ALTER TABLE alerts ADD COLUMN volume_spike_ratio DECIMAL(10, 4);
    END IF;
END $$;

-- Index for filtering by alert type
CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type, created_at DESC);

-- =============================================================================
-- VOLUME HISTORY MATERIALIZED VIEW (Optional optimization)
-- Precomputes 7-day average volume per token for fast spike detection
-- =============================================================================
-- Note: This is a regular view, not materialized, for simplicity
-- Can be converted to materialized view with REFRESH for production scale

CREATE OR REPLACE VIEW volume_averages AS
SELECT
    s.token_id,
    AVG(s.volume_24h) as avg_volume_7d,
    STDDEV(s.volume_24h) as stddev_volume_7d,
    MAX(s.volume_24h) as max_volume_7d,
    MIN(s.volume_24h) as min_volume_7d,
    COUNT(*) as sample_count
FROM (
    -- Get one volume reading per day per token (daily snapshots)
    SELECT DISTINCT ON (token_id, DATE(ts))
        token_id,
        volume_24h,
        ts
    FROM snapshots
    WHERE ts > NOW() - INTERVAL '7 days'
      AND volume_24h IS NOT NULL
      AND volume_24h > 0
    ORDER BY token_id, DATE(ts), ts DESC
) s
GROUP BY s.token_id
HAVING COUNT(*) >= 2;  -- Require at least 2 data points

COMMIT;
