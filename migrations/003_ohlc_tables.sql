-- 003_ohlc_tables.sql
-- Add OHLC rollup tables for efficient historical charting
-- Retention policy: snapshots deleted > 7 days, OHLC retained longer

-- 1-minute candles (Raw data rollup)
CREATE TABLE IF NOT EXISTS ohlc_1m (
    token_id UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    bucket_ts TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (token_id, bucket_ts)
);

-- 1-hour candles (Long-term trends)
CREATE TABLE IF NOT EXISTS ohlc_1h (
    token_id UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    bucket_ts TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (token_id, bucket_ts)
);

-- Index for efficient range queries
CREATE INDEX IF NOT EXISTS idx_ohlc_1m_ts ON ohlc_1m(bucket_ts);
CREATE INDEX IF NOT EXISTS idx_ohlc_1h_ts ON ohlc_1h(bucket_ts);

-- Function to clean up old snapshots
CREATE OR REPLACE PROCEDURE clean_old_snapshots()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Delete raw snapshots older than 7 days
    DELETE FROM snapshots 
    WHERE ts < NOW() - INTERVAL '7 days';
END;
$$;
