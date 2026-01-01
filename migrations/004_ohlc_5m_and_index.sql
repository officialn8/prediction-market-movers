-- 004_ohlc_5m_and_index.sql
-- Add 5-minute OHLC table and performance index for snapshots

-- 5-minute candles (Intermediate aggregation)
CREATE TABLE IF NOT EXISTS ohlc_5m (
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

CREATE INDEX IF NOT EXISTS idx_ohlc_5m_ts ON ohlc_5m(bucket_ts);

-- Index for efficient retention deletion on snapshots
-- Using concurrent if possible, but standard create is fine for now in this setup
CREATE INDEX IF NOT EXISTS idx_snapshots_ts_btree ON snapshots(ts);
