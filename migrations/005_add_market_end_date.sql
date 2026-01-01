-- 005_add_market_end_date.sql
-- Add end_date column to markets table for filtering expiring/closed markets

ALTER TABLE markets 
ADD COLUMN IF NOT EXISTS end_date TIMESTAMPTZ;

-- Index for efficient date filtering
CREATE INDEX IF NOT EXISTS idx_markets_end_date ON markets(end_date);
