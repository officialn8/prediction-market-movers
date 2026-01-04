-- Migration: 009_system_status.sql
-- Create a simple key-value table for system status sharing between services

CREATE TABLE IF NOT EXISTS system_status (
    key TEXT PRIMARY KEY,
    value JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_system_status_updated ON system_status(updated_at);

-- Comment
COMMENT ON TABLE system_status IS 'Key-value store for system status (WSS metrics, etc.)';

