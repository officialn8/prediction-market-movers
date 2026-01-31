-- Migration tracking table
-- This table records which migrations have been successfully applied
-- to ensure idempotency and prevent re-running migrations

CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_name TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum TEXT  -- Optional: store file hash for drift detection
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_schema_migrations_applied 
ON schema_migrations(applied_at DESC);




