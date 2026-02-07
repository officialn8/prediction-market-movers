-- Resolved outcome tracking + daily scoring diagnostics (Brier/log-loss/calibration)

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'markets'
          AND column_name = 'resolved_outcome'
    ) THEN
        ALTER TABLE markets ADD COLUMN resolved_outcome VARCHAR(8);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'markets'
          AND column_name = 'resolved_at'
    ) THEN
        ALTER TABLE markets ADD COLUMN resolved_at TIMESTAMPTZ;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_markets_resolved_outcome'
    ) THEN
        ALTER TABLE markets
        ADD CONSTRAINT ck_markets_resolved_outcome
        CHECK (resolved_outcome IS NULL OR resolved_outcome IN ('YES', 'NO'));
    END IF;
END $$;

UPDATE markets
SET resolved_at = updated_at
WHERE status = 'resolved'
  AND resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_markets_resolved_at
ON markets(resolved_at DESC)
WHERE status = 'resolved';

CREATE TABLE IF NOT EXISTS model_scoring_daily (
    score_date DATE NOT NULL,
    source VARCHAR(32) NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    brier_score DECIMAL(12, 8),
    log_loss DECIMAL(12, 8),
    ece DECIMAL(12, 8),
    calibration_bins JSONB NOT NULL DEFAULT '[]'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (score_date, source)
);

CREATE INDEX IF NOT EXISTS idx_model_scoring_daily_generated
ON model_scoring_daily(generated_at DESC);

COMMIT;
