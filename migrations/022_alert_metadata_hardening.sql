-- Alert metadata hardening for deterministic dedupe.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'alerts'
          AND column_name = 'alert_type'
    ) THEN
        ALTER TABLE alerts
        ADD COLUMN alert_type VARCHAR(20) NOT NULL DEFAULT 'price_move';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'alerts'
          AND column_name = 'volume_spike_ratio'
    ) THEN
        ALTER TABLE alerts
        ADD COLUMN volume_spike_ratio DECIMAL(10, 4);
    END IF;
END $$;

UPDATE alerts
SET alert_type = 'price_move'
WHERE alert_type IS NULL;

CREATE INDEX IF NOT EXISTS idx_alerts_token_window_type_created
ON alerts(token_id, window_seconds, alert_type, created_at DESC);

COMMIT;
