-- Optional TimescaleDB setup for time-series scale.
-- This migration is safe on plain PostgreSQL: it will no-op when TimescaleDB
-- is unavailable.

DO $$
DECLARE
    timescale_available BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_available_extensions
        WHERE name = 'timescaledb'
    ) INTO timescale_available;

    IF NOT timescale_available THEN
        RAISE NOTICE 'timescaledb extension is not available on this server; skipping setup';
        RETURN;
    END IF;

    BEGIN
        CREATE EXTENSION IF NOT EXISTS timescaledb;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Unable to enable timescaledb extension: %', SQLERRM;
        RETURN;
    END;

    -- Convert core time-series tables to hypertables.
    BEGIN
        PERFORM create_hypertable('snapshots', 'ts', if_not_exists => TRUE, migrate_data => TRUE);
        PERFORM create_hypertable('ohlc_1m', 'bucket_ts', if_not_exists => TRUE, migrate_data => TRUE);
        PERFORM create_hypertable('ohlc_1h', 'bucket_ts', if_not_exists => TRUE, migrate_data => TRUE);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Hypertable conversion failed: %', SQLERRM;
    END;

    -- Compression settings.
    BEGIN
        ALTER TABLE snapshots
            SET (
                timescaledb.compress,
                timescaledb.compress_orderby = 'ts DESC',
                timescaledb.compress_segmentby = 'token_id'
            );
        ALTER TABLE ohlc_1m
            SET (
                timescaledb.compress,
                timescaledb.compress_orderby = 'bucket_ts DESC',
                timescaledb.compress_segmentby = 'token_id'
            );
        ALTER TABLE ohlc_1h
            SET (
                timescaledb.compress,
                timescaledb.compress_orderby = 'bucket_ts DESC',
                timescaledb.compress_segmentby = 'token_id'
            );
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Compression configuration failed: %', SQLERRM;
    END;

    -- Compression policies.
    BEGIN
        PERFORM add_compression_policy('snapshots', INTERVAL '1 day', if_not_exists => TRUE);
        PERFORM add_compression_policy('ohlc_1m', INTERVAL '2 days', if_not_exists => TRUE);
        PERFORM add_compression_policy('ohlc_1h', INTERVAL '7 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Compression policy configuration failed: %', SQLERRM;
    END;

    -- Retention policies aligned with containment defaults.
    BEGIN
        PERFORM add_retention_policy('snapshots', INTERVAL '3 days', if_not_exists => TRUE);
        PERFORM add_retention_policy('ohlc_1m', INTERVAL '14 days', if_not_exists => TRUE);
        PERFORM add_retention_policy('ohlc_1h', INTERVAL '120 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Retention policy configuration failed: %', SQLERRM;
    END;
END $$;
