-- Allow 'simulated' source for development/testing
-- Replaces the check constraint on markets.source

BEGIN;

ALTER TABLE markets DROP CONSTRAINT IF EXISTS markets_source_check;

ALTER TABLE markets 
    ADD CONSTRAINT markets_source_check 
    CHECK (source IN ('polymarket', 'kalshi', 'simulated'));

COMMIT;
