BEGIN;

-- CEX 16: immutable provenance for executions settled through SettleExecution.
-- A trade must be financially POSTED before its provenance receipt is inserted.
-- The FK and both unique identities share the existing settlement transaction:
-- there is no independent processed-events registry or second SQL commit.
CREATE TABLE accounting.trade_executions (
    trade_id TEXT PRIMARY KEY
        REFERENCES accounting.trade_settlements (trade_id) ON DELETE RESTRICT,
    source_market_id BIGINT NOT NULL
        CHECK (source_market_id BETWEEN 0 AND 4294967295),
    market_sequence NUMERIC(20, 0) NOT NULL
        CHECK (market_sequence BETWEEN 1 AND 18446744073709551615::NUMERIC),
    event_ordinal NUMERIC(20, 0) NOT NULL
        CHECK (event_ordinal BETWEEN 0 AND 18446744073709551615::NUMERIC),
    market_id TEXT NOT NULL CHECK (market_id <> ''),
    maker_order_id TEXT NOT NULL CHECK (maker_order_id <> ''),
    taker_order_id TEXT NOT NULL CHECK (taker_order_id <> ''),
    maker_side TEXT NOT NULL CHECK (maker_side IN ('BUY', 'SELL')),
    price_units NUMERIC(20, 0) NOT NULL
        CHECK (price_units BETWEEN 1 AND 18446744073709551615::NUMERIC),
    base_asset TEXT NOT NULL REFERENCES accounting.assets (asset_code) ON DELETE RESTRICT,
    quote_asset TEXT NOT NULL REFERENCES accounting.assets (asset_code) ON DELETE RESTRICT,
    base_atoms NUMERIC(20, 0) NOT NULL
        CHECK (base_atoms BETWEEN 1 AND 18446744073709551615::NUMERIC),
    quote_atoms NUMERIC(20, 0) NOT NULL
        CHECK (quote_atoms BETWEEN 1 AND 18446744073709551615::NUMERIC),
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT trade_executions_unique_source
        UNIQUE (source_market_id, market_sequence, event_ordinal),
    CONSTRAINT trade_executions_distinct_assets
        CHECK (base_asset <> quote_asset)
);

COMMENT ON TABLE accounting.trade_executions IS
    'Immutable CEX16 canonical matching-execution provenance, inserted atomically with one posted trade settlement. The source triple is globally unique and is not a Kafka offset.';

CREATE FUNCTION accounting.guard_trade_execution_receipt()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'trade execution receipts are immutable';
    END IF;
    -- The FK requires the settlement receipt; this check also requires that
    -- its corresponding economic ledger transaction is already POSTED.
    IF NOT EXISTS (
        SELECT 1 FROM accounting.trade_settlements AS ts
        JOIN accounting.ledger_transactions AS lt ON lt.operation_id = ts.operation_id
        WHERE ts.trade_id = NEW.trade_id
          AND lt.reference_type = 'TRADE'
          AND lt.reference_id = NEW.trade_id
          AND lt.state = 'POSTED'
    ) THEN
        RAISE EXCEPTION 'trade execution receipt requires a matching posted trade settlement';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trade_executions_guard
BEFORE INSERT OR UPDATE OR DELETE ON accounting.trade_executions
FOR EACH ROW EXECUTE FUNCTION accounting.guard_trade_execution_receipt();

COMMIT;
