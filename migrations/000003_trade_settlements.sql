BEGIN;

-- CEX 14: immutable receipt proving exact semantic idempotency for a trade.
-- The core ledger and reservation tables from CEX 13 remain unchanged.
CREATE TABLE accounting.trade_settlements (
    trade_id TEXT PRIMARY KEY CHECK (trade_id <> ''),
    operation_id TEXT NOT NULL UNIQUE
        REFERENCES accounting.ledger_transactions (operation_id)
        ON DELETE RESTRICT,
    request_sha256 BYTEA NOT NULL CHECK (octet_length(request_sha256) = 32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT trade_settlements_operation_identity
        CHECK (operation_id = 'SETTLE_TRADE:' || trade_id)
);

COMMENT ON TABLE accounting.trade_settlements IS
    'Immutable trade-settlement idempotency receipts. Fingerprint covers all canonical settlement inputs, including reservations and completion flags.';

CREATE FUNCTION accounting.guard_trade_settlement_receipt()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM accounting.ledger_transactions AS lt
            WHERE lt.operation_id = NEW.operation_id
              AND lt.reference_type = 'TRADE'
              AND lt.reference_id = NEW.trade_id
              AND lt.state = 'POSTED'
        ) THEN
            RAISE EXCEPTION 'trade settlement receipt must reference a matching POSTED TRADE ledger transaction';
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'trade settlement receipts are immutable';
END;
$$;

CREATE TRIGGER trade_settlements_guard
BEFORE INSERT OR UPDATE OR DELETE
ON accounting.trade_settlements
FOR EACH ROW
EXECUTE FUNCTION accounting.guard_trade_settlement_receipt();

COMMIT;
