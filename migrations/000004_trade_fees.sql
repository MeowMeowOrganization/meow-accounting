BEGIN;

-- CEX 14 fee extension. Existing 000001-000003 are left untouched.
-- The fee receipt is an immutable, searchable account of the exact quote-asset
-- fees that are ALREADY posted as separate pairs in the trade's ledger transaction.
-- Principal entry positions: 0-3; buyer fee debit/credit: 4/5;
-- seller fee debit/credit: 6/7. A zero fee emits no ledger entries or receipt.
CREATE TABLE accounting.trade_fees (
    trade_id TEXT PRIMARY KEY
        REFERENCES accounting.trade_settlements (trade_id) ON DELETE RESTRICT,
    collector_account_id TEXT NOT NULL
        REFERENCES accounting.accounts (account_id) ON DELETE RESTRICT,
    quote_asset TEXT NOT NULL
        REFERENCES accounting.assets (asset_code) ON DELETE RESTRICT,
    buyer_fee_atoms NUMERIC(20, 0) NOT NULL
        CHECK (buyer_fee_atoms >= 0 AND buyer_fee_atoms <= 18446744073709551615::NUMERIC),
    seller_fee_atoms NUMERIC(20, 0) NOT NULL
        CHECK (seller_fee_atoms >= 0 AND seller_fee_atoms <= 18446744073709551615::NUMERIC),
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    CHECK (buyer_fee_atoms > 0 OR seller_fee_atoms > 0)
);

COMMENT ON TABLE accounting.trade_fees IS
    'Immutable audit metadata for explicit quote-asset fee postings within a TRADE ledger transaction; no separate economic posting.';

CREATE FUNCTION accounting.guard_trade_fee_receipt()
RETURNS TRIGGER LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
DECLARE
    collector_kind TEXT;
    collector_active BOOLEAN;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'trade fee receipts are immutable';
    END IF;

    SELECT account_kind, is_active INTO collector_kind, collector_active
    FROM accounting.accounts WHERE account_id = NEW.collector_account_id;
    IF NOT FOUND OR collector_kind <> 'SYSTEM' OR NOT collector_active THEN
        RAISE EXCEPTION 'fee collector % must be an active SYSTEM account', NEW.collector_account_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trade_fees_guard
BEFORE INSERT OR UPDATE OR DELETE ON accounting.trade_fees
FOR EACH ROW EXECUTE FUNCTION accounting.guard_trade_fee_receipt();

-- The receipt and exact debit/credit lines MUST match at COMMIT.
-- A matching result may generate multiple fills, so these positions are local
-- to one trade ledger_transaction_id, not global event ordinals.
CREATE FUNCTION accounting.assert_trade_fee_posting()
RETURNS TRIGGER LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
DECLARE
    target_trade_id TEXT;
    trade_txn BIGINT;
    collector TEXT;
    fee_asset TEXT;
    buyer_fee NUMERIC;
    seller_fee NUMERIC;
    buyer_id TEXT;
    seller_id TEXT;
BEGIN
    IF TG_TABLE_NAME = 'ledger_entries' THEN
        SELECT reference_id INTO target_trade_id
        FROM accounting.ledger_transactions
        WHERE ledger_transaction_id = NEW.ledger_transaction_id
          AND reference_type = 'TRADE';
        IF NOT FOUND THEN
            RETURN NULL;
        END IF;
    ELSE
        target_trade_id := NEW.trade_id;
    END IF;

    SELECT lt.ledger_transaction_id, f.collector_account_id, f.quote_asset,
           f.buyer_fee_atoms, f.seller_fee_atoms
    INTO trade_txn, collector, fee_asset, buyer_fee, seller_fee
    FROM accounting.trade_fees AS f
    JOIN accounting.trade_settlements AS s ON s.trade_id = f.trade_id
    JOIN accounting.ledger_transactions AS lt ON lt.operation_id = s.operation_id
    WHERE f.trade_id = target_trade_id
      AND lt.reference_type = 'TRADE'
      AND lt.reference_id = target_trade_id
      AND lt.state = 'POSTED';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TRADE % has fee entries without a matching POSTED fee receipt', target_trade_id;
    END IF;

    SELECT account_id INTO buyer_id FROM accounting.ledger_entries
    WHERE ledger_transaction_id = trade_txn AND entry_index = 2
      AND asset_code = fee_asset AND amount_atoms < 0;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TRADE % lacks buyer principal quote debit', target_trade_id;
    END IF;
    SELECT account_id INTO seller_id FROM accounting.ledger_entries
    WHERE ledger_transaction_id = trade_txn AND entry_index = 3
      AND asset_code = fee_asset AND amount_atoms > 0;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TRADE % lacks seller principal quote credit', target_trade_id;
    END IF;

    IF buyer_fee > 0 THEN
        IF NOT EXISTS (
            SELECT 1 FROM accounting.ledger_entries d
            JOIN accounting.ledger_entries c
              ON c.ledger_transaction_id = d.ledger_transaction_id
             AND c.entry_index = 5
            WHERE d.ledger_transaction_id = trade_txn AND d.entry_index = 4
              AND d.account_id = buyer_id AND d.asset_code = fee_asset
              AND d.amount_atoms = -buyer_fee
              AND c.account_id = collector AND c.asset_code = fee_asset
              AND c.amount_atoms = buyer_fee
        ) THEN
            RAISE EXCEPTION 'TRADE % buyer fee receipt/entries mismatch', target_trade_id;
        END IF;
    ELSIF EXISTS (
        SELECT 1 FROM accounting.ledger_entries
        WHERE ledger_transaction_id = trade_txn AND entry_index IN (4, 5)
    ) THEN
        RAISE EXCEPTION 'TRADE % has unexpected buyer fee entries', target_trade_id;
    END IF;

    IF seller_fee > 0 THEN
        IF NOT EXISTS (
            SELECT 1 FROM accounting.ledger_entries d
            JOIN accounting.ledger_entries c
              ON c.ledger_transaction_id = d.ledger_transaction_id
             AND c.entry_index = 7
            WHERE d.ledger_transaction_id = trade_txn AND d.entry_index = 6
              AND d.account_id = seller_id AND d.asset_code = fee_asset
              AND d.amount_atoms = -seller_fee
              AND c.account_id = collector AND c.asset_code = fee_asset
              AND c.amount_atoms = seller_fee
        ) THEN
            RAISE EXCEPTION 'TRADE % seller fee receipt/entries mismatch', target_trade_id;
        END IF;
    ELSIF EXISTS (
        SELECT 1 FROM accounting.ledger_entries
        WHERE ledger_transaction_id = trade_txn AND entry_index IN (6, 7)
    ) THEN
        RAISE EXCEPTION 'TRADE % has unexpected seller fee entries', target_trade_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM accounting.ledger_entries
        WHERE ledger_transaction_id = trade_txn AND entry_index >= 8
    ) THEN
        RAISE EXCEPTION 'TRADE % has unsupported ledger fee entry positions', target_trade_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trade_fees_validate_on_commit
AFTER INSERT ON accounting.trade_fees
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION accounting.assert_trade_fee_posting();

-- Detect fee ledger entries that were written without a receipt. Only trade
-- entries at indices >= 4 participate, so legacy 4-entry trades are unaffected.
CREATE CONSTRAINT TRIGGER ledger_trade_fees_validate_on_commit
AFTER INSERT ON accounting.ledger_entries
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
WHEN (NEW.entry_index >= 4)
EXECUTE FUNCTION accounting.assert_trade_fee_posting();

COMMIT;
