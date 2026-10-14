BEGIN;

CREATE SCHEMA accounting;

-- ============================================================================
-- Assets
-- ============================================================================
--
-- Financial amounts are never stored as floating-point values.
--
-- Every amount is expressed as an integer number of the asset's smallest
-- configured unit ("atoms").
--
-- Examples:
--
--   BTC, scale = 8:
--     1 BTC = 100_000_000 atoms
--
--   USDT, scale = 6:
--     1 USDT = 1_000_000 atoms
--
-- asset_code and scale define the interpretation of all historical atom
-- amounts and therefore must never change after creation.
-- ============================================================================

CREATE TABLE accounting.assets (
    asset_code TEXT PRIMARY KEY,

    scale INTEGER NOT NULL
        CHECK (scale >= 0),

    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    CHECK (asset_code <> '')
);

COMMENT ON TABLE accounting.assets IS
    'Canonical Accounting-owned asset definitions. Financial quantities are stored as exact integer atoms.';

COMMENT ON COLUMN accounting.assets.asset_code IS
    'Stable business asset identifier such as BTC or USDT.';

COMMENT ON COLUMN accounting.assets.scale IS
    'Number of decimal places represented by one atom. Immutable after asset creation.';

COMMENT ON COLUMN accounting.assets.is_active IS
    'Whether new accounting operations may use the asset. Historical financial rows remain valid when false.';


-- ============================================================================
-- Accounts
-- ============================================================================
--
-- CUSTOMER:
--   User/customer financial ownership.
--
-- SYSTEM:
--   Accounting-owned system, clearing, custody, or other balancing positions.
--
-- CUSTOMER balances may never become negative.
--
-- SYSTEM balances may be negative where that is meaningful for the accounting
-- model.
-- ============================================================================

CREATE TABLE accounting.accounts (
    account_id TEXT PRIMARY KEY,

    account_kind TEXT NOT NULL
        CHECK (
            account_kind IN (
                'CUSTOMER',
                'SYSTEM'
            )
        ),

    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    CHECK (account_id <> '')
);

COMMENT ON TABLE accounting.accounts IS
    'Accounting ledger participants. Accounts may represent customers or Accounting-owned system positions.';

COMMENT ON COLUMN accounting.accounts.account_kind IS
    'CUSTOMER balances may not become negative. SYSTEM accounts may represent clearing or system positions.';

COMMENT ON COLUMN accounting.accounts.is_active IS
    'Whether new financial operations may target the account. Historical ledger state remains valid when false.';


-- ============================================================================
-- Global Accounting operation idempotency registry
-- ============================================================================
--
-- Every logical mutation of financial state must use exactly one stable
-- caller-controlled operation_id.
--
-- Retrying the SAME logical operation reuses the SAME operation_id.
--
-- Reusing an operation_id for different semantics is corruption and must be
-- rejected by the application layer.
--
-- operation_kind is deliberately TEXT rather than a PostgreSQL enum. New
-- Accounting capabilities should not require rewriting an enum type merely to
-- add an operation category.
--
-- Examples:
--
--   RESERVE_FUNDS
--   RELEASE_RESERVATION
--   SETTLE_TRADE
--   DEPOSIT
--   WITHDRAWAL
-- ============================================================================

CREATE TABLE accounting.operations (
    operation_id TEXT PRIMARY KEY,

    operation_kind TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    CHECK (operation_id <> ''),
    CHECK (operation_kind <> '')
);

COMMENT ON TABLE accounting.operations IS
    'Global registry of Accounting mutation idempotency identities.';

COMMENT ON COLUMN accounting.operations.operation_id IS
    'Caller-controlled logical-operation identity. Never reuse for different financial semantics.';

COMMENT ON COLUMN accounting.operations.operation_kind IS
    'Stable diagnostic category of the logical Accounting mutation.';


-- ============================================================================
-- Current balance projection
-- ============================================================================
--
-- balances is durable current state, but it is NOT independent economic truth.
--
-- posted_atoms is derived from committed ledger entries.
--
-- Reservations are introduced by migration 000002.
-- ============================================================================

CREATE TABLE accounting.balances (
    account_id TEXT NOT NULL,

    asset_code TEXT NOT NULL,

    posted_atoms NUMERIC(38, 0) NOT NULL
        DEFAULT 0,

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    PRIMARY KEY (
        account_id,
        asset_code
    ),

    CONSTRAINT balances_account_fk
        FOREIGN KEY (account_id)
        REFERENCES accounting.accounts (account_id)
        ON DELETE RESTRICT,

    CONSTRAINT balances_asset_fk
        FOREIGN KEY (asset_code)
        REFERENCES accounting.assets (asset_code)
        ON DELETE RESTRICT
);

COMMENT ON TABLE accounting.balances IS
    'Durable current posted-balance projection maintained from ledger entries. The append-only ledger remains canonical economic truth.';

COMMENT ON COLUMN accounting.balances.posted_atoms IS
    'Current ledger-derived ownership expressed as exact integer asset atoms.';

CREATE INDEX balances_asset_account_idx
    ON accounting.balances (
        asset_code,
        account_id
    );


-- ============================================================================
-- Ledger transactions
-- ============================================================================
--
-- One ledger transaction represents one atomic economic posting.
--
-- Creation lifecycle:
--
--   PENDING
--       |
--       | insert all ledger entries
--       v
--   POSTED
--
-- PENDING may exist only inside the PostgreSQL transaction creating the
-- posting. Deferred constraints below reject COMMIT unless the transaction is
-- POSTED and balanced independently for every asset.
-- ============================================================================

CREATE TABLE accounting.ledger_transactions (
    ledger_transaction_id BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    operation_id TEXT NOT NULL
        UNIQUE,

    reference_type TEXT,

    reference_id TEXT,

    state TEXT NOT NULL
        DEFAULT 'PENDING'
        CHECK (
            state IN (
                'PENDING',
                'POSTED'
            )
        ),

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    posted_at TIMESTAMPTZ,

    CONSTRAINT ledger_transactions_operation_fk
        FOREIGN KEY (operation_id)
        REFERENCES accounting.operations (operation_id)
        ON DELETE RESTRICT,

    CHECK (
        (
            reference_type IS NULL
            AND reference_id IS NULL
        )
        OR
        (
            reference_type IS NOT NULL
            AND reference_type <> ''
            AND reference_id IS NOT NULL
            AND reference_id <> ''
        )
    ),

    CHECK (
        (
            state = 'PENDING'
            AND posted_at IS NULL
        )
        OR
        (
            state = 'POSTED'
            AND posted_at IS NOT NULL
        )
    )
);

COMMENT ON TABLE accounting.ledger_transactions IS
    'One atomic append-only financial posting. A committed transaction must be POSTED and balanced independently for every asset.';

COMMENT ON COLUMN accounting.ledger_transactions.operation_id IS
    'Global Accounting operation identity responsible for this financial posting.';

COMMENT ON COLUMN accounting.ledger_transactions.reference_type IS
    'Optional business/audit source category such as TRADE, DEPOSIT, or WITHDRAWAL.';

COMMENT ON COLUMN accounting.ledger_transactions.reference_id IS
    'Optional stable business identifier corresponding to reference_type.';

CREATE INDEX ledger_transactions_reference_idx
    ON accounting.ledger_transactions (
        reference_type,
        reference_id
    )
    WHERE reference_type IS NOT NULL;


-- ============================================================================
-- Ledger entries
-- ============================================================================
--
-- Each entry is a signed asset movement:
--
--   positive = credit
--   negative = debit
--
-- A transaction may contain multiple assets, but each asset must independently
-- balance to zero:
--
--   SUM(amount_atoms)
--   GROUP BY asset_code
--   = 0
--
-- NUMERIC(20,0) is used because the external contracts admit uint64 atom
-- magnitudes, while PostgreSQL BIGINT is signed 64-bit and cannot represent the
-- full uint64 range.
-- ============================================================================

CREATE TABLE accounting.ledger_entries (
    ledger_entry_id BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    ledger_transaction_id BIGINT NOT NULL,

    entry_index INTEGER NOT NULL
        CHECK (entry_index >= 0),

    account_id TEXT NOT NULL,

    asset_code TEXT NOT NULL,

    amount_atoms NUMERIC(20, 0) NOT NULL,

    CONSTRAINT ledger_entries_transaction_fk
        FOREIGN KEY (ledger_transaction_id)
        REFERENCES accounting.ledger_transactions (
            ledger_transaction_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT ledger_entries_account_fk
        FOREIGN KEY (account_id)
        REFERENCES accounting.accounts (account_id)
        ON DELETE RESTRICT,

    CONSTRAINT ledger_entries_asset_fk
        FOREIGN KEY (asset_code)
        REFERENCES accounting.assets (asset_code)
        ON DELETE RESTRICT,

    CONSTRAINT ledger_entries_transaction_index_uniq
        UNIQUE (
            ledger_transaction_id,
            entry_index
        ),

    CHECK (amount_atoms <> 0),

    CHECK (
        abs(amount_atoms)
        <= 18446744073709551615::NUMERIC
    )
);

COMMENT ON TABLE accounting.ledger_entries IS
    'Immutable signed atom movements belonging to one ledger transaction.';

COMMENT ON COLUMN accounting.ledger_entries.entry_index IS
    'Stable zero-based or caller-defined deterministic position within one posting.';

COMMENT ON COLUMN accounting.ledger_entries.amount_atoms IS
    'Signed exact asset delta: positive credit, negative debit. Magnitude is bounded to protobuf uint64 range.';

CREATE INDEX ledger_entries_account_asset_history_idx
    ON accounting.ledger_entries (
        account_id,
        asset_code,
        ledger_entry_id DESC
    );


-- ============================================================================
-- Asset master-data immutability
-- ============================================================================
--
-- asset_code and scale may never change because doing so would reinterpret
-- historical financial data.
--
-- is_active may change.
-- ============================================================================

CREATE FUNCTION accounting.guard_asset_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'accounting assets cannot be deleted; deactivate the asset instead';
    END IF;

    IF OLD.asset_code IS DISTINCT FROM NEW.asset_code
       OR OLD.scale IS DISTINCT FROM NEW.scale
       OR OLD.created_at IS DISTINCT FROM NEW.created_at
    THEN
        RAISE EXCEPTION
            'asset identity, scale, and creation timestamp are immutable';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER assets_guard_mutation
BEFORE UPDATE OR DELETE
ON accounting.assets
FOR EACH ROW
EXECUTE FUNCTION accounting.guard_asset_mutation();


-- ============================================================================
-- Account master-data immutability
-- ============================================================================
--
-- account identity and kind may never change.
--
-- is_active may change.
-- ============================================================================

CREATE FUNCTION accounting.guard_account_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'accounting accounts cannot be deleted; deactivate the account instead';
    END IF;

    IF OLD.account_id IS DISTINCT FROM NEW.account_id
       OR OLD.account_kind IS DISTINCT FROM NEW.account_kind
       OR OLD.created_at IS DISTINCT FROM NEW.created_at
    THEN
        RAISE EXCEPTION
            'account identity, account kind, and creation timestamp are immutable';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER accounts_guard_mutation
BEFORE UPDATE OR DELETE
ON accounting.accounts
FOR EACH ROW
EXECUTE FUNCTION accounting.guard_account_mutation();


-- ============================================================================
-- Operations are immutable
-- ============================================================================

CREATE FUNCTION accounting.reject_operation_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    RAISE EXCEPTION
        'accounting operations are immutable';
END;
$$;

CREATE TRIGGER operations_reject_mutation
BEFORE UPDATE OR DELETE
ON accounting.operations
FOR EACH ROW
EXECUTE FUNCTION accounting.reject_operation_mutation();


-- ============================================================================
-- Balance row identity is immutable
-- ============================================================================
--
-- A balance row may change its monetary projection, but its account/asset
-- identity may not be rewritten and the row may not be deleted.
-- ============================================================================

CREATE FUNCTION accounting.guard_balance_identity()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'accounting balance rows cannot be deleted';
    END IF;

    IF OLD.account_id IS DISTINCT FROM NEW.account_id
       OR OLD.asset_code IS DISTINCT FROM NEW.asset_code
    THEN
        RAISE EXCEPTION
            'balance account_id and asset_code are immutable';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER balances_guard_identity
BEFORE UPDATE OR DELETE
ON accounting.balances
FOR EACH ROW
EXECUTE FUNCTION accounting.guard_balance_identity();


-- ============================================================================
-- CUSTOMER final balance invariant
-- ============================================================================
--
-- This is a DEFERRED constraint trigger intentionally.
--
-- A single ledger transaction may contain multiple entries affecting the same
-- account/asset. We therefore validate the FINAL balance at COMMIT rather than
-- making correctness depend on entry insertion order.
--
-- Migration 000002 strengthens this function to:
--
--     posted_atoms >= reserved_atoms >= 0
--
-- without changing the existing constraint trigger.
-- ============================================================================

CREATE FUNCTION accounting.assert_customer_balance_valid()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
DECLARE
    current_posted_atoms NUMERIC(38, 0);
    current_account_kind TEXT;
BEGIN
    SELECT
        b.posted_atoms,
        a.account_kind
    INTO
        current_posted_atoms,
        current_account_kind
    FROM accounting.balances AS b
    JOIN accounting.accounts AS a
      ON a.account_id = b.account_id
    WHERE b.account_id = NEW.account_id
      AND b.asset_code = NEW.asset_code;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'balance row for account % asset % disappeared',
            NEW.account_id,
            NEW.asset_code;
    END IF;

    IF current_account_kind = 'CUSTOMER'
       AND current_posted_atoms < 0
    THEN
        RAISE EXCEPTION
            'customer account % would have negative % posted balance: % atoms',
            NEW.account_id,
            NEW.asset_code,
            current_posted_atoms;
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER balances_validate_customer_balance
AFTER INSERT OR UPDATE
ON accounting.balances
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION accounting.assert_customer_balance_valid();


-- ============================================================================
-- Ledger entries may only be appended while their transaction is PENDING
-- ============================================================================

CREATE FUNCTION accounting.ensure_ledger_transaction_pending()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
DECLARE
    transaction_state TEXT;
BEGIN
    SELECT state
    INTO transaction_state
    FROM accounting.ledger_transactions
    WHERE ledger_transaction_id = NEW.ledger_transaction_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'ledger transaction % does not exist',
            NEW.ledger_transaction_id;
    END IF;

    IF transaction_state <> 'PENDING' THEN
        RAISE EXCEPTION
            'cannot append entry to ledger transaction % in state %',
            NEW.ledger_transaction_id,
            transaction_state;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER ledger_entries_require_pending_transaction
BEFORE INSERT
ON accounting.ledger_entries
FOR EACH ROW
EXECUTE FUNCTION accounting.ensure_ledger_transaction_pending();


-- ============================================================================
-- Ledger entries update the durable current balance projection
-- ============================================================================
--
-- The update is part of the same PostgreSQL transaction as the ledger entry.
--
-- If ledger validation later fails, PostgreSQL rolls back both the ledger entry
-- and this projection update.
-- ============================================================================

CREATE FUNCTION accounting.apply_ledger_entry_to_balance()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    INSERT INTO accounting.balances (
        account_id,
        asset_code,
        posted_atoms,
        updated_at
    )
    VALUES (
        NEW.account_id,
        NEW.asset_code,
        NEW.amount_atoms,
        transaction_timestamp()
    )
    ON CONFLICT (
        account_id,
        asset_code
    )
    DO UPDATE
    SET
        posted_atoms =
            accounting.balances.posted_atoms
            + EXCLUDED.posted_atoms,
        updated_at = transaction_timestamp();

    RETURN NEW;
END;
$$;

CREATE TRIGGER ledger_entries_apply_balance
AFTER INSERT
ON accounting.ledger_entries
FOR EACH ROW
EXECUTE FUNCTION accounting.apply_ledger_entry_to_balance();


-- ============================================================================
-- Ledger transactions are immutable after creation except for the one-way
-- PENDING -> POSTED finalization transition.
-- ============================================================================

CREATE FUNCTION accounting.guard_ledger_transaction_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'ledger transactions cannot be deleted';
    END IF;

    IF OLD.state = 'PENDING'
       AND NEW.state = 'POSTED'
       AND OLD.ledger_transaction_id IS NOT DISTINCT FROM NEW.ledger_transaction_id
       AND OLD.operation_id IS NOT DISTINCT FROM NEW.operation_id
       AND OLD.reference_type IS NOT DISTINCT FROM NEW.reference_type
       AND OLD.reference_id IS NOT DISTINCT FROM NEW.reference_id
       AND OLD.created_at IS NOT DISTINCT FROM NEW.created_at
       AND OLD.posted_at IS NULL
       AND NEW.posted_at IS NOT NULL
    THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION
        'ledger transaction % is immutable except for PENDING -> POSTED finalization',
        OLD.ledger_transaction_id;
END;
$$;

CREATE TRIGGER ledger_transactions_guard_mutation
BEFORE UPDATE OR DELETE
ON accounting.ledger_transactions
FOR EACH ROW
EXECUTE FUNCTION accounting.guard_ledger_transaction_mutation();


-- ============================================================================
-- Ledger entries are fully immutable
-- ============================================================================
--
-- Corrections must always be represented by new compensating ledger
-- transactions.
-- ============================================================================

CREATE FUNCTION accounting.reject_ledger_entry_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    RAISE EXCEPTION
        'ledger entries are immutable; post a compensating transaction instead';
END;
$$;

CREATE TRIGGER ledger_entries_reject_mutation
BEFORE UPDATE OR DELETE
ON accounting.ledger_entries
FOR EACH ROW
EXECUTE FUNCTION accounting.reject_ledger_entry_mutation();


-- ============================================================================
-- Deferred ledger posting invariant
-- ============================================================================
--
-- At COMMIT every affected ledger transaction must:
--
--   1. exist;
--   2. be POSTED;
--   3. contain at least one entry;
--   4. balance independently for every asset.
--
-- A multi-asset trade therefore cannot use a BTC imbalance to numerically
-- cancel a USDT imbalance.
-- ============================================================================

CREATE FUNCTION accounting.assert_ledger_transaction_valid()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
DECLARE
    transaction_id BIGINT;
    transaction_state TEXT;
BEGIN
    transaction_id := NEW.ledger_transaction_id;

    SELECT state
    INTO transaction_state
    FROM accounting.ledger_transactions
    WHERE ledger_transaction_id = transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'ledger transaction % disappeared',
            transaction_id;
    END IF;

    IF transaction_state <> 'POSTED' THEN
        RAISE EXCEPTION
            'ledger transaction % cannot commit in state %',
            transaction_id,
            transaction_state;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM accounting.ledger_entries
        WHERE ledger_transaction_id = transaction_id
    ) THEN
        RAISE EXCEPTION
            'ledger transaction % has no entries',
            transaction_id;
    END IF;

    IF EXISTS (
        SELECT asset_code
        FROM accounting.ledger_entries
        WHERE ledger_transaction_id = transaction_id
        GROUP BY asset_code
        HAVING SUM(amount_atoms) <> 0
    ) THEN
        RAISE EXCEPTION
            'ledger transaction % is not balanced independently per asset',
            transaction_id;
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER ledger_transactions_validate_on_commit
AFTER INSERT OR UPDATE
ON accounting.ledger_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION accounting.assert_ledger_transaction_valid();

CREATE CONSTRAINT TRIGGER ledger_entries_validate_on_commit
AFTER INSERT
ON accounting.ledger_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION accounting.assert_ledger_transaction_valid();


COMMIT;