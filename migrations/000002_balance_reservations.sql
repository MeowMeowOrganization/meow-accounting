BEGIN;

-- ============================================================================
-- Balance reservation projection
-- ============================================================================
--
-- Reservation semantics:
--
--   posted_atoms
--       Ledger-derived economic ownership.
--
--   reserved_atoms
--       Portion currently unavailable for unrelated spending.
--
--   available_atoms
--       posted_atoms - reserved_atoms
--
-- Creating or releasing a reservation does NOT create ledger entries because
-- ownership has not changed.
-- ============================================================================

ALTER TABLE accounting.balances
    ADD COLUMN reserved_atoms NUMERIC(38, 0)
        NOT NULL
        DEFAULT 0;

ALTER TABLE accounting.balances
    ADD CONSTRAINT balances_reserved_atoms_nonnegative
    CHECK (reserved_atoms >= 0);

ALTER TABLE accounting.balances
    ADD COLUMN available_atoms NUMERIC(38, 0)
        GENERATED ALWAYS AS (
            posted_atoms - reserved_atoms
        )
        STORED;

COMMENT ON COLUMN accounting.balances.reserved_atoms IS
    'Aggregate amount currently unavailable because of active balance reservations.';

COMMENT ON COLUMN accounting.balances.available_atoms IS
    'Computed spendable amount: posted_atoms - reserved_atoms.';


-- ============================================================================
-- Strengthen CUSTOMER balance invariant
-- ============================================================================
--
-- Migration 000001 required:
--
--     posted_atoms >= 0
--
-- Once reservations exist the stronger invariant is:
--
--     posted_atoms >= reserved_atoms >= 0
--
-- This function is executed through the already-existing deferred constraint
-- trigger accounting.balances_validate_customer_balance.
--
-- Deferred validation avoids making correctness depend on the order of
-- individual writes inside one atomic settlement transaction.
-- ============================================================================

CREATE OR REPLACE FUNCTION accounting.assert_customer_balance_valid()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
DECLARE
    current_posted_atoms NUMERIC(38, 0);
    current_reserved_atoms NUMERIC(38, 0);
    current_account_kind TEXT;
BEGIN
    SELECT
        b.posted_atoms,
        b.reserved_atoms,
        a.account_kind
    INTO
        current_posted_atoms,
        current_reserved_atoms,
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
       AND current_posted_atoms < current_reserved_atoms
    THEN
        RAISE EXCEPTION
            'customer account % asset % has posted balance % below reserved balance %',
            NEW.account_id,
            NEW.asset_code,
            current_posted_atoms,
            current_reserved_atoms;
    END IF;

    RETURN NULL;
END;
$$;


-- ============================================================================
-- Balance reservations
-- ============================================================================
--
-- Reservations are current mutable state representing funds held unavailable.
--
-- The immutable reservation mutation table below supplies the corresponding
-- audit history.
--
-- Lifecycle:
--
--   ACTIVE
--       remaining_atoms > 0
--       released_atoms = 0
--
--   CONSUMED
--       remaining_atoms = 0
--       consumed_atoms = original_atoms
--       released_atoms = 0
--
--   RELEASED
--       remaining_atoms = 0
--       released_atoms > 0
--
-- In every state:
--
--   original_atoms
--     =
--   remaining_atoms
--     + consumed_atoms
--     + released_atoms
-- ============================================================================

CREATE TABLE accounting.balance_reservations (
    reservation_id UUID PRIMARY KEY,

    created_operation_id TEXT NOT NULL
        UNIQUE,

    account_id TEXT NOT NULL,

    asset_code TEXT NOT NULL,

    reference_kind TEXT NOT NULL
        CHECK (
            reference_kind = 'ORDER'
        ),

    reference_id TEXT NOT NULL,

    original_atoms NUMERIC(20, 0) NOT NULL,

    remaining_atoms NUMERIC(20, 0) NOT NULL,

    consumed_atoms NUMERIC(20, 0) NOT NULL
        DEFAULT 0,

    released_atoms NUMERIC(20, 0) NOT NULL
        DEFAULT 0,

    status TEXT NOT NULL
        CHECK (
            status IN (
                'ACTIVE',
                'CONSUMED',
                'RELEASED'
            )
        ),

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    closed_at TIMESTAMPTZ,

    CONSTRAINT balance_reservations_operation_fk
        FOREIGN KEY (created_operation_id)
        REFERENCES accounting.operations (operation_id)
        ON DELETE RESTRICT,

    CONSTRAINT balance_reservations_balance_fk
        FOREIGN KEY (
            account_id,
            asset_code
        )
        REFERENCES accounting.balances (
            account_id,
            asset_code
        )
        ON DELETE RESTRICT,

    CONSTRAINT balance_reservations_reference_uniq
        UNIQUE (
            account_id,
            reference_kind,
            reference_id,
            asset_code
        ),

    CHECK (reference_id <> ''),

    CHECK (
        original_atoms > 0
        AND original_atoms
            <= 18446744073709551615::NUMERIC
    ),

    CHECK (
        remaining_atoms >= 0
        AND remaining_atoms
            <= 18446744073709551615::NUMERIC
    ),

    CHECK (
        consumed_atoms >= 0
        AND consumed_atoms
            <= 18446744073709551615::NUMERIC
    ),

    CHECK (
        released_atoms >= 0
        AND released_atoms
            <= 18446744073709551615::NUMERIC
    ),

    CHECK (
        original_atoms
        =
        remaining_atoms
        + consumed_atoms
        + released_atoms
    ),

    CHECK (
        (
            status = 'ACTIVE'
            AND remaining_atoms > 0
            AND released_atoms = 0
            AND closed_at IS NULL
        )
        OR
        (
            status = 'CONSUMED'
            AND remaining_atoms = 0
            AND consumed_atoms = original_atoms
            AND released_atoms = 0
            AND closed_at IS NOT NULL
        )
        OR
        (
            status = 'RELEASED'
            AND remaining_atoms = 0
            AND released_atoms > 0
            AND closed_at IS NOT NULL
        )
    )
);

COMMENT ON TABLE accounting.balance_reservations IS
    'Current state of Accounting-owned funds reservations. Reservations affect spendability but not economic ownership.';

COMMENT ON COLUMN accounting.balance_reservations.reservation_id IS
    'Externally visible Accounting reservation identity. Generated by the Accounting application and represented as a string over protobuf.';

COMMENT ON COLUMN accounting.balance_reservations.created_operation_id IS
    'ReserveFunds operation that originally created this reservation.';

COMMENT ON COLUMN accounting.balance_reservations.reference_kind IS
    'Business object type responsible for the reservation. CEX 13 currently supports ORDER only.';

COMMENT ON COLUMN accounting.balance_reservations.reference_id IS
    'Stable identifier of the business object responsible for the reservation, initially order_id.';

COMMENT ON COLUMN accounting.balance_reservations.original_atoms IS
    'Exact amount originally reserved. Immutable after creation.';

COMMENT ON COLUMN accounting.balance_reservations.remaining_atoms IS
    'Amount still held unavailable for unrelated operations.';

COMMENT ON COLUMN accounting.balance_reservations.consumed_atoms IS
    'Amount already converted from reserved funds into settled economic activity.';

COMMENT ON COLUMN accounting.balance_reservations.released_atoms IS
    'Amount returned from reserved state to available balance without ownership transfer.';


CREATE INDEX balance_reservations_active_account_asset_idx
    ON accounting.balance_reservations (
        account_id,
        asset_code,
        reservation_id
    )
    WHERE status = 'ACTIVE';

CREATE INDEX balance_reservations_reference_idx
    ON accounting.balance_reservations (
        reference_kind,
        reference_id
    );


-- ============================================================================
-- Immutable reservation mutation history
-- ============================================================================
--
-- CREATED:
--   records original reservation creation.
--
-- CONSUMED:
--   records reservation amount consumed by settlement.
--   Multiple partial settlement consumptions may exist.
--
-- RELEASED:
--   records the final unused remainder returned to available balance.
--
-- One SETTLE_TRADE operation may mutate both buyer and seller reservations.
-- Therefore operation_id is intentionally NOT globally unique in this table.
-- ============================================================================

CREATE TABLE accounting.balance_reservation_mutations (
    reservation_mutation_id BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    operation_id TEXT NOT NULL,

    reservation_id UUID NOT NULL,

    mutation_kind TEXT NOT NULL
        CHECK (
            mutation_kind IN (
                'CREATED',
                'CONSUMED',
                'RELEASED'
            )
        ),

    amount_atoms NUMERIC(20, 0) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT transaction_timestamp(),

    CONSTRAINT balance_reservation_mutations_operation_fk
        FOREIGN KEY (operation_id)
        REFERENCES accounting.operations (operation_id)
        ON DELETE RESTRICT,

    CONSTRAINT balance_reservation_mutations_reservation_fk
        FOREIGN KEY (reservation_id)
        REFERENCES accounting.balance_reservations (
            reservation_id
        )
        ON DELETE RESTRICT,

    CONSTRAINT balance_reservation_mutation_idempotency_uniq
        UNIQUE (
            operation_id,
            reservation_id,
            mutation_kind
        ),

    CHECK (
        amount_atoms > 0
        AND amount_atoms
            <= 18446744073709551615::NUMERIC
    )
);

COMMENT ON TABLE accounting.balance_reservation_mutations IS
    'Immutable audit history of reservation creation, consumption, and release.';

COMMENT ON COLUMN accounting.balance_reservation_mutations.operation_id IS
    'Accounting mutation operation responsible for this reservation change.';

COMMENT ON COLUMN accounting.balance_reservation_mutations.mutation_kind IS
    'CREATED, settlement CONSUMED, or unused-funds RELEASED.';

CREATE INDEX balance_reservation_mutations_history_idx
    ON accounting.balance_reservation_mutations (
        reservation_id,
        reservation_mutation_id
    );


-- ============================================================================
-- Reservation creation invariant
-- ============================================================================
--
-- Every new reservation must begin as:
--
--   ACTIVE
--   remaining = original
--   consumed = 0
--   released = 0
--   closed_at = NULL
--
-- It is not legal to insert historical CONSUMED/RELEASED state directly.
-- ============================================================================

CREATE FUNCTION accounting.assert_new_reservation_state()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF NEW.status <> 'ACTIVE'
       OR NEW.remaining_atoms <> NEW.original_atoms
       OR NEW.consumed_atoms <> 0
       OR NEW.released_atoms <> 0
       OR NEW.closed_at IS NOT NULL
    THEN
        RAISE EXCEPTION
            'new reservation % must start ACTIVE with its full original amount remaining',
            NEW.reservation_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER balance_reservations_validate_insert
BEFORE INSERT
ON accounting.balance_reservations
FOR EACH ROW
EXECUTE FUNCTION accounting.assert_new_reservation_state();


-- ============================================================================
-- Reservation transition protection
-- ============================================================================
--
-- Identity/original amount are immutable.
--
-- Remaining amount may only decrease.
-- Consumed amount may only increase.
-- Released amount may only increase.
--
-- CONSUMED and RELEASED are terminal.
-- ============================================================================

CREATE FUNCTION accounting.guard_balance_reservation_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'balance reservations cannot be deleted';
    END IF;

    IF OLD.status IN (
        'CONSUMED',
        'RELEASED'
    ) THEN
        RAISE EXCEPTION
            'terminal reservation % cannot be modified',
            OLD.reservation_id;
    END IF;

    IF OLD.reservation_id IS DISTINCT FROM NEW.reservation_id
       OR OLD.created_operation_id IS DISTINCT FROM NEW.created_operation_id
       OR OLD.account_id IS DISTINCT FROM NEW.account_id
       OR OLD.asset_code IS DISTINCT FROM NEW.asset_code
       OR OLD.reference_kind IS DISTINCT FROM NEW.reference_kind
       OR OLD.reference_id IS DISTINCT FROM NEW.reference_id
       OR OLD.original_atoms IS DISTINCT FROM NEW.original_atoms
       OR OLD.created_at IS DISTINCT FROM NEW.created_at
    THEN
        RAISE EXCEPTION
            'reservation identity and original amount are immutable';
    END IF;

    IF NEW.remaining_atoms > OLD.remaining_atoms THEN
        RAISE EXCEPTION
            'reservation % remaining amount cannot increase',
            OLD.reservation_id;
    END IF;

    IF NEW.consumed_atoms < OLD.consumed_atoms THEN
        RAISE EXCEPTION
            'reservation % consumed amount cannot decrease',
            OLD.reservation_id;
    END IF;

    IF NEW.released_atoms < OLD.released_atoms THEN
        RAISE EXCEPTION
            'reservation % released amount cannot decrease',
            OLD.reservation_id;
    END IF;

    NEW.updated_at := transaction_timestamp();

    RETURN NEW;
END;
$$;

CREATE TRIGGER balance_reservations_guard_mutation
BEFORE UPDATE OR DELETE
ON accounting.balance_reservations
FOR EACH ROW
EXECUTE FUNCTION accounting.guard_balance_reservation_mutation();


-- ============================================================================
-- Reservation mutation history is append-only
-- ============================================================================

CREATE FUNCTION accounting.reject_reservation_mutation_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
BEGIN
    RAISE EXCEPTION
        'reservation mutation history is immutable';
END;
$$;

CREATE TRIGGER balance_reservation_mutations_reject_change
BEFORE UPDATE OR DELETE
ON accounting.balance_reservation_mutations
FOR EACH ROW
EXECUTE FUNCTION accounting.reject_reservation_mutation_change();


-- ============================================================================
-- Reservation current-state / audit-history reconciliation
-- ============================================================================
--
-- This is a deferred invariant.
--
-- At COMMIT:
--
--   exactly one CREATED mutation must exist;
--
--   CREATED operation_id must equal created_operation_id;
--
--   CREATED amount must equal original_atoms;
--
--   SUM(CONSUMED mutations) must equal consumed_atoms;
--
--   SUM(RELEASED mutations) must equal released_atoms.
--
-- This permits the application to update the current reservation row and add
-- its corresponding mutation in either order inside the same PostgreSQL
-- transaction while preventing them from diverging durably.
-- ============================================================================

CREATE FUNCTION accounting.assert_reservation_audit_consistent()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, accounting
AS $$
DECLARE
    target_reservation_id UUID;

    current_original_atoms NUMERIC(20, 0);
    current_consumed_atoms NUMERIC(20, 0);
    current_released_atoms NUMERIC(20, 0);
    reservation_created_operation_id TEXT;

    created_count BIGINT;
    consumed_total NUMERIC;
    released_total NUMERIC;
BEGIN
    target_reservation_id := NEW.reservation_id;

    SELECT
        original_atoms,
        consumed_atoms,
        released_atoms,
        created_operation_id
    INTO
        current_original_atoms,
        current_consumed_atoms,
        current_released_atoms,
        reservation_created_operation_id
    FROM accounting.balance_reservations
    WHERE reservation_id = target_reservation_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'reservation % disappeared',
            target_reservation_id;
    END IF;

    SELECT
        COUNT(*) FILTER (
            WHERE mutation_kind = 'CREATED'
        ),
        COALESCE(
            SUM(amount_atoms) FILTER (
                WHERE mutation_kind = 'CONSUMED'
            ),
            0
        ),
        COALESCE(
            SUM(amount_atoms) FILTER (
                WHERE mutation_kind = 'RELEASED'
            ),
            0
        )
    INTO
        created_count,
        consumed_total,
        released_total
    FROM accounting.balance_reservation_mutations
    WHERE reservation_id = target_reservation_id;

    IF created_count <> 1 THEN
        RAISE EXCEPTION
            'reservation % must have exactly one CREATED mutation; found %',
            target_reservation_id,
            created_count;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM accounting.balance_reservation_mutations
        WHERE reservation_id = target_reservation_id
          AND mutation_kind = 'CREATED'
          AND operation_id = reservation_created_operation_id
          AND amount_atoms = current_original_atoms
    ) THEN
        RAISE EXCEPTION
            'reservation % CREATED audit mutation does not match reservation creation',
            target_reservation_id;
    END IF;

    IF consumed_total <> current_consumed_atoms THEN
        RAISE EXCEPTION
            'reservation % consumed audit total % does not match current consumed_atoms %',
            target_reservation_id,
            consumed_total,
            current_consumed_atoms;
    END IF;

    IF released_total <> current_released_atoms THEN
        RAISE EXCEPTION
            'reservation % released audit total % does not match current released_atoms %',
            target_reservation_id,
            released_total,
            current_released_atoms;
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER balance_reservations_validate_audit
AFTER INSERT OR UPDATE
ON accounting.balance_reservations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION accounting.assert_reservation_audit_consistent();

CREATE CONSTRAINT TRIGGER reservation_mutations_validate_audit
AFTER INSERT
ON accounting.balance_reservation_mutations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION accounting.assert_reservation_audit_consistent();


COMMIT;