-- Each anomaly query should return ZERO ROWS. Never auto-fix with direct balance UPDATEs.

-- 1. Unbalanced committed ledger postings (per asset).
SELECT lt.ledger_transaction_id, le.asset_code, SUM(le.amount_atoms) AS discrepancy_atoms
FROM accounting.ledger_transactions lt
JOIN accounting.ledger_entries le USING (ledger_transaction_id)
GROUP BY lt.ledger_transaction_id, le.asset_code
HAVING SUM(le.amount_atoms) <> 0;

-- 2. Ledger-derived posted balances disagree with balance projection.
WITH expected AS (
    SELECT account_id, asset_code, SUM(amount_atoms) AS posted_atoms
    FROM accounting.ledger_entries
    GROUP BY account_id, asset_code
)
SELECT COALESCE(e.account_id, b.account_id) AS account_id,
       COALESCE(e.asset_code, b.asset_code) AS asset_code,
       COALESCE(e.posted_atoms, 0) AS ledger_atoms,
       COALESCE(b.posted_atoms, 0) AS projection_atoms
FROM expected e
FULL OUTER JOIN accounting.balances b
  ON b.account_id = e.account_id AND b.asset_code = e.asset_code
WHERE COALESCE(e.posted_atoms, 0) <> COALESCE(b.posted_atoms, 0);

-- 3. Aggregate held reservations disagree with reservation rows.
WITH expected AS (
    SELECT account_id, asset_code, SUM(remaining_atoms) AS reserved_atoms
    FROM accounting.balance_reservations
    WHERE status = 'ACTIVE'
    GROUP BY account_id, asset_code
)
SELECT b.account_id, b.asset_code, b.reserved_atoms AS projection_atoms,
       COALESCE(e.reserved_atoms, 0) AS reservation_atoms
FROM accounting.balances b
LEFT JOIN expected e
  ON b.account_id = e.account_id AND b.asset_code = e.asset_code
WHERE b.reserved_atoms <> COALESCE(e.reserved_atoms, 0);

-- 4. Reservation history does not sum to current state.
WITH audit AS (
    SELECT reservation_id,
           COUNT(*) FILTER (WHERE mutation_kind = 'CREATED') AS created_count,
           COALESCE(SUM(amount_atoms) FILTER (WHERE mutation_kind = 'CREATED'), 0) AS created_atoms,
           COALESCE(SUM(amount_atoms) FILTER (WHERE mutation_kind = 'CONSUMED'), 0) AS consumed_atoms,
           COALESCE(SUM(amount_atoms) FILTER (WHERE mutation_kind = 'RELEASED'), 0) AS released_atoms
    FROM accounting.balance_reservation_mutations
    GROUP BY reservation_id
)
SELECT r.reservation_id
FROM accounting.balance_reservations r
LEFT JOIN audit a USING (reservation_id)
WHERE COALESCE(a.created_count, 0) <> 1
   OR COALESCE(a.created_atoms, 0) <> r.original_atoms
   OR COALESCE(a.consumed_atoms, 0) <> r.consumed_atoms
   OR COALESCE(a.released_atoms, 0) <> r.released_atoms;

-- 5. Globally claimed settlements without their immutable receipt or posting.
SELECT o.operation_id
FROM accounting.operations o
LEFT JOIN accounting.trade_settlements ts USING (operation_id)
LEFT JOIN accounting.ledger_transactions lt USING (operation_id)
WHERE o.operation_kind = 'SETTLE_TRADE'
  AND (ts.operation_id IS NULL OR lt.state IS DISTINCT FROM 'POSTED'
       OR lt.reference_type IS DISTINCT FROM 'TRADE'
       OR lt.reference_id IS DISTINCT FROM ts.trade_id);

-- 6. Customer funds are negative or over-reserved.
SELECT b.account_id, b.asset_code, b.posted_atoms, b.reserved_atoms
FROM accounting.balances b
JOIN accounting.accounts a USING (account_id)
WHERE a.account_kind = 'CUSTOMER'
  AND (b.reserved_atoms < 0 OR b.posted_atoms < b.reserved_atoms);
