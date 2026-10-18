package settlement

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Repository uses database/sql and standard PostgreSQL SQL. The hosting service
// supplies a *sql.DB opened with a PostgreSQL driver (e.g. pgx/stdlib).
// Isolation: READ COMMITTED + stable operation key + explicit ordered row locks.
type Repository struct{ db *sql.DB }

func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", ErrInvalidRequest)
	}
	return &Repository{db: db}, nil
}

// SettleTrade atomically consumes reservations, posts four principal entries
// plus optional balanced quote-asset fee entries, and writes immutable receipts.
//
// IMPORTANT: the input MUST be produced by a trusted ordered matching-result
// projection with independently validated order/account/reservation mapping.
// A Kafka offset is not part of the financial identity.
func (r *Repository) SettleTrade(ctx context.Context, request Request) (Result, error) {
	entries, err := request.Entries()
	if err != nil {
		return Result{}, err
	}
	if ctx == nil {
		return Result{}, fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Result{}, fmt.Errorf("begin settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	operationID := request.operationID()
	var claimed string
	err = tx.QueryRowContext(ctx, `
        INSERT INTO accounting.operations (operation_id, operation_kind)
        VALUES ($1, 'SETTLE_TRADE')
        ON CONFLICT (operation_id) DO NOTHING
        RETURNING operation_id`, operationID).Scan(&claimed)
	if errors.Is(err, sql.ErrNoRows) {
		result, lookupErr := findPrevious(ctx, tx, request)
		if lookupErr != nil {
			return Result{}, lookupErr
		}
		if err := tx.Commit(); err != nil {
			return Result{}, fmt.Errorf("commit replay lookup: %w", err)
		}
		return result, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("claim settlement identity: %w", err)
	}

	// Lock EVERY potentially affected (account, asset) row in deterministic
	// order BEFORE taking any reservation locks. This aligns with the CEX13
	// balance-first ReserveFunds pattern and prevents cross-trade lock cycles.
	if err := lockBalanceRows(ctx, tx, entries); err != nil {
		return Result{}, err
	}

	// Fee collectors cannot be customer accounts, even if a trusted adapter
	// accidentally supplies one. The SYSTEM account must already exist and be
	// active; this is never silently created by settlement.
	if request.Fees.enabled() {
		if err := validateFeeCollector(ctx, tx, request.Fees.CollectorAccountID); err != nil {
			return Result{}, err
		}
	}

	// Both reservations are then acquired in sorted UUID order, independently
	// of maker/taker or buyer/seller arrival ordering.
	buyer := reservationUse{
		id: request.Buyer.ReservationID, account: request.Buyer.AccountID,
		order: request.Buyer.OrderID, asset: request.QuoteAsset,
		consume: request.QuoteAtoms + request.Fees.BuyerAtoms, close: request.CloseBuyer,
	}
	seller := reservationUse{
		id: request.Seller.ReservationID, account: request.Seller.AccountID,
		order: request.Seller.OrderID, asset: request.BaseAsset,
		consume: request.BaseAtoms, close: request.CloseSeller,
	}
	uses := []reservationUse{buyer, seller}
	sort.Slice(uses, func(i, j int) bool { return strings.ToLower(uses[i].id) < strings.ToLower(uses[j].id) })
	for i := range uses {
		if err := uses[i].lockAndValidate(ctx, tx); err != nil {
			return Result{}, err
		}
	}
	for _, use := range uses {
		if err := use.apply(ctx, tx, operationID); err != nil {
			return Result{}, err
		}
	}

	var ledgerID int64
	err = tx.QueryRowContext(ctx, `
        INSERT INTO accounting.ledger_transactions
            (operation_id, reference_type, reference_id)
        VALUES ($1, 'TRADE', $2)
        RETURNING ledger_transaction_id`, operationID, request.TradeID).Scan(&ledgerID)
	if err != nil {
		return Result{}, fmt.Errorf("create pending ledger transaction: %w", err)
	}

	for _, entry := range entries {
		_, err = tx.ExecContext(ctx, `
            INSERT INTO accounting.ledger_entries
                (ledger_transaction_id, entry_index, account_id, asset_code, amount_atoms)
            VALUES ($1, $2, $3, $4, $5::numeric)`,
			ledgerID, entry.Index, entry.AccountID, entry.Asset, entry.signedDecimal())
		if err != nil {
			return Result{}, fmt.Errorf("append ledger entry %d: %w", entry.Index, err)
		}
	}

	_, err = tx.ExecContext(ctx, `
        UPDATE accounting.ledger_transactions
        SET state = 'POSTED', posted_at = transaction_timestamp()
        WHERE ledger_transaction_id = $1 AND state = 'PENDING'`, ledgerID)
	if err != nil {
		return Result{}, fmt.Errorf("finalize ledger posting: %w", err)
	}

	digest := request.fingerprint()
	_, err = tx.ExecContext(ctx, `
        INSERT INTO accounting.trade_settlements
            (trade_id, operation_id, request_sha256)
        VALUES ($1, $2, $3)`, request.TradeID, operationID, digest[:])
	if err != nil {
		return Result{}, fmt.Errorf("write immutable settlement receipt: %w", err)
	}
	if request.Fees.enabled() {
		_, err = tx.ExecContext(ctx, `
        INSERT INTO accounting.trade_fees
            (trade_id, collector_account_id, quote_asset, buyer_fee_atoms, seller_fee_atoms)
        VALUES ($1, $2, $3, $4::numeric, $5::numeric)`,
			request.TradeID, request.Fees.CollectorAccountID, request.QuoteAsset,
			strconv.FormatUint(request.Fees.BuyerAtoms, 10),
			strconv.FormatUint(request.Fees.SellerAtoms, 10))
		if err != nil {
			return Result{}, fmt.Errorf("write immutable fee audit receipt: %w", err)
		}
	}

	// COMMIT runs the CEX13 deferred per-asset conservation, customer solvency,
	// and reservation current-state/audit-history checks.
	if err := tx.Commit(); err != nil {
		return Result{}, fmt.Errorf("commit settlement (retry same trade identity if outcome uncertain): %w", err)
	}
	return Result{OperationID: operationID, LedgerTransactionID: ledgerID}, nil
}

func validateFeeCollector(ctx context.Context, tx *sql.Tx, accountID string) error {
	var kind string
	var active bool
	err := tx.QueryRowContext(ctx, `
        SELECT account_kind, is_active FROM accounting.accounts
        WHERE account_id = $1 FOR SHARE`, accountID).Scan(&kind, &active)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && (kind != "SYSTEM" || !active)) {
		return fmt.Errorf("%w: fee collector must be an active SYSTEM account", ErrInvalidRequest)
	}
	if err != nil {
		return fmt.Errorf("load fee collector: %w", err)
	}
	return nil
}

func findPrevious(ctx context.Context, tx *sql.Tx, request Request) (Result, error) {
	var kind string
	var digest []byte
	var ledgerID sql.NullInt64
	var state sql.NullString
	var reference sql.NullString
	var collector, feeAsset, buyerFee, sellerFee sql.NullString
	err := tx.QueryRowContext(ctx, `
        SELECT o.operation_kind, ts.request_sha256,
               lt.ledger_transaction_id, lt.state, lt.reference_id,
               tf.collector_account_id, tf.quote_asset,
               tf.buyer_fee_atoms::text, tf.seller_fee_atoms::text
        FROM accounting.operations AS o
        LEFT JOIN accounting.trade_settlements AS ts
          ON ts.operation_id = o.operation_id
        LEFT JOIN accounting.ledger_transactions AS lt
          ON lt.operation_id = o.operation_id
        LEFT JOIN accounting.trade_fees AS tf
          ON tf.trade_id = ts.trade_id
        WHERE o.operation_id = $1`, request.operationID()).Scan(
		&kind, &digest, &ledgerID, &state, &reference,
		&collector, &feeAsset, &buyerFee, &sellerFee)
	if err != nil {
		return Result{}, fmt.Errorf("read prior settlement: %w", err)
	}
	expected := request.fingerprint()
	if kind != "SETTLE_TRADE" || !bytes.Equal(digest, expected[:]) {
		return Result{}, fmt.Errorf("%w: operation_id reused with different settlement semantics", ErrIdempotencyConflict)
	}
	if !ledgerID.Valid || state.String != "POSTED" || reference.String != request.TradeID {
		return Result{}, fmt.Errorf("%w: committed operation lacks a valid posted ledger receipt", ErrInvariant)
	}
	if request.Fees.enabled() {
		if !collector.Valid || collector.String != request.Fees.CollectorAccountID ||
			feeAsset.String != request.QuoteAsset ||
			buyerFee.String != strconv.FormatUint(request.Fees.BuyerAtoms, 10) ||
			sellerFee.String != strconv.FormatUint(request.Fees.SellerAtoms, 10) {
			return Result{}, fmt.Errorf("%w: fee-bearing posting is missing its immutable fee receipt", ErrInvariant)
		}
	} else if collector.Valid {
		return Result{}, fmt.Errorf("%w: historical no-fee posting has unexpected fee receipt", ErrInvariant)
	}
	return Result{OperationID: request.operationID(), LedgerTransactionID: ledgerID.Int64, AlreadyApplied: true}, nil
}

type balanceKey struct{ account, asset string }

func lockBalanceRows(ctx context.Context, tx *sql.Tx, entries []Entry) error {
	distinct := make(map[balanceKey]struct{}, len(entries))
	for _, entry := range entries {
		distinct[balanceKey{entry.AccountID, entry.Asset}] = struct{}{}
	}
	keys := make([]balanceKey, 0, len(distinct))
	for key := range distinct {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].account == keys[j].account {
			return keys[i].asset < keys[j].asset
		}
		return keys[i].account < keys[j].account
	})
	for _, key := range keys {
		// Recipient balances may not exist before their first received trade.
		// No economic value is created by inserting an all-zero projection row.
		_, err := tx.ExecContext(ctx, `
            INSERT INTO accounting.balances (account_id, asset_code)
            VALUES ($1, $2) ON CONFLICT (account_id, asset_code) DO NOTHING`,
			key.account, key.asset)
		if err != nil {
			return fmt.Errorf("ensure balance %q/%q: %w", key.account, key.asset, err)
		}
		var locked int
		err = tx.QueryRowContext(ctx, `
            SELECT 1 FROM accounting.balances
            WHERE account_id = $1 AND asset_code = $2 FOR UPDATE`,
			key.account, key.asset).Scan(&locked)
		if err != nil {
			return fmt.Errorf("lock balance %q/%q: %w", key.account, key.asset, err)
		}
	}
	return nil
}

type reservationUse struct {
	id, account, order, asset string
	consume                   uint64
	close                     bool
	release                   uint64
}

func (use *reservationUse) lockAndValidate(ctx context.Context, tx *sql.Tx) error {
	var accountID, asset, kind, orderID, remainingText, status string
	err := tx.QueryRowContext(ctx, `
        SELECT account_id, asset_code, reference_kind, reference_id,
               remaining_atoms::text, status
        FROM accounting.balance_reservations
        WHERE reservation_id = $1::uuid
        FOR UPDATE`, use.id).Scan(&accountID, &asset, &kind, &orderID, &remainingText, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: reservation %s not found", ErrReservationMismatch, use.id)
	}
	if err != nil {
		return fmt.Errorf("lock reservation: %w", err)
	}
	if accountID != use.account || asset != use.asset || kind != "ORDER" || orderID != use.order || status != "ACTIVE" {
		return fmt.Errorf("%w: reservation %s ownership/asset/order/status", ErrReservationMismatch, use.id)
	}
	remaining, err := strconv.ParseUint(remainingText, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid reserved atom magnitude: %v", ErrInvariant, err)
	}
	if remaining < use.consume {
		return fmt.Errorf("%w: reservation %s cannot cover %d atoms", ErrInsufficientReserved, use.id, use.consume)
	}
	if use.close {
		use.release = remaining - use.consume
	}
	return nil
}

func (use reservationUse) apply(ctx context.Context, tx *sql.Tx, operationID string) error {
	consume := strconv.FormatUint(use.consume, 10)
	release := strconv.FormatUint(use.release, 10)

	// This writes only reserved_atoms; posted_atoms is exclusively updated by
	// the pre-existing ledger-entry trigger in migration 000001.
	result, err := tx.ExecContext(ctx, `
        UPDATE accounting.balances
        SET reserved_atoms = reserved_atoms - ($3::numeric + $4::numeric),
            updated_at = transaction_timestamp()
        WHERE account_id = $1 AND asset_code = $2
          AND reserved_atoms >= ($3::numeric + $4::numeric)`,
		use.account, use.asset, consume, release)
	if err != nil {
		return fmt.Errorf("decrement aggregate reserved balance: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect reserved balance update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: aggregate reservation underflow", ErrInvariant)
	}

	result, err = tx.ExecContext(ctx, `
        UPDATE accounting.balance_reservations
        SET remaining_atoms = remaining_atoms - ($2::numeric + $3::numeric),
            consumed_atoms = consumed_atoms + $2::numeric,
            released_atoms = released_atoms + $3::numeric,
            status = CASE
                WHEN remaining_atoms = ($2::numeric + $3::numeric) AND $3::numeric > 0 THEN 'RELEASED'
                WHEN remaining_atoms = ($2::numeric + $3::numeric) THEN 'CONSUMED'
                ELSE 'ACTIVE'
            END,
            closed_at = CASE
                WHEN remaining_atoms = ($2::numeric + $3::numeric)
                THEN transaction_timestamp() ELSE NULL
            END
        WHERE reservation_id = $1::uuid AND status = 'ACTIVE'
          AND remaining_atoms >= ($2::numeric + $3::numeric)`, use.id, consume, release)
	if err != nil {
		return fmt.Errorf("consume reservation: %w", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect reservation update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: unexpected reservation concurrent change", ErrInvariant)
	}

	_, err = tx.ExecContext(ctx, `
        INSERT INTO accounting.balance_reservation_mutations
            (operation_id, reservation_id, mutation_kind, amount_atoms)
        VALUES ($1, $2::uuid, 'CONSUMED', $3::numeric)`, operationID, use.id, consume)
	if err != nil {
		return fmt.Errorf("append reservation consumption audit: %w", err)
	}
	if use.release != 0 {
		_, err = tx.ExecContext(ctx, `
            INSERT INTO accounting.balance_reservation_mutations
                (operation_id, reservation_id, mutation_kind, amount_atoms)
            VALUES ($1, $2::uuid, 'RELEASED', $3::numeric)`, operationID, use.id, release)
		if err != nil {
			return fmt.Errorf("append remainder release audit: %w", err)
		}
	}
	return nil
}
