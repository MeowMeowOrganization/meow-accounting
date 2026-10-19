package reservation

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Repository is Accounting's PostgreSQL-owned reservation write model.
// Like CEX14, transactions run at READ COMMITTED and take balance locks before
// reservation locks. Callers must authenticate and authorize the Order Service.
type Repository struct{ db *sql.DB }

var (
	ErrInvalidRequest      = errors.New("invalid reservation request")
	ErrInsufficientFunds   = errors.New("insufficient available funds")
	ErrIdempotencyConflict = errors.New("reservation operation idempotency conflict")
	ErrReservationMismatch = errors.New("reservation ownership mismatch or not found")
	ErrReservationClosed   = errors.New("reservation has no releasable remainder")
	ErrInvariant           = errors.New("reservation accounting invariant violation")
)

var reservationUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func identity(s string) bool { return s != "" && s == strings.TrimSpace(s) && len(s) <= 256 }

// ReserveRequest is an authorized, already-calculated *single-asset* hold.
// The caller must compute Atoms with CalculateLimit and trusted market/fee data.
// OperationID is stable across retries, not a Kafka offset or HTTP request ID.
// OrderID is the durable canonical Order Service order identifier.
type ReserveRequest struct {
	OperationID string
	AccountID   string
	OrderID     string
	AssetCode   string
	Atoms       uint64
}

func (r ReserveRequest) Validate() error {
	if !identity(r.OperationID) || !identity(r.AccountID) || !identity(r.OrderID) ||
		!identity(r.AssetCode) || r.Atoms == 0 {
		return fmt.Errorf("%w: require canonical operation/account/order/asset and positive atoms", ErrInvalidRequest)
	}
	return nil
}

type ReserveResult struct {
	OperationID    string
	ReservationID  string
	AssetCode      string
	OriginalAtoms  uint64
	AlreadyApplied bool
}

// ReleaseRequest is authorized only after a terminal matching/order decision
// and all earlier executions have been durably settled in event order.
// Those cross-service ordering guarantees CANNOT be inferred from this database.
// There is no caller-supplied release amount: always release the whole remainder.
type ReleaseRequest struct {
	OperationID   string
	ReservationID string
	AccountID     string
	OrderID       string
	AssetCode     string
}

func (r ReleaseRequest) Validate() error {
	if !identity(r.OperationID) || !identity(r.AccountID) || !identity(r.OrderID) ||
		!identity(r.AssetCode) || !reservationUUID.MatchString(r.ReservationID) {
		return fmt.Errorf("%w: require canonical release identities and UUID", ErrInvalidRequest)
	}
	return nil
}

type ReleaseResult struct {
	OperationID    string
	ReservationID  string
	ReleasedAtoms  uint64
	AlreadyApplied bool
}

func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", ErrInvalidRequest)
	}
	return &Repository{db: db}, nil
}

// ReserveFunds commits the operation identity, balance hold, reservation row,
// and immutable CREATED audit mutation in one transaction. No ledger entry is
// created because reservation does not change economic ownership.
func (r *Repository) ReserveFunds(ctx context.Context, request ReserveRequest) (ReserveResult, error) {
	if err := request.Validate(); err != nil {
		return ReserveResult{}, err
	}
	if ctx == nil {
		return ReserveResult{}, fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ReserveResult{}, fmt.Errorf("begin reservation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	claimed, err := claimOperation(ctx, tx, request.OperationID, "RESERVE_FUNDS")
	if err != nil {
		return ReserveResult{}, err
	}
	if !claimed {
		previous, lookupErr := priorReserve(ctx, tx, request)
		if lookupErr != nil {
			return ReserveResult{}, lookupErr
		}
		if err := tx.Commit(); err != nil {
			return ReserveResult{}, fmt.Errorf("commit reservation replay lookup: %w", err)
		}
		return previous, nil
	}

	// Fail explicitly if the account or asset is missing. A second locked
	// check below ensures the flags cannot change before we commit.
	var kind string
	var accountActive, assetActive bool
	err = tx.QueryRowContext(ctx, `
        SELECT a.account_kind, a.is_active, s.is_active
        FROM accounting.accounts a CROSS JOIN accounting.assets s
        WHERE a.account_id=$1 AND s.asset_code=$2`, request.AccountID, request.AssetCode).
		Scan(&kind, &accountActive, &assetActive)
	if errors.Is(err, sql.ErrNoRows) {
		return ReserveResult{}, fmt.Errorf("%w: unknown account or asset", ErrInvalidRequest)
	}
	if err != nil {
		return ReserveResult{}, fmt.Errorf("check account and asset: %w", err)
	}

	// Exactly the same ordering as CEX14 lockBalanceRows: first create a
	// zero projection if absent, then lock the balance row. Two concurrent
	// reservations must serialize on that single (account,asset) row.
	_, err = tx.ExecContext(ctx, `
        INSERT INTO accounting.balances (account_id,asset_code)
        VALUES ($1,$2) ON CONFLICT (account_id,asset_code) DO NOTHING`, request.AccountID, request.AssetCode)
	if err != nil {
		return ReserveResult{}, fmt.Errorf("ensure balance projection: %w", err)
	}
	var locked int
	err = tx.QueryRowContext(ctx, `
        SELECT 1 FROM accounting.balances
        WHERE account_id=$1 AND asset_code=$2 FOR UPDATE`, request.AccountID, request.AssetCode).Scan(&locked)
	if err != nil {
		return ReserveResult{}, fmt.Errorf("lock balance: %w", err)
	}

	// The active flags remain stable until COMMIT (FOR SHARE of metadata).
	err = tx.QueryRowContext(ctx, `
        SELECT a.account_kind, a.is_active, s.is_active
        FROM accounting.accounts a CROSS JOIN accounting.assets s
        WHERE a.account_id=$1 AND s.asset_code=$2
        FOR SHARE OF a,s`, request.AccountID, request.AssetCode).
		Scan(&kind, &accountActive, &assetActive)
	if err != nil {
		return ReserveResult{}, fmt.Errorf("lock account/asset metadata: %w", err)
	}
	if kind != "CUSTOMER" || !accountActive || !assetActive {
		return ReserveResult{}, fmt.Errorf("%w: reservation requires active CUSTOMER account and active asset", ErrInvalidRequest)
	}

	amount := strconv.FormatUint(request.Atoms, 10)
	update, err := tx.ExecContext(ctx, `
        UPDATE accounting.balances
        SET reserved_atoms = reserved_atoms + $3::numeric,
            updated_at = transaction_timestamp()
        WHERE account_id=$1 AND asset_code=$2
          AND posted_atoms - reserved_atoms >= $3::numeric`, request.AccountID, request.AssetCode, amount)
	if err != nil {
		return ReserveResult{}, fmt.Errorf("apply reservation hold: %w", err)
	}
	affected, err := update.RowsAffected()
	if err != nil {
		return ReserveResult{}, fmt.Errorf("inspect reserved update: %w", err)
	}
	if affected != 1 {
		return ReserveResult{}, fmt.Errorf("%w: available balance below %s atoms", ErrInsufficientFunds, amount)
	}

	id, err := newUUID()
	if err != nil {
		return ReserveResult{}, fmt.Errorf("generate reservation UUID: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
        INSERT INTO accounting.balance_reservations
          (reservation_id,created_operation_id,account_id,asset_code,
           reference_kind,reference_id,original_atoms,remaining_atoms,status)
        VALUES ($1::uuid,$2,$3,$4,'ORDER',$5,$6::numeric,$6::numeric,'ACTIVE')`,
		id, request.OperationID, request.AccountID, request.AssetCode, request.OrderID, amount)
	if isUniqueViolation(err) {
		return ReserveResult{}, fmt.Errorf("%w: order or reservation identity already used", ErrIdempotencyConflict)
	}
	if err != nil {
		return ReserveResult{}, fmt.Errorf("insert reservation: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
        INSERT INTO accounting.balance_reservation_mutations
          (operation_id,reservation_id,mutation_kind,amount_atoms)
        VALUES ($1,$2::uuid,'CREATED',$3::numeric)`, request.OperationID, id, amount)
	if err != nil {
		return ReserveResult{}, fmt.Errorf("insert creation audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ReserveResult{}, fmt.Errorf("commit reserve (retry identical operation if uncertain): %w", err)
	}
	return ReserveResult{OperationID: request.OperationID, ReservationID: id, AssetCode: request.AssetCode, OriginalAtoms: request.Atoms}, nil
}

// ReleaseFunds atomically returns ONLY unused remaining atoms to spendable
// balance. It is safe to race with CEX14 settlement at the DB locking level:
// both paths lock the balance before the reservation. A release that wins first
// makes a later fill fail; Order Service MUST prevent premature release.
func (r *Repository) ReleaseFunds(ctx context.Context, request ReleaseRequest) (ReleaseResult, error) {
	if err := request.Validate(); err != nil {
		return ReleaseResult{}, err
	}
	if ctx == nil {
		return ReleaseResult{}, fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("begin release: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	claimed, err := claimOperation(ctx, tx, request.OperationID, "RELEASE_RESERVATION")
	if err != nil {
		return ReleaseResult{}, err
	}
	if !claimed {
		previous, lookupErr := priorRelease(ctx, tx, request)
		if lookupErr != nil {
			return ReleaseResult{}, lookupErr
		}
		if err := tx.Commit(); err != nil {
			return ReleaseResult{}, fmt.Errorf("commit release replay lookup: %w", err)
		}
		return previous, nil
	}

	// Lock the balance first, exactly as settlement and ReserveFunds do.
	var locked int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM accounting.balances
        WHERE account_id=$1 AND asset_code=$2 FOR UPDATE`, request.AccountID, request.AssetCode).Scan(&locked)
	if errors.Is(err, sql.ErrNoRows) {
		return ReleaseResult{}, fmt.Errorf("%w: balance not found", ErrReservationMismatch)
	}
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("lock release balance: %w", err)
	}

	var account, asset, order, kind, status, remainingText string
	err = tx.QueryRowContext(ctx, `
        SELECT account_id,asset_code,reference_id,reference_kind,status,remaining_atoms::text
        FROM accounting.balance_reservations
        WHERE reservation_id=$1::uuid FOR UPDATE`, request.ReservationID).
		Scan(&account, &asset, &order, &kind, &status, &remainingText)
	if errors.Is(err, sql.ErrNoRows) {
		return ReleaseResult{}, fmt.Errorf("%w: reservation not found", ErrReservationMismatch)
	}
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("lock release reservation: %w", err)
	}
	if account != request.AccountID || asset != request.AssetCode ||
		kind != "ORDER" || order != request.OrderID {
		return ReleaseResult{}, fmt.Errorf("%w: account/asset/order mismatch", ErrReservationMismatch)
	}
	if status != "ACTIVE" {
		return ReleaseResult{}, fmt.Errorf("%w: status %s", ErrReservationClosed, status)
	}
	remaining, err := strconv.ParseUint(remainingText, 10, 64)
	if err != nil || remaining == 0 {
		return ReleaseResult{}, fmt.Errorf("%w: invalid active remainder %q", ErrInvariant, remainingText)
	}
	amount := strconv.FormatUint(remaining, 10)

	update, err := tx.ExecContext(ctx, `
        UPDATE accounting.balances
        SET reserved_atoms=reserved_atoms-$3::numeric,
            updated_at=transaction_timestamp()
        WHERE account_id=$1 AND asset_code=$2 AND reserved_atoms >= $3::numeric`,
		request.AccountID, request.AssetCode, amount)
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("release aggregate hold: %w", err)
	}
	affected, err := update.RowsAffected()
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("inspect release balance update: %w", err)
	}
	if affected != 1 {
		return ReleaseResult{}, fmt.Errorf("%w: reserved balance underflow", ErrInvariant)
	}

	update, err = tx.ExecContext(ctx, `
        UPDATE accounting.balance_reservations
        SET remaining_atoms=0, released_atoms=released_atoms+$2::numeric,
            status='RELEASED', closed_at=transaction_timestamp(),
            updated_at=transaction_timestamp()
        WHERE reservation_id=$1::uuid AND status='ACTIVE' AND remaining_atoms=$2::numeric`,
		request.ReservationID, amount)
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("close reservation: %w", err)
	}
	affected, err = update.RowsAffected()
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("inspect release reservation update: %w", err)
	}
	if affected != 1 {
		return ReleaseResult{}, fmt.Errorf("%w: reservation changed unexpectedly", ErrInvariant)
	}
	_, err = tx.ExecContext(ctx, `
        INSERT INTO accounting.balance_reservation_mutations
          (operation_id,reservation_id,mutation_kind,amount_atoms)
        VALUES ($1,$2::uuid,'RELEASED',$3::numeric)`, request.OperationID, request.ReservationID, amount)
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("append release audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ReleaseResult{}, fmt.Errorf("commit release (retry identical operation if uncertain): %w", err)
	}
	return ReleaseResult{OperationID: request.OperationID, ReservationID: strings.ToLower(request.ReservationID), ReleasedAtoms: remaining}, nil
}

func claimOperation(ctx context.Context, tx *sql.Tx, id, kind string) (bool, error) {
	var claimed string
	err := tx.QueryRowContext(ctx, `
        INSERT INTO accounting.operations (operation_id,operation_kind)
        VALUES ($1,$2) ON CONFLICT (operation_id) DO NOTHING RETURNING operation_id`, id, kind).Scan(&claimed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim %s operation: %w", kind, err)
	}
	return true, nil
}

// Inspect immutable create fields, rather than mutable remaining/status: a
// legitimate later fill/cancel must not make an identical reserve retry fail.
func priorReserve(ctx context.Context, tx *sql.Tx, request ReserveRequest) (ReserveResult, error) {
	var kind, id, account, asset, order, original, auditAmount string
	err := tx.QueryRowContext(ctx, `
        SELECT o.operation_kind, r.reservation_id::text,r.account_id,r.asset_code,
               r.reference_id,r.original_atoms::text,m.amount_atoms::text
        FROM accounting.operations o
        LEFT JOIN accounting.balance_reservations r ON r.created_operation_id=o.operation_id
        LEFT JOIN accounting.balance_reservation_mutations m
          ON m.operation_id=o.operation_id AND m.reservation_id=r.reservation_id AND m.mutation_kind='CREATED'
        WHERE o.operation_id=$1`, request.OperationID).
		Scan(&kind, &id, &account, &asset, &order, &original, &auditAmount)
	if errors.Is(err, sql.ErrNoRows) {
		return ReserveResult{}, fmt.Errorf("%w: operation not found", ErrInvariant)
	}
	// A prior operation of another kind may have no reservation; NULL scans
	// produce an error. Diagnose as a conflict if kind differs (see below).
	if err != nil {
		var existingKind string
		kindErr := tx.QueryRowContext(ctx, `SELECT operation_kind FROM accounting.operations WHERE operation_id=$1`, request.OperationID).Scan(&existingKind)
		if kindErr == nil && existingKind != "RESERVE_FUNDS" {
			return ReserveResult{}, fmt.Errorf("%w: operation kind already %s", ErrIdempotencyConflict, existingKind)
		}
		return ReserveResult{}, fmt.Errorf("%w: missing/malformed previous reserve receipt: %v", ErrInvariant, err)
	}
	if kind != "RESERVE_FUNDS" || account != request.AccountID || asset != request.AssetCode ||
		order != request.OrderID || original != strconv.FormatUint(request.Atoms, 10) {
		return ReserveResult{}, fmt.Errorf("%w: same operation ID has different reserve semantics", ErrIdempotencyConflict)
	}
	if auditAmount != original {
		return ReserveResult{}, fmt.Errorf("%w: missing original reservation audit", ErrInvariant)
	}
	return ReserveResult{OperationID: request.OperationID, ReservationID: id, AssetCode: asset, OriginalAtoms: request.Atoms, AlreadyApplied: true}, nil
}

func priorRelease(ctx context.Context, tx *sql.Tx, request ReleaseRequest) (ReleaseResult, error) {
	var kind, id, account, asset, order, amount string
	err := tx.QueryRowContext(ctx, `
        SELECT o.operation_kind,m.reservation_id::text,r.account_id,r.asset_code,
               r.reference_id,m.amount_atoms::text
        FROM accounting.operations o
        LEFT JOIN accounting.balance_reservation_mutations m
          ON m.operation_id=o.operation_id AND m.mutation_kind='RELEASED'
        LEFT JOIN accounting.balance_reservations r ON r.reservation_id=m.reservation_id
        WHERE o.operation_id=$1`, request.OperationID).
		Scan(&kind, &id, &account, &asset, &order, &amount)
	if errors.Is(err, sql.ErrNoRows) {
		return ReleaseResult{}, fmt.Errorf("%w: prior operation vanished", ErrInvariant)
	}
	if err != nil {
		var existingKind string
		kindErr := tx.QueryRowContext(ctx, `SELECT operation_kind FROM accounting.operations WHERE operation_id=$1`, request.OperationID).Scan(&existingKind)
		if kindErr == nil && existingKind != "RELEASE_RESERVATION" {
			return ReleaseResult{}, fmt.Errorf("%w: operation kind already %s", ErrIdempotencyConflict, existingKind)
		}
		return ReleaseResult{}, fmt.Errorf("%w: missing/malformed release receipt: %v", ErrInvariant, err)
	}
	if kind != "RELEASE_RESERVATION" || !strings.EqualFold(id, request.ReservationID) ||
		account != request.AccountID || asset != request.AssetCode || order != request.OrderID {
		return ReleaseResult{}, fmt.Errorf("%w: same operation ID has different release semantics", ErrIdempotencyConflict)
	}
	released, err := strconv.ParseUint(amount, 10, 64)
	if err != nil {
		return ReleaseResult{}, fmt.Errorf("%w: invalid released amount: %v", ErrInvariant, err)
	}
	return ReleaseResult{OperationID: request.OperationID, ReservationID: id, ReleasedAtoms: released, AlreadyApplied: true}, nil
}

// Errors returned by pgx/stdlib expose SQLState(); do not couple the domain
// package to a particular PostgreSQL driver concrete error type.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	var state interface{ SQLState() string }
	return errors.As(err, &state) && state.SQLState() == "23505"
}

func newUUID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	data[6] = (data[6] & 0x0f) | 0x40
	data[8] = (data[8] & 0x3f) | 0x80
	h := hex.EncodeToString(data[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:]), nil
}
