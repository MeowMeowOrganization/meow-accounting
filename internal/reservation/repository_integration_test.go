//go:build integration

package reservation

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/MeowMeowOrganization/meow-accounting/internal/settlement"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Tests intentionally never DROP/TRUNCATE anything. Use ONLY a disposable
// database with CEX13 migrations 000001/000002 and CEX14 000003/000004.
func pgDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CEX15_TEST_DSN")
	if dsn == "" {
		t.Skip("set CEX15_TEST_DSN to a disposable PostgreSQL database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(32)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var schemaReady bool
	err = db.QueryRowContext(ctx, `SELECT
        to_regclass('accounting.balance_reservations') IS NOT NULL
        AND to_regclass('accounting.balance_reservation_mutations') IS NOT NULL
        AND to_regclass('accounting.trade_settlements') IS NOT NULL
        AND to_regclass('accounting.trade_fees') IS NOT NULL`).Scan(&schemaReady)
	if err != nil || !schemaReady {
		t.Fatalf("apply migrations 000001-000004: %v", err)
	}
	return db
}

func token(t *testing.T) string {
	t.Helper()
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

func mustExec(t *testing.T, tx *sql.Tx, q string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(q, args...); err != nil {
		t.Fatalf("fixture SQL: %v", err)
	}
}

// A balanced, committed ledger posting is the ONLY source of fixture money.
// This function never directly writes posted_atoms or reserved_atoms.
func fundedAccount(t *testing.T, db *sql.DB, asset string, atoms uint64) (account string) {
	t.Helper()
	suffix := token(t)
	account = "reserve-customer-" + suffix
	system := "reserve-system-" + suffix
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	mustExec(t, tx, `INSERT INTO accounting.assets(asset_code,scale)
        VALUES('USDT',6),('BTC',8) ON CONFLICT DO NOTHING`)
	mustExec(t, tx, `INSERT INTO accounting.accounts(account_id,account_kind)
        VALUES($1,'CUSTOMER'),($2,'SYSTEM')`, account, system)
	operation := "TEST_SEED:" + suffix
	mustExec(t, tx, `INSERT INTO accounting.operations(operation_id,operation_kind)
        VALUES($1,'TEST_SEED')`, operation)
	var ledger int64
	err = tx.QueryRow(`INSERT INTO accounting.ledger_transactions
        (operation_id,reference_type,reference_id)
        VALUES($1,'TEST_SEED',$2) RETURNING ledger_transaction_id`, operation, suffix).Scan(&ledger)
	if err != nil {
		t.Fatal(err)
	}
	amount := strconv.FormatUint(atoms, 10)
	mustExec(t, tx, `INSERT INTO accounting.ledger_entries
        (ledger_transaction_id,entry_index,account_id,asset_code,amount_atoms)
        VALUES($1,0,$2,$3,-$4::numeric),($1,1,$5,$3,$4::numeric)`, ledger, system, asset, amount, account)
	mustExec(t, tx, `UPDATE accounting.ledger_transactions
        SET state='POSTED',posted_at=transaction_timestamp() WHERE ledger_transaction_id=$1`, ledger)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return account
}

func reserveRequest(account, asset, order string, atoms uint64) ReserveRequest {
	return ReserveRequest{OperationID: "RESERVE_FUNDS:" + order, AccountID: account, OrderID: order, AssetCode: asset, Atoms: atoms}
}

func releaseRequest(res ReserveResult, req ReserveRequest) ReleaseRequest {
	return ReleaseRequest{
		OperationID:   "RELEASE_RESERVATION:" + req.OrderID,
		ReservationID: res.ReservationID, AccountID: req.AccountID,
		OrderID: req.OrderID, AssetCode: req.AssetCode,
	}
}

func assertAmounts(t *testing.T, db *sql.DB, account, asset, posted, reserved string) {
	t.Helper()
	var p, r, a string
	err := db.QueryRow(`SELECT posted_atoms::text,reserved_atoms::text,available_atoms::text
        FROM accounting.balances WHERE account_id=$1 AND asset_code=$2`, account, asset).Scan(&p, &r, &a)
	if err != nil {
		t.Fatal(err)
	}
	if p != posted || r != reserved {
		t.Fatalf("%s/%s: posted=%s reserved=%s available=%s (want %s/%s)", account, asset, p, r, a, posted, reserved)
	}
}

// Reconciles aggregate hold against ALL still active reservations, not just
// those created within an individual test.
func assertReconciled(t *testing.T, db *sql.DB, account, asset string) {
	t.Helper()
	var aggregate, activeSum string
	err := db.QueryRow(`
        SELECT b.reserved_atoms::text,COALESCE(SUM(r.remaining_atoms),0)::text
        FROM accounting.balances b
        LEFT JOIN accounting.balance_reservations r
          ON r.account_id=b.account_id AND r.asset_code=b.asset_code AND r.status='ACTIVE'
        WHERE b.account_id=$1 AND b.asset_code=$2
        GROUP BY b.reserved_atoms`, account, asset).Scan(&aggregate, &activeSum)
	if err != nil {
		t.Fatal(err)
	}
	if aggregate != activeSum {
		t.Fatalf("aggregate reserved=%s; sum(ACTIVE remainder)=%s", aggregate, activeSum)
	}
}

func countOperation(t *testing.T, db *sql.DB, id string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM accounting.operations WHERE operation_id=$1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostgresReserveReplayAndSemanticConflicts(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "USDT", 2000)
	repo, _ := NewRepository(db)
	req := reserveRequest(account, "USDT", "buy-"+token(t), 650)
	first, err := repo.ReserveFunds(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.AlreadyApplied || first.OriginalAtoms != 650 || !reservationUUID.MatchString(first.ReservationID) {
		t.Fatalf("first result: %+v", first)
	}
	replay, err := repo.ReserveFunds(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.AlreadyApplied || replay.ReservationID != first.ReservationID {
		t.Fatalf("unexpected replay: %+v", replay)
	}
	assertAmounts(t, db, account, "USDT", "2000", "650")
	assertReconciled(t, db, account, "USDT")
	changed := req
	changed.Atoms = 651
	if _, err := repo.ReserveFunds(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	changed = req
	changed.OperationID = "RESERVE_FUNDS:duplicate-" + token(t)
	if _, err := repo.ReserveFunds(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same order different op must conflict: %v", err)
	}
	if countOperation(t, db, changed.OperationID) != 0 {
		t.Fatal("conflicting operation must roll back")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM accounting.balance_reservation_mutations
        WHERE reservation_id=$1::uuid AND mutation_kind='CREATED'`, first.ReservationID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("duplicate creation audit count %d", n)
	}
}

func TestPostgresInsufficientFundsRollsBackEverything(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "USDT", 1000)
	repo, _ := NewRepository(db)
	req := reserveRequest(account, "USDT", "too-large-"+token(t), 1001)
	for i := 0; i < 2; i++ {
		if _, err := repo.ReserveFunds(context.Background(), req); !errors.Is(err, ErrInsufficientFunds) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if countOperation(t, db, req.OperationID) != 0 {
		t.Fatal("failed hold left a committed operation")
	}
	assertAmounts(t, db, account, "USDT", "1000", "0")
	assertReconciled(t, db, account, "USDT")
}

func TestPostgresReserveConcurrentNoOverspend(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "USDT", 1000)
	repo, _ := NewRepository(db)
	const workers = 20
	start := make(chan struct{})
	outcomes := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			<-start
			_, err := repo.ReserveFunds(ctx, reserveRequest(account, "USDT", fmt.Sprintf("concurrent-%s-%02d", account, i), 150))
			outcomes <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(outcomes)
	var accepted, insufficient int
	for err := range outcomes {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrInsufficientFunds):
			insufficient++
		default:
			t.Fatalf("unexpected concurrent reservation result: %v", err)
		}
	}
	if accepted != 6 || insufficient != 14 {
		t.Fatalf("accepted=%d insufficient=%d expected 6/14", accepted, insufficient)
	}
	assertAmounts(t, db, account, "USDT", "1000", "900")
	assertReconciled(t, db, account, "USDT")
}

func TestPostgresSameKeyConcurrentOnlyOneHold(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "USDT", 1000)
	repo, _ := NewRepository(db)
	req := reserveRequest(account, "USDT", "same-"+token(t), 600)
	const workers = 12
	start := make(chan struct{})
	type outcome struct {
		result ReserveResult
		err    error
	}
	results := make(chan outcome, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			<-start
			res, err := repo.ReserveFunds(ctx, req)
			results <- outcome{res, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var id string
	fresh := 0
	for out := range results {
		if out.err != nil {
			t.Fatal(out.err)
		}
		if id == "" {
			id = out.result.ReservationID
		}
		if id != out.result.ReservationID {
			t.Fatalf("different IDs on replay: %s != %s", id, out.result.ReservationID)
		}
		if !out.result.AlreadyApplied {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("one first application expected, got %d", fresh)
	}
	assertAmounts(t, db, account, "USDT", "1000", "600")
	assertReconciled(t, db, account, "USDT")
}

func TestPostgresReleaseRemainderReplayAndErrors(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "BTC", 1000)
	repo, _ := NewRepository(db)
	req := reserveRequest(account, "BTC", "sell-"+token(t), 800)
	reservation, err := repo.ReserveFunds(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	release := releaseRequest(reservation, req)
	done, err := repo.ReleaseFunds(context.Background(), release)
	if err != nil {
		t.Fatal(err)
	}
	if done.ReleasedAtoms != 800 || done.AlreadyApplied {
		t.Fatalf("bad release: %+v", done)
	}
	repeated, err := repo.ReleaseFunds(context.Background(), release)
	if err != nil || !repeated.AlreadyApplied || repeated.ReleasedAtoms != 800 {
		t.Fatalf("bad release retry: %+v %v", repeated, err)
	}
	mismatched := release
	mismatched.OrderID = "wrong-order"
	if _, err := repo.ReleaseFunds(context.Background(), mismatched); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("release replay with mismatched order: %v", err)
	}
	other := release
	other.OperationID += "-other"
	if _, err := repo.ReleaseFunds(context.Background(), other); !errors.Is(err, ErrReservationClosed) {
		t.Fatalf("expected terminal reservation: %v", err)
	}
	if countOperation(t, db, other.OperationID) != 0 {
		t.Fatal("failed second release must roll back")
	}
	assertAmounts(t, db, account, "BTC", "1000", "0")
	assertReconciled(t, db, account, "BTC")
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM accounting.balance_reservation_mutations
       WHERE reservation_id=$1::uuid AND mutation_kind='RELEASED'`, reservation.ReservationID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected one release audit row, got %d", n)
	}
}

func TestPostgresFullUint64AmountExact(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "USDT", math.MaxUint64)
	repo, _ := NewRepository(db)
	req := reserveRequest(account, "USDT", "max-"+token(t), math.MaxUint64)
	result, err := repo.ReserveFunds(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	all := strconv.FormatUint(math.MaxUint64, 10)
	assertAmounts(t, db, account, "USDT", all, all)
	release, err := repo.ReleaseFunds(context.Background(), releaseRequest(result, req))
	if err != nil || release.ReleasedAtoms != math.MaxUint64 {
		t.Fatalf("release full uint64: %+v %v", release, err)
	}
	assertAmounts(t, db, account, "USDT", all, "0")
	assertReconciled(t, db, account, "USDT")
}

// Demonstrates interoperability with the real CEX14 transaction path, rather
// than writing directly to mutable reservation columns as a test shortcut.
func TestPostgresPartialSettlementThenRelease(t *testing.T) {
	db := pgDB(t)
	buyer := fundedAccount(t, db, "USDT", 1000)
	seller := fundedAccount(t, db, "BTC", 500)
	repo, _ := NewRepository(db)
	settle, _ := settlement.NewRepository(db)
	buyReq := reserveRequest(buyer, "USDT", "buy-"+token(t), 900)
	sellReq := reserveRequest(seller, "BTC", "sell-"+token(t), 400)
	buy, err := repo.ReserveFunds(context.Background(), buyReq)
	if err != nil {
		t.Fatal(err)
	}
	sell, err := repo.ReserveFunds(context.Background(), sellReq)
	if err != nil {
		t.Fatal(err)
	}
	trade := settlement.Request{
		TradeID: "trade-" + token(t), MarketID: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT",
		Buyer:     settlement.Participant{AccountID: buyer, OrderID: buyReq.OrderID, ReservationID: buy.ReservationID},
		Seller:    settlement.Participant{AccountID: seller, OrderID: sellReq.OrderID, ReservationID: sell.ReservationID},
		BaseAtoms: 100, QuoteAtoms: 500, CloseBuyer: false, CloseSeller: false,
	}
	if _, err := settle.SettleTrade(context.Background(), trade); err != nil {
		t.Fatal(err)
	}
	assertAmounts(t, db, buyer, "USDT", "500", "400")
	assertAmounts(t, db, seller, "BTC", "400", "300")
	// A later terminal acknowledgement makes release of both unused holds safe.
	bRel, err := repo.ReleaseFunds(context.Background(), releaseRequest(buy, buyReq))
	if err != nil {
		t.Fatal(err)
	}
	sRel, err := repo.ReleaseFunds(context.Background(), releaseRequest(sell, sellReq))
	if err != nil {
		t.Fatal(err)
	}
	if bRel.ReleasedAtoms != 400 || sRel.ReleasedAtoms != 300 {
		t.Fatalf("remainders %d/%d", bRel.ReleasedAtoms, sRel.ReleasedAtoms)
	}
	assertAmounts(t, db, buyer, "USDT", "500", "0")
	assertAmounts(t, db, seller, "BTC", "400", "0")
	assertReconciled(t, db, buyer, "USDT")
	assertReconciled(t, db, seller, "BTC")
	// The same trade receipt can replay after the unused remainder was released.
	if result, err := settle.SettleTrade(context.Background(), trade); err != nil || !result.AlreadyApplied {
		t.Fatalf("settlement replay: %+v %v", result, err)
	}
}

// A database race is serialized, not a double spend. Release is only valid
// after an upstream terminal barrier; this test purposely violates that rule
// to verify that CEX14 never settles against already released money.
func TestPostgresReleaseVsSettlementRaceNoDoubleSpend(t *testing.T) {
	db := pgDB(t)
	buyer := fundedAccount(t, db, "USDT", 1000)
	seller := fundedAccount(t, db, "BTC", 500)
	repo, _ := NewRepository(db)
	settle, _ := settlement.NewRepository(db)
	buyReq := reserveRequest(buyer, "USDT", "buy-race-"+token(t), 900)
	sellReq := reserveRequest(seller, "BTC", "sell-race-"+token(t), 400)
	buy, err := repo.ReserveFunds(context.Background(), buyReq)
	if err != nil {
		t.Fatal(err)
	}
	sell, err := repo.ReserveFunds(context.Background(), sellReq)
	if err != nil {
		t.Fatal(err)
	}
	trade := settlement.Request{
		TradeID: "trade-race-" + token(t), MarketID: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT",
		Buyer:     settlement.Participant{AccountID: buyer, OrderID: buyReq.OrderID, ReservationID: buy.ReservationID},
		Seller:    settlement.Participant{AccountID: seller, OrderID: sellReq.OrderID, ReservationID: sell.ReservationID},
		BaseAtoms: 100, QuoteAtoms: 500, CloseBuyer: false, CloseSeller: false,
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var settleErr, releaseErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, settleErr = settle.SettleTrade(ctx, trade)
	}()
	go func() {
		defer wg.Done()
		<-start
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, releaseErr = repo.ReleaseFunds(ctx, releaseRequest(buy, buyReq))
	}()
	close(start)
	wg.Wait()
	if releaseErr != nil {
		t.Fatalf("release should succeed either before or after partial fill: %v", releaseErr)
	}
	if settleErr != nil && !errors.Is(settleErr, settlement.ErrReservationMismatch) {
		t.Fatalf("unexpected settlement failure: %v", settleErr)
	}
	if settleErr == nil {
		assertAmounts(t, db, buyer, "USDT", "500", "0")
		assertAmounts(t, db, seller, "BTC", "400", "300")
	} else {
		assertAmounts(t, db, buyer, "USDT", "1000", "0")
		assertAmounts(t, db, seller, "BTC", "500", "400")
	}
	assertReconciled(t, db, buyer, "USDT")
	assertReconciled(t, db, seller, "BTC")
}

func TestPostgresReleaseAuthorizationAndConcurrentReplay(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "USDT", 1000)
	repo, _ := NewRepository(db)
	req := reserveRequest(account, "USDT", "release-race-"+token(t), 700)
	held, err := repo.ReserveFunds(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	release := releaseRequest(held, req)
	wrong := release
	wrong.OperationID += "-wrong"
	wrong.OrderID = "different-order"
	if _, err := repo.ReleaseFunds(context.Background(), wrong); !errors.Is(err, ErrReservationMismatch) {
		t.Fatalf("wrong order must not release: %v", err)
	}
	if countOperation(t, db, wrong.OperationID) != 0 {
		t.Fatal("unauthorized attempted release left operation")
	}
	const workers = 10
	start := make(chan struct{})
	type outcome struct {
		result ReleaseResult
		err    error
	}
	results := make(chan outcome, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			<-start
			res, err := repo.ReleaseFunds(ctx, release)
			results <- outcome{res, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	fresh := 0
	for out := range results {
		if out.err != nil {
			t.Fatal(out.err)
		}
		if out.result.ReleasedAtoms != 700 || out.result.ReservationID != held.ReservationID {
			t.Fatalf("unexpected repeated release %+v", out.result)
		}
		if !out.result.AlreadyApplied {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("expected one original release, got %d", fresh)
	}
	assertAmounts(t, db, account, "USDT", "1000", "0")
	assertReconciled(t, db, account, "USDT")
}

func TestPostgresInactiveAccountDisallowsNewHoldButCanRelease(t *testing.T) {
	db := pgDB(t)
	account := fundedAccount(t, db, "USDT", 1000)
	repo, _ := NewRepository(db)
	existing := reserveRequest(account, "USDT", "pre-deactivate-"+token(t), 200)
	held, err := repo.ReserveFunds(context.Background(), existing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE accounting.accounts SET is_active=false WHERE account_id=$1`, account); err != nil {
		t.Fatal(err)
	}
	next := reserveRequest(account, "USDT", "post-deactivate-"+token(t), 200)
	if _, err := repo.ReserveFunds(context.Background(), next); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("inactive account must reject new reservation: %v", err)
	}
	if countOperation(t, db, next.OperationID) != 0 {
		t.Fatal("rejected inactive account left operation")
	}
	if _, err := repo.ReleaseFunds(context.Background(), releaseRequest(held, existing)); err != nil {
		t.Fatalf("must release funds even after deactivation: %v", err)
	}
	assertAmounts(t, db, account, "USDT", "1000", "0")
	assertReconciled(t, db, account, "USDT")
}
