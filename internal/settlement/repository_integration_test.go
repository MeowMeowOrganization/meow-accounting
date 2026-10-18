//go:build integration

package settlement

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Requires a disposable, separately prepared database with migrations
// 000001 through 000004 applied, and CEX14_TEST_DSN pointing to it.
// No production database is ever reset, truncated, or dropped by these tests.
func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("CEX14_TEST_DSN")
	if dsn == "" {
		t.Skip("set CEX14_TEST_DSN for real PostgreSQL integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var exists bool
	err = db.QueryRowContext(ctx, `
        SELECT to_regclass('accounting.trade_settlements') IS NOT NULL
        AND to_regclass('accounting.trade_fees') IS NOT NULL`).Scan(&exists)
	if err != nil || !exists {
		t.Fatalf("apply migrations 000001-000004 first: %v", err)
	}
	return db
}

func randomHex(t *testing.T) string {
	t.Helper()
	data := make([]byte, 12)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data)
}

func randomUUID(t *testing.T) string {
	t.Helper()
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	data[6] = (data[6] & 0x0f) | 0x40
	data[8] = (data[8] & 0x3f) | 0x80
	h := hex.EncodeToString(data)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:])
}

func execMust(t *testing.T, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(query, args...); err != nil {
		t.Fatalf("fixture SQL failed: %v", err)
	}
}

func commitMust(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Fatalf("fixture commit failed: %v", err)
	}
}

// createFixture seeds ownership through a balanced ledger transaction, not
// by directly mutating posted_atoms, then reserves the admitted BUY and SELL.
func createFixture(t *testing.T, db *sql.DB) Request {
	t.Helper()
	suffix := randomHex(t)
	request := Request{
		TradeID:   "trade-" + suffix,
		MarketID:  "BTC-USDT",
		Buyer:     Participant{AccountID: "alice-" + suffix, OrderID: "buy-" + suffix, ReservationID: randomUUID(t)},
		Seller:    Participant{AccountID: "bob-" + suffix, OrderID: "sell-" + suffix, ReservationID: randomUUID(t)},
		BaseAsset: "BTC", QuoteAsset: "USDT",
		BaseAtoms: 1_000_000, QuoteAtoms: 640_000_000,
		CloseBuyer: true, CloseSeller: true,
	}
	systemID := "system-" + suffix
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	execMust(t, tx, `INSERT INTO accounting.assets(asset_code,scale) VALUES('BTC',8),('USDT',6) ON CONFLICT DO NOTHING`)
	for _, account := range []struct{ id, kind string }{
		{request.Buyer.AccountID, "CUSTOMER"},
		{request.Seller.AccountID, "CUSTOMER"},
		{systemID, "SYSTEM"},
	} {
		execMust(t, tx, `INSERT INTO accounting.accounts (account_id,account_kind) VALUES($1,$2)`, account.id, account.kind)
	}
	seedOp := "TEST_SEED:" + suffix
	execMust(t, tx, `INSERT INTO accounting.operations(operation_id,operation_kind) VALUES($1,'TEST_SEED')`, seedOp)
	var transactionID int64
	if err := tx.QueryRow(`
        INSERT INTO accounting.ledger_transactions(operation_id,reference_type,reference_id)
        VALUES ($1,'TEST_SEED',$2) RETURNING ledger_transaction_id`, seedOp, suffix).Scan(&transactionID); err != nil {
		t.Fatal(err)
	}
	for index, posting := range []struct{ account, asset, atoms string }{
		{systemID, "USDT", "-1000000000"},
		{request.Buyer.AccountID, "USDT", "1000000000"},
		{systemID, "BTC", "-2000000"},
		{request.Seller.AccountID, "BTC", "2000000"},
	} {
		execMust(t, tx, `INSERT INTO accounting.ledger_entries
            (ledger_transaction_id,entry_index,account_id,asset_code,amount_atoms)
            VALUES($1,$2,$3,$4,$5::numeric)`, transactionID, index, posting.account, posting.asset, posting.atoms)
	}
	execMust(t, tx, `UPDATE accounting.ledger_transactions
        SET state='POSTED', posted_at=transaction_timestamp()
        WHERE ledger_transaction_id=$1`, transactionID)
	commitMust(t, tx)

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, hold := range []struct {
		participant   Participant
		asset, amount string
	}{
		{request.Buyer, "USDT", "650000000"},
		{request.Seller, "BTC", "1000000"},
	} {
		operationID := "RESERVE_TEST:" + hold.participant.OrderID
		execMust(t, tx, `INSERT INTO accounting.operations(operation_id,operation_kind)
            VALUES($1,'RESERVE_FUNDS')`, operationID)
		execMust(t, tx, `UPDATE accounting.balances
            SET reserved_atoms=reserved_atoms+$3::numeric
            WHERE account_id=$1 AND asset_code=$2
              AND posted_atoms-reserved_atoms >= $3::numeric`,
			hold.participant.AccountID, hold.asset, hold.amount)
		execMust(t, tx, `INSERT INTO accounting.balance_reservations
            (reservation_id,created_operation_id,account_id,asset_code,
             reference_kind,reference_id,original_atoms,remaining_atoms,status)
            VALUES($1::uuid,$2,$3,$4,'ORDER',$5,$6::numeric,$6::numeric,'ACTIVE')`,
			hold.participant.ReservationID, operationID,
			hold.participant.AccountID, hold.asset, hold.participant.OrderID, hold.amount)
		execMust(t, tx, `INSERT INTO accounting.balance_reservation_mutations
            (operation_id,reservation_id,mutation_kind,amount_atoms)
            VALUES($1,$2::uuid,'CREATED',$3::numeric)`, operationID, hold.participant.ReservationID, hold.amount)
	}
	commitMust(t, tx)
	return request
}

func readBalance(t *testing.T, db *sql.DB, account, asset string) (posted, reserved string) {
	t.Helper()
	err := db.QueryRow(`SELECT posted_atoms::text, reserved_atoms::text
        FROM accounting.balances WHERE account_id=$1 AND asset_code=$2`, account, asset).Scan(&posted, &reserved)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func assertBalance(t *testing.T, db *sql.DB, account, asset, expectedPosted, expectedReserved string) {
	t.Helper()
	posted, reserved := readBalance(t, db, account, asset)
	if posted != expectedPosted || reserved != expectedReserved {
		t.Fatalf("%s/%s posted/reserved: got %s/%s want %s/%s",
			account, asset, posted, reserved, expectedPosted, expectedReserved)
	}
}

func assertPosting(t *testing.T, db *sql.DB, transactionID int64) {
	assertPostingCount(t, db, transactionID, 4)
}

func assertPostingCount(t *testing.T, db *sql.DB, transactionID int64, expected int) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM accounting.ledger_entries
        WHERE ledger_transaction_id=$1`, transactionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("expected %d immutable ledger entries, got %d", expected, count)
	}
	var unbalanced int
	if err := db.QueryRow(`SELECT COUNT(*) FROM (
        SELECT asset_code FROM accounting.ledger_entries WHERE ledger_transaction_id=$1
        GROUP BY asset_code HAVING SUM(amount_atoms) <> 0
    ) x`, transactionID).Scan(&unbalanced); err != nil {
		t.Fatal(err)
	}
	if unbalanced != 0 {
		t.Fatal("ledger does not conserve each asset")
	}
}

func TestPostgresWholeTradePriceImprovementAndReplay(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	repo, _ := NewRepository(db)
	ctx := context.Background()
	result, err := repo.SettleTrade(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.AlreadyApplied {
		t.Fatal("first settlement is not a replay")
	}
	assertPosting(t, db, result.LedgerTransactionID)
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "360000000", "0")
	assertBalance(t, db, request.Buyer.AccountID, "BTC", "1000000", "0")
	assertBalance(t, db, request.Seller.AccountID, "BTC", "1000000", "0")
	assertBalance(t, db, request.Seller.AccountID, "USDT", "640000000", "0")
	var consumed, released string
	err = db.QueryRow(`SELECT consumed_atoms::text, released_atoms::text
        FROM accounting.balance_reservations WHERE reservation_id=$1::uuid`,
		request.Buyer.ReservationID).Scan(&consumed, &released)
	if err != nil || consumed != "640000000" || released != "10000000" {
		t.Fatalf("price improvement not audited: consumed=%s released=%s err=%v", consumed, released, err)
	}
	retry, err := repo.SettleTrade(ctx, request)
	if err != nil || !retry.AlreadyApplied || retry.LedgerTransactionID != result.LedgerTransactionID {
		t.Fatalf("incorrect idempotent replay: %+v %v", retry, err)
	}
	changed := request
	changed.CloseBuyer = false
	if _, err := repo.SettleTrade(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("reused operation with different finality must conflict: %v", err)
	}
}

func TestPostgresPartialThenFinalFill(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	repo, _ := NewRepository(db)
	first := request
	first.BaseAtoms = 500_000
	first.QuoteAtoms = 320_000_000
	first.CloseBuyer = false
	first.CloseSeller = false
	a, err := repo.SettleTrade(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	assertPosting(t, db, a.LedgerTransactionID)
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "680000000", "330000000")
	assertBalance(t, db, request.Seller.AccountID, "BTC", "1500000", "500000")
	second := request
	second.TradeID = request.TradeID + "-fill2"
	second.BaseAtoms = 500_000
	second.QuoteAtoms = 320_000_000
	b, err := repo.SettleTrade(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	assertPosting(t, db, b.LedgerTransactionID)
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "360000000", "0")
	assertBalance(t, db, request.Seller.AccountID, "BTC", "1000000", "0")
}

func TestPostgresInsufficientHoldLeavesNoPosting(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	request.QuoteAtoms = 700_000_000 // buyer reserved only 650,000,000
	repo, _ := NewRepository(db)
	if _, err := repo.SettleTrade(context.Background(), request); !errors.Is(err, ErrInsufficientReserved) {
		t.Fatalf("expected reservation coverage failure, got %v", err)
	}
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "1000000000", "650000000")
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM accounting.operations
        WHERE operation_id=$1`, request.operationID()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("failed settlement left durable operation state")
	}
}

func TestPostgresConcurrentDuplicateExactlyOnePosting(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	repo, _ := NewRepository(db)
	var wg sync.WaitGroup
	results := make([]Result, 2)
	errs := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			results[i], errs[i] = repo.SettleTrade(ctx, request)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if results[0].LedgerTransactionID != results[1].LedgerTransactionID ||
		results[0].AlreadyApplied == results[1].AlreadyApplied {
		t.Fatalf("want exactly one first writer and one replay, got %+v", results)
	}
	assertPosting(t, db, results[0].LedgerTransactionID)
}

func TestPostgresDeferredConservationRejectsAndRollsBack(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	operation := "TEST_UNBALANCED:" + randomHex(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	execMust(t, tx, `INSERT INTO accounting.operations(operation_id,operation_kind)
        VALUES($1,'TEST_UNBALANCED')`, operation)
	var transactionID int64
	err = tx.QueryRow(`INSERT INTO accounting.ledger_transactions
        (operation_id,reference_type,reference_id) VALUES($1,'TEST_UNBALANCED',$2)
        RETURNING ledger_transaction_id`, operation, strconv.FormatUint(1, 10)).Scan(&transactionID)
	if err != nil {
		t.Fatal(err)
	}
	execMust(t, tx, `INSERT INTO accounting.ledger_entries
        (ledger_transaction_id,entry_index,account_id,asset_code,amount_atoms)
        VALUES($1,0,$2,'USDT',-100)`, transactionID, request.Buyer.AccountID)
	execMust(t, tx, `UPDATE accounting.ledger_transactions
        SET state='POSTED', posted_at=transaction_timestamp()
        WHERE ledger_transaction_id=$1`, transactionID)
	if err := tx.Commit(); err == nil {
		t.Fatal("unbalanced ledger transaction committed")
	}
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "1000000000", "650000000")
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM accounting.ledger_transactions WHERE operation_id=$1`, operation).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("deferred error failed to roll back ledger")
	}
}

func TestPostgresConcurrentDistinctTradesCannotSpendSameHoldTwice(t *testing.T) {
	db := integrationDB(t)
	original := createFixture(t, db)
	another := original
	another.TradeID += "-other-trade"
	repo, _ := NewRepository(db)
	requests := []Request{original, another}
	errs := make([]error, 2)
	results := make([]Result, 2)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			results[i], errs[i] = repo.SettleTrade(ctx, requests[i])
		}(i)
	}
	wg.Wait()
	successes := 0
	for i, err := range errs {
		if err == nil {
			successes++
			assertPosting(t, db, results[i].LedgerTransactionID)
		} else if !errors.Is(err, ErrReservationMismatch) && !errors.Is(err, ErrInsufficientReserved) {
			t.Fatalf("unexpected failure for competing trade: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("exactly one trade may consume the holds, got %d", successes)
	}
	assertBalance(t, db, original.Buyer.AccountID, "USDT", "360000000", "0")
	assertBalance(t, db, original.Seller.AccountID, "BTC", "1000000", "0")
}

func TestPostgresImmutableLedgerCannotBeEdited(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	repo, _ := NewRepository(db)
	result, err := repo.SettleTrade(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`UPDATE accounting.ledger_entries SET amount_atoms=1
        WHERE ledger_transaction_id=$1 AND entry_index=0`, result.LedgerTransactionID)
	if err == nil {
		t.Fatal("immutable ledger entry was updated")
	}
	_, err = db.Exec(`DELETE FROM accounting.ledger_transactions WHERE ledger_transaction_id=$1`, result.LedgerTransactionID)
	if err == nil {
		t.Fatal("immutable ledger transaction was deleted")
	}
	assertPosting(t, db, result.LedgerTransactionID)
}

func createCollector(t *testing.T, db *sql.DB, kind string) string {
	t.Helper()
	id := "collector-" + randomHex(t)
	if _, err := db.Exec(`INSERT INTO accounting.accounts(account_id, account_kind)
        VALUES ($1, $2)`, id, kind); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPostgresExplicitBuyerAndSellerFees(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	collector := createCollector(t, db, "SYSTEM")
	request.Fees = QuoteFees{CollectorAccountID: collector, BuyerAtoms: 640_000, SellerAtoms: 320_000}
	repo, _ := NewRepository(db)
	got, err := repo.SettleTrade(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertPostingCount(t, db, got.LedgerTransactionID, 8)
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "359360000", "0")
	assertBalance(t, db, request.Buyer.AccountID, "BTC", "1000000", "0")
	assertBalance(t, db, request.Seller.AccountID, "USDT", "639680000", "0")
	assertBalance(t, db, request.Seller.AccountID, "BTC", "1000000", "0")
	assertBalance(t, db, collector, "USDT", "960000", "0")
	var consumed, released string
	if err := db.QueryRow(`SELECT consumed_atoms::text, released_atoms::text
        FROM accounting.balance_reservations WHERE reservation_id=$1::uuid`,
		request.Buyer.ReservationID).Scan(&consumed, &released); err != nil {
		t.Fatal(err)
	}
	if consumed != "640640000" || released != "9360000" {
		t.Fatalf("fee-aware hold: consumed %s released %s", consumed, released)
	}
	var feeBuyer, feeSeller, feeCollector string
	if err := db.QueryRow(`SELECT buyer_fee_atoms::text, seller_fee_atoms::text, collector_account_id
        FROM accounting.trade_fees WHERE trade_id=$1`, request.TradeID).Scan(&feeBuyer, &feeSeller, &feeCollector); err != nil {
		t.Fatal(err)
	}
	if feeBuyer != "640000" || feeSeller != "320000" || feeCollector != collector {
		t.Fatalf("unexpected immutable receipt %s %s %s", feeBuyer, feeSeller, feeCollector)
	}
	retry, err := repo.SettleTrade(context.Background(), request)
	if err != nil || !retry.AlreadyApplied || retry.LedgerTransactionID != got.LedgerTransactionID {
		t.Fatalf("retry failed: %+v %v", retry, err)
	}
	changed := request
	changed.Fees.BuyerAtoms++
	if _, err := repo.SettleTrade(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("fee change must conflict: %v", err)
	}
	changed = request
	changed.Fees.CollectorAccountID = createCollector(t, db, "SYSTEM")
	if _, err := repo.SettleTrade(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("collector change must conflict: %v", err)
	}
	assertBalance(t, db, collector, "USDT", "960000", "0")
	if _, err := db.Exec(`UPDATE accounting.trade_fees SET buyer_fee_atoms=1 WHERE trade_id=$1`, request.TradeID); err == nil {
		t.Fatal("fee receipt mutation incorrectly succeeded")
	}
}

func TestPostgresBuyerOnlyFee(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	collector := createCollector(t, db, "SYSTEM")
	request.Fees = QuoteFees{CollectorAccountID: collector, BuyerAtoms: 640_000}
	repo, _ := NewRepository(db)
	got, err := repo.SettleTrade(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertPostingCount(t, db, got.LedgerTransactionID, 6)
	assertBalance(t, db, collector, "USDT", "640000", "0")
	assertBalance(t, db, request.Seller.AccountID, "USDT", "640000000", "0")
}

func TestPostgresSellerOnlyFeeWithNonContiguousIndices(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	collector := createCollector(t, db, "SYSTEM")
	request.Fees = QuoteFees{CollectorAccountID: collector, SellerAtoms: 320_000}
	repo, _ := NewRepository(db)
	got, err := repo.SettleTrade(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertPostingCount(t, db, got.LedgerTransactionID, 6)
	assertBalance(t, db, collector, "USDT", "320000", "0")
	var indices string
	if err := db.QueryRow(`SELECT string_agg(entry_index::text, ',' ORDER BY entry_index)
        FROM accounting.ledger_entries WHERE ledger_transaction_id=$1`, got.LedgerTransactionID).Scan(&indices); err != nil {
		t.Fatal(err)
	}
	if indices != "0,1,2,3,6,7" {
		t.Fatalf("seller-only fee indices: %s", indices)
	}
}

func TestPostgresFeeAwarePartialFillThenFinal(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	collector := createCollector(t, db, "SYSTEM")
	repo, _ := NewRepository(db)
	first := request
	first.BaseAtoms, first.QuoteAtoms = 500_000, 320_000_000
	first.CloseBuyer, first.CloseSeller = false, false
	first.Fees = QuoteFees{CollectorAccountID: collector, BuyerAtoms: 320_000, SellerAtoms: 160_000}
	firstResult, err := repo.SettleTrade(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	assertPostingCount(t, db, firstResult.LedgerTransactionID, 8)
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "679680000", "329680000")
	second := first
	second.TradeID = request.TradeID + "-fill2"
	second.CloseBuyer, second.CloseSeller = true, true
	secondResult, err := repo.SettleTrade(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	assertPostingCount(t, db, secondResult.LedgerTransactionID, 8)
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "359360000", "0")
	assertBalance(t, db, request.Seller.AccountID, "USDT", "639680000", "0")
	assertBalance(t, db, collector, "USDT", "960000", "0")
}

func TestPostgresBuyerFeeNotReservedRollsBack(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	request.Fees = QuoteFees{CollectorAccountID: createCollector(t, db, "SYSTEM"), BuyerAtoms: 20_000_000}
	repo, _ := NewRepository(db)
	if _, err := repo.SettleTrade(context.Background(), request); !errors.Is(err, ErrInsufficientReserved) {
		t.Fatalf("fee needs reserved coverage: %v", err)
	}
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "1000000000", "650000000")
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM accounting.operations WHERE operation_id=$1`, request.operationID()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("failed fee settlement left an operation")
	}
}

func TestPostgresRejectCustomerOrInactiveFeeCollector(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	repo, _ := NewRepository(db)
	for _, kind := range []string{"CUSTOMER", "SYSTEM"} {
		collector := createCollector(t, db, kind)
		if kind == "SYSTEM" {
			if _, err := db.Exec(`UPDATE accounting.accounts SET is_active = FALSE WHERE account_id=$1`, collector); err != nil {
				t.Fatal(err)
			}
		}
		request.Fees = QuoteFees{CollectorAccountID: collector, BuyerAtoms: 1}
		if _, err := repo.SettleTrade(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("rejected collector kind %s: %v", kind, err)
		}
	}
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "1000000000", "650000000")
}

func TestPostgresDeferredFeeReceiptMustMatchLedger(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	repo, _ := NewRepository(db)
	_, err := repo.SettleTrade(context.Background(), request) // legacy no-fee posting
	if err != nil {
		t.Fatal(err)
	}
	collector := createCollector(t, db, "SYSTEM")
	// The base trade has no fee entries. Adding a fabricated immutable fee
	// receipt must fail at the deferred consistency check.
	_, err = db.Exec(`INSERT INTO accounting.trade_fees
        (trade_id, collector_account_id, quote_asset, buyer_fee_atoms, seller_fee_atoms)
        VALUES($1,$2,'USDT',1,0)`, request.TradeID, collector)
	if err == nil {
		t.Fatal("fabricated fee receipt committed without fee ledger entries")
	}
}

func TestPostgresConcurrentTradesShareFeeCollectorWithoutLostCredit(t *testing.T) {
	db := integrationDB(t)
	first := createFixture(t, db)
	second := createFixture(t, db)
	collector := createCollector(t, db, "SYSTEM")
	fee := QuoteFees{CollectorAccountID: collector, BuyerAtoms: 640_000, SellerAtoms: 320_000}
	first.Fees, second.Fees = fee, fee
	repo, _ := NewRepository(db)
	var wg sync.WaitGroup
	requests := []Request{first, second}
	results := make([]Result, 2)
	errs := make([]error, 2)
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			results[i], errs[i] = repo.SettleTrade(ctx, requests[i])
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent fee-bearing trade %d failed: %v", i, err)
		}
		assertPostingCount(t, db, results[i].LedgerTransactionID, 8)
	}
	assertBalance(t, db, collector, "USDT", "1920000", "0")
}

func TestPostgresFeeLedgerCannotCommitWithoutReceipt(t *testing.T) {
	db := integrationDB(t)
	request := createFixture(t, db)
	collector := createCollector(t, db, "SYSTEM")
	opID := "TEST_ORPHAN_FEE:" + randomHex(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	execMust(t, tx, `INSERT INTO accounting.operations(operation_id, operation_kind)
        VALUES ($1, 'TEST_ORPHAN_FEE')`, opID)
	var ledgerID int64
	if err := tx.QueryRow(`INSERT INTO accounting.ledger_transactions
        (operation_id,reference_type,reference_id)
        VALUES ($1,'TRADE',$2) RETURNING ledger_transaction_id`, opID, request.TradeID+"-orphan").Scan(&ledgerID); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		index   int
		account string
		asset   string
		atoms   string
	}{
		{0, request.Seller.AccountID, "BTC", "-1000000"},
		{1, request.Buyer.AccountID, "BTC", "1000000"},
		{2, request.Buyer.AccountID, "USDT", "-640000000"},
		{3, request.Seller.AccountID, "USDT", "640000000"},
		{4, request.Buyer.AccountID, "USDT", "-1"},
		{5, collector, "USDT", "1"},
	} {
		execMust(t, tx, `INSERT INTO accounting.ledger_entries
            (ledger_transaction_id, entry_index, account_id, asset_code, amount_atoms)
            VALUES($1,$2,$3,$4,$5::numeric)`, ledgerID, entry.index, entry.account, entry.asset, entry.atoms)
	}
	execMust(t, tx, `UPDATE accounting.ledger_transactions
        SET state='POSTED', posted_at=transaction_timestamp()
        WHERE ledger_transaction_id=$1`, ledgerID)
	if err := tx.Commit(); err == nil {
		t.Fatal("fee-bearing trade ledger committed without a fee receipt")
	}
	assertBalance(t, db, request.Buyer.AccountID, "USDT", "1000000000", "650000000")
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM accounting.ledger_transactions WHERE operation_id=$1`, opID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("orphan-fee ledger was not rolled back")
	}
}
