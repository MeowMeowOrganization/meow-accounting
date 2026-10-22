//go:build integration

package settlement

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Uses the existing disposable CEX14_TEST_DSN fixture helper. This suite
// additionally requires migration 000005; production databases are untouched.
func executionDB(t *testing.T) *sql.DB {
	t.Helper()
	db := integrationDB(t)
	var ready bool
	if err := db.QueryRow(`SELECT to_regclass('accounting.trade_executions') IS NOT NULL`).Scan(&ready); err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("CEX16 requires migration 000005_trade_executions.sql")
	}
	return db
}

// Each test uses a random committed sequence so fixtures remain safe on
// repeated runs against the same disposable database (no global truncation).
func uniqueMarketSequence(t *testing.T) uint64 {
	t.Helper()
	hexID := randomHex(t)
	value, err := strconv.ParseUint(hexID[:16], 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	if value == 0 {
		return 1
	}
	return value
}

func makeExecution(r Request, n uint64) ExecutionInstruction {
	return ExecutionInstruction{
		Settlement:   r,
		SourceEvent:  ExecutionEventID{MarketID: 7, MarketSequence: n, EventOrdinal: 0},
		MakerOrderID: r.Seller.OrderID,
		TakerOrderID: r.Buyer.OrderID,
		MakerSide:    MakerSell,
		PriceUnits:   640_000,
	}
}

func countFor(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPostgresCEX16AtomicExecutionAndReplay(t *testing.T) {
	db := executionDB(t)
	req := createFixture(t, db)
	instruction := makeExecution(req, math.MaxUint64)
	instruction.SourceEvent.MarketID = math.MaxUint32
	instruction.SourceEvent.EventOrdinal = uniqueMarketSequence(t)
	instruction.PriceUnits = math.MaxUint64
	repo, _ := NewRepository(db)

	first, err := repo.SettleExecution(context.Background(), instruction)
	if err != nil || first.AlreadyApplied {
		t.Fatalf("first write: %+v %v", first, err)
	}
	assertPosting(t, db, first.LedgerTransactionID)
	assertBalance(t, db, req.Buyer.AccountID, "USDT", "360000000", "0")
	assertBalance(t, db, req.Seller.AccountID, "BTC", "1000000", "0")

	var sourceMarket int64
	var seq, ordinal, price, base, quote string
	err = db.QueryRow(`SELECT source_market_id, market_sequence::text,
        event_ordinal::text, price_units::text, base_atoms::text,
        quote_atoms::text FROM accounting.trade_executions WHERE trade_id=$1`,
		req.TradeID).Scan(&sourceMarket, &seq, &ordinal, &price, &base, &quote)
	if err != nil || sourceMarket != int64(math.MaxUint32) ||
		seq != strconv.FormatUint(math.MaxUint64, 10) ||
		ordinal != strconv.FormatUint(instruction.SourceEvent.EventOrdinal, 10) ||
		price != strconv.FormatUint(math.MaxUint64, 10) ||
		base != "1000000" || quote != "640000000" {
		t.Fatalf("full-range receipt failed: market=%d seq=%s ordinal=%s price=%s base=%s quote=%s err=%v",
			sourceMarket, seq, ordinal, price, base, quote, err)
	}

	// Simulates Kafka redelivery after PostgreSQL committed but BEFORE the
	// offset was acknowledged: there must be no second financial movement.
	second, err := repo.SettleExecution(context.Background(), instruction)
	if err != nil || !second.AlreadyApplied || second.LedgerTransactionID != first.LedgerTransactionID {
		t.Fatalf("idempotent replay: %+v %v", second, err)
	}
	if countFor(t, db, `SELECT COUNT(*) FROM accounting.trade_executions WHERE trade_id=$1`, req.TradeID) != 1 ||
		countFor(t, db, `SELECT COUNT(*) FROM accounting.balance_reservation_mutations WHERE operation_id=$1`, req.operationID()) != 3 {
		t.Fatal("replay changed execution/audit record cardinality")
	}
}

func TestPostgresCEX16RejectsAlteredExecutionAndLegacyUpgrade(t *testing.T) {
	db := executionDB(t)
	req := createFixture(t, db)
	instruction := makeExecution(req, uniqueMarketSequence(t))
	repo, _ := NewRepository(db)
	if _, err := repo.SettleExecution(context.Background(), instruction); err != nil {
		t.Fatal(err)
	}

	variants := []struct {
		name string
		edit func(*ExecutionInstruction)
	}{
		{"ordinal", func(i *ExecutionInstruction) { i.SourceEvent.EventOrdinal++ }},
		{"market", func(i *ExecutionInstruction) { i.SourceEvent.MarketID++ }},
		{"price", func(i *ExecutionInstruction) { i.PriceUnits++ }},
		{"maker side", func(i *ExecutionInstruction) {
			i.MakerSide = MakerBuy
			i.MakerOrderID = req.Buyer.OrderID
			i.TakerOrderID = req.Seller.OrderID
		}},
		{"fee semantics", func(i *ExecutionInstruction) { i.Settlement.CloseBuyer = false }},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			changed := instruction
			v.edit(&changed)
			if _, err := repo.SettleExecution(context.Background(), changed); !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("want idempotency conflict: %v", err)
			}
		})
	}
	assertBalance(t, db, req.Buyer.AccountID, "USDT", "360000000", "0")

	oldReq := createFixture(t, db)
	if _, err := repo.SettleTrade(context.Background(), oldReq); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SettleExecution(context.Background(), makeExecution(oldReq, uniqueMarketSequence(t))); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("legacy trade must not acquire unproven source provenance: %v", err)
	}
	if countFor(t, db, `SELECT COUNT(*) FROM accounting.trade_executions WHERE trade_id=$1`, oldReq.TradeID) != 0 {
		t.Fatal("legacy replay fabricated provenance")
	}
}

func TestPostgresCEX16UniqueSourceRollsBackAllFinancialWrites(t *testing.T) {
	db := executionDB(t)
	first := createFixture(t, db)
	second := createFixture(t, db)
	repo, _ := NewRepository(db)
	sourceID := uniqueMarketSequence(t)
	if _, err := repo.SettleExecution(context.Background(), makeExecution(first, sourceID)); err != nil {
		t.Fatal(err)
	}
	beforePosted, beforeReserved := readBalance(t, db, second.Buyer.AccountID, "USDT")
	// The second trade has separate accounts/holds. It could settle, but its
	// duplicate canonical event identity MUST reject the entire transaction.
	if _, err := repo.SettleExecution(context.Background(), makeExecution(second, sourceID)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("duplicate matching event must conflict: %v", err)
	}
	assertBalance(t, db, second.Buyer.AccountID, "USDT", beforePosted, beforeReserved)
	if countFor(t, db, `SELECT COUNT(*) FROM accounting.operations WHERE operation_id=$1`, second.operationID()) != 0 ||
		countFor(t, db, `SELECT COUNT(*) FROM accounting.trade_settlements WHERE trade_id=$1`, second.TradeID) != 0 ||
		countFor(t, db, `SELECT COUNT(*) FROM accounting.balance_reservation_mutations WHERE operation_id=$1`, second.operationID()) != 0 ||
		countFor(t, db, `SELECT COUNT(*) FROM accounting.trade_executions WHERE trade_id=$1`, second.TradeID) != 0 {
		t.Fatal("provenance conflict leaked financial writes")
	}
	// Retry with a new valid source identity demonstrates rollback left the
	// reservations available; no separate repair step should be necessary.
	if _, err := repo.SettleExecution(context.Background(), makeExecution(second, uniqueMarketSequence(t))); err != nil {
		t.Fatalf("valid retry after rollback: %v", err)
	}
}

func TestPostgresCEX16InsufficientFundsNeverWritesReceipt(t *testing.T) {
	db := executionDB(t)
	req := createFixture(t, db)
	req.QuoteAtoms = 750_000_000
	repo, _ := NewRepository(db)
	if _, err := repo.SettleExecution(context.Background(), makeExecution(req, uniqueMarketSequence(t))); !errors.Is(err, ErrInsufficientReserved) {
		t.Fatalf("want reserve underflow: %v", err)
	}
	if countFor(t, db, `SELECT COUNT(*) FROM accounting.operations WHERE operation_id=$1`, req.operationID()) != 0 ||
		countFor(t, db, `SELECT COUNT(*) FROM accounting.trade_executions WHERE trade_id=$1`, req.TradeID) != 0 {
		t.Fatal("failed financial settlement left durable identity or receipt")
	}
}

func TestPostgresCEX16FeesAndTerminalReleaseAtomic(t *testing.T) {
	db := executionDB(t)
	req := createFixture(t, db)
	req.Fees = QuoteFees{CollectorAccountID: createCollector(t, db, "SYSTEM"), BuyerAtoms: 640_000, SellerAtoms: 320_000}
	repo, _ := NewRepository(db)
	sourceID := uniqueMarketSequence(t)
	first, err := repo.SettleExecution(context.Background(), makeExecution(req, sourceID))
	if err != nil {
		t.Fatal(err)
	}
	assertPostingCount(t, db, first.LedgerTransactionID, 8)
	assertBalance(t, db, req.Fees.CollectorAccountID, "USDT", "960000", "0")
	assertBalance(t, db, req.Buyer.AccountID, "USDT", "359360000", "0")
	replay, err := repo.SettleExecution(context.Background(), makeExecution(req, sourceID))
	if err != nil || !replay.AlreadyApplied {
		t.Fatalf("fee replay: %+v %v", replay, err)
	}
}

func TestPostgresCEX16ConcurrentDuplicateAndImmutableReceipt(t *testing.T) {
	db := executionDB(t)
	req := createFixture(t, db)
	instruction := makeExecution(req, uniqueMarketSequence(t))
	repo, _ := NewRepository(db)
	var wg sync.WaitGroup
	results := make([]Result, 2)
	errs := make([]error, 2)
	for index := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			results[index], errs[index] = repo.SettleExecution(ctx, instruction)
		}(index)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if results[0].LedgerTransactionID != results[1].LedgerTransactionID ||
		results[0].AlreadyApplied == results[1].AlreadyApplied {
		t.Fatalf("want one initial posting and one replay: %+v", results)
	}
	assertPosting(t, db, results[0].LedgerTransactionID)
	if _, err := db.Exec(`UPDATE accounting.trade_executions SET price_units=1 WHERE trade_id=$1`, req.TradeID); err == nil {
		t.Fatal("execution receipt allowed mutation")
	}
	if _, err := db.Exec(`DELETE FROM accounting.trade_executions WHERE trade_id=$1`, req.TradeID); err == nil {
		t.Fatal("execution receipt allowed deletion")
	}
}

func TestPostgresCEX16MultipleExecutionsSameMatchingResult(t *testing.T) {
	db := executionDB(t)
	req := createFixture(t, db)
	repo, _ := NewRepository(db)
	partial := req
	partial.BaseAtoms = 500_000
	partial.QuoteAtoms = 320_000_000
	partial.CloseBuyer, partial.CloseSeller = false, false
	sourceID := uniqueMarketSequence(t)
	first := makeExecution(partial, sourceID)
	first.SourceEvent.EventOrdinal = 0
	if _, err := repo.SettleExecution(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	assertBalance(t, db, req.Buyer.AccountID, "USDT", "680000000", "330000000")
	last := partial
	last.TradeID += "-second-fill"
	last.CloseBuyer, last.CloseSeller = true, true
	second := makeExecution(last, sourceID)
	second.SourceEvent.EventOrdinal = 1
	if _, err := repo.SettleExecution(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	assertBalance(t, db, req.Buyer.AccountID, "USDT", "360000000", "0")
	assertBalance(t, db, req.Seller.AccountID, "BTC", "1000000", "0")
	if countFor(t, db, `SELECT COUNT(*) FROM accounting.trade_executions
        WHERE source_market_id=7 AND market_sequence=$1::numeric`, strconv.FormatUint(sourceID, 10)) != 2 {
		t.Fatal("multiple executions in one matching result must have distinct receipts")
	}
}
