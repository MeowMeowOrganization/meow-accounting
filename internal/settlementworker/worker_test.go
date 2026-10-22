package settlementworker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/MeowMeowOrganization/meow-accounting/internal/settlement"
)

type fakeReader struct {
	records   []Record
	position  int
	committed []Record
	commitErr error
}

func (r *fakeReader) Fetch(_ context.Context) (Record, error) {
	if r.position == len(r.records) {
		return Record{}, io.EOF
	}
	v := r.records[r.position]
	r.position++
	return v, nil
}
func (r *fakeReader) Commit(_ context.Context, record Record) error {
	if r.commitErr != nil {
		return r.commitErr
	}
	r.committed = append(r.committed, record)
	return nil
}

type fakeDecoder struct {
	i   settlement.ExecutionInstruction
	err error
}

func (d fakeDecoder) Decode(_ []byte) (settlement.ExecutionInstruction, error) { return d.i, d.err }

type fakeSettler struct {
	mu      sync.Mutex
	seen    map[string]struct{}
	calls   int
	applied int
	failed  int
	failErr error
}

func (s *fakeSettler) SettleExecution(_ context.Context, i settlement.ExecutionInstruction) (settlement.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failed > 0 {
		s.failed--
		return settlement.Result{}, s.failErr
	}
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	if _, ok := s.seen[i.Settlement.TradeID]; ok {
		return settlement.Result{AlreadyApplied: true}, nil
	}
	s.seen[i.Settlement.TradeID] = struct{}{}
	s.applied++
	return settlement.Result{}, nil
}

func fixture() settlement.ExecutionInstruction {
	return settlement.ExecutionInstruction{
		Settlement: settlement.Request{
			TradeID: "trade-1", MarketID: "BTC-USDT",
			Buyer:     settlement.Participant{AccountID: "buyer-1", OrderID: "buy-1", ReservationID: "11111111-1111-1111-1111-111111111111"},
			Seller:    settlement.Participant{AccountID: "seller-1", OrderID: "sell-1", ReservationID: "22222222-2222-2222-2222-222222222222"},
			BaseAsset: "BTC", QuoteAsset: "USDT", BaseAtoms: 100, QuoteAtoms: 900,
			Fees:       settlement.QuoteFees{CollectorAccountID: "fees", BuyerAtoms: 2, SellerAtoms: 3},
			CloseBuyer: true, CloseSeller: true,
		},
		SourceEvent:  settlement.ExecutionEventID{MarketID: 7, MarketSequence: 21, EventOrdinal: 0},
		MakerOrderID: "sell-1", TakerOrderID: "buy-1", MakerSide: settlement.MakerSell, PriceUnits: 640000,
	}
}

func mkWorker(t *testing.T, r Reader, d Decoder, s Settler) *Worker {
	t.Helper()
	w, err := New(r, d, s, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{RetryInitial: time.Millisecond, RetryMax: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func TestNoAckBeforeDurableWriteAndRetry(t *testing.T) {
	i := fixture()
	r := &fakeReader{records: []Record{{Topic: "settlement", Key: []byte("7"), Offset: 10}, {Topic: "settlement", Key: []byte("7"), Offset: 11}}}
	s := &fakeSettler{failed: 2, failErr: errors.New("connection reset")}
	err := mkWorker(t, r, fakeDecoder{i: i}, s).Run(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if s.applied != 1 || s.calls != 4 || len(r.committed) != 2 || r.committed[0].Offset != 10 || r.committed[1].Offset != 11 {
		t.Fatalf("bad retry and offset behavior: calls=%d applied=%d committed=%+v", s.calls, s.applied, r.committed)
	}
}
func TestCrashAfterDBCommitBeforeOffsetCommitReplaysWithoutNewPosting(t *testing.T) {
	i := fixture()
	s := &fakeSettler{}
	record := Record{Topic: "settlement", Partition: 0, Offset: 42, Key: []byte("7")}
	first := &fakeReader{records: []Record{record}, commitErr: errors.New("coordinator disconnected")}
	if err := mkWorker(t, first, fakeDecoder{i: i}, s).Run(context.Background()); err == nil || len(first.committed) != 0 {
		t.Fatalf("expected uncommitted crash: %v", err)
	}
	if s.applied != 1 {
		t.Fatal("commit failure rolled back money erroneously")
	}
	replay := &fakeReader{records: []Record{record}}
	if err := mkWorker(t, replay, fakeDecoder{i: i}, s).Run(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if s.applied != 1 || len(replay.committed) != 1 {
		t.Fatalf("duplicate after crash: applications=%d committed=%v", s.applied, replay.committed)
	}
}
func TestPoisonAndSourceKeyNeverCommit(t *testing.T) {
	i := fixture()
	for _, tc := range []struct {
		name   string
		key    string
		decErr error
	}{
		{"bad key", "BTC-USDT", nil}, {"bad protobuf", "7", errors.New("bad wire")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeReader{records: []Record{{Key: []byte(tc.key), Offset: 5}}}
			s := &fakeSettler{}
			err := mkWorker(t, r, fakeDecoder{i: i, err: tc.decErr}, s).Run(context.Background())
			if !errors.Is(err, ErrPoisonEvent) || s.applied != 0 || len(r.committed) != 0 {
				t.Fatalf("poison advanced offset: %v", err)
			}
		})
	}
}
func TestFinancialConflictsStopWithoutOffsetCommit(t *testing.T) {
	for _, errInput := range []error{settlement.ErrIdempotencyConflict, settlement.ErrInsufficientReserved, settlement.ErrReservationMismatch, settlement.ErrInvariant, settlement.ErrInvalidRequest} {
		r := &fakeReader{records: []Record{{Key: []byte("7")}}}
		s := &fakeSettler{failed: 1, failErr: errInput}
		err := mkWorker(t, r, fakeDecoder{i: fixture()}, s).Run(context.Background())
		if !errors.Is(err, ErrPoisonEvent) || s.calls != 1 || len(r.committed) != 0 {
			t.Fatalf("%v: %v", errInput, err)
		}
	}
}
func TestCancellationDuringTransientRetryDoesNotCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &fakeReader{records: []Record{{Key: []byte("7")}}}
	s := &cancelSettler{cancel: cancel}
	w := mkWorker(t, r, fakeDecoder{i: fixture()}, s)
	if err := w.Run(ctx); !errors.Is(err, context.Canceled) || len(r.committed) != 0 {
		t.Fatalf("cancel must not ack: %v", err)
	}
}

type cancelSettler struct{ cancel context.CancelFunc }

func (s *cancelSettler) SettleExecution(_ context.Context, _ settlement.ExecutionInstruction) (settlement.Result, error) {
	s.cancel()
	return settlement.Result{}, errors.New("postgres unavailable")
}

func TestDecoderPreservesUnsignedInt64AndRejectsUnknownFields(t *testing.T) {
	decoder, err := NewProtoDecoder()
	if err != nil {
		t.Fatal(err)
	}
	i := fixture()
	i.SourceEvent.MarketID = math.MaxUint32
	i.SourceEvent.MarketSequence = math.MaxUint64
	i.SourceEvent.EventOrdinal = math.MaxUint64
	i.PriceUnits = math.MaxUint64
	i.Settlement.QuoteAtoms = math.MaxUint64 - 100
	i.Settlement.BaseAtoms = math.MaxUint64
	encoded, err := decoder.Encode(i)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decoder.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceEvent != i.SourceEvent || got.PriceUnits != i.PriceUnits || got.Settlement.QuoteAtoms != i.Settlement.QuoteAtoms || got.Settlement.BaseAtoms != i.Settlement.BaseAtoms {
		t.Fatalf("uint64 truncation: %+v", got)
	}
	if _, err := decoder.Decode(append(encoded, 0xf8, 0x07, 0x01)); err == nil {
		t.Fatal("accepted unknown field")
	}
	if _, err := decoder.Decode([]byte{0xff}); err == nil {
		t.Fatal("accepted corrupt protobuf")
	}
}

type stateError string

func (e stateError) Error() string    { return "postgres SQLSTATE " + string(e) }
func (e stateError) SQLState() string { return string(e) }

func TestPostgresErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		state string
		retry bool
	}{
		{"23505", false}, // duplicate/unique constraints
		{"P0001", false}, // financial guard trigger exception
		{"22003", false}, // bad unsigned amount
		{"42P01", false}, // missing migrations/schema
		{"40001", true},  // serialization retry
		{"40P01", true},  // deadlock retry
		{"08006", true},  // connection loss
	} {
		if got := retriable(stateError(tc.state)); got != tc.retry {
			t.Fatalf("SQLSTATE %s retriable=%v want %v", tc.state, got, tc.retry)
		}
	}
}
