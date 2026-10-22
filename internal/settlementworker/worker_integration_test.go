//go:build integration

package settlementworker_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MeowMeowOrganization/meow-accounting/internal/kafkaadapter"
	"github.com/MeowMeowOrganization/meow-accounting/internal/settlement"
	"github.com/MeowMeowOrganization/meow-accounting/internal/settlementworker"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

// End-to-end tests require an isolated real PostgreSQL 17 and real Kafka,
// explicitly opt-in with CEX14_TEST_DSN and CEX16_KAFKA_BROKERS.
func integrationEnv(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn, brokers := os.Getenv("CEX14_TEST_DSN"), os.Getenv("CEX16_KAFKA_BROKERS")
	if dsn == "" || brokers == "" {
		t.Skip("set CEX14_TEST_DSN and CEX16_KAFKA_BROKERS to run real Kafka+PG tests")
	}
	if strings.Contains(brokers, ",") {
		t.Fatal("integration tests require one advertised Kafka bootstrap broker")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var exists bool
	if err := db.QueryRow(`SELECT to_regclass('accounting.trade_executions') IS NOT NULL`).Scan(&exists); err != nil || !exists {
		t.Fatalf("apply 000005 before Kafka tests: exists=%v, err=%v", exists, err)
	}
	return db, brokers
}
func randID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}
func uniqueSeq(t *testing.T) uint64 {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	v := binary.BigEndian.Uint64(b)
	if v == 0 {
		return 1
	}
	return v
}
func randUUID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func sqlMust(t *testing.T, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}
func commitMust(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func kafkaFixture(t *testing.T, db *sql.DB) settlement.ExecutionInstruction {
	t.Helper()
	suf := randID(t)
	buyer, seller, system := "buyer-"+suf, "seller-"+suf, "test-treasury-"+suf
	buyerOrder, sellerOrder := "buy-"+suf, "sell-"+suf
	buyerReservation, sellerReservation := randUUID(t), randUUID(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	sqlMust(t, tx, `INSERT INTO accounting.assets(asset_code,scale) VALUES('BTC',8),('USDT',6) ON CONFLICT DO NOTHING`)
	for _, account := range []struct{ id, kind string }{{buyer, "CUSTOMER"}, {seller, "CUSTOMER"}, {system, "SYSTEM"}} {
		sqlMust(t, tx, `INSERT INTO accounting.accounts(account_id,account_kind) VALUES($1,$2)`, account.id, account.kind)
	}
	sqlMust(t, tx, `INSERT INTO accounting.operations(operation_id,operation_kind) VALUES($1,'TEST_SEED')`, "TEST_SEED:"+suf)
	var ledgerID int64
	if err := tx.QueryRow(`INSERT INTO accounting.ledger_transactions(operation_id,reference_type,reference_id) VALUES($1,'TEST_SEED',$2) RETURNING ledger_transaction_id`, "TEST_SEED:"+suf, suf).Scan(&ledgerID); err != nil {
		t.Fatal(err)
	}
	for idx, entry := range []struct{ account, asset, atoms string }{{system, "USDT", "-1000000000"}, {buyer, "USDT", "1000000000"}, {system, "BTC", "-2000000"}, {seller, "BTC", "2000000"}} {
		sqlMust(t, tx, `INSERT INTO accounting.ledger_entries(ledger_transaction_id,entry_index,account_id,asset_code,amount_atoms) VALUES($1,$2,$3,$4,$5::numeric)`, ledgerID, idx, entry.account, entry.asset, entry.atoms)
	}
	sqlMust(t, tx, `UPDATE accounting.ledger_transactions SET state='POSTED', posted_at=transaction_timestamp() WHERE ledger_transaction_id=$1`, ledgerID)
	commitMust(t, tx)
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, hold := range []struct{ account, order, reservation, asset, amount string }{
		{buyer, buyerOrder, buyerReservation, "USDT", "650000000"},
		{seller, sellerOrder, sellerReservation, "BTC", "1000000"},
	} {
		op := "RESERVE_TEST:" + hold.order
		sqlMust(t, tx, `INSERT INTO accounting.operations(operation_id,operation_kind) VALUES($1,'RESERVE_FUNDS')`, op)
		sqlMust(t, tx, `UPDATE accounting.balances SET reserved_atoms=reserved_atoms+$3::numeric WHERE account_id=$1 AND asset_code=$2 AND posted_atoms-reserved_atoms >= $3::numeric`, hold.account, hold.asset, hold.amount)
		sqlMust(t, tx, `INSERT INTO accounting.balance_reservations(reservation_id,created_operation_id,account_id,asset_code,reference_kind,reference_id,original_atoms,remaining_atoms,status) VALUES($1::uuid,$2,$3,$4,'ORDER',$5,$6::numeric,$6::numeric,'ACTIVE')`, hold.reservation, op, hold.account, hold.asset, hold.order, hold.amount)
		sqlMust(t, tx, `INSERT INTO accounting.balance_reservation_mutations(operation_id,reservation_id,mutation_kind,amount_atoms) VALUES($1,$2::uuid,'CREATED',$3::numeric)`, op, hold.reservation, hold.amount)
	}
	commitMust(t, tx)
	return settlement.ExecutionInstruction{
		Settlement: settlement.Request{
			TradeID: "trade-" + suf, MarketID: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT", BaseAtoms: 1000000, QuoteAtoms: 640000000,
			Buyer:  settlement.Participant{AccountID: buyer, OrderID: buyerOrder, ReservationID: buyerReservation},
			Seller: settlement.Participant{AccountID: seller, OrderID: sellerOrder, ReservationID: sellerReservation}, CloseBuyer: true, CloseSeller: true,
		},
		SourceEvent:  settlement.ExecutionEventID{MarketID: 7, MarketSequence: uniqueSeq(t), EventOrdinal: 0},
		MakerOrderID: sellerOrder, TakerOrderID: buyerOrder, MakerSide: settlement.MakerSell, PriceUnits: 640000,
	}
}
func createTopic(t *testing.T, broker string) string {
	t.Helper()
	topic := "cex16-it-" + randID(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := kafka.Client{Addr: kafka.TCP(broker)}
	result, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if topicErr := result.Errors[topic]; topicErr != nil {
		t.Fatal(topicErr)
	}
	return topic
}
func send(t *testing.T, broker, topic string, key, value []byte) {
	t.Helper()
	writer := kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value}); err != nil {
		t.Fatal(err)
	}
}

type oneMessageReader struct {
	reader     *kafkaadapter.Reader
	fetched    bool
	failCommit bool
	acks       int
}

func (r *oneMessageReader) Fetch(ctx context.Context) (settlementworker.Record, error) {
	if r.fetched {
		return settlementworker.Record{}, io.EOF
	}
	v, err := r.reader.Fetch(ctx)
	if err == nil {
		r.fetched = true
	}
	return v, err
}
func (r *oneMessageReader) Commit(ctx context.Context, v settlementworker.Record) error {
	if r.failCommit {
		return errors.New("simulated crash after PostgreSQL commit before offset acknowledgment")
	}
	if err := r.reader.Commit(ctx, v); err != nil {
		return err
	}
	r.acks++
	return nil
}
func testWorker(t *testing.T, r settlementworker.Reader, repo *settlement.Repository, d *settlementworker.ProtoDecoder) error {
	t.Helper()
	w, err := settlementworker.New(r, d, repo, slog.New(slog.NewTextHandler(io.Discard, nil)), settlementworker.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	return w.Run(ctx)
}
func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestKafkaPostgresCEX16CommitLossAndReplay(t *testing.T) {
	db, broker := integrationEnv(t)
	d, err := settlementworker.NewProtoDecoder()
	if err != nil {
		t.Fatal(err)
	}
	i := kafkaFixture(t, db)
	topic := createTopic(t, broker)
	group := "cex16-test-" + randID(t)
	encoded, err := d.Encode(i)
	if err != nil {
		t.Fatal(err)
	}
	send(t, broker, topic, []byte("7"), encoded)
	repo, err := settlement.NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	firstKafka, err := kafkaadapter.NewReader([]string{broker}, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	first := &oneMessageReader{reader: firstKafka, failCommit: true}
	if err := testWorker(t, first, repo, d); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected simulated lost commit, got %v", err)
	}
	if err := firstKafka.Close(); err != nil {
		t.Fatal(err)
	}
	if countRows(t, db, `SELECT count(*) FROM accounting.trade_executions WHERE trade_id=$1`, i.Settlement.TradeID) != 1 {
		t.Fatal("PostgreSQL financial commit missing")
	}
	secondKafka, err := kafkaadapter.NewReader([]string{broker}, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	defer secondKafka.Close()
	second := &oneMessageReader{reader: secondKafka}
	if err := testWorker(t, second, repo, d); !errors.Is(err, io.EOF) {
		t.Fatalf("Kafka replay failed: %v", err)
	}
	if second.acks != 1 {
		t.Fatal("replayed record was not acknowledged")
	}
	if countRows(t, db, `SELECT count(*) FROM accounting.trade_settlements WHERE trade_id=$1`, i.Settlement.TradeID) != 1 ||
		countRows(t, db, `SELECT count(*) FROM accounting.ledger_transactions WHERE operation_id=$1`, "SETTLE_TRADE:"+i.Settlement.TradeID) != 1 ||
		countRows(t, db, `SELECT count(*) FROM accounting.trade_executions WHERE trade_id=$1`, i.Settlement.TradeID) != 1 {
		t.Fatal("crash replay created duplicate economic effect")
	}
}

type shouldNotSettle struct{}

func (shouldNotSettle) SettleExecution(_ context.Context, _ settlement.ExecutionInstruction) (settlement.Result, error) {
	return settlement.Result{}, errors.New("invalid protobuf must not reach SQL")
}

func TestKafkaPoisonDeliveryDoesNotAdvanceOffset(t *testing.T) {
	_, broker := integrationEnv(t)
	topic := createTopic(t, broker)
	group := "cex16-poison-" + randID(t)
	send(t, broker, topic, []byte("7"), []byte{0xff, 0x01})
	decoder, err := settlementworker.NewProtoDecoder()
	if err != nil {
		t.Fatal(err)
	}
	r1, err := kafkaadapter.NewReader([]string{broker}, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	worker, err := settlementworker.New(r1, decoder, shouldNotSettle{}, slog.New(slog.NewTextHandler(io.Discard, nil)), settlementworker.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Run(ctx); !errors.Is(err, settlementworker.ErrPoisonEvent) {
		t.Fatalf("want poison stop, got %v", err)
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}
	r2, err := kafkaadapter.NewReader([]string{broker}, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	replay, err := r2.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Offset != 0 || replay.Partition != 0 {
		t.Fatalf("poison offset advanced unexpectedly: %+v", replay)
	}
}
