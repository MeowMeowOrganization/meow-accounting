// Package settlementworker consumes immutable Kafka settlement commands in
// partition order and commits offsets only after durable financial completion.
package settlementworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/MeowMeowOrganization/meow-accounting/internal/settlement"
)

var ErrPoisonEvent = errors.New("non-retriable settlement event")

// Record deliberately contains only the Kafka fields the worker needs.
// Identity is in the protobuf, NOT the Kafka offset or this partition.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// Reader must fetch one unacknowledged record at a time and synchronously
// commit its offset; auto-commit MUST be disabled. Closing is owned by caller.
type Reader interface {
	Fetch(context.Context) (Record, error)
	Commit(context.Context, Record) error
}

type Decoder interface {
	Decode([]byte) (settlement.ExecutionInstruction, error)
}

type Settler interface {
	SettleExecution(context.Context, settlement.ExecutionInstruction) (settlement.Result, error)
}

type Config struct {
	RetryInitial time.Duration
	RetryMax     time.Duration
}

type Worker struct {
	reader  Reader
	decoder Decoder
	settler Settler
	logger  *slog.Logger
	cfg     Config
}

func New(reader Reader, decoder Decoder, settler Settler, logger *slog.Logger, cfg Config) (*Worker, error) {
	if reader == nil || decoder == nil || settler == nil {
		return nil, fmt.Errorf("worker requires Kafka reader, protobuf decoder, and settlement repository")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.RetryInitial == 0 {
		cfg.RetryInitial = 250 * time.Millisecond
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = 10 * time.Second
	}
	if cfg.RetryInitial < 0 || cfg.RetryMax < cfg.RetryInitial {
		return nil, fmt.Errorf("invalid worker retry bounds")
	}
	return &Worker{reader: reader, decoder: decoder, settler: settler, logger: logger, cfg: cfg}, nil
}

// Run is intentionally sequential across all assigned partitions. It does
// not allow commits to jump over an unprocessed offset in any partition.
// Independent markets can be scaled by group members, not goroutines here.
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("nil worker context")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := w.reader.Fetch(ctx)
		if err != nil {
			return fmt.Errorf("fetch Kafka settlement record: %w", err)
		}
		instruction, err := w.decoder.Decode(record.Value)
		if err != nil {
			return fmt.Errorf("%w: topic=%s partition=%d offset=%d: %v", ErrPoisonEvent, record.Topic, record.Partition, record.Offset, err)
		}
		// Contract: every canonical source_market_id is published under its
		// exact decimal UTF-8 key. Stable key + fixed partition count protects
		// the market's per-execution order across Kafka records.
		expectedKey := strconv.FormatUint(uint64(instruction.SourceEvent.MarketID), 10)
		if string(record.Key) != expectedKey {
			return fmt.Errorf("%w: topic=%s partition=%d offset=%d: source market Kafka key mismatch", ErrPoisonEvent, record.Topic, record.Partition, record.Offset)
		}
		backoff := w.cfg.RetryInitial
		for {
			result, err := w.settler.SettleExecution(ctx, instruction)
			if err == nil {
				w.logger.Info("trade settled", "trade_id", instruction.Settlement.TradeID,
					"market_sequence", instruction.SourceEvent.MarketSequence,
					"event_ordinal", instruction.SourceEvent.EventOrdinal,
					"already_applied", result.AlreadyApplied,
					"partition", record.Partition, "offset", record.Offset)
				break
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !retriable(err) {
				return fmt.Errorf("%w: trade_id=%s partition=%d offset=%d: %v", ErrPoisonEvent,
					instruction.Settlement.TradeID, record.Partition, record.Offset, err)
			}
			w.logger.Error("settlement failed, retaining Kafka offset for retry",
				"trade_id", instruction.Settlement.TradeID,
				"partition", record.Partition, "offset", record.Offset, "error", err)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
			}
			if backoff >= w.cfg.RetryMax/2 {
				backoff = w.cfg.RetryMax
			} else {
				backoff *= 2
			}
		}
		// kafka-go is configured for synchronous commits (CommitInterval=0).
		// Never fetch the next record if this acknowledgment failed: exiting
		// causes replay, which the PostgreSQL receipts safely deduplicate.
		if err := w.reader.Commit(ctx, record); err != nil {
			return fmt.Errorf("commit Kafka offset after durable trade settlement (replay safe): %w", err)
		}
	}
}

func retriable(err error) bool {
	if errors.Is(err, settlement.ErrInvalidRequest) ||
		errors.Is(err, settlement.ErrIdempotencyConflict) ||
		errors.Is(err, settlement.ErrReservationMismatch) ||
		errors.Is(err, settlement.ErrInsufficientReserved) ||
		errors.Is(err, settlement.ErrInvariant) {
		return false
	}
	// PostgreSQL integrity errors are not fixed by repeated delivery. 40xxx
	// (serialization/deadlock) and transient network/connection errors retry.
	var pg interface{ SQLState() string }
	if errors.As(err, &pg) {
		state := pg.SQLState()
		for _, prefix := range []string{"22", "23", "28", "3D", "42", "0A", "P0", "XX"} {
			if strings.HasPrefix(state, prefix) {
				return false
			}
		}
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
