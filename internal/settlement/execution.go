package settlement

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

// ExecutionEventID is the canonical MatchingEventId from meow-contracts,
// not a Kafka offset or a locally generated UUID. An ordinal of zero is valid.
type ExecutionEventID struct {
	MarketID       uint32
	MarketSequence uint64
	EventOrdinal   uint64
}

// MakerSide is the resting order's side in the immutable trade projection.
type MakerSide string

const (
	MakerBuy  MakerSide = "BUY"
	MakerSell MakerSide = "SELL"
)

// ExecutionInstruction combines one already-resolved, fee-aware settlement
// request with the corresponding matching execution provenance. The trusted
// upstream projector MUST freeze all fields for replay, including terminal
// flags, fees, and fee collector. This type has no Kafka/protobuf dependency.
type ExecutionInstruction struct {
	Settlement   Request
	SourceEvent  ExecutionEventID
	MakerOrderID string
	TakerOrderID string
	MakerSide    MakerSide
	PriceUnits   uint64
}

func (i ExecutionInstruction) Validate() error {
	if err := i.Settlement.Validate(); err != nil {
		return err
	}
	if i.SourceEvent.MarketSequence == 0 {
		return fmt.Errorf("%w: committed execution requires positive market sequence", ErrInvalidRequest)
	}
	if !canonicalID(i.MakerOrderID) || !canonicalID(i.TakerOrderID) ||
		i.MakerOrderID == i.TakerOrderID || i.PriceUnits == 0 {
		return fmt.Errorf("%w: invalid maker/taker order or execution price", ErrInvalidRequest)
	}
	switch i.MakerSide {
	case MakerBuy:
		if i.Settlement.Buyer.OrderID != i.MakerOrderID ||
			i.Settlement.Seller.OrderID != i.TakerOrderID {
			return fmt.Errorf("%w: maker BUY ownership disagrees with settlement", ErrInvalidRequest)
		}
	case MakerSell:
		if i.Settlement.Seller.OrderID != i.MakerOrderID ||
			i.Settlement.Buyer.OrderID != i.TakerOrderID {
			return fmt.Errorf("%w: maker SELL ownership disagrees with settlement", ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: unknown maker side", ErrInvalidRequest)
	}
	return nil
}

// SettleExecution is the CEX16 financial entry point. Both the immutable
// execution receipt and every CEX14 ledger/reservation/fee write occur under
// the SAME PostgreSQL transaction and its original locking scheme.
// A successful replay proves both financial and source-event equality.
func (r *Repository) SettleExecution(ctx context.Context, instruction ExecutionInstruction) (Result, error) {
	if err := instruction.Validate(); err != nil {
		return Result{}, err
	}
	return r.settle(ctx, instruction.Settlement, &instruction)
}

func insertExecution(ctx context.Context, tx *sql.Tx, i ExecutionInstruction) error {
	s := i.Settlement
	_, err := tx.ExecContext(ctx, `
        INSERT INTO accounting.trade_executions
            (trade_id, source_market_id, market_sequence, event_ordinal,
             market_id, maker_order_id, taker_order_id, maker_side,
             price_units, base_asset, quote_asset, base_atoms, quote_atoms)
        VALUES ($1, $2, $3::numeric, $4::numeric, $5, $6, $7, $8,
                $9::numeric, $10, $11, $12::numeric, $13::numeric)`,
		s.TradeID, int64(i.SourceEvent.MarketID),
		strconv.FormatUint(i.SourceEvent.MarketSequence, 10),
		strconv.FormatUint(i.SourceEvent.EventOrdinal, 10),
		s.MarketID, i.MakerOrderID, i.TakerOrderID, string(i.MakerSide),
		strconv.FormatUint(i.PriceUnits, 10), s.BaseAsset, s.QuoteAsset,
		strconv.FormatUint(s.BaseAtoms, 10), strconv.FormatUint(s.QuoteAtoms, 10))
	if err != nil {
		// Both PRIMARY KEY(trade_id) and UNIQUE(canonical event identity)
		// represent an idempotency violation, never a retriable trade.
		var stateErr interface{ SQLState() string }
		if errors.As(err, &stateErr) && stateErr.SQLState() == "23505" {
			return fmt.Errorf("%w: trade id or matching source event already recorded: %v", ErrIdempotencyConflict, err)
		}
		return fmt.Errorf("insert immutable execution receipt: %w", err)
	}
	return nil
}

func verifyPreviousExecution(ctx context.Context, tx *sql.Tx, i ExecutionInstruction) error {
	s := i.Settlement
	var sourceMarketID int64
	var sequence, ordinal, price, base, quote string
	var marketID, makerOrderID, takerOrderID, makerSide, baseAsset, quoteAsset string
	err := tx.QueryRowContext(ctx, `
        SELECT source_market_id, market_sequence::text, event_ordinal::text,
               market_id, maker_order_id, taker_order_id, maker_side,
               price_units::text, base_asset, quote_asset,
               base_atoms::text, quote_atoms::text
        FROM accounting.trade_executions WHERE trade_id = $1`, s.TradeID).Scan(
		&sourceMarketID, &sequence, &ordinal, &marketID, &makerOrderID,
		&takerOrderID, &makerSide, &price, &baseAsset, &quoteAsset, &base, &quote)
	if errors.Is(err, sql.ErrNoRows) {
		// A historical SettleTrade receipt cannot be retroactively assigned
		// an unproven source event by this new entry point.
		return fmt.Errorf("%w: settled trade has no source execution receipt", ErrIdempotencyConflict)
	}
	if err != nil {
		return fmt.Errorf("read existing execution receipt: %w", err)
	}
	if sourceMarketID != int64(i.SourceEvent.MarketID) ||
		sequence != strconv.FormatUint(i.SourceEvent.MarketSequence, 10) ||
		ordinal != strconv.FormatUint(i.SourceEvent.EventOrdinal, 10) ||
		marketID != s.MarketID || makerOrderID != i.MakerOrderID ||
		takerOrderID != i.TakerOrderID || makerSide != string(i.MakerSide) ||
		price != strconv.FormatUint(i.PriceUnits, 10) ||
		baseAsset != s.BaseAsset || quoteAsset != s.QuoteAsset ||
		base != strconv.FormatUint(s.BaseAtoms, 10) ||
		quote != strconv.FormatUint(s.QuoteAtoms, 10) {
		return fmt.Errorf("%w: trade id reused with different matching execution provenance", ErrIdempotencyConflict)
	}
	return nil
}
