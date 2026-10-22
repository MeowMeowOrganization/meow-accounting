package settlement

import (
	"errors"
	"math"
	"testing"
)

func validExecutionInstruction() ExecutionInstruction {
	return ExecutionInstruction{
		Settlement: Request{
			TradeID: "trade-1", MarketID: "BTC-USDT",
			Buyer:     Participant{AccountID: "buyer-1", OrderID: "buy-1", ReservationID: "11111111-1111-4111-8111-111111111111"},
			Seller:    Participant{AccountID: "seller-1", OrderID: "sell-1", ReservationID: "22222222-2222-4222-8222-222222222222"},
			BaseAsset: "BTC", QuoteAsset: "USDT", BaseAtoms: 100, QuoteAtoms: 900,
			CloseBuyer: true, CloseSeller: true,
		},
		SourceEvent:  ExecutionEventID{MarketID: 0, MarketSequence: math.MaxUint64, EventOrdinal: math.MaxUint64},
		MakerOrderID: "sell-1", TakerOrderID: "buy-1", MakerSide: MakerSell,
		PriceUnits: math.MaxUint64,
	}
}

func TestExecutionInstructionValidateAcceptsFullSourceRange(t *testing.T) {
	instruction := validExecutionInstruction()
	if err := instruction.Validate(); err != nil {
		t.Fatalf("valid execution rejected: %v", err)
	}
	instruction.MakerSide = MakerBuy
	instruction.MakerOrderID = "buy-1"
	instruction.TakerOrderID = "sell-1"
	if err := instruction.Validate(); err != nil {
		t.Fatalf("valid maker BUY execution rejected: %v", err)
	}
}

func TestExecutionInstructionRejectsInvalidMappingsAndEvents(t *testing.T) {
	cases := []struct {
		name   string
		change func(*ExecutionInstruction)
	}{
		{"uncommitted sequence", func(i *ExecutionInstruction) { i.SourceEvent.MarketSequence = 0 }},
		{"missing maker", func(i *ExecutionInstruction) { i.MakerOrderID = "" }},
		{"duplicate maker and taker", func(i *ExecutionInstruction) { i.MakerOrderID = i.TakerOrderID }},
		{"wrong maker", func(i *ExecutionInstruction) { i.MakerOrderID = "other-order" }},
		{"wrong taker", func(i *ExecutionInstruction) { i.TakerOrderID = "other-order" }},
		{"unknown side", func(i *ExecutionInstruction) { i.MakerSide = "NONE" }},
		{"no price", func(i *ExecutionInstruction) { i.PriceUnits = 0 }},
		{"invalid settlement", func(i *ExecutionInstruction) { i.Settlement.BaseAtoms = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instruction := validExecutionInstruction()
			tc.change(&instruction)
			if err := instruction.Validate(); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("want ErrInvalidRequest, got %v", err)
			}
		})
	}
}
