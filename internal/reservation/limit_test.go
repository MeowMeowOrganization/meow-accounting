package reservation

import (
	"errors"
	"math"
	"testing"
)

// 1 BTC = 1e8 base atoms; 1 USDT = 1e6 quote atoms.
// Price tick = 0.01 USDT/BTC: 10,000 quote atoms per 100,000,000 base atoms.
// Lot = 0.0001 BTC: one price tick on one lot is exactly one quote atom.
func btcUSDTBuy() LimitOrder {
	return LimitOrder{
		Side: Buy, BaseAsset: "BTC", QuoteAsset: "USDT",
		BaseAtoms:       1_000_000, // 0.01 BTC
		LimitPriceTicks: 6_500_000, // 65,000.00 USDT/BTC
		Units: MarketUnits{
			LotSizeBaseAtoms: 10_000,
			TickQuoteAtoms:   10_000,
			TickBaseAtoms:    100_000_000,
		},
		BuyerFeeBudgetQuoteAtoms: 650_000, // 0.65 USDT, trusted total fee cap
	}
}

func TestLimitBuyReservesQuotePrincipalAndBuyerFeeBudget(t *testing.T) {
	req, err := CalculateLimit(btcUSDTBuy())
	if err != nil {
		t.Fatal(err)
	}
	if req != (Requirement{Asset: "USDT", Atoms: 650_650_000, PrincipalAtoms: 650_000_000, BuyerFeeBudgetAtoms: 650_000}) {
		t.Fatalf("unexpected requirement: %+v", req)
	}
}

func TestLimitSellReservesOnlyBaseQuantity(t *testing.T) {
	order := btcUSDTBuy()
	order.Side = Sell
	order.BuyerFeeBudgetQuoteAtoms = 0
	req, err := CalculateLimit(order)
	if err != nil {
		t.Fatal(err)
	}
	if req != (Requirement{Asset: "BTC", Atoms: 1_000_000, PrincipalAtoms: 1_000_000}) {
		t.Fatalf("unexpected SELL requirement: %+v", req)
	}
}

func TestZeroBuyerFeesAndOneLot(t *testing.T) {
	order := btcUSDTBuy()
	order.BuyerFeeBudgetQuoteAtoms = 0
	order.BaseAtoms = order.Units.LotSizeBaseAtoms
	req, err := CalculateLimit(order)
	if err != nil {
		t.Fatal(err)
	}
	if req.Asset != "USDT" || req.Atoms != 6_500_000 { // 6.50 USDT
		t.Fatalf("one-lot quote reservation: %+v", req)
	}
}

func TestInvalidLimitOrders(t *testing.T) {
	checks := []struct {
		name   string
		change func(*LimitOrder)
	}{
		{"unsupported side", func(o *LimitOrder) { o.Side = "MARKET" }},
		{"missing base asset", func(o *LimitOrder) { o.BaseAsset = "" }},
		{"padded quote asset", func(o *LimitOrder) { o.QuoteAsset = " USDT " }},
		{"same asset", func(o *LimitOrder) { o.QuoteAsset = o.BaseAsset }},
		{"zero quantity", func(o *LimitOrder) { o.BaseAtoms = 0 }},
		{"quantity not lot aligned", func(o *LimitOrder) { o.BaseAtoms++ }},
		{"zero limit price", func(o *LimitOrder) { o.LimitPriceTicks = 0 }},
		{"zero lot size", func(o *LimitOrder) { o.Units.LotSizeBaseAtoms = 0 }},
		{"zero tick numerator", func(o *LimitOrder) { o.Units.TickQuoteAtoms = 0 }},
		{"zero tick denominator", func(o *LimitOrder) { o.Units.TickBaseAtoms = 0 }},
		{"SELL buyer fee", func(o *LimitOrder) { o.Side = Sell }},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			o := btcUSDTBuy()
			tc.change(&o)
			if _, err := CalculateLimit(o); !errors.Is(err, ErrInvalidLimitOrder) {
				t.Fatalf("expected invalid order, got %v", err)
			}
		})
	}
}

func TestRejectFractionalQuoteAtomsPerTickPerLot(t *testing.T) {
	order := btcUSDTBuy()
	order.Units.LotSizeBaseAtoms = 1
	order.BaseAtoms = 100
	// One unit at a 0.01 USDT/BTC price tick costs less than one quote atom.
	// Even if this order's final price yields an integer amount, another legal
	// execution tick could cause per-fill rounding and under-reservation.
	if _, err := CalculateLimit(order); !errors.Is(err, ErrInexactMarketUnits) {
		t.Fatalf("expected inexact market rejection, got %v", err)
	}
}

func TestUInt64BoundaryAndOverflow(t *testing.T) {
	order := LimitOrder{
		Side: Buy, BaseAsset: "A", QuoteAsset: "B", BaseAtoms: math.MaxUint64,
		LimitPriceTicks: 1, Units: MarketUnits{LotSizeBaseAtoms: 1, TickQuoteAtoms: 1, TickBaseAtoms: 1},
	}
	req, err := CalculateLimit(order)
	if err != nil || req.Atoms != math.MaxUint64 {
		t.Fatalf("full uint64 should fit exactly: %+v, %v", req, err)
	}
	order.LimitPriceTicks = 2
	if _, err := CalculateLimit(order); !errors.Is(err, ErrAmountOverflow) {
		t.Fatalf("principal overflow: %v", err)
	}
	order.LimitPriceTicks = 1
	order.BuyerFeeBudgetQuoteAtoms = 1
	if _, err := CalculateLimit(order); !errors.Is(err, ErrAmountOverflow) {
		t.Fatalf("fee addition overflow: %v", err)
	}
}

func TestIntermediateProductMayExceedUint64AndStillDivideExactly(t *testing.T) {
	order := LimitOrder{
		Side: Buy, BaseAsset: "A", QuoteAsset: "B", BaseAtoms: 1 << 63,
		LimitPriceTicks: 1,
		Units:           MarketUnits{LotSizeBaseAtoms: 1 << 63, TickQuoteAtoms: 2, TickBaseAtoms: 1 << 63},
	}
	req, err := CalculateLimit(order)
	if err != nil || req.Atoms != 2 {
		t.Fatalf("arbitrary precision intermediate arithmetic failed: %+v %v", req, err)
	}
}

func TestPriceTicksMultiplyExactQuotePerLot(t *testing.T) {
	order := btcUSDTBuy()
	order.LimitPriceTicks = 6_400_000
	order.BuyerFeeBudgetQuoteAtoms = 0
	req, err := CalculateLimit(order)
	if err != nil || req.Atoms != 640_000_000 {
		t.Fatalf("price ticks not reflected exactly: %+v %v", req, err)
	}
}

func TestFragmentedPriceImprovedFillsFitThePrincipalHold(t *testing.T) {
	order := btcUSDTBuy()
	order.BuyerFeeBudgetQuoteAtoms = 0
	req, err := CalculateLimit(order)
	if err != nil {
		t.Fatal(err)
	}
	// All fills obey the same market's integer lot and tick constraints.
	// 45 + 25 + 30 = 100 ordered lots. Some execute below the BUY limit.
	fills := []struct{ lots, ticks uint64 }{
		{45, 6_400_000},
		{25, 6_500_000},
		{30, 6_300_000},
	}
	var lots, spent uint64
	for _, fill := range fills {
		if fill.ticks > order.LimitPriceTicks {
			t.Fatal("execution price is worse than the buyer's limit")
		}
		lots += fill.lots
		// The market units make 1 tick x 1 lot equal exactly 1 quote atom.
		spent += fill.lots * fill.ticks
	}
	if lots != order.BaseAtoms/order.Units.LotSizeBaseAtoms || spent > req.PrincipalAtoms {
		t.Fatalf("fragmented fills consumed %d quote atoms from %d reserved, lots=%d", spent, req.PrincipalAtoms, lots)
	}
	if spent != 639_500_000 {
		t.Fatalf("unexpected consumed quote principal: %d", spent)
	}
}
