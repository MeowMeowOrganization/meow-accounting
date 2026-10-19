// Package reservation contains pure, exact-atom pre-trade reservation calculations.
// It does not read balances, hold funds, or depend on transport or PostgreSQL.
package reservation

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

var (
	ErrInvalidLimitOrder  = errors.New("invalid limit order reservation request")
	ErrInexactMarketUnits = errors.New("market price tick and quantity lot cannot settle in exact quote atoms")
	ErrAmountOverflow     = errors.New("reservation exceeds uint64 atom limit")
)

type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

// MarketUnits is a trusted, canonical per-market configuration, NOT input from
// the end user. One price tick corresponds to TickQuoteAtoms / TickBaseAtoms
// quote atoms per base atom; executable base quantities are multiples of
// LotSizeBaseAtoms. The compatibility check below requires every price tick
// applied to one lot to produce an integer number of quote atoms. This means
// any legal partial fill at any legal tick price has exact quote-atom notional.
// The matching engine must enforce these same lot/tick rules.
type MarketUnits struct {
	LotSizeBaseAtoms uint64
	TickQuoteAtoms   uint64
	TickBaseAtoms    uint64
}

// LimitOrder is a normalized LIMIT order supplied by the trusted Order Service.
// LimitPriceTicks is a positive integer multiple of the market's price tick.
// BuyerFeeBudgetQuoteAtoms must be a PROVEN upper bound supplied by trusted fee
// policy for every possible fill sequence of this order, not an untrusted hint
// or a one-trade fee estimate. Current CEX14 fees are charged in quote atoms;
// SELL fees are deducted from quote proceeds and need no extra prefunding.
type LimitOrder struct {
	Side                     Side
	BaseAsset                string
	QuoteAsset               string
	BaseAtoms                uint64
	LimitPriceTicks          uint64
	Units                    MarketUnits
	BuyerFeeBudgetQuoteAtoms uint64
}

// Requirement is one asset and one uint64 atom amount, compatible with the
// existing accounting.balance_reservations.original_atoms column. Principal
// is kept distinct from the fee allowance for auditing/admission diagnostics.
type Requirement struct {
	Asset               string
	Atoms               uint64
	PrincipalAtoms      uint64
	BuyerFeeBudgetAtoms uint64
}

func canonicalAsset(s string) bool {
	return s != "" && s == strings.TrimSpace(s) && len(s) <= 256
}

// CalculateLimit determines what must be reserved BEFORE an order can enter
// matching. It does not check spendability or create a reservation: those steps
// must happen in one Accounting-owned PostgreSQL transaction (future slice).
//
// BUY:  (BaseAtoms/LotSizeBaseAtoms) * LimitPriceTicks *
//
//	(LotSizeBaseAtoms*TickQuoteAtoms/TickBaseAtoms) + buyer fee budget.
//
// SELL: BaseAtoms (the base asset is debited during CEX14 settlement).
//
// All arithmetic is exact. math/big prevents intermediate uint64 overflow;
// the result must still fit the existing uint64 reservation contract.
func CalculateLimit(order LimitOrder) (Requirement, error) {
	if (order.Side != Buy && order.Side != Sell) ||
		!canonicalAsset(order.BaseAsset) || !canonicalAsset(order.QuoteAsset) ||
		order.BaseAsset == order.QuoteAsset || order.BaseAtoms == 0 ||
		order.LimitPriceTicks == 0 || order.Units.LotSizeBaseAtoms == 0 ||
		order.Units.TickQuoteAtoms == 0 || order.Units.TickBaseAtoms == 0 ||
		order.BaseAtoms%order.Units.LotSizeBaseAtoms != 0 {
		return Requirement{}, fmt.Errorf("%w: invalid side, assets, quantity, price, or market units", ErrInvalidLimitOrder)
	}
	if order.Side == Sell && order.BuyerFeeBudgetQuoteAtoms != 0 {
		return Requirement{}, fmt.Errorf("%w: SELL cannot specify a BUY quote fee budget", ErrInvalidLimitOrder)
	}

	// A one-lot execution at price tick 1 must have an exactly representable
	// quote-atom cost. Otherwise per-fill rounding can exceed order prefunding.
	lotTickNumerator := new(big.Int).Mul(
		new(big.Int).SetUint64(order.Units.LotSizeBaseAtoms),
		new(big.Int).SetUint64(order.Units.TickQuoteAtoms),
	)
	perLotPerTick, remainder := new(big.Int).QuoRem(
		lotTickNumerator,
		new(big.Int).SetUint64(order.Units.TickBaseAtoms),
		new(big.Int),
	)
	if remainder.Sign() != 0 || perLotPerTick.Sign() == 0 {
		return Requirement{}, fmt.Errorf("%w: quote atoms per price tick per lot must be a positive integer", ErrInexactMarketUnits)
	}

	if order.Side == Sell {
		return Requirement{
			Asset:          order.BaseAsset,
			Atoms:          order.BaseAtoms,
			PrincipalAtoms: order.BaseAtoms,
		}, nil
	}

	lots := order.BaseAtoms / order.Units.LotSizeBaseAtoms
	principal := new(big.Int).Mul(perLotPerTick, new(big.Int).SetUint64(lots))
	principal.Mul(principal, new(big.Int).SetUint64(order.LimitPriceTicks))
	if !principal.IsUint64() {
		return Requirement{}, fmt.Errorf("%w: BUY quote principal", ErrAmountOverflow)
	}
	combined := new(big.Int).Add(principal, new(big.Int).SetUint64(order.BuyerFeeBudgetQuoteAtoms))
	if !combined.IsUint64() {
		return Requirement{}, fmt.Errorf("%w: BUY principal plus fee budget", ErrAmountOverflow)
	}
	return Requirement{
		Asset:               order.QuoteAsset,
		Atoms:               combined.Uint64(),
		PrincipalAtoms:      principal.Uint64(),
		BuyerFeeBudgetAtoms: order.BuyerFeeBudgetQuoteAtoms,
	}, nil
}
