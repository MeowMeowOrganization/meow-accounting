// Package settlement implements CEX 14 exact-atom trade settlement and fees,
// extended with CEX 16 immutable execution provenance.
// It contains no transport/Kafka/protobuf dependencies.
package settlement

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidRequest       = errors.New("invalid settlement request")
	ErrIdempotencyConflict  = errors.New("settlement idempotency conflict")
	ErrReservationMismatch  = errors.New("reservation does not match trade")
	ErrInsufficientReserved = errors.New("insufficient reserved funds")
	ErrInvariant            = errors.New("accounting invariant violation")
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Participant is resolved from durable Order Service ownership; never inferred
// from an untrusted matching command (which deliberately omits account identity).
type Participant struct {
	AccountID     string
	OrderID       string
	ReservationID string // PostgreSQL UUID, encoded as a string on the wire
}

// QuoteFees are exact quote-asset atoms already determined by the trusted fee
// policy/projector. Accounting never calculates a basis-point rate or rounds.
// Nonzero fees require an active SYSTEM collector account. Zero values are
// backward-compatible with the original four-entry no-fee settlement.
type QuoteFees struct {
	CollectorAccountID string
	BuyerAtoms         uint64
	SellerAtoms        uint64
}

func (f QuoteFees) enabled() bool { return f.BuyerAtoms != 0 || f.SellerAtoms != 0 }

// Request contains the trusted, validated trade projection's exact amounts.
// CloseBuyer/CloseSeller may be true ONLY when that order is terminal after
// this execution, not merely because this particular execution succeeded.
// A nonterminal partial fill must leave the unused reservation held.
// The caller must process executions in deterministic per-market event order.
type Request struct {
	TradeID     string
	MarketID    string // business market symbol, e.g. BTC-USDT
	Buyer       Participant
	Seller      Participant
	BaseAsset   string
	QuoteAsset  string
	BaseAtoms   uint64
	QuoteAtoms  uint64
	Fees        QuoteFees
	CloseBuyer  bool
	CloseSeller bool
}

type Result struct {
	OperationID         string
	LedgerTransactionID int64
	AlreadyApplied      bool
}

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// Entry is an unsigned magnitude + direction, so the magnitude may be the
// full uint64 range without an unsafe int64 conversion.
type Entry struct {
	// Index is the immutable economic role: principal 0-3, buyer fee 4-5, seller fee 6-7.
	Index     int
	AccountID string
	Asset     string
	Direction Direction
	Atoms     uint64
}

func (e Entry) signedDecimal() string {
	amount := strconv.FormatUint(e.Atoms, 10)
	if e.Direction == Debit {
		return "-" + amount
	}
	return amount
}

func (r Request) operationID() string { return "SETTLE_TRADE:" + r.TradeID }

func canonicalID(s string) bool { return s != "" && s == strings.TrimSpace(s) && len(s) <= 256 }

func (r Request) Validate() error {
	if !canonicalID(r.TradeID) || !canonicalID(r.MarketID) ||
		!canonicalID(r.Buyer.AccountID) || !canonicalID(r.Buyer.OrderID) ||
		!canonicalID(r.Seller.AccountID) || !canonicalID(r.Seller.OrderID) ||
		!canonicalID(r.BaseAsset) || !canonicalID(r.QuoteAsset) {
		return fmt.Errorf("%w: missing, whitespace-padded, or oversized identity", ErrInvalidRequest)
	}
	if !uuidPattern.MatchString(r.Buyer.ReservationID) || !uuidPattern.MatchString(r.Seller.ReservationID) ||
		strings.EqualFold(r.Buyer.ReservationID, r.Seller.ReservationID) {
		return fmt.Errorf("%w: invalid or repeated reservation ID", ErrInvalidRequest)
	}
	if r.BaseAsset == r.QuoteAsset || r.BaseAtoms == 0 || r.QuoteAtoms == 0 {
		return fmt.Errorf("%w: require distinct assets and nonzero exact atom amounts", ErrInvalidRequest)
	}
	if !r.Fees.enabled() {
		if r.Fees.CollectorAccountID != "" {
			return fmt.Errorf("%w: zero fees must not specify a collector", ErrInvalidRequest)
		}
		return nil
	}
	if !canonicalID(r.Fees.CollectorAccountID) ||
		r.Fees.CollectorAccountID == r.Buyer.AccountID ||
		r.Fees.CollectorAccountID == r.Seller.AccountID {
		return fmt.Errorf("%w: fee collector must be a separate accounting account", ErrInvalidRequest)
	}
	if r.Fees.SellerAtoms > r.QuoteAtoms {
		return fmt.Errorf("%w: seller quote fee cannot exceed trade proceeds", ErrInvalidRequest)
	}
	if r.Fees.BuyerAtoms > math.MaxUint64-r.QuoteAtoms {
		return fmt.Errorf("%w: buyer principal plus quote fee exceeds one uint64 reservation", ErrInvalidRequest)
	}
	return nil
}

// Entries returns four principal entries and up to four explicit fee entries.
// Each fee pair independently balances the quote asset; principal amounts are
// never netted or altered to hide fees. No SQL executes unless assets conserve.
func (r Request) Entries() ([]Entry, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	entries := []Entry{
		{0, r.Seller.AccountID, r.BaseAsset, Debit, r.BaseAtoms},
		{1, r.Buyer.AccountID, r.BaseAsset, Credit, r.BaseAtoms},
		{2, r.Buyer.AccountID, r.QuoteAsset, Debit, r.QuoteAtoms},
		{3, r.Seller.AccountID, r.QuoteAsset, Credit, r.QuoteAtoms},
	}
	if r.Fees.BuyerAtoms != 0 {
		entries = append(entries,
			Entry{4, r.Buyer.AccountID, r.QuoteAsset, Debit, r.Fees.BuyerAtoms},
			Entry{5, r.Fees.CollectorAccountID, r.QuoteAsset, Credit, r.Fees.BuyerAtoms})
	}
	if r.Fees.SellerAtoms != 0 {
		entries = append(entries,
			Entry{6, r.Seller.AccountID, r.QuoteAsset, Debit, r.Fees.SellerAtoms},
			Entry{7, r.Fees.CollectorAccountID, r.QuoteAsset, Credit, r.Fees.SellerAtoms})
	}
	totals := map[string]*big.Int{}
	for _, entry := range entries {
		if totals[entry.Asset] == nil {
			totals[entry.Asset] = new(big.Int)
		}
		delta := new(big.Int).SetUint64(entry.Atoms)
		if entry.Direction == Debit {
			delta.Neg(delta)
		}
		totals[entry.Asset].Add(totals[entry.Asset], delta)
	}
	for asset, amount := range totals {
		if amount.Sign() != 0 {
			return nil, fmt.Errorf("%w: posting for %s does not conserve atoms", ErrInvariant, asset)
		}
	}
	return entries, nil
}

// fingerprint is length-delimited, versioned binary SHA-256: neither ambiguous
// concatenation nor JSON field ordering can change the idempotency semantics.
func (r Request) fingerprint() [sha256.Size]byte {
	h := sha256.New()
	putString := func(s string) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(s)))
		_, _ = h.Write(n[:])
		_, _ = h.Write([]byte(s))
	}
	putU64 := func(value uint64) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], value)
		_, _ = h.Write(n[:])
	}
	putBool := func(value bool) {
		if value {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	}
	// Keep the exact legacy digest for historical no-fee settlements. Their
	// immutable receipts must remain retryable after deploying this version.
	if r.Fees.enabled() {
		putString("meow.settlement.request.v2.quote-fees")
	} else {
		putString("meow.settlement.request.v1")
	}
	putString(r.TradeID)
	putString(r.MarketID)
	putString(r.Buyer.AccountID)
	putString(r.Buyer.OrderID)
	putString(strings.ToLower(r.Buyer.ReservationID))
	putString(r.Seller.AccountID)
	putString(r.Seller.OrderID)
	putString(strings.ToLower(r.Seller.ReservationID))
	putString(r.BaseAsset)
	putString(r.QuoteAsset)
	putU64(r.BaseAtoms)
	putU64(r.QuoteAtoms)
	putBool(r.CloseBuyer)
	putBool(r.CloseSeller)
	if r.Fees.enabled() {
		putString(r.Fees.CollectorAccountID)
		putU64(r.Fees.BuyerAtoms)
		putU64(r.Fees.SellerAtoms)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}
