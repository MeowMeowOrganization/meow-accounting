package settlement

import (
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"testing"
)

const (
	buyerReservation  = "11111111-1111-4111-8111-111111111111"
	sellerReservation = "22222222-2222-4222-8222-222222222222"
)

func sampleRequest() Request {
	return Request{
		TradeID: "BTC-USDT:481:2", MarketID: "BTC-USDT",
		Buyer:     Participant{AccountID: "alice", OrderID: "buy-1", ReservationID: buyerReservation},
		Seller:    Participant{AccountID: "bob", OrderID: "sell-1", ReservationID: sellerReservation},
		BaseAsset: "BTC", QuoteAsset: "USDT",
		BaseAtoms: 1_000_000, QuoteAtoms: 640_000_000,
		CloseBuyer: true, CloseSeller: true,
	}
}

func TestCanonicalSpotPosting(t *testing.T) {
	request := sampleRequest()
	entries, err := request.Entries()
	if err != nil {
		t.Fatal(err)
	}
	expected := []Entry{
		{Index: 0, AccountID: "bob", Asset: "BTC", Direction: Debit, Atoms: 1_000_000},
		{Index: 1, AccountID: "alice", Asset: "BTC", Direction: Credit, Atoms: 1_000_000},
		{Index: 2, AccountID: "alice", Asset: "USDT", Direction: Debit, Atoms: 640_000_000},
		{Index: 3, AccountID: "bob", Asset: "USDT", Direction: Credit, Atoms: 640_000_000},
	}
	if !reflect.DeepEqual(entries, expected) {
		t.Fatalf("entries:\n got %#v\nwant %#v", entries, expected)
	}
	if request.operationID() != "SETTLE_TRADE:BTC-USDT:481:2" {
		t.Fatal("incorrect operation identity")
	}
}

func TestFullUint64ValueNeverConvertsToInt64(t *testing.T) {
	request := sampleRequest()
	request.BaseAtoms, request.QuoteAtoms = math.MaxUint64, math.MaxUint64
	entries, err := request.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if entries[0].signedDecimal() != "-18446744073709551615" {
		t.Fatal(entries[0].signedDecimal())
	}
	if entries[1].signedDecimal() != "18446744073709551615" {
		t.Fatal(entries[1].signedDecimal())
	}
}

func TestRequestValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Request)
	}{
		{"zero BTC", func(r *Request) { r.BaseAtoms = 0 }},
		{"zero quote", func(r *Request) { r.QuoteAtoms = 0 }},
		{"same asset", func(r *Request) { r.QuoteAsset = "BTC" }},
		{"invalid UUID", func(r *Request) { r.Buyer.ReservationID = "oops" }},
		{"identical UUID", func(r *Request) { r.Buyer.ReservationID = r.Seller.ReservationID }},
		{"missing seller", func(r *Request) { r.Seller.AccountID = "" }},
		{"padded trade", func(r *Request) { r.TradeID = " abc " }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := sampleRequest()
			tc.mutate(&r)
			if _, err := r.Entries(); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("expected invalid request, got %v", err)
			}
		})
	}
}

func TestFingerprintCoversEveryEconomicAndFinalityField(t *testing.T) {
	base := sampleRequest()
	base.Buyer.ReservationID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	baseline := base.fingerprint()
	mutations := []func(*Request){
		func(r *Request) { r.TradeID += "x" },
		func(r *Request) { r.MarketID += "x" },
		func(r *Request) { r.Buyer.AccountID += "x" },
		func(r *Request) { r.Buyer.OrderID += "x" },
		func(r *Request) { r.Buyer.ReservationID = "33333333-3333-4333-8333-333333333333" },
		func(r *Request) { r.Seller.AccountID += "x" },
		func(r *Request) { r.Seller.OrderID += "x" },
		func(r *Request) { r.Seller.ReservationID = "44444444-4444-4444-8444-444444444444" },
		func(r *Request) { r.BaseAsset += "x" },
		func(r *Request) { r.QuoteAsset += "x" },
		func(r *Request) { r.BaseAtoms++ },
		func(r *Request) { r.QuoteAtoms++ },
		func(r *Request) { r.CloseBuyer = !r.CloseBuyer },
		func(r *Request) { r.CloseSeller = !r.CloseSeller },
	}
	for i, change := range mutations {
		altered := base
		change(&altered)
		if altered.fingerprint() == baseline {
			t.Fatalf("mutation %d did not alter semantic fingerprint", i)
		}
	}
	repeated := base
	if repeated.fingerprint() != baseline {
		t.Fatal("fingerprint not stable")
	}
	repeated.Buyer.ReservationID = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
	if repeated.fingerprint() != baseline {
		t.Fatal("canonical UUID case normalization failed")
	}
}

func TestRepositoryRequiresDB(t *testing.T) {
	if _, err := NewRepository(nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("got %v", err)
	}
}

func TestExplicitQuoteFeesAreSeparateBalancedEntries(t *testing.T) {
	request := sampleRequest()
	request.Fees = QuoteFees{CollectorAccountID: "exchange-fees", BuyerAtoms: 640_000, SellerAtoms: 320_000}
	actual, err := request.Entries()
	if err != nil {
		t.Fatal(err)
	}
	expected := []Entry{
		{0, "bob", "BTC", Debit, 1_000_000},
		{1, "alice", "BTC", Credit, 1_000_000},
		{2, "alice", "USDT", Debit, 640_000_000},
		{3, "bob", "USDT", Credit, 640_000_000},
		{4, "alice", "USDT", Debit, 640_000},
		{5, "exchange-fees", "USDT", Credit, 640_000},
		{6, "bob", "USDT", Debit, 320_000},
		{7, "exchange-fees", "USDT", Credit, 320_000},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("entries:\n got %#v\nwant %#v", actual, expected)
	}
}

func TestOneSidedFeesPreserveLedgerPositions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		buyer, seller uint64
		indices       []int
	}{
		{"buyer only", 100, 0, []int{0, 1, 2, 3, 4, 5}},
		{"seller only", 0, 100, []int{0, 1, 2, 3, 6, 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := sampleRequest()
			r.Fees = QuoteFees{CollectorAccountID: "exchange-fees", BuyerAtoms: tc.buyer, SellerAtoms: tc.seller}
			entries, err := r.Entries()
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != len(tc.indices) {
				t.Fatalf("got %d entries", len(entries))
			}
			for i, index := range tc.indices {
				if entries[i].Index != index {
					t.Fatalf("position %d: got %d want %d", i, entries[i].Index, index)
				}
			}
		})
	}
}

func TestFeeValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Request)
	}{
		{"zero fee with collector", func(r *Request) { r.Fees.CollectorAccountID = "fee" }},
		{"missing collector", func(r *Request) { r.Fees.BuyerAtoms = 1 }},
		{"collector is buyer", func(r *Request) { r.Fees = QuoteFees{CollectorAccountID: r.Buyer.AccountID, BuyerAtoms: 1} }},
		{"collector is seller", func(r *Request) { r.Fees = QuoteFees{CollectorAccountID: r.Seller.AccountID, SellerAtoms: 1} }},
		{"seller fee > proceeds", func(r *Request) { r.Fees = QuoteFees{CollectorAccountID: "fees", SellerAtoms: r.QuoteAtoms + 1} }},
		{"buyer expense overflow", func(r *Request) {
			r.QuoteAtoms = math.MaxUint64
			r.Fees = QuoteFees{CollectorAccountID: "fees", BuyerAtoms: 1}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := sampleRequest()
			tc.change(&r)
			if _, err := r.Entries(); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("expected invalid request, got %v", err)
			}
		})
	}
}

func TestFeeFingerprintAndLegacyNoFeeCompatibility(t *testing.T) {
	r := sampleRequest()
	legacy := r.fingerprint()
	const oldDigest = "6192ef4981936300131442531b3dc73e9657b75830b4ec83f1d34f4bf6f98e25"
	if hex.EncodeToString(legacy[:]) != oldDigest {
		t.Fatal("historical no-fee fingerprint was changed")
	}
	r.Fees = QuoteFees{CollectorAccountID: "fees", BuyerAtoms: 640_000, SellerAtoms: 320_000}
	original := r.fingerprint()
	if original == legacy {
		t.Fatal("fee details omitted from fingerprint")
	}
	changes := []func(*Request){
		func(r *Request) { r.Fees.BuyerAtoms++ },
		func(r *Request) { r.Fees.SellerAtoms++ },
		func(r *Request) { r.Fees.CollectorAccountID += "-other" },
		func(r *Request) { r.Fees.SellerAtoms = 0 },
		func(r *Request) { r.Fees.BuyerAtoms = 0 },
	}
	for i, change := range changes {
		x := r
		change(&x)
		if x.fingerprint() == original {
			t.Fatalf("fee mutation %d did not change the fingerprint", i)
		}
	}
	if r.fingerprint() != original {
		t.Fatal("fee fingerprint unstable")
	}
}

func TestFullUint64SellerQuoteFeeRemainsExact(t *testing.T) {
	request := sampleRequest()
	request.QuoteAtoms = math.MaxUint64
	request.Fees = QuoteFees{CollectorAccountID: "exchange-fees", SellerAtoms: math.MaxUint64}
	entries, err := request.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 || entries[4].Index != 6 || entries[5].Index != 7 {
		t.Fatalf("incorrect fee entries: %#v", entries)
	}
	if entries[4].signedDecimal() != "-18446744073709551615" ||
		entries[5].signedDecimal() != "18446744073709551615" {
		t.Fatal("fee was narrowed or rounded")
	}
}
