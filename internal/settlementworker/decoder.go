package settlementworker

import (
	_ "embed"
	"fmt"

	"github.com/MeowMeowOrganization/meow-accounting/internal/settlement"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

//go:embed contract/settlement_instruction.pbdesc
var instructionDescriptor []byte

// ProtoDecoder uses the protoc-compiled descriptor bundled with this service.
// It is a genuine protobuf wire decoder (NOT JSON or a hand-rolled parser).
// The .proto is also supplied as an addition to meow-contracts. When that
// repository publishes generated Go code, replace dynamicpb with its types.
type ProtoDecoder struct {
	message protoreflect.MessageDescriptor
}

func NewProtoDecoder() (*ProtoDecoder, error) {
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(instructionDescriptor, &set); err != nil {
		return nil, fmt.Errorf("load settlement instruction descriptor: %w", err)
	}
	if len(set.File) != 1 {
		return nil, fmt.Errorf("settlement instruction descriptor: expected one file, got %d", len(set.File))
	}
	file, err := protodesc.NewFile(set.File[0], nil)
	if err != nil {
		return nil, fmt.Errorf("resolve settlement instruction descriptor: %w", err)
	}
	message := file.Messages().ByName("TradeSettlementInstruction")
	if message == nil || message.Fields().Len() != 24 {
		return nil, fmt.Errorf("unexpected settlement instruction contract")
	}
	return &ProtoDecoder{message: message}, nil
}

func (d *ProtoDecoder) Decode(value []byte) (settlement.ExecutionInstruction, error) {
	if d == nil || d.message == nil || len(value) == 0 {
		return settlement.ExecutionInstruction{}, fmt.Errorf("empty or unconfigured protobuf instruction")
	}
	m := dynamicpb.NewMessage(d.message)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(value, m); err != nil {
		return settlement.ExecutionInstruction{}, fmt.Errorf("decode settlement protobuf: %w", err)
	}
	// Fail closed on unknown fields until the accounting consumer is explicitly
	// upgraded for a revised trusted command contract.
	if len(m.GetUnknown()) != 0 {
		return settlement.ExecutionInstruction{}, fmt.Errorf("unknown protobuf settlement instruction fields")
	}
	get := func(field string) protoreflect.Value {
		return m.Get(d.message.Fields().ByName(protoreflect.Name(field)))
	}
	str := func(field string) string { return get(field).String() }
	u := func(field string) uint64 { return get(field).Uint() }
	s := settlement.Request{
		TradeID: str("trade_id"), MarketID: str("market_id"),
		Buyer: settlement.Participant{
			AccountID: str("buyer_account_id"), OrderID: str("buyer_order_id"),
			ReservationID: str("buyer_reservation_id"),
		},
		Seller: settlement.Participant{
			AccountID: str("seller_account_id"), OrderID: str("seller_order_id"),
			ReservationID: str("seller_reservation_id"),
		},
		BaseAsset: str("base_asset"), QuoteAsset: str("quote_asset"),
		BaseAtoms: u("base_atoms"), QuoteAtoms: u("quote_atoms"),
		Fees: settlement.QuoteFees{
			CollectorAccountID: str("fee_collector_account_id"),
			BuyerAtoms:         u("buyer_fee_atoms"), SellerAtoms: u("seller_fee_atoms"),
		},
		CloseBuyer: get("close_buyer").Bool(), CloseSeller: get("close_seller").Bool(),
	}
	i := settlement.ExecutionInstruction{
		Settlement: s,
		SourceEvent: settlement.ExecutionEventID{
			MarketID:       uint32(u("source_market_id")),
			MarketSequence: u("market_sequence"), EventOrdinal: u("event_ordinal"),
		},
		MakerOrderID: str("maker_order_id"), TakerOrderID: str("taker_order_id"),
		MakerSide: settlement.MakerSide(str("maker_side")), PriceUnits: u("price_units"),
	}
	if err := i.Validate(); err != nil {
		return settlement.ExecutionInstruction{}, fmt.Errorf("invalid settlement instruction: %w", err)
	}
	return i, nil
}

// Encode generates the published protobuf wire format for test fixtures and
// trusted producer interoperability checks. It is not an order projector.
// Only fully resolved and valid instructions can be encoded.
func (d *ProtoDecoder) Encode(i settlement.ExecutionInstruction) ([]byte, error) {
	if d == nil || d.message == nil {
		return nil, fmt.Errorf("protobuf codec is unconfigured")
	}
	if err := i.Validate(); err != nil {
		return nil, err
	}
	m := dynamicpb.NewMessage(d.message)
	setStr := func(name, value string) {
		m.Set(d.message.Fields().ByName(protoreflect.Name(name)), protoreflect.ValueOfString(value))
	}
	setUint := func(name string, value uint64) {
		field := d.message.Fields().ByName(protoreflect.Name(name))
		if field.Kind() == protoreflect.Uint32Kind {
			m.Set(field, protoreflect.ValueOfUint32(uint32(value)))
		} else {
			m.Set(field, protoreflect.ValueOfUint64(value))
		}
	}
	setBool := func(name string, value bool) {
		m.Set(d.message.Fields().ByName(protoreflect.Name(name)), protoreflect.ValueOfBool(value))
	}
	s := i.Settlement
	setStr("trade_id", s.TradeID)
	setStr("market_id", s.MarketID)
	setUint("source_market_id", uint64(i.SourceEvent.MarketID))
	setUint("market_sequence", i.SourceEvent.MarketSequence)
	setUint("event_ordinal", i.SourceEvent.EventOrdinal)
	setStr("maker_order_id", i.MakerOrderID)
	setStr("taker_order_id", i.TakerOrderID)
	setStr("maker_side", string(i.MakerSide))
	setUint("price_units", i.PriceUnits)
	setStr("buyer_account_id", s.Buyer.AccountID)
	setStr("buyer_order_id", s.Buyer.OrderID)
	setStr("buyer_reservation_id", s.Buyer.ReservationID)
	setStr("seller_account_id", s.Seller.AccountID)
	setStr("seller_order_id", s.Seller.OrderID)
	setStr("seller_reservation_id", s.Seller.ReservationID)
	setStr("base_asset", s.BaseAsset)
	setStr("quote_asset", s.QuoteAsset)
	setUint("base_atoms", s.BaseAtoms)
	setUint("quote_atoms", s.QuoteAtoms)
	setUint("buyer_fee_atoms", s.Fees.BuyerAtoms)
	setUint("seller_fee_atoms", s.Fees.SellerAtoms)
	setStr("fee_collector_account_id", s.Fees.CollectorAccountID)
	setBool("close_buyer", s.CloseBuyer)
	setBool("close_seller", s.CloseSeller)
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}
