package settlementworker

import "testing"

func TestProtoRejectsInvalidOwnership(t *testing.T) {
	d, err := NewProtoDecoder()
	if err != nil {
		t.Fatal(err)
	}
	i := fixture()
	i.Settlement.Buyer.OrderID = "not-maker-or-taker"
	if _, err := d.Encode(i); err == nil {
		t.Fatal("invalid order ownership accepted by encoder")
	}
}
