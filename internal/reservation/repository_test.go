package reservation

import (
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestReservationRequestValidation(t *testing.T) {
	reserve := ReserveRequest{"RESERVE_FUNDS:o1", "alice", "o1", "USDT", 100}
	if err := reserve.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ReserveRequest{
		{"", "alice", "o1", "USDT", 100},
		{"same key ", "alice", "o1", "USDT", 100},
		{"r1", "", "o1", "USDT", 100},
		{"r1", "alice", "", "USDT", 100},
		{"r1", "alice", "o1", "", 100},
		{"r1", "alice", "o1", "USDT", 0},
	} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("invalid %#v: %v", bad, err)
		}
	}
	reserve.Atoms = math.MaxUint64
	if err := reserve.Validate(); err != nil {
		t.Fatal(err)
	}

	release := ReleaseRequest{"RELEASE_RESERVATION:o1", "01234567-89ab-4cde-8fab-0123456789ab", "alice", "o1", "USDT"}
	if err := release.Validate(); err != nil {
		t.Fatal(err)
	}
	release.ReservationID = "bad"
	if err := release.Validate(); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
}

func TestReservationUUIDGeneration(t *testing.T) {
	one, err := newUUID()
	if err != nil {
		t.Fatal(err)
	}
	two, err := newUUID()
	if err != nil {
		t.Fatal(err)
	}
	if one == two || !reservationUUID.MatchString(one) || !reservationUUID.MatchString(two) {
		t.Fatalf("invalid generated UUIDs %q %q", one, two)
	}
	if one[14] != '4' {
		t.Fatalf("expected uuid v4: %s", one)
	}
}

func TestNilDatabaseRejected(t *testing.T) {
	_, err := NewRepository(nil)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
}

type fakeSQLState struct{ code string }

func (e fakeSQLState) Error() string    { return "postgres error" }
func (e fakeSQLState) SQLState() string { return e.code }

func TestUniqueViolationDetection(t *testing.T) {
	if !isUniqueViolation(fmt.Errorf("wrapped: %w", fakeSQLState{code: "23505"})) {
		t.Fatal("wrapped unique violation must be detected")
	}
	if isUniqueViolation(fakeSQLState{code: "23503"}) || isUniqueViolation(nil) || isUniqueViolation(errors.New("different")) {
		t.Fatal("only SQLSTATE 23505 should be treated as unique violation")
	}
}
