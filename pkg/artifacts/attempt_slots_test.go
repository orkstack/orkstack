package artifacts

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var slotEpoch = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func slotRequest(attempt int32, started time.Time, slots int) AttemptSlotRequest {
	return AttemptSlotRequest{Namespace: "ns", RunID: "ork-1", TaskID: "build", Item: NoItem, Slots: slots,
		Holder: SlotHolder{Attempt: attempt, StartedAt: started}, Now: slotEpoch}
}

// TestAttemptSlotKey_Shape: a slot's key lies under the run's identity
// prefix, by task, item ("-" for none), scope and number, and a coordinate
// that cannot be addressed is refused.
func TestAttemptSlotKey_Shape(t *testing.T) {
	got, err := AttemptSlotKey("ns", "ork-1", "build", NoItem, 0, 1)
	if err != nil || !strings.HasSuffix(got, "/ns/ork-1/identity/slots/build/-/0/1.json") || !strings.HasPrefix(got, "metadata/snapshots/") {
		t.Fatalf("no item = %q, %v", got, err)
	}
	got, err = AttemptSlotKey("ns", "ork-1", "convert", ItemAt(3), 7, MaxAttemptSlots)
	if err != nil || !strings.HasSuffix(got, "/identity/slots/convert/3/7/256.json") {
		t.Fatalf("an item = %q, %v", got, err)
	}
	for name, k := range map[string]func() (string, error){
		"slot 0":               func() (string, error) { return AttemptSlotKey("ns", "ork-1", "build", NoItem, 0, 0) },
		"slot past the most":   func() (string, error) { return AttemptSlotKey("ns", "ork-1", "build", NoItem, 0, MaxAttemptSlots+1) },
		"an item below none":   func() (string, error) { return AttemptSlotKey("ns", "ork-1", "build", ItemAt(-2), 0, 1) },
		"a task id with slash": func() (string, error) { return AttemptSlotKey("ns", "ork-1", "a/b", NoItem, 0, 1) },
		"a namespace":          func() (string, error) { return AttemptSlotKey("Bad", "ork-1", "build", NoItem, 0, 1) },
		"no run":               func() (string, error) { return AttemptSlotKey("ns", "", "build", NoItem, 0, 1) },
	} {
		if key, err := k(); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s = %q, %v; want ErrInvalidRecordKey", name, key, err)
		}
	}
}

// TestAttemptSlots_WrapAndSameAttemptHit: an attempt's search starts at
// its own number and wraps; the same attempt (number and start) finds the
// slot it holds and keeps its first time; another dispatch with the same
// number takes the next free one; with none left the answer is exhausted;
// and each item and each scope has slots of its own.
func TestAttemptSlots_WrapAndSameAttemptHit(t *testing.T) {
	s, _ := newTestRecordStore(t)
	ctx := context.Background()
	take := func(req AttemptSlotRequest) AttemptSlot {
		t.Helper()
		got, err := s.TakeAttemptSlot(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	first := slotEpoch.Add(-time.Minute)
	if got := take(slotRequest(1, first, 3)); got.Answer != SlotHeld || got.N != 1 || !got.TakenAt.Equal(slotEpoch) {
		t.Fatalf("the first attempt = %+v", got)
	}
	refresh := slotRequest(1, first, 3)
	refresh.Now = slotEpoch.Add(time.Hour)
	if got := take(refresh); got.Answer != SlotHeld || got.N != 1 || !got.TakenAt.Equal(slotEpoch) {
		t.Fatalf("the same attempt again = %+v", got)
	}
	if got := take(slotRequest(5, first.Add(time.Second), 3)); got.Answer != SlotHeld || got.N != 2 {
		t.Fatalf("attempt 5 of 3 slots starts at 2 = %+v", got)
	}
	if got := take(slotRequest(1, first.Add(2*time.Second), 3)); got.Answer != SlotHeld || got.N != 3 {
		t.Fatalf("a second dispatch's attempt 1 = %+v", got)
	}
	if got := take(slotRequest(1, first.Add(3*time.Second), 3)); got.Answer != SlotExhausted || got.N != 0 {
		t.Fatalf("a fourth attempt of three slots = %+v", got)
	}
	item := slotRequest(1, first.Add(4*time.Second), 3)
	item.Item = ItemAt(0)
	if got := take(item); got.Answer != SlotHeld || got.N != 1 {
		t.Fatalf("item 0 = %+v", got)
	}
	scope := slotRequest(1, first.Add(4*time.Second), 3)
	scope.Scope = 2
	if got := take(scope); got.Answer != SlotHeld || got.N != 1 {
		t.Fatalf("scope 2 = %+v", got)
	}
}

// TestAttemptSlots_ExhaustedNamesTheTakers: an exhausted answer names the
// attempts that hold the slots, for the audit record.
func TestAttemptSlots_ExhaustedNamesTheTakers(t *testing.T) {
	s, _ := newTestRecordStore(t)
	ctx := context.Background()
	thief := SlotHolder{Attempt: 1, StartedAt: slotEpoch.Add(-time.Hour)}
	if _, err := s.TakeAttemptSlot(ctx, slotRequest(thief.Attempt, thief.StartedAt, 1)); err != nil {
		t.Fatal(err)
	}
	got, err := s.TakeAttemptSlot(ctx, slotRequest(1, slotEpoch.Add(-time.Minute), 1))
	if err != nil || got.Answer != SlotExhausted || len(got.Holders) != 1 || got.Holders[0].Attempt != 1 || !got.Holders[0].StartedAt.Equal(thief.StartedAt) {
		t.Fatalf("exhausted = %+v, %v", got, err)
	}
}

// TestAttemptSlots_StoreErrorIsUnanswered: a store that does not answer
// is RECORD_STORE_UNANSWERED, never a slot or an exhaustion.
func TestAttemptSlots_StoreErrorIsUnanswered(t *testing.T) {
	s, b := newTestRecordStore(t)
	b.down = true
	var un *RecordStoreUnanswered
	if got, err := s.TakeAttemptSlot(context.Background(), slotRequest(1, slotEpoch, 2)); !errors.As(err, &un) || got.Answer != 0 {
		t.Fatalf("a store that does not answer = %+v, %v", got, err)
	}
}

// TestAttemptSlots_AnObjectNotOfOursIsInvalid: an object at a slot's key
// that is not a slot stops the search, naming it, rather than being
// skipped or taken for ours.
func TestAttemptSlots_AnObjectNotOfOursIsInvalid(t *testing.T) {
	s, b := newTestRecordStore(t)
	ctx := context.Background()
	key := mustKey(AttemptSlotKey("ns", "ork-1", "build", NoItem, 0, 1))
	if err := b.create(ctx, key, "application/json", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	got, err := s.TakeAttemptSlot(ctx, slotRequest(1, slotEpoch, 2))
	if err != nil || got.Answer != SlotInvalid || got.Object != key {
		t.Fatalf("a slot that does not decode = %+v, %v", got, err)
	}
}

// TestAttemptSlots_RequestsThatCannotBeAnswered: no slots, too many, an
// attempt below 1, no start time and no clock are refused before the store
// is asked.
func TestAttemptSlots_RequestsThatCannotBeAnswered(t *testing.T) {
	s, b := newTestRecordStore(t)
	b.down = true
	for name, mutate := range map[string]func(*AttemptSlotRequest){
		"no slots":       func(r *AttemptSlotRequest) { r.Slots = 0 },
		"too many slots": func(r *AttemptSlotRequest) { r.Slots = MaxAttemptSlots + 1 },
		"attempt 0":      func(r *AttemptSlotRequest) { r.Holder.Attempt = 0 },
		"no start":       func(r *AttemptSlotRequest) { r.Holder.StartedAt = time.Time{} },
		"no clock":       func(r *AttemptSlotRequest) { r.Now = time.Time{} },
		"a bad task":     func(r *AttemptSlotRequest) { r.TaskID = "A" },
	} {
		req := slotRequest(1, slotEpoch, 2)
		mutate(&req)
		if _, err := s.TakeAttemptSlot(context.Background(), req); !errors.Is(err, ErrInvalidRecordKey) {
			t.Errorf("%s = %v; want ErrInvalidRecordKey", name, err)
		}
	}
}

// FuzzAttemptSlot: a slot read back either decodes, and then re-encodes
// to exactly its bytes, or is refused; it never panics.
func FuzzAttemptSlot(f *testing.F) {
	f.Add(encodeAttemptSlot(SlotHolder{Attempt: 1, StartedAt: slotEpoch}, slotEpoch))
	f.Add(encodeAttemptSlot(SlotHolder{Attempt: 9, StartedAt: slotEpoch.Add(time.Nanosecond)}, slotEpoch.Add(time.Hour)))
	f.Add([]byte(`{"v":1}`))
	f.Add([]byte(`{"v":2,"x":1}`))
	f.Add([]byte(`{"v":1,"attempt":1,"started_at":null,"taken_at":"2026-11-16T12:00:00Z"}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		h, taken, answer, _ := decodeAttemptSlot(raw)
		if answer != RecordFound {
			return
		}
		if h.Attempt < 1 || h.StartedAt.IsZero() || taken.IsZero() {
			t.Fatalf("a found slot holds %+v at %v", h, taken)
		}
		if again := encodeAttemptSlot(h, taken); !bytes.Equal(again, raw) {
			t.Fatalf("%q re-encodes as %q", raw, again)
		}
	})
}
