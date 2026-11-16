package artifacts

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRecordSlots_ServeTheClassesInOrder: with every slot taken, a freed
// slot goes to a waiting first-class request before a second, and a second
// before a third, whatever order they came in.
func TestRecordSlots_ServeTheClassesInOrder(t *testing.T) {
	s := newRecordSlots(1, 1)
	s.queued = make(chan struct{}, 3)
	ctx := context.Background()
	release, err := s.acquire(ctx, SlotRun)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan SlotClass, 3)
	for _, c := range []SlotClass{SlotList, SlotDoor, SlotRun} {
		go func(c SlotClass) {
			r, err := s.acquire(ctx, c)
			if err != nil {
				t.Error(err)
				return
			}
			served <- c
			r()
		}(c)
		<-s.queued // each waits before the next asks
	}
	release()
	for _, want := range []SlotClass{SlotRun, SlotDoor, SlotList} {
		if got := <-served; got != want {
			t.Fatalf("served %v, want %v", got, want)
		}
	}
}

// TestRecordSlots_ListingsAreCapped: listings hold at most their cap of the
// slots, and a listing that waits past its attempt's time, less the margin,
// is busy, while other classes still get slots.
func TestRecordSlots_ListingsAreCapped(t *testing.T) {
	s := newRecordSlots(4, 2)
	ctx := context.Background()
	for range 2 {
		if _, err := s.acquire(ctx, SlotList); err != nil {
			t.Fatal(err)
		}
	}
	short, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancel()
	var un *RecordStoreUnanswered
	if _, err := s.acquireWithin(short, SlotList, 500*time.Millisecond); !errors.As(err, &un) || un.Class != UnansweredBusy {
		t.Fatalf("a third listing = %v, want busy", err)
	}
	if _, err := s.acquire(ctx, SlotRun); err != nil {
		t.Fatalf("a run's read beside two listings: %v", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := s.acquire(canceled, SlotList); !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled wait = %v", err)
	}
}

// TestRecordSlots_ADoorIsNeverStarved: a run's requests hold at most all
// but the doors' reserve, so a resolution's door finds a slot however many
// run reads wait.
func TestRecordSlots_ADoorIsNeverStarved(t *testing.T) {
	s := newRecordSlotsReserving(4, 1, 1)
	ctx := context.Background()
	for range 3 {
		if _, err := s.acquire(ctx, SlotRun); err != nil {
			t.Fatal(err)
		}
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	var un *RecordStoreUnanswered
	if _, err := s.acquireWithin(short, SlotRun, 0); !errors.As(err, &un) || un.Class != UnansweredBusy {
		t.Fatalf("a fourth run read = %v, want busy", err)
	}
	if _, err := s.acquire(ctx, SlotDoor); err != nil {
		t.Fatalf("a door beside three run reads: %v", err)
	}
}
