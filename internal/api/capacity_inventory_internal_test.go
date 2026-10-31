package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// gatedFleet is an Orker inventory with one Orker whose presence read can be
// held until the test releases it, and which counts its reads.
type gatedFleet struct {
	mu       sync.Mutex
	observes int
	release  chan struct{} // nil: the read answers at once
	err      error
}

func (f *gatedFleet) ObserveOrkers(ctx context.Context) ([]engine.OrkerPresence, error) {
	f.mu.Lock()
	f.observes++
	release := f.release
	f.mu.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return []engine.OrkerPresence{{Identity: "orker-1", LastSeen: time.Now()}}, nil
}

func (f *gatedFleet) DescribeOrkerByID(context.Context, string) (*orkstackv1.DescribeOrkerResult, error) {
	return &orkstackv1.DescribeOrkerResult{OrkerId: "orker-1", Pool: "build"}, nil
}

func (f *gatedFleet) reads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.observes
}

// awaitJoined blocks until n callers wait on the read in flight.
func awaitJoined(t *testing.T, inv *sharedInventory, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		inv.mu.Lock()
		joined := inv.joined
		inv.mu.Unlock()
		if joined == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d callers joined the read in flight, want %d", joined, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// Every caller that asks while a read is in flight is answered by that read:
// however many viewers poll at once, the Orkers are asked once.
func TestSharedInventory_ConcurrentCallersShareOneRead(t *testing.T) {
	fleet := &gatedFleet{release: make(chan struct{})}
	inv := newSharedInventory(fleet, time.Now)
	const callers = 8
	results := make(chan int, callers)
	for range callers {
		go func() {
			orkers, err := inv.read(context.Background())
			if err != nil {
				results <- -1
				return
			}
			results <- len(orkers)
		}()
	}
	// One caller reads; the other seven wait on its read.
	awaitJoined(t, inv, callers-1)
	close(fleet.release)
	for range callers {
		if got := <-results; got != 1 {
			t.Fatalf("a caller got %d Orkers, want the one the shared read found", got)
		}
	}
	if got := fleet.reads(); got != 1 {
		t.Fatalf("presence reads = %d for %d concurrent callers, want 1", got, callers)
	}
}

// A reading answers for capacityInventoryTTL, and the next caller after it
// reads again.
func TestSharedInventory_AReadingIsKeptForItsTTLAndReadAgainAfter(t *testing.T) {
	fleet := &gatedFleet{}
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	inv := newSharedInventory(fleet, func() time.Time { return now })
	read := func() {
		t.Helper()
		if _, err := inv.read(context.Background()); err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	read()
	now = now.Add(capacityInventoryTTL - time.Millisecond)
	read()
	if got := fleet.reads(); got != 1 {
		t.Fatalf("presence reads within the TTL = %d, want 1", got)
	}
	now = now.Add(time.Millisecond)
	read()
	if got := fleet.reads(); got != 2 {
		t.Fatalf("presence reads once the TTL ran out = %d, want 2", got)
	}
}

// A failed read is handed to the callers that waited for it and never kept:
// the next caller asks again rather than being told the old failure.
func TestSharedInventory_AFailedReadIsNeverKept(t *testing.T) {
	fleet := &gatedFleet{err: errors.New("frontend unavailable")}
	inv := newSharedInventory(fleet, time.Now)
	for i := 0; i < 2; i++ {
		if _, err := inv.read(context.Background()); err == nil {
			t.Fatal("a failed presence read answered without its error")
		}
	}
	if got := fleet.reads(); got != 2 {
		t.Fatalf("presence reads after two failures = %d, want 2", got)
	}
}

// A caller that leaves stops waiting at once, and the read it started still
// answers the others and is kept.
func TestSharedInventory_ACallerThatLeavesDoesNotCancelTheSharedRead(t *testing.T) {
	fleet := &gatedFleet{release: make(chan struct{})}
	inv := newSharedInventory(fleet, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	left := make(chan error, 1)
	go func() {
		_, err := inv.read(ctx)
		left <- err
	}()
	stayed := make(chan error, 1)
	go func() {
		_, err := inv.read(context.Background())
		stayed <- err
	}()
	// Whichever of the two started the read, the other has joined it.
	awaitJoined(t, inv, 1)
	cancel()
	if err := <-left; !errors.Is(err, context.Canceled) {
		t.Fatalf("the caller that left got %v, want context.Canceled", err)
	}
	close(fleet.release)
	if err := <-stayed; err != nil {
		t.Fatalf("the caller that stayed got %v, want the shared reading", err)
	}
	if _, err := inv.read(context.Background()); err != nil || fleet.reads() != 1 {
		t.Fatalf("after the shared read: err %v, reads %d; want it kept and 1 read", err, fleet.reads())
	}
}
