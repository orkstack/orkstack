package api

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// testClock is a clock the test moves, which tells the test each time it is
// read. sharedInventory.read takes the time once, under its lock, in the step
// that answers from the reading kept, joins the read in flight or starts one,
// so once the test has seen n readings, n callers have done one of the three.
type testClock struct {
	mu    sync.Mutex
	at    time.Time
	asked chan struct{}
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.asked <- struct{}{}:
	default: // a test that counts readings reads them long before this fills
	}
	return c.at
}

// current is the clock's time, without counting as a reading.
func (c *testClock) current() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// awaitReadings blocks until the clock has been read n more times.
func (c *testClock) awaitReadings(t *testing.T, n int) {
	t.Helper()
	for got := 0; got < n; got++ {
		select {
		case <-c.asked:
		case <-time.After(10 * time.Second):
			t.Fatalf("the clock was read %d times, want %d: a caller never reached the read", got, n)
		}
	}
}

// manualTimer stands in for the read bound's time.AfterFunc: it records each
// bound it is set for, and runs its function only when the test fires it,
// moving the clock on by that bound first.
type manualTimer struct {
	clock *testClock
	mu    sync.Mutex
	set   []time.Duration
	fns   []func() // nil once stopped or fired
}

func (m *manualTimer) afterFunc(d time.Duration, f func()) func() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.fns)
	m.set = append(m.set, d)
	m.fns = append(m.fns, f)
	return func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		pending := m.fns[n] != nil
		m.fns[n] = nil
		return pending
	}
}

func (m *manualTimer) bounds() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.set)
}

// fire runs the n-th timer set, as time.AfterFunc would once its bound passed.
func (m *manualTimer) fire(t *testing.T, n int) {
	t.Helper()
	m.mu.Lock()
	if n >= len(m.fns) || m.fns[n] == nil {
		m.mu.Unlock()
		t.Fatalf("timer %d was never set, or was stopped or fired already", n)
	}
	f, d := m.fns[n], m.set[n]
	m.fns[n] = nil
	m.mu.Unlock()
	m.clock.advance(d)
	f()
}

// testInventory is a sharedInventory as the Service builds it, on a test
// clock, with a bound timer that only the test fires.
func testInventory(fleet engine.FleetReader) (*sharedInventory, *testClock, *manualTimer) {
	clock := &testClock{at: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), asked: make(chan struct{}, 1024)}
	inv := newSharedInventory(fleet, clock.now)
	timer := &manualTimer{clock: clock}
	inv.afterFunc = timer.afterFunc
	return inv, clock, timer
}

// gatedFleet is an Orker inventory whose presence read can be held until the
// test releases it, and which counts its reads.
type gatedFleet struct {
	mu       sync.Mutex
	observes int
	started  chan struct{} // told as each presence read begins, when set
	release  chan struct{} // nil: the presence read answers at once
	during   func()        // run inside each presence read, when set
	err      error
	ids      []string // the live Orkers; one, "orker-1", when empty
	silent   string   // an Orker whose describe fails at once
}

func (f *gatedFleet) ObserveOrkers(ctx context.Context) ([]engine.OrkerPresence, error) {
	f.mu.Lock()
	f.observes++
	release := f.release
	f.mu.Unlock()
	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.during != nil {
		f.during()
	}
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
	ids := f.ids
	if len(ids) == 0 {
		ids = []string{"orker-1"}
	}
	presence := make([]engine.OrkerPresence, len(ids))
	for n, id := range ids {
		presence[n] = engine.OrkerPresence{Identity: id, LastSeen: time.Now()}
	}
	return presence, nil
}

func (f *gatedFleet) DescribeOrkerByID(_ context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	if id == f.silent {
		return nil, errors.New("no answer on its control channel")
	}
	return &orkstackv1.DescribeOrkerResult{OrkerId: id, Pool: "build"}, nil
}

func (f *gatedFleet) reads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.observes
}

// awaitSignal waits for one value on ch, naming what it waits for.
func awaitSignal[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("waited 10s for %s", what)
		var zero T
		return zero
	}
}

// Every caller that asks while a read is in flight is answered by that read:
// however many viewers poll at once, the Orkers are asked once.
func TestSharedInventory_ConcurrentCallersShareOneRead(t *testing.T) {
	fleet := &gatedFleet{release: make(chan struct{})}
	inv, clock, _ := testInventory(fleet)
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
	// Every caller has started the read or joined it before it answers.
	clock.awaitReadings(t, callers)
	close(fleet.release)
	for range callers {
		if got := awaitSignal(t, results, "a caller's answer"); got != 1 {
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
	inv, clock, _ := testInventory(fleet)
	read := func() {
		t.Helper()
		if _, err := inv.read(context.Background()); err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	read()
	clock.advance(capacityInventoryTTL - time.Millisecond)
	read()
	if got := fleet.reads(); got != 1 {
		t.Fatalf("presence reads within the TTL = %d, want 1", got)
	}
	clock.advance(time.Millisecond)
	read()
	if got := fleet.reads(); got != 2 {
		t.Fatalf("presence reads once the TTL ran out = %d, want 2", got)
	}
}

// A reading is as old as its read: its TTL runs from when the read began, so
// a slow read answers for less long, and no caption is older than one poll.
func TestSharedInventory_AReadingIsAsOldAsWhenItsReadBegan(t *testing.T) {
	fleet := &gatedFleet{}
	inv, clock, _ := testInventory(fleet)
	const slow = 3 * time.Second
	fleet.during = func() { clock.advance(slow) }
	if _, err := inv.read(context.Background()); err != nil {
		t.Fatalf("read: %v", err)
	}
	fleet.during = nil
	clock.advance(capacityInventoryTTL - slow)
	if _, err := inv.read(context.Background()); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := fleet.reads(); got != 2 {
		t.Fatalf("presence reads %s after a %s read began = %d, want 2: the reading had expired",
			capacityInventoryTTL, slow, got)
	}
}

// A failed read is handed to the callers that waited for it and never kept:
// the next caller asks again rather than being told the old failure.
func TestSharedInventory_AFailedReadIsNeverKept(t *testing.T) {
	fleet := &gatedFleet{err: errors.New("frontend unavailable")}
	inv, _, _ := testInventory(fleet)
	for i := 0; i < 2; i++ {
		if _, err := inv.read(context.Background()); err == nil {
			t.Fatal("a failed presence read answered without its error")
		}
	}
	if got := fleet.reads(); got != 2 {
		t.Fatalf("presence reads after two failures = %d, want 2", got)
	}
}

// A reading in which a live Orker did not answer is a partial failure: its
// callers get it, row and error, and the next caller reads again.
func TestSharedInventory_AReadingAnOrkerDidNotAnswerIsNeverKept(t *testing.T) {
	fleet := &gatedFleet{ids: []string{"orker-1", "orker-2"}, silent: "orker-2"}
	inv, _, _ := testInventory(fleet)
	for i := 0; i < 2; i++ {
		orkers, err := inv.read(context.Background())
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(orkers) != 2 || orkers[1].Err == nil {
			t.Fatalf("reading = %+v, want both Orkers and orker-2's error", orkers)
		}
	}
	if got := fleet.reads(); got != 2 {
		t.Fatalf("presence reads after two partial readings = %d, want 2", got)
	}
}

// A caller that leaves stops waiting at once, and the read it started still
// answers the others and is kept.
func TestSharedInventory_ACallerThatLeavesDoesNotCancelTheSharedRead(t *testing.T) {
	fleet := &gatedFleet{release: make(chan struct{}), started: make(chan struct{}, 1)}
	inv, clock, _ := testInventory(fleet)
	ctx, cancel := context.WithCancel(context.Background())
	left := make(chan error, 1)
	go func() {
		_, err := inv.read(ctx)
		left <- err
	}()
	// The caller that will leave started the read, and the read is under way.
	clock.awaitReadings(t, 1)
	awaitSignal(t, fleet.started, "the presence read to begin")
	stayed := make(chan error, 1)
	go func() {
		_, err := inv.read(context.Background())
		stayed <- err
	}()
	clock.awaitReadings(t, 1)
	cancel()
	if err := awaitSignal(t, left, "the leaving caller"); !errors.Is(err, context.Canceled) {
		t.Fatalf("the caller that left got %v, want context.Canceled", err)
	}
	close(fleet.release)
	if err := awaitSignal(t, stayed, "the staying caller"); err != nil {
		t.Fatalf("the caller that stayed got %v, want the shared reading", err)
	}
	if _, err := inv.read(context.Background()); err != nil || fleet.reads() != 1 {
		t.Fatalf("after the shared read: err %v, reads %d; want it kept and 1 read", err, fleet.reads())
	}
}

// hangingFleet lists two Orkers, one of which never answers its describe —
// not even to its context — until the test lets it.
type hangingFleet struct {
	mu       sync.Mutex
	observes int
	ctxs     []context.Context // each hung describe's context
	hung     chan struct{}     // told as each hung describe begins
	hang     chan struct{}     // closed once by unhang
	once     sync.Once
}

func newHangingFleet() *hangingFleet {
	return &hangingFleet{hung: make(chan struct{}, 8), hang: make(chan struct{})}
}

func (f *hangingFleet) ObserveOrkers(context.Context) ([]engine.OrkerPresence, error) {
	f.mu.Lock()
	f.observes++
	f.mu.Unlock()
	seen := time.Now()
	return []engine.OrkerPresence{{Identity: "orker-1", LastSeen: seen}, {Identity: "orker-hung", LastSeen: seen}}, nil
}

func (f *hangingFleet) DescribeOrkerByID(ctx context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	if id == "orker-hung" {
		f.mu.Lock()
		f.ctxs = append(f.ctxs, ctx)
		f.mu.Unlock()
		f.hung <- struct{}{}
		<-f.hang
	}
	return &orkstackv1.DescribeOrkerResult{OrkerId: id, Pool: "build"}, nil
}

func (f *hangingFleet) unhang() { f.once.Do(func() { close(f.hang) }) }

// A read the fleet never finishes ends at its bound: the fleet is told to
// stop, every caller is answered then — two seconds on the read's clock,
// inside the five-second poll — and the next caller starts a read of its own
// rather than joining the hung one.
func TestSharedInventory_AHungReadEndsAtItsBound(t *testing.T) {
	fleet := newHangingFleet()
	t.Cleanup(fleet.unhang)
	inv, clock, timer := testInventory(fleet)
	if inv.bound != capacityReadTimeout || capacityReadTimeout >= capacityInventoryTTL {
		t.Fatalf("read bound = %s, want capacityReadTimeout (%s), inside the %s poll",
			inv.bound, capacityReadTimeout, capacityInventoryTTL)
	}
	readOnce := func(n int) {
		t.Helper()
		asked := clock.current()
		result := make(chan error, 1)
		go func() {
			_, err := inv.read(context.Background())
			result <- err
		}()
		awaitSignal(t, fleet.hung, "the hung describe to begin")
		if got := timer.bounds(); len(got) != n+1 || got[n] != capacityReadTimeout {
			t.Fatalf("read bounds set = %v, want read %d bounded at %s", got, n+1, capacityReadTimeout)
		}
		timer.fire(t, n)
		if err := awaitSignal(t, result, "the caller of a hung read"); err == nil {
			t.Fatal("a read that never finished answered without an error")
		}
		if elapsed := clock.current().Sub(asked); elapsed != capacityReadTimeout {
			t.Fatalf("the caller was answered %s after it asked, want at the %s bound", elapsed, capacityReadTimeout)
		}
		fleet.mu.Lock()
		hungCtx := fleet.ctxs[n]
		fleet.mu.Unlock()
		if hungCtx.Err() == nil {
			t.Fatal("the hung describe's context was not cancelled at the bound")
		}
	}
	readOnce(0)
	readOnce(1)
	fleet.mu.Lock()
	observes := fleet.observes
	fleet.mu.Unlock()
	if observes != 2 {
		t.Fatalf("presence reads = %d after two hung reads, want 2: each started its own", observes)
	}
}

// The read ends at the inventory's own bound, not at the default: a
// replica given another bound waits that long, and no longer.
func TestSharedInventory_AReadEndsAtTheBoundItWasGiven(t *testing.T) {
	fleet := newHangingFleet()
	t.Cleanup(fleet.unhang)
	inv, clock, timer := testInventory(fleet)
	const bound = 3 * time.Second
	inv.bound = bound
	asked := clock.current()
	result := make(chan error, 1)
	go func() {
		_, err := inv.read(context.Background())
		result <- err
	}()
	awaitSignal(t, fleet.hung, "the hung describe to begin")
	if got := timer.bounds(); len(got) != 1 || got[0] != bound {
		t.Fatalf("read bounds set = %v, want one at %s", got, bound)
	}
	timer.fire(t, 0)
	if err := awaitSignal(t, result, "the caller of a hung read"); err == nil {
		t.Fatal("a read that never finished answered without an error")
	}
	if elapsed := clock.current().Sub(asked); elapsed != bound {
		t.Fatalf("the caller was answered %s after it asked, want at the %s bound", elapsed, bound)
	}
}
