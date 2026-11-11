package api_test

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// The Orker inventory behind InspectRun's capacity answer is read once per
// replica, however many pages and watches ask, and kept for a poll's length.
// These tests ask through Service.InspectRun, as a run page does, and count
// what the fleet was asked: the inventory itself is not reached for.

const inventoryRunID = "ork-inventory-1"

// inventoryClock is the Service's clock, which a test moves.
type inventoryClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *inventoryClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *inventoryClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// gatedFleet is capacityFleet's inventory with gates a test controls: it
// tells the test as each presence read begins, holds the read until the test
// releases it, and can leave one Orker's describe unanswered, even to its
// context, until the test lets it go.
type gatedFleet struct {
	*stubFleet
	started chan struct{} // told as each presence read begins, when set
	release chan struct{} // when set, holds each presence read until it is closed
	during  func()        // run inside each presence read, when set

	hang     string        // the Orker whose describe is held; none when empty
	hung     chan struct{} // told as the hung describe begins
	unhang   chan struct{} // closed by the test (and at cleanup) to let it answer
	hungMu   sync.Mutex
	hungCtxs []context.Context // each hung describe's context
}

func (g *gatedFleet) ObserveOrkers(ctx context.Context) ([]engine.OrkerPresence, error) {
	presence, err := g.stubFleet.ObserveOrkers(ctx) // counts the read as it begins
	if g.started != nil {
		g.started <- struct{}{}
	}
	if g.during != nil {
		g.during()
	}
	if g.release != nil {
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return presence, err
}

func (g *gatedFleet) DescribeOrkerByID(ctx context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	if g.hang != "" && id == g.hang {
		g.hungMu.Lock()
		g.hungCtxs = append(g.hungCtxs, ctx)
		g.hungMu.Unlock()
		g.hung <- struct{}{}
		<-g.unhang
	}
	return g.stubFleet.DescribeOrkerByID(ctx, id)
}

// reads is how many presence reads the fleet has been asked for.
func (g *gatedFleet) reads() int {
	g.stubFleet.mu.Lock()
	defer g.stubFleet.mu.Unlock()
	return g.stubFleet.observeOrkers
}

func (g *gatedFleet) hungContext(n int) context.Context {
	g.hungMu.Lock()
	defer g.hungMu.Unlock()
	return g.hungCtxs[n]
}

// inventoryFixture is a Service over one running run with a single task
// waiting on the pool "build", whose one Orker is full: every InspectRun that
// reads the inventory answers that attempt with the pool's capacity wait.
type inventoryFixture struct {
	svc   *api.Service
	clock *inventoryClock
	fleet *gatedFleet
	logs  *syncBuffer
	// arrived is told as each InspectRun gets the run's live attempts: the
	// viewer has done everything but ask for the inventory.
	arrived <-chan struct{}
}

func newInventoryFixture(t *testing.T, configure func(*gatedFleet)) *inventoryFixture {
	t.Helper()
	run := inspectRun(inventoryRunID, orkstackv1.Status_STATUS_RUNNING, "build")
	routeTasksTo(run, "build", "build")
	fleet := &gatedFleet{stubFleet: capacityFleet(),
		hung: make(chan struct{}, 8), unhang: make(chan struct{})}
	var unhang sync.Once
	t.Cleanup(func() { unhang.Do(func() { close(fleet.unhang) }) })
	if configure != nil {
		configure(fleet)
	}
	clock := &inventoryClock{at: inspectNow}
	logs := &syncBuffer{}
	arrived := make(chan struct{}, 64)
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
	live := &liveFakeReader{fakeReader: reader,
		// Fresh attempts for every call: InspectRun writes the capacity wait
		// onto the attempts it is handed, and viewers asking together must
		// not share them.
		derive: func(time.Time) engine.LiveRun {
			select {
			case arrived <- struct{}{}:
			default:
			}
			return engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{waitingAttempt("build")}}
		}}
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Reader: live, Runs: engine.NewRunStore(),
		Bus: engine.NewEventBus(), Starter: &fakeStarter{},
		Now: clock.now, Fleet: fleet, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	return &inventoryFixture{svc: svc, clock: clock, fleet: fleet, logs: logs, arrived: arrived}
}

// inspected is one InspectRun's answer for the waiting attempt.
type inspected struct {
	wait *orkstackv1.CapacityWait
	err  error
}

func (f *inventoryFixture) inspect(ctx context.Context) inspected {
	resp, err := f.svc.InspectRun(ctx, connect.NewRequest(&orkstackv1.InspectRunRequest{RunId: inventoryRunID}))
	if err != nil {
		return inspected{err: err}
	}
	if attempts := resp.Msg.GetAttempts(); len(attempts) != 1 {
		return inspected{err: fmt.Errorf("InspectRun answered %d attempts, want the one waiting", len(attempts))}
	}
	return inspected{wait: resp.Msg.GetAttempts()[0].GetCapacityWait()}
}

// inspectAsync asks InspectRun on a goroutine of its own.
func (f *inventoryFixture) inspectAsync(ctx context.Context) <-chan inspected {
	answer := make(chan inspected, 1)
	go func() { answer <- f.inspect(ctx) }()
	return answer
}

// requireFull fails unless got is the answer of a viewer who learned that the
// pool "build" has no room.
func requireFull(t *testing.T, got inspected) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("InspectRun: %v", got.err)
	}
	if got.wait.GetPool() != "build" {
		t.Fatalf("capacity_wait = %v, want the full pool build", got.wait)
	}
}

// requireNoWait fails unless got is the answer of a viewer who was told
// nothing of capacity (and was not failed either).
func requireNoWait(t *testing.T, got inspected) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("InspectRun: %v", got.err)
	}
	if got.wait != nil {
		t.Fatalf("capacity_wait = %v, want none", got.wait)
	}
}

// receive waits for one value on ch, naming what it waits for.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
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

// logRecords decodes the records a slog JSON handler wrote.
func logRecords(t *testing.T, logs string) []map[string]any {
	t.Helper()
	var records []map[string]any
	decoder := jsontext.NewDecoder(strings.NewReader(logs))
	for {
		var record map[string]any
		err := json.UnmarshalDecode(decoder, &record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("log output is not JSON records: %v\n%s", err, logs)
		}
		records = append(records, record)
	}
	return records
}

// recordsAt is the records at one level.
func recordsAt(records []map[string]any, level string) []map[string]any {
	var at []map[string]any
	for _, record := range records {
		if record["level"] == level {
			at = append(at, record)
		}
	}
	return at
}

// manualTimer stands in for the timer that ends a read of the inventory at
// its bound: it records each bound it is set for and runs its function only
// when the test fires it, moving the clock on by that bound first.
type manualTimer struct {
	clock *inventoryClock
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
	return append([]time.Duration(nil), m.set...)
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

// A reading of the inventory answers every viewer for one poll's length,
// counted from when its read began, and the first viewer after that reads
// again.
func TestInspectRun_AReadingOfTheInventoryAnswersEveryViewerUntilItsTTL(t *testing.T) {
	ttl := api.CapacityInventoryTTLForTest
	for _, tc := range []struct {
		name      string
		later     time.Duration // how long after the first viewer the second asks
		wantReads int
	}{
		{"the same instant", 0, 1},
		{"just inside the TTL", ttl - time.Millisecond, 1},
		{"as the TTL ends", ttl, 2},
		{"long after", time.Hour, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInventoryFixture(t, nil)
			requireFull(t, f.inspect(context.Background()))
			f.clock.advance(tc.later)
			requireFull(t, f.inspect(context.Background()))
			if got := f.fleet.reads(); got != tc.wantReads {
				t.Fatalf("the Orkers were asked %d times for two viewers %s apart, want %d", got, tc.later, tc.wantReads)
			}
		})
	}
}

// A reading is as old as its read: the TTL runs from when the read began, so
// a slow read answers for less long and no caption is older than one poll.
func TestInspectRun_AReadingIsAsOldAsWhenItsReadBegan(t *testing.T) {
	ttl := api.CapacityInventoryTTLForTest
	slow := ttl * 3 / 5
	f := newInventoryFixture(t, nil)
	f.fleet.during = func() { f.clock.advance(slow) }
	requireFull(t, f.inspect(context.Background()))
	f.fleet.during = nil
	f.clock.advance(ttl - slow)
	requireFull(t, f.inspect(context.Background()))
	if got := f.fleet.reads(); got != 2 {
		t.Fatalf("the Orkers were asked %d times, want 2: a TTL after the read began, its reading had expired", got)
	}
}

// Viewers who ask while a read is in flight are answered by that read: no
// matter how many pages poll at once, the Orkers are asked once.
func TestInspectRun_ViewersAskingTogetherShareOneRead(t *testing.T) {
	const viewers = 8
	f := newInventoryFixture(t, func(g *gatedFleet) {
		g.release = make(chan struct{})
		g.started = make(chan struct{}, viewers)
	})
	answers := make([]<-chan inspected, viewers)
	for i := range answers {
		answers[i] = f.inspectAsync(context.Background())
	}
	for range viewers {
		receive(t, f.arrived, "a viewer to reach the inventory")
	}
	receive(t, f.fleet.started, "the presence read to begin")
	close(f.fleet.release)
	for i, answer := range answers {
		got := receive(t, answer, fmt.Sprintf("viewer %d's answer", i))
		requireFull(t, got)
	}
	if got := f.fleet.reads(); got != 1 {
		t.Fatalf("the Orkers were asked %d times for %d viewers asking together, want 1", got, viewers)
	}
}

// A viewer who leaves stops waiting at once and is told nothing, and the read
// it started still answers the viewers who stay and is kept for the next.
func TestInspectRun_AViewerWhoLeavesDoesNotCancelTheSharedRead(t *testing.T) {
	f := newInventoryFixture(t, func(g *gatedFleet) {
		g.release = make(chan struct{})
		g.started = make(chan struct{}, 8)
	})
	leaving, leave := context.WithCancel(context.Background())
	defer leave()
	left := f.inspectAsync(leaving)
	receive(t, f.fleet.started, "the leaving viewer's read to begin")
	stayed := f.inspectAsync(context.Background())
	leave()
	requireNoWait(t, receive(t, left, "the viewer who left"))

	close(f.fleet.release)
	requireFull(t, receive(t, stayed, "the viewer who stayed"))
	requireFull(t, f.inspect(context.Background()))
	if got := f.fleet.reads(); got != 1 {
		t.Fatalf("the Orkers were asked %d times, want 1: the read outlives the viewer who started it and is kept", got)
	}
	if warned := recordsAt(logRecords(t, f.logs.String()), "WARN"); len(warned) != 0 {
		t.Fatalf("a viewer leaving was warned of: %v", warned)
	}
}

// A reading in which a live Orker did not answer is not kept: the next
// viewer asks again, and neither is told the pool is full.
func TestInspectRun_AReadingAnOrkerDidNotAnswerIsNeverKept(t *testing.T) {
	f := newInventoryFixture(t, func(g *gatedFleet) {
		g.presence = append(g.presence, engine.OrkerPresence{Identity: "orker-b2", LastSeen: fleetSeen})
		g.failures = map[string]error{"orker-b2": fmt.Errorf("describe orker: context deadline exceeded")}
	})
	for range 2 {
		requireNoWait(t, f.inspect(context.Background()))
	}
	if got := f.fleet.reads(); got != 2 {
		t.Fatalf("the Orkers were asked %d times for two viewers over a partial reading, want 2", got)
	}
}

// A read the fleet never finishes ends at its bound: the fleet is told to
// stop, the viewer is answered then, and the next viewer starts a read of its
// own rather than joining the hung one.
func TestInspectRun_AReadTheFleetNeverFinishesEndsAtItsBound(t *testing.T) {
	f := newInventoryFixture(t, func(g *gatedFleet) { g.hang = "orker-g1" })
	timer := &manualTimer{clock: f.clock}
	f.svc.SetInventoryTimerForTest(timer.afterFunc)

	for n := range 2 {
		answer := f.inspectAsync(context.Background())
		receive(t, f.fleet.hung, "the hung describe to begin")
		bounds := timer.bounds()
		if len(bounds) != n+1 {
			t.Fatalf("read bounds set = %v, want one for each of the %d reads so far", bounds, n+1)
		}
		// A read that outlasted the poll would still be hung when the page
		// asks again, so the bound is inside it.
		if bound := bounds[n]; bound <= 0 || bound >= api.CapacityInventoryTTLForTest {
			t.Fatalf("read %d is bounded at %s, want a bound inside the %s poll", n+1, bound, api.CapacityInventoryTTLForTest)
		}
		timer.fire(t, n)
		requireNoWait(t, receive(t, answer, "the viewer of a read that never finished"))
		if f.fleet.hungContext(n).Err() == nil {
			t.Fatalf("the hung describe of read %d was not told to stop at the bound", n+1)
		}
	}
	if got := f.fleet.reads(); got != 2 {
		t.Fatalf("the Orkers were asked %d times after two hung reads, want 2: each viewer started its own", got)
	}
	warned := recordsAt(logRecords(t, f.logs.String()), "WARN")
	if len(warned) != 1 || warned[0]["run_id"] != inventoryRunID || warned[0]["error"] == nil {
		t.Fatalf("WARN records = %v, want one naming the run and the error, for two failures inside a minute", warned)
	}
}
