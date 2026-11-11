//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enums "go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// c32Orker is one real item Orker C32 can kill: its own client dials
// Temporal through a proxy, so cutting the proxy is its host dying.
type c32Orker struct {
	name  string
	proxy *faultproxy.Proxy
	orker *chaosOrker
}

// c32Kill is one Orker C32 killed and the items it was running.
type c32Kill struct {
	Orker   string   `json:"orker"`
	OrkerID string   `json:"orker_id"`
	When    string   `json:"when"`
	At      string   `json:"at"`
	Items   []string `json:"items"`
}

// TestIntegration_ContinuationChaosC32_OrkersKilledAcrossGenerations: the
// gated-size fan-out, no max_parallel, with three real item Orkers beside the
// in-process ones. While a hold waits for the items a real Orker is running,
// that Orker is killed (its frontend connection cut and its worker stopped,
// the C2 precedent: the Orkers are in-process workers the test owns); right
// after the next switch, another real Orker is killed with an item of the new
// generation running. Each killed item is retried elsewhere or adopts its
// commit.json; no item's container runs twice except in a retried attempt
// (the attempt records); every item SUCCEEDED exactly once; the stream is
// contiguous across the switches. The kills watch the run from its start, so
// the hold they wait for is one the run reaches at the default in-flight cap,
// and the test fails as soon as the run closes without both.
func TestIntegration_ContinuationChaosC32_OrkersKilledAcrossGenerations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), soakTestTimeout)
	defer cancel()
	items := soakItems(t)
	soakAlone(t, ctx)
	e := startEachSoak(t, ctx, "C32", items)
	// C32 asserts the kills' outcomes, not E4's wall-clock bounds: it keeps the
	// soak's measurements as evidence and fails nothing for them.
	e.timing.RecordOnly()
	var killable []*c32Orker
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("real-%d", i)
		proxy := e.temporalProxy()
		o := e.startRealItemOrker(name, e.dial(name, proxy.Addr()), 1)
		killable = append(killable, &c32Orker{name: name, proxy: proxy, orker: o})
	}
	var (
		killsMu sync.Mutex
		kills   []c32Kill
	)
	kill := func(o *c32Orker, when string) {
		running := e.runningItems(o.name)
		o.proxy.Cut()
		o.orker.stop()
		killsMu.Lock()
		kills = append(kills, c32Kill{Orker: o.name, OrkerID: e.realIDs[o.name], When: when,
			At: time.Now().UTC().Format(time.RFC3339Nano), Items: running})
		killsMu.Unlock()
		e.transcript.step("killed %s (%s) %s, running %v", o.name, e.realIDs[o.name], when, running)
	}
	// busy is the first live Orker of candidates running an item now.
	busy := func(candidates []*c32Orker) *c32Orker {
		for _, o := range candidates {
			if len(e.runningItems(o.name)) > 0 {
				return o
			}
		}
		return nil
	}

	// The kills watch the run from before it is submitted. A generation
	// reaches its hold line at a history set by the in-flight cap (its
	// reserve): at the default of 500 the first generation holds before half
	// the items have finished, which is when soakRun calls live, and the last
	// may finish without one. So the watcher takes the first hold of the run
	// that finds a real Orker running an item, and the switch after it. C32's
	// stack runs one run, so its log lines are that run's.
	stop := make(chan struct{})
	defer close(stop)
	// nextLog is the first workflow log line with msg stamped after after, or
	// false once the test is over.
	nextLog := func(msg string, after time.Time) (chaosLogEntry, bool) {
		for {
			e.logs.mu.Lock()
			notify := e.logs.notify
			e.logs.mu.Unlock()
			for _, entry := range e.logs.matching(msg, "") {
				if entry.At.After(after) {
					return entry, true
				}
			}
			select {
			case <-notify:
			case <-stop:
				return chaosLogEntry{}, false
			case <-ctx.Done():
				return chaosLogEntry{}, false
			}
		}
	}
	watched := make(chan error, 1)
	var progress atomic.Value // what the watcher last saw, for a failure's message
	progress.Store("no hold yet")
	go func() {
		watched <- func() error {
			// 1. During a hold, the real Orker the hold is waiting for.
			after := time.Now()
			var first *c32Orker
			for attempt := 0; attempt < 20 && first == nil; attempt++ {
				hold, ok := nextLog(logQuiesce, after)
				if !ok {
					return fmt.Errorf("the test ended with %d holds seen and no real Orker running an item at any", attempt)
				}
				after = hold.At
				if first = busy(killable); first == nil {
					e.transcript.step("hold at %v events: no real Orker running an item; waiting for the next hold",
						hold.Fields["history_events"])
					progress.Store(fmt.Sprintf("%d holds seen, none with a real Orker running an item", attempt+1))
				} else {
					e.transcript.step("hold at %v events with %s running %v", hold.Fields["history_events"], first.name,
						e.runningItems(first.name))
				}
			}
			if first == nil {
				return fmt.Errorf("no hold of 20 found a real Orker running an item")
			}
			kill(first, "while a hold waited for its items")
			progress.Store("killed " + first.name + " during a hold; no switch after it yet")
			killedAt := time.Now()

			// 2. Right after the next switch, another one with an item of the new
			// generation running: nothing is in flight at a switch, so every item
			// running after it was dispatched by the new generation.
			switched, ok := nextLog(logSwitched, killedAt)
			if !ok {
				return fmt.Errorf("the test ended before a switch after the first kill")
			}
			e.transcript.step("switched at %v events after the first kill", switched.Fields["history_events"])
			progress.Store("killed " + first.name + " during a hold; switched, no real Orker of the new generation busy yet")
			var rest []*c32Orker
			for _, o := range killable {
				if o != first {
					rest = append(rest, o)
				}
			}
			deadline := time.Now().Add(5 * time.Minute)
			var second *c32Orker
			for second == nil {
				second = busy(rest)
				if second == nil {
					if time.Now().After(deadline) {
						return fmt.Errorf("no surviving real Orker ran an item of the new generation within 5m")
					}
					select {
					case <-stop:
						return fmt.Errorf("the test ended before a real Orker ran an item of the new generation")
					case <-time.After(50 * time.Millisecond):
					}
				}
			}
			kill(second, "right after a switch, with an item of the new generation running")
			return nil
		}()
	}()

	// live waits for the watcher, and fails the moment the run closes
	// without both kills: a run that ends cannot hold or switch again.
	live := func(runID string) {
		for {
			select {
			case err := <-watched:
				if err != nil {
					t.Fatal(err)
				}
				return
			case <-time.After(100 * time.Millisecond):
			}
			if _, status := e.current(runID); status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
				select {
				case err := <-watched:
					if err != nil {
						t.Fatal(err)
					}
					return
				default:
				}
				killsMu.Lock()
				made := len(kills)
				killsMu.Unlock()
				t.Fatalf("the run closed %v with %d of its 2 kills made (%v)", status, made, progress.Load())
			}
		}
	}

	rec, events := e.soakRun(t, ctx, "c32", 0, live)
	requireContiguous(t, events)
	if len(kills) != 2 {
		t.Fatalf("%d kills, want 2", len(kills))
	}

	// Every item an Orker was killed running: retried elsewhere, or adopted
	// its commit; and no item's container ran twice in one attempt.
	ranOn := e.ranOnAll()
	checked := map[string]bool{}
	retried, adopted := 0, 0
	for _, k := range kills {
		if len(k.Items) == 0 {
			t.Fatalf("%s was killed running nothing", k.Orker)
		}
		for _, key := range k.Items {
			on := ranOn[key]
			if len(on) < 2 || on[len(on)-1] == k.Orker {
				t.Fatalf("%s ran on %v: the killed %s's item was never retried elsewhere", key, on, k.Orker)
			}
			records := c32ItemAttempts(t, ctx, e, rec.RunID, key)
			wasAdopted := false
			for _, r := range records {
				if r.GetCommitAdopted() {
					wasAdopted = true
				}
			}
			if wasAdopted {
				adopted++
			} else {
				retried++
			}
			e.transcript.step("%s (killed on %s): attempts on %v, %d attempt records, adopted %t", key, k.Orker, on,
				len(records), wasAdopted)
			checked[key] = true
		}
	}
	runs := 0
	for key, on := range ranOn {
		if !strings.HasPrefix(key, "spread[") || checked[key] {
			continue
		}
		for _, o := range on {
			if _, real := e.realIDs[o]; real {
				c32ItemAttempts(t, ctx, e, rec.RunID, key)
				runs++
				break
			}
		}
	}
	e.transcript.figure("kills", kills)
	e.transcript.figure("killed_items_retried_elsewhere", retried)
	e.transcript.figure("killed_items_adopted_their_commit", adopted)
	e.transcript.figure("real_items_checked_one_container_per_attempt", runs+len(checked))
	e.transcript.figure("generations", rec.GenerationCount)
	e.transcript.figure("events", len(events))
	e.transcript.figure("soak_record", rec)
	t.Logf("C32: %d items over %d generations; killed %s during a hold (%v) and %s after a switch (%v); %d killed "+
		"items retried elsewhere, %d adopted their commit; %d real-Orker items with one container per attempt; "+
		"every item SUCCEEDED once; %d events contiguous", items, rec.GenerationCount, kills[0].Orker, kills[0].Items,
		kills[1].Orker, kills[1].Items, retried, adopted, runs+len(checked), len(events))
}

// c32ItemAttempts reads one item's attempt records and fails when any attempt
// ran its container twice (two records of one attempt that both ran it).
func c32ItemAttempts(t *testing.T, ctx context.Context, e *eachSoak, runID, key string) []*orkstackv1.TaskAttempt {
	t.Helper()
	lb, rb := strings.Index(key, "["), strings.Index(key, "]")
	index, err := strconv.Atoi(key[lb+1 : rb])
	must.OKf(t, err, "item key %q", key)
	records, err := e.stores.attempts.ListAttempts(ctx, runID, key[:lb], artifacts.ItemAt(int32(index)))
	must.OKf(t, err, "attempt records of %s", key)
	ran := map[int32]int{}
	for _, r := range records {
		if !r.GetCommitAdopted() {
			ran[r.GetAttempt()]++
		}
	}
	for attempt, n := range ran {
		if n > 1 {
			t.Fatalf("%s attempt %d ran its container %d times", key, attempt, n)
		}
	}
	return records
}
