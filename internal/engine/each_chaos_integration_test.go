//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// waitAfter blocks until the workflow log holds an entry with msg for runID
// stamped after after, and returns the first such entry.
func (b *chaosLogBook) waitAfter(t *testing.T, ctx context.Context, msg, runID string, after time.Time, within time.Duration) chaosLogEntry {
	t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	for {
		b.mu.Lock()
		notify := b.notify
		b.mu.Unlock()
		for _, e := range b.matching(msg, runID) {
			if e.At.After(after) {
				return e
			}
		}
		select {
		case <-notify:
		case <-deadline.C:
			t.Fatalf("no %q for %s after %s within %s", msg, runID, after.Format(time.RFC3339Nano), within)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

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
// contiguous across the switches.
func TestIntegration_ContinuationChaosC32_OrkersKilledAcrossGenerations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), soakTestTimeout)
	defer cancel()
	items := soakItems(t)
	e := startEachSoak(t, ctx, "C32", items)
	var killable []*c32Orker
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("real-%d", i)
		proxy := e.temporalProxy()
		o := e.startRealItemOrker(name, e.dial(name, proxy.Addr()))
		killable = append(killable, &c32Orker{name: name, proxy: proxy, orker: o})
	}
	var kills []c32Kill
	kill := func(o *c32Orker, when string) {
		running := e.running(o.name)
		o.proxy.Cut()
		o.orker.stop()
		kills = append(kills, c32Kill{Orker: o.name, OrkerID: e.realIDs[o.name], When: when,
			At: time.Now().UTC().Format(time.RFC3339Nano), Items: running})
		e.transcript.step("killed %s (%s) %s, running %v", o.name, e.realIDs[o.name], when, running)
	}
	// busy is the first live Orker of candidates running an item now.
	busy := func(candidates []*c32Orker) *c32Orker {
		for _, o := range candidates {
			if len(e.running(o.name)) > 0 {
				return o
			}
		}
		return nil
	}

	live := func(runID string) {
		// 1. During a hold, the real Orker the hold is waiting for.
		after := time.Now()
		var first *c32Orker
		for attempt := 0; attempt < 20 && first == nil; attempt++ {
			hold := e.logs.waitAfter(t, ctx, logQuiesce, runID, after, 10*time.Minute)
			after = hold.At
			if first = busy(killable); first == nil {
				e.transcript.step("hold at %v events: no real Orker running an item; waiting for the next hold",
					hold.Fields["history_events"])
			} else {
				e.transcript.step("hold at %v events with %s running %v", hold.Fields["history_events"], first.name,
					e.running(first.name))
			}
		}
		if first == nil {
			t.Fatal("no hold found a real Orker running an item")
		}
		kill(first, "while a hold waited for its items")
		killedAt := time.Now()

		// 2. Right after the next switch, another one with an item of the new
		// generation running: nothing is in flight at a switch, so every item
		// running after it was dispatched by the new generation.
		switched := e.logs.waitAfter(t, ctx, logSwitched, runID, killedAt, 10*time.Minute)
		e.transcript.step("switched at %v events after the first kill", switched.Fields["history_events"])
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
					t.Fatal("no surviving real Orker ran an item of the new generation within 5m")
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		kill(second, "right after a switch, with an item of the new generation running")
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
			records := itemAttempts(t, ctx, e, rec.RunID, key)
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
				itemAttempts(t, ctx, e, rec.RunID, key)
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

// itemAttempts reads one item's attempt records and fails when any attempt
// ran its container twice (two records of one attempt that both ran it).
func itemAttempts(t *testing.T, ctx context.Context, e *eachSoak, runID, key string) []*orkstackv1.TaskAttempt {
	t.Helper()
	lb, rb := strings.Index(key, "["), strings.Index(key, "]")
	index, err := strconv.Atoi(key[lb+1 : rb])
	if err != nil {
		t.Fatalf("item key %q: %v", key, err)
	}
	records, err := e.stores.attempts.ListAttempts(ctx, runID, key[:lb], artifacts.ItemAt(int32(index)))
	if err != nil {
		t.Fatalf("attempt records of %s: %v", key, err)
	}
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
