package engine

import (
	"fmt"
	"strings"
	"testing"

	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestFailReserve_ChargesEverySleep: the history guard counts each sleep —
// a PENDING sleep as a pending task, an open one as an open await (its wake
// or its cancel), never as in flight — and opening a sleep moves the exact
// reserve by exactly openSleep's charge (chargeAwaitOpen), so the per-pass
// bound stays an upper bound. The quick bound counts every sleep up front
// and is never below the exact one.
func TestFailReserve_ChargesEverySleep(t *testing.T) {
	const n = 40
	var doc strings.Builder
	doc.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: guard-sleeps\ntasks:\n")
	for i := range n {
		fmt.Fprintf(&doc, "  - id: s%d\n    sleep: {seconds: 60}\n", i)
	}
	r := DefaultHistoryReserve
	s := buildRunState("ork-guard-sleeps", guardCompile(t, doc.String()), nil,
		guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes))
	inFlight := map[string]workflow.Future{}

	events, _ := s.failReserve(inFlight, r)
	if want := r.Fixed + r.PerPending*n; events != want {
		t.Fatalf("all pending: %d events, want Fixed + %d pending = %d", events, n, want)
	}
	for i := range n {
		id := fmt.Sprintf("s%d", i)
		before, _ := s.failReserve(inFlight, r)
		s.taskRuns[id].Status = orkstackv1.Status_STATUS_SLEEPING
		s.sleeps[id].open = true
		after, _ := s.failReserve(inFlight, r)
		if got, charge := after-before, r.PerAwait-r.PerPending; got != charge {
			t.Fatalf("opening %s moved failReserve by %d events, openSleep charges %d", id, got, charge)
		}
		if len(inFlight) != 0 || s.inFlightWork(inFlight) != 0 {
			t.Fatalf("an open sleep is in flight: %d entries, %d work", len(inFlight), s.inFlightWork(inFlight))
		}
		qe, qb := s.quickReserve(inFlight, r)
		ee, eb := s.failReserve(inFlight, r)
		if qe < ee || qb < ee*r.BytesPerEvent+eb {
			t.Fatalf("after opening %s the quick bound (%d, %d) is below the exact one (%d, %d)", id, qe, qb, ee, eb)
		}
	}
	if events, _ := s.failReserve(inFlight, r); events != r.Fixed+r.PerAwait*n {
		t.Fatalf("all sleeping: %d events, want Fixed + %d open sleeps = %d", events, n, r.Fixed+r.PerAwait*n)
	}
	// A wake lowers it by the open sleep's charge.
	before, _ := s.failReserve(inFlight, r)
	s.sleeps["s0"].open = false
	s.taskRuns["s0"].Status = orkstackv1.Status_STATUS_SUCCEEDED
	if after, _ := s.failReserve(inFlight, r); after != before-r.PerAwait {
		t.Fatalf("a wake moved failReserve %d → %d, want -%d", before, after, r.PerAwait)
	}
	// The run's failure message names what was still sleeping.
	if blockers := s.historyBlockers(inFlight); !strings.Contains(blockers, "sleeping: s1") {
		t.Fatalf("historyBlockers = %q, want it to name the open sleeps", blockers)
	}
}
