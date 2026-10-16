package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// tenThousandDocument is one fan-out at the built-in item maximum, over an
// upstream task's array — an input array of 10,000 elements would travel in
// every item's input and exceed the per-item bound, which is exactly the
// rule docs/DSL.md states — with no max_parallel, so only the in-flight cap
// and the holds bound it.
const tenThousandDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: ten-thousand
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: spread
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 10000}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: after
    uses: shell
    needs: [spread]
    with: {op: exec}
`

// TestEach_TenThousandItemsSpanGenerations: a 10,000-item fan-out with the
// run's in-flight cap (1,000) and continuation on runs across several
// generations — each one's history is driven to its hold line after another
// 2,000 items, so it holds new items, goes idle and switches — and every item
// is dispatched exactly once across them, the next generation resuming at
// the first item its predecessor never dispatched.
func TestEach_TenThousandItemsSpanGenerations(t *testing.T) {
	const items = 10000
	plan := continuedPlan(500)
	limit := engine.DefaultInFlightLimit(engine.DefaultRunMaxInFlight)
	plan.InFlight = &limit

	var mu sync.Mutex
	dispatched := map[int32]int{}
	behavior := func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach() != nil {
			mu.Lock()
			dispatched[in.GetEach().GetIndex()]++
			mu.Unlock()
			return itemResult(in), nil
		}
		return okBehavior(ctx, in)
	}
	g := runGenerations(t, "ork-ten-thousand", compile(t, tenThousandDocument), payload(t, nil), plan, behavior,
		func(h *harness, generation int) {
			h.countItems = func(*orkstackv1.ArtifactRef) (int32, error) { return items, nil }
			// The cluster's history jumps once this generation has seen
			// another 2,000 items succeed: 30,000 events plus the reserve of
			// what is in flight is past the 60% hold line of 51,200.
			threshold := (generation + 1) * 2000
			var once sync.Once
			h.onSnapshot = func(run *orkstackv1.Run, _ int64) error {
				if itemsSucceeded(run, "spread") >= threshold {
					once.Do(func() { h.env.SetCurrentHistoryLength(30000) })
				}
				return nil
			}
		})
	run := generationsOK(t, g, 3)
	if got := itemsSucceeded(run, "spread"); got != items {
		t.Fatalf("%d items SUCCEEDED, want %d", got, items)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dispatched) != items {
		t.Fatalf("%d distinct items dispatched, want %d", len(dispatched), items)
	}
	for index, n := range dispatched {
		if n != 1 {
			t.Fatalf("item %d dispatched %d times across generations", index, n)
		}
	}
	var per []string
	next := int32(0)
	for i, h := range g.harnesses {
		got := itemsDispatched(h)
		if len(got) == 0 {
			continue
		}
		if got[0] != next || got[len(got)-1] != next+int32(len(got))-1 {
			t.Fatalf("generation %d dispatched items %d..%d (%d), want a contiguous run from %d",
				i, got[0], got[len(got)-1], len(got), next)
		}
		next += int32(len(got))
		per = append(per, fmt.Sprintf("g%d:%d", i, len(got)))
	}
	t.Logf("%d items across %d generations, each exactly once: %v", items, len(g.harnesses), per)
}
