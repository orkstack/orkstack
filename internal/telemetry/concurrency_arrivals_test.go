package telemetry

import (
	"context"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestConcurrencyMetrics_ArrivalsAndExitsKeepTheirClosedLabels: an arrival
// and an exit are counted under the §9.9.1 labels, the Orkflow's name only
// under telemetry.orkflow_label, and a value outside a closed set is not
// recorded.
func TestConcurrencyMetrics_ArrivalsAndExitsKeepTheirClosedLabels(t *testing.T) {
	for _, orkflowLabel := range []bool{false, true} {
		p, reader := areaProviders(t)
		m := NewConcurrencyMetrics(p, nil, orkflowLabel, discardLogger())
		ctx := context.Background()
		a := ConcurrencyArrival{Namespace: "acme", Orkflow: "migrate", Limit: "db:{{ .inputs.env }}#1a2b3c4d", Level: "run", Strategy: "queue", Outcome: "queued"}
		m.Arrival(ctx, a)
		bad := a
		bad.Outcome = "teleported"
		m.Arrival(ctx, bad)
		exit := a
		exit.Outcome = "started"
		m.Exit(ctx, exit, 90*time.Second)
		bad = exit
		bad.Strategy = "lottery"
		m.Exit(ctx, bad, time.Second)
		points := collectGC(t, reader)
		want := map[string]string{AttrNamespace: "acme", AttrConcurrencyLimit: a.Limit, AttrConcurrencyLevel: "run",
			AttrConcurrencyStrategy: "queue", AttrConcurrencyOutcome: "queued"}
		if orkflowLabel {
			want[AttrOrkflowName] = "migrate"
		}
		if got := value(t, points, ConcurrencyArrivalsMetric, want); got != 1 || len(points[ConcurrencyArrivalsMetric]) != 1 {
			t.Fatalf("orkflow label %v: arrivals = %v (%d series), want one queued", orkflowLabel, got, len(points[ConcurrencyArrivalsMetric]))
		}
		want[AttrConcurrencyOutcome] = "started"
		if got := value(t, points, ConcurrencyExitsMetric, want); got != 1 || len(points[ConcurrencyExitsMetric]) != 1 {
			t.Fatalf("orkflow label %v: exits = %v, want one started", orkflowLabel, got)
		}
		if len(points[ConcurrencyQueueWaitMetric]) != 1 {
			t.Fatalf("orkflow label %v: queue wait series = %d, want one", orkflowLabel, len(points[ConcurrencyQueueWaitMetric]))
		}
	}
	var none *ConcurrencyMetrics
	none.Arrival(context.Background(), ConcurrencyArrival{})
	none.Exit(context.Background(), ConcurrencyArrival{}, time.Second)
}

// TestWaitOutcomes_EveryValueMaps: every outcome and strategy the event's
// enums declare, but UNSPECIFIED, has its label in the exits' closed sets.
func TestWaitOutcomes_EveryValueMaps(t *testing.T) {
	for v, name := range orkstackv1.ConcurrencyWaitOutcome_name {
		o := orkstackv1.ConcurrencyWaitOutcome(v)
		if o == orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_UNSPECIFIED {
			continue
		}
		if label, ok := waitOutcomes[o]; !ok || !slices.Contains(concurrencyExitOutcomes, label) {
			t.Errorf("%s maps to %q, not an exit outcome", name, label)
		}
	}
	for v, name := range orkstackv1.ConcurrencyStrategy_name {
		s := orkstackv1.ConcurrencyStrategy(v)
		if s == orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_UNSPECIFIED {
			continue
		}
		if label, ok := waitStrategies[s]; !ok || !slices.Contains(concurrencyStrategies, label) {
			t.Errorf("%s maps to %q, not a strategy", name, label)
		}
	}
}

// TestEngineObserver_CountsTheWaitAnEventEnds: an event that ends a wait
// in line counts one exit and its wait; any other counts none.
func TestEngineObserver_CountsTheWaitAnEventEnds(t *testing.T) {
	p, reader := areaProviders(t)
	observer, err := NewEngineObserver(p, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	o := observer.WithConcurrencyMetrics(NewConcurrencyMetrics(p, nil, false, discardLogger()))
	run := &orkstackv1.Run{Id: "ork-1", Namespace: "acme", OrkflowName: "migrate"}
	o.ObserveEvent(context.Background(), &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		WaitEnd: &orkstackv1.ConcurrencyWaitEnd{Outcome: orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED,
			Waited: durationpb.New(90 * time.Second), Strategy: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE, LimitLabel: "db#1a2b3c4d"}}, run)
	o.ObserveEvent(context.Background(), &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED}, run)
	points := collectGC(t, reader)
	want := map[string]string{AttrNamespace: "acme", AttrConcurrencyLimit: "db#1a2b3c4d", AttrConcurrencyLevel: "run",
		AttrConcurrencyStrategy: "queue", AttrConcurrencyOutcome: "started"}
	if got := value(t, points, ConcurrencyExitsMetric, want); got != 1 || len(points[ConcurrencyExitsMetric]) != 1 {
		t.Fatalf("exits = %v (%d series); want one started", got, len(points[ConcurrencyExitsMetric]))
	}
}
