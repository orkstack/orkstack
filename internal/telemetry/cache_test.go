package telemetry

import (
	"bytes"
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
)

// ork.cache.outcome joins the ork.tasks counter and the ork.task.duration
// histogram ONLY for a cached task: an uncached task's series are exactly
// what they were.
func TestEngineObserver_CacheOutcomeAttributeOnlyWhenSet(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)
	started := time.Unix(1755302400, 0).UTC()
	task := func(id string, cache *orkstackv1.CacheResult) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: id, Pool: "main", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1,
			ScheduledAt: timestamppb.New(started), FinishedAt: timestamppb.New(started.Add(time.Second)), Cache: cache}
	}
	run := &orkstackv1.Run{Id: "run-c", Namespace: "default", TaskRuns: []*orkstackv1.TaskRun{
		task("featurize", &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT}),
		task("stamp", nil),
	}}
	for _, id := range []string{"featurize", "stamp"} {
		observer.ObserveEvent(context.Background(),
			runEvent("run-c", orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED, id), run)
	}
	rm := collect(t, reader)
	var withLabel, withoutLabel, durationWithLabel int
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "ork.tasks":
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					if v, ok := dp.Attributes.Value(AttrCacheOutcome); ok {
						if v.AsString() != "hit" {
							t.Fatalf("ork.cache.outcome = %q", v.AsString())
						}
						withLabel++
					} else {
						withoutLabel++
					}
				}
			case "ork.task.duration":
				for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					if _, ok := dp.Attributes.Value(AttrCacheOutcome); ok {
						durationWithLabel++
					}
				}
			}
		}
	}
	if withLabel != 1 || withoutLabel != 1 || durationWithLabel != 1 {
		t.Fatalf("series with the label %d, without %d, duration with %d; want 1, 1, 1", withLabel, withoutLabel, durationWithLabel)
	}
}

func TestOrkerSpans_CacheOutcome(t *testing.T) {
	providers, exporter, _ := newObserverProviders(t)
	inst, err := NewOrkerInstruments(providers)
	if err != nil {
		t.Fatal(err)
	}
	obs := NewOrkerObserver(providers, inst, nil)
	for _, c := range []*orkstackv1.CacheResult{
		{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, Key: "k", MissReason: "MEMO_EXPIRED"},
		{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT, Key: "k"},
		nil,
	} {
		_, attempt := obs.Begin(context.Background(), observerTestInput(), 1)
		attempt.Phase("memo")
		attempt.End(orker.AttemptOutcome{Cache: c})
	}
	var spans []tracetest.SpanStub
	for _, s := range exporter.GetSpans() {
		if s.Name == "ork.task.attempt" {
			spans = append(spans, s)
		}
	}
	if len(spans) != 3 {
		t.Fatalf("%d attempt spans", len(spans))
	}
	if got, _ := attrString(spans[0].Attributes, AttrCacheOutcome); got != "miss" {
		t.Fatalf("miss span outcome = %q", got)
	}
	if got, _ := attrString(spans[0].Attributes, AttrCacheMissReason); got != "MEMO_EXPIRED" {
		t.Fatalf("miss span reason = %q", got)
	}
	if got, _ := attrString(spans[1].Attributes, AttrCacheOutcome); got != "hit" {
		t.Fatalf("hit span outcome = %q", got)
	}
	if _, ok := attrString(spans[1].Attributes, AttrCacheMissReason); ok {
		t.Fatal("a hit carries a miss reason")
	}
	if _, ok := attrString(spans[2].Attributes, AttrCacheOutcome); ok {
		t.Fatal("an uncached attempt carries ork.cache.outcome")
	}
	for _, s := range spans {
		for _, kv := range s.Attributes {
			if string(kv.Key) == "ork.cache.key" {
				t.Fatal("the key became an attribute")
			}
		}
	}
}
