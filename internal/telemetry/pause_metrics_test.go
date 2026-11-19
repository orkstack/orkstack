package telemetry

import (
	"bytes"
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// pauseCount is ork.runs.pauses's value for one cause and outcome.
func pauseCount(rm metricdata.ResourceMetrics, cause, outcome string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != PausesMetric {
				continue
			}
			sum, _ := m.Data.(metricdata.Sum[int64])
			for _, dp := range sum.DataPoints {
				c, _ := dp.Attributes.Value(AttrPauseCause)
				o, _ := dp.Attributes.Value(AttrPauseOutcome)
				if c.AsString() == cause && o.AsString() == outcome {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

func pauseDurationSum(rm metricdata.ResourceMetrics, outcome string) float64 {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != PauseDurationMetric {
				continue
			}
			h, _ := m.Data.(metricdata.Histogram[float64])
			for _, dp := range h.DataPoints {
				if o, _ := dp.Attributes.Value(AttrPauseOutcome); o.AsString() == outcome {
					return dp.Sum
				}
			}
		}
	}
	return -1
}

// One transition counts once, by who caused it and what it did; an own
// pause's end records how long it lasted; and a namespace's first pause
// event starts every outcome series at 0.
func TestObserveEvent_PauseTransitions(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	o := newTestEngineObserver(t, reader, &buf, false)
	at := time.Unix(1755302400, 0).UTC()
	run := &orkstackv1.Run{Id: "run-1", Namespace: "default", OrkflowName: "hello", Status: orkstackv1.Status_STATUS_PAUSED}
	ctx := context.Background()
	o.ObserveEvent(ctx, &orkstackv1.Event{RunId: "run-1", Type: orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, RequestId: "pause-1",
		PauseTransition: orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED, Status: orkstackv1.Status_STATUS_PAUSED}, run)
	rm := collect(t, reader)
	if n, _ := pauseCount(rm, "self", "paused"); n != 1 {
		t.Fatalf("self/paused = %d, want 1", n)
	}
	if n, ok := pauseCount(rm, "parent", "timed_out"); !ok || n != 0 {
		t.Fatalf("parent/timed_out = %d (present %t), want a 0 series", n, ok)
	}
	unpaused := &orkstackv1.Run{Id: "run-1", Namespace: "default", OrkflowName: "hello", Status: orkstackv1.Status_STATUS_RUNNING,
		LastPause: &orkstackv1.RunPause{PausedAt: timestamppb.New(at), EndedAt: timestamppb.New(at.Add(90 * time.Second)), End: orkstackv1.PauseEnd_PAUSE_END_UNPAUSED}}
	o.ObserveEvent(ctx, &orkstackv1.Event{RunId: "run-1", Type: orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED, RequestId: "unpause-1",
		PauseTransition: orkstackv1.PauseTransition_PAUSE_TRANSITION_UNPAUSED, Status: orkstackv1.Status_STATUS_RUNNING}, unpaused)
	o.ObserveEvent(ctx, &orkstackv1.Event{RunId: "run-2", Type: orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED,
		PauseTransition: orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED, Status: orkstackv1.Status_STATUS_PAUSED},
		&orkstackv1.Run{Id: "run-2", Namespace: "default", PausedByParent: &orkstackv1.InheritedPause{ParentRunId: "run-1"}})
	o.ObserveEvent(ctx, &orkstackv1.Event{RunId: "run-3", Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		PauseTransition: orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_TIMED_OUT, Status: orkstackv1.Status_STATUS_FAILED},
		&orkstackv1.Run{Id: "run-3", Namespace: "default", Status: orkstackv1.Status_STATUS_FAILED,
			LastPause: &orkstackv1.RunPause{PausedAt: timestamppb.New(at), EndedAt: timestamppb.New(at.Add(time.Hour)), End: orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT}})
	o.ObserveEvent(ctx, &orkstackv1.Event{RunId: "run-4", Type: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED}, run)
	rm = collect(t, reader)
	for _, want := range []struct {
		cause, outcome string
		n              int64
	}{{"self", "paused", 1}, {"self", "unpaused", 1}, {"parent", "paused", 1}, {"self", "timed_out", 1}, {"parent", "not_paused", 0}} {
		if n, _ := pauseCount(rm, want.cause, want.outcome); n != want.n {
			t.Errorf("%s/%s = %d, want %d", want.cause, want.outcome, n, want.n)
		}
	}
	if got := pauseDurationSum(rm, "unpaused"); got != 90 {
		t.Errorf("unpaused duration sum = %v, want 90", got)
	}
	if got := pauseDurationSum(rm, "timed_out"); got != 3600 {
		t.Errorf("timed_out duration sum = %v, want 3600", got)
	}
}

// The paused-run census reports each namespace's count and how long until
// its soonest own pause ends; a namespace whose runs are unpaused reads 0,
// and a failed pass reports nothing.
func TestRunsPausedGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	now := time.Unix(1755302400, 0).UTC()
	answer := map[string]PausedSummary{"payments": {Count: 2, Soonest: now.Add(time.Hour)}, "default": {Count: 1}}
	var fail error
	pass := newRunsPausedGauges(&Providers{Meter: mp}, nil, func() time.Time { return now },
		func(context.Context) (map[string]PausedSummary, error) { return answer, fail })
	if err := pass(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	gauge := func(rm metricdata.ResourceMetrics, name, ns string) (float64, bool) {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != name {
					continue
				}
				switch d := m.Data.(type) {
				case metricdata.Gauge[int64]:
					for _, dp := range d.DataPoints {
						if v, _ := dp.Attributes.Value(AttrNamespace); v.AsString() == ns {
							return float64(dp.Value), true
						}
					}
				case metricdata.Gauge[float64]:
					for _, dp := range d.DataPoints {
						if v, _ := dp.Attributes.Value(AttrNamespace); v.AsString() == ns {
							return dp.Value, true
						}
					}
				}
			}
		}
		return 0, false
	}
	rm := collect(t, reader)
	if v, _ := gauge(rm, RunsPausedMetric, "payments"); v != 2 {
		t.Errorf("payments paused = %v, want 2", v)
	}
	if v, _ := gauge(rm, RunsPausedSoonestMetric, "payments"); v != 3600 {
		t.Errorf("payments soonest = %v, want 3600", v)
	}
	if _, ok := gauge(rm, RunsPausedSoonestMetric, "default"); ok {
		t.Error("a namespace with no own pause reports a soonest end")
	}
	answer = map[string]PausedSummary{}
	_ = pass(context.Background())
	if v, ok := gauge(collect(t, reader), RunsPausedMetric, "payments"); !ok || v != 0 {
		t.Errorf("unpaused namespace reads %v (present %t), want 0", v, ok)
	}
	fail = context.DeadlineExceeded
	_ = pass(context.Background())
	if _, ok := gauge(collect(t, reader), RunsPausedMetric, "payments"); ok {
		t.Error("a failed pass still reports a count")
	}
}
