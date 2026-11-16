package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// twinLabels are the only labels a twin may carry: closed enums and the pool.
var twinLabels = map[attribute.Key]bool{
	AttrStatus: true, AttrFailureCategory: true, AttrFailureReason: true, AttrPool: true,
	AttrCacheOutcome: true, AttrRefusalReason: true, AttrOutcome: true,
}

// metricAttrSets is every data point of every metric a reader holds, by name.
func metricAttrSets(t *testing.T, reader sdkmetric.Reader) map[string][]attribute.Set {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	got := map[string][]attribute.Set{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					got[m.Name] = append(got[m.Name], p.Attributes)
				}
			case metricdata.Histogram[float64]:
				for _, p := range data.DataPoints {
					got[m.Name] = append(got[m.Name], p.Attributes)
				}
			case metricdata.Histogram[int64]:
				for _, p := range data.DataPoints {
					got[m.Name] = append(got[m.Name], p.Attributes)
				}
			}
		}
	}
	return got
}

// TestInstruments_TwinsDropNamespaceLabels: a namespace on a named sink, with
// high cardinality on, reaches the default sink only through its twins, and
// a twin carries no namespace, name or selection label; its own records go
// to its own sink. A run whose sink is undeclared still counts its twin.
func TestInstruments_TwinsDropNamespaceLabels(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"payments": "team"}}
	f := newPolicyFixture(t, reg)
	f.def.cfg.HighCardinality = true
	f.team.cfg.HighCardinality = true
	must.OK(t, f.cache.Load(context.Background(), time.Second))
	o, err := NewEngineObserverForSinks(f.sinks, nil)
	must.OK(t, err)
	o.WithPolicyCache(f.cache)
	ctx := context.Background()

	now := time.Now()
	run := &orkstackv1.Run{
		Id: "ork-1", Namespace: "payments", OrkflowName: "secret-flow", Status: orkstackv1.Status_STATUS_FAILED,
		Telemetry: &orkstackv1.TelemetrySelection{Sink: "team", Attributes: map[string]string{"ork.ns.team": "blue"}},
		CreatedAt: timestamppb.New(now.Add(-time.Minute)), FinishedAt: timestamppb.New(now),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "charge", Pool: "gpu", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			ScheduledAt: timestamppb.New(now.Add(-time.Minute)), FinishedAt: timestamppb.New(now)}},
	}
	o.ObserveEvent(ctx, &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, TaskId: "charge"}, run)
	o.ObserveEvent(ctx, &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED}, run)
	o.ObserveRefusal(ctx, "payments", run.GetTelemetry(), "secret-flow", "NAMESPACE_HELD")
	o.RecordContinuationWrite(ctx, "payments", "checkpoint", "written", 100, 0.5, 1)
	stray := &orkstackv1.Run{Namespace: "stray", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Telemetry: &orkstackv1.TelemetrySelection{Sink: "ghost"}}
	o.ObserveEvent(ctx, &orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED}, stray)

	def := metricAttrSets(t, f.defRead)
	for _, name := range []string{DeploymentRunsMetric, DeploymentTasksMetric, DeploymentRunsRefusedMetric, DeploymentContinuationsMetric} {
		if len(def[name]) == 0 {
			t.Errorf("the default sink has no %s", name)
		}
	}
	for name, sets := range def {
		if strings.HasPrefix(name, "ork.deployment.") {
			for _, set := range sets {
				for _, kv := range set.ToSlice() {
					if !twinLabels[kv.Key] {
						t.Errorf("twin %s carries %s=%q", name, kv.Key, kv.Value.String())
					}
				}
			}
			continue
		}
		for _, set := range sets {
			if v, ok := set.Value(AttrNamespace); ok && v.AsString() == "payments" {
				t.Errorf("the default sink carries %s of payments, a namespace on another sink", name)
			}
		}
	}
	var runs int64
	var rm metricdata.ResourceMetrics
	must.OK(t, f.defRead.Collect(ctx, &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == DeploymentRunsMetric {
				for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
					runs += p.Value
				}
			}
		}
	}
	if runs != 2 {
		t.Errorf("ork.deployment.runs = %d; want 2 (the undeclared sink's run counts its twin)", runs)
	}

	team := metricAttrSets(t, f.teamRead)
	for _, name := range []string{"ork.runs", "ork.tasks", "ork.runs.refused", "ork.run.continuations"} {
		if len(team[name]) == 0 {
			t.Errorf("payments' own sink has no %s", name)
		}
	}
	for name := range team {
		if strings.HasPrefix(name, "ork.deployment.") {
			t.Errorf("a named sink carries the twin %s; twins are the default sink's alone", name)
		}
	}
}

// TestContinuation_FollowsTheNamespaceSink: a continuation's records go to
// its namespace's sink through the policy cache, and a namespace the cache
// cannot route is dropped and counted, never sent to the default sink.
func TestContinuation_FollowsTheNamespaceSink(t *testing.T) {
	reg := &fakeRegistry{sinks: map[string]string{"payments": "team", "ops": ""}}
	f := newPolicyFixture(t, reg)
	o, err := NewEngineObserverForSinks(f.sinks, nil)
	must.OK(t, err)
	o.WithPolicyCache(f.cache)
	ctx := context.Background()

	o.RecordContinuationRestore(ctx, "payments", "restored", 1, true, 2)
	if got := droppedRecords(t, f.defRead); got[SignalMetrics+"/"+DropReasonUnrouted] != 1 {
		t.Fatalf("dropped = %v; want one unrouted before the cache loads", got)
	}
	must.OK(t, f.cache.Load(ctx, time.Second))
	o.RecordContinuationRestore(ctx, "payments", "restored", 1, true, 2)
	o.RecordContinuationRestore(ctx, "ops", "restored", 1, true, 2)

	namespaces := func(reader sdkmetric.Reader) map[string]bool {
		out := map[string]bool{}
		for _, set := range metricAttrSets(t, reader)["ork.run.continuations"] {
			v, _ := set.Value(AttrNamespace)
			out[v.AsString()] = true
		}
		return out
	}
	if got := namespaces(f.teamRead); !got["payments"] || got["ops"] {
		t.Errorf("team sink continuations by namespace = %v; want payments only", got)
	}
	if got := namespaces(f.defRead); !got["ops"] || got["payments"] {
		t.Errorf("default sink continuations by namespace = %v; want ops only", got)
	}
}
