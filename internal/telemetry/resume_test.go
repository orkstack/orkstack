package telemetry

import (
	"bytes"
	"context"
	"maps"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordResumeAnomaly_CountsByNamespaceAndKind(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	o := newTestEngineObserver(t, reader, &buf, false)
	ctx := context.Background()
	o.RecordResumeAnomaly(ctx, "team-a", "reset_unreadable")
	o.RecordResumeAnomaly(ctx, "team-a", "reset_unreadable")
	var nilObserver *EngineObserver
	nilObserver.RecordResumeAnomaly(ctx, "team-a", "reset_unreadable") // no observer, nothing recorded

	var got int64
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != ResumeAnomaliesMetric {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				kind, _ := dp.Attributes.Value(AttrResumeAnomalyKind)
				ns, _ := dp.Attributes.Value(AttrNamespace)
				if kind.AsString() == "reset_unreadable" && ns.AsString() == "team-a" {
					got += dp.Value
				}
			}
		}
	}
	if got != 2 {
		t.Fatalf("%s{kind=reset_unreadable} = %d, want 2", ResumeAnomaliesMetric, got)
	}
}

// Every ending of a run counts once; one a resume led to says so, on
// ork.runs, ork.run.duration and the operator's twin alike, and a run's
// first ending carries no such label.
func TestObserveEvent_AResumedRunsEndingIsLabelled(t *testing.T) {
	row := func(d orkstackv1.ResumeDisposition) []*orkstackv1.TaskRun {
		return []*orkstackv1.TaskRun{{TaskId: "build"}, {TaskId: "deploy", ResumeDisposition: d}}
	}
	for name, tc := range map[string]struct {
		rows    []*orkstackv1.TaskRun
		code    string
		labeled bool
	}{
		"never resumed":                {rows: row(orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED), code: "RUN_FAILED"},
		"resumed, a task ran again":    {rows: row(orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN), code: "RUN_FAILED", labeled: true},
		"resumed, a result was kept":   {rows: row(orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT), labeled: true},
		"its plan could not be run":    {rows: row(orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED), code: "RESUME_PLAN_INVALID", labeled: true},
		"its plan could not be loaded": {code: "RESUME_PLAN_UNREADABLE", labeled: true},
	} {
		t.Run(name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			o := newTestEngineObserver(t, reader, &buf, false)
			started := time.Unix(1755302400, 0).UTC()
			run := &orkstackv1.Run{Id: "run-1", Namespace: "team-a", Status: orkstackv1.Status_STATUS_FAILED, TaskRuns: tc.rows,
				Error: &orkstackv1.Error{Code: tc.code}, StartedAt: timestamppb.New(started), FinishedAt: timestamppb.New(started.Add(time.Second))}
			o.ObserveEvent(context.Background(), runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, run.GetStatus(), ""), run)

			seen := map[string]bool{}
			check := func(metricName string, attrs attribute.Set) {
				seen[metricName] = true
				v, has := attrs.Value(AttrRunResumption)
				if has != tc.labeled || has && v.AsString() != "true" {
					t.Errorf("%s{%s} = %q (present %v), want present %v", metricName, AttrRunResumption, v.AsString(), has, tc.labeled)
				}
			}
			for _, sm := range collect(t, reader).ScopeMetrics {
				for _, m := range sm.Metrics {
					switch m.Name {
					case "ork.runs", DeploymentRunsMetric:
						for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
							check(m.Name, dp.Attributes)
						}
					case "ork.run.duration":
						for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
							check(m.Name, dp.Attributes)
						}
					}
				}
			}
			for _, want := range []string{"ork.runs", DeploymentRunsMetric, "ork.run.duration"} {
				if !seen[want] {
					t.Errorf("%s was not recorded", want)
				}
			}
		})
	}
}

func TestResumeEndingReasons_AreTheEngines(t *testing.T) {
	if resumePlanInvalid != engine.ReasonResumePlanInvalid || resumePlanUnreadable != engine.ReasonResumePlanUnreadable {
		t.Fatalf("telemetry reads %q and %q, the engine writes %q and %q", resumePlanInvalid, resumePlanUnreadable,
			engine.ReasonResumePlanInvalid, engine.ReasonResumePlanUnreadable)
	}
}

// points are every datapoint of one metric, by its attributes, as a count:
// a counter's and a gauge's value, a histogram's count of records.
func resumePoints(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	add := func(set attribute.Set, v float64) { out[set.Encoded(attribute.DefaultEncoder())] += v }
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, float64(dp.Value))
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, float64(dp.Value))
				}
			case metricdata.Gauge[float64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, dp.Value)
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, float64(dp.Count))
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, float64(dp.Count))
				}
			default:
				t.Fatalf("%s is a %T", name, d)
			}
		}
	}
	return out
}

// Every resume metric carries its namespace and only closed labels.
func TestResumeMetrics_AreRecordedWithTheirLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	o := newTestEngineObserver(t, reader, &buf, false)
	ctx := context.Background()
	run := &orkstackv1.Run{Id: "run-1", Namespace: "team-a"}

	o.RecordResumeRequest(ctx, ResumeRequest{Namespace: "team-a", Action: orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM, ForceCanceled: true, Outcome: "refused", Refusal: "CHILD_RUN"})
	o.RecordResumeRequest(ctx, ResumeRequest{Namespace: "team-a", Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME, Outcome: "resumed"})
	o.RecordResumeRequest(ctx, ResumeRequest{Namespace: "team-a", Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME, Outcome: "dial tcp: refused"})
	o.RecordResumePreflight(ctx, "team-a", 0.4)
	o.RecordResumeConfirmed(ctx, ResumeConfirmed{Namespace: "team-a", Tasks: map[string]int{"kept": 3}, Ordinal: 2, Took: time.Second, SinceFailure: time.Minute})
	o.RecordResumeConfirmed(ctx, ResumeConfirmed{Namespace: "team-a", Ordinal: 1, SinceFailure: -time.Second})
	o.RecordReadBackCall(ctx, "team-a", 2)
	o.RecordResumeEnding(ctx, ResumeEnding{Namespace: "team-a", Outcome: "failed", Nth: 7, BeforeAnythingReran: true, Active: time.Minute})
	o.RecordResumeEnding(ctx, ResumeEnding{Namespace: "team-a", Outcome: "terminated", Nth: 1, Active: -1})
	o.RecordResumeSweep(ctx, ResumeSweepPass{OldestReadingBack: 90 * time.Second, Unconfirmed: 2, Stalled: 1})
	o.RecordResumeSweep(ctx, ResumeSweepPass{OldestReadingBack: 30 * time.Second})
	for _, reason := range []string{"RUN_FAILED", "RUN_FORCE_CANCELED"} {
		e := runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED, orkstackv1.Status_STATUS_RUNNING, "")
		e.Resumed = &orkstackv1.RunResumed{Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME, SupersededReason: reason}
		o.ObserveEvent(ctx, e, run)
	}
	kept := runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT, orkstackv1.Status_STATUS_RUNNING, "")
	kept.ResultsKept = &orkstackv1.RunResultsKept{ReadBackSeconds: 1.5}
	o.ObserveEvent(ctx, kept, run)

	for name, want := range map[string]map[string]float64{
		ResumesMetric: {
			"action=retry_from,from_state=canceled_force,ork.namespace=team-a,ork.refusal.reason=child_run,outcome=refused": 1,
			"action=resume,from_state=failed,ork.namespace=team-a,outcome=resumed":                                          1,
			"action=resume,from_state=failed,ork.namespace=team-a,outcome=other":                                            1,
		},
		ResumedMetric: {
			"action=resume,from_state=failed,ork.namespace=team-a":         1,
			"action=resume,from_state=canceled_force,ork.namespace=team-a": 1,
		},
		ResumeHeadroomMetric:         {"ork.namespace=team-a": 1},
		ResumeTasksMetric:            {"kind=kept,ork.namespace=team-a": 1},
		ResumeOrdinalMetric:          {"ork.namespace=team-a": 2},
		ResumeConfirmDurationMetric:  {"ork.namespace=team-a": 2},
		ResumeSinceFailureMetric:     {"ork.namespace=team-a": 1}, // never a negative age
		ResumeReadBackAttemptsMetric: {"ork.namespace=team-a": 1},
		ResumeReadBackDurationMetric: {"ork.namespace=team-a,outcome=kept": 1},
		ResumeOutcomeMetric: {
			"before_anything_reran=true,nth=3+,ork.namespace=team-a,outcome=failed":     1,
			"before_anything_reran=false,nth=1,ork.namespace=team-a,outcome=terminated": 1,
		},
		ResumeActiveDurationMetric: {"ork.namespace=team-a,status=failed": 1}, // an unknown one is not recorded
		ResumeReadingBackMetric:    {"": 30},
		ResumeUnconfirmedMetric:    {"": 0},
		ResumeStalledMetric:        {"": 0},
	} {
		if got := resumePoints(t, reader, name); !maps.Equal(got, want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// Without telemetry every recorder is a no-op, and an event with no run
// records nothing.
func TestResumeMetrics_NilObserverAndRun(t *testing.T) {
	ctx := context.Background()
	var none *EngineObserver
	none.RecordResumeRequest(ctx, ResumeRequest{})
	none.RecordResumePreflight(ctx, "a", 1)
	none.RecordResumeConfirmed(ctx, ResumeConfirmed{})
	none.RecordReadBackCall(ctx, "a", 1)
	none.RecordResumeEnding(ctx, ResumeEnding{})
	none.RecordResumeSweep(ctx, ResumeSweepPass{})

	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	o := newTestEngineObserver(t, reader, &buf, false)
	o.ObserveEvent(ctx, runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED, orkstackv1.Status_STATUS_RUNNING, ""), nil)
	if got := resumePoints(t, reader, ResumedMetric); len(got) != 0 {
		t.Fatalf("an event with no run recorded %v", got)
	}
}
