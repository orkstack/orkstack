package telemetry

import (
	"bytes"
	"context"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// reasonOf is the failure reason a metric's one data point carries.
func reasonOf(t *testing.T, sets []attribute.Set, name string) (string, bool) {
	t.Helper()
	if len(sets) != 1 {
		t.Fatalf("%s has %d data points, want 1", name, len(sets))
	}
	v, ok := sets[0].Value(AttrFailureReason)
	return v.AsString(), ok
}

// A run's ending says why on ork.runs and on its twin, for the reasons of
// the closed set alone: any other, and any the code does not know, is never
// a label value, so the label cannot grow. Durations never carry it.
func TestObserveEvent_ARunsEndingNamesItsReasonFromAClosedSet(t *testing.T) {
	at := time.Unix(1755302400, 0)
	for code, want := range map[string]string{
		"RUN_HISTORY_LIMIT": "RUN_HISTORY_LIMIT",
		"RUN_FAILED":        "",
		"MADE_UP_BY_A_TASK": "",
		// A task's reason, not a run's.
		"PIN_HOST_LOST": "",
		"":              "",
	} {
		t.Run(code, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			o := newTestEngineObserver(t, reader, &buf, false)
			run := &orkstackv1.Run{Id: "run-1", Namespace: "team-a", Status: orkstackv1.Status_STATUS_FAILED,
				StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Second))}
			if code != "" {
				run.Error = &orkstackv1.Error{Code: code}
			}
			o.ObserveEvent(context.Background(), runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, run.GetStatus(), ""), run)
			got := metricAttrSets(t, reader)
			for _, name := range []string{"ork.runs", DeploymentRunsMetric} {
				if reason, has := reasonOf(t, got[name], name); reason != want || has != (want != "") {
					t.Errorf("%s{%s} = %q (present %v), want %q", name, AttrFailureReason, reason, has, want)
				}
			}
			if _, has := reasonOf(t, got["ork.run.duration"], "ork.run.duration"); has {
				t.Error("ork.run.duration carries the failure reason")
			}
		})
	}
}

// A task's ending names a pin's reasons, which its run's error (RUN_FAILED)
// does not, on ork.tasks and on its twin.
func TestObserveEvent_ATasksEndingNamesAPinsReason(t *testing.T) {
	at := time.Unix(1755302400, 0)
	for code, want := range map[string]string{
		"PIN_HOST_LOST":     "PIN_HOST_LOST",
		"PIN_UNAVAILABLE":   "PIN_UNAVAILABLE",
		"EXIT_NONZERO":      "",
		"RUN_HISTORY_LIMIT": "",
		"":                  "",
	} {
		t.Run(code, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			o := newTestEngineObserver(t, reader, &buf, false)
			task := &orkstackv1.TaskRun{TaskId: "build", Pool: "gpu", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 1,
				ScheduledAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Second))}
			if code != "" {
				task.Error = &orkstackv1.Error{Code: code}
			}
			run := &orkstackv1.Run{Id: "run-1", Namespace: "team-a", Status: orkstackv1.Status_STATUS_RUNNING, TaskRuns: []*orkstackv1.TaskRun{task}}
			o.ObserveEvent(context.Background(), runEvent("run-1", orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, task.GetStatus(), "build"), run)
			got := metricAttrSets(t, reader)
			for _, name := range []string{"ork.tasks", DeploymentTasksMetric} {
				if reason, has := reasonOf(t, got[name], name); reason != want || has != (want != "") {
					t.Errorf("%s{%s} = %q (present %v), want %q", name, AttrFailureReason, reason, has, want)
				}
			}
			for _, name := range []string{"ork.task.duration", "ork.task.attempts"} {
				if _, has := reasonOf(t, got[name], name); has {
					t.Errorf("%s carries the failure reason", name)
				}
			}
		})
	}
}

func TestRunFailureReasons_AreTheEngines(t *testing.T) {
	if !runFailureReasons[engine.RunHistoryLimitCode] {
		t.Fatalf("the engine ends a run %s, which the metric does not name", engine.RunHistoryLimitCode)
	}
}
