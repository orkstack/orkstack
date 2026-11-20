package engine_test

import (
	"errors"
	"path/filepath"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestRecordedHistories_ReplayAsUnderTheTypedEntry: every recorded
// RunWorkflow history replays under the raw-argument entry, which is what
// RegisterWorkflows registers under RunWorkflowType, exactly as it replays
// under the typed one: same commands, same outcome. A run that is not sealed
// opens each argument into the value the SDK's own decode gave.
func TestRecordedHistories_ReplayAsUnderTheTypedEntry(t *testing.T) {
	paths, err := filepath.Glob("testdata/history/*.json")
	must.OK(t, err)
	replayed := 0
	for _, path := range paths {
		recorded := loadHistoryFile(t, path)
		started := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
		if started.GetWorkflowType().GetName() != engine.RunWorkflowType {
			continue
		}
		replay := func(fn any) error {
			r := worker.NewWorkflowReplayer()
			r.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{Name: engine.RunWorkflowType})
			return r.ReplayWorkflowHistory(nil, recorded)
		}
		// A history that needs a replay option of its own (an original
		// execution id) fails under both entries alike: then the two errors
		// must be the same. Every other must replay cleanly under both.
		typed, raw := replay(engine.RunWorkflow), replay(engine.RunWorkflowRaw)
		switch {
		case typed == nil && raw == nil:
		case typed != nil && raw != nil && typed.Error() == raw.Error():
		default:
			t.Errorf("%s: the typed entry replays with %v, the raw one with %v", path, typed, raw)
		}
		replayed++
	}
	if replayed == 0 {
		t.Fatal("no recorded RunWorkflow history was replayed")
	}
}

// TestRunWorkflowRaw_WritesWhatTheTypedEntryWrites: for a run that is not
// sealed the raw entry dispatches the same task inputs, publishes the same
// events and returns the same result, byte for byte, as the typed one.
func TestRunWorkflowRaw_WritesWhatTheTypedEntryWrites(t *testing.T) {
	run := func(raw bool) ([]*orkstackv1.ExecuteTaskInput, []*orkstackv1.Event, *orkstackv1.Run) {
		h := rawHarness(t, "ork-same", nil, nil)
		args := sealedStartArgs(t, "ork-same", nil)
		if raw {
			h.env.ExecuteWorkflow(rawRunWorkflow, args...)
		} else {
			h.env.ExecuteWorkflow(engine.RunWorkflowType, args...)
		}
		must.OK(t, h.env.GetWorkflowError())
		var result converter.RawValue
		must.OK(t, h.env.GetWorkflowResult(&result))
		// The result's encoding is what is compared, then its contents with
		// the run's own times cleared.
		if got := string(result.Payload().GetMetadata()["encoding"]); got != "json/protobuf" {
			t.Fatalf("result encoding = %q", got)
		}
		var out *orkstackv1.Run
		must.OK(t, engine.DataConverter().FromPayload(proto.Clone(result.Payload()).(*commonpb.Payload), &out))
		out.CreatedAt, out.StartedAt, out.FinishedAt = nil, nil, nil
		for _, tr := range out.GetTaskRuns() {
			tr.StartedAt, tr.FinishedAt = nil, nil
		}
		return h.invocations(), h.bus.Events("ork-same", 0), out
	}
	typedIn, typedEvents, typedRun := run(false)
	rawIn, rawEvents, rawRun := run(true)
	if len(typedIn) != len(rawIn) || len(typedIn) == 0 {
		t.Fatalf("task inputs: %d typed, %d raw", len(typedIn), len(rawIn))
	}
	for i := range typedIn {
		if !proto.Equal(typedIn[i], rawIn[i]) {
			t.Errorf("task input %d differs:\n typed %v\n raw   %v", i, typedIn[i], rawIn[i])
		}
	}
	if len(typedEvents) != len(rawEvents) {
		t.Fatalf("events: %d typed, %d raw", len(typedEvents), len(rawEvents))
	}
	for i := range typedEvents {
		a, b := proto.Clone(typedEvents[i]).(*orkstackv1.Event), proto.Clone(rawEvents[i]).(*orkstackv1.Event)
		a.Timestamp, b.Timestamp = nil, nil
		if !proto.Equal(a, b) {
			t.Errorf("event %d differs:\n typed %v\n raw   %v", i, a, b)
		}
	}
	if !proto.Equal(typedRun, rawRun) {
		t.Errorf("results differ:\n typed %v\n raw   %v", typedRun, rawRun)
	}
}

// TestRunWorkflowRaw_AnUnsealedFailureIsTheErrorItself: with no engines' keys
// in the process, an unsealed run's failure is written by the SDK's own
// failure converter exactly as the typed entry's, its type its reason code.
func TestRunWorkflowRaw_AnUnsealedFailureIsTheErrorItself(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(engine.RunWorkflowRaw, workflow.RegisterOptions{Name: "raw"})
	env.ExecuteWorkflow("raw", "ork-empty", &orkstackv1.Orkflow{Name: "empty"}, payload(t, nil), defaultPlan())
	err := env.GetWorkflowError()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != "INVALID_ORKFLOW" {
		t.Fatalf("workflow error = %v, want an INVALID_ORKFLOW application error", err)
	}
	f := temporal.GetDefaultFailureConverter().ErrorToFailure(errors.Unwrap(err))
	if got := f.GetApplicationFailureInfo().GetType(); got != "INVALID_ORKFLOW" {
		t.Fatalf("the SDK default writes the failure with type %q, want INVALID_ORKFLOW", got)
	}
}
