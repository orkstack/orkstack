package main

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// allCapable is a fleet in which every engine process can pause a run.
type allCapable struct{}

func (allCapable) MissingCapability(context.Context, string, engine.CapabilityScope) ([]string, error) {
	return nil, nil
}

// pauseCLIFixture holds one running, pausable run with two events.
func pauseCLIFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Capabilities = allCapable{} })
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true, OrkflowName: "hello-ork"})
	f.publish("ork-r", orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.Status_STATUS_PENDING, "created")
	f.publish("ork-r", orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING, "started")
	return f
}

// applyPauses makes the stub run apply each request as the engine would:
// the run reads PAUSED or RUNNING, and the event carrying the request's id
// is published.
func (f *fixture) applyPauses(expires time.Time) {
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	f.starter.onPause = func(runID string, sig *orkstackv1.RunPauseSignal) {
		run, _ := f.runs.Get(runID)
		typ, status, transition := orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED, orkstackv1.Status_STATUS_RUNNING,
			orkstackv1.PauseTransition_PAUSE_TRANSITION_UNPAUSED
		run.Pause = nil
		if sig.GetPause() {
			typ, status, transition = orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, orkstackv1.Status_STATUS_PAUSED,
				orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED
			run.Pause = &orkstackv1.RunPause{PausedBy: sig.GetPrincipal(), RequestId: sig.GetRequestId(),
				PausedAt: timestamppb.New(fixedNow), ExpiresAt: timestamppb.New(expires)}
		}
		run.Status = status
		f.runs.Apply(run)
		f.bus.Publish(&orkstackv1.Event{RunId: runID, Type: typ, Status: status, RequestId: sig.GetRequestId(),
			PauseTransition: transition, Timestamp: timestamppb.New(fixedNow)}, "")
	}
}

// run pause --wait exits 0 once the run's event carries its request id;
// unpause too.
func TestRunPause_WaitApplied(t *testing.T) {
	f := pauseCLIFixture(t)
	expires := fixedNow.Add(7 * 24 * time.Hour)
	f.applyPauses(expires)
	f.run("run", "pause", "ork-r", "--reason", "db", "--wait", "--wait-timeout", "10s").
		requireExit(t, 0).requireContains(t, "run ork-r paused until "+expires.Format(time.RFC3339))
	f.run("run", "unpause", "ork-r", "--wait", "--wait-timeout", "10s").
		requireExit(t, 0).requireContains(t, "run ork-r unpaused")
}

// Without --wait the command exits once the request is recorded, and
// prints the warnings to stderr.
func TestRunPause_NoWait(t *testing.T) {
	f := pauseCLIFixture(t)
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true,
		Error: &orkstackv1.Error{Code: "X"},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "h", Status: orkstackv1.Status_STATUS_PENDING,
			Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED}}})
	f.run("run", "pause", "ork-r", "--timeout", "1h").requireExit(t, 0).
		requireContains(t, "Pause of run ork-r recorded (request pause-").
		requireStderrContains(t, "warning: the run is failing")
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.pauses) != 1 || f.starter.pauses[0].GetTimeoutSeconds() != 3600 {
		t.Fatalf("pauses = %v, want one of 3600 s", f.starter.pauses)
	}
}

// --wait exits 4 when the run ends first, 5 when the wait runs out, 6 when
// another request paused it first, and 1 when the request is refused.
func TestRunPause_WaitOutcomes(t *testing.T) {
	t.Run("the run ended first", func(t *testing.T) {
		f := pauseCLIFixture(t)
		f.starter.onPause = func(runID string, _ *orkstackv1.RunPauseSignal) {
			f.finishRun(runID, orkstackv1.Status_STATUS_FAILED, &orkstackv1.Error{Code: "TASK_FAILED"})
			f.publish(runID, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, orkstackv1.Status_STATUS_FAILED, "failed")
		}
		f.run("run", "pause", "ork-r", "--wait", "--wait-timeout", "10s").
			requireExit(t, exitPauseRunEnded).requireContains(t, "run ork-r ended (FAILED TASK_FAILED); the request had no effect")
	})
	t.Run("the wait ran out", func(t *testing.T) {
		f := pauseCLIFixture(t)
		f.run("run", "pause", "ork-r", "--wait", "--wait-timeout", "300ms").
			requireExit(t, exitPauseTimeout).requireContains(t, "the run has not applied request pause-")
	})
	t.Run("another request paused it first", func(t *testing.T) {
		f := pauseCLIFixture(t)
		f.starter.onPause = func(runID string, _ *orkstackv1.RunPauseSignal) {
			f.bus.Publish(&orkstackv1.Event{RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED,
				Status: orkstackv1.Status_STATUS_PAUSED, RequestId: "pause-other",
				PauseTransition: orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED}, "")
		}
		f.run("run", "pause", "ork-r", "--wait", "--wait-timeout", "10s").
			requireExit(t, exitPauseOtherRequest).requireContains(t, "run ork-r is paused, by another request")
	})
	t.Run("refused", func(t *testing.T) {
		f := pauseCLIFixture(t)
		f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING})
		f.run("run", "pause", "ork-r", "--wait").requireExit(t, 1).
			requireStderrContains(t, "RUN_PAUSE_UNSUPPORTED")
	})
}

// --json with --wait prints the response, the outcome and the event.
func TestRunPause_WaitJSON(t *testing.T) {
	f := pauseCLIFixture(t)
	f.applyPauses(fixedNow.Add(time.Hour))
	out := f.run("--json", "run", "pause", "ork-r", "--wait", "--wait-timeout", "10s").requireExit(t, 0)
	var doc struct {
		Response struct {
			RequestID string `json:"requestId"`
		} `json:"response"`
		Outcome string `json:"outcome"`
		Event   struct {
			RequestID string `json:"requestId"`
			Type      string `json:"type"`
		} `json:"event"`
	}
	if err := json.Unmarshal([]byte(out.stdout), &doc); err != nil {
		t.Fatalf("decode %q: %v", out.stdout, err)
	}
	if doc.Outcome != "applied" || doc.Response.RequestID == "" || doc.Event.RequestID != doc.Response.RequestID {
		t.Fatalf("document = %+v", doc)
	}
}

// --dry-run prints the warnings on stdout and sends nothing.
func TestRunPause_DryRun(t *testing.T) {
	f := pauseCLIFixture(t)
	f.runs.Save(&orkstackv1.Run{Id: "ork-r", Status: orkstackv1.Status_STATUS_RUNNING, Pausable: true,
		Error: &orkstackv1.Error{Code: "X"},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "h", Status: orkstackv1.Status_STATUS_PENDING,
			Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED}}})
	f.run("run", "pause", "ork-r", "--dry-run").requireExit(t, 0).
		requireContains(t, "Run ork-r can be paused; nothing was sent", "warning: the run is failing")
	f.run("run", "pause", "ork-r", "--dry-run", "--wait").requireExit(t, 1)
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.pauses) != 0 {
		t.Fatalf("a dry run sent %v", f.starter.pauses)
	}
}
