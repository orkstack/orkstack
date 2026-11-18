package engine_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The pause carry's fixtures (TestIntegration_RecordPauseContinuedHistories).
const (
	pauseCarriedG0 = "testdata/history/pause-continued-carried-g0-v1.json"
	pauseCarriedG1 = "testdata/history/pause-continued-carried-g1-v1.json"
	// v2, recorded paced (TestIntegration_RecordPauseContinuedHistories now).
	pauseCarriedV2G0 = "testdata/history/pause-continued-carried-g0-v2.json"
	pauseCarriedV2G1 = "testdata/history/pause-continued-carried-g1-v2.json"
)

// The paused index's fixtures (TestIntegration_RecordPauseIndexedHistories).
const (
	pauseIndexedG0 = "testdata/history/pause-indexed-continued-g0-v1.json"
	pauseIndexedG1 = "testdata/history/pause-indexed-continued-g1-v1.json"
)

// E103 PR2a's fixtures: the carried pause request
// (TestIntegration_RecordPauseDeferredHistories), the cascade's parent and
// child (TestIntegration_RecordPauseCascadeHistories) and a paused run's pin
// (TestIntegration_RecordPausePinHistory).
const (
	pauseDeferredG0    = "testdata/history/pause-deferred-g0-v1.json"
	pauseDeferredG1    = "testdata/history/pause-deferred-g1-v1.json"
	pauseCascadeParent = "testdata/history/pause-cascade-parent-v1.json"
	pauseCascadeChild  = "testdata/history/pause-cascade-child-v1.json"
	pausePinFixture    = "testdata/history/pause-pin-clock-v1.json"
)

// pausePR2Fixtures pairs each with the start whose run id it replays as, and
// the plan field that enters its path: with it cleared from the start, the
// replayer must refuse the history.
// nondeterminismCode is the SDK's code for a history the code does not
// reproduce: a negative replay must be refused with it, not for a reason of
// its own.
const nondeterminismCode = "TMPRL1100"

var pausePR2Fixtures = []struct {
	path, first string
	clear       func(p *engine.PausePlan)
}{
	{pauseDeferredG0, pauseDeferredG0, func(p *engine.PausePlan) { p.MaxDeferredRequests = 0 }},
	{pauseDeferredG1, pauseDeferredG0, func(p *engine.PausePlan) { p.MaxDeferredRequests = 0 }},
	{pauseCascadeParent, pauseCascadeParent, func(p *engine.PausePlan) { p.Cascades = false }},
	{pausePinFixture, pausePinFixture, func(p *engine.PausePlan) { p.PinTimeoutPausable = false }},
}

// TestRunWorkflow_ReplaysCommittedPausePR2Histories is the command half of
// the carried request, the cascade and the pausable pin, and each one's
// negative: replayed as recorded, and refused with the plan field that
// enters its path cleared. The cascade's child replays as recorded; its
// commands are PR1's inherited pause.
func TestRunWorkflow_ReplaysCommittedPausePR2Histories(t *testing.T) {
	for _, f := range pausePR2Fixtures {
		t.Run(filepath.Base(f.path), func(t *testing.T) {
			requireCommittedFixture(t, f.path)
			requireCommittedFixture(t, f.first)
			runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, f.first))
			recorded := loadHistoryFile(t, f.path)
			if err := replayRunTimeoutHistory(t, recorded, runID); err != nil {
				t.Fatalf("replaying %s against current code failed: %v", f.path, err)
			}
			attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
			id, wf, input, plan, carry := startArgs(t, recorded)
			if plan.Pause == nil {
				t.Fatalf("%s started without a pause plan", f.path)
			}
			// The control: the start re-encoded as it was still replays, so
			// the cleared field is the one difference the refusal can be for.
			payloads, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = payloads
			if err := replayRunTimeoutHistory(t, recorded, runID); err != nil {
				t.Fatalf("%s re-encoded unchanged does not replay: %v", f.path, err)
			}
			f.clear(plan.Pause)
			payloads, err = converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = payloads
			err = replayRunTimeoutHistory(t, recorded, runID)
			if err == nil {
				t.Fatalf("%s replayed with its path's plan field cleared: it does not pin that path", f.path)
			}
			if !strings.Contains(err.Error(), nondeterminismCode) {
				t.Fatalf("%s with its plan field cleared was refused for another reason: %v", f.path, err)
			}
		})
	}
	t.Run(filepath.Base(pauseCascadeChild), func(t *testing.T) {
		requireCommittedFixture(t, pauseCascadeChild)
		runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, pauseCascadeChild))
		if err := replayRunTimeoutHistory(t, loadHistoryFile(t, pauseCascadeChild), runID); err != nil {
			t.Fatalf("replaying %s against current code failed: %v", pauseCascadeChild, err)
		}
	})
}

var pauseContinuedFixtures = []struct{ path, first string }{
	{pauseCarriedG0, pauseCarriedG0},
	{pauseCarriedG1, pauseCarriedG0},
	{pauseCarriedV2G0, pauseCarriedV2G0},
	{pauseCarriedV2G1, pauseCarriedV2G0},
}

var pauseIndexedFixtures = []struct{ path, first string }{
	{pauseIndexedG0, pauseIndexedG0},
	{pauseIndexedG1, pauseIndexedG0},
}

// TestRunWorkflow_ReplaysCommittedPauseIndexedHistories is the paused
// index's command half, and its negative: each generation replays as
// recorded, and with Indexed removed from its plan the replayer refuses it,
// so the fixtures pin the index writes on both sides of the switch.
func TestRunWorkflow_ReplaysCommittedPauseIndexedHistories(t *testing.T) {
	for _, f := range pauseIndexedFixtures {
		t.Run(filepath.Base(f.path), func(t *testing.T) {
			requireCommittedFixture(t, f.path)
			requireCommittedFixture(t, f.first)
			runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, f.first))
			recorded := loadHistoryFile(t, f.path)
			if err := replayRunTimeoutHistory(t, recorded, runID); err != nil {
				t.Fatalf("replaying %s against current code failed: %v", f.path, err)
			}
			attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
			id, wf, input, plan, carry := startArgs(t, recorded)
			if plan.Pause == nil || !plan.Pause.Indexed {
				t.Fatalf("%s started without an Indexed pause plan", f.path)
			}
			plan.Pause.Indexed = false
			payloads, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = payloads
			if err := replayRunTimeoutHistory(t, recorded, runID); err == nil {
				t.Fatalf("%s replayed with Indexed removed: it does not pin the index writes", f.path)
			}
		})
	}
}

// TestRunWorkflow_ReplaysCommittedPauseCarryHistories is the command half:
// each generation replays as the execution of its run's id.
func TestRunWorkflow_ReplaysCommittedPauseCarryHistories(t *testing.T) {
	for _, f := range pauseContinuedFixtures {
		t.Run(filepath.Base(f.path), func(t *testing.T) {
			requireCommittedFixture(t, f.path)
			requireCommittedFixture(t, f.first)
			runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, f.first))
			recorded := loadHistoryFile(t, f.path)
			if _, _, _, plan, _ := startArgs(t, recorded); plan.Pause == nil || plan.Continuation == nil {
				t.Fatalf("%s started without its pause plan or continuation budget", f.path)
			}
			if err := replayRunTimeoutHistory(t, recorded, runID); err != nil {
				t.Fatalf("replaying %s against current code failed: %v", f.path, err)
			}
		})
	}
}

// TestPauseCarryHistories_CatchThePauseRemoved is the negative test: with
// the pause plan removed from generation 0's start, and the carried pause
// from generation 1's, the replayer must refuse each, so the fixtures pin
// the pause's commands on both sides of the switch.
func TestPauseCarryHistories_CatchThePauseRemoved(t *testing.T) {
	for _, f := range pauseContinuedFixtures {
		t.Run(filepath.Base(f.path), func(t *testing.T) {
			requireCommittedFixture(t, f.path)
			requireCommittedFixture(t, f.first)
			runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, f.first))
			recorded := loadHistoryFile(t, f.path)
			attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
			id, wf, input, plan, carry := startArgs(t, recorded)
			unchanged, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = unchanged
			must.OKf(t, replayRunTimeoutHistory(t, recorded, runID), "%s re-encoded unchanged does not replay", f.path)
			if carry == nil {
				plan.Pause = nil
			} else {
				carry.Pause = nil
			}
			payloads, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = payloads
			if err := replayRunTimeoutHistory(t, recorded, runID); err == nil {
				t.Fatalf("%s replayed with its pause removed: it does not pin the pause's commands", f.path)
			}
		})
	}
}

// TestPauseCarryHistories_CatchAMovedTimer is the timer's negative test: in
// each generation the pause's timer (the own pause's in generation 0, the
// one re-armed for what remains of it in generation 1) renamed must be
// refused by the replayer, so each fixture pins it.
func TestPauseCarryHistories_CatchAMovedTimer(t *testing.T) {
	for _, f := range pauseContinuedFixtures {
		t.Run(filepath.Base(f.path), func(t *testing.T) {
			requireCommittedFixture(t, f.path)
			requireCommittedFixture(t, f.first)
			runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, f.first))
			recorded := loadHistoryFile(t, f.path)
			moved := false
			for _, event := range recorded.GetEvents() {
				// The pause's timer is the only one of at least a minute: a
				// bootstrap yield is a millisecond.
				if attrs := event.GetTimerStartedEventAttributes(); attrs != nil && attrs.GetStartToFireTimeout().AsDuration() >= time.Minute {
					attrs.TimerId = "moved-" + attrs.GetTimerId()
					moved = true
					break
				}
			}
			if !moved {
				t.Fatalf("%s started no pause timer", f.path)
			}
			if err := replayRunTimeoutHistory(t, recorded, runID); err == nil {
				t.Fatalf("%s replayed with its pause timer moved: it does not pin the timer", f.path)
			}
		})
	}
}
