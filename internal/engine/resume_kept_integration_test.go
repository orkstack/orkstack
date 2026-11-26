//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// What a resume keeps and what it runs again (E85 §3.4, §7.2): a retry from a
// chosen task, the pins, and a run of many tasks.

// TestIntegration_ResumeRetryFromHere: a retry from a task that succeeded
// runs it and what needs it again, each under the resume's own key, and keeps
// what comes before it.
func TestIntegration_ResumeRetryFromHere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/retry-from")
	s.startEngine()
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))
	runID := s.startFailing(resumeDocument(t, "resume-retry-from", resumeTask{id: "build"},
		resumeTask{id: "test", needs: []string{"build"}}, resumeTask{id: "beside", needs: []string{"build"}},
		resumeTask{id: "verify", needs: []string{"test"}}, resumeTask{id: "deploy", needs: []string{"verify", "beside"}}))
	door := s.serveResume(pastContainers)
	// The positive control: a plain resume keeps test.
	if kept := door.preview(runID).GetKeptCount(); kept != 4 {
		t.Fatalf("a plain resume keeps %d results, want build, test, beside and verify", kept)
	}
	preview, refused := door.ask(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true, FromTaskId: "test"})
	if refused != nil || preview.GetKeptCount() != 2 || preview.GetAction() != orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM {
		t.Fatalf("preview = %v, refused %v; want build and beside kept", preview, refused)
	}
	confirm := confirmOf(preview)
	confirm.FromTaskId = "test"
	resumed, refused := door.ask(confirm)
	if refused != nil || resumed.GetResume().GetFromTaskId() != "test" {
		t.Fatalf("confirm = %v, refused %v", resumed, refused)
	}
	scope := preview.GetSeqBase()
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, scope)
	for id, again := range map[string]int{"build": 0, "beside": 0, "test": 1, "verify": 1, "deploy": 1} {
		s.requireRow(run, id, orkstackv1.Status_STATUS_SUCCEEDED, "")
		if first, n := made.of(runID, id, 0), made.of(runID, id, scope); first != 1 || n != again {
			t.Errorf("%s ran %d times as submitted and %d under the resume, want 1 and %d", id, first, n, again)
		}
	}
	if row := taskRunOfRun(run, "test"); row.GetResultEpoch() != scope {
		t.Errorf("test's row names epoch %d, want %d", row.GetResultEpoch(), scope)
	}
}

// sharedOnce writes the attempt's key to the pin's shared directory, beside
// what its pin has written before, and fails if that is anything.
const sharedOnce = `[ ! -e /orkstack/shared/marker ] || exit 3; grep -o '"idempotencyKey": *"[^"]*"' /orkstack/input/task.json > /orkstack/shared/marker && ` + succeedCommand

// TestIntegration_ResumePinSameHostFreshDir: a member of a pin failed its
// run. The resume runs every member again, the one that had committed
// included, together on the one pin host, in a shared directory of its own
// that holds nothing of the first.
func TestIntegration_ResumePinSameHostFreshDir(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/pin-fresh")
	s.startEngine()
	host := s.startOrkerProcess("host", "ORK_ORKER_MAX_PINS=1")
	runID := s.startFailing(resumeDocument(t, "resume-pin-fresh",
		resumeTask{id: "fill", pin: "share", command: sharedOnce},
		// The marker it reads is the one its own run's fill wrote.
		resumeTask{id: "use", pin: "share", needs: []string{"fill"}, command: `grep -q '@r' /orkstack/shared/marker || exit 1; ` + succeedCommand}))
	s.requireRow(s.read(runID), "fill", orkstackv1.Status_STATUS_SUCCEEDED, "")
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	if preview.GetKeptCount() != 0 {
		t.Fatalf("the resume keeps %d results; a pin runs again whole", preview.GetKeptCount())
	}
	door.confirm(preview)
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, preview.GetSeqBase())
	for _, id := range []string{"fill", "use"} {
		if row := s.requireRow(run, id, orkstackv1.Status_STATUS_SUCCEEDED, ""); row.GetOrkerId() != host.id {
			t.Errorf("%s ran on %s, want %s", id, row.GetOrkerId(), host.id)
		}
		s.requireAttempts(runID, id, 2)
	}
}

// TestIntegration_ResumeAfterItsPinHostWasLost: a pin's only host loses the
// cluster while a member runs, so its run fails and the pin's release never
// reaches it. Back, it gives up that pin's slot by itself, and the resume
// opens the pin there anew, in a directory of its own: the first one is left,
// as it was, to its lease.
func TestIntegration_ResumeAfterItsPinHostWasLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/pin-lost")
	s.startEngine()
	host := s.startOrkerProcess("host", "ORK_ORKER_MAX_PINS=1")
	wf := resumeDocument(t, "resume-pin-lost",
		resumeTask{id: "fill", pin: "share", command: sharedOnce},
		resumeTask{id: "use", pin: "share", needs: []string{"fill"}, command: hangsUntilResumed})
	runID := s.recordedRunID()
	s.start(runID, wf, nil, s.resumePlan())
	s.eventually(3*time.Minute, "use to run on its pin's host", func() (bool, string) {
		return s.runsNow(runID, "use") && s.containers("ork.run="+runID) == 1, "it does not"
	})
	dirs := pinDirsFor(t, host.workRoot, runID)
	if len(dirs) != 1 {
		t.Fatalf("pin directories of the run on its host = %v, want one", dirs)
	}
	host.proxy.Cut()
	s.awaitFailed(runID)
	run := s.read(runID)
	if row := taskRunOfRun(run, "use"); run.GetError().GetCode() != "RUN_FAILED" || row.GetError().GetCode() != "PIN_HOST_LOST" {
		t.Fatalf("the run failed %v and use reads %v; want PIN_HOST_LOST", run.GetError(), row.GetError())
	}
	s.awaitContainers("ork.run="+runID, 0, 3*time.Minute)
	s.awaitNoOpenClaims(runID)
	host.proxy.Restore()
	// How long the slot is held once the host is reachable again: seen, not
	// bounded, because a loaded host makes its own delays.
	restored, held := time.Now(), []string{}
	s.eventually(2*time.Minute, "the host to be rid of the lost pin", func() (bool, string) {
		answer, err := s.starter.DescribeOrkerByID(ctx, host.id)
		if err == nil {
			held = append(held, fmt.Sprintf("%s: %d", time.Since(restored).Round(100*time.Millisecond), answer.GetOpenPins()))
		}
		return err == nil && answer.GetOpenPins() == 0, fmt.Sprintf("%d open, %v", answer.GetOpenPins(), err)
	})
	s.transcript.figure("open_pins_after_restore", held)
	t.Logf("EVIDENCE the lost pin's slot, by time since its host was reachable again: %v", held)

	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	if preview.GetKeptCount() != 0 {
		t.Fatalf("the resume keeps %d results; a pin runs again whole", preview.GetKeptCount())
	}
	door.confirm(preview)
	run = s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, preview.GetSeqBase())
	for _, id := range []string{"fill", "use"} {
		// sharedOnce succeeds only in a directory that holds nothing yet.
		if row := s.requireRow(run, id, orkstackv1.Status_STATUS_SUCCEEDED, ""); row.GetOrkerId() != host.id {
			t.Errorf("%s ran on %s, want %s", id, row.GetOrkerId(), host.id)
		}
	}
	if after := pinDirsFor(t, host.workRoot, runID); !slices.Contains(after, dirs[0]) {
		t.Fatalf("the host's directories are %v, want %s still left to its lease", after, dirs[0])
	}
}

// TestIntegration_ResumeKeptPinOpensNothing: a pin whose members all
// succeeded is kept, read back from its commits: no session is opened for it
// and no directory made.
func TestIntegration_ResumeKeptPinOpensNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/pin-kept")
	s.startEngine()
	host := s.startOrkerProcess("host", "ORK_ORKER_MAX_PINS=1")
	runID := s.startFailing(resumeDocument(t, "resume-pin-kept",
		resumeTask{id: "fill", pin: "share", command: sharedOnce},
		resumeTask{id: "use", pin: "share", needs: []string{"fill"}, command: succeedCommand},
		resumeTask{id: "deploy", needs: []string{"use"}, command: failUntilResumed}))
	base, _ := s.current(runID)
	// The positive control: the base's history opened the pin's session.
	if n := s.sessionsOpened(runID, base); n != 1 {
		t.Fatalf("the run opened %d sessions, want the pin's one", n)
	}
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	if preview.GetKeptCount() != 2 {
		t.Fatalf("the resume keeps %d results, want the pin's two", preview.GetKeptCount())
	}
	door.confirm(preview)
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, preview.GetSeqBase())
	execution, _ := s.current(runID)
	if n := s.sessionsOpened(runID, execution); n != 0 {
		t.Fatalf("the resumed run opened %d sessions, want none", n)
	}
	if dirs := pinDirsFor(t, host.workRoot, runID); len(dirs) != 0 {
		t.Fatalf("pin directories of the run = %v, want none", dirs)
	}
	s.requireRow(run, "use", orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.requireAttempts(runID, "fill", 1)
	s.requireAttempts(runID, "use", 1)
	s.requireAttempts(runID, "deploy", 2)
}

// sessionsOpened counts the sessions an execution asked for.
func (s *resumeStack) sessionsOpened(runID, execution string) int {
	s.t.Helper()
	n := 0
	for _, e := range s.history(runID, execution).GetEvents() {
		if a := e.GetActivityTaskScheduledEventAttributes(); a != nil &&
			a.GetActivityType().GetName() == "internalSessionCreationActivity" {
			n++
		}
	}
	return n
}

// TestIntegration_ResumeManyKeptTasks: 400 tasks are kept and one runs again.
// They are read back in one history, which hardly grows, and each is applied
// once. It stands for the largest run a resume takes, which the nightly tier
// is to prove once a run that has carried on can be resumed.
func TestIntegration_ResumeManyKeptTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/many")
	s.startEngine()
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))
	const kept = 400
	tasks := make([]resumeTask, 0, kept+1)
	last := resumeTask{id: "deploy"}
	for _, id := range taskKeys("w", kept) {
		tasks = append(tasks, resumeTask{id: id})
		last.needs = append(last.needs, id)
	}
	runID := s.startFailing(resumeDocument(t, "resume-many", append(tasks, last)...))
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	if preview.GetKeptCount() != kept {
		t.Fatalf("the resume keeps %d results, want %d", preview.GetKeptCount(), kept)
	}
	door.confirm(preview)
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, preview.GetSeqBase())
	results := s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT)[0].GetResultsKept()
	if results.GetKeptCount() != kept {
		t.Fatalf("RUN_RESULTS_KEPT names %d results, want %d", results.GetKeptCount(), kept)
	}
	scope := preview.GetSeqBase()
	for _, tk := range tasks {
		if row := s.requireRow(run, tk.id, orkstackv1.Status_STATUS_SUCCEEDED, ""); !row.GetAdopted() {
			t.Fatalf("%s's row does not say it was kept", tk.id)
		}
		if first, n := made.of(runID, tk.id, 0), made.of(runID, tk.id, scope); first != 1 || n != 0 {
			t.Fatalf("%s ran %d times as submitted and %d under the resume, want 1 and 0", tk.id, first, n)
		}
	}
	// The history is the base's up to its reset point, and what the resume added.
	execution, _ := s.current(runID)
	events := s.history(runID, execution).GetEvents()
	if events[0].GetWorkflowExecutionStartedEventAttributes().GetOriginalExecutionRunId() == execution {
		t.Fatal("the run's current execution is not a reset run")
	}
	if n := len(events); n >= 300 {
		t.Fatalf("the resumed run's history holds %d events for %d kept results and one task, want fewer than 300", n, kept)
	}
}
