//go:build integration

package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A resume never runs a task beside an attempt of it that may still run
// (E85 §23.4, §26): what the cluster and the Orkers say of the run's
// containers, attempts and pins, against real Orkers and real containers.

// ignoresTERM is a container that outlives its SIGTERM until it is killed.
const ignoresTERM = `trap '' TERM; echo running; while :; do sleep 1; done`

// hangsUntilResumed runs until it is killed in the run as submitted, and
// succeeds when a resume runs it again.
const hangsUntilResumed = `grep -q '"idempotencyKey": *"[^"]*@r' /orkstack/input/task.json && { ` + succeedCommand + `; exit 0; }; ` + ignoresTERM

// lostAttempt is when the base's attempt of the run was closed by its
// heartbeat timeout, and that timeout.
func lostAttempt(t *testing.T, h *historypb.History) (time.Time, time.Duration) {
	t.Helper()
	heartbeats := map[int64]time.Duration{}
	for _, e := range h.GetEvents() {
		if a := e.GetActivityTaskScheduledEventAttributes(); a != nil {
			heartbeats[e.GetEventId()] = a.GetHeartbeatTimeout().AsDuration()
		}
		if a := e.GetActivityTaskTimedOutEventAttributes(); a != nil && heartbeats[a.GetScheduledEventId()] > 0 {
			return e.GetEventTime().AsTime(), heartbeats[a.GetScheduledEventId()]
		}
	}
	t.Fatal("the base's history holds no attempt its heartbeat timeout ended")
	return time.Time{}, 0
}

// TestIntegration_ResumeWaitsForItsContainers: an Orker loses the cluster
// under a container that ignores SIGTERM, and its run fails. The resume is
// refused until the instant the container must be gone by; past it, while
// the Orker, back, still has the container, it is refused naming the
// attempt; once the container is gone it goes on, and the task runs once
// more. The service's clock is moved to the bound: a live Orker kills its
// container before it, so a container alive past it cannot be made on demand.
func TestIntegration_ResumeWaitsForItsContainers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/containers")
	s.startEngine()
	orker := s.startOrkerProcess("a")
	// Long enough that the container is still there when a slow host gets to
	// look at it; only the moved clock waits it out, and the Orker ends it at
	// its grace.
	const grace = 420
	wf := resumeDocument(t, "resume-containers",
		resumeTask{id: "build", command: succeedCommand},
		resumeTask{id: "work", needs: []string{"build"}, grace: grace,
			command: hangsUntilResumed})
	runID := s.recordedRunID()
	s.start(runID, wf, nil, s.resumePlan())
	label := "ork.run=" + runID
	s.eventually(2*time.Minute, "work to run", func() (bool, string) {
		return s.runsNow(runID, "work") && s.containers(label) == 1, "it does not"
	})
	orker.proxy.Cut()
	s.awaitFailed(runID)
	if n := s.containers(label); n != 1 {
		t.Fatalf("%d containers of the run are running when it has failed, want the one that ignores SIGTERM", n)
	}
	orker.proxy.Restore()

	execution, _ := s.current(runID)
	ended, heartbeat := lostAttempt(t, s.history(runID, execution))
	bound := ended.Add(heartbeat + grace*time.Second + 30*time.Second)
	door := s.serveResume(0)
	// A confirm is held to everything a preview is; it names the ending it
	// saw, and needs no preview.
	unpreviewed := &orkstackv1.ResumeRunRequest{RunId: runID,
		ExpectedTerminalSequence: s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)[0].GetSequence()}
	asked := time.Until(bound)
	r := door.refusedPreview(runID, "STILL_STOPPING", "containers_stopping")
	// Asked at most as long before the answer as the answer took.
	if got := time.Duration(r.GetRetryAfterSeconds()) * time.Second; got < time.Until(bound)-time.Second || got > asked+time.Second {
		t.Fatalf("retry after %s; want between %s and %s, until %s", got, time.Until(bound), asked, bound)
	}
	door.clock.moveTo(bound.Add(-30 * time.Second))
	door.refusedPreview(runID, "STILL_STOPPING", "containers_stopping")
	door.requireRefused(unpreviewed, "STILL_STOPPING", "containers_stopping")

	door.clock.moveTo(bound.Add(time.Second))
	s.eventually(time.Minute, "the Orker to answer again", func() (bool, string) {
		answer, err := s.starter.DescribeOrkerByID(ctx, orker.id)
		return err == nil && len(answer.GetRunning()) == 1, "it does not answer, or names no attempt"
	})
	r = door.refusedPreview(runID, "STILL_STOPPING", "container_still_running")
	if len(r.GetTasks()) != 1 || r.GetTasks()[0] != "work" || !strings.Contains(r.GetMessage(), orker.id) {
		t.Fatalf("refusal = %v; want it to name work on %s", r, orker.id)
	}
	if n := door.anomalyCount("container_after_bound"); n != 1 {
		t.Fatalf("container_after_bound was counted %d times, want once", n)
	}
	door.requireRefused(unpreviewed, "STILL_STOPPING", "container_still_running")
	if n := s.containers(label); n != 1 {
		t.Fatalf("%d containers running while the resume was refused for one, want 1", n)
	}

	s.awaitContainers(label, 0, 12*time.Minute)
	s.awaitNoOpenClaims(runID)
	preview := door.preview(runID)
	if w := unknownAttempts(preview); len(w) != 0 {
		t.Fatalf("the Orker released its attempt, and the preview warns %v", w)
	}
	resumed := door.confirm(preview)
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, resumed.GetSeqBase())
	if !s.requireRow(run, "build", orkstackv1.Status_STATUS_SUCCEEDED, "").GetAdopted() {
		t.Fatal("build was kept, and its row does not say so")
	}
	s.requireRow(run, "work", orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.requireAttempts(runID, "build", 1)
	s.requireAttempts(runID, "work", 2)
}

// killedUnder starts a run of wf on Orker a and kills a's process while
// taskID's container runs, leaving that container to the daemon.
func (s *resumeStack) killedUnder(a *orkerProcess, wf *orkstackv1.Orkflow, taskID string) string {
	s.t.Helper()
	runID := s.recordedRunID()
	s.start(runID, wf, nil, s.resumePlan())
	s.eventually(2*time.Minute, taskID+" to run on "+a.id, func() (bool, string) {
		return s.runsNow(runID, taskID) && s.containers("ork.orker="+a.id) == 1, "it does not"
	})
	s.kill(a)
	return runID
}

// TestIntegration_ResumeOrkerDiedAfterSigterm: an Orker is killed under a
// container that ignores SIGTERM, and nothing on the host stops it. Past the
// containers' bound the resume shows the attempt as one whose outcome is
// unknown and is refused until exactly that attempt is accepted; accepted, it
// runs the task again while the old container is still running. The
// service's clock is moved past the bound.
func TestIntegration_ResumeOrkerDiedAfterSigterm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/orker-died")
	s.startEngine()
	a := s.startOrkerProcess("a")
	runID := s.killedUnder(a, resumeDocument(t, "resume-orker-died", resumeTask{id: "work", command: hangsUntilResumed}), "work")
	s.awaitFailed(runID)
	b := s.startOrkerProcess("b")

	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	warned := unknownAttempts(preview)
	if len(warned) != 1 || warned[0].GetTaskId() != "work" || warned[0].GetOrkerId() != a.id || warned[0].GetOrdinal() != 1 || warned[0].GetAcknowledgeKey() == "" {
		t.Fatalf("warnings = %v; want attempt 1 of work on %s", preview.GetWarnings(), a.id)
	}
	key := warned[0].GetAcknowledgeKey()
	for name, accepted := range map[string][]string{"nothing accepted": nil, "another attempt accepted": {key + "0"}, "another Orker's": {b.id + strings.TrimPrefix(key, a.id)}} {
		r := door.requireRefused(confirmOf(preview, accepted...), "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED", "")
		if len(r.GetTasks()) != 1 || r.GetTasks()[0] != "work" {
			t.Fatalf("%s: refusal = %v; want it to name work", name, r)
		}
		door.clock.move(time.Minute) // past the confirms a run takes a minute
	}
	undigested := confirmOf(preview, key)
	undigested.ExpectedPlanDigest = ""
	if _, refused, err := door.try(ctx, undigested); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("an attempt accepted for no plan = %v, refused %v; want InvalidArgument", err, refused)
	}
	if n := s.containers("ork.orker=" + a.id); n != 1 {
		t.Fatalf("%d containers of the killed Orker, want its one still running", n)
	}

	resumed := door.confirm(preview, key)
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, resumed.GetSeqBase())
	if row := s.requireRow(run, "work", orkstackv1.Status_STATUS_SUCCEEDED, ""); row.GetOrkerId() != b.id {
		t.Fatalf("work ran again on %s, want %s", row.GetOrkerId(), b.id)
	}
	if n := s.containers("ork.orker=" + a.id); n != 1 {
		t.Fatalf("%d containers of the killed Orker after the resume, want its one still running: it was accepted, not stopped", n)
	}
}

// TestIntegration_ResumeAfterADeadOrkerBootedAgain: the Orker that died under
// a container boots again under its identity. Its boot removes the container
// and releases the attempt, so the resume warns of nothing. The service's
// clock is moved past the containers' bound.
func TestIntegration_ResumeAfterADeadOrkerBootedAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/orker-rebooted")
	s.startEngine()
	a := s.startOrkerProcess("a")
	runID := s.killedUnder(a, resumeDocument(t, "resume-orker-rebooted", resumeTask{id: "work", command: hangsUntilResumed}), "work")
	s.awaitFailed(runID)
	door := s.serveResume(pastContainers)
	// The positive control: before it boots, the attempt is unknown.
	if w := unknownAttempts(door.preview(runID)); len(w) != 1 {
		t.Fatalf("warnings before the boot = %v, want the one attempt", w)
	}

	s.restartOrkerProcess(a)
	s.awaitContainers("ork.orker="+a.id, 0, time.Minute)
	s.awaitNoOpenClaims(runID)
	preview := door.preview(runID)
	if w := unknownAttempts(preview); len(w) != 0 {
		t.Fatalf("warnings after the boot = %v, want none", w)
	}
	resumed := door.confirm(preview)
	s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, resumed.GetSeqBase())
}

// TestIntegration_ResumeWarnsOfAKeptTasksLostAttempt: an attempt an Orker
// died under was retried to success on another Orker, and a later task failed
// the run. The lost attempt's container may still run, so the resume shows
// it although its task is kept; accepted, the task stays kept and only the
// failed one runs again. The service's clock is moved past the bound.
func TestIntegration_ResumeWarnsOfAKeptTasksLostAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/kept-lost")
	s.startEngine()
	a := s.startOrkerProcess("a")
	wf := resumeDocument(t, "resume-kept-lost",
		// Its first attempt hangs; the next succeeds.
		resumeTask{id: "build", attempts: 2, command: `grep -qE '"attempt": *1(,|$)' /orkstack/input/task.json || { ` + succeedCommand + `; exit 0; }; ` + ignoresTERM},
		resumeTask{id: "deploy", needs: []string{"build"}, command: failUntilResumed})
	runID := s.killedUnder(a, wf, "build")
	b := s.startOrkerProcess("b")
	s.awaitFailed(runID)
	s.requireRow(s.read(runID), "build", orkstackv1.Status_STATUS_SUCCEEDED, "")

	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	warned := unknownAttempts(preview)
	if len(warned) != 1 || warned[0].GetTaskId() != "build" || warned[0].GetOrkerId() != a.id || preview.GetKeptCount() != 1 {
		t.Fatalf("kept %d, warnings %v; want build kept and its attempt on %s unknown", preview.GetKeptCount(), preview.GetWarnings(), a.id)
	}
	door.requireRefused(confirmOf(preview), "UNKNOWN_ATTEMPTS_NOT_ACKNOWLEDGED", "")
	resumed := door.confirm(preview, warned[0].GetAcknowledgeKey())
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, resumed.GetSeqBase())
	if row := s.requireRow(run, "build", orkstackv1.Status_STATUS_SUCCEEDED, ""); !row.GetAdopted() || row.GetOrkerId() != b.id {
		t.Fatalf("build reads adopted %t on %s; want it kept as %s committed it", row.GetAdopted(), row.GetOrkerId(), b.id)
	}
	s.requireAttempts(runID, "build", 2)
	s.requireAttempts(runID, "deploy", 2)
}
