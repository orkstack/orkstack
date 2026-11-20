//go:build integration

package engine_test

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
)

// AGENTS.md §11's stopping-Orker coverage (E92 §4.13), on a real cluster: real
// Temporal, real Docker containers and real Orkers built as cmd/orker builds
// them, in the cordon suite's deployment (cordon_integration_test.go).
//
// The heartbeat timeout is task.HeartbeatTimeout, 30 s, pinned into every
// activity the engine schedules and not a setting, so the waits below are the
// real ones: a task is lost no sooner than 30 s after its Orker's last beat.

// stoppingWorkYAML is one unpinned task that sleeps __HOLD_SECONDS__ and
// writes its values: the task a stopping Orker has running. Its grace is
// short so a cancelled attempt ends within seconds.
const stoppingWorkYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: __NAME__
version: 0.1.0
tasks:
  - id: work
    uses: __IMAGE__
    pool: __POOL__
    run_as_orker_user: true
    network: none
    timeout_seconds: 300
    cancellation_grace_period_seconds: 2
    command:
      - /bin/sh
      - -c
      - |
        sleep __HOLD_SECONDS__
        printf '{"ok":true}' > /orkstack/output/values.json
`

// stoppingOrker is one unpinned Orker of the deployment, its log kept,
// returned once the fleet lists it and it answers: submit's admission reads
// the same fleet, and refuses a pool whose only Orker's first poll has not yet
// reached the cluster.
func stoppingOrker(d *cordonDeployment, id, pool string) (*pinOrker, *syncBuffer) {
	logs := &syncBuffer{}
	o := d.orker(id, pool, pinOrkerConfig{hostsNoPins: true, logger: slog.New(slog.NewTextHandler(logs, nil))})
	d.awaitAnswering(id)
	return o, logs
}

// awaitRunningOn waits until o is running the one task of runID, in the phase
// its container runs in.
func awaitRunningOn(d *cordonDeployment, o *pinOrker, runID string) {
	d.t.Helper()
	d.awaitFleet(orkersWhere("orker_id", o.id), 3*time.Minute, o.id+" running run "+runID+"'s container",
		func(rows []*orkstackv1.Orker) bool {
			return len(rows) == 1 && rows[0].GetRunningTasks() == 1 && o.sawPhase(runID, "work", "run")
		})
}

// TestIntegration_KilledBeforeTheGraceEndsIsLostWhileStopping: an Orker that
// began to stop with a long grace, and whose platform then killed it before the
// grace ended, leaves a task the cluster times out. The task runs again on
// another Orker without spending an attempt of its own, and the run records the
// loss as one that happened while its Orker was stopping, naming when the
// stop began: a known shutdown, charged as a hand-back.
//
// The kill is the death of a process, not a drain: the Orker's connection to
// the cluster is closed, so it can heartbeat and report nothing more, and its
// worker is stopped after (pinOrker.kill); the cluster learns of it as it
// learns of any lost host, from the heartbeat that stops. What the stop did
// before it is real: BeginStop's t0 beat, sent synchronously to the cluster.
//
// The positive control is a stop that fits: the same task on another Orker, a
// grace that ends while it runs, hands the task back at t1. That is a clean
// hand-back, counted as one, with no loss and no stop instant recorded.
func TestIntegration_KilledBeforeTheGraceEndsIsLostWhileStopping(t *testing.T) {
	ctx := cordonContext(t, 15*time.Minute)
	token := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	poolKilled, poolFits := "lwskill-"+token, "lwsfit-"+token
	d := newCordonDeployment(t, ctx, "lostwhilestopping", poolKilled, poolFits)
	d.storeOrkflow("killed-"+d.token, poolKilled, stoppingWorkYAML, 25)
	d.storeOrkflow("fits-"+d.token, poolFits, stoppingWorkYAML, 25)

	t.Run("an Orker killed before its grace ends", func(t *testing.T) {
		a, logsA := stoppingOrker(d, "orker-lws-a-"+d.token, poolKilled)
		runID := d.submit("killed-" + d.token)
		awaitRunningOn(d, a, runID)

		before := time.Now()
		a.activities.BeginStop(orker.StopSequence{Grace: 10 * time.Minute})
		after := time.Now()
		d.transcript.line("A %s began to stop (grace 10m) and is killed", a.id)
		// The platform's kill, long before the grace ends.
		a.kill()
		b, _ := stoppingOrker(d, "orker-lws-b-"+d.token, poolKilled)
		d.transcript.line("B %s started", b.id)

		run := d.await(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("the run ended %v: %v; a task lost with its Orker must run again", run.GetStatus(), run.GetError())
		}
		work := pinTaskRun(t, run, "work")
		st := work.GetInterruptions()
		if st.GetLostWhileStopping() != 1 || st.GetHandedBack() != 1 || st.GetLost() != 0 || st.GetOwnAttemptsUsed() != 0 {
			t.Errorf("interruptions = %v, want one loss while stopping, charged as one hand-back: no plain loss, no own attempt spent", st)
		}
		last := st.GetLast()
		if last.GetReason() != "ORKER_STOPPED" || last.GetOrkerId() != a.id {
			t.Errorf("last interruption = %v, want ORKER_STOPPED of %s", last, a.id)
		}
		if since := last.GetOrkerStoppingSince(); since == nil || since.AsTime().Before(before.Add(-time.Second)) || since.AsTime().After(after.Add(time.Second)) {
			t.Errorf("the loss names the stop as beginning at %v, want a time between %v and %v: the t0 beat did not reach the cluster",
				since.AsTime(), before, after)
		}
		if work.GetOrkerId() != b.id || !b.sawTask(runID, "work") {
			t.Errorf("the task finished on %s, want it run again on %s", work.GetOrkerId(), b.id)
		}
		if strings.Contains(logsA.String(), "stopping beat was not acknowledged") {
			t.Errorf("A's t0 beat went unacknowledged:\n%s", logsA.String())
		}
		t.Logf("EVIDENCE lost while stopping: %v, ran again on %s", st, work.GetOrkerId())
	})

	t.Run("a stop that fits hands the task back with no loss", func(t *testing.T) {
		a, _ := stoppingOrker(d, "orker-lws-fa-"+d.token, poolFits)
		runID := d.submit("fits-" + d.token)
		awaitRunningOn(d, a, runID)
		b, _ := stoppingOrker(d, "orker-lws-fb-"+d.token, poolFits)
		d.transcript.line("fit: B %s started beside A %s, which holds the task", b.id, a.id)

		// A grace that ends while the task runs: t1 sweeps the attempt, and it
		// reports itself handed back.
		stop := a.activities.BeginStop(orker.StopSequence{Grace: 2 * time.Second, StopWorker: a.stop})
		outcome := stop.Finish()
		if outcome.Ended != orker.GraceEndedTimeout || outcome.HandedBack != 1 {
			t.Fatalf("the stop ended %v having handed back %d attempts, want its grace running out on the one running task", outcome.Ended, outcome.HandedBack)
		}

		run := d.await(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("the run ended %v: %v", run.GetStatus(), run.GetError())
		}
		work := pinTaskRun(t, run, "work")
		st := work.GetInterruptions()
		if st.GetHandedBack() != 1 || st.GetLostWhileStopping() != 0 || st.GetLost() != 0 || st.GetOwnAttemptsUsed() != 0 {
			t.Errorf("interruptions = %v, want one clean hand-back and nothing else", st)
		}
		if last := st.GetLast(); last.GetReason() != "ORKER_STOPPED" || last.GetOrkerStoppingSince() != nil {
			t.Errorf("last interruption = %v, want ORKER_STOPPED with no stop instant (a loss is what records one)", last)
		}
		if work.GetOrkerId() != b.id {
			t.Errorf("the task finished on %s, want %s", work.GetOrkerId(), b.id)
		}
	})
}

// TestIntegration_StoppingOrkerHeartbeatsAcrossALongGrace: a stopping Orker is
// cordoned as an operator's cordon is, which holds without limit: its worker
// and its periodic beats run on until the grace ends. A grace of four heartbeat
// timeouts therefore passes with its task alive: not lost, not run again, one
// attempt, finished on the Orker that is stopping, inside the grace.
//
// The positive controls are two. The beats are seen by the cluster across the
// window: the attempt's last heartbeat, as `InspectRun` reads it, passes
// a point more than one and a half timeouts after the stop began, which an
// Orker that stopped beating could not make while its attempt stayed alive. And
// a second Orker of the pool takes a new task in the same window, so the
// stopping Orker's taking none is a fact about the stop and not about a pool
// with nobody polling.
func TestIntegration_StoppingOrkerHeartbeatsAcrossALongGrace(t *testing.T) {
	ctx := cordonContext(t, 10*time.Minute)
	pool := fmt.Sprintf("stopbeat-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "stopbeat", pool)
	const hold = 80 // more than two heartbeat timeouts, inside the grace below
	grace := 4 * task.HeartbeatTimeout
	d.storeOrkflow("long-"+d.token, pool, stoppingWorkYAML, hold)
	d.storeOrkflow("quick-"+d.token, pool, cordonEchoYAML, 0)

	a, _ := stoppingOrker(d, "orker-sb-a-"+d.token, pool)
	long := d.submit("long-" + d.token)
	awaitRunningOn(d, a, long)
	b, _ := stoppingOrker(d, "orker-sb-b-"+d.token, pool)

	stopped := time.Now()
	stop := a.activities.BeginStop(orker.StopSequence{Grace: grace})
	d.transcript.line("A %s began to stop at %s with a grace of %s; its task holds %ds", a.id, stopped.Format(time.RFC3339), grace, hold)
	d.awaitFleet(orkersWhere("orker_id", a.id), time.Minute, "A reading stopping",
		func(rows []*orkstackv1.Orker) bool { return len(rows) == 1 && rows[0].GetStopping() != nil })

	// Positive control: the pool takes and finishes new work in the window.
	// Which Orker runs it is not asserted: a stopping Orker asks for no new
	// work, but a poll it had open when its stop began may still deliver a
	// task, which it then runs in flight (AGENTS.md §3.3).
	quick := d.submit("quick-" + d.token)
	second := d.await(quick)
	if second.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("a new task submitted while A stops ended %v, want SUCCEEDED", second.GetStatus())
	}
	d.transcript.line("a new task submitted while A stops succeeded on %s", pinTaskRun(t, second, "echo").GetOrkerId())

	// Positive control: its beats are heard across the window, by the same
	// attempt, while it runs. A beat at the t0 instant alone reaches
	// stopped+30s; passing stopped+45s takes beats sent well after it.
	heard := stopped.Add(task.HeartbeatTimeout + task.HeartbeatTimeout/2)
	deadline := time.Now().Add(3 * task.HeartbeatTimeout)
	for {
		resp, err := d.runs.InspectRun(ctx, connect.NewRequest(&orkstackv1.InspectRunRequest{RunId: long, Namespace: d.orkNS}))
		if err != nil {
			t.Fatalf("InspectRun(%s): %v", long, err)
		}
		attempts := resp.Msg.GetAttempts()
		if len(attempts) != 1 || attempts[0].GetAttempt() != 1 || (attempts[0].GetOrkerId() != "" && attempts[0].GetOrkerId() != a.id) {
			t.Fatalf("InspectRun lists %v while the task runs on A, want its one first attempt", attempts)
		}
		if beat := attempts[0].GetLastHeartbeatAt(); beat != nil && beat.AsTime().After(heard) {
			d.transcript.line("last heartbeat %s, %s after the stop began", beat.AsTime().Format(time.RFC3339),
				beat.AsTime().Sub(stopped).Round(time.Second))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no heartbeat after %v within %s of the stop (the attempt's last: %v): the stopping Orker stopped beating",
				heard, 3*task.HeartbeatTimeout, attempts[0].GetLastHeartbeatAt())
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}

	run := d.await(long)
	finished := time.Since(stopped)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the task ended %v: %v; a stopping Orker's task must finish within the grace", run.GetStatus(), run.GetError())
	}
	work := pinTaskRun(t, run, "work")
	if work.GetOrkerId() != a.id || work.GetAttempt() != 1 {
		t.Errorf("the task finished on %s as attempt %d, want attempt 1 on the stopping Orker %s", work.GetOrkerId(), work.GetAttempt(), a.id)
	}
	if st := work.GetInterruptions(); st.GetLast() != nil || st.GetLost() != 0 || st.GetHandedBack() != 0 || st.GetOwnAttemptsUsed() != 0 {
		t.Errorf("interruptions = %v, want none: the task was never lost", st)
	}
	if got := a.ranFor(long); len(got) != 1 || len(b.ranFor(long)) != 0 {
		t.Errorf("the task was handed to A as %v and to B as %v, want A once and B never", got, b.ranFor(long))
	}
	if finished <= 2*task.HeartbeatTimeout || finished >= grace {
		t.Errorf("the task finished %s after the stop began, want it past two heartbeat timeouts (%s) and within the grace (%s)",
			finished.Round(time.Second), 2*task.HeartbeatTimeout, grace)
	}
	// Positive control: the stop itself proceeded. With its one task done, A
	// has nothing in flight, so its grace ends at once as idle, long before
	// it would run out, and it hands nothing back: a stop that had stalled
	// would wait out the grace instead.
	done := make(chan orker.StopOutcome, 1)
	go func() { done <- stop.Finish() }()
	select {
	case outcome := <-done:
		if outcome.Ended != orker.GraceEndedIdle || outcome.HandedBack != 0 {
			t.Errorf("A's stop ended %v having handed back %d, want its grace ended idle with nothing handed back", outcome.Ended, outcome.HandedBack)
		}
		d.transcript.line("A's stop ended %v, %s after it began", outcome.Ended, time.Since(stopped).Round(time.Second))
	case <-time.After(task.HeartbeatTimeout):
		t.Fatalf("A's stop did not end within %s of its last task finishing: the stop stalled", task.HeartbeatTimeout)
	}
	t.Logf("EVIDENCE heartbeats across a long grace: attempt 1 on %s finished %s into a %s grace", a.id, finished.Round(time.Second), grace)
}
