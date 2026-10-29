//go:build integration

package engine_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// A pool whose every Orker is busy is a live pool.
//
// A busy Orker reserves no activity slot, so it polls nothing on its pool's
// channel, and once its last poll is older than the freshness window the
// cluster's poller records for that channel say nobody serves the pool. Submit
// used to read exactly that: a pool doing all the work it can was refused as
// empty, and the capability probes it asked on the same channel waited behind
// the busy slots and were skipped with a log line. An Orker's presence and
// control channels have workers of their own, so the inventory still sees and
// asks every busy Orker, and that is what submit reads now.

// busyHoldSeconds outlasts the freshness window with room to spare: the pool
// channel's last poll may have started right as the hold was dispatched.
const busyHoldSeconds = 300

// busyHoldYAML is two independent tasks that each hold one slot.
const busyHoldYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: __NAME__
version: 0.1.0
tasks:
  - id: hold-a
    uses: __IMAGE__
    pool: __POOL__
    run_as_orker_user: true
    network: none
    timeout_seconds: 600
    command: ["/bin/sh", "-c", "sleep __HOLD_SECONDS__ && printf '{}' > /orkstack/output/values.json"]
  - id: hold-b
    uses: __IMAGE__
    pool: __POOL__
    run_as_orker_user: true
    network: none
    timeout_seconds: 600
    command: ["/bin/sh", "-c", "sleep __HOLD_SECONDS__ && printf '{}' > /orkstack/output/values.json"]
`

func TestIntegration_ASaturatedPoolIsLiveAtSubmitAndAnswersItsProbes(t *testing.T) {
	ctx := cordonContext(t, 12*time.Minute)
	pool := fmt.Sprintf("busy-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "busy", pool)
	d.transcript.section("two Orkers of one pool, one slot each, neither hosts pins")
	a := d.orker("orker-busy-a-"+d.token, pool, pinOrkerConfig{maxConcurrent: 1, hostsNoPins: true})
	d.orker("orker-busy-b-"+d.token, pool, pinOrkerConfig{maxConcurrent: 1, hostsNoPins: true})
	probe := engine.NewTemporalPoolProbe(d.temporal)
	awaitPoolServedBy(t, ctx, probe, pool, a.queue, 2)
	d.awaitFleet(orkersWhere("pool", pool), time.Minute, "both Orkers listed",
		func(rows []*orkstackv1.Orker) bool { return len(rows) == 2 })

	d.storeOrkflow("hold-"+d.token, pool, busyHoldYAML, busyHoldSeconds)
	d.storeOrkflow("echo-"+d.token, pool, cordonEchoYAML, 0)
	d.storeOrkflow("pinned-"+d.token, pool, cordonQuickPinYAML, 0)

	holdID := d.submit("hold-" + d.token)
	d.awaitFleet(orkersWhere("pool", pool), 2*time.Minute, "both Orkers running a hold",
		func(rows []*orkstackv1.Orker) bool {
			return len(rows) == 2 && rows[0].GetRunningTasks() == 1 && rows[1].GetRunningTasks() == 1
		})
	saturated := time.Now()
	d.transcript.line("run %s holds both slots from %s", holdID, saturated.UTC().Format(time.RFC3339))

	// The reproduction's precondition: the pool's own channel reads empty.
	// Without it a submit would be accepted on the poller records alone, and
	// this test would prove nothing about a busy pool.
	var live engine.PoolLiveness
	for {
		var err error
		live, err = probe.ObservePool(ctx, a.queue)
		if err != nil {
			t.Fatalf("ObservePool: %v", err)
		}
		if !live.Fresh(time.Now(), engine.DefaultPollerFreshness) {
			break
		}
		if time.Since(saturated) > 3*time.Minute {
			t.Fatalf("the pool channel still reads live %s after both slots were taken: %+v",
				time.Since(saturated).Round(time.Second), live)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for the pool channel to age out: %v", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	d.transcript.line("after %s the pool channel reads %+v: no poller inside %s",
		time.Since(saturated).Round(time.Second), live, engine.DefaultPollerFreshness)

	// Positive control, inside the same window: both Orkers are still there
	// and still running their holds, and the hold run has lost no task.
	rows := d.list(orkersWhere("pool", pool))
	d.transcript.line("ListOrkers %s", describeRows(rows))
	if len(rows) != 2 {
		t.Fatalf("the fleet lists %d Orkers of %s while the pool channel reads empty; want both: %s",
			len(rows), pool, describeRows(rows))
	}
	for _, row := range rows {
		if row.GetUnavailableReason() != "" || row.GetRunningTasks() != 1 {
			t.Fatalf("Orker %s is not answering with its hold running: %s", row.GetOrkerId(), describeRows(rows))
		}
	}
	hold := d.getRun(holdID)
	for _, id := range []string{"hold-a", "hold-b"} {
		if tr := pinTaskRun(t, hold, id); tr.GetStatus() != orkstackv1.Status_STATUS_RUNNING || tr.GetError() != nil {
			t.Fatalf("hold task %s is %v (%v) while its Orker holds the slot; want RUNNING",
				id, tr.GetStatus(), tr.GetError())
		}
	}

	// 1. A plain submit is accepted: the pool is busy, not empty.
	echoID := d.submit("echo-" + d.token)
	d.transcript.line("plain run %s accepted onto the busy pool", echoID)

	// 2. A pinned submit has its probe answered on the Orkers' own channels,
	// so the pool's missing pin host is refused at submit, before a run id is
	// minted.
	// The count is taken once the plain run is listed: run listing is
	// eventually consistent, and a plain run listed late would read as one
	// the refusal left behind.
	reader := engine.NewTemporalRunReader(d.temporal)
	before := d.awaitListed(reader, echoID)
	_, refused := d.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "pinned-" + d.token, Namespace: d.orkNS}))
	d.transcript.line("pinned submit: %v", refused)
	if connect.CodeOf(refused) != connect.CodeFailedPrecondition ||
		!strings.Contains(refused.Error(), "none of them hosts pins") {
		t.Fatalf("pinned submit onto a busy pool that hosts no pins = %v; want FailedPrecondition "+
			"naming the missing pin host", refused)
	}
	after, err := reader.ListRuns(ctx, engine.RunQuery{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	for _, run := range after {
		if !before[run.GetId()] {
			t.Fatalf("the refused pinned submit left run %s behind", run.GetId())
		}
	}

	// 3. Nothing was skipped quietly: no probe went unanswered.
	if log := d.apiLog.String(); strings.Contains(log, "is unknown") {
		t.Fatalf("submit logged an unanswered probe on a busy pool:\n%s", log)
	}

	// The accepted run was never stranded: once the holds end, it runs.
	if err := d.starter.CancelRun(ctx, holdID, ""); err != nil {
		t.Fatalf("cancel the hold run: %v", err)
	}
	if got := d.await(holdID).GetStatus(); got != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("hold run ended %v; want CANCELED", got)
	}
	echo := d.await(echoID)
	d.transcript.line("plain run %s ended %v once a slot was free", echoID, echo.GetStatus())
	if echo.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the plain run accepted onto the busy pool ended %v (%v); want SUCCEEDED",
			echo.GetStatus(), echo.GetError())
	}
	t.Logf("EVIDENCE busy pool: pool channel %+v after %s, plain submit accepted and ran, pinned submit refused: %v",
		live, time.Since(saturated).Round(time.Second), refused)
}

// awaitListed lists the deployment's runs until runID is among them, and
// returns the listed ids.
func (d *cordonDeployment) awaitListed(reader *engine.TemporalRunReader, runID string) map[string]bool {
	d.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		runs, err := reader.ListRuns(d.ctx, engine.RunQuery{})
		if err != nil {
			d.t.Fatalf("ListRuns: %v", err)
		}
		listed := map[string]bool{}
		for _, run := range runs {
			listed[run.GetId()] = true
		}
		if listed[runID] {
			return listed
		}
		if time.Now().After(deadline) {
			d.t.Fatalf("run %s was never listed", runID)
		}
		select {
		case <-d.ctx.Done():
			d.t.Fatalf("context ended waiting for run %s to be listed: %v", runID, d.ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (d *cordonDeployment) getRun(runID string) *orkstackv1.Run {
	d.t.Helper()
	resp, err := d.runs.GetRun(d.ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: d.orkNS}))
	if err != nil {
		d.t.Fatalf("GetRun(%s): %v", runID, err)
	}
	return resp.Msg.GetRun()
}
