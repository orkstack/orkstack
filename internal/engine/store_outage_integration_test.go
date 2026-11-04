//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The store-outage cordon's acceptance, against real Temporal, real object
// storage and real containers (docs/DEPLOY.md "When object storage goes away").
// The Orker under test reaches the store through a fault proxy and runs the
// shipped watch with the shipped check; the engine, and a control Orker on
// another pool, reach the store directly, so cutting the proxy is this one
// Orker losing the store while the rest of the system visibly keeps working.

// storeOutagePollDrain outlasts a poll already outstanding when the cordon
// lands: the cluster answers an idle long poll within about a minute, and a
// task can land on a cordoned Orker only through one of those (docs/DEPLOY.md,
// "What a cordon does").
const storeOutagePollDrain = 75 * time.Second

// storeOutageWatchConfig is the shipped watch at a test's cadence.
func storeOutageWatchConfig() orker.StoreWatchConfig {
	return orker.StoreWatchConfig{
		Enabled: true, Interval: time.Second, Timeout: time.Second,
		FailureThreshold: 3, RecoveryThreshold: 2,
	}
}

// storeCheckCounts is how the shipped check answered the running watch.
type storeCheckCounts struct{ passed, failed atomic.Int64 }

// storeOutageOrker starts one Orker whose every store goes through a proxy,
// running its store watch exactly as cmd/orker does, and counts its checks.
func storeOutageOrker(d *cordonDeployment, id, pool string) (*pinOrker, *faultproxy.Proxy, *storeCheckCounts) {
	t := d.t
	t.Helper()
	endpoint, err := url.Parse(d.cfg.S3Endpoint)
	if err != nil || endpoint.Host == "" {
		t.Fatalf("the store endpoint %q cannot be proxied: %v", d.cfg.S3Endpoint, err)
	}
	proxy := faultproxy.New(t, endpoint.Host)
	cfg := d.cfg
	cfg.S3Endpoint = "http://" + proxy.Addr()

	store, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open the Orker's artifact store: %v", err)
	}
	commits, err := artifacts.NewCommitStore(d.ctx, cfg)
	if err != nil {
		t.Fatalf("open the Orker's commit store: %v", err)
	}
	logs, err := artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("open the Orker's log store: %v", err)
	}
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict content-addressed references", store)
	}
	if err := artifacts.ProbeCommitStore(d.ctx, commits); err != nil {
		t.Fatalf("the boot probe through the proxy: %v", err)
	}
	o := newPinOrker(t, d.ctx, pinOrkerConfig{
		id: id, pool: pool, queue: mustQueueForPool(t, pool),
		store: store, commits: commits, logs: logs, refs: refs,
		temporalNamespace: d.namespace,
	})
	watchCtx, stop := context.WithCancel(d.ctx)
	t.Cleanup(stop)
	checks := &storeCheckCounts{}
	check := func(ctx context.Context) error {
		err := artifacts.CheckCommitStore(ctx, commits)
		if err != nil {
			checks.failed.Add(1)
		} else {
			checks.passed.Add(1)
		}
		return err
	}
	go o.activities.NewStoreWatch(check, storeOutageWatchConfig(), nil).Run(watchCtx)
	return o, proxy, checks
}

func (d *cordonDeployment) awaitStoreOutage(id string, within time.Duration, want bool) *orkstackv1.Orker {
	d.t.Helper()
	rows := d.awaitFleet(orkersWhere("orker_id", id), within, fmt.Sprintf("%s store outage == %v", id, want),
		func(rows []*orkstackv1.Orker) bool {
			return len(rows) == 1 && rows[0].GetUnavailableReason() == "" && (rows[0].GetStoreOutage() != nil) == want
		})
	return rows[0]
}

func (d *cordonDeployment) submitAllowingAnEmptyPool(orkflow string) string {
	d.t.Helper()
	resp, err := d.runs.SubmitRun(d.ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: orkflow, Namespace: d.orkNS, AllowEmptyPool: true}))
	if err != nil {
		d.t.Fatalf("SubmitRun(%s): %v", orkflow, err)
	}
	return resp.Msg.GetRun().GetId()
}

// awaitStoreCheck waits until the watch's check has answered as the step needs.
func awaitStoreCheck(t *testing.T, ctx context.Context, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("%s never happened within 30s", what)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for %s: %v", what, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// With the store cut, a task waits: the Orker is handed nothing, no attempt is
// used, and other work keeps finishing. A blip cordons nothing. When the store
// returns the Orker resumes with no operator step and the run completes on its
// first attempt. Once every Orker of the pool holds this cordon a new submit is
// refused, which is what docs/DEPLOY.md says it does.
func TestIntegration_StoreOutage_TasksWaitWithNoAttemptUsedAndCompleteWhenTheStoreReturns(t *testing.T) {
	ctx := cordonContext(t, 12*time.Minute)
	pool := fmt.Sprintf("outage-%d", time.Now().UnixNano()%1_000_000)
	controlPool := pool + "-control"
	d := newCordonDeployment(t, ctx, "outage", pool, controlPool)
	o, proxy, checks := storeOutageOrker(d, "orker-out-"+d.token, pool)
	control := d.orker("orker-ctl-"+d.token, controlPool, pinOrkerConfig{})
	d.storeOrkflow("echo-"+d.token, pool, cordonEchoYAML, 0)
	d.storeOrkflow("ctl-"+d.token, controlPool, cordonEchoYAML, 0)

	// A blip shorter than the failure threshold cordons nothing, and a good check
	// between blips starts the count again: three blips of one failed check would
	// cordon the Orker if it did not. Each step waits for the watch's own check to
	// have answered, so no window passes with no check inside it.
	d.transcript.section("three blips")
	for blip := 1; blip <= 3; blip++ {
		failed := checks.failed.Load()
		proxy.Cut()
		awaitStoreCheck(t, ctx, "a check fails", func() bool { return checks.failed.Load() > failed })
		passed := checks.passed.Load()
		proxy.Restore()
		awaitStoreCheck(t, ctx, "a check passes", func() bool { return checks.passed.Load() > passed })
		if row := d.one(o.id); row.GetStoreOutage() != nil || row.GetCordoned() {
			t.Fatalf("blip %d cordoned the Orker: %v", blip, row)
		}
	}
	blipRun := d.await(d.submit("echo-" + d.token))
	if blipRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("a run after the blips ended %v: %v", blipRun.GetStatus(), blipRun.GetError())
	}

	d.transcript.section("the store is cut")
	proxy.Cut()
	cutAt := time.Now()
	row := d.awaitStoreOutage(o.id, time.Minute, true)
	d.transcript.line("%s cordoned itself after %s: %v", o.id, time.Since(cutAt).Round(time.Second), row.GetStoreOutage())
	if !row.GetCordoned() || row.GetCordonReason() != "" || row.GetCordonedAt() != nil {
		t.Fatalf("an outage alone reads as an operator's cordon: %v", row)
	}
	if !strings.HasPrefix(row.GetStoreOutage().GetReason(), "object storage unreachable") || row.GetStoreOutage().GetSince() == nil {
		t.Fatalf("store outage = %v", row.GetStoreOutage())
	}

	// A poll already out when the cordon landed can still be handed a task, so
	// let those end; the control Orker working throughout is the positive
	// control that the system was making progress in this window.
	controlRuns := 0
	for time.Since(cutAt) < storeOutagePollDrain {
		ctlRun := d.await(d.submit("ctl-" + d.token))
		if ctlRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("the control run ended %v: %v", ctlRun.GetStatus(), ctlRun.GetError())
		}
		controlRuns++
		time.Sleep(2 * time.Second)
	}

	runID := d.submitAllowingAnEmptyPool("echo-" + d.token)
	d.transcript.line("run %s submitted into the outage", runID)
	windowEnd := time.Now().Add(15 * time.Second)
	progressed := 0
	for time.Now().Before(windowEnd) {
		ctlRun := d.await(d.submit("ctl-" + d.token))
		if ctlRun.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("the control run ended %v: %v", ctlRun.GetStatus(), ctlRun.GetError())
		}
		progressed++
		if handed := o.ranFor(runID); len(handed) != 0 {
			t.Fatalf("the Orker in a store outage was handed %v", handed)
		}
	}
	if progressed == 0 {
		t.Fatal("no control run finished in the window, so nothing was measured")
	}
	if control.ranFor(runID) != nil {
		t.Fatalf("the task crossed pools: %v", control.ranFor(runID))
	}
	mid, err := d.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: d.orkNS}))
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if status := mid.Msg.GetRun().GetStatus(); status == orkstackv1.Status_STATUS_SUCCEEDED || status == orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("the run ended %v during the outage", status)
	}
	for _, tr := range mid.Msg.GetRun().GetTaskRuns() {
		if tr.GetAttempt() > 1 || tr.GetStartedAt() != nil {
			t.Fatalf("task %s used an attempt during the outage: %v", tr.GetTaskId(), tr)
		}
	}
	d.transcript.line("%d control runs finished while the run was handed nothing", controlRuns+progressed)

	// Every Orker of the pool holds the cordon, so once the pool's poller record
	// is stale a new submit is refused.
	var refused error
	refusalDeadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(refusalDeadline) {
		_, refused = d.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: "echo-" + d.token, Namespace: d.orkNS}))
		if refused != nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if connect.CodeOf(refused) != connect.CodeFailedPrecondition || !strings.Contains(refused.Error(), "no live Orkers") {
		t.Fatalf("a submit to a pool whose every Orker is in a store outage = %v, want FailedPrecondition, no live Orkers", refused)
	}
	d.transcript.line("a new submit is refused after %s: %v", time.Since(cutAt).Round(time.Second), refused)

	d.transcript.section("the store returns")
	proxy.Restore()
	recovered := d.awaitStoreOutage(o.id, time.Minute, false)
	if recovered.GetCordoned() {
		t.Fatalf("no operator's cordon holds, but after recovery: %v", recovered)
	}
	run := d.await(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the run ended %v after the store returned: %v", run.GetStatus(), run.GetError())
	}
	echo := pinTaskRun(t, run, "echo")
	if echo.GetAttempt() != 1 || echo.GetOrkerId() != o.id || len(o.ranFor(runID)) != 1 {
		t.Fatalf("attempt=%d orker=%s handed=%v; want the first attempt on %s", echo.GetAttempt(), echo.GetOrkerId(), o.ranFor(runID), o.id)
	}
	t.Logf("EVIDENCE store outage: %d control runs finished in the outage, the task used no attempt during it, "+
		"a submit was refused, and attempt 1 succeeded after recovery", controlRuns+progressed)
}

const storeOutageHoldYAML = `
apiVersion: orkstack/v1
kind: Orkflow
name: __NAME__
version: 0.1.0
tasks:
  - id: hold
    uses: __IMAGE__
    pool: __POOL__
    run_as_orker_user: true
    network: none
    timeout_seconds: 300
    command:
      - /bin/sh
      - -c
      - |
        sleep __HOLD_SECONDS__
        printf '{"held":true}' > /orkstack/output/values.json
`

// A container running when the store goes away is left alone: the cordon holds
// beside it, the running count stays, and the same attempt finishes.
func TestIntegration_StoreOutage_LeavesARunningContainerAlone(t *testing.T) {
	ctx := cordonContext(t, 8*time.Minute)
	pool := fmt.Sprintf("outage-run-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "outagerun", pool)
	o, proxy, _ := storeOutageOrker(d, "orker-out-"+d.token, pool)
	d.storeOrkflow("hold-"+d.token, pool, storeOutageHoldYAML, 45)

	runID := d.submit("hold-" + d.token)
	d.awaitFleet(orkersWhere("orker_id", o.id), 2*time.Minute, "the container is running", func(rows []*orkstackv1.Orker) bool {
		return len(rows) == 1 && rows[0].GetRunningTasks() == 1 && o.sawPhase(runID, "hold", "run")
	})

	proxy.Cut()
	row := d.awaitStoreOutage(o.id, 30*time.Second, true)
	if row.GetRunningTasks() != 1 {
		t.Fatalf("the cordon landed with %d running tasks, want the container still counted", row.GetRunningTasks())
	}
	proxy.Restore()
	d.awaitStoreOutage(o.id, 30*time.Second, false)

	run := d.await(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the run ended %v: %v", run.GetStatus(), run.GetError())
	}
	if hold := pinTaskRun(t, run, "hold"); hold.GetAttempt() != 1 || len(o.ranFor(runID)) != 1 {
		t.Fatalf("attempt=%d handed=%v; the running attempt should have finished by itself", hold.GetAttempt(), o.ranFor(runID))
	}
	t.Logf("EVIDENCE store outage: a container running through the outage finished on its first attempt")
}

// The two causes are independent, end to end through the API, against a real
// store outage: recovery leaves an operator's cordon, and an operator's uncordon
// leaves an outage that is still failing. The second outage is a stalled store,
// so the check's own timeout is what declares it.
func TestIntegration_StoreOutage_AndAnOperatorsCordonAreIndependent(t *testing.T) {
	ctx := cordonContext(t, 6*time.Minute)
	pool := fmt.Sprintf("outage-two-%d", time.Now().UnixNano()%1_000_000)
	d := newCordonDeployment(t, ctx, "outagetwo", pool)
	o, proxy, _ := storeOutageOrker(d, "orker-out-"+d.token, pool)

	const reason = "runtime patch"
	d.cordon(o.id, reason)
	proxy.Cut()
	both := d.awaitStoreOutage(o.id, time.Minute, true)
	if both.GetCordonReason() != reason {
		t.Fatalf("during the outage: %v", both)
	}
	proxy.Restore()
	after := d.awaitStoreOutage(o.id, time.Minute, false)
	if !after.GetCordoned() || after.GetCordonReason() != reason || after.GetCordonedAt() == nil {
		t.Fatalf("recovery touched the operator's cordon: %v", after)
	}

	d.uncordon(o.id)
	if row := d.one(o.id); row.GetCordoned() {
		t.Fatalf("uncordon with no outage: %v", row)
	}
	proxy.Stall()
	stalled := d.awaitStoreOutage(o.id, time.Minute, true)
	if !strings.Contains(stalled.GetStoreOutage().GetReason(), "did not answer") {
		t.Fatalf("a stalled store's reason = %q", stalled.GetStoreOutage().GetReason())
	}
	answered := d.uncordon(o.id)
	if !answered.GetCordoned() || answered.GetStoreOutage() == nil {
		t.Fatalf("an operator's uncordon overrode a store that is still failing: %v", answered)
	}
	proxy.Restore()
	if row := d.awaitStoreOutage(o.id, time.Minute, false); row.GetCordoned() {
		t.Fatalf("after recovery with no operator's cordon: %v", row)
	}
	t.Logf("EVIDENCE store outage: each cause cleared alone, leaving the other")
}
