//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resetTunables are the gate's test tunables with a freeze a loaded host's
// reset fits in.
func resetTunables() engine.GateTunables {
	t := gateTunablesForTest()
	t.FreezeTimeout = 20 * time.Second
	return t
}

// resetter resets gates of this stack as an engine does.
func (s *gateStack) resetter(tun engine.GateTunables, margin time.Duration) *engine.TemporalConcurrencyResetter {
	return engine.NewTemporalConcurrencyResetter(s.c, s.namespace, "reset-test", s.gateQueue,
		engine.ConcurrencyResetSettings{Tunables: tun, FreezeMargin: margin}, nil, nil)
}

// resetKey is a key of its own and its gate.
func resetKey(label string) (key, gate string) {
	key = gateRunID(label)
	return key, engine.GateID(gateNamespace, "", key)
}

func resetOf(key, id string) engine.ConcurrencyReset {
	return engine.ConcurrencyReset{Namespace: gateNamespace, Key: key, RequestID: id, IssuedAt: time.Now()}
}

// wantReset asks and fails unless the reset stands as state.
func wantReset(t *testing.T, r *engine.TemporalConcurrencyResetter, in engine.ConcurrencyReset, state string) engine.ConcurrencyResetResult {
	t.Helper()
	res, err := r.ResetConcurrencyLimit(t.Context(), in)
	must.OKf(t, err, "reset %s as %s", in.Key, in.RequestID)
	if res.State != state {
		t.Fatalf("reset %s as %s = %+v; want %s", in.Key, in.RequestID, res, state)
	}
	return res
}

// wantRefused asks and fails unless the reset is refused code.
func wantRefused(t *testing.T, r *engine.TemporalConcurrencyResetter, in engine.ConcurrencyReset, code string) *engine.ConcurrencyResetError {
	t.Helper()
	res, err := r.ResetConcurrencyLimit(t.Context(), in)
	var refused *engine.ConcurrencyResetError
	if !errors.As(err, &refused) || refused.Code != code {
		t.Fatalf("reset %s as %s = %+v, %v; want %s", in.Key, in.RequestID, res, err, code)
	}
	return refused
}

// waitReset asks until the reset stands as state.
func (s *gateStack) waitReset(t *testing.T, r *engine.TemporalConcurrencyResetter, in engine.ConcurrencyReset, state string) engine.ConcurrencyResetResult {
	t.Helper()
	var last engine.ConcurrencyResetResult
	s.poll(t, "the reset of "+in.Key+" "+state, func() (bool, error) {
		var err error
		last, err = r.ResetConcurrencyLimit(s.ctx, in)
		return err == nil && last.State == state, err
	})
	return last
}

// executions are every execution of the gate's chain, newest first.
func (s *gateStack) executions(t *testing.T, gate string, n int) []string {
	t.Helper()
	var ids []string
	s.poll(t, fmt.Sprintf("%d executions of %s", n, gate), func() (bool, error) {
		resp, err := s.c.ListWorkflow(s.ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query: fmt.Sprintf("WorkflowId = %q", gate), PageSize: 50})
		if err != nil {
			return false, err
		}
		ids = ids[:0]
		for _, e := range resp.GetExecutions() {
			ids = append(ids, e.GetExecution().GetRunId())
		}
		return len(ids) >= n, nil
	})
	return ids
}

// resetRecords are the audit records of request id, in the order written.
func (s *gateStack) resetRecords(t *testing.T, id string) []string {
	t.Helper()
	page, err := s.audit.ListAuditRecords(s.ctx, artifacts.AuditQuery{Since: time.Now().Add(-time.Hour), Until: time.Now().Add(time.Hour), Limit: 500})
	must.OK(t, err)
	var kinds []string
	for _, r := range page.Records {
		if strings.HasPrefix(r.GetDetail(), "request "+id+":") {
			kinds = append(kinds, strings.TrimPrefix(r.GetAction(), "concurrency.reset."))
		}
	}
	slices.Sort(kinds)
	return kinds
}

// held starts a gate of key that run holds, each run a stand-in that records
// its own entry, so a check pass and a rebuild find it.
func (s *gateStack) held(t *testing.T, gate string, tun engine.GateTunables, limit int, runs ...string) string {
	t.Helper()
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	for i, run := range runs {
		want, isHeld := engine.GateAnswerGrantedForTest, i < limit
		if !isHeld {
			want = engine.GateAnswerQueuedForTest
		}
		ans := s.wantAnswer(t, gate, gateAcq(run, 1, gateLevel, limit, "queue"), want)
		s.startStandIn(t, gate, run, 1, map[int]engine.GateMemoEntryForTest{
			gateLevel: memoEntry(isHeld, limit, ans.ArrivedAt, ans.Seq, 1)})
	}
	s.waitListed(t, gate, len(runs))
	return first
}

func runsOf(entries []engine.GateEntryForTest) []string {
	var out []string
	for _, e := range entries {
		out = append(out, e.Run)
	}
	return out
}

// TestIntegration_ConcurrencyResetStartIsOnePerRequest: the cluster answers a
// start repeated under its request id with the execution it already started,
// and neither starts nor ends another, also while it is told to end the one
// running. What makes a reset's start act once rests on this.
func TestIntegration_ConcurrencyResetStartIsOnePerRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-once")
	for _, policy := range []enums.WorkflowIdConflictPolicy{
		enums.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING, enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL} {
		id := gateRunID("once")
		start := func(request string) (*workflowservice.StartWorkflowExecutionResponse, error) {
			return s.c.WorkflowService().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
				Namespace: s.namespace, WorkflowId: id, WorkflowType: &commonpb.WorkflowType{Name: engine.ConcurrencyGateWorkflowType},
				TaskQueue: &taskqueuepb.TaskQueue{Name: s.idleQueue, Kind: enums.TASK_QUEUE_KIND_NORMAL}, RequestId: request,
				WorkflowIdReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, WorkflowIdConflictPolicy: policy})
		}
		first, err := start("request-a")
		must.OKf(t, err, "%s: first start", policy)
		again, err := start("request-a")
		must.OKf(t, err, "%s: the same start again", policy)
		t.Logf("EVIDENCE policy=%s first=%s started=%t again=%s started=%t", policy, first.GetRunId(), first.GetStarted(), again.GetRunId(), again.GetStarted())
		if again.GetRunId() != first.GetRunId() {
			t.Fatalf("%s: the repeat started %s; want the first's %s", policy, again.GetRunId(), first.GetRunId())
		}
		d, err := describeExecution(s.c, ctx, id, first.GetRunId())
		must.OK(t, err)
		if st := d.GetWorkflowExecutionInfo().GetStatus(); st != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			t.Fatalf("%s: the execution a repeat found is %s; want it running", policy, st)
		}
		// The positive control: another request does what the policy says.
		other, err := start("request-b")
		if policy == enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
			if err == nil {
				t.Fatalf("%s: another request started %s beside a running one", policy, other.GetRunId())
			}
			continue
		}
		must.OKf(t, err, "%s: another request", policy)
		if other.GetRunId() == first.GetRunId() {
			t.Fatalf("%s: another request was answered with the first's execution", policy)
		}
		// The first's repeat, after another replaced it, starts nothing that
		// ends the one running now without this test seeing it.
		late, err := start("request-a")
		t.Logf("EVIDENCE a repeat after another replaced it = %s, %v (running: %s)", late.GetRunId(), err, other.GetRunId())
	}
}

// TestIntegration_ConcurrencyResetAnsweringDropsNothing: a reset of a limit
// that answers keeps every run that holds it or waits, in order, and drops one
// whose run has ended; its records are written by the engine that runs it, and
// the same request asked again reports it and starts nothing.
func TestIntegration_ConcurrencyResetAnsweringDropsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-keep")
	s.startWorker()
	tun := resetTunables()
	r := s.resetter(tun, 5*time.Second)
	key, gate := resetKey("keep")
	a, b, c, d := gateRunID("a"), gateRunID("b"), gateRunID("c"), gateRunID("d")
	first := s.held(t, gate, tun, 2, a, b, c, d)

	dry := engine.ConcurrencyReset{Namespace: gateNamespace, Key: key, DryRun: true}
	verdict := wantReset(t, r, dry, engine.ResetStateDryRun)
	if verdict.Holders != 2 || verdict.Queued != 2 || verdict.Dropped != 0 || verdict.ConsequenceCode != engine.ResetAnswering {
		t.Fatalf("dry run = %+v; want two holding, two waiting, none dropped", verdict)
	}
	if got := s.executions(t, gate, 1); len(got) != 1 || got[0] != first {
		t.Fatalf("a dry run left executions %v; want %s alone", got, first)
	}

	s.terminate(t, d)
	in := resetOf(key, "keep-1")
	res := wantReset(t, r, in, engine.ResetStateRunning)
	if !res.Started || res.Dropped != 1 || res.Holders != 2 || res.Queued != 1 || res.Epoch != verdict.Epoch || res.NewEpoch != verdict.Epoch+1 {
		t.Fatalf("reset = %+v; want d dropped and the rest kept in the next epoch", res)
	}
	v := s.waitState(t, gate, "the new record rebuilt", func(v engine.GateStateView) bool { return v.State.Rebuild.State == "" })
	if h, q := runsOf(v.State.Holders), runsOf(v.State.Queue); !slices.Equal(h, []string{a, b}) || !slices.Equal(q, []string{c}) || v.State.Epoch != res.NewEpoch {
		t.Fatalf("holders %v, queue %v, epoch %d; want %v, %v, %d", h, q, v.State.Epoch, []string{a, b}, []string{c}, res.NewEpoch)
	}
	s.waitReset(t, r, in, engine.ResetStateCompleted)
	if got := s.executions(t, gate, 2); len(got) != 2 {
		t.Fatalf("executions %v; want the one replaced and the new one", got)
	}
	s.poll(t, "the reset's records", func() (bool, error) {
		return slices.Equal(s.resetRecords(t, "keep-1"), []string{"applied", "completed"}), nil
	})
	// Its line still moves: a's release grants c, by signal, once.
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "c granted", func(v engine.GateStateView) bool { return slices.Contains(runsOf(v.State.Holders), c) })
	s.poll(t, "c signalled", func() (bool, error) { return s.grants(t, c) == 1, nil })
}

// TestIntegration_ConcurrencyResetConcurrentFirstActionsSameId: the same
// request from two replicas at once starts one gate and is answered by both.
func TestIntegration_ConcurrencyResetConcurrentFirstActionsSameId(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-race")
	s.startWorker()
	tun := resetTunables()
	for _, absent := range []bool{false, true} {
		key, gate := resetKey("race")
		executions := 1
		a := gateRunID("a")
		if !absent {
			s.held(t, gate, tun, 1, a)
			executions = 2
		}
		in := resetOf(key, "race-1")
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := s.resetter(tun, 5*time.Second)
				for {
					_, err := r.ResetConcurrencyLimit(ctx, in)
					var refused *engine.ConcurrencyResetError
					if errors.As(err, &refused) && refused.Retryable && ctx.Err() == nil {
						select {
						case <-ctx.Done():
						case <-time.After(50 * time.Millisecond):
						}
						continue
					}
					errs[i] = err
					return
				}
			}()
		}
		wg.Wait()
		must.OKf(t, errors.Join(errs...), "absent %v: four replicas asking at once", absent)
		s.waitReset(t, s.resetter(tun, 5*time.Second), in, engine.ResetStateCompleted)
		if got := s.executions(t, gate, executions); len(got) != executions {
			t.Fatalf("absent %v: executions %v; want %d", absent, got, executions)
		}
		if !absent {
			if h := runsOf(s.state(t, gate).State.Holders); !slices.Equal(h, []string{a}) {
				t.Fatalf("holders %v; want %s", h, a)
			}
		}
		s.poll(t, "its records, each once", func() (bool, error) {
			return slices.Equal(s.resetRecords(t, "race-1"), []string{"applied", "completed"}), nil
		})
		s.audit = artifacts.NewMemoryAuditStore()
		s.stopWorker()
		s.startWorker()
	}
}

// TestIntegration_ConcurrencyResetRacingAcquires: acquires racing two resets
// are each answered, none is lost, and no more than the limit ever hold.
func TestIntegration_ConcurrencyResetRacingAcquires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-acquires")
	s.startWorker()
	tun := resetTunables()
	tun.AcquireTimeout, tun.Linger = time.Minute, 2*time.Minute
	key, gate := resetKey("acq")
	const limit = 2
	first := gateRunID("first")
	s.held(t, gate, tun, limit, first)
	acquirer := engine.NewGateActivities(s.c)
	runs := []string{first}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range 5 {
		run := gateRunID(fmt.Sprintf("r%d", i))
		runs = append(runs, run)
		s.startStandIn(t, gate, run, 1, nil)
		wg.Add(1)
		go func() {
			defer wg.Done()
			actx, acancel := context.WithTimeout(ctx, time.Minute)
			defer acancel()
			req := gateAcq(run, 1, gateLevel, limit, "queue")
			res, err := acquirer.AcquireConcurrency(actx, engine.AcquireConcurrencyInput{GateID: gate, GateQueue: s.gateQueue,
				Start:   engine.GateStart{GateID: gate, Namespace: gateNamespace, Key: key, Tunables: tun},
				Request: req, UpdateID: "acq-" + run + "-1", BackoffMin: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond})
			mu.Lock()
			defer mu.Unlock()
			if err != nil || (res.Answer.Answer != engine.GateAnswerGrantedForTest && res.Answer.Answer != engine.GateAnswerQueuedForTest) {
				t.Errorf("%s = %+v, %v; want granted or queued", run, res, err)
			}
		}()
	}
	r := s.resetter(tun, 5*time.Second)
	for _, id := range []string{"acq-1", "acq-2"} {
		in := resetOf(key, id)
		s.waitReset(t, r, in, engine.ResetStateCompleted)
	}
	wg.Wait()
	v := s.waitState(t, gate, "every run holding or waiting", func(v engine.GateStateView) bool {
		return len(v.State.Holders)+len(v.State.Queue) == len(runs)
	})
	if got := len(v.State.Holders); got > limit {
		t.Fatalf("%d hold a key of limit %d", got, limit)
	}
	all := slices.Sorted(slices.Values(append(runsOf(v.State.Holders), runsOf(v.State.Queue)...)))
	if !slices.Equal(all, slices.Sorted(slices.Values(runs))) || !slices.Contains(runsOf(v.State.Holders), first) {
		t.Fatalf("holding or waiting %v; want %v, with %s holding", all, runs, first)
	}
}

// TestIntegration_ConcurrencyResetSlowRpcPastFreezeStartsNothing: a reset that
// is too late for its freeze starts nothing and unfreezes the limit; asked
// again past its time it reports not_applied, and it never acts.
func TestIntegration_ConcurrencyResetSlowRpcPastFreezeStartsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-slow")
	s.startWorker()
	tun := resetTunables()
	key, gate := resetKey("slow")
	a, b := gateRunID("a"), gateRunID("b")
	first := s.held(t, gate, tun, 2, a)
	// Its margin is the whole freeze, so it is always too late.
	slow := s.resetter(tun, tun.FreezeTimeout+time.Minute)
	in := resetOf(key, "slow-1")
	in.IssuedAt = time.Now().Add(-time.Minute)
	if refused := wantRefused(t, slow, in, engine.ResetRefusedCode); !refused.Retryable {
		t.Fatalf("refused %+v; want it asked again", refused)
	}
	s.waitState(t, gate, "unfrozen", func(v engine.GateStateView) bool { return v.State.FrozenUntil.IsZero() })
	s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 2, "queue"), engine.GateAnswerGrantedForTest)
	// Past the time its first step had to come, it never acts.
	old := in
	old.RequestID, old.IssuedAt = "slow-2", time.Now().Add(-10*time.Minute)
	if res := wantReset(t, s.resetter(tun, 5*time.Second), old, engine.ResetStateNotApplied); res.Code != engine.ResetNotAppliedCode {
		t.Fatalf("= %+v", res)
	}
	if got := s.executions(t, gate, 1); len(got) != 1 || got[0] != first {
		t.Fatalf("executions %v; want %s alone", got, first)
	}
	if got := append(s.resetRecords(t, "slow-1"), s.resetRecords(t, "slow-2")...); len(got) != 0 {
		t.Fatalf("records %v of resets that never acted", got)
	}
}

// TestIntegration_ConcurrencyResetGateFirstTaskPendingReportsRunning: a reset
// whose new limit has not run yet reports running, and says it does not answer.
func TestIntegration_ConcurrencyResetGateFirstTaskPendingReportsRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-pending")
	tun := resetTunables()
	r := s.resetter(tun, 5*time.Second)
	key, _ := resetKey("pending")
	in := resetOf(key, "pending-1")
	if res := wantReset(t, r, in, engine.ResetStateRunning); !res.Started || res.ConsequenceCode != engine.ResetAbsent {
		t.Fatalf("reset of an absent limit = %+v", res)
	}
	if res := wantReset(t, r, in, engine.ResetStateRunning); res.Started || res.Note == "" {
		t.Fatalf("asked again before it ran = %+v; want running, with a note", res)
	}
	s.startWorker()
	s.waitReset(t, r, in, engine.ResetStateCompleted)
}

// TestIntegration_ConcurrencyResetUnansweredRefusedThenForced: a limit that
// does not answer is refused, and forced it is rebuilt from the runs' records.
func TestIntegration_ConcurrencyResetUnansweredRefusedThenForced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-silent")
	s.startWorker()
	tun := resetTunables()
	r := s.resetter(tun, 5*time.Second)
	key, gate := resetKey("silent")
	a, b := gateRunID("a"), gateRunID("b")
	s.held(t, gate, tun, 1, a, b)
	s.stopWorker()
	in := resetOf(key, "silent-1")
	refused := wantRefused(t, r, in, engine.ResetUnansweredCode)
	if !strings.Contains(refused.Reason, "up to 1 extra runs") {
		t.Fatalf("refused %q; want it to name the limit", refused.Reason)
	}
	forced := resetOf(key, "silent-2")
	forced.ForceUnanswered = true
	if res := wantReset(t, r, forced, engine.ResetStateRunning); res.ConsequenceCode != engine.ResetUnansweredForced || !res.Started {
		t.Fatalf("forced = %+v", res)
	}
	s.startWorker()
	s.waitReset(t, r, forced, engine.ResetStateCompleted)
	v := s.waitState(t, gate, "rebuilt", func(v engine.GateStateView) bool { return v.State.Rebuild.State == "" })
	if h, q := runsOf(v.State.Holders), runsOf(v.State.Queue); !slices.Equal(h, []string{a}) || !slices.Equal(q, []string{b}) {
		t.Fatalf("holders %v, queue %v; want %s and %s from their records", h, q, a, b)
	}
}

// TestIntegration_ConcurrencyResetInterruptedByLaterReset: a reset whose
// rebuild has not finished when a later one replaces it reports interrupted,
// and never acts again.
func TestIntegration_ConcurrencyResetInterruptedByLaterReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-twice")
	s.startWorker()
	tun := resetTunables()
	// Two entries never fit, so a rebuild keeps failing.
	tun.MaxEntries, tun.RebuildTimeout = 1, time.Millisecond
	r := s.resetter(tun, 5*time.Second)
	key, gate := resetKey("twice")
	a, b := gateRunID("a"), gateRunID("b")
	now := time.Now()
	s.startStandIn(t, gate, a, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 2, now.Add(-2*time.Minute), 1, 1)})
	s.startStandIn(t, gate, b, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 2, now.Add(-time.Minute), 2, 1)})
	s.waitListed(t, gate, 2)
	one, two := resetOf(key, "twice-1"), resetOf(key, "twice-2")
	s.waitReset(t, r, one, engine.ResetStateFailed)
	if refused := wantRefused(t, r, two, engine.ResetRefusedCode); !refused.Retryable {
		t.Fatalf("a reset while the one before it may be repeated: %+v; want it asked again", refused)
	}
	s.waitReset(t, r, two, engine.ResetStateFailed)
	if res := wantReset(t, r, one, engine.ResetStateInterrupted); res.Code != engine.ResetInterruptedCode {
		t.Fatalf("= %+v", res)
	}
	s.terminate(t, b)
	s.waitListed(t, gate, 1)
	s.waitReset(t, r, two, engine.ResetStateCompleted)
	wantReset(t, r, one, engine.ResetStateInterrupted)
	s.poll(t, "the second's records", func() (bool, error) {
		return slices.Equal(s.resetRecords(t, "twice-2"), []string{"applied", "completed", "failed"}), nil
	})
}

// TestIntegration_ConcurrencyResetRetrySameRequestId: the same request never
// acts again, whether the limit runs, has closed, was started again by an
// acquire, or was ended and started again (CR-1); another under its id is
// refused at each, and an acquire is granted throughout.
func TestIntegration_ConcurrencyResetRetrySameRequestId(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-retry")
	s.startWorker()
	tun := resetTunables()
	tun.Linger = 15 * time.Second
	r := s.resetter(tun, 5*time.Second)
	key, gate := resetKey("retry")
	in := resetOf(key, "retry-1")
	s.waitReset(t, r, in, engine.ResetStateCompleted)
	executions := 1
	stillOnce := func(when string) {
		t.Helper()
		wantReset(t, r, in, engine.ResetStateCompleted)
		other := in
		other.IssuedAt = in.IssuedAt.Add(time.Second)
		wantRefused(t, r, other, engine.ResetRefusedCode)
		other = in
		other.ForceUnanswered = true
		wantRefused(t, r, other, engine.ResetRefusedCode)
		if got := s.executions(t, gate, executions); len(got) != executions {
			t.Fatalf("%s: executions %v; want %d", when, got, executions)
		}
	}
	stillOnce("running")
	s.closeEmpty(t, gate)
	stillOnce("closed")
	acquire := func(run string) {
		t.Helper()
		s.startStandIn(t, gate, run, 1, nil)
		res, err := engine.NewGateActivities(s.c).AcquireConcurrency(ctx, engine.AcquireConcurrencyInput{GateID: gate, GateQueue: s.gateQueue,
			Start:   engine.GateStart{GateID: gate, Namespace: gateNamespace, Key: key, Tunables: tun},
			Request: gateAcq(run, 1, gateLevel, 2, "queue"), UpdateID: "acq-" + run + "-1", BackoffMin: 50 * time.Millisecond, BackoffMax: 500 * time.Millisecond})
		must.OKf(t, err, "acquire for %s", run)
		if res.Answer.Answer == "" {
			t.Fatalf("acquire for %s = %+v", run, res)
		}
		executions++
	}
	acquire(gateRunID("clean"))
	stillOnce("after a clean end")
	must.OK(t, s.c.TerminateWorkflow(ctx, gate, "", "the test ends it uncleanly"))
	acquire(gateRunID("unclean"))
	stillOnce("after an unclean end")
	// A new request is taken there.
	next := resetOf(key, "retry-2")
	s.waitReset(t, r, next, engine.ResetStateCompleted)
	executions++
	stillOnce("after a later reset")
}

// TestIntegration_ConcurrencyResetExpectedEpochConflict: a reset at an epoch
// the limit has left is refused, changes nothing, and leaves it unfrozen.
func TestIntegration_ConcurrencyResetExpectedEpochConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "reset-epoch")
	s.startWorker()
	tun := resetTunables()
	r := s.resetter(tun, 5*time.Second)
	key, gate := resetKey("epoch")
	first := s.held(t, gate, tun, 1, gateRunID("a"))
	epoch := s.state(t, gate).State.Epoch
	in := resetOf(key, "epoch-1")
	stale := epoch + 7
	in.ExpectedEpoch = &stale
	wantRefused(t, r, in, engine.ResetConflictCode)
	if got := s.executions(t, gate, 1); len(got) != 1 || got[0] != first {
		t.Fatalf("executions %v; want %s alone", got, first)
	}
	if v := s.state(t, gate); !v.State.FrozenUntil.IsZero() && time.Now().Before(v.State.FrozenUntil) {
		t.Fatalf("frozen until %v", v.State.FrozenUntil)
	}
	right := resetOf(key, "epoch-2")
	right.ExpectedEpoch = &epoch
	if res := wantReset(t, r, right, engine.ResetStateRunning); res.NewEpoch != epoch+1 {
		t.Fatalf("= %+v; want epoch %d", res, epoch+1)
	}
	s.waitReset(t, r, right, engine.ResetStateCompleted)
}

// strayStarts is a cluster whose starts are kept, so a test can send a copy of
// one again long after it was made, as a network or a process that stood still
// may: a fault a real cluster does not produce on demand.
type strayStarts struct {
	client.Client
	mu   sync.Mutex
	made []*workflowservice.StartWorkflowExecutionRequest
	// refuse, when set, is the cluster's answer to every start.
	refuse error
}

type strayService struct {
	workflowservice.WorkflowServiceClient
	starts *strayStarts
}

func (s *strayStarts) WorkflowService() workflowservice.WorkflowServiceClient {
	return strayService{s.Client.WorkflowService(), s}
}

func (s strayService) StartWorkflowExecution(ctx context.Context, in *workflowservice.StartWorkflowExecutionRequest, opts ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
	s.starts.mu.Lock()
	s.starts.made = append(s.starts.made, proto.Clone(in).(*workflowservice.StartWorkflowExecutionRequest))
	refuse := s.starts.refuse
	s.starts.mu.Unlock()
	if refuse != nil {
		return nil, refuse
	}
	return s.WorkflowServiceClient.StartWorkflowExecution(ctx, in, opts...)
}

// stray sends a copy of the n-th start again.
func (s *gateStack) stray(t *testing.T, starts *strayStarts, n int) (*workflowservice.StartWorkflowExecutionResponse, error) {
	t.Helper()
	starts.mu.Lock()
	in := proto.Clone(starts.made[n]).(*workflowservice.StartWorkflowExecutionRequest)
	starts.mu.Unlock()
	resp, err := s.c.WorkflowService().StartWorkflowExecution(s.ctx, in)
	t.Logf("EVIDENCE a stray copy of start %d (%s) = %s started=%t, %v", n, in.GetWorkflowIdConflictPolicy(), resp.GetRunId(), resp.GetStarted(), err)
	return resp, err
}

// strayStack is a stack whose resetter's starts are kept.
func newStrayStack(t *testing.T, ctx context.Context, label string, tun engine.GateTunables) (*gateStack, *strayStarts, *engine.TemporalConcurrencyResetter) {
	t.Helper()
	s := newGateStack(t, ctx, label)
	s.startWorker()
	starts := &strayStarts{Client: s.c}
	return s, starts, engine.NewTemporalConcurrencyResetter(starts, s.namespace, "reset-test", s.gateQueue,
		engine.ConcurrencyResetSettings{Tunables: tun, FreezeMargin: 5 * time.Second}, nil, nil)
}

// TestIntegration_ConcurrencyResetLateDuplicateAfterAnother: reset A was
// applied and B replaced it; then A arrives again. Asked again it changes
// nothing. While A's start may still be repeated B is refused, so a repeat of
// it finds A's own execution. A copy of A's start that only reaches the
// cluster after B (CR-2) replaces its execution, and drops no run, grants no
// extra slot, and lets no reset, A or B, act on the key again.
func TestIntegration_ConcurrencyResetLateDuplicateAfterAnother(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	tun := resetTunables()
	s, starts, r := newStrayStack(t, ctx, "reset-late", tun)
	key, gate := resetKey("late")
	h, q := gateRunID("h"), gateRunID("q")
	s.held(t, gate, tun, 1, h, q)
	a, b, c := resetOf(key, "late-a"), resetOf(key, "late-b"), resetOf(key, "late-c")
	// unchanged fails unless the limit still has its line, in executions.
	unchanged := func(when string, executions int) {
		t.Helper()
		v := s.waitState(t, gate, "rebuilt "+when, func(v engine.GateStateView) bool { return v.State.Rebuild.State == "" })
		if got := s.executions(t, gate, executions); len(got) != executions {
			t.Fatalf("%s: executions %v; want %d", when, got, executions)
		}
		if h2, q2 := runsOf(v.State.Holders), runsOf(v.State.Queue); !slices.Equal(h2, []string{h}) || !slices.Equal(q2, []string{q}) {
			t.Fatalf("%s: holders %v, queue %v; want %s and %s", when, h2, q2, h, q)
		}
		if n := s.grants(t, q); n != 0 {
			t.Fatalf("%s: the waiting run was granted %d times", when, n)
		}
	}
	made := len(starts.made)
	wantReset(t, r, a, engine.ResetStateRunning)
	if refused := wantRefused(t, r, b, engine.ResetRefusedCode); !refused.Retryable {
		t.Fatalf("B while A's start may be repeated: %+v; want it asked again", refused)
	}
	// A's start again, while it may be: A's own execution.
	resp, err := s.stray(t, starts, made)
	must.OK(t, err)
	if d, err := describeExecution(s.c, ctx, gate, ""); err != nil || d.GetWorkflowExecutionInfo().GetExecution().GetRunId() != resp.GetRunId() {
		t.Fatalf("the repeat was answered %s, and %v runs (%v)", resp.GetRunId(), d.GetWorkflowExecutionInfo().GetExecution(), err)
	}
	s.waitReset(t, r, a, engine.ResetStateCompleted)
	unchanged("after A", 2)
	s.waitReset(t, r, b, engine.ResetStateCompleted)
	unchanged("after B", 3)
	epoch := s.state(t, gate).State.Epoch
	wantReset(t, r, a, engine.ResetStateCompleted)
	unchanged("A asked again", 3)
	if got := s.state(t, gate).State.Epoch; got != epoch {
		t.Fatalf("A asked again moved the epoch from %d to %d", epoch, got)
	}

	_, err = s.stray(t, starts, made)
	must.OK(t, err)
	unchanged("a stray copy of A's start", 4)
	wantReset(t, r, a, engine.ResetStateCompleted)
	for _, again := range []engine.ConcurrencyReset{b, c} {
		if refused := wantRefused(t, r, again, engine.ResetRefusedCode); refused.Retryable {
			t.Fatalf("%s after it: %+v; want it refused for good", again.RequestID, refused)
		}
	}
	unchanged("every reset after it", 4)
	// Its line still moves, once.
	s.release(t, gate, h, 1, gateLevel)
	s.poll(t, "the waiting run granted", func() (bool, error) { return s.grants(t, q) == 1, nil })
}

// TestIntegration_ConcurrencyResetLateDuplicateAfterClose: reset A of a key
// with no limit was applied, B replaced it, and the limit closed. Asked again
// A changes nothing. A copy of A's start is refused while B's limit runs, and
// one that finds the key closed starts a limit that knows nothing of B: by its
// start_by it takes no reset for the window, and grants as ever.
func TestIntegration_ConcurrencyResetLateDuplicateAfterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	tun := resetTunables()
	tun.Linger = 40 * time.Second
	s, starts, r := newStrayStack(t, ctx, "reset-late-closed", tun)
	key, gate := resetKey("late-closed")
	a, b := resetOf(key, "closed-a"), resetOf(key, "closed-b")
	made := len(starts.made)
	s.waitReset(t, r, a, engine.ResetStateCompleted)
	s.waitReset(t, r, b, engine.ResetStateCompleted)
	// With B's limit running a copy of A's start, made for an absent
	// limit, is refused by the cluster.
	if resp, err := s.stray(t, starts, made); err == nil {
		t.Fatalf("a copy of A's start was taken beside B's limit: %v", resp)
	}
	s.closeEmpty(t, gate)
	for _, again := range []engine.ConcurrencyReset{a, b} {
		wantReset(t, r, again, engine.ResetStateCompleted)
	}
	before := s.executions(t, gate, 2)
	if len(before) != 2 {
		t.Fatalf("executions %v; want A's and B's", before)
	}
	resp, err := s.stray(t, starts, made)
	must.OK(t, err)
	if slices.Contains(before, resp.GetRunId()) {
		t.Fatalf("the cluster answered a stray copy with %s, which has closed: this test no longer reaches the gate's own rule", resp.GetRunId())
	}
	s.waitState(t, gate, "rebuilt", func(v engine.GateStateView) bool { return v.State.Rebuild.State == "" })
	s.waitReset(t, r, a, engine.ResetStateCompleted)
	if refused := wantRefused(t, r, b, engine.ResetRefusedCode); refused.Retryable {
		t.Fatalf("B after it: %+v; want it refused for good", refused)
	}
	if got := s.executions(t, gate, 3); len(got) != 3 {
		t.Fatalf("executions %v; want no reset to have acted", got)
	}
	run := gateRunID("after")
	s.startStandIn(t, gate, run, 1, nil)
	s.wantAnswer(t, gate, gateAcq(run, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
}

// TestIntegration_ConcurrencyResetRefusedStartLeavesNothingFrozen: a reset whose
// new limit the cluster refuses to start is refused for good and leaves the
// limit as it was, granting at once; the same request acts once the cluster
// takes it.
func TestIntegration_ConcurrencyResetRefusedStartLeavesNothingFrozen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	tun := resetTunables()
	tun.FreezeTimeout = 50 * time.Second // long enough that the freeze cannot end by itself below
	s, starts, r := newStrayStack(t, ctx, "reset-refused", tun)
	key, gate := resetKey("refused")
	a, b := gateRunID("a"), gateRunID("b")
	first := s.held(t, gate, tun, 2, a)
	in := resetOf(key, "refused-1")
	asked := time.Now()
	starts.refuse = serviceerror.NewInvalidArgument("the cluster refuses it")
	if refused := wantRefused(t, r, in, engine.ResetRefusedCode); refused.Retryable {
		t.Fatalf("refused %+v; want it refused for good", refused)
	}
	s.waitState(t, gate, "unfrozen", func(v engine.GateStateView) bool { return v.State.FrozenUntil.IsZero() })
	ans := s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 2, "queue"), engine.GateAnswerGrantedForTest)
	if took := time.Since(asked); took >= tun.FreezeTimeout {
		t.Fatalf("granted %v after the reset was asked; its freeze ended by itself", took)
	}
	if got := s.executions(t, gate, 1); len(got) != 1 || got[0] != first {
		t.Fatalf("executions %v; want %s alone", got, first)
	}
	starts.refuse = nil
	s.startStandIn(t, gate, b, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 2, ans.ArrivedAt, ans.Seq, 1)})
	s.waitListed(t, gate, 2)
	s.waitReset(t, r, in, engine.ResetStateCompleted)
	if h := runsOf(s.state(t, gate).State.Holders); len(h) != 2 {
		t.Fatalf("holders %v; want %s and %s", h, a, b)
	}
}
