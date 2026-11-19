//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// The concurrency gate's integration harness (E88 P2). P2 has the gate and
// the census but no run side yet: P3's runs acquire, record their entries in
// their memo and receive grants. Until then a test plays the runs' part
// through the gate's own wire: it sends the acquire Update and the release
// signals a run will send, and starts each run as a stand-in, an execution of
// RunWorkflow on a task queue nothing polls, carrying the memo entries and
// index attributes a P3 run writes. A stand-in is open, describable, listed
// by the gate's rebuild query and receives its grant signal exactly as a run
// does; it is not a fake of anything the cluster does.

// gateTunablesForTest are tunables a scenario finishes on within seconds:
// every bound Check holds them to, a linger above a tombstone's life, and no
// check pass or sweep unless a scenario asks for one.
func gateTunablesForTest() engine.GateTunables {
	t := engine.DefaultGateTunables()
	t.MaxEntries, t.MaxTombstones, t.MaxLabels = 8, 16, 4
	t.AcquireTimeout, t.ClockGrace, t.TombstoneMargin = 3*time.Second, time.Second, time.Second
	t.VerifyMinAge, t.VerifyDebounce, t.VerifyAwait = time.Hour, 200*time.Millisecond, 3*time.Second
	t.VerifyParallel, t.VerifyBatch, t.VerifyTimeout = 4, 2, 10*time.Second
	t.TaskAckGrace, t.TaskConfirmBatch, t.StalledAfter = time.Second, 8, time.Hour
	t.HolderSweepInterval = time.Hour
	t.GrantBackoffMin, t.GrantBackoffMax = 200*time.Millisecond, time.Second
	t.RebuildPageTimeout, t.RebuildSettle = 10*time.Second, 2*time.Second
	t.RebuildBackoffMin, t.RebuildBackoffMax, t.RebuildTimeout = 300*time.Millisecond, time.Second, time.Second
	t.FreezeTimeout, t.DrainMargin = time.Second, time.Second
	t.ContinueEvents, t.ContinueUpdates = 2000, 500
	t.Linger = 6 * time.Second
	return t
}

// gateLevel is a run-level entry's task index.
const gateLevel = engine.GateRunLevelForTest

// gateNamespace is the OrkStack namespace every test gate holds a key in.
const gateNamespace = "payments"

// gateLimitLabel is every test acquire's limit template.
const gateLimitLabel = "deploy-{{ inputs.env }}"

// censusSnapshots is a census sink that keeps every snapshot.
type censusSnapshots struct {
	mu  sync.Mutex
	got []engine.CensusSnapshot
}

func (c *censusSnapshots) RecordConcurrencyCensus(s engine.CensusSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, s)
}

func (c *censusSnapshots) last() (engine.CensusSnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.got) == 0 {
		return engine.CensusSnapshot{}, false
	}
	return c.got[len(c.got)-1], true
}

// gateStack is one private Temporal namespace with the concurrency index
// attributes registered, an engine worker that runs the gate and the census
// with their local activities, and a queue no worker polls for the stand-in
// runs.
type gateStack struct {
	t         *testing.T
	ctx       context.Context
	namespace string
	c         client.Client
	// activities is the client the gate's and the census's local activities
	// read the cluster through: c, or one dialled through a fault proxy.
	activities client.Client
	gateQueue  string
	idleQueue  string
	census     *censusSnapshots

	mu     sync.Mutex
	worker worker.Worker
}

func newGateStack(t *testing.T, ctx context.Context, label string) *gateStack {
	t.Helper()
	namespace := chaosNamespace("conc-" + label)
	c := dialRoutingCluster(t, ctx, temporalAddress(), namespace)
	usable, err := task.EnsureConcurrencySearchAttributes(ctx, c, namespace)
	must.OKf(t, err, "register the concurrency attributes in %s", namespace)
	if !usable {
		t.Fatalf("the concurrency attributes are not usable in %s", namespace)
	}
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	return &gateStack{
		t: t, ctx: ctx, namespace: namespace, c: c, activities: c,
		gateQueue: "ork-conc-" + label + "-engine-" + suffix,
		idleQueue: "ork-conc-" + label + "-idle-" + suffix,
		census:    &censusSnapshots{},
	}
}

// startWorker starts an engine worker: the gate and census workflows and
// their local activities, as cmd/ork registers them on its run worker.
func (s *gateStack) startWorker() {
	s.t.Helper()
	w := worker.New(s.c, s.gateQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(engine.ConcurrencyGateWorkflow, workflow.RegisterOptions{Name: engine.ConcurrencyGateWorkflowType})
	w.RegisterWorkflowWithOptions(engine.ConcurrencyCensusWorkflow, workflow.RegisterOptions{Name: engine.ConcurrencyCensusWorkflowType})
	engine.RegisterGateActivities(w, engine.NewGateActivities(s.activities))
	engine.RegisterCensusActivities(w, engine.NewCensusActivities(s.activities, s.census))
	must.OKf(s.t, w.Start(), "start the engine worker")
	s.mu.Lock()
	s.worker = w
	s.mu.Unlock()
	s.t.Cleanup(s.stopWorker)
}

// stopWorker stops the engine worker, if one runs.
func (s *gateStack) stopWorker() {
	s.mu.Lock()
	w := s.worker
	s.worker = nil
	s.mu.Unlock()
	if w != nil {
		w.Stop()
	}
}

// gateRunIDs numbers the run ids this process makes.
var gateRunIDs atomic.Int64

// gateRunID is a run id unique to this process, at most 48 bytes.
func gateRunID(label string) string {
	return fmt.Sprintf("ork-%s-%d-%d", label, os.Getpid(), gateRunIDs.Add(1))
}

// gateAcq is an acquire as a run sends it now.
func gateAcq(run string, seqBase int64, taskIx, limit int, strategy string) engine.GateAcquireForTest {
	now := time.Now()
	return engine.GateAcquireForTest{
		Proto: engine.GateProtoForTest, Run: run, SeqBase: seqBase, TaskIx: taskIx, Limit: limit,
		Strategy: strategy, Label: gateLimitLabel, WaitUntil: now.Add(time.Hour), IssuedAt: now,
	}
}

// memoEntry is one entry as a run records it in its memo.
func memoEntry(held bool, limit int, arrivedAt time.Time, seq, seqBase int64) engine.GateMemoEntryForTest {
	e := engine.GateMemoEntryForTest{
		Proto: engine.GateProtoForTest, Held: held, Limit: limit, Strategy: "queue", Label: gateLimitLabel,
		ArrivedAt: arrivedAt, Seq: seq, SeqBase: seqBase,
	}
	if !held {
		e.WaitUntil = arrivedAt.Add(time.Hour)
	}
	return e
}

// startStandIn starts run as a stand-in for a run of incarnation seq with
// these entries at gateID in its memo, and the index attribute each entry
// names it under.
func (s *gateStack) startStandIn(t *testing.T, gateID, run string, seq int64, entries map[int]engine.GateMemoEntryForTest) {
	t.Helper()
	memo := map[string]any{engine.GateMemoSeqKeyForTest: seq}
	var held, queued bool
	for ix, e := range entries {
		memo[engine.GateMemoKeyForTest(gateID, ix)] = e
		held, queued = held || e.Held, queued || !e.Held
	}
	var attrs []temporal.SearchAttributeUpdate
	if held {
		attrs = append(attrs, task.SearchKeyOrkflowConcurrencyHeld.ValueSet([]string{gateID}))
	}
	if queued {
		attrs = append(attrs, task.SearchKeyOrkflowConcurrencyQueued.ValueSet([]string{gateID}))
	}
	_, err := s.c.ExecuteWorkflow(s.ctx, client.StartWorkflowOptions{
		ID: run, TaskQueue: s.idleQueue, Memo: memo, TypedSearchAttributes: temporal.NewSearchAttributes(attrs...),
	}, engine.RunWorkflowType)
	must.OKf(t, err, "start stand-in run %s", run)
	t.Cleanup(func() {
		if err := s.c.TerminateWorkflow(context.Background(), run, "", "test over"); err != nil && !isGone(err) {
			t.Logf("terminate stand-in %s: %v", run, err)
		}
	})
}

// isGone reports an error saying the execution no longer runs.
func isGone(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

// deleteRun deletes a stand-in run and its history.
func (s *gateStack) deleteRun(t *testing.T, run string) {
	t.Helper()
	if err := s.c.TerminateWorkflow(s.ctx, run, "", "deleting it"); err != nil && !isGone(err) {
		t.Fatalf("terminate %s before deleting it: %v", run, err)
	}
	_, err := s.c.WorkflowService().DeleteWorkflowExecution(s.ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run},
	})
	must.OKf(t, err, "delete %s", run)
}

// terminate ends a stand-in run, as a run that finished or was terminated
// leaves its gate.
func (s *gateStack) terminate(t *testing.T, run string) {
	t.Helper()
	must.OKf(t, s.c.TerminateWorkflow(s.ctx, run, "", "the test ends this run"), "terminate %s", run)
}

// waitListed waits until the gate's rebuild query lists n open runs.
func (s *gateStack) waitListed(t *testing.T, gateID string, n int) {
	t.Helper()
	query := fmt.Sprintf(`WorkflowType = %q AND ExecutionStatus = "Running" AND (%s = %q OR %s = %q)`,
		engine.RunWorkflowType, task.SearchAttrOrkflowConcurrencyHeld, gateID, task.SearchAttrOrkflowConcurrencyQueued, gateID)
	s.poll(t, fmt.Sprintf("%d runs listed at %s", n, gateID), func() (bool, error) {
		resp, err := s.c.ListWorkflow(s.ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: query, PageSize: 100})
		return err == nil && len(resp.GetExecutions()) == n, err
	})
}

// poll waits, at most a minute, until ok holds. An error is "not yet", and
// the last one is named if the wait runs out.
func (s *gateStack) poll(t *testing.T, what string, ok func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		done, err := ok()
		if done {
			return
		}
		if err != nil {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited a minute for %s (last error: %v)", what, lastErr)
		}
		select {
		case <-ticker.C:
		case <-s.ctx.Done():
			t.Fatalf("waiting for %s: %v (last error: %v)", what, s.ctx.Err(), lastErr)
		}
	}
}

// startGate starts the gate gateID as AcquireConcurrency will, after a
// previous execution that ended as previous; it returns the execution's id.
func (s *gateStack) startGate(t *testing.T, gateID, previous string, tun engine.GateTunables) string {
	t.Helper()
	must.OKf(t, tun.Check(), "the test's gate tunables")
	run, err := s.c.ExecuteWorkflow(s.ctx, client.StartWorkflowOptions{ID: gateID, TaskQueue: s.gateQueue},
		engine.ConcurrencyGateWorkflowType, engine.GateStart{
			GateID: gateID, Namespace: gateNamespace, Key: "deploy", Tunables: tun, Previous: previous,
		})
	must.OKf(t, err, "start gate %s", gateID)
	// Its first workflow task completes before any acquire is sent, as the
	// history shows, so every repeat starts down the same path.
	s.poll(t, gateID+"'s first workflow task", func() (bool, error) {
		return s.historyHas(t, gateID, run.GetRunID(), enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED), nil
	})
	return run.GetRunID()
}

// acquire sends the acquire Update and returns the gate's answer.
func (s *gateStack) acquire(gateID string, req engine.GateAcquireForTest) (engine.GateAnswerForTest, error) {
	var ans engine.GateAnswerForTest
	h, err := s.c.UpdateWorkflow(s.ctx, client.UpdateWorkflowOptions{
		WorkflowID: gateID, UpdateName: engine.GateUpdateAcquire, Args: []any{req},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return ans, err
	}
	return ans, h.Get(s.ctx, &ans)
}

// wantAnswer acquires and fails unless the gate answers want.
func (s *gateStack) wantAnswer(t *testing.T, gateID string, req engine.GateAcquireForTest, want string) engine.GateAnswerForTest {
	t.Helper()
	ans, err := s.acquire(gateID, req)
	must.OKf(t, err, "acquire for %s at %s", req.Run, gateID)
	if ans.Answer != want {
		t.Fatalf("acquire for %s (task %d, seq_base %d) answered %+v, want %s", req.Run, req.TaskIx, req.SeqBase, ans, want)
	}
	return ans
}

func (s *gateStack) signal(t *testing.T, gateID, name string, arg any) {
	t.Helper()
	must.OKf(t, s.c.SignalWorkflow(s.ctx, gateID, "", name, arg), "signal %s to %s", name, gateID)
}

func (s *gateStack) release(t *testing.T, gateID, run string, seqBase int64, taskIx int) {
	t.Helper()
	s.signal(t, gateID, engine.GateSignalRelease, engine.GateRelease{Run: run, SeqBase: seqBase, TaskIx: taskIx})
}

// state is the gate's current execution's state.
func (s *gateStack) state(t *testing.T, gateID string) engine.GateStateView {
	t.Helper()
	v, err := s.c.QueryWorkflow(s.ctx, gateID, "", engine.GateQueryState)
	must.OKf(t, err, "query gate %s", gateID)
	var view engine.GateStateView
	must.OKf(t, v.Get(&view), "decode gate %s's state", gateID)
	return view
}

// waitState waits until the gate's state satisfies ok.
func (s *gateStack) waitState(t *testing.T, gateID, what string, ok func(engine.GateStateView) bool) engine.GateStateView {
	t.Helper()
	var last engine.GateStateView
	defer func() {
		if t.Failed() {
			t.Logf("gate %s's last state: %+v", gateID, last.State)
		}
	}()
	s.poll(t, what, func() (bool, error) {
		v, err := s.c.QueryWorkflow(s.ctx, gateID, "", engine.GateQueryState)
		if err != nil {
			return false, err
		}
		// A fresh value each time: a field the gate omits when empty must
		// not keep what an earlier answer said.
		var view engine.GateStateView
		if err := v.Get(&view); err != nil {
			return false, err
		}
		last = view
		return ok(last), nil
	})
	return last
}

// holdsOnly reports that run alone holds, its grant delivered.
func holdsOnly(run string) func(engine.GateStateView) bool {
	return func(v engine.GateStateView) bool {
		h := v.State.Holders
		return len(h) == 1 && h[0].Run == run && !h[0].GrantPending
	}
}

func isEmpty(v engine.GateStateView) bool {
	return len(v.State.Holders) == 0 && len(v.State.Queue) == 0
}

// grants is how many grant signals run received.
func (s *gateStack) grants(t *testing.T, run string) int {
	t.Helper()
	n := 0
	for _, e := range s.history(t, run, "").GetEvents() {
		if a := e.GetWorkflowExecutionSignaledEventAttributes(); a != nil && a.GetSignalName() == engine.ConcurrencyGrantSignal {
			n++
		}
	}
	return n
}

// closeEmpty waits for the gate's chain to close and fails unless it closed
// empty.
func (s *gateStack) closeEmpty(t *testing.T, gateID string) engine.GateResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	var res engine.GateResult
	must.OKf(t, s.c.GetWorkflow(ctx, gateID, "").Get(ctx, &res), "gate %s's close", gateID)
	if res.Closed != "empty" {
		t.Fatalf("gate %s closed %+v, want empty", gateID, res)
	}
	return res
}

// waitContinued waits until the gate's chain has moved past execution first.
func (s *gateStack) waitContinued(t *testing.T, gateID, first string) string {
	t.Helper()
	var next string
	s.poll(t, gateID+" carrying its state on", func() (bool, error) {
		d, err := describeExecution(s.c, s.ctx, gateID, "")
		if err != nil {
			return false, err
		}
		next = d.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		return next != first, nil
	})
	return next
}

// history is one execution's whole history.
func (s *gateStack) history(t *testing.T, id, execution string) *historypb.History {
	t.Helper()
	h := &historypb.History{}
	iter := s.c.GetWorkflowHistory(s.ctx, id, execution, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		e, err := iter.Next()
		must.OKf(t, err, "read %s/%s's history", id, execution)
		h.Events = append(h.Events, e)
	}
	return h
}

// historyHas reports that execution's history holds an event of type et.
func (s *gateStack) historyHas(t *testing.T, id, execution string, et enums.EventType) bool {
	t.Helper()
	for _, e := range s.history(t, id, execution).GetEvents() {
		if e.GetEventType() == et {
			return true
		}
	}
	return false
}

// requireNoActivityTasks fails if the history scheduled an activity on a
// task queue: the gate's and census's reads are local activities, which
// never leave the engine worker, so no Orker polling ork-tasks can take one.
func requireNoActivityTasks(t *testing.T, name string, h *historypb.History) int {
	t.Helper()
	markers := 0
	for _, e := range h.GetEvents() {
		switch e.GetEventType() {
		case enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			t.Fatalf("%s scheduled activity %s on queue %s", name,
				e.GetActivityTaskScheduledEventAttributes().GetActivityType().GetName(),
				e.GetActivityTaskScheduledEventAttributes().GetTaskQueue().GetName())
		case enums.EVENT_TYPE_MARKER_RECORDED:
			markers++
		}
	}
	t.Logf("%s: %d events, %d local-activity markers, no activity task", name, len(h.GetEvents()), markers)
	return markers
}

// recordConcurrencyFixture replays a recorded history against current code,
// holds it to never scheduling an activity task, and writes it under the
// proof directory, and, with ORK_RECORD_GOLDEN_HISTORY=1, over the
// committed fixture.
func recordConcurrencyFixture(t *testing.T, name string, h *historypb.History) {
	t.Helper()
	requireNoActivityTasks(t, name, h)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.ConcurrencyGateWorkflow, workflow.RegisterOptions{Name: engine.ConcurrencyGateWorkflowType})
	replayer.RegisterWorkflowWithOptions(engine.ConcurrencyCensusWorkflow, workflow.RegisterOptions{Name: engine.ConcurrencyCensusWorkflowType})
	must.OKf(t, replayer.ReplayWorkflowHistory(nil, h), "%s does not replay against current code", name)
	encoded := encodeHistory(t, h)
	proof.Write(t, "concurrency-fixtures", name, encoded)
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
		path := filepath.Join("testdata", "history", name)
		// A committed fixture is never re-recorded by a run that records a
		// new one: removing it first is how one is re-recorded, on purpose
		// (testdata/history/README.md).
		if _, err := os.Stat(path); err == nil {
			written = "the proof directory only (" + path + " is committed and kept)"
		} else {
			must.OKf(t, os.WriteFile(path, encoded, 0o644), "write %s", path)
			written = path
		}
	}
	t.Logf("recorded %d events into %s, replayed clean", len(h.GetEvents()), written)
}

// lastTimerFired reports that the history's latest timer has fired.
func lastTimerFired(h *historypb.History) bool {
	var started int64
	fired := map[int64]bool{}
	for _, e := range h.GetEvents() {
		switch e.GetEventType() {
		case enums.EVENT_TYPE_TIMER_STARTED:
			started = e.GetEventId()
		case enums.EVENT_TYPE_TIMER_FIRED:
			fired[e.GetTimerFiredEventAttributes().GetStartedEventId()] = true
		}
	}
	return started != 0 && fired[started]
}

// saturateSignals sends run signals until the cluster refuses one for the
// execution's signal limit, so a later signal to it fails for a reason other
// than the run being gone. It fails the test if no refusal comes.
func (s *gateStack) saturateSignals(t *testing.T, run string) {
	t.Helper()
	const most = 60_000
	var (
		mu      sync.Mutex
		sent    int
		refused error
	)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if refused != nil || sent >= most {
					mu.Unlock()
					return
				}
				sent++
				mu.Unlock()
				err := s.c.SignalWorkflow(s.ctx, run, "", "ork-test-fill", nil)
				var exhausted *serviceerror.ResourceExhausted
				if errors.As(err, &exhausted) {
					// A rate limit, not the execution's own: send again.
					continue
				}
				if err != nil {
					mu.Lock()
					if refused == nil {
						refused = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if refused == nil {
		t.Fatalf("the cluster took %d signals to %s and refused none: this scenario needs its per-execution signal limit", sent, run)
	}
	if msg := strings.ToLower(refused.Error()); !strings.Contains(msg, "signal") || !strings.Contains(msg, "limit") {
		t.Fatalf("a signal to %s was refused for another reason: %v", run, refused)
	}
	// The refusal stands: the execution is at its own limit, not a
	// moment's.
	if err := s.c.SignalWorkflow(s.ctx, run, "", "ork-test-fill", nil); err == nil {
		t.Fatalf("a signal to %s was taken after the cluster refused one (%v): the refusal was not the execution's limit", run, refused)
	}
	t.Logf("the cluster refused a signal to %s after about %d: %v", run, sent, refused)
}

// waitIdle waits until the gate has no workflow task pending, so a signal or
// an update sent next is decided in a workflow task of its own on every
// repeat.
func (s *gateStack) waitIdle(t *testing.T, gateID string) {
	t.Helper()
	s.poll(t, gateID+" idle", func() (bool, error) {
		d, err := s.c.DescribeWorkflow(s.ctx, gateID, "")
		if err != nil {
			return false, err
		}
		return d.RawResponse.GetPendingWorkflowTask() == nil, nil
	})
}

// waitGrantsDelivered waits until the gate has recorded n grant signals
// delivered.
func (s *gateStack) waitGrantsDelivered(t *testing.T, gate string, n int) {
	t.Helper()
	s.poll(t, fmt.Sprintf("%s's %d grants delivered", gate, n), func() (bool, error) {
		got := 0
		for _, e := range s.history(t, gate, "").GetEvents() {
			if e.GetEventType() == enums.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
				got++
			}
		}
		return got >= n, nil
	})
}
