//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/listfilter"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/webhooktest"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// The run side of E88 P3's run-level concurrency limit, on a real cluster, a
// real gate and real object storage: what a paused run does with its slot,
// what a failing run does to get one back for its handlers, and what a
// queued run looks like through the API and the webhooks. The recorders of
// concurrency_run_record_integration_test.go own the paths a recording
// covers (granted, queued, requeued, canceled, timed out, continuing, emit
// failure); none is redone here.

// concRun is a canStack with the gate's activities, the index attributes a
// limit needs, and the helpers every scenario here uses.
type concRun struct {
	*canStack
	ctx   context.Context
	holds *holdGate
	stamp string
}

// newConcRun builds the stack and registers the attributes a run with a
// limit upserts. behavior is the Orker's (the holdGate's when nil).
func newConcRun(t *testing.T, ctx context.Context, label string, behavior func(holds *holdGate) canBehavior) *concRun {
	t.Helper()
	gateClient := dialVersioningCluster(t, ctx, versioningNamespace())
	holds := newHoldGate()
	run := holds.behavior
	if behavior != nil {
		run = behavior(holds)
	}
	s := startCANStackWith(t, ctx, label, true, run, nil, nil, func(w worker.Worker) {
		engine.RegisterGateActivities(w, engine.NewGateActivities(gateClient))
	})
	indexed, err := task.EnsureRunSearchAttributes(ctx, s.client, s.namespace)
	must.OKf(t, err, "register the namespace attribute in %s", s.namespace)
	if !indexed {
		t.Fatalf("the namespace attribute is not usable in %s", s.namespace)
	}
	usable, err := task.EnsureConcurrencySearchAttributes(ctx, s.client, s.namespace)
	must.OKf(t, err, "register the concurrency attributes in %s", s.namespace)
	if !usable {
		t.Fatalf("the concurrency attributes are not usable in %s", s.namespace)
	}
	return &concRun{canStack: s, ctx: ctx, holds: holds, stamp: strconv.FormatInt(time.Now().UnixNano(), 36)}
}

// id is a run id of at most the 48 bytes a gate carries.
func (c *concRun) id(label string) string {
	id := "ork-" + label + "-" + c.stamp
	if len(id) > 48 {
		c.t.Fatalf("test run id %s is over the 48 bytes a gate carries", id)
	}
	return id
}

// key is a limit key no other test or round shares: gates live in the
// process's namespace.
func (c *concRun) key(label string) string { return label + "-" + c.stamp }

func (c *concRun) gate(key string) string { return engine.GateID("default", "", key) }

// pausable is the plan of a run under a limit of one on key that a pause
// may reach (the numbers of pausePlan, pause_internal_test.go).
func (c *concRun) pausable(key string) engine.RunPlan {
	plan := concurrencyRunPlan(c.canStack, key, 120, time.Minute, nil, nil)
	plan.Pause = &engine.PausePlan{DefaultTimeoutSeconds: 7 * 24 * 3600, MaxTimeoutSeconds: 31 * 24 * 3600,
		ApplyReserveEvents: 20, CascadeEventsPerChild: 4, InheritedGraceSeconds: 600, InheritedGraceMaxLevels: 8}
	return plan
}

func (c *concRun) start(id, document string, plan engine.RunPlan) {
	c.t.Helper()
	must.OKf(c.t, c.starter.StartRun(c.ctx, id, compile(c.t, document), nil, plan), "StartRun %s", id)
}

func (c *concRun) end(id string) error { return c.client.GetWorkflow(c.ctx, id, "").Get(c.ctx, nil) }

func (c *concRun) mustEnd(id string) {
	c.t.Helper()
	must.OKf(c.t, c.end(id), "run %s", id)
}

func (c *concRun) waitFor(what string, ok func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for !ok() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s", what)
		}
		select {
		case <-c.ctx.Done():
			c.t.Fatalf("waiting for %s: %v", what, c.ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// signalPause sends a pause (or an unpause) request, as the API's does.
func (c *concRun) signalPause(id string, pause bool, request string) {
	c.t.Helper()
	must.OKf(c.t, c.client.SignalWorkflow(c.ctx, id, "", engine.PauseSignal, map[string]any{
		"format": 1, "pause": pause, "principal": "alice", "reason": "db maintenance",
		"requestId": request, "timeoutSeconds": "0"}), "signal pause=%v to %s", pause, id)
}

// gateView is the gate's state, or false while the gate cannot be asked
// (not started yet, or closed after its linger).
func (c *concRun) gateView(gate string) (engine.GateStateView, bool) {
	v, err := c.client.QueryWorkflow(c.ctx, gate, "", engine.GateQueryState)
	if err != nil {
		return engine.GateStateView{}, false
	}
	var view engine.GateStateView
	if err := v.Get(&view); err != nil {
		return engine.GateStateView{}, false
	}
	return view, true
}

func (c *concRun) waitGate(gate, what string, ok func(engine.GateStateView) bool) engine.GateStateView {
	c.t.Helper()
	var last engine.GateStateView
	c.waitFor(what, func() bool {
		v, answered := c.gateView(gate)
		if !answered {
			return false
		}
		last = v
		return ok(v)
	})
	return last
}

// holders and waiting are the runs a gate holds and queues.
func holders(v engine.GateStateView) []string {
	var out []string
	for _, h := range v.State.Holders {
		out = append(out, h.Run)
	}
	return out
}

func waiting(v engine.GateStateView) []string {
	var out []string
	for _, q := range v.State.Queue {
		out = append(out, q.Run)
	}
	return out
}

// gateFault is what a gate's state may never show: more holders than the
// limit, or one run twice (held and queued, or twice in either).
func gateFault(v engine.GateStateView) string {
	if len(v.State.Holders) > 1 {
		return fmt.Sprintf("holders %v exceed the limit of one", holders(v))
	}
	seen := map[string]string{}
	for where, runs := range map[string][]string{"holder": holders(v), "queue": waiting(v)} {
		for _, r := range runs {
			if prior, dup := seen[r]; dup {
				return fmt.Sprintf("run %s is a %s and a %s", r, prior, where)
			}
			seen[r] = where
		}
	}
	return ""
}

// events are the run's recorded events, read as any replica reads them.
func (c *concRun) events(id string) []*orkstackv1.Event {
	c.t.Helper()
	events, err := c.reader.Events(c.ctx, id, 0)
	must.OKf(c.t, err, "read the events of %s", id)
	return events
}

func (c *concRun) countEvents(id string, typ orkstackv1.EventType, contains string) int {
	n := 0
	for _, e := range c.events(id) {
		if e.GetType() == typ && strings.Contains(e.GetMessage(), contains) {
			n++
		}
	}
	return n
}

func (c *concRun) open(id string) bool {
	d, err := describeExecution(c.client, c.ctx, id, "")
	return err == nil && d.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// TestIntegration_ConcurrencyPausedRunGivesItsSlotToTheLine: a paused run
// that is quiet gives its slot back, so the run queued behind it is granted
// while it stays paused; on unpause it waits at the back of the line
// (RUN_QUEUED, cause UNPAUSE, still RUNNING) and is granted once the other
// run releases. Both succeed.
func TestIntegration_ConcurrencyPausedRunGivesItsSlotToTheLine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newConcRun(t, ctx, "conc-pause", nil)
	key := s.key("pause")
	gate := s.gate(key)
	a, b := s.id("pa"), s.id("pb")

	// A holds and is paused with its task in flight: not quiet, so it keeps
	// its slot.
	s.start(a, concurrencyDocument("conc-pause", "hold", "after"), s.pausable(key))
	s.waitFor(a+" holding", func() bool { return s.holds.holding(a) })
	s.signalPause(a, true, "pause-1")
	s.waitFor(a+" paused", func() bool { return s.countEvents(a, orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, "") == 1 })
	if v, ok := s.gateView(gate); !ok || !slices.Equal(holders(v), []string{a}) {
		t.Fatalf("paused with a task in flight, run %s gave its slot back: %v", a, holders(v))
	}

	// B queues behind it.
	s.start(b, concurrencyDocument("conc-pause", "hold"), s.pausable(key))
	s.waitGate(gate, b+" queued behind "+a, func(v engine.GateStateView) bool { return slices.Equal(waiting(v), []string{b}) })

	// A's task ends; A is paused and quiet, so B is granted while A is paused.
	s.holds.let(a)
	s.waitGate(gate, b+" granted while "+a+" is paused", func(v engine.GateStateView) bool {
		return slices.Equal(holders(v), []string{b}) && len(waiting(v)) == 0
	})
	s.waitFor(b+" holding", func() bool { return s.holds.holding(b) })
	if !s.open(a) {
		t.Fatalf("run %s ended while paused", a)
	}

	// Unpause: A waits at the back of the line, RUNNING, queued by an unpause.
	s.signalPause(a, false, "unpause-1")
	s.waitGate(gate, a+" in line again", func(v engine.GateStateView) bool { return slices.Equal(waiting(v), []string{a}) })
	s.waitFor(a+"'s RUN_QUEUED", func() bool {
		return s.countEvents(a, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "after an unpause") == 1
	})
	run, err := s.reader.GetRun(ctx, a)
	must.OKf(t, err, "GetRun %s", a)
	if run.GetStatus() != orkstackv1.Status_STATUS_RUNNING ||
		run.GetQueued().GetCause() != orkstackv1.ConcurrencyQueueCause_CONCURRENCY_QUEUE_CAUSE_UNPAUSE {
		t.Fatalf("run %s is %s, queued %v; want RUNNING queued by an unpause", a, run.GetStatus(), run.GetQueued())
	}
	if v, ok := s.gateView(gate); !ok || !slices.Equal(holders(v), []string{b}) {
		t.Fatalf("B no longer holds while A waits: %v", holders(v))
	}

	// B releases; A is granted and both succeed.
	s.holds.let(b)
	s.mustEnd(b)
	s.mustEnd(a)
	s.waitGate(gate, "the gate empty", isEmpty)
	// Its one wait after the unpause is the only one that names an unpause
	// (a run's first acquire on a gate that has just started also queues).
	if got := s.countEvents(a, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "after an unpause"); got != 1 {
		t.Fatalf("run %s published %d RUN_QUEUED events for its wait after the unpause, want 1", a, got)
	}
}

// TestIntegration_ConcurrencyGrantRacesPause: a grant that reaches a run as
// its pause takes it out of the line is ignored, and the slot is never held
// twice. In each round a re-queued run waits behind a holder whose release
// and the waiter's pause are sent together; a sampler reads the gate's state
// throughout, and the line must empty with every run still able to finish.
func TestIntegration_ConcurrencyGrantRacesPause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newConcRun(t, ctx, "conc-race", nil)

	for round := 0; round < 3; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) {
			key := s.key("race" + strconv.Itoa(round))
			gate := s.gate(key)
			p, h := s.id("rp"+strconv.Itoa(round)), s.id("rh"+strconv.Itoa(round))
			req := func(what string) string { return fmt.Sprintf("%s-%d", what, round) }

			// P takes the slot, is paused, finishes its task and gives the slot
			// back; H takes it; P is unpaused and waits behind H.
			s.start(p, concurrencyDocument("conc-race", "hold", "after"), s.pausable(key))
			s.waitFor(p+" holding", func() bool { return s.holds.holding(p) })
			s.signalPause(p, true, req("pause-a"))
			s.waitFor(p+" paused", func() bool { return s.countEvents(p, orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, "") == 1 })
			s.holds.let(p)
			s.waitGate(gate, p+" gave its slot back", func(v engine.GateStateView) bool { return len(holders(v)) == 0 })
			s.start(h, concurrencyDocument("conc-race", "hold"), s.pausable(key))
			s.waitFor(h+" holding", func() bool { return s.holds.holding(h) })
			s.signalPause(p, false, req("unpause-a"))
			s.waitGate(gate, p+" re-queued behind "+h, func(v engine.GateStateView) bool { return slices.Equal(waiting(v), []string{p}) })

			// A sampler reads the gate until the round is over.
			stop := make(chan struct{})
			var wg sync.WaitGroup
			var mu sync.Mutex
			var faults []string
			samples := 0
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if v, ok := s.gateView(gate); ok {
						mu.Lock()
						samples++
						if f := gateFault(v); f != "" {
							faults = append(faults, f)
						}
						mu.Unlock()
					}
					select {
					case <-stop:
						return
					case <-time.After(20 * time.Millisecond):
					}
				}
			}()

			// The race: H's release and P's pause, started together.
			var start sync.WaitGroup
			start.Add(1)
			var race sync.WaitGroup
			race.Add(2)
			go func() { defer race.Done(); start.Wait(); s.holds.let(h) }()
			go func() { defer race.Done(); start.Wait(); s.signalPause(p, true, req("pause-b")) }()
			start.Done()
			race.Wait()

			// Whichever won, H ends and P is paused again with nothing held:
			// either it was granted and gave the slot back (quiet), or it left
			// the line.
			s.mustEnd(h)
			s.waitFor(p+" paused again", func() bool { return s.countEvents(p, orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, "") == 2 })
			s.waitGate(gate, "the line empty with "+p+" paused", isEmpty)
			if !s.open(p) {
				t.Fatalf("run %s ended while paused", p)
			}

			// It is still a run the gate can serve: unpaused, it is granted at
			// once and succeeds, and the gate drains.
			s.signalPause(p, false, req("unpause-b"))
			s.mustEnd(p)
			s.waitGate(gate, "the gate empty at the end", isEmpty)
			close(stop)
			wg.Wait()
			mu.Lock()
			defer mu.Unlock()
			if samples == 0 {
				t.Fatalf("the sampler never read the gate's state")
			}
			if len(faults) > 0 {
				t.Fatalf("the gate's state broke the limit (%d samples): %v", samples, faults)
			}
		})
	}
}

// TestIntegration_ConcurrencyFailingRunRegainsItsSlotForItsHandlers: a run
// paused and quiet gives its slot back; its await expires while it is
// paused, so it is failing; on unpause it regains the slot before its failure
// handler starts, runs it, and ends FAILED with the await's own error.
func TestIntegration_ConcurrencyFailingRunRegainsItsSlotForItsHandlers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	var mu sync.Mutex
	var handlerRuns []string // who held the gate each time the handler ran
	var gateOf string
	s := newConcRun(t, ctx, "conc-fail", func(holds *holdGate) canBehavior {
		return func(actx context.Context, st *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			if in.GetTaskId() == "cleanup" {
				mu.Lock()
				gate := gateOf
				mu.Unlock()
				held := "unreadable"
				if v, err := st.client.QueryWorkflow(actx, gate, "", engine.GateQueryState); err == nil {
					var view engine.GateStateView
					if v.Get(&view) == nil {
						held = strings.Join(holders(view), ",")
					}
				}
				mu.Lock()
				handlerRuns = append(handlerRuns, held)
				mu.Unlock()
			}
			return holds.behavior(actx, st, in)
		}
	})
	key := s.key("fail")
	gate := s.gate(key)
	mu.Lock()
	gateOf = gate
	mu.Unlock()
	id := s.id("f")

	const document = `apiVersion: orkstack/v1
kind: Orkflow
name: conc-failing
version: "1.0.0"
concurrency:
  limit: 1
tasks:
  - id: approve
    await: {timeout_seconds: 20}
  - id: cleanup
    uses: shell
    needs: [approve]
    join: any_failed
    with: {op: exec}
`
	s.start(id, document, s.pausable(key))
	s.waitFor("the await to open", func() bool {
		return s.countEvents(id, orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING, "") == 1
	})
	s.waitGate(gate, id+" holding", func(v engine.GateStateView) bool { return slices.Equal(holders(v), []string{id}) })

	// Paused with only an await open, the run is quiet and gives the slot back.
	s.signalPause(id, true, "pause-f")
	s.waitGate(gate, id+" gave its slot back", func(v engine.GateStateView) bool { return len(holders(v)) == 0 })

	// The await expires while the run is paused: it is failing, holds no
	// slot, and its handler has not started.
	s.waitFor("the await to expire", func() bool {
		for _, e := range s.events(id) {
			if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_FAILED && e.GetTaskId() == "approve" {
				return true
			}
		}
		return false
	})
	if v, ok := s.gateView(gate); !ok || len(holders(v)) != 0 {
		t.Fatalf("the failing paused run holds the slot: %v", holders(v))
	}
	mu.Lock()
	ran := len(handlerRuns)
	mu.Unlock()
	if ran != 0 || !s.open(id) {
		t.Fatalf("handler ran %d times or the run ended while paused", ran)
	}

	// Unpaused, it regains the slot, runs the handler under it, and fails
	// with the await's own error.
	s.signalPause(id, false, "unpause-f")
	err := s.end(id)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.AwaitTimedOutCode {
		t.Fatalf("run %s ended %v, want %s", id, err, engine.AwaitTimedOutCode)
	}
	mu.Lock()
	got := slices.Clone(handlerRuns)
	mu.Unlock()
	if len(got) != 1 || got[0] != id {
		t.Fatalf("the handler ran under holders %q, want exactly once with only %s holding", got, id)
	}
	s.waitGate(gate, "the gate empty", isEmpty)
}

// concAPI is the API over a stack, wired as cmd/ork wires it: the reader and
// starter answer the namespace and concurrency indexes, and the service
// reads gates through a concurrency reader.
type concAPI struct {
	svc   *api.Service
	stats *api.StatsHandler
}

func newConcAPI(s *concRun) *concAPI {
	s.reader.WithNamespaceIndex(true)
	svc := api.NewService(api.Config{
		Reader:            s.reader,
		Runs:              s.runs,
		Bus:               s.bus,
		Archive:           s.archive,
		Starter:           s.starter,
		ConcurrencyReader: engine.NewTemporalConcurrencyReader(s.client, 10*time.Second),
	})
	return &concAPI{svc: svc, stats: api.NewStatsHandler(svc, 0)}
}

func (a *concAPI) list(t *testing.T, filter string) []string {
	t.Helper()
	f, err := listfilter.Parse(filter)
	must.OKf(t, err, "parse %q", filter)
	resp, err := a.svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{
		Filters: []*orkstackv1.FieldFilter{f}}))
	must.OKf(t, err, "ListRuns %s", filter)
	var ids []string
	for _, r := range resp.Msg.GetRuns() {
		ids = append(ids, r.GetId())
	}
	slices.Sort(ids)
	return ids
}

// TestIntegration_ConcurrencyListAndStatsShowTheLine: through the API as
// cmd/ork wires it, a run in line and the run it waits for are told apart by
// ListRuns' queued=, queued_on= and holding=, counted by GetStats, and the
// waiting one's place is read live by GetRun; when both end, nothing matches.
func TestIntegration_ConcurrencyListAndStatsShowTheLine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := newConcRun(t, ctx, "conc-list", nil)
	// A starter that answers the boot questions as cmd/ork's does: both
	// indexes are registered in the stack's namespace.
	s.starter = s.starter.WithNamespaceIndex(true).WithConcurrencyIndex(true)
	a := newConcAPI(s)
	key := s.key("list")
	gate := s.gate(key)
	h, q := s.id("lh"), s.id("lq")

	s.start(h, concurrencyDocument("conc-list", "hold"), concurrencyRunPlan(s.canStack, key, 120, time.Minute, nil, nil))
	s.waitFor(h+" holding", func() bool { return s.holds.holding(h) })
	s.start(q, concurrencyDocument("conc-list", "a"), concurrencyRunPlan(s.canStack, key, 120, time.Minute, nil, nil))
	s.waitGate(gate, q+" queued", func(v engine.GateStateView) bool { return slices.Equal(waiting(v), []string{q}) })

	// The cluster's index of open runs is eventually consistent.
	expect := func(what string, ask func() []string, want []string) {
		t.Helper()
		var got []string
		s.waitFor(what, func() bool { got = ask(); return slices.Equal(got, want) })
	}
	expect("queued=true to list only "+q, func() []string { return a.list(t, "queued=true") }, []string{q})
	expect("holding=key to list only "+h, func() []string { return a.list(t, "holding="+key) }, []string{h})
	expect("queued_on=key to list only "+q, func() []string { return a.list(t, "queued_on="+key) }, []string{q})

	var stats *orkstackv1.GetStatsResponse
	s.waitFor("stats to count one queued and one executing", func() bool {
		resp, err := a.stats.GetStats(ctx, connect.NewRequest(&orkstackv1.GetStatsRequest{}))
		must.OKf(t, err, "GetStats")
		stats = resp.Msg
		return stats.GetQueued() == 1 && stats.GetExecuting() == 1
	})

	got, err := a.svc.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: q}))
	must.OKf(t, err, "GetRun %s", q)
	if queued := got.Msg.GetRun().GetQueued(); queued.GetKey() != key || !queued.GetPositionKnown() || queued.GetPosition() != 1 {
		t.Fatalf("GetRun %s: queued %v, want key %s at a known position 1", q, queued, key)
	}

	s.holds.let(h)
	s.mustEnd(h)
	s.mustEnd(q)
	for _, filter := range []string{"queued=true", "holding=" + key, "queued_on=" + key} {
		expect(filter+" to list nothing", func() []string { return a.list(t, filter) }, nil)
	}
}

// TestIntegration_WebhookRunQueued: a webhook subscribed to RUN_QUEUED gets
// one delivery for the run that waited for its slot, and a webhook that
// names no events, which means the run transitions only, gets none. Only
// RUN_QUEUED is tested: TASK_QUEUED is emitted by a task-level limit, which
// is P7's, not by a run-level one.
func TestIntegration_WebhookRunQueued(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	subscriber := newWebhookSubscriber(t)
	registry := webhooks.NewRegistry(webhooks.WithDestinationPolicy(webhooktest.LoopbackPolicy()))
	notifier := webhooks.NewNotifier(registry, webhooks.NewDispatcher(webhooks.DispatcherOptions{Policy: webhooktest.LoopbackPolicy()}),
		webhooks.NotifierOptions{})
	notifier.Observe(subscriber.record)
	canStackRunNotifier = api.NewWebhookNotifier(notifier, nil)
	s := newConcRun(t, ctx, "conc-hook", nil)
	canStackRunNotifier = nil

	queuedHook, err := registry.Register(webhooks.Registration{URL: subscriber.server.URL,
		Events: []webhooks.Event{webhooks.EventRunQueued}})
	must.OKf(t, err, "register the RUN_QUEUED webhook")
	plainHook, err := registry.Register(webhooks.Registration{URL: subscriber.server.URL})
	must.OKf(t, err, "register the webhook with no events")

	key := s.key("hook")
	gate := s.gate(key)
	h, q := s.id("wh"), s.id("wq")
	s.start(h, concurrencyDocument("conc-hook", "hold"), concurrencyRunPlan(s.canStack, key, 120, time.Minute, nil, nil))
	s.waitFor(h+" holding", func() bool { return s.holds.holding(h) })
	s.start(q, concurrencyDocument("conc-hook", "a"), concurrencyRunPlan(s.canStack, key, 120, time.Minute, nil, nil))
	s.waitGate(gate, q+" queued", func(v engine.GateStateView) bool { return slices.Equal(waiting(v), []string{q}) })
	s.holds.let(h)
	s.mustEnd(h)
	s.mustEnd(q)

	// The plain webhook's RUN_STARTED and RUN_SUCCEEDED for both runs are
	// the progress that proves a missing RUN_QUEUED is not a slow delivery.
	forWebhook := func(runID, hookID string) []string {
		var events []string
		for _, d := range subscriber.deliveries(runID) {
			if d.webhookID == hookID {
				events = append(events, d.event)
			}
		}
		return events
	}
	s.waitFor("the deliveries to arrive", func() bool {
		return len(forWebhook(q, queuedHook.ID)) >= 1 && len(forWebhook(q, plainHook.ID)) >= 2 && len(forWebhook(h, plainHook.ID)) >= 2
	})
	drain, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	defer drainCancel()
	must.OKf(t, notifier.Wait(drain), "deliveries did not settle")

	// One delivery per RUN_QUEUED the run published: the queued run waited
	// behind the holder, so it published at least one, and it is told once. (A
	// run whose key had no gate yet may also queue for an instant before its
	// first grant; its deliveries match its events either way.)
	for _, run := range []string{h, q} {
		want := s.countEvents(run, orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED, "")
		if run == q && want != 1 {
			t.Fatalf("run %s published %d RUN_QUEUED events, want 1", run, want)
		}
		got := forWebhook(run, queuedHook.ID)
		if len(got) != want || slices.ContainsFunc(got, func(e string) bool { return e != "RUN_QUEUED" }) {
			t.Fatalf("the RUN_QUEUED webhook got %v for %s, want %d RUN_QUEUED deliveries", got, run, want)
		}
	}
	for _, run := range []string{h, q} {
		got := forWebhook(run, plainHook.ID)
		slices.Sort(got)
		if !slices.Equal(got, []string{"RUN_STARTED", "RUN_SUCCEEDED"}) {
			t.Fatalf("the webhook with no events got %v for %s, want its run transitions and no RUN_QUEUED", got, run)
		}
	}
}
