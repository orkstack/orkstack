//go:build integration

package engine_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIntegration_ConcurrencyCensusScheduleEnsuredOnce: a boot creates the
// census schedule, a second boot with the same settings leaves it alone, a
// boot with other settings updates it, and its note then holds their digest.
func TestIntegration_ConcurrencyCensusScheduleEnsuredOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "census-once")
	t.Cleanup(func() {
		_ = s.c.ScheduleClient().GetHandle(context.Background(), engine.ConcurrencyCensusScheduleID).Delete(context.Background())
	})
	settings := engine.CensusSettings{Interval: time.Hour, Timeout: time.Minute, MaxGates: 10, Parallel: 2}
	changed := settings
	changed.Interval = 2 * time.Hour
	for i, step := range []struct {
		settings engine.CensusSettings
		want     engine.CensusEnsureOutcome
	}{
		{settings, engine.CensusCreated},
		{settings, engine.CensusUnchanged},
		{changed, engine.CensusUpdated},
		{changed, engine.CensusUnchanged},
	} {
		got, err := engine.EnsureCensusSchedule(ctx, s.c, s.gateQueue, step.settings)
		must.OKf(t, err, "boot %d", i+1)
		if got != step.want {
			t.Fatalf("boot %d ensured %s, want %s", i+1, got, step.want)
		}
	}
	desc, err := s.c.ScheduleClient().GetHandle(ctx, engine.ConcurrencyCensusScheduleID).Describe(ctx)
	must.OKf(t, err, "describe the census schedule")
	digest, err := changed.Digest()
	must.OKf(t, err, "digest")
	if note := desc.Schedule.State.Note; note != "settings_digest="+digest {
		t.Fatalf("the schedule's note is %q, want settings_digest=%s", note, digest)
	}
}

// TestIntegration_ConcurrencyCensusScheduleEnsuredByConcurrentBoots: eight
// replicas booting at once create the schedule exactly once; every other
// one finds it with the same settings, and none fails.
func TestIntegration_ConcurrencyCensusScheduleEnsuredByConcurrentBoots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "census-boots")
	t.Cleanup(func() {
		_ = s.c.ScheduleClient().GetHandle(context.Background(), engine.ConcurrencyCensusScheduleID).Delete(context.Background())
	})
	settings := engine.CensusSettings{Interval: time.Hour, Timeout: time.Minute, MaxGates: 10, Parallel: 2}
	const boots = 8
	outcomes := make([]engine.CensusEnsureOutcome, boots)
	errs := make([]error, boots)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range boots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			outcomes[i], errs[i] = engine.EnsureCensusSchedule(ctx, s.c, s.gateQueue, settings)
		}()
	}
	close(start)
	wg.Wait()
	created := 0
	for i := range boots {
		must.OKf(t, errs[i], "boot %d", i+1)
		switch outcomes[i] {
		case engine.CensusCreated:
			created++
		case engine.CensusUnchanged:
		default:
			t.Fatalf("boot %d ensured %s, want created or unchanged", i+1, outcomes[i])
		}
	}
	if created != 1 {
		t.Fatalf("%d boots created the schedule, want exactly 1 (outcomes %v)", created, outcomes)
	}
}

// TestIntegration_ConcurrencyGateActivitiesNeverReachOrkers: a gate's check
// pass and rebuild, and a census pass, run their reads as local activities
// on the engine worker. An Orker polling the engine's own task queue, as one
// polling ork-tasks does on a default deployment, with activities of those
// very names registered, is never handed one, and no history schedules an
// activity task.
func TestIntegration_ConcurrencyGateActivitiesNeverReachOrkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "never-orkers")
	s.startWorker()
	var reached atomic.Int64
	orker := worker.New(s.c, s.gateQueue, worker.Options{DisableWorkflowWorker: true})
	for _, name := range []string{engine.ActivityDescribeConcurrencyEntries, engine.ActivityListConcurrencyEntries, engine.ActivityRunConcurrencyCensus} {
		orker.RegisterActivityWithOptions(func(context.Context, any) (any, error) {
			reached.Add(1)
			return nil, nil
		}, activity.RegisterOptions{Name: name})
	}
	must.OKf(t, orker.Start(), "start the Orker")
	t.Cleanup(orker.Stop)

	gate := engine.GateID(gateNamespace, "", "never-orkers")
	h, a := gateRunID("h"), gateRunID("a")
	s.startStandIn(t, gate, h, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 2, time.Now().Add(-time.Minute), 1, 1)})
	s.startStandIn(t, gate, a, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 2, time.Now(), 2, 1)})
	s.waitListed(t, gate, 2)
	first := s.startGate(t, gate, engine.GatePreviousAbsent, gateTunablesForTest())
	s.waitState(t, gate, "h and a restored", func(v engine.GateStateView) bool { return len(v.State.Holders) == 2 })
	s.terminate(t, a)
	s.signal(t, gate, engine.GateSignalVerify, nil)
	s.waitState(t, gate, "a released by the pass", holdsOnly(h))
	census, err := s.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: gateRunID("census"), TaskQueue: s.gateQueue},
		engine.ConcurrencyCensusWorkflowType, engine.CensusSettings{Interval: time.Hour, Timeout: 30 * time.Second, MaxGates: 10, Parallel: 2})
	must.OKf(t, err, "start a census")
	must.OKf(t, census.Get(ctx, nil), "the census pass")
	s.release(t, gate, h, 1, gateLevel)
	s.closeEmpty(t, gate)

	// Positive control: both did read the cluster, through local
	// activities.
	if requireNoActivityTasks(t, "the gate", s.history(t, gate, first)) == 0 ||
		requireNoActivityTasks(t, "the census", s.history(t, census.GetID(), census.GetRunID())) == 0 {
		t.Fatal("the gate or the census recorded no local activity, so nothing was there to reach an Orker")
	}
	if n := reached.Load(); n != 0 {
		t.Fatalf("the Orker was handed %d of the gate's or census's activities", n)
	}
}

// TestIntegration_ConcurrencyEngineKilledMidGateContinue: the engine worker
// is stopped as the acquire that crosses gate_continue_updates arrives, and
// a new one takes over. Whichever side of the continue-as-new the stop fell
// on, the gate keeps its holder and its line in order, grants them, and
// closes empty.
func TestIntegration_ConcurrencyEngineKilledMidGateContinue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "killed-continue")
	s.startWorker()
	tun := gateTunablesForTest()
	tun.ContinueUpdates = 4
	// The crossing acquire may wait out a worker's restart: it must not go
	// stale meanwhile.
	tun.AcquireTimeout, tun.Linger = 20*time.Second, 26*time.Second
	gate := engine.GateID(gateNamespace, "", "killed-continue")
	a, b, c, d, e := gateRunID("a"), gateRunID("b"), gateRunID("c"), gateRunID("d"), gateRunID("e")
	s.startStandIn(t, gate, b, 1, nil)
	s.startStandIn(t, gate, c, 1, nil)
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	s.wantAnswer(t, gate, gateAcq(c, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	s.wantAnswer(t, gate, gateAcq(d, 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)

	answered := make(chan error, 1)
	go func() {
		ans, err := s.acquire(gate, gateAcq(e, 1, gateLevel, 1, "refuse"))
		if err == nil && ans.Answer != engine.GateAnswerFullForTest {
			t.Errorf("e answered %+v, want full", ans)
		}
		answered <- err
	}()
	s.stopWorker()
	s.startWorker()
	select {
	case err := <-answered:
		must.OKf(t, err, "the acquire that crosses the threshold")
	case <-ctx.Done():
		t.Fatal("the acquire that crosses the threshold was never answered")
	}
	s.waitContinued(t, gate, first)
	v := s.waitState(t, gate, "a holding and b, c queued after the continue-as-new", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && v.State.Holders[0].Run == a && len(v.State.Queue) == 2 &&
			v.State.Queue[0].Run == b && v.State.Queue[1].Run == c
	})
	if v.State.Seq != 3 || v.State.Epoch != 0 {
		t.Fatalf("the carried state has seq %d, epoch %d; want 3, 0", v.State.Seq, v.State.Epoch)
	}
	for _, next := range []string{b, c} {
		s.release(t, gate, s.state(t, gate).State.Holders[0].Run, 1, gateLevel)
		s.waitState(t, gate, next+" holding", holdsOnly(next))
	}
	s.release(t, gate, c, 1, gateLevel)
	s.closeEmpty(t, gate)
}

// TestIntegration_ConcurrencyGateTerminatedWithQueue: a gate terminated
// with a holder and a line loses its record; the next start, unclean,
// rebuilds both from the runs' own records, in the same order with the same
// arrivals, and grants the line in that order.
func TestIntegration_ConcurrencyGateTerminatedWithQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "terminated-queue")
	s.startWorker()
	tun := gateTunablesForTest()
	gate := engine.GateID(gateNamespace, "", "terminated-queue")
	a, b, c := gateRunID("a"), gateRunID("b"), gateRunID("c")
	s.startGate(t, gate, engine.GatePreviousClean, tun)
	answers := map[string]engine.GateAnswerForTest{
		a: s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest),
		b: s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest),
		c: s.wantAnswer(t, gate, gateAcq(c, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest),
	}
	// Each run records its entry as the gate answered it, as a P3 run does.
	for run, ans := range answers {
		s.startStandIn(t, gate, run, 1, map[int]engine.GateMemoEntryForTest{
			gateLevel: memoEntry(run == a, 1, ans.ArrivedAt, ans.Seq, 1),
		})
	}
	s.waitListed(t, gate, 3)
	must.OKf(t, s.c.TerminateWorkflow(ctx, gate, "", "the test loses the gate's record"), "terminate the gate")

	s.startGate(t, gate, engine.GatePreviousUnclean, tun)
	v := s.waitState(t, gate, "a holding and b, c queued after the rebuild", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && v.State.Holders[0].Run == a && len(v.State.Queue) == 2 &&
			v.State.Queue[0].Run == b && v.State.Queue[1].Run == c && v.State.Rebuild.State == ""
	})
	for i, e := range append(v.State.Holders, v.State.Queue...) {
		want := answers[e.Run]
		if e.Seq != want.Seq || !e.ArrivedAt.Equal(want.ArrivedAt) {
			t.Fatalf("entry %d (%s) rebuilt at seq %d, %s; it arrived at seq %d, %s", i, e.Run, e.Seq, e.ArrivedAt, want.Seq, want.ArrivedAt)
		}
	}
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "b holding", holdsOnly(b))
	s.release(t, gate, b, 1, gateLevel)
	s.waitState(t, gate, "c holding", holdsOnly(c))
	s.release(t, gate, c, 1, gateLevel)
	s.closeEmpty(t, gate)
}

// TestIntegration_ConcurrencyRebuildVisibilityUnavailable: while the
// cluster cannot be read, a rebuilding gate grants nothing, answers refuse
// unavailable, queues a queue arrival and turns failing past
// rebuild_timeout; once it can, it restores the holder ahead of that
// arrival and grants in order.
func TestIntegration_ConcurrencyRebuildVisibilityUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "visibility-down")
	tun := gateTunablesForTest()
	// The gate's first workflow task waits out a list read the proxy
	// stalls, so an acquire sent meanwhile must not go stale.
	tun.AcquireTimeout, tun.Linger = 20*time.Second, 26*time.Second
	proxy := faultproxy.New(t, temporalAddress())
	via, err := client.DialContext(ctx, client.Options{HostPort: proxy.Addr(), Namespace: s.namespace})
	must.OKf(t, err, "dial through the proxy")
	t.Cleanup(via.Close)
	s.activities = via
	s.startWorker()

	gate := engine.GateID(gateNamespace, "", "visibility-down")
	h, x, y := gateRunID("h"), gateRunID("x"), gateRunID("y")
	s.startStandIn(t, gate, h, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, time.Now().Add(-time.Minute), 5, 1)})
	s.startStandIn(t, gate, y, 1, nil)
	s.waitListed(t, gate, 1)
	proxy.Cut()
	s.startGate(t, gate, engine.GatePreviousUnclean, tun)
	s.wantAnswer(t, gate, gateAcq(x, 1, gateLevel, 1, "refuse"), engine.GateAnswerUnavailableForTest)
	s.wantAnswer(t, gate, gateAcq(y, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	s.waitState(t, gate, "the rebuild failing while the cluster cannot be read", func(v engine.GateStateView) bool {
		return v.State.Rebuild.State == engine.GateRebuildFailingForTest && len(v.State.Holders) == 0
	})
	proxy.Restore()
	s.waitState(t, gate, "h restored ahead of y, the settle over", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && v.State.Holders[0].Run == h && len(v.State.Queue) == 1 &&
			v.State.Queue[0].Run == y && v.State.Rebuild.State == ""
	})
	s.release(t, gate, h, 1, gateLevel)
	s.waitState(t, gate, "y holding", holdsOnly(y))
	s.release(t, gate, y, 1, gateLevel)
	s.closeEmpty(t, gate)
}

// TestIntegration_ConcurrencyGateReleaseAtContinueIsNotLost: a release sent
// as the gate drains for a continue-as-new is applied, before the switch or
// after it, never lost. Each round races the release with the acquire that
// crosses gate_continue_updates, and the chain must have continued (the
// race was run) with the released holder gone.
func TestIntegration_ConcurrencyGateReleaseAtContinueIsNotLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "release-continue")
	s.startWorker()
	tun := gateTunablesForTest()
	tun.ContinueUpdates = 4
	for round := range 3 {
		gate := engine.GateID(gateNamespace, "", "release-continue-"+string(rune('a'+round)))
		holder := gateRunID("h")
		first := s.startGate(t, gate, engine.GatePreviousClean, tun)
		s.wantAnswer(t, gate, gateAcq(holder, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
		for range 3 {
			s.wantAnswer(t, gate, gateAcq(gateRunID("f"), 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)
		}
		// The crossing acquire is granted if the release reached the gate
		// first, and then holds; either way the released holder must go.
		crossing := gateRunID("x")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			ans, err := s.acquire(gate, gateAcq(crossing, 1, gateLevel, 1, "refuse"))
			if err != nil {
				t.Errorf("round %d: the crossing acquire: %v", round, err)
				return
			}
			// granted: the release reached the gate first; full: after.
			t.Logf("round %d: the crossing acquire answered %s", round, ans.Answer)
		}()
		go func() {
			defer wg.Done()
			if err := s.c.SignalWorkflow(ctx, gate, "", engine.GateSignalRelease,
				engine.GateRelease{Run: holder, SeqBase: 1, TaskIx: gateLevel}); err != nil {
				t.Errorf("round %d: the release: %v", round, err)
			}
		}()
		wg.Wait()
		s.waitContinued(t, gate, first)
		v := s.waitState(t, gate, "the released holder gone after the continue-as-new", func(v engine.GateStateView) bool {
			return len(v.State.Queue) == 0 && (len(v.State.Holders) == 0 || (len(v.State.Holders) == 1 && v.State.Holders[0].Run == crossing))
		})
		if len(v.State.Holders) == 1 {
			s.release(t, gate, crossing, 1, gateLevel)
		}
		s.closeEmpty(t, gate)
	}
}
