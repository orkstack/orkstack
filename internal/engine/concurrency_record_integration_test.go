//go:build integration

package engine_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// gateScenario drives one gate from its start to its close on a real
// cluster and returns the execution its fixture records.
type gateScenario struct {
	fixture string
	tune    func(*engine.GateTunables)
	run     func(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History
}

// TestIntegration_RecordConcurrencyGateHistories records E88 P2's gate
// fixtures (replay_concurrency_test.go's gateFixtures) from real gates on a
// real cluster, each driven through the wire a P3 run will use. It never
// skips: by default the recordings land under ORK_PROOF_DIR and the
// committed fixtures are left alone, so every proof shows today's code still
// takes each path on the cluster; only ORK_RECORD_GOLDEN_HISTORY=1 also
// writes them. It FAILS unless every scenario reaches the state its path
// promises, closes empty, schedules no activity task and replays clean.
func TestIntegration_RecordConcurrencyGateHistories(t *testing.T) {
	// The scenarios run in parallel, after this function returns: the
	// context ends with the test, not with this function.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	s := newGateStack(t, ctx, "record")
	s.startWorker()
	scenarios := gateScenarios()
	names := make([]string, len(scenarios))
	for i, sc := range scenarios {
		names[i] = sc.fixture
	}
	if !slices.Equal(names, gateFixtures) {
		t.Fatalf("the scenarios record %v; the replay test reads %v", names, gateFixtures)
	}
	for _, sc := range scenarios {
		t.Run(strings.TrimSuffix(sc.fixture, "-v1.json"), func(t *testing.T) {
			t.Parallel()
			tun := gateTunablesForTest()
			if sc.tune != nil {
				sc.tune(&tun)
			}
			// Three paced repeats, each on a fresh gate, must take the same
			// path event for event before the last is recorded.
			var shape []string
			var h *historypb.History
			for repeat := range fixtureRepeats {
				gate := engine.GateID(gateNamespace, "", sc.fixture+"#"+strconv.Itoa(repeat))
				h = sc.run(t, s, gate, tun)
				shape = requireSameShape(t, sc.fixture, repeat, shape, h)
			}
			recordConcurrencyFixture(t, sc.fixture, h)
		})
	}
}

// longFence keeps a tombstone standing for 22s, so a scenario's fence still
// stands on a loaded host when it acquires against it.
func longFence(t *engine.GateTunables) { t.TombstoneMargin, t.Linger = 20*time.Second, 26*time.Second }

// longAcquire lets an acquire wait 20s for its answer before it is stale,
// for a scenario that holds one back across a worker's absence.
func longAcquire(t *engine.GateTunables) { t.AcquireTimeout, t.Linger = 20*time.Second, 26*time.Second }

func gateScenarios() []gateScenario {
	return []gateScenario{
		{fixture: "concurrency-gate-basic-v1.json", run: gateBasic},
		{fixture: "concurrency-gate-cold-clean-v1.json", run: gateColdClean},
		{fixture: "concurrency-gate-cold-absent-v1.json", run: gateColdAbsent},
		{fixture: "concurrency-gate-verify-v1.json", run: gateVerify},
		{fixture: "concurrency-gate-sweep-v1.json", run: gateSweep,
			tune: func(t *engine.GateTunables) { t.HolderSweepInterval = 2 * time.Second }},
		{fixture: "concurrency-gate-task-confirm-v1.json", run: gateTaskConfirm},
		{fixture: "concurrency-gate-slot-tombstones-v1.json", run: gateSlotTombstones, tune: longFence},
		{fixture: "concurrency-gate-release-below-v1.json", run: gateReleaseBelow, tune: longFence},
		{fixture: "concurrency-gate-stale-incarnation-v1.json", run: gateStaleIncarnation},
		{fixture: "concurrency-gate-reassert-place-v1.json", run: gateReassertPlace,
			// The re-assert asks for a check pass only past verify_debounce
			// since the last one, which a gate starts with: a debounce of a
			// millisecond makes it ask on every repeat, however soon after
			// the start the re-assert arrives.
			tune: func(t *engine.GateTunables) { t.VerifyDebounce = time.Millisecond }},
		{fixture: "concurrency-gate-rebuild-v1.json", run: gateRebuild},
		{fixture: "concurrency-gate-rebuild-failing-v1.json", run: gateRebuildFailing,
			// One failed attempt turns the rebuild failing, and the next
			// comes at a fixed 5s, by when the line fits.
			tune: func(t *engine.GateTunables) {
				t.MaxEntries, t.RebuildTimeout = 1, time.Millisecond
				t.RebuildBackoffMin, t.RebuildBackoffMax = 5*time.Second, 5*time.Second
			}},
		{fixture: "concurrency-gate-continue-v1.json", run: gateContinue,
			tune: func(t *engine.GateTunables) { t.ContinueUpdates = 4 }},
		{fixture: "concurrency-gate-continue-under-load-v1.json", run: gateContinueUnderLoad,
			tune: func(t *engine.GateTunables) { t.ContinueUpdates = 4 }},
		{fixture: "concurrency-gate-drain-pending-grant-v1.json", run: gateDrainPendingGrant,
			// One failed attempt, then a back-off only the drain ends.
			tune: func(t *engine.GateTunables) {
				t.ContinueUpdates = 4
				t.GrantBackoffMin, t.GrantBackoffMax = time.Hour, time.Hour
			}},
		{fixture: "concurrency-gate-close-completes-empty-v1.json", run: gateCloseCompletesEmpty},
		{fixture: "concurrency-gate-close-race-v1.json", run: gateCloseRace, tune: longAcquire},
		{fixture: "concurrency-gate-rebuild-first-v1.json", run: gateRebuildFirst},
		{fixture: "concurrency-gate-fenced-v1.json", run: gateFenced,
			// A tombstone lives long enough that the degraded fence
			// still stands when the next grant is made.
			tune: func(t *engine.GateTunables) {
				t.MaxTombstones, t.TombstoneMargin, t.Linger = 1, 20*time.Second, 26*time.Second
			}},
	}
}

// basic: a clean start grants the first arrival, queues the second, and on
// the release grants the line's head by signal, then closes empty.
func gateBasic(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a, b := gateRunID("a"), gateRunID("b")
	s.startStandIn(t, gate, b, 1, nil)
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	if ans := s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest); ans.Position != 1 || ans.HoldersAhead != 1 {
		t.Fatalf("b queued at %+v, want position 1 behind 1 holder", ans)
	}
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "b holding, its grant delivered", holdsOnly(b))
	if n := s.grants(t, b); n != 1 {
		t.Fatalf("b received %d grant signals, want 1", n)
	}
	s.release(t, gate, b, 1, gateLevel)
	s.closeEmpty(t, gate)
	if n := s.grants(t, b); n != 1 {
		t.Fatalf("by the close b had received %d grant signals, want 1", n)
	}
	return s.history(t, gate, first)
}

// cold-clean: a start after a clean close grants at once, with no rebuild,
// and answers a refuse arrival on a full key full.
func gateColdClean(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a, b := gateRunID("a"), gateRunID("b")
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	if ans := s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest); ans.Epoch != 0 {
		t.Fatalf("a clean start answered epoch %d, want 0", ans.Epoch)
	}
	s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)
	s.release(t, gate, a, 1, gateLevel)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// cold-absent: a start with no previous execution rebuilds from the open
// runs' records without settling, and starts a new epoch.
func gateColdAbsent(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	h, x := gateRunID("h"), gateRunID("x")
	s.startStandIn(t, gate, h, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, time.Now().Add(-time.Minute), 7, 1)})
	s.waitListed(t, gate, 1)
	first := s.startGate(t, gate, engine.GatePreviousAbsent, tun)
	v := s.waitState(t, gate, "h restored as the holder", holdsOnly(h))
	if v.State.Seq != 7 || v.State.Epoch != 1 || v.State.Rebuild.State != "" {
		t.Fatalf("restored state seq %d, epoch %d, rebuild %q; want 7, 1, none", v.State.Seq, v.State.Epoch, v.State.Rebuild.State)
	}
	s.wantAnswer(t, gate, gateAcq(x, 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)
	s.release(t, gate, h, 1, gateLevel)
	if res := s.closeEmpty(t, gate); res.Epoch != 1 || res.Seq != 7 {
		t.Fatalf("closed %+v, want epoch 1, seq 7", res)
	}
	return s.history(t, gate, first)
}

// verify: a verify signal runs a check pass, which releases a holder whose
// run has closed.
func gateVerify(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a := gateRunID("a")
	s.startStandIn(t, gate, a, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, time.Now(), 1, 1)})
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	s.terminate(t, a)
	s.signal(t, gate, engine.GateSignalVerify, nil)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// sweep: the holder sweep's own pass releases a holder whose run closed,
// with nothing asking for it.
func gateSweep(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a := gateRunID("a")
	s.startStandIn(t, gate, a, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, time.Now(), 1, 1)})
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	s.terminate(t, a)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// task-confirm: a task entry whose run never recorded it is marked by one
// pass once task_ack_grace has passed, and released by the next.
func gateTaskConfirm(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a := gateRunID("a")
	s.startStandIn(t, gate, a, 1, nil)
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, 0, 1, "queue"), engine.GateAnswerGrantedForTest)
	granted := s.state(t, gate).State.Holders[0].GrantedAt
	s.poll(t, "task_ack_grace passing", func() (bool, error) { return time.Since(granted) > tun.TaskAckGrace, nil })
	s.signal(t, gate, engine.GateSignalVerify, nil)
	s.waitState(t, gate, "a's task entry marked missing", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && !v.State.Holders[0].MissingSince.IsZero()
	})
	s.signal(t, gate, engine.GateSignalVerify, nil)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// slot-tombstones: a released slot is fenced against a late acquire for it,
// while the run's next incarnation is granted.
func gateSlotTombstones(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a := gateRunID("a")
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "a released", isEmpty)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "refuse"), engine.GateAnswerFencedForTest)
	s.wantAnswer(t, gate, gateAcq(a, 2, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	s.release(t, gate, a, 2, gateLevel)
	if v := s.waitState(t, gate, "a's second slot released", isEmpty); len(v.State.Tombstones) != 2 {
		t.Fatalf("%d tombstones, want 2", len(v.State.Tombstones))
	}
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// release-below: release_all removes every entry of the run's older
// incarnations and fences them, run level and task alike.
func gateReleaseBelow(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a := gateRunID("a")
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 2, "refuse"), engine.GateAnswerGrantedForTest)
	s.wantAnswer(t, gate, gateAcq(a, 1, 0, 2, "refuse"), engine.GateAnswerGrantedForTest)
	s.signal(t, gate, engine.GateSignalReleaseAll, engine.GateReleaseAll{Run: a, Below: 2})
	s.waitState(t, gate, "a's first incarnation released", isEmpty)
	s.wantAnswer(t, gate, gateAcq(a, 1, 1, 2, "refuse"), engine.GateAnswerFencedForTest)
	s.wantAnswer(t, gate, gateAcq(a, 2, gateLevel, 2, "refuse"), engine.GateAnswerGrantedForTest)
	s.signal(t, gate, engine.GateSignalReleaseAll, engine.GateReleaseAll{Run: a, Below: 3})
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// stale-incarnation: a pass that finds the run on a newer incarnation
// releases the older one's entry.
func gateStaleIncarnation(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a := gateRunID("a")
	s.startStandIn(t, gate, a, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, time.Now(), 1, 1)})
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	s.terminate(t, a)
	s.startStandIn(t, gate, a, 2, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, time.Now(), 2, 2)})
	s.signal(t, gate, engine.GateSignalVerify, nil)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// reassert-place: an entry re-asserted with an earlier arrival takes its
// place ahead of a later one, and the line is granted in that order. The
// re-assert asks for a check pass, so every run records its entry, as a P3
// run does, and the pass keeps them all.
func gateReassertPlace(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a, c, d := gateRunID("a"), gateRunID("c"), gateRunID("d")
	now := time.Now()
	for _, r := range []string{a, c, d} {
		s.startStandIn(t, gate, r, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(r == a, 1, now, 1, 1)})
	}
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	cAns := s.wantAnswer(t, gate, gateAcq(c, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	re := gateAcq(d, 1, gateLevel, 1, "queue")
	re.ArrivedAt, re.Seq = cAns.ArrivedAt.Add(-time.Second), 1
	if ans := s.wantAnswer(t, gate, re, engine.GateAnswerQueuedForTest); ans.Position != 1 {
		t.Fatalf("the re-asserted entry queued at %d, want 1 (ahead of %s)", ans.Position, c)
	}
	// The check pass the re-assert asked for runs on its own: only once it
	// is done does the release reach the gate, so every repeat takes one
	// path.
	// Its local activity's marker is a command of the workflow task that
	// applied the pass, so once it is in the history the pass is done.
	s.poll(t, "the re-assert's check pass done", func() (bool, error) {
		return s.historyHas(t, gate, first, enums.EVENT_TYPE_MARKER_RECORDED), nil
	})
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "d holding first", holdsOnly(d))
	s.release(t, gate, d, 1, gateLevel)
	s.waitState(t, gate, "c holding next", holdsOnly(c))
	s.release(t, gate, c, 1, gateLevel)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// rebuild: an unclean start restores the holders and the line from the open
// runs' records, answers refuse unavailable while it settles, then grants in
// the restored order.
func gateRebuild(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	x, y, z := gateRunID("x"), gateRunID("y"), gateRunID("z")
	now := time.Now()
	s.startStandIn(t, gate, x, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, now.Add(-2*time.Minute), 3, 1)})
	s.startStandIn(t, gate, y, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(false, 1, now.Add(-time.Minute), 4, 1)})
	s.waitListed(t, gate, 2)
	first := s.startGate(t, gate, engine.GatePreviousUnclean, tun)
	s.wantAnswer(t, gate, gateAcq(z, 1, gateLevel, 1, "refuse"), engine.GateAnswerUnavailableForTest)
	v := s.waitState(t, gate, "x holding, y queued, the settle over", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && v.State.Holders[0].Run == x && len(v.State.Queue) == 1 &&
			v.State.Queue[0].Run == y && v.State.Rebuild.State == ""
	})
	if v.State.Seq != 4 || v.State.Epoch != 1 {
		t.Fatalf("rebuilt seq %d, epoch %d; want 4, 1", v.State.Seq, v.State.Epoch)
	}
	s.release(t, gate, x, 1, gateLevel)
	s.waitState(t, gate, "y holding", holdsOnly(y))
	s.release(t, gate, y, 1, gateLevel)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// rebuild-failing: a rebuild that cannot read the line within
// rebuild_timeout is failing and keeps retrying; once the line fits it
// restores.
func gateRebuildFailing(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	p, q := gateRunID("p"), gateRunID("q")
	now := time.Now()
	s.startStandIn(t, gate, p, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 2, now.Add(-2*time.Minute), 1, 1)})
	s.startStandIn(t, gate, q, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 2, now.Add(-time.Minute), 2, 1)})
	s.waitListed(t, gate, 2)
	first := s.startGate(t, gate, engine.GatePreviousAbsent, tun)
	s.waitState(t, gate, "the rebuild failing over max_entries", func(v engine.GateStateView) bool {
		return v.State.Rebuild.State == engine.GateRebuildFailingForTest
	})
	s.terminate(t, q)
	s.waitListed(t, gate, 1)
	s.waitState(t, gate, "p restored once q is gone", holdsOnly(p))
	s.release(t, gate, p, 1, gateLevel)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// continue: past gate_continue_updates the gate drains and carries its
// state on; the fixture is the execution that continues.
func gateContinue(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	runs := []string{gateRunID("a"), gateRunID("b"), gateRunID("c"), gateRunID("d"), gateRunID("e")}
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(runs[0], 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	for _, r := range runs[1:] {
		s.wantAnswer(t, gate, gateAcq(r, 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)
	}
	s.waitContinued(t, gate, first)
	s.waitState(t, gate, "the carried holder", holdsOnly(runs[0]))
	s.release(t, gate, runs[0], 1, gateLevel)
	s.closeEmpty(t, gate)
	h := s.history(t, gate, first)
	if last := h.GetEvents()[len(h.GetEvents())-1]; last.GetEventType() != enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
		t.Fatalf("the first execution ended %v, want continued as new", last.GetEventType())
	}
	return h
}

// continue-under-load: the execution a continue-as-new starts, carrying a
// holder and a line, grants that line in order; the fixture is that second
// execution.
func gateContinueUnderLoad(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a, b, c, d, e := gateRunID("a"), gateRunID("b"), gateRunID("c"), gateRunID("d"), gateRunID("e")
	s.startStandIn(t, gate, b, 1, nil)
	s.startStandIn(t, gate, c, 1, nil)
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	s.wantAnswer(t, gate, gateAcq(c, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	s.wantAnswer(t, gate, gateAcq(d, 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)
	s.wantAnswer(t, gate, gateAcq(e, 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)
	second := s.waitContinued(t, gate, first)
	s.waitState(t, gate, "a holding and b, c queued in the carried state", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && v.State.Holders[0].Run == a && len(v.State.Queue) == 2 &&
			v.State.Queue[0].Run == b && v.State.Queue[1].Run == c
	})
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "b holding", holdsOnly(b))
	s.release(t, gate, b, 1, gateLevel)
	s.waitState(t, gate, "c holding", holdsOnly(c))
	s.release(t, gate, c, 1, gateLevel)
	s.closeEmpty(t, gate)
	return s.history(t, gate, second)
}

// drain-pending-grant: a grant the cluster will not deliver (the run is at
// its signal limit) is retried until the gate drains for a continue-as-new,
// then carried pending; the fixture is the execution that drains. A grant
// that fails for a reason other than the run being gone is a fault a
// cluster does not produce on demand, so the scenario fills the stand-in to
// its per-execution signal limit, in this test's own namespace, and deletes
// it afterwards.
func gateDrainPendingGrant(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a, b := gateRunID("a"), gateRunID("b")
	s.startStandIn(t, gate, b, 1, nil)
	s.saturateSignals(t, b)
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	s.release(t, gate, a, 1, gateLevel)
	// The gate has acted on the failed grant (its back-off timer is
	// started) once a workflow task completed after the failure: only then
	// do the next acquires reach it, so every repeat takes one path.
	s.poll(t, "the gate acting on the grant to b failing", func() (bool, error) {
		return s.historyHasAfter(t, gate, first, enums.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
			enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED), nil
	})
	for _, r := range []string{gateRunID("c"), gateRunID("d"), gateRunID("e")} {
		s.wantAnswer(t, gate, gateAcq(r, 1, gateLevel, 1, "refuse"), engine.GateAnswerFullForTest)
	}
	s.waitContinued(t, gate, first)
	s.waitState(t, gate, "b holding with its grant still pending in the carried state", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && v.State.Holders[0].Run == b && v.State.Holders[0].GrantPending
	})
	s.release(t, gate, b, 1, gateLevel)
	s.closeEmpty(t, gate)
	s.deleteRun(t, b)
	return s.history(t, gate, first)
}

// close-completes-empty: an empty gate turns lingering, and past its linger
// completes with its epoch and seq.
func gateCloseCompletesEmpty(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a := gateRunID("a")
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "the gate lingering", func(v engine.GateStateView) bool {
		return isEmpty(v) && v.State.Phase == engine.GatePhaseLingeringForTest
	})
	if res := s.closeEmpty(t, gate); res.Epoch != 0 || res.Seq != 1 {
		t.Fatalf("closed %+v, want epoch 0, seq 1", res)
	}
	return s.history(t, gate, first)
}

// close-race: an acquire delivered in the workflow task that ends the
// linger is granted, and the close lifts its drain and carries on. The
// engine worker is stopped across the linger's end, so the timer and the
// acquire reach the gate together.
func gateCloseRace(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	// This scenario stops its stack's worker, so it runs on a stack of its
	// own.
	own := newGateStack(t, s.ctx, "close-race")
	own.startWorker()
	a, b := gateRunID("a"), gateRunID("b")
	first := own.startGate(t, gate, engine.GatePreviousClean, tun)
	own.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	own.release(t, gate, a, 1, gateLevel)
	own.waitState(t, gate, "the gate lingering", func(v engine.GateStateView) bool {
		return isEmpty(v) && v.State.Phase == engine.GatePhaseLingeringForTest
	})
	own.stopWorker()
	// The acquire is sent a little before the linger's timer fires, so it
	// is not stale when the gate decides it: a valid linger always outlasts
	// acquire_timeout + clock_grace.
	fires := lingerFiresAt(t, own.history(t, gate, first))
	own.poll(t, "the linger nearly over", func() (bool, error) { return time.Until(fires) < 3*time.Second, nil })
	type result struct {
		ans engine.GateAnswerForTest
		err error
	}
	// The acquire is sent before the timer fires and waits, admitted, for a
	// workflow task; the worker returns only once the timer has fired, so
	// the one task it is handed holds both.
	answered := make(chan result, 1)
	go func() {
		ans, err := own.acquire(gate, gateAcq(b, 1, gateLevel, 1, "refuse"))
		answered <- result{ans, err}
	}()
	own.poll(t, "the linger's timer firing", func() (bool, error) { return lastTimerFired(own.history(t, gate, first)), nil })
	own.startWorker()
	var r result
	select {
	case r = <-answered:
	case <-own.ctx.Done():
		t.Fatal("the acquire was never answered")
	}
	must.OKf(t, r.err, "the acquire racing the close")
	if r.ans.Answer != engine.GateAnswerGrantedForTest {
		t.Fatalf("the acquire racing the close answered %+v, want granted", r.ans)
	}
	own.waitState(t, gate, "b holding after the close lifted", holdsOnly(b))
	own.release(t, gate, b, 1, gateLevel)
	own.closeEmpty(t, gate)
	h := own.history(t, gate, first)
	requireUpdateWithTimerFire(t, h)
	return h
}

// fenced: an acquire older than acquire_timeout + clock_grace is fenced,
// and a tombstone evicted for room degrades the fence, so the next grant is
// checked.
func gateFenced(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	a, b, c, d := gateRunID("a"), gateRunID("b"), gateRunID("c"), gateRunID("d")
	first := s.startGate(t, gate, engine.GatePreviousClean, tun)
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "a released", isEmpty)
	s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	s.release(t, gate, b, 1, gateLevel)
	s.waitState(t, gate, "a's tombstone evicted for b's", func(v engine.GateStateView) bool {
		return isEmpty(v) && !v.State.FenceDegradedUntil.IsZero()
	})
	s.wantAnswer(t, gate, gateAcq(c, 1, gateLevel, 1, "refuse"), engine.GateAnswerGrantedForTest)
	if v := s.state(t, gate); len(v.State.Holders) != 1 || !v.State.Holders[0].VerifyPending {
		t.Fatalf("a grant under a degraded fence is %+v, want verify_pending", v.State.Holders)
	}
	stale := gateAcq(d, 1, gateLevel, 1, "refuse")
	stale.IssuedAt = time.Now().Add(-time.Hour)
	s.wantAnswer(t, gate, stale, engine.GateAnswerFencedForTest)
	s.release(t, gate, c, 1, gateLevel)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}

// TestIntegration_RecordConcurrencyCensusHistories records the census
// fixtures from real census executions over a real gate: a pass that
// reports the gate's holder and line, which is also the census's metrics
// contract at the engine's edge (a row per namespace, limit label and level,
// the key's counts and the queued runs), and a pass whose local activity
// runs out of time, which fails its execution. It never skips; see
// TestIntegration_RecordConcurrencyGateHistories.
func TestIntegration_RecordConcurrencyCensusHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newGateStack(t, ctx, "census")
	s.startWorker()
	gate := engine.GateID(gateNamespace, "", "census")
	a, b := gateRunID("a"), gateRunID("b")
	s.startStandIn(t, gate, b, 1, nil)
	s.startGate(t, gate, engine.GatePreviousClean, gateTunablesForTest())
	s.wantAnswer(t, gate, gateAcq(a, 1, gateLevel, 1, "queue"), engine.GateAnswerGrantedForTest)
	s.wantAnswer(t, gate, gateAcq(b, 1, gateLevel, 1, "queue"), engine.GateAnswerQueuedForTest)
	s.poll(t, "the gate listed", func() (bool, error) {
		resp, err := s.c.CountWorkflow(s.ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Query: `WorkflowType = "` + engine.ConcurrencyGateWorkflowType + `" AND ExecutionStatus = "Running"`,
		})
		return err == nil && resp.GetCount() == 1, err
	})

	census := func(t *testing.T, name string, settings engine.CensusSettings) (*historypb.History, error) {
		t.Helper()
		run, err := s.c.ExecuteWorkflow(s.ctx, client.StartWorkflowOptions{ID: gateRunID(name), TaskQueue: s.gateQueue},
			engine.ConcurrencyCensusWorkflowType, settings)
		must.OKf(t, err, "start the census")
		runErr := run.Get(s.ctx, nil)
		return s.history(t, run.GetID(), run.GetRunID()), runErr
	}
	settings := engine.CensusSettings{Interval: time.Hour, Timeout: 30 * time.Second, MaxGates: 10, Parallel: 2}

	t.Run("pass", func(t *testing.T) {
		var shape []string
		var h *historypb.History
		for repeat := range fixtureRepeats {
			var err error
			h, err = census(t, "census-pass", settings)
			must.OKf(t, err, "the census pass")
			shape = requireSameShape(t, "concurrency-census-pass-v1.json", repeat, shape, h)
		}
		snap, ok := s.census.last()
		if !ok {
			t.Fatal("the census recorded no snapshot")
		}
		if snap.Gates != 1 || snap.Unavailable || snap.Truncated || snap.Unanswered != 0 || len(snap.Rows) != 1 {
			t.Fatalf("snapshot %+v, want one gate read whole and one row", snap)
		}
		row := snap.Rows[0]
		if row.Namespace != gateNamespace || row.Limit != engine.CensusLimitLabel(gateLimitLabel) || row.Level != "run" ||
			row.SlotsUsed != 1 || row.SlotsLimit != 1 || row.QueueDepth != 1 {
			t.Fatalf("row %+v, want %s/%s/run with 1 of 1 slots used and 1 queued", row, gateNamespace, engine.CensusLimitLabel(gateLimitLabel))
		}
		if requireNoActivityTasks(t, "the census", h) == 0 {
			t.Fatal("the census pass recorded no local activity")
		}
		recordConcurrencyFixture(t, "concurrency-census-pass-v1.json", h)
	})
	t.Run("activity-failure", func(t *testing.T) {
		failing := settings
		failing.Timeout = time.Millisecond
		var shape []string
		var h *historypb.History
		for repeat := range fixtureRepeats {
			var err error
			h, err = census(t, "census-failure", failing)
			// Its deadline ends the local activity, as the SDK's timeout or
			// as the list call it cut short, which the activity reports as
			// its own error; either way the activity failed.
			var failed *temporal.ActivityError
			var timeout *temporal.TimeoutError
			var own *temporal.ApplicationError
			if !errors.As(err, &failed) && !errors.As(err, &timeout) &&
				!(errors.As(err, &own) && strings.HasPrefix(own.Error(), "list the concurrency gates: ")) {
				t.Fatalf("a census pass with a millisecond to run ended %v; its local activity must fail", err)
			}
			shape = requireSameShape(t, "concurrency-census-activity-failure-v1.json", repeat, shape, h)
		}
		recordConcurrencyFixture(t, "concurrency-census-activity-failure-v1.json", h)
	})

	s.release(t, gate, a, 1, gateLevel)
	s.waitState(t, gate, "b holding", holdsOnly(b))
	s.release(t, gate, b, 1, gateLevel)
	s.closeEmpty(t, gate)
}

// requireUpdateWithTimerFire fails unless the acquire's Update was accepted
// in the workflow task the linger's timer fire started: the race the
// close-race fixture is named for.
func requireUpdateWithTimerFire(t *testing.T, h *historypb.History) {
	t.Helper()
	// An Update's acceptance is written after the task that accepted it
	// completes, so it is matched to that task's inputs.
	firedSinceTask, taskHadFire := false, false
	for _, e := range h.GetEvents() {
		switch e.GetEventType() {
		case enums.EVENT_TYPE_TIMER_FIRED:
			firedSinceTask = true
		case enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			taskHadFire, firedSinceTask = firedSinceTask, false
		case enums.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			if taskHadFire {
				return
			}
		}
	}
	t.Fatal("no Update was accepted in the workflow task a timer fire started: the acquire did not race the close")
}

// fixtureRepeats is how many paced repeats of a scenario must take the same
// path before its fixture is recorded.
const fixtureRepeats = 3

// historyShape is a history's path: each event's type, and the name of each
// signal, Update and local activity, without the ids, times and payloads
// that differ between two runs of one path.
func historyShape(h *historypb.History) []string {
	out := make([]string, 0, len(h.GetEvents()))
	for _, e := range h.GetEvents() {
		step := e.GetEventType().String()
		switch {
		case e.GetWorkflowExecutionSignaledEventAttributes() != nil:
			step += ":" + e.GetWorkflowExecutionSignaledEventAttributes().GetSignalName()
		case e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes() != nil:
			step += ":" + e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetSignalName()
		case e.GetWorkflowExecutionUpdateAcceptedEventAttributes() != nil:
			step += ":" + e.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest().GetInput().GetName()
		case e.GetMarkerRecordedEventAttributes() != nil:
			step += ":" + e.GetMarkerRecordedEventAttributes().GetMarkerName()
		}
		out = append(out, step)
	}
	return out
}

// requireSameShape fails unless h takes the path the earlier repeats took,
// naming the first event where it differs; it returns the path.
func requireSameShape(t *testing.T, fixture string, repeat int, want []string, h *historypb.History) []string {
	t.Helper()
	got := historyShape(h)
	if want == nil {
		return got
	}
	for i := range max(len(got), len(want)) {
		var g, w string
		if i < len(got) {
			g = got[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if g != w {
			t.Fatalf("%s: repeat %d took another path at event %d: %q, where repeat 0 had %q (%d and %d events)",
				fixture, repeat, i+1, g, w, len(got), len(want))
		}
	}
	return got
}

// lingerFiresAt is when the history's latest timer is due to fire.
func lingerFiresAt(t *testing.T, h *historypb.History) time.Time {
	t.Helper()
	for i := len(h.GetEvents()) - 1; i >= 0; i-- {
		e := h.GetEvents()[i]
		if a := e.GetTimerStartedEventAttributes(); a != nil {
			return e.GetEventTime().AsTime().Add(a.GetStartToFireTimeout().AsDuration())
		}
	}
	t.Fatal("the gate started no timer")
	return time.Time{}
}

// rebuild-first: a gate started by an acquire (Update-with-Start, as a run
// starts one) beside a holder its rebuild restores is rebuilding from its
// first instant, so the acquire queues behind that holder and is granted by
// signal once it releases, never past it.
func gateRebuildFirst(t *testing.T, s *gateStack, gate string, tun engine.GateTunables) *historypb.History {
	h, b := gateRunID("h"), gateRunID("b")
	s.startStandIn(t, gate, h, 1, map[int]engine.GateMemoEntryForTest{gateLevel: memoEntry(true, 1, time.Now().Add(-time.Minute), 7, 1)})
	s.startStandIn(t, gate, b, 1, nil)
	s.waitListed(t, gate, 1)
	req := gateAcq(b, 1, gateLevel, 1, "queue")
	op := s.c.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID: gate, TaskQueue: s.gateQueue,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, engine.ConcurrencyGateWorkflowType, engine.GateStart{
		GateID: gate, Namespace: gateNamespace, Key: "deploy", Tunables: tun, Previous: engine.GatePreviousAbsent,
	})
	handle, err := s.c.UpdateWithStartWorkflow(s.ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: op,
		UpdateOptions: client.UpdateWorkflowOptions{UpdateName: engine.GateUpdateAcquire, Args: []any{req},
			WaitForStage: client.WorkflowUpdateStageCompleted},
	})
	must.OKf(t, err, "acquire with the start of %s", gate)
	var ans engine.GateAnswerForTest
	must.OKf(t, handle.Get(s.ctx, &ans), "the acquire's answer")
	if ans.Answer != engine.GateAnswerQueuedForTest {
		t.Fatalf("the acquire sent with the start = %+v, want queued behind the holder the rebuild restores", ans)
	}
	run, err := op.Get(s.ctx)
	must.OKf(t, err, "the gate's start")
	first := run.GetRunID()
	s.waitState(t, gate, "h restored as the holder, b waiting", func(v engine.GateStateView) bool {
		return len(v.State.Holders) == 1 && v.State.Holders[0].Run == h && len(v.State.Queue) == 1
	})
	s.waitIdle(t, gate)
	s.release(t, gate, h, 1, gateLevel)
	s.waitState(t, gate, "b holding", holdsOnly(b))
	s.waitGrantsDelivered(t, gate, 1)
	s.waitIdle(t, gate)
	s.release(t, gate, b, 1, gateLevel)
	s.closeEmpty(t, gate)
	return s.history(t, gate, first)
}
