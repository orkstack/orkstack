package engine

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// eagerClosure is the needs closure as newRunState computed it before 7.2,
// written independently: a breadth-first walk over needs, sorted.
func eagerClosure(wf *orkstackv1.Orkflow, id string) []string {
	byID := map[string]*orkstackv1.Task{}
	for _, t := range wf.GetTasks() {
		byID[t.GetId()] = t
	}
	seen := map[string]bool{}
	queue := slices.Clone(byID[id].GetNeeds())
	for len(queue) > 0 {
		need := queue[0]
		queue = queue[1:]
		if seen[need] {
			continue
		}
		seen[need] = true
		queue = append(queue, byID[need].GetNeeds()...)
	}
	out := make([]string, 0, len(seen))
	for need := range seen {
		out = append(out, need)
	}
	sort.Strings(out)
	return out
}

// fixtureDefinitions is every definition a committed history fixture starts
// a run with — the top-level run's and every child's — read from the
// recorded start payloads; a definition passed by reference is read from the
// fixture's recorded object.
func fixtureDefinitions(t *testing.T) map[string]*orkstackv1.Orkflow {
	t.Helper()
	paths, err := filepath.Glob("testdata/history/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no committed fixtures (%v)", err)
	}
	out := map[string]*orkstackv1.Orkflow{}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		must.OK(t, err)
		recorded := &historypb.History{}
		must.OKf(t, (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, recorded), "decode %s", path)
		for _, event := range recorded.GetEvents() {
			payloads := event.GetWorkflowExecutionStartedEventAttributes().GetInput()
			if attrs := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); attrs != nil {
				payloads = attrs.GetInput()
			}
			if len(payloads.GetPayloads()) < 2 {
				continue
			}
			wf := &orkstackv1.Orkflow{}
			if err := converter.GetDefaultDataConverter().FromPayload(payloads.GetPayloads()[1], wf); err != nil {
				continue // not RunWorkflow's arguments
			}
			if len(wf.GetTasks()) == 0 {
				wf = fixtureObjectDefinition(t, path)
			}
			out[fmt.Sprintf("%s#%d", filepath.Base(path), event.GetEventId())] = wf
		}
	}
	return out
}

// fixtureObjectDefinition is the definition object recorded beside a
// definition-by-reference fixture.
func fixtureObjectDefinition(t *testing.T, path string) *orkstackv1.Orkflow {
	t.Helper()
	raw, err := os.ReadFile(strings.TrimSuffix(path, ".json") + ".objects/definition.pb.gz")
	must.OKf(t, err, "%s starts with a definition stub and has no recorded definition", path)
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	must.OK(t, err)
	plain, err := io.ReadAll(zr)
	must.OK(t, err)
	wf := &orkstackv1.Orkflow{}
	must.OKf(t, proto.Unmarshal(plain, wf), "decode %s's definition object", path)
	return wf
}

// TestClosureOf_EqualsTheEagerClosure: for every committed fixture's
// definition, closureOf(id) is the closure newRunState used to compute and
// keep for every task, so every command input built from it is
// byte-identical (spec 7.2).
func TestClosureOf_EqualsTheEagerClosure(t *testing.T) {
	definitions := fixtureDefinitions(t)
	checked := 0
	for name, wf := range definitions {
		s := buildRunState("ork-closure", wf, nil, RunPlan{})
		for _, id := range s.order {
			got, want := s.closureOf(id), eagerClosure(wf, id)
			if !slices.Equal(got, want) {
				t.Fatalf("%s: closureOf(%s) = %v, the eager closure %v", name, id, got, want)
			}
			checked++
		}
	}
	if len(definitions) < 10 || checked == 0 {
		t.Fatalf("checked %d tasks of %d definitions, want every committed fixture's", checked, len(definitions))
	}
	t.Logf("closureOf equals the eager closure for %d tasks of %d recorded definitions", checked, len(definitions))
}

// soakShape is one of E7's shapes at n tasks (spec 7.4): flat (every task
// needs nothing), staged (250 lanes; stage i of a lane needs stage i−1 of
// the same lane) and deep (⌈n/380⌉ lanes of at most 380).
func soakShape(shape string, n int) *orkstackv1.Orkflow {
	wf := &orkstackv1.Orkflow{Name: shape, Version: "v1", Namespace: "team-a"}
	lanes := 1
	switch shape {
	case "flat":
		lanes = n
	case "staged":
		lanes = 250
	case "deep":
		lanes = (n + 379) / 380
	}
	for i := range n {
		lane, stage := i%lanes, i/lanes
		t := &orkstackv1.Task{Id: fmt.Sprintf("t%d", i), Image: &orkstackv1.ImageRef{Reference: "alpine"}}
		if stage > 0 {
			t.Needs = []string{fmt.Sprintf("t%d", (stage-1)*lanes+lane)}
		}
		wf.Tasks = append(wf.Tasks, t)
	}
	return wf
}

// passClock is the clock a ready-loop pass's cost is bounded on: the CPU time
// of the workflow goroutine's own thread where the platform has one
// (threadCPU), the wall clock otherwise. Its first reading locks the calling
// goroutine — the workflow's, since every probe callback runs on it — to its
// thread, so every later reading is that goroutine's and no other goroutine
// runs on the thread. A pass whose code regresses costs more CPU on any host;
// a host loaded by other processes delays the pass without adding its waits
// to its CPU time, and those waits are what moved the wall-clock form of
// these bounds (plan F19).
//
// The thread's clock also counts the collector's work the runtime hands the
// goroutine: mark assists for what it allocates while a cycle runs, and the
// start or end of a cycle it lands in. How much of that one pass pays depends
// not on its code but on how far the background mark workers have got, which
// a loaded host slows: beside 32 CPU burners on 16 CPUs, 18 of the join
// chain's passes ran over a quarter more instructions than the same pass on
// a quiet host, the worst 21.1M against 11.5M. So a pass clock holds the
// collector off from when it is made until its test ends, and collects only
// where no segment is being timed (between), at GOGC's default pace: a timed
// segment counts the code and its own allocations, never a collection. With
// it held, no pass ran a quarter more instructions than on a quiet host.
type passClock struct {
	started  bool
	cpu      bool
	wallBase time.Time
	// live is the heap the last collection left, collections how many
	// between made, and cyclesBase the cycles completed once the clock held
	// the collector.
	live, cyclesBase uint64
	collections      int
}

// newPassClock is a pass clock, holding the collector off until t ends, after
// one collection.
func newPassClock(t testing.TB) *passClock {
	t.Helper()
	gcPercent := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(gcPercent) })
	c := &passClock{}
	c.collect()
	c.collections, c.cyclesBase = 0, gcCycles()
	return c
}

// between is called where a test has ended one timed segment and not begun
// the next. It collects once the heap has doubled since the last collection,
// as GOGC=100 would.
func (c *passClock) between() {
	if heapObjectBytes() >= 2*c.live {
		c.collect()
	}
}

func (c *passClock) collect() {
	runtime.GC()
	c.live = heapObjectBytes()
	c.collections++
}

// otherCycles is how many collections completed since the clock was made that
// between did not make: none, unless the heap reached a memory limit
// (GOMEMLIMIT) or other code called runtime.GC, and a measurement taken
// meanwhile is not the code's alone.
func (c *passClock) otherCycles() uint64 {
	return gcCycles() - c.cyclesBase - uint64(c.collections)
}

func heapObjectBytes() uint64 { return readMetric("/memory/classes/heap/objects:bytes").Uint64() }

func gcCycles() uint64 { return readMetric("/gc/cycles/total:gc-cycles").Uint64() }

func readMetric(name string) metrics.Value {
	sample := []metrics.Sample{{Name: name}}
	metrics.Read(sample)
	return sample[0].Value
}

// leastOfRuns measures a deterministic workflow's passes up to three times
// and returns each pass's least cost over the runs on the pass clock, the
// least wall clock beside it, and how many runs it made — the best of three,
// as newRunState's bound is taken. Pass i does the same work in every run, so
// a slowdown only one run's pass met is not the code's cost: a host's other
// threads share a core's pipeline and caches, and on this host they cut one
// pass's instructions per cycle to a quarter (9.5M instructions in 7.55 ms of
// thread CPU time, against 2.03 ms on a quiet core). It runs again only while
// some pass is over bound, so its answer is the best of three's and a host
// that keeps every pass within it pays one run.
func leastOfRuns(t *testing.T, bound time.Duration, measure func() (cpu, wall []time.Duration)) (cpu, wall []time.Duration, runs int) {
	t.Helper()
	for runs < 3 && (cpu == nil || slices.Max(cpu) > bound) {
		runs++
		runCPU, runWall := measure()
		switch {
		case len(runCPU) == 0 || len(runWall) != len(runCPU):
			t.Fatalf("run %d timed %d passes on the pass clock and %d on the wall clock", runs, len(runCPU), len(runWall))
		case cpu == nil:
			cpu, wall = runCPU, runWall
		case len(runCPU) != len(cpu):
			t.Fatalf("run %d timed %d passes, run 1 %d: the runs did not do the same work", runs, len(runCPU), len(cpu))
		default:
			for i := range cpu {
				cpu[i], wall[i] = min(cpu[i], runCPU[i]), min(wall[i], runWall[i])
			}
		}
	}
	return cpu, wall, runs
}

func (c *passClock) now() time.Duration {
	if !c.started {
		c.started = true
		runtime.LockOSThread()
		_, c.cpu = threadCPU()
		c.wallBase = time.Now()
	}
	if c.cpu {
		if d, ok := threadCPU(); ok {
			return d
		}
	}
	return time.Since(c.wallBase)
}

// kind names what now measures, for a failure message.
func (c *passClock) kind() string {
	if c.cpu {
		return "thread CPU time"
	}
	return "wall clock"
}

// gatedSizeBound scales a CPU-time bound (passClock) under the race detector.
func gatedSizeBound(bound time.Duration) time.Duration {
	if raceBuild {
		return bound * raceSlowdown
	}
	return bound
}

// TestNewRunState_AtTheGatedSizeUnderTheDeadlockBudget: with the closure
// computed on demand, building a run's state at the gated task ceiling
// (DefaultOrkflowMaxTasks) in each of E7's shapes takes at most 200 ms of the
// building thread's CPU time (passClock), a fifth of the 1 s deadlock
// detector, every generation's first workflow task and every replay paying it
// once.
func TestNewRunState_AtTheGatedSizeUnderTheDeadlockBudget(t *testing.T) {
	n := int(DefaultOrkflowMaxTasks)
	bound := gatedSizeBound(200 * time.Millisecond)
	clock := newPassClock(t)
	timings := map[string]float64{}
	for _, shape := range []string{"flat", "staged", "deep"} {
		wf := soakShape(shape, n)
		var best time.Duration
		for attempt := range 3 { // the best of three: a cold cache is not the code's cost
			clock.between()
			started := clock.now()
			s := buildRunState("ork-gated", wf, nil, RunPlan{})
			elapsed := clock.now() - started
			if len(s.order) != n {
				t.Fatalf("%s: %d tasks, want %d", shape, len(s.order), n)
			}
			if attempt == 0 || elapsed < best {
				best = elapsed
			}
		}
		timings[shape] = float64(best.Microseconds()) / 1000
		t.Logf("E7 newRunState at %d tasks, %s: %.2f ms of %s (bound %s)", n, shape, timings[shape], clock.kind(), bound)
		if best > bound {
			t.Fatalf("%s: newRunState at %d tasks took %s of %s, over %s", shape, n, best, clock.kind(), bound)
		}
	}
	proof.WriteJSON(t, "task-ceiling", "new-run-state.json", map[string]any{"tasks": n, "ms": timings, "clock": clock.kind()})
}

// TestReadyLoop_JoinChainClosureCost: a chain of the gated size whose tasks
// all join none_failed and carry a condition that skips them — accepted by
// rule 44, which counts no ancestor behind a join policy — has the largest
// closure total a definition can have, T(T−1)/2 entries. Every link also
// needs the chain's root, which succeeds, so none_failed admits it and its
// condition, which reads its whole closure, is evaluated (a none_failed task
// whose every need was skipped is skipped by its join, before any
// condition). It asserts three things of every run: every task's condition is
// evaluated exactly once (conditions == T); closureOf computes at most
// T(T−1)/2 entries over the run and at least T(T−1)/2 − T, spread over the
// passes; and the run holds at most T closure entries between passes (it has
// no open pin to add its members' closures). Run under the 1 s deadlock
// detector, its largest pass's workflow code (outside the event writes, which
// yield) stays within 20 ms of its thread's CPU time (passClock, with the
// collector held off; the wall clock is logged), each pass's cost the least
// of up to three runs (leastOfRuns).
func TestReadyLoop_JoinChainClosureCost(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test: every proof's unit gate runs it; make check skips it")
	}
	n := int(DefaultOrkflowMaxTasks)
	wf := &orkstackv1.Orkflow{Name: "join-chain", Version: "v1", Namespace: "team-a"}
	wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: "t0", Image: &orkstackv1.ImageRef{Reference: "alpine"}})
	for i := 1; i < n; i++ {
		tk := &orkstackv1.Task{Id: fmt.Sprintf("t%d", i), Image: &orkstackv1.ImageRef{Reference: "alpine"},
			Condition: "false", Join: orkstackv1.JoinPolicy_JOIN_POLICY_NONE_FAILED, Needs: []string{"t0"}}
		if i > 1 {
			tk.Needs = append(tk.Needs, fmt.Sprintf("t%d", i-1))
		}
		wf.Tasks = append(wf.Tasks, tk)
	}
	if size, required, err := dsl.MinimalInputBytes(wf, fmt.Sprintf("t%d", n-1)); err != nil || required != 0 ||
		size > task.MaxActivityInputBytes {
		t.Fatalf("the chain's last task: %d B, %d required ancestors (%v); want rule 44 to accept it", size, required, err)
	}

	total := n * (n - 1) / 2
	bound := gatedSizeBound(20 * time.Millisecond)
	var clock *passClock
	var probe *readyLoopProbe
	var otherCycles uint64
	var collections int
	least, leastWall, runs := leastOfRuns(t, bound, func() (costs, walls []time.Duration) {
		// Each pass's workflow code, outside its event writes, timed twice:
		// on the pass clock, which the bound is asserted on, and on the wall
		// clock, which is logged as evidence. A run's workflow is a goroutine
		// of its own, so each run has its own clock.
		clock, probe = newPassClock(t), &readyLoopProbe{}
		var segment, current time.Duration
		var wallSegment time.Time
		var wallCurrent time.Duration
		passes := 0
		probe.pass = func() {
			if passes > 0 {
				current += clock.now() - segment
				wallCurrent += time.Since(wallSegment)
				costs, walls = append(costs, current), append(walls, wallCurrent)
			}
			passes++
			current, wallCurrent = 0, 0
			clock.between()
			segment, wallSegment = clock.now(), time.Now()
		}
		probe.emit = func(start bool) {
			if start {
				current += clock.now() - segment
				wallCurrent += time.Since(wallSegment)
				return
			}
			segment, wallSegment = clock.now(), time.Now()
		}
		readyLoopProbeForTest = probe
		defer func() { readyLoopProbeForTest = nil }()

		h := newRefusalHarnessOver(t, newMemSnapshotStore())
		h.env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: productionDeadlock()})
		h.env.SetTestTimeout(10 * time.Minute)
		plan := refusalRoutedPlan(t)
		plan.History, plan.Continuation = nil, nil
		h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, wf, (*orkstackv1.Payload)(nil), plan, (*orkstackv1.RunContinuation)(nil))
		if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
			t.Fatalf("the join chain did not complete under the 1 s deadlock detector: %v", h.env.GetWorkflowError())
		}
		if probe.conditions.Load() != int64(n) {
			t.Fatalf("%d conditions evaluated, want every task's once (%d)", probe.conditions.Load(), n)
		}
		if entries := probe.closureEntries.Load(); entries > int64(total) || entries < int64(total-n) {
			t.Fatalf("closureOf computed %d entries over the run, want about, and at most, T(T−1)/2 = %d",
				entries, total)
		}
		if retained := probe.maxRetained.Load(); retained > int64(n) {
			t.Fatalf("the run held %d closure entries between passes, over T = %d", retained, n)
		}
		otherCycles, collections = otherCycles+clock.otherCycles(), collections+clock.collections
		return costs, walls
	})
	worst, wallWorst := slices.Max(least), slices.Max(leastWall)
	t.Logf("E7 join chain of %d tasks: %d passes, largest pass %.2f ms of %s, the least of %d runs (bound %s; "+
		"%.2f ms wall clock; collections between passes %d, any other %d), closure entries computed %d (bound %d), "+
		"most retained between passes %d (bound %d)",
		n, len(least), float64(worst.Microseconds())/1000, clock.kind(), runs, bound, float64(wallWorst.Microseconds())/1000,
		collections, otherCycles, probe.closureEntries.Load(), total, probe.maxRetained.Load(), n)
	proof.WriteJSON(t, "task-ceiling", "join-chain-closure.json", map[string]any{
		"tasks": n, "passes": len(least), "runs": runs, "largest_pass_ms": float64(worst.Microseconds()) / 1000,
		"largest_pass_clock": clock.kind(), "largest_pass_wall_ms": float64(wallWorst.Microseconds()) / 1000,
		"closure_entries_computed": probe.closureEntries.Load(), "closure_entries_bound": total,
		"max_retained_entries": probe.maxRetained.Load(), "other_collections": otherCycles,
	})
	if worst > bound {
		t.Fatalf("the largest pass's workflow code took %s of %s, the least of %d runs, over %s "+
			"(collections the pass clock did not make: %d)", worst, clock.kind(), runs, bound, otherCycles)
	}
}

// TestPinMemberClosures_ReleasedWhenThePinCloses: an opened pin's members'
// closures are the one memo a run keeps, and it is gone once the pin's
// members are all terminal.
func TestPinMemberClosures_ReleasedWhenThePinCloses(t *testing.T) {
	wf, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: pinned
pins: {build: {}}
tasks:
  - id: fetch
    uses: shell
  - id: compile
    uses: shell
    needs: [fetch]
    pin: build
  - id: test
    uses: shell
    needs: [compile]
    pin: build
`))
	must.OK(t, err)
	s := buildRunState("ork-pins", wf, nil, RunPlan{})
	p := s.pins["build"]
	p.opened = true
	s.rebuildPinClosure()
	if got := s.retainedClosureEntries(); got != 3 { // compile: fetch; test: compile, fetch
		t.Fatalf("an open pin retains %d closure entries, want its members' 3", got)
	}
	for _, id := range []string{"fetch", "compile"} {
		s.taskRuns[id].Status = orkstackv1.Status_STATUS_SUCCEEDED
	}
	s.rebuildPinClosure()
	if got := s.retainedClosureEntries(); got != 2 {
		t.Fatalf("with compile terminal the pin retains %d entries, want test's 2", got)
	}
	p.doneSent = true
	s.rebuildPinClosure()
	if got := s.retainedClosureEntries(); got != 0 {
		t.Fatalf("a closed pin's members still hold %d closure entries", got)
	}
}
