package engine

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
		if err != nil {
			t.Fatal(err)
		}
		recorded := &historypb.History{}
		if err := (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, recorded); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
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
	if err != nil {
		t.Fatalf("%s starts with a definition stub and has no recorded definition: %v", path, err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	wf := &orkstackv1.Orkflow{}
	if err := proto.Unmarshal(plain, wf); err != nil {
		t.Fatalf("decode %s's definition object: %v", path, err)
	}
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
// thread, so every later reading is that goroutine's. A pass whose code
// regresses costs more CPU on any host; a host loaded by other processes
// delays the pass without adding to its CPU time, which is what moved the
// wall-clock form of these bounds (plan F19).
type passClock struct {
	started  bool
	cpu      bool
	wallBase time.Time
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

// gatedSizeBound scales a wall-clock bound under the race detector.
func gatedSizeBound(bound time.Duration) time.Duration {
	if raceBuild {
		return bound * raceSlowdown
	}
	return bound
}

// TestNewRunState_AtTheGatedSizeUnderTheDeadlockBudget: with the closure
// computed on demand, building a run's state at the gated task ceiling
// (DefaultOrkflowMaxTasks) in each of E7's shapes takes at most 200 ms, a
// fifth of the 1 s deadlock detector, every generation's first workflow task
// and every replay paying it once.
func TestNewRunState_AtTheGatedSizeUnderTheDeadlockBudget(t *testing.T) {
	n := int(DefaultOrkflowMaxTasks)
	bound := gatedSizeBound(200 * time.Millisecond)
	timings := map[string]float64{}
	for _, shape := range []string{"flat", "staged", "deep"} {
		wf := soakShape(shape, n)
		var best time.Duration
		for attempt := range 3 { // the best of three: a scheduler hiccup is not the code's cost
			started := time.Now()
			s := buildRunState("ork-gated", wf, nil, RunPlan{})
			elapsed := time.Since(started)
			if len(s.order) != n {
				t.Fatalf("%s: %d tasks, want %d", shape, len(s.order), n)
			}
			if attempt == 0 || elapsed < best {
				best = elapsed
			}
		}
		timings[shape] = float64(best.Microseconds()) / 1000
		t.Logf("E7 newRunState at %d tasks, %s: %.2f ms (bound %s)", n, shape, timings[shape], bound)
		if best > bound {
			t.Fatalf("%s: newRunState at %d tasks took %s, over %s", shape, n, best, bound)
		}
	}
	proof.WriteJSON(t, "task-ceiling", "new-run-state.json", map[string]any{"tasks": n, "ms": timings})
}

// TestReadyLoop_JoinChainClosureCost: a chain of the gated size whose tasks
// all join none_failed and carry a condition that skips them — accepted by
// rule 44, which counts no ancestor behind a join policy — has the largest
// closure total a definition can have, T(T−1)/2 entries. Every link also
// needs the chain's root, which succeeds, so none_failed admits it and its
// condition, which reads its whole closure, is evaluated (a none_failed task
// whose every need was skipped is skipped by its join, before any
// condition). It asserts three things: every task's condition is evaluated
// exactly once (conditions == T); closureOf computes at most T(T−1)/2
// entries over the run and at least T(T−1)/2 − T, spread over the passes;
// and the run holds at most T closure entries between passes (it has no open
// pin to add its members' closures). Run under the 1 s deadlock detector, its
// largest pass's workflow code (outside the event writes, which yield) stays
// within 20 ms of its thread's CPU time (passClock; the wall clock is logged).
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

	// Each pass's workflow code, outside its event writes, timed twice: on
	// the pass clock, which the bound is asserted on, and on the wall clock,
	// which is logged as evidence.
	probe := &readyLoopProbe{}
	clock := &passClock{}
	var segment, current, worst time.Duration
	var wallSegment time.Time
	var wallCurrent, wallWorst time.Duration
	passes := 0
	probe.pass = func() {
		if passes > 0 {
			current += clock.now() - segment
			worst = max(worst, current)
			wallCurrent += time.Since(wallSegment)
			wallWorst = max(wallWorst, wallCurrent)
		}
		passes++
		current, wallCurrent = 0, 0
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
	h.env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Second})
	h.env.SetTestTimeout(10 * time.Minute)
	plan := refusalRoutedPlan(t)
	plan.History, plan.Continuation = nil, nil
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, wf, (*orkstackv1.Payload)(nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
		t.Fatalf("the join chain did not complete under the 1 s deadlock detector: %v", h.env.GetWorkflowError())
	}
	total := n * (n - 1) / 2
	bound := gatedSizeBound(20 * time.Millisecond)
	t.Logf("E7 join chain of %d tasks: %d passes, largest pass %.2f ms of %s (bound %s; %.2f ms wall clock), "+
		"closure entries computed %d (bound %d), most retained between passes %d (bound %d)",
		n, passes, float64(worst.Microseconds())/1000, clock.kind(), bound, float64(wallWorst.Microseconds())/1000,
		probe.closureEntries, total, probe.maxRetained, n)
	proof.WriteJSON(t, "task-ceiling", "join-chain-closure.json", map[string]any{
		"tasks": n, "passes": passes, "largest_pass_ms": float64(worst.Microseconds()) / 1000,
		"largest_pass_clock": clock.kind(), "largest_pass_wall_ms": float64(wallWorst.Microseconds()) / 1000,
		"closure_entries_computed": probe.closureEntries, "closure_entries_bound": total,
		"max_retained_entries": probe.maxRetained,
	})
	if probe.conditions != n {
		t.Fatalf("%d conditions evaluated, want every task's once (%d)", probe.conditions, n)
	}
	if probe.closureEntries > total || probe.closureEntries < total-n {
		t.Fatalf("closureOf computed %d entries over the run, want about, and at most, T(T−1)/2 = %d",
			probe.closureEntries, total)
	}
	if probe.maxRetained > n {
		t.Fatalf("the run held %d closure entries between passes, over T = %d", probe.maxRetained, n)
	}
	if worst > bound {
		t.Fatalf("the largest pass's workflow code took %s of %s, over %s", worst, clock.kind(), bound)
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
	if err != nil {
		t.Fatal(err)
	}
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
