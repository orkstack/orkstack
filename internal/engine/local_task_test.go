package engine_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// localParityDiamond is corpus (a) of TestLocalTaskInput_AgreesWithTheInterpreter:
// a diamond a -> {b, c} -> d whose with: reads inputs, an upstream output and
// an upstream artifact, where `a` declares every Task field ExecuteTaskInput
// carries, plus a task `e` whose condition is false and is never dispatched.
const localParityDiamond = `
name: local-parity
version: "3"
inputs:
  greeting:
    type: string
    default: hello
  target:
    type: string
tasks:
  - id: a
    uses: shell
    pool: gpu
    command: ["sh", "-c", "true"]
    env:
      STAGE: a
    secrets:
      - name: tok
        source: "env:LOCAL_PARITY_TOKEN"
    resources:
      cpu: "0.5"
      memory: 64Mi
    network: none
    timeout_seconds: 42
    cancellation_grace_period_seconds: 7
    run_as_orker_user: true
    with:
      op: exec
      command: ["sh", "-c", "echo {{ .inputs.greeting }}"]
  - id: b
    uses: shell
    needs: [a]
    with:
      op: exec
      command: ["sh", "-c", "echo {{ .tasks.a.output.task }} {{ .inputs.target }}"]
  - id: c
    uses: shell
    needs: [a]
    with:
      op: exec
      command: ["sh", "-c", "cat {{ artifact \"a/a.out\" }}"]
  - id: d
    uses: shell
    needs: [b, c]
    with:
      op: exec
      command: ["sh", "-c", "echo {{ .tasks.a.output.task }}"]
  - id: e
    uses: shell
    needs: [d]
    condition: '{{ eq .inputs.target "never" }}'
    with:
      op: exec
      command: ["sh", "-c", "echo skipped"]
`

// localParityPinned is corpus (b): a two-member pin, so pin is shown to be the
// only additional difference for a pinned task.
const localParityPinned = `
name: local-parity-pin
pins:
  build:
    timeout_seconds: 600
tasks:
  - id: prep
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {command: "echo prep"}
  - id: compile
    uses: shell
    pin: build
    needs: [prep]
    run_as_orker_user: true
    with: {command: "echo {{ .tasks.prep.output.task }}"}
  - id: publish
    uses: shell
    needs: [compile]
    with: {command: "echo publish"}
`

// parityResult is the behavior every agreement-test task succeeds with: values naming the
// task, one artifact <id>.out with a URI and sha256 no other task shares, and
// a commit under the parity commit root.
func parityResult(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	id := in.GetTaskId()
	result := succeeded(id, map[string]any{"task": id})
	result.Artifacts = []*orkstackv1.ArtifactRef{{
		Name:        id + ".out",
		Uri:         "s3://parity/artifacts/" + id + ".out",
		Sha256:      fmt.Sprintf("%064x", len(id)*7919+int(id[0])),
		SizeBytes:   int64(len(id)),
		ContentType: "text/plain",
	}}
	result.Commit = &orkstackv1.CommitRef{Uri: "s3://parity/commits/" + id}
	return result, nil
}

// parityPlan is defaultPlan routing every pool the document names, with a
// commit root and a telemetry selection set, so the cleared fields of the
// recorded inputs are shown to have been set.
func parityPlan(wf *orkstackv1.Orkflow) engine.RunPlan {
	pools := map[string]bool{}
	for _, t := range wf.GetTasks() {
		if p := t.GetPool(); p != "" && p != task.DefaultPool {
			pools[p] = true
		}
	}
	names := make([]string, 0, len(pools))
	for p := range pools {
		names = append(names, p)
	}
	sort.Strings(names)
	plan := defaultPlan(names...)
	plan.CommitRoot = "s3://parity"
	plan.Telemetry = &engine.TelemetrySelection{
		Sink:     "otel-parity",
		TaskLogs: "TASK_LOG_EXPORT_ENABLED",
	}
	plan.ImagePolicy = &engine.ImagePolicySelection{
		RequireDigest:     true,
		AllowedRegistries: []string{"registry.parity.example"},
	}
	return plan
}

// requiredInputs gives every declared input WITHOUT a default a value of its
// declared type, the way the agreement test's example corpus is submitted.
func requiredInputs(t *testing.T, wf *orkstackv1.Orkflow) *orkstackv1.Payload {
	t.Helper()
	values := map[string]any{}
	for name, decl := range wf.GetInputs().GetFields() {
		fields := decl.GetStructValue().GetFields()
		if _, ok := fields["default"]; ok {
			continue
		}
		switch fields["type"].GetStringValue() {
		case "number":
			values[name] = 1
		case "boolean":
			values[name] = true
		case "object":
			values[name] = map[string]any{}
		case "array":
			values[name] = []any{}
		default:
			values[name] = "x"
		}
	}
	return payload(t, values)
}

type parityCase struct {
	name   string
	wf     *orkstackv1.Orkflow
	input  *orkstackv1.Payload
	pinned bool
	// fail names tasks whose activity fails (non-retryable ActivityFailed,
	// with its output artifact on the failure detail), so a failure handler
	// of them is dispatched and compared too.
	fail map[string]bool
}

// localParityHandler is corpus (c): a deploy that fails with an artifact
// while a sibling is in flight, an any_failed handler of both and an
// all_done handler, so a failure handler's input — its JoinContext and the
// failed need's artifacts — is compared too.
const localParityHandler = `
name: local-parity-handler
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: deploy
    uses: shell
    needs: [build]
    with: {op: exec}
  - id: smoke
    uses: shell
    needs: [build]
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [deploy, smoke]
    join: any_failed
    with: {op: exec}
  - id: notify
    uses: shell
    needs: [build, deploy]
    join: all_done
    with: {op: exec}
`

func parityCorpus(t *testing.T) []parityCase {
	t.Helper()
	corpus := []parityCase{
		{name: "diamond", wf: compile(t, localParityDiamond), input: payload(t, map[string]any{"target": "staging"})},
		{name: "pinned", wf: compile(t, localParityPinned), input: payload(t, nil), pinned: true},
		{name: "failure-handler", wf: compile(t, localParityHandler), input: payload(t, nil),
			fail: map[string]bool{"deploy": true}},
	}
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.ork.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob examples: %v (%d found)", err, len(paths))
	}
	sort.Strings(paths)
	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		wf, err := dsl.Compile(source)
		if err != nil {
			t.Fatalf("compile %s: %v", path, err)
		}
		// A composing example pins its children's digests, as apply stamps
		// them (composed-release composes release-block).
		stampExampleChildren(t, wf)
		corpus = append(corpus, parityCase{
			name:   filepath.Base(path),
			wf:     wf,
			input:  requiredInputs(t, wf),
			pinned: len(wf.GetPins()) > 0,
		})
	}
	return corpus
}

// runParity runs RunWorkflow for one corpus document and returns every
// ExecuteTaskInput it scheduled and the run it ended with.
func runParity(t *testing.T, c parityCase, runID string) ([]*orkstackv1.ExecuteTaskInput, *orkstackv1.Run) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	var h *harness
	// Every task's values are stored where CountEachItems reads them, so a
	// fan-out over an upstream output (examples/each-fan-out.ork.yaml) really
	// expands and its items are compared too. parityExampleValues gives the
	// tasks a fan-out or a condition reads the values they really produce.
	behavior := func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result, err := parityResult(in)
		if c.fail[in.GetTaskId()] {
			return nil, temporal.NewNonRetryableApplicationError("workload exited 1", task.CategoryActivityFailed, nil,
				&orkstackv1.ActivityFailureDetail{
					ReasonCode: "CONTAINER_NONZERO_EXIT",
					Category:   orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
					ExitCode:   1,
					Artifacts:  result.GetArtifacts(),
				})
		}
		if err != nil || in.GetEach() != nil {
			return result, err
		}
		values := map[string]any{"task": in.GetTaskId(), "value": in.GetTaskId()}
		// Keyed by the corpus document: a composed child's tasks run in
		// the parent's harness, so the parent's entry stubs them too.
		for k, v := range parityExampleValues[c.name][in.GetTaskId()] {
			values[k] = v
		}
		result.ValuesJson = valuesJSON(values)
		result.ValuesRef = h.values.put("mem://parity/"+in.GetTaskId(), result.ValuesJson)
		return result, nil
	}
	if c.pinned {
		h = newPinHarness(t, &suite, behavior, nil)
	} else {
		h = newHarness(t, &suite, behavior)
	}
	plan := parityPlan(c.wf)
	plan.Children = parityChildren(t, c.wf)
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, c.wf, c.input, plan)
	if !h.env.IsWorkflowCompleted() {
		t.Fatalf("%s: workflow did not complete", c.name)
	}
	var run *orkstackv1.Run
	if err := h.env.GetWorkflowError(); err == nil {
		run = mustFinalRun(t, h.store, runID)
	} else {
		// A run whose own conditions fail it still dispatched what it
		// dispatched; its last projected snapshot holds the same task runs.
		snapshot, ok := h.store.Get(runID)
		if !ok {
			t.Fatalf("%s: workflow error %v and no snapshot", c.name, err)
		}
		run = snapshot
	}
	// Only the run's own tasks: a composed task's child runs its tasks under
	// its own run id, and those are the child's, not this definition's.
	var own []*orkstackv1.ExecuteTaskInput
	for _, in := range h.invocations() {
		if in.GetRunId() == runID {
			own = append(own, in)
		}
	}
	return own, run
}

// exampleOrkflow compiles the shipped example named name@version, with its
// own digest stamped as apply would.
func exampleOrkflow(t *testing.T, name, version string) *orkstackv1.Orkflow {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		wf, err := dsl.CompileFile(path)
		if err != nil || wf.GetName() != name || wf.GetVersion() != version {
			continue
		}
		stampExampleChildren(t, wf)
		digest, err := dsl.DefinitionDigest(wf)
		if err != nil {
			t.Fatal(err)
		}
		wf.DefinitionDigest = digest
		return wf
	}
	t.Fatalf("no shipped example is %s@%s", name, version)
	return nil
}

// stampExampleChildren stamps every composed task's definition digest from
// the shipped example it names.
func stampExampleChildren(t *testing.T, wf *orkstackv1.Orkflow) {
	t.Helper()
	for _, task := range wf.GetTasks() {
		if ref := task.GetOrkflow(); ref != nil {
			ref.DefinitionDigest = exampleOrkflow(t, ref.GetName(), ref.GetVersion()).GetDefinitionDigest()
		}
	}
}

// parityChildren plans every composed task of wf as a child run of the
// shipped example it names.
func parityChildren(t *testing.T, wf *orkstackv1.Orkflow) map[string]*engine.ChildRunPlan {
	t.Helper()
	var children map[string]*engine.ChildRunPlan
	for _, task := range wf.GetTasks() {
		ref := task.GetOrkflow()
		if ref == nil {
			continue
		}
		child := exampleOrkflow(t, ref.GetName(), ref.GetVersion())
		cp, err := engine.NewChildRunPlan(child, parityPlan(child), false, false)
		if err != nil {
			t.Fatal(err)
		}
		cp.Plan.Children = parityChildren(t, child)
		cp.RunID = "ork-parity-child-" + task.GetId()
		if children == nil {
			children = map[string]*engine.ChildRunPlan{}
		}
		children[task.GetId()] = cp
	}
	return children
}

// TestLocalTaskInput_AgreesWithTheInterpreter: for every task the
// interpreter schedules, LocalTaskInput reproduces the recorded input
// byte-for-byte except the four named, deliberate differences.
func TestLocalTaskInput_AgreesWithTheInterpreter(t *testing.T) {
	for _, c := range parityCorpus(t) {
		t.Run(c.name, func(t *testing.T) {
			runID := "ork-parity-" + strings.TrimSuffix(c.name, ".ork.yaml")
			recorded, run := runParity(t, c, runID)
			handlers := 0
			if len(recorded) == 0 {
				t.Fatalf("%s: the interpreter scheduled nothing; nothing was compared", c.name)
			}
			upstream := engine.UpstreamFromRun(run)
			// A failure handler is handed its direct FAILED needs' artifacts;
			// orkctl task run --from-run merges them the same way.
			for id, ref := range engine.FailedNeedUpstreamFromRun(run) {
				upstream[id] = ref
			}
			// A task downstream of a composed task is compared too: its
			// upstream ref for the composed task is {task_id, values}.
			if needsAComposedTask(c.wf) {
				downstream := false
				for _, want := range recorded {
					for _, up := range want.GetUpstream() {
						if t := taskByIDOf(c.wf, up.GetTaskId()); t.GetOrkflow() != nil && up.GetValues() != nil {
							downstream = true
						}
					}
				}
				if !downstream {
					t.Fatalf("%s: no task downstream of a composed task was scheduled; nothing was compared", c.name)
				}
			}
			for _, want := range recorded {
				if want.GetCommitUri() == "" {
					t.Errorf("%s/%s: the recorded input carries no commit_uri, so clearing it proves nothing",
						c.name, want.GetTaskId())
				}
				if want.GetTelemetry() == nil {
					t.Errorf("%s/%s: the recorded input carries no telemetry, so clearing it proves nothing",
						c.name, want.GetTaskId())
				}
				if want.GetImagePolicy() == nil {
					t.Errorf("%s/%s: the recorded input carries no image_policy, so clearing it proves nothing",
						c.name, want.GetTaskId())
				}
				local := engine.LocalTask{
					Orkflow:  c.wf,
					TaskID:   want.GetTaskId(),
					RunID:    runID,
					Input:    c.input,
					Upstream: upstream,
					// Nil for every task that is not a failure handler.
					Join: engine.JoinContextFromRun(run, c.wf, want.GetTaskId()),
				}
				if want.GetJoin() != nil {
					handlers++
				}
				// An item of a fan-out: compose that item, with the run's
				// count (orkctl task run --from-run … --item N).
				if each := want.GetEach(); each != nil {
					index, count := each.GetIndex(), taskRunOf(t, run, want.GetTaskId()).GetEach().GetCount()
					local.Item, local.ItemCount = &index, &count
				}
				// A direct dependent of a fan-out is refused locally: its
				// items are read from their commits.
				if dependsOnFanOut(c.wf, want.GetTaskId()) {
					if _, err := engine.LocalTaskInput(local); err == nil ||
						!strings.Contains(err.Error(), "a local run has no commit store") {
						t.Errorf("%s/%s: a direct dependent of a fan-out = %v, want the refusal", c.name, want.GetTaskId(), err)
					}
					continue
				}
				got, err := engine.LocalTaskInput(local)
				if err != nil {
					t.Fatalf("%s/%s: LocalTaskInput: %v", c.name, want.GetTaskId(), err)
				}
				cleared := proto.Clone(want).(*orkstackv1.ExecuteTaskInput)
				cleared.CommitUri = ""
				cleared.Pin = nil
				cleared.Telemetry = nil
				cleared.ImagePolicy = nil
				if !proto.Equal(got, cleared) {
					t.Errorf("%s/%s: LocalTaskInput differs from the interpreter's input\n got: %v\nwant: %v",
						c.name, want.GetTaskId(), got, cleared)
				}
				if c.pinned && want.GetPin() == nil && taskPin(c.wf, want.GetTaskId()) != "" {
					t.Errorf("%s/%s: a pinned member was scheduled without a pin", c.name, want.GetTaskId())
				}
			}
			if len(c.fail) > 0 && handlers == 0 {
				t.Fatalf("%s: no failure handler was dispatched; nothing of its input was compared", c.name)
			}
			t.Logf("corpus %s: %d invocations compared (%d failure handlers)", c.name, len(recorded), handlers)
		})
	}
}

func dependsOnFanOut(wf *orkstackv1.Orkflow, id string) bool {
	for _, tk := range wf.GetTasks() {
		if tk.GetId() != id {
			continue
		}
		for _, need := range tk.GetNeeds() {
			for _, other := range wf.GetTasks() {
				if other.GetId() == need && other.GetEach() != nil {
					return true
				}
			}
		}
	}
	return false
}

func taskPin(wf *orkstackv1.Orkflow, id string) string {
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == id {
			return tk.GetPin()
		}
	}
	return ""
}

// TestLocalTaskInput_LeavesCommitPinTelemetryAndImagePolicyUnset.
func TestLocalTaskInput_LeavesCommitPinTelemetryAndImagePolicyUnset(t *testing.T) {
	wf := compile(t, localParityPinned)
	for _, id := range []string{"prep", "compile", "publish"} {
		in, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: id, RunID: "local-0123456789abcdef"})
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if in.GetCommitUri() != "" || in.GetPin() != nil || in.GetTelemetry() != nil || in.GetImagePolicy() != nil {
			t.Errorf("%s: commit_uri %q, pin %v, telemetry %v, image_policy %v — want all unset",
				id, in.GetCommitUri(), in.GetPin(), in.GetTelemetry(), in.GetImagePolicy())
		}
		if in.GetIdempotencyKey() != "default/local-0123456789abcdef/"+id {
			t.Errorf("%s: idempotency key %q is not the local run's own", id, in.GetIdempotencyKey())
		}
		if !proto.Equal(in.GetLimits(), engine.DefaultLimits().Proto()) {
			t.Errorf("%s: limits %v, want the contract defaults", id, in.GetLimits())
		}
	}
}

// TestLocalTaskInput_UsesOnlyTheClosureInClosureOrder.
func TestLocalTaskInput_UsesOnlyTheClosureInClosureOrder(t *testing.T) {
	wf := compile(t, localParityDiamond)
	ref := func(id string) *orkstackv1.UpstreamRef {
		return &orkstackv1.UpstreamRef{TaskId: id, Values: &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://" + id}}
	}
	in, err := engine.LocalTaskInput(engine.LocalTask{
		Orkflow: wf, TaskID: "d", RunID: "local-1",
		// c is in the closure but absent (it contributed nothing); e is not in
		// d's closure; d is the target itself.
		Upstream: map[string]*orkstackv1.UpstreamRef{"b": ref("b"), "a": ref("a"), "e": ref("e"), "d": ref("d")},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, up := range in.GetUpstream() {
		got = append(got, up.GetTaskId())
	}
	if strings.Join(got, ",") != "a,b" {
		t.Errorf("upstream = %v, want [a b]", got)
	}
}

// TestLocalTaskInput_RefusesAnUnknownTaskAnEmptyRunIDAndANilOrkflow.
func TestLocalTaskInput_RefusesAnUnknownTaskAnEmptyRunIDAndANilOrkflow(t *testing.T) {
	wf := compile(t, localParityDiamond)
	for name, lt := range map[string]engine.LocalTask{
		"unknown task": {Orkflow: wf, TaskID: "zzz", RunID: "local-1"},
		"empty run id": {Orkflow: wf, TaskID: "a"},
		"nil orkflow":  {TaskID: "a", RunID: "local-1"},
	} {
		in, err := engine.LocalTaskInput(lt)
		if err == nil || in != nil {
			t.Errorf("%s: got (%v, %v), want a plain error", name, in, err)
		}
		var refusal *engine.LocalTaskRefusal
		if errors.As(err, &refusal) {
			t.Errorf("%s: refused as the task's own failure (%v); want a caller error", name, err)
		}
	}
}

// TestLocalTaskInput_RefusesAnAwaitedTask is the awaited-tasks half of
// `orkctl task run`'s obligation: the interpreter never schedules an
// awaited task (it waits for a resolution and starts no container), so no
// local input may be composed for one. The refusal is a caller error naming
// the node, never the task's own failure. A task DOWNSTREAM of an await is
// still composable: the await is an ordinary upstream whose values the caller
// supplies, as for any other ancestor.
func TestLocalTaskInput_RefusesAnAwaitedTask(t *testing.T) {
	wf := compile(t, `
name: local-await
version: "1"
tasks:
  - id: approve
    await: {timeout_seconds: 60}
  - id: after
    uses: shell
    needs: [approve]
    command: ["sh", "-c", "true"]
`)
	in, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "approve", RunID: "local-1"})
	if err == nil || in != nil {
		t.Fatalf("got (%v, %v), want a refusal", in, err)
	}
	var refusal *engine.LocalTaskRefusal
	if errors.As(err, &refusal) {
		t.Errorf("refused as the task's own failure (%v); want a caller error", err)
	}
	if !strings.Contains(err.Error(), "await:") || !strings.Contains(err.Error(), `"approve"`) {
		t.Errorf("refusal %q does not name the task and the await: node", err)
	}
	in, err = engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "after", RunID: "local-1"})
	if err != nil || in == nil {
		t.Fatalf("downstream of an await: got (%v, %v), want an input", in, err)
	}
}

// TestLocalTaskInput_AppliesDeclaredDefaultsOnce.
func TestLocalTaskInput_AppliesDeclaredDefaultsOnce(t *testing.T) {
	wf := compile(t, localParityDiamond)
	compose := func(input *orkstackv1.Payload) map[string]any {
		t.Helper()
		in, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "a", RunID: "local-1", Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return in.GetRunInput().GetData().AsMap()
	}
	already := compose(payload(t, map[string]any{"greeting": "hello", "target": "t"}))
	if already["greeting"] != "hello" || already["target"] != "t" || len(already) != 2 {
		t.Errorf("an already defaulted input changed: %v", already)
	}
	missing := compose(payload(t, map[string]any{"target": "t"}))
	if missing["greeting"] != "hello" {
		t.Errorf("a missing declared default was not applied: %v", missing)
	}
	explicit := &orkstackv1.Payload{Data: &structpb.Struct{Fields: map[string]*structpb.Value{
		"greeting": structpb.NewNullValue(),
	}}}
	nulled := compose(explicit)
	if v, ok := nulled["greeting"]; !ok || v != nil {
		t.Errorf("an explicit null lost to the default: %v", nulled)
	}
}

// TestLocalTaskInput_RefusesAnOversizeInputAsTheInterpreterDoes.
func TestLocalTaskInput_RefusesAnOversizeInputAsTheInterpreterDoes(t *testing.T) {
	big := strings.Repeat("x", 300*1024)
	wf := compile(t, `
name: oversize
tasks:
  - id: huge
    uses: shell
    with:
      blob: "`+big+`"
`)
	lt := engine.LocalTask{Orkflow: wf, TaskID: "huge", RunID: "local-1"}
	_, err := engine.LocalTaskInput(lt)
	var refusal *engine.LocalTaskRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %v, want a *LocalTaskRefusal", err)
	}
	if refusal.Code != task.ReasonInputTooLarge || refusal.Category != task.CategoryContractViolation {
		t.Errorf("refusal = %s/%s, want %s/%s", refusal.Code, refusal.Category,
			task.ReasonInputTooLarge, task.CategoryContractViolation)
	}
	// The message is CheckInputSize's own for the same composed input.
	composed := &orkstackv1.ExecuteTaskInput{}
	{
		small := proto.Clone(wf).(*orkstackv1.Orkflow)
		small.Tasks[0].With = nil
		in, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: small, TaskID: "huge", RunID: "local-1"})
		if err != nil {
			t.Fatal(err)
		}
		composed = in
		composed.With = wf.GetTasks()[0].GetWith()
	}
	want := task.CheckInputSize(composed, engine.DefaultLimits().Proto().GetValuesMaxBytes())
	if want == nil || refusal.Error() != want.Error() {
		t.Errorf("refusal message %q, want CheckInputSize's %v", refusal.Error(), want)
	}
}

// TestUpstreamFromRun_TakesSucceededTasksOnly.
func TestUpstreamFromRun_TakesSucceededTasksOnly(t *testing.T) {
	values := &orkstackv1.ArtifactRef{Name: "values.json", Uri: "s3://b/values", Sha256: "aa"}
	out := &orkstackv1.ArtifactRef{Name: "out.txt", Uri: "s3://b/out", Sha256: "bb"}
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "ok", Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: values,
			Artifacts: []*orkstackv1.ArtifactRef{out}, Commit: &orkstackv1.CommitRef{Uri: "s3://b/commit"}},
		{TaskId: "failed", Status: orkstackv1.Status_STATUS_FAILED, Artifacts: []*orkstackv1.ArtifactRef{out}},
		{TaskId: "skipped", Status: orkstackv1.Status_STATUS_SKIPPED},
		{TaskId: "canceled", Status: orkstackv1.Status_STATUS_CANCELED},
		{TaskId: "pending", Status: orkstackv1.Status_STATUS_PENDING},
		{TaskId: "running", Status: orkstackv1.Status_STATUS_RUNNING},
	}}
	got := engine.UpstreamFromRun(run)
	if len(got) != 1 {
		t.Fatalf("UpstreamFromRun = %v, want exactly the succeeded task", got)
	}
	want := &orkstackv1.UpstreamRef{TaskId: "ok", CommitUri: "s3://b/commit", Values: values,
		Artifacts: []*orkstackv1.ArtifactRef{out}}
	if !proto.Equal(got["ok"], want) {
		t.Errorf("ok = %v, want %v", got["ok"], want)
	}
}

// TestTaskClosure_MatchesTheInterpreterClosure.
func TestTaskClosure_MatchesTheInterpreterClosure(t *testing.T) {
	c := parityCase{name: "diamond", wf: compile(t, localParityDiamond), input: payload(t, map[string]any{"target": "s"})}
	recorded, _ := runParity(t, c, "ork-parity-closure")
	for _, in := range recorded {
		closure, err := engine.TaskClosure(c.wf, in.GetTaskId())
		if err != nil {
			t.Fatal(err)
		}
		var passed []string
		for _, up := range in.GetUpstream() {
			passed = append(passed, up.GetTaskId())
		}
		if strings.Join(closure, ",") != strings.Join(passed, ",") {
			t.Errorf("%s: TaskClosure = %v, interpreter passed %v", in.GetTaskId(), closure, passed)
		}
	}
	if _, err := engine.TaskClosure(c.wf, "nope"); err == nil {
		t.Error("TaskClosure of an unknown task succeeded")
	}
}

func TestLocalTaskInput_FanOutItemRules(t *testing.T) {
	wf := compileExample(t, "each-fan-out.ork.yaml")
	count := int32(4)
	if _, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "convert", RunID: "local-1", ItemCount: &count}); err == nil ||
		!strings.Contains(err.Error(), "--item N") {
		t.Fatalf("a fan-out without an item = %v", err)
	}
	two := int32(2)
	if _, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "list", RunID: "local-1", Item: &two}); err == nil ||
		!strings.Contains(err.Error(), "is not a fan-out") {
		t.Fatalf("--item on a task that is not a fan-out = %v", err)
	}
	nine := int32(9)
	if _, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "convert", RunID: "local-1", Item: &nine, ItemCount: &count}); err == nil ||
		!strings.Contains(err.Error(), "has 4 items (0–3)") {
		t.Fatalf("an item outside the count = %v", err)
	}
	if _, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "convert", RunID: "local-1", Item: &two}); err == nil ||
		!strings.Contains(err.Error(), "--from-run") {
		t.Fatalf("a task-sourced fan-out with no count = %v", err)
	}
	in, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "convert", RunID: "local-1", Item: &two, ItemCount: &count})
	if err != nil {
		t.Fatalf("convert --item 2: %v", err)
	}
	if in.GetEach().GetIndex() != 2 || in.GetEach().GetCount() != 4 || in.GetIdempotencyKey() != "default/local-1/convert/2" {
		t.Fatalf("item input = %v", in)
	}
	_, err = engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "merge", RunID: "local-1",
		Upstream: map[string]*orkstackv1.UpstreamRef{"convert": {TaskId: "convert", Each: &orkstackv1.EachUpstream{Count: 120}}}})
	if err == nil || !strings.Contains(err.Error(), `task run cannot stage the 120 items of fan-out "convert"`) {
		t.Fatalf("a direct dependent of a fan-out = %v", err)
	}
}

// needsAComposedTask reports whether some task of wf names a composed task in
// its needs:. Only then is anything downstream of a composed task to compare:
// order-saga and approval-gate compose notify-block from leaf tasks.
func needsAComposedTask(wf *orkstackv1.Orkflow) bool {
	for _, t := range wf.GetTasks() {
		for _, need := range t.GetNeeds() {
			if taskByIDOf(wf, need).GetOrkflow() != nil {
				return true
			}
		}
	}
	return false
}

// parityExampleValues is, per shipped example file and task id, the keys
// merged into that task's stubbed values: what a fan-out expands over and
// what a condition or a composed task's engine-rendered with: reads, so every
// fan-out expands, every condition evaluates and every composed task renders
// in the corpus, and their tasks' inputs are compared too.
var parityExampleValues = map[string]map[string]map[string]any{
	"each-fan-out.ork.yaml": {
		"list": {"result": []any{map[string]any{"name": "a"}, map[string]any{"name": "b"},
			map[string]any{"name": "c"}, map[string]any{"name": "d"}}},
	},
	"decide-and-route.ork.yaml": {
		"decide": {"result": map[string]any{"route": "canary", "notify": true}},
	},
	"approval-gate.ork.yaml": {
		"assess": {"result": map[string]any{"risky": false, "summary": "web: replicas = 3"}},
		"post":   {"status": 200}, // notify-block's, composed by announce
	},
	"notify-block.ork.yaml": {
		"post": {"status": 200}, // its declared output reads it
	},
	"order-saga.ork.yaml": {
		"report": {"result": "x"},
		"post":   {"status": 200}, // notify-block's, composed by notify
	},
	"config-from-git.ork.yaml": {
		"resolve": {"commit": "0123456789abcdef0123456789abcdef01234567", "ref": "refs/heads/main"},
		"prepare": {"result": []any{map[string]any{"name": "api", "file": "api.conf", "port": "8080",
			"replicas": "2", "environment": "staging", "commit": "0123456789ab"}}},
	},
	"custom-image.ork.yaml": {
		"labels": {"result": []any{"alpha", "beta"}},
	},
}

func taskByIDOf(wf *orkstackv1.Orkflow, id string) *orkstackv1.Task {
	for _, t := range wf.GetTasks() {
		if t.GetId() == id {
			return t
		}
	}
	return nil
}

func TestLocalTaskInput_RefusesAComposedTask(t *testing.T) {
	wf := compile(t, composedParentDocument)
	_, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "release", RunID: "ork-local"})
	want := `task "release" runs orkflow release-block@2.1.0 as a child run, not a container: run one of its tasks with orkctl task run --from-run <child-run-id> --task <task>`
	if err == nil || err.Error() != want {
		t.Fatalf("LocalTaskInput = %v\nwant %s", err, want)
	}
}

func TestLocalTaskInput_RequiresAJoinForAHandlerAndRefusesOneOtherwise(t *testing.T) {
	wf := compile(t, localParityHandler)
	join := &orkstackv1.JoinContext{
		Policy: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED,
		Needs: []*orkstackv1.NeedOutcome{
			{TaskId: "deploy", Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: "TASK_FAILED", Message: "supplied with --need"}},
			{TaskId: "smoke", Status: orkstackv1.Status_STATUS_SUCCEEDED},
		},
	}
	_, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "rollback", RunID: "local-1"})
	want := `task "rollback" is a failure handler (join any_failed): its input names how its needs ended; ` +
		`run it with --from-run RUN or name each need with --need TASK=STATUS`
	if err == nil || err.Error() != want {
		t.Fatalf("a handler with no join = %v", err)
	}
	if _, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: "deploy", RunID: "local-1", Join: join}); err == nil ||
		!strings.Contains(err.Error(), "is not a failure handler") {
		t.Fatalf("a non-handler with a join = %v", err)
	}
	log := &orkstackv1.ArtifactRef{Name: "deploy.out", Uri: "mem://deploy.out"}
	in, err := engine.LocalTaskInput(engine.LocalTask{
		Orkflow: wf, TaskID: "rollback", RunID: "local-1", Join: join,
		Upstream: map[string]*orkstackv1.UpstreamRef{
			"build":  {TaskId: "build", CommitUri: "mem://c/build", Values: &orkstackv1.ArtifactRef{Uri: "mem://v/build"}},
			"deploy": {TaskId: "deploy", Artifacts: []*orkstackv1.ArtifactRef{log}},
			"smoke":  {TaskId: "smoke", CommitUri: "mem://c/smoke", Values: &orkstackv1.ArtifactRef{Uri: "mem://v/smoke"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in.GetJoin(), join) {
		t.Fatalf("input join = %v", in.GetJoin())
	}
	var ids []string
	for _, ref := range in.GetUpstream() {
		ids = append(ids, ref.GetTaskId())
		if ref.GetTaskId() == "deploy" && (len(ref.GetArtifacts()) != 1 || ref.GetValues() != nil) {
			t.Errorf("deploy ref = %v, want {task_id, artifacts}", ref)
		}
	}
	if fmt.Sprint(ids) != "[build deploy smoke]" {
		t.Errorf("upstream = %v", ids)
	}
}

func TestJoinContextFromRun_ReadsStatusesAndBoundedErrors(t *testing.T) {
	wf := compile(t, localParityHandler)
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED},
		{TaskId: "deploy", Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{
			Code: "CONTAINER_NONZERO_EXIT", Message: strings.Repeat("m", 3000), Details: `task "deploy"`,
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
		}, Artifacts: []*orkstackv1.ArtifactRef{{Name: "deploy.log"}}},
		{TaskId: "smoke", Status: orkstackv1.Status_STATUS_CANCELED, Error: &orkstackv1.Error{Code: "TASK_CANCELED", Message: "canceled"}},
	}}
	jc := engine.JoinContextFromRun(run, wf, "rollback")
	if jc.GetPolicy() != orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED || len(jc.GetNeeds()) != 2 {
		t.Fatalf("join = %v", jc)
	}
	deploy, smoke := jc.GetNeeds()[0], jc.GetNeeds()[1]
	if deploy.GetStatus() != orkstackv1.Status_STATUS_FAILED || deploy.GetError().GetCode() != "CONTAINER_NONZERO_EXIT" ||
		len(deploy.GetError().GetMessage()) > 1024 || !strings.HasSuffix(deploy.GetError().GetMessage(), "…") {
		t.Errorf("deploy = %v", deploy)
	}
	if smoke.GetStatus() != orkstackv1.Status_STATUS_CANCELED || smoke.GetError().GetCode() != "TASK_CANCELED" {
		t.Errorf("smoke = %v", smoke)
	}
	if engine.JoinContextFromRun(run, wf, "deploy") != nil {
		t.Error("a JoinContext for a task that is not a failure handler")
	}
	refs := engine.FailedNeedUpstreamFromRun(run)
	if len(refs) != 1 || refs["deploy"] == nil {
		t.Errorf("failed-need refs = %v", refs)
	}
	if _, ok := engine.UpstreamFromRun(run)["deploy"]; ok {
		t.Error("UpstreamFromRun lost its succeeded-only contract")
	}
}
