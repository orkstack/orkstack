package engine_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

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

// parityResult is the behavior every E1 task succeeds with: values naming the
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
	return plan
}

// requiredInputs gives every declared input WITHOUT a default a value of its
// declared type, the way E1's example corpus is submitted.
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
}

func parityCorpus(t *testing.T) []parityCase {
	t.Helper()
	corpus := []parityCase{
		{name: "diamond", wf: compile(t, localParityDiamond), input: payload(t, map[string]any{"target": "staging"})},
		{name: "pinned", wf: compile(t, localParityPinned), input: payload(t, nil), pinned: true},
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
	if c.pinned {
		h = newPinHarness(t, &suite, parityResult, nil)
	} else {
		h = newHarness(t, &suite, parityResult)
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, c.wf, c.input, parityPlan(c.wf))
	if !h.env.IsWorkflowCompleted() {
		t.Fatalf("%s: workflow did not complete", c.name)
	}
	var run *orkstackv1.Run
	if err := h.env.GetWorkflowError(); err == nil {
		run = mustWorkflowResult(t, h.env)
	} else {
		// A run whose own conditions fail it still dispatched what it
		// dispatched; its last projected snapshot holds the same task runs.
		snapshot, ok := h.store.Get(runID)
		if !ok {
			t.Fatalf("%s: workflow error %v and no snapshot", c.name, err)
		}
		run = snapshot
	}
	return h.invocations(), run
}

// TestLocalTaskInput_AgreesWithTheInterpreter is E1: for every task the
// interpreter schedules, LocalTaskInput reproduces the recorded input
// byte-for-byte except the four named, deliberate differences.
func TestLocalTaskInput_AgreesWithTheInterpreter(t *testing.T) {
	for _, c := range parityCorpus(t) {
		t.Run(c.name, func(t *testing.T) {
			runID := "ork-parity-" + strings.TrimSuffix(c.name, ".ork.yaml")
			recorded, run := runParity(t, c, runID)
			if len(recorded) == 0 {
				t.Fatalf("%s: the interpreter scheduled nothing; nothing was compared", c.name)
			}
			upstream := engine.UpstreamFromRun(run)
			for _, want := range recorded {
				if want.GetCommitUri() == "" {
					t.Errorf("%s/%s: the recorded input carries no commit_uri, so clearing it proves nothing",
						c.name, want.GetTaskId())
				}
				if want.GetTelemetry() == nil {
					t.Errorf("%s/%s: the recorded input carries no telemetry, so clearing it proves nothing",
						c.name, want.GetTaskId())
				}
				got, err := engine.LocalTaskInput(engine.LocalTask{
					Orkflow:  c.wf,
					TaskID:   want.GetTaskId(),
					RunID:    runID,
					Input:    c.input,
					Upstream: upstream,
				})
				if err != nil {
					t.Fatalf("%s/%s: LocalTaskInput: %v", c.name, want.GetTaskId(), err)
				}
				cleared := proto.Clone(want).(*orkstackv1.ExecuteTaskInput)
				cleared.CommitUri = ""
				cleared.Pin = nil
				cleared.Telemetry = nil
				if !proto.Equal(got, cleared) {
					t.Errorf("%s/%s: LocalTaskInput differs from the interpreter's input\n got: %v\nwant: %v",
						c.name, want.GetTaskId(), got, cleared)
				}
				if c.pinned && want.GetPin() == nil && taskPin(c.wf, want.GetTaskId()) != "" {
					t.Errorf("%s/%s: a pinned member was scheduled without a pin", c.name, want.GetTaskId())
				}
			}
			t.Logf("corpus %s: %d invocations compared", c.name, len(recorded))
		})
	}
}

func taskPin(wf *orkstackv1.Orkflow, id string) string {
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == id {
			return tk.GetPin()
		}
	}
	return ""
}

// TestLocalTaskInput_LeavesCommitPinTelemetryAndImagePolicyUnset is E2.
// (image_policy, claimed by image-supply-chain-policy, is not on the wire in
// this tree; there is nothing for LocalTaskInput to set.)
func TestLocalTaskInput_LeavesCommitPinTelemetryAndImagePolicyUnset(t *testing.T) {
	wf := compile(t, localParityPinned)
	for _, id := range []string{"prep", "compile", "publish"} {
		in, err := engine.LocalTaskInput(engine.LocalTask{Orkflow: wf, TaskID: id, RunID: "local-0123456789abcdef"})
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if in.GetCommitUri() != "" || in.GetPin() != nil || in.GetTelemetry() != nil {
			t.Errorf("%s: commit_uri %q, pin %v, telemetry %v — want all unset",
				id, in.GetCommitUri(), in.GetPin(), in.GetTelemetry())
		}
		if in.GetIdempotencyKey() != "default/local-0123456789abcdef/"+id {
			t.Errorf("%s: idempotency key %q is not the local run's own", id, in.GetIdempotencyKey())
		}
		if !proto.Equal(in.GetLimits(), engine.DefaultLimits().Proto()) {
			t.Errorf("%s: limits %v, want the contract defaults", id, in.GetLimits())
		}
	}
}

// TestLocalTaskInput_UsesOnlyTheClosureInClosureOrder is E3.
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

// TestLocalTaskInput_RefusesAnUnknownTaskAnEmptyRunIDAndANilOrkflow is E4.
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

// TestLocalTaskInput_AppliesDeclaredDefaultsOnce is E5.
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

// TestLocalTaskInput_RefusesAnOversizeInputAsTheInterpreterDoes is E6.
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

// TestUpstreamFromRun_TakesSucceededTasksOnly is E7.
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

// TestTaskClosure_MatchesTheInterpreterClosure is E8.
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
