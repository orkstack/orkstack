package engine_test

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// planTaskOf returns id's PlanTask, failing the test when the plan has none.
func planTaskOf(t *testing.T, plan *orkstackv1.PlanRunResponse, id string) *orkstackv1.PlanTask {
	t.Helper()
	for _, task := range plan.GetTasks() {
		if task.GetTaskId() == id {
			return task
		}
	}
	t.Fatalf("plan has no task %q", id)
	return nil
}

func simulate(t *testing.T, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, assumed map[string]*orkstackv1.Payload) *orkstackv1.PlanRunResponse {
	t.Helper()
	plan, err := engine.Simulate(wf, input, assumed, nil)
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	return plan
}

// assumedPayloads builds the assumed-output map from plain maps.
func assumedPayloads(t *testing.T, values map[string]map[string]any) map[string]*orkstackv1.Payload {
	t.Helper()
	if values == nil {
		return nil
	}
	out := make(map[string]*orkstackv1.Payload, len(values))
	for id, v := range values {
		out[id] = payload(t, v)
	}
	return out
}

// interpret runs wf through the real RunWorkflow with every activity
// succeeding and returning assumed[id] (or {}) as its values.json, and returns
// the final run and the harness that observed it.
func interpret(t *testing.T, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, assumed map[string]map[string]any, pools ...string) (*orkstackv1.Run, *harness) {
	t.Helper()
	return interpretFailing(t, runID, wf, input, assumed, nil, pools...)
}

// interpretFailing is interpret with every task named in failing returning a
// non-retryable ActivityFailed: the run in which exactly the plan's assumed
// failures happen (§6, "a plan and a run agree").
func interpretFailing(t *testing.T, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, assumed map[string]map[string]any, failing []string, pools ...string) (*orkstackv1.Run, *harness) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	var h *harness
	h = newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		for _, id := range failing {
			if id == in.TaskId {
				return nil, temporal.NewNonRetryableApplicationError(in.TaskId+" failed as assumed",
					task.CategoryActivityFailed, nil)
			}
		}
		values := assumed[in.TaskId]
		if values == nil {
			values = map[string]any{}
		}
		encoded := valuesJSON(values)
		result := &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: encoded}
		// The values object a fan-out over this task counts, stored where
		// CountEachItems reads it. An item's own values are not stored.
		if in.GetEach() == nil {
			result.ValuesRef = h.values.put("mem://values/"+in.TaskId, encoded)
		}
		return result, nil
	})
	plan := defaultPlan(pools...)
	plan.CommitRoot = "mem://commits"
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, input, plan)
	if !h.env.IsWorkflowCompleted() {
		t.Fatalf("%s: workflow did not complete", runID)
	}
	run, ok := h.store.Get(runID)
	if !ok {
		t.Fatalf("%s: run read model was not populated", runID)
	}
	return run, h
}

// skipMessages maps task id to every task-skipped event message the run
// emitted for it.
func skipMessages(h *harness, runID string) map[string][]string {
	out := map[string][]string{}
	for _, event := range h.bus.Events(runID, 0) {
		if event.GetStatus() == orkstackv1.Status_STATUS_SKIPPED && event.GetTaskId() != "" {
			out[event.GetTaskId()] = append(out[event.GetTaskId()], event.GetMessage())
		}
	}
	return out
}

// assertAgreement is E1's per-task check: the plan and the interpreter agree
// on every task of wf for this input and these stubs — and, with assumed
// failures, task-join-policy §6's table: a failure handler is held to its
// plan exactly; any other task may also have been CANCELED once the run
// started failing, because it may have been pending or running then.
func assertAgreement(t *testing.T, label string, plan *orkstackv1.PlanRunResponse, run *orkstackv1.Run, h *harness, runID string) {
	t.Helper()
	failing := plan.GetFailsAt()
	if failing == "" {
		// An older server: the first FAILS.
		for _, task := range plan.GetTasks() {
			if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS {
				failing = task.GetTaskId()
				break
			}
		}
	}
	isHandler := func(p *orkstackv1.PlanTask) bool {
		return p.GetJoin() == orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED ||
			p.GetJoin() == orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE
	}
	hasHandler := false
	for _, task := range plan.GetTasks() {
		hasHandler = hasHandler || isHandler(task)
	}
	skips := skipMessages(h, runID)
	for _, task := range plan.GetTasks() {
		id := task.GetTaskId()
		got := statusOf(run, id)
		// Once the run started failing, a task that is not a failure handler
		// may have been pending or running and been canceled.
		mayBeCanceled := failing != "" && id != failing && !isHandler(task) && got == orkstackv1.Status_STATUS_CANCELED
		switch task.GetOutcome() {
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED:
			if got != orkstackv1.Status_STATUS_FAILED && !mayBeCanceled {
				t.Errorf("%s: %s planned ASSUMED_FAILED, interpreter %v", label, id, got)
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS:
			// With a FAILS task, a RUNS task may be cancelled in flight: the
			// plan does not model timing, and says so.
			if got != orkstackv1.Status_STATUS_SUCCEEDED && !mayBeCanceled {
				t.Errorf("%s: %s planned RUNS, interpreter %v", label, id, got)
			}
			// each_count set ⇔ the fan-out expands to exactly that many items.
			if task.EachCount != nil {
				if each := taskRunOf(t, run, id).GetEach(); !each.GetExpanded() || each.GetCount() != task.GetEachCount() {
					t.Errorf("%s: %s planned %d items, interpreter expanded %v", label, id, task.GetEachCount(), each)
				}
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
			if mayBeCanceled {
				continue
			}
			if got != orkstackv1.Status_STATUS_SKIPPED {
				t.Errorf("%s: %s planned SKIPPED, interpreter %v", label, id, got)
				continue
			}
			want := "task skipped: " + task.GetSkipReason()
			found := false
			for _, message := range skips[id] {
				if message == want {
					found = true
				}
			}
			if !found {
				t.Errorf("%s: %s skip events %q, want one equal to %q", label, id, skips[id], want)
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS, orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED:
			// A fan-out that FAILS ends FAILED with that message: its
			// expansion was refused (spec each-fan-out §5.12).
			if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS && task.GetEach() != nil {
				if got != orkstackv1.Status_STATUS_FAILED {
					t.Errorf("%s: fan-out %s planned FAILS, interpreter %v", label, id, got)
				}
				continue
			}
			// In a definition with a failure handler a broken condition
			// records its task FAILED (CONDITION_FAILED); without one, the
			// task is CANCELED exactly as before.
			if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS && hasHandler {
				tr := taskRunOf(t, run, id)
				if (got != orkstackv1.Status_STATUS_FAILED || tr.GetError().GetCode() != "CONDITION_FAILED") && !mayBeCanceled {
					t.Errorf("%s: %s planned FAILS, interpreter %v %v", label, id, got, tr.GetError())
				}
				continue
			}
			if got != orkstackv1.Status_STATUS_CANCELED && got != orkstackv1.Status_STATUS_PENDING {
				t.Errorf("%s: %s planned %v, interpreter %v", label, id, task.GetOutcome(), got)
			}
		default:
			t.Errorf("%s: %s has outcome %v", label, id, task.GetOutcome())
		}
	}
	if failing != "" {
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
			t.Errorf("%s: plan FAILS at %s, run status %v", label, failing, run.GetStatus())
		}
		// The run's error is the plan's for a decision-time failure; an
		// assumed failure's text is the run's own.
		if planTaskOf(t, plan, failing).GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS {
			want := planTaskOf(t, plan, failing).GetError().GetMessage()
			if got := run.GetError().GetMessage(); got != want {
				t.Errorf("%s: run error %q, plan error %q", label, got, want)
			}
		} else if got := run.GetError().GetDetails(); got != fmt.Sprintf("task %q", failing) {
			t.Errorf("%s: run error details %q, want the assumed failure %q", label, got, failing)
		}
	} else if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("%s: plan has no FAILS, run status %v (%s)", label, run.GetStatus(), run.GetError().GetMessage())
	}
	var summary []string
	for _, task := range plan.GetTasks() {
		summary = append(summary, fmt.Sprintf("%s=%s/%s", task.GetTaskId(),
			strings.TrimPrefix(task.GetOutcome().String(), "PLAN_OUTCOME_"), statusOf(run, task.GetTaskId())))
	}
	t.Logf("agreement %s: %s", label, strings.Join(summary, " "))
}

const diamondDocument = `
name: diamond
inputs:
  take_c:
    type: boolean
    default: false
tasks:
  - id: a
    uses: shell
    with: {command: "echo a"}
  - id: b
    uses: shell
    needs: [a]
    with: {command: "echo b"}
  - id: c
    uses: shell
    needs: [a]
    condition: "{{ .inputs.take_c }}"
    with: {command: "echo c"}
  - id: d
    uses: shell
    needs: [b, c]
    with: {command: "echo d"}
`

const forwardDocument = `
name: forward
tasks:
  - id: second
    uses: shell
    needs: [first]
    with: {command: "echo second"}
  - id: first
    uses: shell
    with: {command: "echo first"}
  - id: free
    uses: shell
    needs: []
    with: {command: "echo free"}
`

const skippedPinDocument = `
name: skipped-pin
inputs:
  build:
    type: boolean
pins:
  release: {}
tasks:
  - id: gate
    uses: shell
    with: {command: "echo gate"}
  - id: compile
    uses: shell
    needs: [gate]
    pin: release
    condition: "{{ .inputs.build }}"
    with: {command: "echo compile"}
  - id: package
    uses: shell
    needs: [compile]
    pin: release
    with: {command: "echo package"}
`

const outputConditionDocument = `
name: output-condition
tasks:
  - id: x
    uses: shell
    with: {command: "echo x"}
  - id: y
    uses: shell
    needs: [x]
    condition: "{{ .tasks.x.output.field }}"
    with: {command: "echo y"}
`

func compileExample(t *testing.T, name string) *orkstackv1.Orkflow {
	t.Helper()
	wf, err := dsl.CompileFile("../../examples/" + name)
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return wf
}

// E1: the plan and the real interpreter agree on every task.
func TestSimulate_AgreesWithTheInterpreter(t *testing.T) {
	cases := []struct {
		name    string
		wf      *orkstackv1.Orkflow
		input   map[string]any
		assumed map[string]map[string]any
	}{
		{"condition enabled", compile(t, conditionDocument), map[string]any{"enabled": true}, nil},
		{"condition disabled", compile(t, conditionDocument), map[string]any{"enabled": false}, nil},
		{"http-post status 200", compileExample(t, "http-post.ork.yaml"), nil,
			map[string]map[string]any{"publish": {"status": 200}}},
		{"http-post status 500", compileExample(t, "http-post.ork.yaml"), nil,
			map[string]map[string]any{"publish": {"status": 500}}},
		{"http-post no stub", compileExample(t, "http-post.ork.yaml"), nil, nil},
		{"advanced-demo defaults, flaky exit 0", compileExample(t, "advanced-demo.ork.yaml"), nil,
			map[string]map[string]any{"flaky": {"exitCode": 0}}},
		{"advanced-demo secret check, flaky exit 1", compileExample(t, "advanced-demo.ork.yaml"),
			map[string]any{"enable_secret_check": true},
			map[string]map[string]any{"flaky": {"exitCode": 1}}},
		{"diamond, c skipped", compile(t, diamondDocument), nil, nil},
		{"diamond, c runs", compile(t, diamondDocument), map[string]any{"take_c": true}, nil},
		{"forward reference", compile(t, forwardDocument), nil, nil},
		{"pin with every member skipped", compile(t, skippedPinDocument), map[string]any{"build": false}, nil},
		{"output field supplied", compile(t, outputConditionDocument), nil,
			map[string]map[string]any{"x": {"field": true}}},
		{"output stub absent", compile(t, outputConditionDocument), nil, nil},
		{"output stub without the field", compile(t, outputConditionDocument), nil,
			map[string]map[string]any{"x": {"other": true}}},
		{"condition error", compile(t, conditionErrorDocument), map[string]any{"a": "x", "b": 1}, nil},
		{"fan-out over an input", compile(t, simulateEachInputDocument), map[string]any{"regions": []any{"eu", "us", "ap"}}, nil},
		{"fan-out over an input, too many", compile(t, simulateEachInputDocument), map[string]any{"regions": []any{"a", "b", "c", "d", "e"}}, nil},
		{"fan-out over a stubbed upstream", compile(t, simulateEachTaskDocument), nil,
			map[string]map[string]any{"list": {"parts": []any{map[string]any{"n": 1}, map[string]any{"n": 2}}}}},
		{"fan-out over a stubbed upstream, not an array", compile(t, simulateEachTaskDocument), nil,
			map[string]map[string]any{"list": {"parts": "nope"}}},
		{"cached pipeline", compileExample(t, "cached-pipeline.ork.yaml"), nil,
			map[string]map[string]any{"stamp": {"value": "1759600000000000000"}}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runID := fmt.Sprintf("run-simulate-agree-%d", i)
			input := payload(t, tc.input)
			plan := simulate(t, tc.wf, input, assumedPayloads(t, tc.assumed))
			run, h := interpret(t, runID, tc.wf, input, tc.assumed)
			assertAgreement(t, tc.name, plan, run, h, runID)
		})
	}
}

const wavesDocument = `
name: waves
tasks:
  - id: build
    uses: shell
    with: {command: "echo"}
  - id: lint
    uses: shell
    needs: [build]
    with: {command: "echo"}
  - id: test
    uses: shell
    needs: [build]
    with: {command: "echo"}
  - id: sign
    uses: shell
    needs: [lint, test]
    with: {command: "echo"}
  - id: docs
    uses: shell
    with: {command: "echo"}
  - id: publish
    uses: shell
    needs: [sign, docs]
    with: {command: "echo"}
`

// E2: a wave is one more than the deepest need.
func TestSimulate_WaveIsOneMoreThanTheDeepestNeed(t *testing.T) {
	plan := simulate(t, compile(t, wavesDocument), nil, nil)
	want := map[string]int32{"build": 1, "lint": 2, "test": 2, "sign": 3, "docs": 1, "publish": 4}
	for id, wave := range want {
		task := planTaskOf(t, plan, id)
		if task.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
			t.Errorf("%s outcome %v, want RUNS", id, task.GetOutcome())
		}
		if task.GetWave() != wave {
			t.Errorf("%s wave %d, want %d", id, task.GetWave(), wave)
		}
	}
	if plan.GetWaveCount() != 4 {
		t.Errorf("wave_count %d, want 4", plan.GetWaveCount())
	}
	wantWaves := [][]string{{"build", "docs"}, {"lint", "test"}, {"sign"}, {"publish"}}
	if len(plan.GetWaves()) != len(wantWaves) {
		t.Fatalf("waves %v, want %v", plan.GetWaves(), wantWaves)
	}
	for i, w := range plan.GetWaves() {
		if w.GetWave() != int32(i+1) || !reflect.DeepEqual(w.GetTaskIds(), wantWaves[i]) {
			t.Errorf("waves[%d] = %d %v, want %d %v", i, w.GetWave(), w.GetTaskIds(), i+1, wantWaves[i])
		}
	}
	if plan.GetRuns() != 6 || plan.GetSkipped() != 0 {
		t.Errorf("runs/skipped = %d/%d, want 6/0", plan.GetRuns(), plan.GetSkipped())
	}
}

const smokeDocument = `
name: smoke
inputs:
  region:
    type: string
tasks:
  - id: build
    uses: shell
    with: {command: "echo"}
  - id: test
    uses: shell
    needs: [build]
    with: {command: "echo"}
  - id: sign
    uses: shell
    needs: [test]
    with: {command: "echo"}
  - id: deploy-eu
    uses: shell
    needs: [sign]
    condition: '{{ eq .inputs.region "eu" }}'
    with: {command: "echo"}
  - id: deploy-us
    uses: shell
    needs: [sign]
    condition: '{{ eq .inputs.region "us" }}'
    with: {command: "echo"}
  - id: smoke-test
    uses: shell
    needs: [deploy-us]
    with: {command: "echo"}
`

// E3: a skipped task keeps the wave it would have had and is not counted.
func TestSimulate_SkippedTaskKeepsItsWouldBeWaveAndIsNotCounted(t *testing.T) {
	plan := simulate(t, compile(t, smokeDocument), payload(t, map[string]any{"region": "eu"}), nil)
	smoke := planTaskOf(t, plan, "smoke-test")
	if smoke.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED || smoke.GetWave() != 5 {
		t.Fatalf("smoke-test = %v wave %d, want SKIPPED wave 5", smoke.GetOutcome(), smoke.GetWave())
	}
	if want := `upstream task "deploy-us" was skipped`; smoke.GetSkipReason() != want || smoke.GetSkippedBy() != "deploy-us" {
		t.Errorf("smoke-test reason %q by %q, want %q by deploy-us", smoke.GetSkipReason(), smoke.GetSkippedBy(), want)
	}
	if plan.GetWaveCount() != 4 {
		t.Errorf("wave_count %d, want 4", plan.GetWaveCount())
	}
	for _, w := range plan.GetWaves() {
		if w.GetWave() == 5 {
			t.Errorf("waves has an entry 5: %v", w.GetTaskIds())
		}
	}
	if len(plan.GetWaves()) != 4 {
		t.Errorf("waves %v, want 4 entries", plan.GetWaves())
	}
	if plan.GetRuns() != 4 || plan.GetSkipped() != 2 {
		t.Errorf("runs/skipped = %d/%d, want 4/2", plan.GetRuns(), plan.GetSkipped())
	}
}

const twoSkippedNeedsDocument = `
name: two-skipped
inputs:
  on:
    type: boolean
tasks:
  - id: x
    uses: shell
    condition: "{{ .inputs.on }}"
    with: {command: "echo"}
  - id: y
    uses: shell
    condition: "{{ .inputs.on }}"
    with: {command: "echo"}
  - id: z
    uses: shell
    needs: [x, y]
    with: {command: "echo"}
`

// E4: an upstream skip names the first skipped need in needs: order.
func TestSimulate_UpstreamSkipNamesTheFirstSkippedNeed(t *testing.T) {
	wf := compile(t, twoSkippedNeedsDocument)
	input := payload(t, map[string]any{"on": false})
	plan := simulate(t, wf, input, nil)
	z := planTaskOf(t, plan, "z")
	if z.GetSkippedBy() != "x" || z.GetSkipReason() != `upstream task "x" was skipped` {
		t.Fatalf("z skipped_by %q reason %q, want x", z.GetSkippedBy(), z.GetSkipReason())
	}
	if len(z.GetAssumedAbsent()) != 0 {
		t.Errorf("z assumed_absent %v, want empty", z.GetAssumedAbsent())
	}
	run, h := interpret(t, "run-simulate-first-skipped", wf, input, nil)
	assertAgreement(t, "two skipped needs", plan, run, h, "run-simulate-first-skipped")
}

const conditionErrorDocument = `
name: condition-error
inputs:
  a:
    type: string
  b:
    type: number
tasks:
  - id: first
    uses: shell
    with: {command: "echo"}
  - id: broken
    uses: shell
    needs: [first]
    condition: "{{ eq .inputs.a .inputs.b }}"
    with: {command: "echo"}
  - id: after
    uses: shell
    needs: [broken]
    with: {command: "echo"}
  - id: later
    uses: shell
    needs: [after]
    with: {command: "echo"}
`

// E5: a condition that errors FAILS the run and leaves the rest UNREACHED.
func TestSimulate_ConditionErrorFailsAndMarksUnreached(t *testing.T) {
	wf := compile(t, conditionErrorDocument)
	input := payload(t, map[string]any{"a": "x", "b": 1})
	plan := simulate(t, wf, input, nil)

	broken := planTaskOf(t, plan, "broken")
	if broken.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS || broken.GetWave() != 0 {
		t.Fatalf("broken = %v wave %d, want FAILS wave 0", broken.GetOutcome(), broken.GetWave())
	}
	if broken.GetError().GetCode() != "RUN_FAILED" || !strings.Contains(broken.GetError().GetMessage(), "incompatible types for comparison") {
		t.Fatalf("broken error = %v, want RUN_FAILED incompatible types", broken.GetError())
	}
	for _, id := range []string{"after", "later"} {
		task := planTaskOf(t, plan, id)
		if task.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED || task.GetWave() != 0 {
			t.Errorf("%s = %v wave %d, want UNREACHED wave 0", id, task.GetOutcome(), task.GetWave())
		}
	}
	if plan.GetRuns() != 1 || plan.GetSkipped() != 0 || plan.GetWaveCount() != 1 {
		t.Errorf("runs/skipped/wave_count = %d/%d/%d, want 1/0/1", plan.GetRuns(), plan.GetSkipped(), plan.GetWaveCount())
	}

	if plan.GetFailsAt() != "broken" {
		t.Errorf("fails_at = %q, want broken", plan.GetFailsAt())
	}
	run, h := interpret(t, "run-simulate-condition-error", wf, input, nil)
	if run.GetError().GetMessage() != broken.GetError().GetMessage() {
		t.Fatalf("run error %q, plan error %q", run.GetError().GetMessage(), broken.GetError().GetMessage())
	}
	t.Logf("E5 run error == plan error: %q", run.GetError().GetMessage())
	for _, id := range []string{"after", "later"} {
		if got := statusOf(run, id); got != orkstackv1.Status_STATUS_CANCELED {
			t.Errorf("interpreter %s = %v, want CANCELED", id, got)
		} else {
			t.Logf("E5 %s planned UNREACHED, interpreter %v", id, got)
		}
	}
	assertAgreement(t, "condition error", plan, run, h, "run-simulate-condition-error")
}

// E6: an assumed output drives a downstream condition both ways.
func TestSimulate_AssumedOutputDrivesAnUpstreamCondition(t *testing.T) {
	wf := compile(t, outputConditionDocument)
	on := simulate(t, wf, nil, assumedPayloads(t, map[string]map[string]any{"x": {"field": true}}))
	off := simulate(t, wf, nil, assumedPayloads(t, map[string]map[string]any{"x": {"field": false}}))
	if got := planTaskOf(t, on, "y"); got.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || len(got.GetAssumedAbsent()) != 0 {
		t.Errorf("y with field=true: %v absent %v, want RUNS, none absent", got.GetOutcome(), got.GetAssumedAbsent())
	}
	if got := planTaskOf(t, off, "y"); got.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED || len(got.GetAssumedAbsent()) != 0 {
		t.Errorf("y with field=false: %v absent %v, want SKIPPED, none absent", got.GetOutcome(), got.GetAssumedAbsent())
	}
}

const publishDocument = `
name: publish-gate
inputs:
  go:
    type: boolean
tasks:
  - id: publish
    uses: shell
    with: {command: "echo"}
  - id: notify
    uses: shell
    needs: [publish]
    condition: "{{ .tasks.publish.output.ok }}"
    with: {command: "echo"}
  - id: gate
    uses: shell
    condition: "{{ .inputs.go }}"
    with: {command: "echo"}
  - id: tail
    uses: shell
    needs: [gate, publish]
    condition: "{{ .tasks.publish.output.ok }}"
    with: {command: "echo"}
`

// E7: an unsupplied upstream output is {} and is reported.
func TestSimulate_UnsuppliedUpstreamOutputIsEmptyAndReported(t *testing.T) {
	wf := compile(t, publishDocument)
	input := payload(t, map[string]any{"go": false})
	plan := simulate(t, wf, input, nil)
	notify := planTaskOf(t, plan, "notify")
	if notify.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
		t.Errorf("notify = %v, want SKIPPED (an empty output renders falsy)", notify.GetOutcome())
	}
	if !reflect.DeepEqual(notify.GetAssumedAbsent(), []string{"publish"}) {
		t.Errorf("notify assumed_absent %v, want [publish]", notify.GetAssumedAbsent())
	}
	tail := planTaskOf(t, plan, "tail")
	if tail.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED || tail.GetSkippedBy() != "gate" {
		t.Errorf("tail = %v by %q, want SKIPPED by gate", tail.GetOutcome(), tail.GetSkippedBy())
	}
	if len(tail.GetAssumedAbsent()) != 0 {
		t.Errorf("tail assumed_absent %v, want empty for an upstream skip", tail.GetAssumedAbsent())
	}
	run, h := interpret(t, "run-simulate-unsupplied", wf, input, nil)
	assertAgreement(t, "unsupplied output", plan, run, h, "run-simulate-unsupplied")
}

const defaultsDocument = `
name: defaults
inputs:
  enabled:
    type: boolean
    default: true
tasks:
  - id: maybe
    uses: shell
    condition: "{{ .inputs.enabled }}"
    with: {command: "echo"}
`

// E8: declared defaults apply; an explicit null is not replaced.
func TestSimulate_AppliesDeclaredInputDefaults(t *testing.T) {
	wf := compile(t, defaultsDocument)
	plan := simulate(t, wf, nil, nil)
	if got := planTaskOf(t, plan, "maybe").GetOutcome(); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Errorf("maybe with the default = %v, want RUNS", got)
	}
	if v, ok := plan.GetInput().GetData().GetFields()["enabled"]; !ok || !v.GetBoolValue() {
		t.Errorf("effective input %v, want enabled=true from the default", plan.GetInput())
	}

	explicitNull := payload(t, map[string]any{"enabled": nil})
	nulled := simulate(t, wf, explicitNull, nil)
	if got := planTaskOf(t, nulled, "maybe").GetOutcome(); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
		t.Errorf("maybe with explicit null = %v, want SKIPPED", got)
	}
	if v := nulled.GetInput().GetData().GetFields()["enabled"]; v.GetNullValue() != 0 || v.GetKind() == nil {
		t.Errorf("effective input %v, want enabled=null kept", nulled.GetInput())
	}
	run, h := interpret(t, "run-simulate-null", wf, explicitNull, nil)
	assertAgreement(t, "explicit null", nulled, run, h, "run-simulate-null")
}

// E9: an unused stub is reported; a stub for a non-task is refused.
func TestSimulate_ReportsUnusedAndRefusesUnknownAssumedOutputs(t *testing.T) {
	wf := compile(t, conditionDocument)
	plan := simulate(t, wf, payload(t, map[string]any{"enabled": false}),
		assumedPayloads(t, map[string]map[string]any{"maybe": {"x": 1}, "after": {"y": 2}, "always": {"z": 3}}))
	if !reflect.DeepEqual(plan.GetUnusedAssumedOutputs(), []string{"after", "maybe"}) {
		t.Errorf("unused_assumed_outputs %v, want [after maybe]", plan.GetUnusedAssumedOutputs())
	}

	_, err := engine.Simulate(wf, nil, assumedPayloads(t, map[string]map[string]any{"nope": {}}), nil)
	var unknown *engine.UnknownAssumedTaskError
	if !errors.As(err, &unknown) || unknown.TaskID != "nope" {
		t.Fatalf("Simulate with an unknown stub = %v, want *UnknownAssumedTaskError naming nope", err)
	}
	if !strings.Contains(err.Error(), `"nope"`) {
		t.Errorf("error %q does not name the key", err)
	}
}

// E10: Simulate is pure and mutates nothing.
func TestSimulate_IsPureAndMutatesNothing(t *testing.T) {
	wf := compile(t, publishDocument)
	input := payload(t, map[string]any{"go": true})
	assumed := assumedPayloads(t, map[string]map[string]any{"publish": {"ok": true}})

	wfClone := proto.Clone(wf).(*orkstackv1.Orkflow)
	inputClone := proto.Clone(input).(*orkstackv1.Payload)
	assumedClone := map[string]*orkstackv1.Payload{}
	for k, v := range assumed {
		assumedClone[k] = proto.Clone(v).(*orkstackv1.Payload)
	}

	first := simulate(t, wf, input, assumed)
	second := simulate(t, wf, input, assumed)
	if !proto.Equal(first, second) {
		t.Fatalf("two calls differ:\n%v\n%v", first, second)
	}
	if !proto.Equal(wf, wfClone) {
		t.Error("Simulate mutated the orkflow")
	}
	if !proto.Equal(input, inputClone) {
		t.Error("Simulate mutated the input")
	}
	keys := make([]string, 0, len(assumed))
	for k := range assumed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(assumed) != len(assumedClone) {
		t.Errorf("Simulate changed the assumed map's keys: %v", keys)
	}
	for _, k := range keys {
		if !proto.Equal(assumed[k], assumedClone[k]) {
			t.Errorf("Simulate mutated assumed[%q]", k)
		}
	}
	// The response's input is not the caller's input object.
	if first.GetInput() == input {
		t.Error("response input aliases the request input")
	}
}

// E11: the interpreter's skip events are spelled by the shared helpers.
func TestClassify_UsesTheSharedSkipReasons(t *testing.T) {
	wf := compile(t, conditionDocument)
	run, h := interpret(t, "run-classify-reasons", wf, payload(t, map[string]any{"enabled": false}), nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run status %v", run.GetStatus())
	}
	skips := skipMessages(h, "run-classify-reasons")
	wantMaybe := "task skipped: " + engine.SkipReasonCondition("{{ .inputs.enabled }}")
	wantAfter := "task skipped: " + engine.SkipReasonUpstream("maybe")
	if !reflect.DeepEqual(skips["maybe"], []string{wantMaybe}) {
		t.Errorf("maybe skip events %q, want [%q]", skips["maybe"], wantMaybe)
	}
	if !reflect.DeepEqual(skips["after"], []string{wantAfter}) {
		t.Errorf("after skip events %q, want [%q]", skips["after"], wantAfter)
	}
	if wantMaybe != `task skipped: condition "{{ .inputs.enabled }}" evaluated false` ||
		wantAfter != `task skipped: upstream task "maybe" was skipped` {
		t.Errorf("shared helpers changed the sentence: %q / %q", wantMaybe, wantAfter)
	}
}

const simulateEachInputDocument = `
name: simulate-each-input
inputs:
  regions: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 4, max_parallel: 2}
    with: {op: exec}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec}
`

const simulateEachTaskDocument = `
name: simulate-each-task
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 50}
    with: {op: exec}
`

func TestSimulate_EachCountsAnInputSource(t *testing.T) {
	plan := simulate(t, compile(t, simulateEachInputDocument), payload(t, map[string]any{"regions": []any{"eu", "us", "ap"}}), nil)
	convert := planTaskOf(t, plan, "convert")
	if convert.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || convert.EachCount == nil || convert.GetEachCount() != 3 {
		t.Fatalf("convert = %v", convert)
	}
	if convert.GetEach().GetMaxItems() != 4 || convert.GetEach().GetMaxParallel() != 2 {
		t.Fatalf("PlanTask.each = %v", convert.GetEach())
	}
	if len(plan.GetTasks()) != 2 || planTaskOf(t, plan, "merge").GetWave() != 2 {
		t.Fatalf("a fan-out is one PlanTask taking one wave: %v", plan.GetTasks())
	}
}

func TestSimulate_EachCountsAStubbedSource(t *testing.T) {
	plan := simulate(t, compile(t, simulateEachTaskDocument), nil, assumedPayloads(t, map[string]map[string]any{
		"list":    {"parts": []any{1.0, 2.0}},
		"convert": {"ignored": true},
	}))
	if got := planTaskOf(t, plan, "convert"); got.EachCount == nil || got.GetEachCount() != 2 {
		t.Fatalf("convert = %v", got)
	}
	if fmt.Sprint(plan.GetUnusedAssumedOutputs()) != "[convert]" {
		t.Fatalf("a fan-out's own stub must be listed unused: %v", plan.GetUnusedAssumedOutputs())
	}
}

func TestSimulate_EachLeavesTheCountUnsetOtherwise(t *testing.T) {
	plan := simulate(t, compile(t, simulateEachTaskDocument), nil, nil)
	got := planTaskOf(t, plan, "convert")
	if got.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || got.EachCount != nil || got.GetEach() == nil {
		t.Fatalf("convert = %v", got)
	}
}

func TestSimulate_EachTooManyItemsIsFails(t *testing.T) {
	plan := simulate(t, compile(t, simulateEachInputDocument), payload(t, map[string]any{"regions": []any{"a", "b", "c", "d", "e"}}), nil)
	convert := planTaskOf(t, plan, "convert")
	if convert.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS || convert.GetWave() != 0 ||
		convert.GetError().GetCode() != "RUN_FAILED" ||
		convert.GetError().GetMessage() != "each.over resolved to 5 items, over max_items 4 declared at tasks[0].each.max_items" {
		t.Fatalf("convert = %v", convert)
	}
	if got := planTaskOf(t, plan, "merge").GetOutcome(); got != orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED {
		t.Fatalf("merge = %v", got)
	}
}
