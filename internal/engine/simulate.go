package engine

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// UnknownAssumedTaskError names an assumed_outputs key that is not a task.
type UnknownAssumedTaskError struct{ TaskID string }

func (e *UnknownAssumedTaskError) Error() string {
	return fmt.Sprintf("assumed_outputs[%q]: not a task of this orkflow", e.TaskID)
}

// Simulate walks wf's DAG for input exactly as RunWorkflow would, deciding
// each task with evaluateCondition, and reports what the interpreter would
// do with every task. It executes nothing.
//
// It is a pure function of its arguments: no clock, no IO, no randomness,
// no process configuration, and no mutation of wf, input, or assumed.
// Calling it twice with equal arguments returns proto-equal responses.
//
// assumed stubs the values.json of tasks that would run, keyed by task id;
// a key that is not a task of wf is *UnknownAssumedTaskError. A running
// task with no entry is given an empty output map, which is what collect
// records for a task whose values.json decodes to nothing.
//
// Simulate is not workflow code and RunWorkflow never calls it. What it
// shares with the interpreter is exactly what decides a task: the defaulted
// inputs (ApplyInputDefaults, payloadMap), the needs closure
// (transitiveNeedsOf), the evaluator (evaluateCondition), and the reason
// sentences (skipReasonUpstream, skipReasonCondition).
func Simulate(
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	assumed map[string]*orkstackv1.Payload,
) (*orkstackv1.PlanRunResponse, error) {
	if wf == nil || len(wf.GetTasks()) == 0 {
		return nil, fmt.Errorf("orkflow has no tasks")
	}

	var order []string
	tasks := make(map[string]*orkstackv1.Task, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		order = append(order, t.GetId())
		tasks[t.GetId()] = t
	}
	assumedKeys := make([]string, 0, len(assumed))
	for id := range assumed {
		assumedKeys = append(assumedKeys, id)
	}
	sort.Strings(assumedKeys)
	for _, id := range assumedKeys {
		if _, ok := tasks[id]; !ok {
			return nil, &UnknownAssumedTaskError{TaskID: id}
		}
	}

	effective := ApplyInputDefaults(wf, input)
	inputs := payloadMap(effective)

	outputs := map[string]map[string]any{}
	decided := map[string]struct{}{}
	result := make(map[string]*orkstackv1.PlanTask, len(order))
	for _, id := range order {
		result[id] = &orkstackv1.PlanTask{
			TaskId:    id,
			Condition: tasks[id].GetCondition(),
			Pin:       tasks[id].GetPin(),
			// Set for every await whatever its outcome, so a reader sees the
			// deadline and whether it takes a callback.
			Await: tasks[id].GetAwait(),
			// Set for every fan-out whatever its outcome (spec each-fan-out
			// §5.12).
			Each: tasks[id].GetEach(),
		}
	}
	taskIndex := make(map[string]int, len(order))
	for i, id := range order {
		taskIndex[id] = i
	}
	waveAfter := func(id string) int32 {
		var deepest int32
		for _, need := range tasks[id].GetNeeds() {
			if w := result[need].GetWave(); w > deepest {
				deepest = w
			}
		}
		return deepest + 1
	}

	failed := ""
	for {
		progressed := false
		// Declaration order, as readyTasks scans; the outer loop re-scans
		// because a task may need one declared after it.
		for _, id := range order {
			if _, done := decided[id]; done {
				continue
			}
			ready := true
			for _, need := range tasks[id].GetNeeds() {
				if _, done := decided[need]; !done {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			progressed = true
			decided[id] = struct{}{}
			planned := result[id]

			if failed != "" {
				// The run is already failing: the interpreter marks every
				// still-pending task CANCELED.
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED
				continue
			}

			// (a) classify's first loop: the first skipped need, in needs:
			// order, wins.
			skippedBy := ""
			for _, need := range tasks[id].GetNeeds() {
				if result[need].GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
					skippedBy = need
					break
				}
			}
			if skippedBy != "" {
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED
				planned.SkipReason = skipReasonUpstream(skippedBy)
				planned.SkippedBy = skippedBy
				planned.Wave = waveAfter(id)
				continue
			}

			// (b) the condition, against exactly what classify passes: the
			// outputs of RUNS ancestors in the closure.
			upstream := map[string]map[string]any{}
			for _, need := range transitiveNeedsOf(tasks, id) {
				if values, ok := outputs[need]; ok {
					upstream[need] = values
				}
			}
			ok, err := evaluateCondition(tasks[id].GetCondition(), inputs, upstream)
			if err != nil {
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS
				planned.Error = &orkstackv1.Error{Code: "RUN_FAILED", Message: err.Error()}
				failed = id
				continue
			}
			planned.Wave = waveAfter(id)
			if tasks[id].GetCondition() != "" {
				// Advisory only: which upstream outputs the condition read
				// that the caller did not stub. A parse error cannot reach
				// here — evaluateCondition parsed the same text.
				refs, _ := dsl.ReferencedUpstream(tasks[id].GetCondition())
				for _, ref := range refs {
					if _, stubbed := assumed[ref]; !stubbed {
						planned.AssumedAbsent = append(planned.AssumedAbsent, ref)
					}
				}
			}
			if !ok {
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED
				planned.SkipReason = skipReasonCondition(tasks[id].GetCondition())
				continue
			}
			if spec := tasks[id].GetEach(); spec != nil {
				// A fan-out RUNS as one PlanTask. Its items are counted when
				// the plan can already count them — an input source, or a
				// source task whose values assumed_outputs stubs — with the
				// interpreter's own count, so the two cannot disagree.
				counted, n, eerr := simulateEachCount(spec, inputs, assumed, taskIndex[id])
				if eerr != nil {
					planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS
					planned.Wave = 0
					planned.Error = &orkstackv1.Error{Code: "RUN_FAILED", Message: eerr.Message}
					failed = id
					continue
				}
				if counted {
					planned.EachCount = proto.Int32(n)
				}
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS
				// outputs[id] is NOT set: rule 24 means no condition can read it.
				continue
			}
			if tasks[id].GetAwait() != nil {
				// Opens, starts no activity: the interpreter's openAwait.
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS
			} else {
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS
			}
			// An await's stub is its resolution.
			// payloadMap(nil) is {}: a task with no stub has an empty output,
			// as collect records for a values.json that decodes to nothing.
			outputs[id] = payloadMap(assumed[id])
		}
		if !progressed {
			break
		}
	}
	if len(decided) != len(order) {
		// Unreachable for a compiled document (checkAcyclic), refused rather
		// than answered with undecided tasks.
		return nil, fmt.Errorf("orkflow %s@%s has tasks that can never become ready", wf.GetName(), wf.GetVersion())
	}

	resp := &orkstackv1.PlanRunResponse{Input: proto.Clone(effective).(*orkstackv1.Payload)}
	for _, id := range order {
		planned := result[id]
		resp.Tasks = append(resp.Tasks, planned)
		switch planned.GetOutcome() {
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS:
			resp.Runs++
			if planned.GetWave() > resp.WaveCount {
				resp.WaveCount = planned.GetWave()
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS:
			resp.Awaits++
			if planned.GetWave() > resp.WaveCount {
				resp.WaveCount = planned.GetWave()
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
			resp.Skipped++
		}
	}
	for w := int32(1); w <= resp.WaveCount; w++ {
		wave := &orkstackv1.PlanWave{Wave: w}
		for _, id := range order {
			if takesAWave(result[id].GetOutcome()) && result[id].GetWave() == w {
				wave.TaskIds = append(wave.TaskIds, id)
			}
		}
		resp.Waves = append(resp.Waves, wave)
	}
	for _, id := range assumedKeys {
		// A fan-out's own stub is accepted and unused: nothing can read it.
		if !takesAWave(result[id].GetOutcome()) || tasks[id].GetEach() != nil {
			resp.UnusedAssumedOutputs = append(resp.UnusedAssumedOutputs, id)
		}
	}
	return resp, nil
}

// simulateEachCount counts a fan-out's array when the plan can: an input
// source from the defaulted input, a task source from its assumed_outputs
// stub. counted is false when the count is a run-time fact.
func simulateEachCount(
	spec *orkstackv1.EachSpec,
	inputs map[string]any,
	assumed map[string]*orkstackv1.Payload,
	taskIndex int,
) (bool, int32, *EachError) {
	source := spec.GetSource()
	var (
		n    int32
		eerr *EachError
	)
	switch {
	case source.GetInput() != "":
		n, eerr = eachCountOfInput(inputs, spec)
	case source.GetTaskId() != "":
		stub, ok := assumed[source.GetTaskId()]
		if !ok {
			return false, 0, nil
		}
		n, eerr = EachArrayLength(payloadMap(stub), source.GetPath(), spec.GetOver())
	default:
		return false, 0, nil
	}
	if eerr == nil {
		eerr = eachTooMany(n, spec, taskIndex)
	}
	if eerr != nil {
		return false, 0, eerr
	}
	return true, n, nil
}

// takesAWave is RUNS or AWAITS: an await occupies a dispatch round from its
// dependants' point of view, and its stub is consumed as its resolution.
func takesAWave(outcome orkstackv1.PlanOutcome) bool {
	return outcome == orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS ||
		outcome == orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS
}
