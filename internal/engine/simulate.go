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
// sentences (skipReasonUpstream, skipReasonCondition), and for a join the
// predicate and its sentences (joinAdmits).
//
// assumeFailed plans each named task, when it would run, as having FAILED
// after its retries (ASSUMED_FAILED): its failure handlers are planned, every
// task that is not a failure handler and transitively needs it is UNREACHED,
// and independent work is planned as if nothing had failed. Entries whose
// task does not run are reported in unused_assume_failed. nil plans no
// failure and returns a response proto-equal to the one before assumptions
// existed.
func Simulate(
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	assumed map[string]*orkstackv1.Payload,
	assumeFailed []string,
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
			// Set for every fan-out whatever its outcome.
			Each: tasks[id].GetEach(),
			// Set for every composed task whatever its outcome. A plan never
			// recurses into the child: a composed task that would run is RUNS,
			// and its stub stands in for the child's declared outputs.
			Orkflow: tasks[id].GetOrkflow(),
			// Set for every cached task whatever its outcome. A plan never
			// predicts a hit: that is decided on the Orker, from the rendered
			// config and the input digests, when the run reaches it.
			Cache: tasks[id].GetCache(),
			// Set for every task with a join other than all_succeeded.
			Join: tasks[id].GetJoin(),
		}
	}
	assumedFailed := make(map[string]bool, len(assumeFailed))
	for _, id := range assumeFailed {
		assumedFailed[id] = true
	}
	// planOutcomeAsNeed is how a join reads a decided need's plan outcome:
	// a task the plan dispatches is planned to succeed, a FAILS or an
	// assumption is a failure, an UNREACHED task is the CANCELED the
	// interpreter records for it.
	planOutcomeAsNeed := func(id string) needOutcome {
		switch result[id].GetOutcome() {
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS, orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS:
			return needSucceeded
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS, orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED:
			return needFailed
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
			return needSkipped
		default:
			return needCanceled
		}
	}
	// firstAssumedFailedAncestor is the first ASSUMED_FAILED task of id's
	// needs closure in declaration order, or "".
	firstAssumedFailedAncestor := func(id string) string {
		closure := map[string]bool{}
		for _, need := range transitiveNeedsOf(tasks, id) {
			closure[need] = true
		}
		for _, candidate := range order {
			if closure[candidate] && result[candidate].GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED {
				return candidate
			}
		}
		return ""
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

	// failingAt is where the planned run starts failing: the first FAILS or
	// ASSUMED_FAILED in decision order. cutBy is the first decision-time
	// failure — a FAILS that set failingAt, or any FAILS of a task that is not
	// a failure handler — after which every task that is not a failure
	// handler is UNREACHED, today's scan-order rule. A task failure happens
	// after dispatch, so an assumption cuts only the work that depends on it.
	failingAt, cutBy := "", ""
	// failsWith records a FAILS: its error, and the two facts above.
	failsWith := func(id string, planned *orkstackv1.PlanTask, code, message string) {
		planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS
		planned.Wave = 0
		if failingAt == "" {
			planned.Error = &orkstackv1.Error{Code: "RUN_FAILED", Message: message}
			failingAt = id
			if cutBy == "" {
				cutBy = id
			}
			return
		}
		planned.Error = &orkstackv1.Error{Code: code, Message: message}
		if cutBy == "" && !isFailureHandler(tasks[id]) {
			cutBy = id
		}
	}
	assumeFailure := func(id string, planned *orkstackv1.PlanTask) {
		planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED
		planned.Wave = waveAfter(id)
		planned.Error = &orkstackv1.Error{Code: "ASSUMED_FAILED", Message: "assumed to fail (assume_failed)"}
		if failingAt == "" {
			failingAt = id
		}
	}
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

			// (0) A task that is not a failure handler is certainly never
			// started once a decision-time failure cut the run, or when it
			// depends on an assumed failure: the interpreter marks it
			// CANCELED. A failure handler is never UNREACHED.
			if !isFailureHandler(tasks[id]) {
				by := cutBy
				if by == "" {
					by = firstAssumedFailedAncestor(id)
				}
				if by != "" {
					planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED
					planned.UnreachedBy = by
					continue
				}
			}

			// (a) The join. all_succeeded is classify's first loop: the first
			// skipped need, in needs: order, wins.
			if tasks[id].GetJoin() == orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
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
			} else if ok, reason := joinAdmits(tasks[id].GetJoin(), tasks[id].GetNeeds(), planOutcomeAsNeed); !ok {
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED
				planned.SkipReason = reason
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
				failsWith(id, planned, "CONDITION_FAILED", err.Error())
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
				// A task source the plan decided not to run has no values: the
				// interpreter fails the node EACH_OVER_MISSING (a none_failed
				// fan-out reached with its source skipped).
				if src := spec.GetSource().GetTaskId(); src != "" && tasks[src] != nil &&
					!dispatched(result[src].GetOutcome()) {
					failsWith(id, planned, EachOverMissingCode, eachOverMissingMessage(
						spec.GetOver(), src, planSourceStatus(result[src].GetOutcome())))
					continue
				}
				counted, n, eerr := simulateEachCount(spec, inputs, assumed, taskIndex[id])
				if eerr != nil {
					failsWith(id, planned, eerr.Code, eerr.Message)
					continue
				}
				if counted {
					planned.EachCount = proto.Int32(n)
				}
				if assumedFailed[id] {
					assumeFailure(id, planned)
					continue
				}
				planned.Outcome = orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS
				// outputs[id] is NOT set: rule 24 means no condition can read it.
				continue
			}
			// (c) The assumption: a task that would run is planned as having
			// run and FAILED. It contributes no output.
			if assumedFailed[id] {
				assumeFailure(id, planned)
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
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED:
			resp.AssumedFailed++
			if planned.GetWave() > resp.WaveCount {
				resp.WaveCount = planned.GetWave()
			}
		case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
			resp.Skipped++
		}
	}
	resp.FailsAt = failingAt
	for _, id := range assumeFailed {
		if result[id].GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED {
			resp.UnusedAssumeFailed = append(resp.UnusedAssumeFailed, id)
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
		// Nor is an ASSUMED_FAILED task's: a failed task has no output.
		if !dispatched(result[id].GetOutcome()) || tasks[id].GetEach() != nil {
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

// takesAWave is RUNS, AWAITS or ASSUMED_FAILED: an await occupies a dispatch
// round from its dependants' point of view, and so does a task assumed to
// fail — it was dispatched and ran.
func takesAWave(outcome orkstackv1.PlanOutcome) bool {
	return dispatched(outcome) || outcome == orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED
}

// dispatched is RUNS or AWAITS: the plan runs the task to success, and its
// stub (an await's resolution) is consumed.
func dispatched(outcome orkstackv1.PlanOutcome) bool {
	return outcome == orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS ||
		outcome == orkstackv1.PlanOutcome_PLAN_OUTCOME_AWAITS
}

// planSourceStatus is the status word a run records for a fan-out's task
// source the plan does not run.
func planSourceStatus(outcome orkstackv1.PlanOutcome) string {
	switch outcome {
	case orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED:
		return "SKIPPED"
	case orkstackv1.PlanOutcome_PLAN_OUTCOME_FAILS, orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED:
		return "FAILED"
	default:
		return "CANCELED"
	}
}
