package engine

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// This file composes ONE task's activity input outside a run, for
// `orkctl task run`. These are ordinary pure functions beside
// ApplyInputDefaults and DefaultLimits — not workflow code: nothing here is
// replayed, recorded, or scheduled, and workflow.go is not edited to call it.
//
// The composition is a second spelling of RunWorkflow's schedule, and a second
// spelling can drift. TestLocalTaskInput_AgreesWithTheInterpreter is what
// keeps it from doing so: it runs RunWorkflow, records every ExecuteTaskInput
// the interpreter schedules, and requires LocalTaskInput to reproduce each one
// exactly, except for the deliberate differences named on LocalTaskInput.
//
// A DSL node that makes the interpreter take a path other than schedule for a
// task (an awaited task, a fan-out, a composed orkflow, a cache hit) must
// state its local behavior and extend that test in the same change; until it
// does, LocalTaskInput must refuse such a task rather than compose an input
// the interpreter would never have scheduled. No such node exists in this
// tree's Task message.

// LocalTask names one task of one definition to be composed OUTSIDE a run:
// the caller runs it itself (`orkctl task run`), so nothing here is replayed,
// recorded, or scheduled.
type LocalTask struct {
	// Orkflow is the compiled definition, images resolved and digest stamped,
	// exactly as ValidateOrkflow or GetOrkflow return it.
	Orkflow *orkstackv1.Orkflow
	TaskID  string
	// RunID is the run id the task is composed under. `orkctl task run`
	// passes a fresh LOCAL id and never a real run's; the parity test passes
	// the workflow's own, which is what makes the comparison exact.
	RunID string
	// Input is the run input BEFORE declared defaults; LocalTaskInput applies
	// ApplyInputDefaults itself, exactly as newRunState does.
	Input *orkstackv1.Payload
	// Upstream is every candidate ancestor result, keyed by task id. Only the
	// members of TaskClosure(Orkflow, TaskID) are used; every other key is
	// ignored, as RunWorkflow ignores completed tasks outside the closure.
	Upstream map[string]*orkstackv1.UpstreamRef
	// Limits are the contract limits to carry. Nil means DefaultLimits().Proto().
	Limits *orkstackv1.WorkspaceLimits
}

// LocalTaskRefusal is a refusal the interpreter would have recorded as the
// task's own failure rather than as a caller error.
type LocalTaskRefusal struct {
	Code     string // task.ReasonInputTooLarge
	Category string // task.CategoryContractViolation
	Err      error
}

// Error is the refusal's message, which is the interpreter's own.
func (r *LocalTaskRefusal) Error() string { return r.Err.Error() }

// Unwrap exposes the underlying error.
func (r *LocalTaskRefusal) Unwrap() error { return r.Err }

// LocalTaskInput returns the ExecuteTaskInput RunWorkflow's schedule composes
// for t.TaskID, with these deliberate differences and nothing else:
// commit_uri, pin and telemetry are UNSET.
//
//   - commit_uri: a local run has no durable result, and an empty URI is the
//     dispatcher's own "no commit to check or write".
//   - pin: no session exists; a caller that needs a PinContext sets its own.
//   - telemetry: a local run exports nothing.
//
// It applies the same wire bound schedule applies (task.CheckInputSize with
// Limits.values_max_bytes); a refusal is a *LocalTaskRefusal carrying
// INPUT_TOO_LARGE / ContractViolation. An unknown task, an empty RunID, or a
// nil Orkflow is a plain error.
func LocalTaskInput(t LocalTask) (*orkstackv1.ExecuteTaskInput, error) {
	wf := t.Orkflow
	if wf == nil {
		return nil, errors.New("compose a local task input: no orkflow definition")
	}
	if t.RunID == "" {
		return nil, errors.New("compose a local task input: the run id is empty")
	}
	spec := taskByID(wf, t.TaskID)
	if spec == nil {
		return nil, fmt.Errorf("task %q is not in orkflow %s@%s", t.TaskID, wf.GetName(), wf.GetVersion())
	}
	closure, err := TaskClosure(wf, t.TaskID)
	if err != nil {
		return nil, err
	}
	upstream := make([]*orkstackv1.UpstreamRef, 0, len(closure))
	for _, id := range closure {
		ref, ok := t.Upstream[id]
		if !ok || ref == nil {
			continue
		}
		clone := proto.Clone(ref).(*orkstackv1.UpstreamRef)
		clone.TaskId = id
		upstream = append(upstream, clone)
	}
	limits := t.Limits
	if limits == nil {
		limits = DefaultLimits().Proto()
	}
	pool := spec.GetPool()
	if pool == "" {
		// routeForTask's own default: a task that names no pool asks for the
		// built-in default.
		pool = task.DefaultPool
	}
	namespace := wf.GetNamespace()
	input := &orkstackv1.ExecuteTaskInput{
		RunId:                   t.RunID,
		OrkflowName:             wf.GetName(),
		OrkflowVersion:          wf.GetVersion(),
		OrkflowDefinitionDigest: wf.GetDefinitionDigest(),
		Namespace:               namespace,
		Pool:                    pool,
		TaskId:                  t.TaskID,

		IdempotencyKey: task.IdempotencyKeyFor(namespace, t.RunID, t.TaskID),

		Image:   spec.GetImage(),
		Command: spec.GetCommand(),
		With:    spec.GetWith(),
		Env:     spec.GetEnv(),
		Secrets: spec.GetSecrets(),

		TimeoutSeconds:                 int32(timeoutSecondsFor(spec)),
		CancellationGracePeriodSeconds: graceSecondsFor(spec),
		Resources:                      spec.GetResources(),
		Network:                        spec.GetNetwork(),
		Limits:                         limits,

		RunInput: ApplyInputDefaults(wf, t.Input),
		Upstream: upstream,

		RunAsOrkerUser: spec.GetRunAsOrkerUser(),
	}
	if err := task.CheckInputSize(input, limits.GetValuesMaxBytes()); err != nil {
		return nil, &LocalTaskRefusal{
			Code:     task.ReasonInputTooLarge,
			Category: task.CategoryContractViolation,
			Err:      err,
		}
	}
	return input, nil
}

// TaskClosure returns the sorted transitive `needs` closure of taskID — the
// same set runState.transitiveNeeds computes and upstreamFor threads into the
// task's input. Unknown taskID is an error.
func TaskClosure(wf *orkstackv1.Orkflow, taskID string) ([]string, error) {
	if taskByID(wf, taskID) == nil {
		return nil, fmt.Errorf("task %q is not in orkflow %s@%s", taskID, wf.GetName(), wf.GetVersion())
	}
	tasks := make(map[string]*orkstackv1.Task, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		tasks[t.GetId()] = t
	}
	return transitiveNeedsOf(tasks, taskID), nil
}

// UpstreamFromRun returns, for every TaskRun of run whose status is
// STATUS_SUCCEEDED, the UpstreamRef RunWorkflow built from the same result:
// {TaskId, CommitUri: commit.uri, Values: values_ref, Artifacts: artifacts}.
// Every other status — SKIPPED, FAILED, CANCELED, PENDING, RUNNING —
// contributes nothing, exactly as it contributes nothing to the interpreter's
// results. A FAILED task's failure-path artifacts are therefore never passed
// downstream, as in a run.
func UpstreamFromRun(run *orkstackv1.Run) map[string]*orkstackv1.UpstreamRef {
	out := map[string]*orkstackv1.UpstreamRef{}
	for _, tr := range run.GetTaskRuns() {
		if tr.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			continue
		}
		out[tr.GetTaskId()] = &orkstackv1.UpstreamRef{
			TaskId:    tr.GetTaskId(),
			CommitUri: tr.GetCommit().GetUri(),
			Values:    tr.GetValuesRef(),
			Artifacts: tr.GetArtifacts(),
		}
	}
	return out
}

func taskByID(wf *orkstackv1.Orkflow, id string) *orkstackv1.Task {
	for _, t := range wf.GetTasks() {
		if t.GetId() == id {
			return t
		}
	}
	return nil
}
