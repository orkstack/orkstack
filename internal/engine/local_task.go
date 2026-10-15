package engine

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
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
// the interpreter would never have scheduled. await: (docs/DSL.md "Awaited tasks") is
// such a node: LocalTaskInput refuses an awaited task with a caller error.
// each: (docs/DSL.md "Fan-out") is another: a fan-out composes ONE ITEM's input,
// exactly as scheduleItem does, and requires Item; a task that names a
// fan-out in its own needs: is refused, because its items are read from
// their commits and a local run has no commit store. join: is a third: a failure handler's input carries how its needs ended, so
// LocalTask.Join is required for one and refused for any other task, and a
// direct need that FAILED contributes {task_id, artifacts} exactly as
// schedule's upstreamFor does.

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
	// Item names the item of a fan-out task to compose; required for a
	// fan-out and refused for any other task.
	Item *int32
	// ItemCount is the fan-out's item count: the run's TaskEach.count
	// (--from-run), or what the caller counted over a stubbed source. Nil
	// for an input source means "count it from Input", as the interpreter
	// does; nil for a task source is a caller error.
	ItemCount *int32
	// Join is how a failure handler's needs ended (JoinContextFromRun, or the
	// caller's --need). Required for a failure handler, refused for any other
	// task.
	Join *orkstackv1.JoinContext
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
// commit_uri, pin, telemetry, image_policy and cache are UNSET.
//
//   - commit_uri: a local run has no durable result, and an empty URI is the
//     dispatcher's own "no commit to check or write".
//   - pin: no session exists; a caller that needs a PinContext sets its own.
//   - telemetry: a local run exports nothing.
//   - image_policy: the deployment's image policy governs what the
//     deployment's Orkers execute; a --file definition already met it at
//     ValidateOrkflow, and a run's own definition met it at submit. With no
//     selection on the input, the dispatcher never enters its policy step.
//   - cache: a local run neither reads nor writes a memo (the CLI's
//     dispatcher has no memo store), so a cached task runs its container.
//
// It applies the same wire bound schedule applies (task.CheckInputSize with
// Limits.values_max_bytes); a refusal is a *LocalTaskRefusal carrying
// INPUT_TOO_LARGE / ContractViolation. An unknown task, an awaited task, an
// empty RunID, or a nil Orkflow is a plain error.
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
	if spec.GetAwait() != nil {
		// The interpreter never schedules an awaited task: it waits for a
		// resolution from outside the run and starts no container. There is
		// no input to compose, so this is a caller error, not the task's own
		// failure (docs/UI_CLI_PARITY.md "Local activity build/test parity decision").
		return nil, fmt.Errorf("task %q is an awaited task (await:): it starts no container, so there is nothing to run locally; "+
			"resolve it in a run with orkctl run resolve, or run a task downstream of it with its values supplied", t.TaskID)
	}
	if ref := spec.GetOrkflow(); ref != nil {
		return nil, ComposedTaskRefusal(t.TaskID, ref)
	}
	handler := isFailureHandler(spec)
	if handler && t.Join == nil {
		return nil, fmt.Errorf("task %q is a failure handler (join %s): its input names how its needs ended; "+
			"run it with --from-run RUN or name each need with --need TASK=STATUS", t.TaskID, dsl.JoinWord(spec.GetJoin()))
	}
	if !handler && t.Join != nil {
		return nil, fmt.Errorf("task %q is not a failure handler (join %s): how its needs ended is not part of "+
			"its input, so --need does not apply", t.TaskID, dsl.JoinWord(spec.GetJoin()))
	}
	if spec.GetEach() == nil && t.Item != nil {
		return nil, fmt.Errorf("task %q is not a fan-out; it has no items, so --item does not apply", t.TaskID)
	}
	if spec.GetEach() != nil && t.Item == nil {
		return nil, fmt.Errorf("task %q is a fan-out (each:): name the item to run with --item N", t.TaskID)
	}
	// A direct dependent of a fan-out stages every item from its commit;
	// a local run has no commit store.
	for _, need := range spec.GetNeeds() {
		producer := taskByID(wf, need)
		if producer.GetEach() == nil {
			continue
		}
		items := "the items"
		if ref := t.Upstream[need]; ref.GetEach() != nil {
			items = fmt.Sprintf("the %d items", ref.GetEach().GetCount())
		}
		return nil, fmt.Errorf("task run cannot stage %s of fan-out %q: they are read from their commits, "+
			"and a local run has no commit store", items, need)
	}
	closure, err := TaskClosure(wf, t.TaskID)
	if err != nil {
		return nil, err
	}
	// For a failure handler, a direct need's contribution follows its
	// recorded (or supplied) outcome: SUCCEEDED as today, FAILED as
	// {task_id, artifacts} when it has any, anything else nothing.
	joined := map[string]orkstackv1.Status{}
	for _, need := range t.Join.GetNeeds() {
		joined[need.GetTaskId()] = need.GetStatus()
	}
	upstream := make([]*orkstackv1.UpstreamRef, 0, len(closure))
	for _, id := range closure {
		ref, ok := t.Upstream[id]
		if !ok || ref == nil {
			continue
		}
		failedShape := ref.GetValues() == nil && ref.GetCommitUri() == "" && ref.GetEach() == nil
		if status, direct := joined[id]; handler && direct {
			switch status {
			case orkstackv1.Status_STATUS_SUCCEEDED:
				if failedShape {
					continue
				}
			case orkstackv1.Status_STATUS_FAILED:
				if len(ref.GetArtifacts()) > 0 {
					clone := proto.Clone(ref).(*orkstackv1.UpstreamRef)
					upstream = append(upstream, &orkstackv1.UpstreamRef{TaskId: id, Artifacts: clone.GetArtifacts()})
				}
				continue
			default:
				continue
			}
		} else if failedShape && len(ref.GetArtifacts()) > 0 {
			// A failed task's artifacts reach only a failure handler that
			// names it directly.
			continue
		}
		// A fan-out further up the closure contributes nothing: its items
		// reach DIRECT dependents only (docs/DSL.md "Fan-out").
		if taskByID(wf, id).GetEach() != nil {
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
	if handler {
		input.Join = proto.Clone(t.Join).(*orkstackv1.JoinContext)
	}
	if each := spec.GetEach(); each != nil {
		return localItemInput(wf, spec, t, input)
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

// localItemInput turns a task's composed input into ONE ITEM's, with
// scheduleItem's differences: the item idempotency key, the per-item artifact
// limit, EachContext, and the per-item input bound.
func localItemInput(wf *orkstackv1.Orkflow, spec *orkstackv1.Task, t LocalTask, input *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskInput, error) {
	each := spec.GetEach()
	var count int32
	switch {
	case t.ItemCount != nil:
		count = *t.ItemCount
	case each.GetSource().GetInput() != "":
		n, eerr := eachCountOfInput(payloadMap(input.GetRunInput()), each)
		if eerr != nil {
			return nil, fmt.Errorf("task %q: %s", t.TaskID, eerr.Message)
		}
		count = n
	default:
		return nil, fmt.Errorf("task %q fans out over task %q's output: run it with --from-run, or stub the "+
			"source with --output %s=JSON so its items can be counted", t.TaskID,
			each.GetSource().GetTaskId(), each.GetSource().GetTaskId())
	}
	index := *t.Item
	if index < 0 || index >= count {
		if count == 0 {
			return nil, fmt.Errorf("task %q fans out over 0 items; there is no item %d", t.TaskID, index)
		}
		return nil, fmt.Errorf("task %q has %d items (0–%d); there is no item %d", t.TaskID, count, count-1, index)
	}
	limits := WorkspaceLimits{
		ValuesMaxBytes:   input.GetLimits().GetValuesMaxBytes(),
		ArtifactMaxCount: input.GetLimits().GetArtifactMaxCount(),
		ArtifactMaxBytes: input.GetLimits().GetArtifactMaxBytes(),
		LogMaxBytes:      input.GetLimits().GetLogMaxBytes(),
		CommitMaxBytes:   input.GetLimits().GetCommitMaxBytes(),
	}
	// The floor every run this could be pointed at pinned: a run declaring
	// more than 1,024 items exists only on a deployment with continuation on,
	// which pins DefaultItemInputFloorBytes, and at 1,024 or fewer the floor
	// is inert. A run carries no plan to --from-run, so the local binary's
	// constant stands in for it, as its other rules already do.
	_, inputLimit, artifactMax := eachBounds(wf, limits, DefaultItemInputFloorBytes)
	input.IdempotencyKey = task.ItemIdempotencyKeyFor(wf.GetNamespace(), t.RunID, t.TaskID, index)
	input.Limits = proto.Clone(input.GetLimits()).(*orkstackv1.WorkspaceLimits)
	input.Limits.ArtifactMaxCount = artifactMax
	input.Each = &orkstackv1.EachContext{Index: index, Count: count, As: each.GetAs(), Source: each.GetSource()}
	if err := task.CheckInputSize(input, inputLimit); err != nil {
		return nil, &LocalTaskRefusal{Code: task.ReasonInputTooLarge, Category: task.CategoryContractViolation, Err: err}
	}
	return input, nil
}

// ComposedTaskRefusal is why a composed task cannot run locally: it starts a
// child run, not a container, so there is no input to compose. One of the
// child's tasks runs locally against the child run's bytes.
func ComposedTaskRefusal(taskID string, ref *orkstackv1.OrkflowRef) error {
	return fmt.Errorf("task %q runs orkflow %s@%s as a child run, not a container: run one of its "+
		"tasks with orkctl task run --from-run <child-run-id> --task <task>", taskID, ref.GetName(), ref.GetVersion())
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
		// A fan-out has no commit of its own: its items do. It contributes
		// its count only, which is what a direct dependent's refusal names.
		if tr.GetEach() != nil {
			out[tr.GetTaskId()] = &orkstackv1.UpstreamRef{
				TaskId: tr.GetTaskId(),
				Each:   &orkstackv1.EachUpstream{Count: tr.GetEach().GetCount()},
			}
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

// JoinContextFromRun is the JoinContext RunWorkflow built for taskID's
// dispatch, rebuilt from a recorded run with the interpreter's own pure
// helper: each need's status and error from its row, bounded field by
// field. Nil when taskID is not a failure handler of wf.
func JoinContextFromRun(run *orkstackv1.Run, wf *orkstackv1.Orkflow, taskID string) *orkstackv1.JoinContext {
	spec := taskByID(wf, taskID)
	if spec == nil || !isFailureHandler(spec) {
		return nil
	}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, tr := range run.GetTaskRuns() {
		rows[tr.GetTaskId()] = tr
	}
	return joinContextFor(spec, func(id string) *orkstackv1.TaskRun { return rows[id] })
}

// FailedNeedUpstreamFromRun returns, for every TaskRun of run that FAILED with
// output artifacts, the {task_id, artifacts} ref a failure handler naming it
// directly is handed. UpstreamFromRun keeps its succeeded-only contract;
// LocalTaskInput uses these only for a failure handler's direct FAILED needs.
func FailedNeedUpstreamFromRun(run *orkstackv1.Run) map[string]*orkstackv1.UpstreamRef {
	out := map[string]*orkstackv1.UpstreamRef{}
	for _, tr := range run.GetTaskRuns() {
		if ref := failedNeedRef(tr); ref != nil {
			out[tr.GetTaskId()] = ref
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
