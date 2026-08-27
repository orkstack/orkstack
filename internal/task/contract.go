// Package task defines the engine<->Orker execution seam: the Temporal
// activity contract by which the engine schedules a task and an Orker executes
// it as an OCI container. This is a load-bearing internal contract (like
// proto): the engine (internal/engine) schedules ActivityExecuteTask; the
// Orker (internal/orker) registers and implements it. Change it deliberately
// and update both sides.
//
// Design note — the argument is a PROTO MESSAGE, not a Go struct:
// orkstack.v1.ExecuteTaskInput is the whole activity argument. It replaced the
// Go struct that lived here outright (design §2.3, §2.4). Temporal's data
// converter encodes a top-level protobuf message with protojson and everything
// else with encoding/json, so this change flips the recorded payload encoding
// — which is exactly why the design permits ONE replay-fixture reset, at the
// end of Phase 1, after the entire new wire shape has landed (§11.4).
//
// Design note — template rendering lives in the Orker, not the engine:
// task `with` values may reference upstream artifacts via the
// `artifact "<task>/<name>"` helper, which resolves to a container path only
// the Orker has, after staging the artifact. To keep the engine workflow
// deterministic (no filesystem/network), the engine passes the RAW `with`
// block plus the upstream references, and the Orker performs all template
// rendering, artifact staging, container execution, and artifact upload.
//
// Fixture-reset history, kept because §11.4 calls a second reset a process
// failure rather than an accident:
//
//	M10.1  Namespace and Pool were added to the activity input, and
//	       internal/engine/testdata/history/run-succeeded-v1.json was
//	       re-recorded once.
//	M11.1  The activity input became orkstack.v1.ExecuteTaskInput and the
//	       recorded payload encoder flipped from encoding/json to protojson
//	       (§2.5). The fixture was re-recorded once, LAST, after every
//	       wire-visible change of Phase 1 had landed — the proto contract,
//	       Error.category, the Task and TaskRun fields, RunPlan's CommitRoot,
//	       Limits and TransferBudgetSeconds, ActivityFailureDetail, and the
//	       ActivityOptions change that adds HeartbeatTimeout and
//	       WaitForCancellation. Recorded by
//	       TestIntegration_RecordGoldenRunHistory against a real cluster with
//	       ORK_RECORD_GOLDEN_HISTORY=1.
//
// This is the last reset the design permits. Anything that would need another
// one is a wire change that should have been in Phase 1.
package task

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	// DefaultQueue is the Temporal task queue Orkers poll for task execution.
	DefaultQueue = "ork-tasks"

	// EngineEventQueue is the Temporal task queue that carries the engine's
	// own projection activities, and the one queue no task may be dispatched
	// to. It is declared here, beside DefaultQueue, because the two places
	// that must refuse it — QueueForPool and `ork` at boot — cannot import
	// internal/engine. engine.EventQueue is this literal, not a second one.
	EngineEventQueue = "ork-engine-events"

	// DefaultNamespace is the Temporal namespace OrkStack runs in when
	// ORK_TEMPORAL_NAMESPACE is unset. Binaries provision it on startup when
	// the cluster does not have it yet (see EnsureNamespace).
	DefaultNamespace = "ork-main"

	// ActivityExecuteTask is the registered activity name for task execution.
	ActivityExecuteTask = "ExecuteTask"

	// ActivityDescribeOrker is the registered activity name for the pool
	// liveness probe.
	//
	// It replaces ActivityListPlugins one-for-one: the probe call site, its
	// 5 s and 10 s timeouts, its MaximumAttempts: 2 and its
	// default-pool-failure -> Unavailable mapping are all unchanged, and only
	// the payload differs. A successful probe was never only a plugin list —
	// it is how a pool proves it is alive, and proof/M11.6 fixed a bug in
	// exactly that path (§6.5).
	ActivityDescribeOrker = "DescribeOrker"
)

// The five failure categories of the OCI Activity Contract (design §8.1).
//
// They live HERE, in the seam package both sides already depend on, rather
// than in internal/orker or internal/engine. The Orker constructs them and the
// engine reads them, so a copy in either package would be a second spelling of
// a shared contract — and these strings tolerate no drift at all: they are the
// exact Temporal ApplicationError type strings, and
// RetryPolicy.NonRetryableErrorTypes matches a type string LITERALLY, not with
// errors.Is. A category that spelled itself differently on the two sides would
// silently stop being non-retryable.
const (
	// CategoryActivityFailed is the container exiting non-zero for its own
	// reasons. Retried per the task's policy.
	CategoryActivityFailed = "ActivityFailed"
	// CategoryInfrastructureError is storage, registry, runtime or workspace
	// failing. Retried.
	CategoryInfrastructureError = "InfrastructureError"
	// CategoryContractViolation is the image or the definition breaking the
	// ABI. Never retried: the same image will do the same thing next time.
	CategoryContractViolation = "ContractViolation"
	// CategoryTimedOut is the Orker killing the container at timeout_seconds,
	// or Temporal producing a *TimeoutError.
	//
	// It is NON-RETRYABLE BY DEFAULT, which deviates from AGENTS.md §4's
	// suggested-default table. Acceptance criterion 7 of this design groups
	// timeout with the permanent failures in as many words; the two cannot
	// both be honored, and the acceptance criteria are this work's literal
	// contract, so the criterion controls. Flipping it back is one line:
	// remove it from NonRetryableCategories (§8.1, §13, §17 C1).
	CategoryTimedOut = "TimedOut"
	// CategoryCancelled is delivered by Temporal through a heartbeat. It is
	// never an ApplicationError; the Orker returns ctx.Err() instead.
	CategoryCancelled = "Cancelled"
)

// NonRetryableCategories is what the engine puts in
// RetryPolicy.NonRetryableErrorTypes.
//
// It is belt and braces rather than the only mechanism: a ContractViolation is
// also constructed with temporal.NewNonRetryableApplicationError, and the
// constructor's flag always wins. The list is what makes the intent visible in
// the workflow, to a reader of the retry policy who never opens the Orker.
func NonRetryableCategories() []string {
	return []string{CategoryContractViolation, CategoryTimedOut}
}

// MaxActivityInputBytes is the hard ceiling on one serialized
// ExecuteTaskInput.
//
// AGENTS.md §2.2 fixes the maximum serialized Temporal payload at 256 KiB and
// requires that anything larger be an Artifact. The engine checks this
// DETERMINISTICALLY, before scheduling, so an oversized input is a clean
// ContractViolation naming the task rather than a Temporal-side rejection
// nobody can attribute.
const MaxActivityInputBytes = 262144

// ReasonInputTooLarge is the §8.2 reason code the engine raises when an input
// would exceed the wire bound. It is spelled here rather than imported from
// internal/orker because the engine may not import the Orker, and a second
// spelling of a reason code is a second place for it to be wrong.
const ReasonInputTooLarge = "INPUT_TOO_LARGE"

// CheckInputSize enforces the 256 KiB wire bound.
//
// It measures the BINARY encoding, which is what Temporal's payload budget
// applies to, and it measures the message the engine is about to send rather
// than an estimate of it. A message that is too large is a fact about the
// definition — a `with:` block or an upstream fan-in that grew — so the error
// names both the size and the limit, and the caller makes it non-retryable.
func CheckInputSize(in *orkstackv1.ExecuteTaskInput) error {
	encoded, err := proto.Marshal(in)
	if err != nil {
		return fmt.Errorf("%s: task %q: encode the activity input: %w",
			ReasonInputTooLarge, in.GetTaskId(), err)
	}
	if len(encoded) > MaxActivityInputBytes {
		return fmt.Errorf(
			"%s: task %q: the activity input serializes to %d bytes, over the %d-byte limit. "+
				"Bytes never travel through Temporal — move large `with:` values into an artifact "+
				"and reference it",
			ReasonInputTooLarge, in.GetTaskId(), len(encoded), MaxActivityInputBytes)
	}
	return nil
}

// IdempotencyKeyFor renders the stable identity of one logical activity.
//
// It is "{namespace}/{run_id}/{task_id}" and it does not change across
// retries, which is what lets an external system implement idempotency against
// it (AGENTS.md §3, §4).
//
// The name is deliberately NOT "task idempotency key" on the wire, and
// deliberately IS distinct in Go. "Idempotency key" already means the run
// SUBMISSION key claimed through IdempotencyClaimWorkflow — a different thing
// with a different lifetime. Proto scopes field names per message so the two
// wire names coexist, and keeping this one plain is what makes
// ExecuteTaskInput's JSON key byte-identical to the `idempotencyKey` that
// task.json already writes. The Go identifiers stay distinct because those
// would genuinely collide (§11.5).
func IdempotencyKeyFor(namespace, runID, taskID string) string {
	if namespace == "" {
		namespace = "default"
	}
	return namespace + "/" + runID + "/" + taskID
}
