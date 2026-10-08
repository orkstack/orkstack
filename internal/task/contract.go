// Package task defines the engine<->Orker execution seam: the Temporal
// activity contract by which the engine schedules a task and an Orker executes
// it as an OCI container. This is a load-bearing internal contract (like
// proto): the engine (internal/engine) schedules ActivityExecuteTask; the
// Orker (internal/orker) registers and implements it. Change it deliberately
// and update both sides.
//
// Design note — the argument is a PROTO MESSAGE, not a Go struct:
// orkstack.v1.ExecuteTaskInput is the whole activity argument
// (docs/OCI_ACTIVITY_CONTRACT.md §2.2). Temporal's data converter encodes a top-level protobuf message with
// protojson and everything else with encoding/json, so the recorded payload
// encoding is protojson and the golden fixture below is pinned to it.
//
// Design note — template rendering lives in the Orker, not the engine:
// task `with` values may reference upstream artifacts via the
// `artifact "<task>/<name>"` helper, which resolves to a container path only
// the Orker has, after staging the artifact. To keep the engine workflow
// deterministic (no filesystem/network), the engine passes the RAW `with`
// block plus the upstream references, and the Orker performs all template
// rendering, artifact staging, container execution, and artifact upload.
//
// Anything that changes what the engine puts into an activity input is a
// wire change. The golden history fixture
// (internal/engine/testdata/history/run-succeeded-v1.json) was recorded by
// TestIntegration_RecordGoldenRunHistory against a real cluster
// (ORK_RECORD_GOLDEN_HISTORY=1) and is never re-recorded: a change that would
// need it is a change to the contract itself, not a refactor.
package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/converter"
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
	// It is how a pool proves it is alive: the engine executes it on the
	// pool's queue with short timeouts and MaximumAttempts: 2, and a default
	// pool that cannot answer maps to Unavailable (§6.5).
	ActivityDescribeOrker = "DescribeOrker"

	// ActivityReleasePin is the registered activity name for removing a pin's
	// shared directory on the Orker that hosts it.
	//
	// It lives beside the other two for the same reason they are here: the
	// engine schedules it and the Orker registers it, so a second spelling in
	// either package would be a second spelling of a shared contract. The
	// engine schedules it on the pin's own Temporal session, which routes it
	// to the host that owns the directory; a host with no such directory
	// returns success (docs/OCI_ACTIVITY_CONTRACT.md §6.4).
	ActivityReleasePin = "ReleasePin"

	// ActivityCordonOrker is the registered activity name for setting or
	// clearing one Orker's cordon. The engine executes it on that Orker's own
	// control channel (QueueForOrker); the Orker registers it there and on no
	// other channel, so it can never land on a different process.
	ActivityCordonOrker = "CordonOrker"
)

// The five failure categories of the OCI Activity Contract
// (docs/OCI_ACTIVITY_CONTRACT.md §8.1).
//
// They live HERE, in the seam package both sides already depend on, rather
// than in internal/orker or internal/engine. The Orker constructs them and the
// engine reads them, so a copy in either package would be a second spelling of
// a shared contract — and these strings tolerate no drift at all: they are the
// exact Temporal ApplicationError type strings the engine maps back to a
// FailureCategory on the reading side.
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
	// The Orker's own CONTAINER_TIMEOUT is NON-RETRYABLE BY DEFAULT, which
	// differs from AGENTS.md §4's suggested-default table: a workload that
	// times out once would time out again on the same wall clock. A Temporal
	// heartbeat timeout (a lost Orker) stays retryable. The decision is made
	// by orker.RetryableReason (docs/OCI_ACTIVITY_CONTRACT.md §8.1).
	CategoryTimedOut = "TimedOut"
	// CategoryCancelled is delivered by Temporal through a heartbeat. It is
	// never an ApplicationError; the Orker returns ctx.Err() instead.
	CategoryCancelled = "Cancelled"
)

// NonRetryableCategories names the categories whose reasons the Orker
// constructs non-retryable by default. It is a statement of intent shared by
// tests, not a retry list: the engine sets no NonRetryableErrorTypes, and
// non-retryability travels on the error itself
// (temporal.NewNonRetryableApplicationError, decided by
// orker.RetryableReason).
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

// HeartbeatTimeout is the single number both halves of the heartbeat contract
// must agree on: the engine sets it in ActivityOptions, and the Orker derives
// its tick rate from it.
//
// It lives HERE, in the engine<->Orker seam, because the two packages cannot
// share it any other way — internal/orker imports internal/engine, so the
// engine cannot import the Orker back. Two spellings of one contract, one in
// each package, is one too many: raising the Orker's constant past three
// times the engine's would make the Orker tick more slowly than Temporal's
// timeout and every activity would be declared dead, with nothing in the tree
// failing first.
//
// Temporal delivers cancellation to an activity ONLY through a heartbeat
// response, so an activity that never heartbeats never observes ctx.Done()
// (§6.3).
const HeartbeatTimeout = 30 * time.Second

// MaxHeartbeatInterval is the CEILING on how often the Orker's loop may tick,
// not the tick itself: the tick is an operator setting
// (ORK_ORKER_HEARTBEAT_INTERVAL, orker.DefaultHeartbeatInterval). A third of
// the timeout leaves room for two missed ticks before Temporal declares the
// activity dead.
//
// The tick is ALSO the cancellation latency, and that is the non-obvious half.
// The Go SDK does not send every RecordHeartbeat: it batches them into windows
// of min(0.8 × HeartbeatTimeout, worker.Options.MaxHeartbeatThrottleInterval),
// overwriting the pending detail inside a window and issuing one RPC when it
// closes. Cancellation is delivered only in a heartbeat RPC's RESPONSE, so the
// interval an activity actually observes cancellation at is that batch window,
// not this ceiling and not the loop's own ticker. An Orker MUST therefore cap
// MaxHeartbeatThrottleInterval to its own tick, or a 10 s tick still means a
// 24 s wait before a running container learns it was cancelled.
const MaxHeartbeatInterval = HeartbeatTimeout / 3

// HeartbeatStaleAfter is how old a live attempt's last heartbeat may be
// before the engine's InspectRun reports it stale. It is two ceiling ticks:
// a healthy Orker beats at least every MaxHeartbeatInterval (the worker's
// throttle is capped to its tick, OCI contract §6.3 item 3), so one missed
// tick is a frontend blip and two are a missing Orker. It is deliberately
// inside HeartbeatTimeout, so an operator sees the gap open BEFORE Temporal
// acts on it; TestHeartbeatStaleAfter_LeavesRoomBeforeTheDeadWorkerWindow
// holds the ordering. It is a code constant, not an operator setting: the
// tick ceiling it is derived from is not one either.
const HeartbeatStaleAfter = 2 * MaxHeartbeatInterval

// HeartbeatProgress is the detail the Orker's heartbeat loop carries and the
// engine's InspectRun reads back from the cluster. It is deliberately small:
// heartbeat details travel through Temporal's data converter into history,
// and bytes never travel through Temporal payloads (AGENTS.md §2.2). Phase,
// a container id and a byte count are diagnosis; the logs are where the
// content lives.
//
// The JSON names are the wire contract between an Orker and an engine of
// possibly different builds: a field is only ever ADDED, with omitempty, so
// an older Orker's beat decodes on a newer engine with the new field empty.
type HeartbeatProgress struct {
	Phase       string `json:"phase"`
	ContainerID string `json:"container_id,omitempty"`
	BytesLogged int64  `json:"bytes_logged,omitempty"`
	// OrkerID is the sender, as ORK_ORKER_ID names it. It is here rather than
	// read from the SDK's worker identity because that identity is
	// "pid@hostname" by default, and a host name may not reach a user-facing
	// surface (AGENTS.md §7.1). ORK_ORKER_ID's own default is
	// "hostname-<hash of the work dir>" (docs/DEPLOY.md §1), which every
	// TaskRun.orker_id already shows; nothing new is disclosed.
	OrkerID string `json:"orker_id,omitempty"`
}

// CheckInputSize enforces the deployment's replay-pinned input bound without
// ever exceeding Temporal's hard 256 KiB ceiling. A zero or omitted plan limit
// uses the ceiling, preserving old histories and small test harness plans.
//
// It measures the encoding TEMPORAL WILL ACTUALLY SEND, which is not the
// binary one. Temporal's default data converter encodes a top-level protobuf
// message with protojson (this package's own doc comment says so, and it is
// why RunPlan is a plain Go struct). Bounding proto.Marshal instead would not
// do: binary and protojson agree closely for plain ASCII, so the gap hides in
// every ordinary input — but each quote, newline or backslash in a `with:`
// value becomes a two-character JSON escape, and a measured input whose binary
// form is 260174 bytes reaches 520337 as a payload: a binary check returns nil
// while the real payload is 1.98x a maximum AGENTS.md §2.2 calls HARD. A gate
// that can pass something Temporal rejects produces exactly the "Temporal-side
// rejection nobody can attribute" this function exists to prevent.
//
// The measurement is taken on the COMPACTED payload plus a worst-case
// whitespace allowance, rather than on the payload's own length, because
// protojson injects random whitespace on purpose to deter byte-comparison
// (§14.10: `if detrand.Bool() { e.out = append(e.out, ' ') }`). That randomness
// is seeded per BUILD, so a raw length is stable within one binary and can
// differ in the next one — measured here at ~1 byte per JSON token, 2001 bytes
// across a 2000-key struct. Bounding a raw length would therefore make this
// decision build-dependent, and this runs in workflow code, where a decision
// that flips between builds is a replay divergence. Compacting removes the
// randomness; adding back one byte per injection site keeps the bound on the
// safe side of the hard ceiling for any build.
func CheckInputSize(in *orkstackv1.ExecuteTaskInput, planLimit ...int64) error {
	limit := int64(MaxActivityInputBytes)
	if len(planLimit) > 0 && planLimit[0] > 0 && planLimit[0] < limit {
		limit = planLimit[0]
	}
	size, err := PayloadSize(in)
	if err != nil {
		return fmt.Errorf("%s: task %q: encode the activity input: %w",
			ReasonInputTooLarge, in.GetTaskId(), err)
	}
	if size > limit {
		return fmt.Errorf(
			"%s: task %q: the activity input serializes to %d bytes as the protojson payload "+
				"Temporal records, over the %d-byte limit. "+
				"Bytes never travel through Temporal — move large `with:` values into an artifact "+
				"and reference it",
			ReasonInputTooLarge, in.GetTaskId(), size, limit)
	}
	return nil
}

// PayloadSize is the deterministic upper bound on the bytes Temporal will carry
// for m — any message, encoded by the default data converter as a top-level
// argument is. See CheckInputSize for why it is a bound and not a length.
// CheckInputSize measures with it unchanged, so every decision it recorded
// stands. Pure.
func PayloadSize(m proto.Message) (int64, error) {
	payload, err := converter.GetDefaultDataConverter().ToPayload(m)
	if err != nil {
		return 0, err
	}
	raw := payload.GetData()
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		// Not JSON after all: the converter picked something else. Its own
		// length is then the honest measure.
		return int64(len(raw)), nil //nolint:nilerr // the raw length is the answer
	}
	return int64(compact.Len() + whitespaceSites(compact.Bytes())), nil
}

// whitespaceSites counts the structural positions protojson may follow with an
// injected space. Separators inside string literals are not structural, so the
// scan tracks quoting and escaping.
func whitespaceSites(compact []byte) int {
	var sites, i int
	inString := false
	for ; i < len(compact); i++ {
		c := compact[i]
		if inString {
			switch c {
			case '\\':
				i++ // skip the escaped byte, which is never structural
			case '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case ':', ',':
			sites++
		}
	}
	return sites
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
// would genuinely collide.
func IdempotencyKeyFor(namespace, runID, taskID string) string {
	if namespace == "" {
		namespace = "default"
	}
	return namespace + "/" + runID + "/" + taskID
}

// ItemIdempotencyKeyFor is the stable key of ONE ITEM of a fan-out: an item
// is one logical activity (AGENTS.md §3, §4), so it has its own key, unchanged
// across that item's retries:
//
//	"{namespace-or-default}/{run_id}/{task_id}/{index}"
//
// The index is decimal and unpadded — a key for external systems, not a sort
// key — and a task key "{ns}/{run}/{task}" has one segment fewer, so the two
// shapes never collide.
func ItemIdempotencyKeyFor(namespace, runID, taskID string, index int32) string {
	return IdempotencyKeyFor(namespace, runID, taskID) + "/" + strconv.FormatInt(int64(index), 10)
}

// InputSize is the deterministic bound on the bytes Temporal records for in:
// exactly the measure CheckInputSize compares against its limit.
func InputSize(in *orkstackv1.ExecuteTaskInput) (int64, error) {
	return PayloadSize(in)
}
