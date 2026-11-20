package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RunPlan is a run's routing, decided at submit and bound into the workflow
// input.
//
// It exists because the two halves of the decision cannot live in the same
// place. Which pool a task runs on is the user's document plus the namespace's
// default — configuration, read from a store, at submit. Which channel that
// pool is, is process configuration, which workflow code may not read at all
// (AGENTS.md §2.1). So the decision is made once, by the process that accepts
// the submission, and travels as an argument: recorded in history, identical
// on every replay, and readable by a second engine finishing a run this one
// started.
//
// It is a plain Go type on purpose. The data converter encodes a top-level
// protobuf message with protojson and everything else with encoding/json;
// nesting the orkflow inside a struct would break the former, so the plan
// travels beside the messages rather than inside one.
type RunPlan struct {
	// Namespace is the namespace the run belongs to.
	Namespace string `json:"namespace"`

	// Routes maps every pool this run uses to the Temporal task queue that
	// carries it. Exactly the pools the run uses: a route table is this run's
	// routing record, not a snapshot of the deployment.
	Routes map[string]string `json:"routes"`

	// CommitRoot is the store root every commit URI in this run is composed
	// against, e.g. "s3://orkstack-artifacts" or "file:///var/lib/ork".
	//
	// It is here for exactly the reason Routes is: a URI needs the artifact
	// backend and endpoint — ORK_ARTIFACT_BACKEND, ORK_S3_ENDPOINT — which is
	// process configuration that workflow code may not read (AGENTS.md §2.1).
	// So the root is decided at SUBMIT, in internal/api, and travels on the
	// argument that is already replay-pinned. The workflow then composes each
	// commit URI as CommitRoot + "/" + artifacts.CommitKey(ns, run, task),
	// which is a pure function of its arguments and replays identically.
	CommitRoot string `json:"commit_root"`

	// Limits are the deployment-wide CONTRACT limits
	// (docs/OCI_ACTIVITY_CONTRACT.md §2.2, WorkspaceLimits): values, artifact count and size, log and commit
	// bounds. They are engine configuration, uniform across the deployment,
	// and the engine needs them itself for its own INPUT_TOO_LARGE check.
	//
	// They are NOT "effective limits after Orker policy" — an earlier draft
	// said that and it cannot be true. pools.go builds routes as a map of
	// MANY pools to MANY queues, so one run genuinely spans pools whose
	// Orkers hold different caps; and internal/api owns no worker-local
	// policy at all, because §9.3 puts cpu/memory/pids caps in ORK_ORKER_* on
	// the Orker. A single submit-time value cannot represent per-pool policy,
	// so it does not try. The Orker applies its own caps and writes the
	// EFFECTIVE values into task.json.
	Limits WorkspaceLimits `json:"limits"`

	// TransferBudgetSeconds is the data-transfer slack in this run's
	// StartToCloseTimeout (§6.3). It is resolved at submit for the same
	// reason CommitRoot is: the derivation reads process configuration, and
	// ActivityOptions is set in workflow code, which may not.
	//
	// Without this field §6.3's formula is unimplementable — it named
	// ORK_-prefixed variables that the ENGINE would have to read, which is
	// precisely the contradiction Limits' own comment rejects.
	TransferBudgetSeconds int32 `json:"transfer_budget_seconds"`

	// Telemetry is the run's telemetry selection, or nil when the namespace
	// set no policy and the orkflow carries no labels. It is decided at
	// SUBMIT (pools.go) for the reason CommitRoot is: which sink a namespace
	// chose is deployment/registry state that workflow code may not read.
	// omitzero, so a plan with none encodes exactly as it did before the
	// field existed.
	Telemetry *TelemetrySelection `json:"telemetry,omitzero"`

	// ImagePolicy is the image policy in force for this run, or nil when the
	// deployment configured none and the namespace names no signer. Decided
	// at submit (internal/api.prepare) from the ImagePolicy document and the
	// namespace, for the reason Limits and Telemetry are: both are state a
	// deterministic workflow may not read (AGENTS.md §2.1), and every
	// attempt of one task must be judged by one rule. It is copied onto
	// every ExecuteTaskInput and is otherwise not read by workflow code.
	// omitzero, so a plan with none encodes exactly as it did before the
	// field existed.
	ImagePolicy *ImagePolicySelection `json:"image_policy,omitzero"`

	// Origin is what started this run, or nil for a plan built before origins
	// existed. It is decided at SUBMIT for the reason CommitRoot and
	// Telemetry are: the door that accepted the submission is the only thing
	// that knows, and workflow code may read neither a request header nor a
	// trigger record (AGENTS.md §2.1). omitzero, so a plan with none encodes
	// exactly as it did before the field existed, and the golden history
	// fixture keeps matching without re-recording.
	Origin *RunOrigin `json:"origin,omitzero"`

	// IdempotencyFingerprint is the fingerprint of the submission that
	// started this run under an idempotency key (internal/api,
	// submissionFingerprint), and empty for a run started with none. A retry
	// under the key reads it from the run's first start event to tell the
	// claim's own run from a workflow something else started under the id.
	// Workflow code never reads it, so it decides no command and needs no
	// gate; omitempty, so a plan with no key encodes exactly as before.
	IdempotencyFingerprint string `json:"idempotency_fingerprint,omitempty"`

	// RunTimeout is set at submit for a run whose definition declares a
	// run-level `timeout:` (runtimeout.go), and nil for every other run, so
	// their plans encode exactly as before.
	RunTimeout *RunTimeoutPlan `json:"run_timeout,omitempty"`

	// EachLines is set at submit for a run whose definition declares a
	// fan-out over an artifact's lines (each_lines.go), and nil for every
	// other run, so their plans encode exactly as before.
	EachLines *EachLinesPolicy `json:"each_lines,omitempty"`

	// Pause, when non-nil, says this run accepts pause requests (pause.go).
	// Set at submit on every run and composed child; nil for a run that
	// predates it, so its plan encodes exactly as before.
	Pause *PausePlan `json:"pause,omitempty"`

	// Concurrency is the run's concurrency limits as its submit decided
	// them (E88; concurrency_run.go): nil for a run with no limit, so its
	// plan encodes exactly as before and its every path is today's.
	Concurrency *ConcurrencyPlan `json:"concurrency,omitempty"`

	// Awaits holds, per task id, the callback credential of every `await:`
	// of this run that declares `callback: true`: a salt and the salted
	// SHA-256 of a token minted at SUBMIT (internal/api). NEVER the token.
	//
	// RunWorkflow does not read it. It rides the plan because the plan is the
	// run's replay-pinned, per-run record that any replica can read back from
	// the run's own start event (TemporalRunReader.RunStart) — which is what
	// lets the callback door verify a token with no table of pending
	// callbacks anywhere (AGENTS.md §13). omitzero, so a plan with none
	// encodes exactly as it did before the field existed and the golden
	// history fixture keeps matching.
	Awaits map[string]AwaitCredential `json:"awaits,omitempty"`

	// Children is, per composed task id, the child run that task starts: its
	// run id, its planned definition and its own plan, recursively. It is
	// decided at SUBMIT for the reason Routes is — which pools a child's
	// tasks resolve to, whether the namespace allows them, which image policy
	// holds, and what the child's run id is are store, cluster and process
	// facts workflow code may not read (AGENTS.md §2.1) — and it is the only
	// place a child's definition reaches workflow code. omitzero, so a plan
	// with none encodes exactly as it did before the field existed and the
	// golden history fixture keeps matching.
	Children map[string]*ChildRunPlan `json:"children,omitempty"`

	// Cache is this run's caching plan, or nil when its definition declares
	// no cached task. Decided at SUBMIT for the reason Limits is: the cap is
	// process configuration workflow code may not read (AGENTS.md §2.1), and
	// no_cache is a request field. omitzero, so a plan with none encodes
	// exactly as before and the golden history fixture keeps matching.
	Cache *CachePlan `json:"cache,omitzero"`

	// ProfileIdentities is, per pool and per profile a task of this run
	// names, the structural identity every answering Orker of that pool
	// reported at SUBMIT (docs/DEPLOY.md, "Profiles"): which Orkers serve a
	// pool and what they offer are cluster facts workflow code may not read
	// (AGENTS.md §2.1). Workflow code only copies it into each task's input
	// (ProfileRefsFor), where the Orker refuses an attempt whose profile no
	// longer matches. omitempty, so a plan with none encodes exactly as
	// before.
	ProfileIdentities map[string]map[string]string `json:"profile_identities,omitempty"`

	// ProfileContracts is, per pool and per profile a task of this run names,
	// the contracts the pool's Orkers served at SUBMIT (docs/DEPLOY.md,
	// "Changing a profile"): the set an undeclared task may run under, and
	// the contract each declared label pinned. Like ProfileIdentities it is a
	// cluster fact decided at submit and only copied by workflow code.
	// omitempty, so a plan with none, and every plan built before it existed,
	// encodes exactly as before.
	ProfileContracts map[string]map[string]ProfileContracts `json:"profile_contracts,omitempty"`

	// profileCensusAt is when the census ProfileContracts was judged on was
	// read (ProfileCensusReadAt). Unexported, so never encoded.
	profileCensusAt time.Time

	// History is the per-execution history ceiling this run must stop below,
	// decided at submit (workflow code may not read config, AGENTS.md §2.1).
	// Its Reserve — every count and byte charge the guard uses — is carried
	// too, so a later release that re-measures one moves only NEW runs'
	// crossing points, never a recorded one. A run-tree limit: set on the
	// top-level plan only, copied into each child's start plan by
	// startChild. Nil on every plan built before this field existed: no
	// recorded history enters the guard (history_guard.go).
	History *HistoryLimit `json:"history,omitzero"`

	// Continuation is this run's per-generation budget and every number the
	// writer, the hold and the bootstrap decide with (continuation.go), set
	// at submit when engine.continuation.enabled. A run-tree limit, like
	// History: set on the top-level plan by pinRunTreeLimits, copied into
	// each child's start plan by startChild. Nil: the run never continues as
	// new (every plan built before this field, and every plan of a
	// deployment that leaves it off). Set only together with History; a
	// plan carrying a Continuation without a History never continues.
	Continuation *ContinuationBudget `json:"continuation,omitzero"`

	// InFlight caps what this run keeps pending (holdDispatch,
	// continuation.go). A run-tree limit. Nil: no cap (every plan built
	// before this field).
	InFlight *InFlightLimit `json:"in_flight,omitzero"`

	// Definition names this run's planned definition when it is too large
	// to travel as a start payload (docs/DEPLOY.md §1,
	// engine.definition_by_reference): the start then carries a stub naming
	// the definition by name, version, namespace and digest, and every
	// generation copies the immutable object into its own history before it
	// decides anything (loadDefinition, definition_ref.go). It carries no
	// URI: the object's key is derived from the run id wherever it is read
	// (artifacts.RunContinuationKey, kind "definition", generation 0), so the
	// plan is the same whatever run id the start ends with, and a plan can
	// never name another run's object. Top-level runs only: a child's
	// definition travels inside its parent's plan. Nil on every plan built
	// before it, and on every run whose definition fits a start payload.
	Definition *DefinitionRef `json:"definition,omitzero"`

	// SnapshotOverlay is when this run writes a snapshot overlay instead of
	// a full snapshot for a non-terminal event (snapshot_overlay.go,
	// docs/DEPLOY.md §1, engine.snapshot_overlays): set at submit on a run
	// tree whose largest run declares more than DefaultOverlayMinRows rows,
	// while every engine process that could decide the run or project its
	// events can read an overlay. A run-tree limit, like History: set on
	// the top-level plan only, copied into each child's start plan by
	// startChild; each run then decides from its OWN declared rows, so a
	// small child of a large parent keeps full snapshots. Nil: every
	// snapshot is full (every plan built before this field, and every plan
	// of a deployment that leaves it off).
	SnapshotOverlay *OverlayPolicy `json:"snapshot_overlay,omitzero"`

	// PinCreationTimeoutSeconds is how long a pin of this run waits for some
	// pin host of its pool to have a free session slot before its first task
	// fails PIN_UNAVAILABLE (engine.pin_creation_timeout, docs/DEPLOY.md §1).
	// Whole seconds, decided at submit for the reason Limits is: it is
	// process configuration workflow code may not read (AGENTS.md §2.1).
	// A run-tree limit, like History: set on the top-level plan by
	// pinRunTreeLimits and copied into each child's start plan by
	// childStartPlan. Zero means unset, and an unset value is the five
	// minutes every build before the field used, which is also what a run
	// whose history was recorded before the pin-creation-timeout version gate
	// takes whatever this says (pinCreationTimeoutFor). omitzero, so a plan
	// with none encodes exactly as it did before the field existed.
	PinCreationTimeoutSeconds int32 `json:"pin_creation_timeout_seconds,omitzero"`

	// Fairness is the fairness key and weight every task activity of this
	// run is dispatched with (fairness.go): the run's namespace and its
	// fair_share_weight, decided at submit because the namespace document is
	// registry state workflow code may not read. A run-tree value: set on the
	// top-level plan, cleared from each nested child plan and copied into a
	// child's start plan by childStartPlan, since a child runs in its
	// parent's namespace. Nil on every plan built before it, which schedules
	// with no priority and asks no gate.
	Fairness *FairnessSelection `json:"fairness,omitzero"`

	// RetryMaxIntervalSeconds is the longest wait between two attempts of a
	// task whose retry: sets no maximum_interval_seconds (retry_spacing.go):
	// DefaultRetryMaxInterval, decided at submit, because a code constant may
	// not decide a retry spacing (AGENTS.md §7.2). A run-tree value, like
	// PinCreationTimeoutSeconds: set on the top-level plan by
	// pinRunTreeLimits and copied into each child's start plan by
	// childStartPlan. Zero means unset, and an unset value is the 100 seconds
	// every build before the field used, which is also what a run whose
	// history was recorded before the retry-max-interval gate takes whatever
	// this says. omitzero, so a plan with none encodes exactly as it did
	// before the field existed.
	RetryMaxIntervalSeconds int32 `json:"retry_max_interval_seconds,omitzero"`

	// Interruptions are the run's interruption budgets (E92): how many times
	// an interrupted attempt runs again without spending a task's own
	// attempts. Decided at submit from the deployment's
	// engine.interruptions.* settings, because a code constant may not decide
	// a re-dispatch (AGENTS.md §7.2); a run-tree value, copied into each
	// child's start plan. Nil on every plan built before it: such a run
	// re-runs nothing for an interruption, as before. omitzero, so a plan
	// with none encodes exactly as it did before the field existed.
	Interruptions *InterruptionBudgets `json:"interruptions,omitzero"`

	// InputArtifacts are the artifacts a child run received from the task
	// that composed it, by the name of the artifact input each binds
	// (docs/DSL.md, "Passing artifacts to a child run"): references to
	// objects a run of the tree recorded, never bytes. Set by the parent's
	// startChild from recorded results; empty on every other plan. A task of
	// the run whose definition names one (Task.input_artifacts) is handed it
	// as the reserved upstream _inputs. omitzero.
	InputArtifacts []NamedArtifactPlan `json:"input_artifacts,omitzero"`

	// ArtifactClaims bounds the claim markers the engine writes for objects
	// passed between the runs of a tree: a parent's claim at a child's start
	// and a child's own before its first dispatch (docs/STORAGE.md,
	// "Reclaiming space"). Decided at submit, a run-tree value copied into
	// each child's start plan. omitzero.
	ArtifactClaims *ArtifactClaimPolicy `json:"artifact_claims,omitzero"`

	// RecordRootRun says this run's child runs record the root of their chain
	// (RunOrigin.RootRunID, childStartPlan). It changes what a child start
	// carries and no command. It rides the plan rather than a gate so that a
	// run accepted before the field keeps sending what its build sent, its
	// children record no root and neither do theirs: a parent that is itself
	// a child, with no root recorded, cannot say what its child's root is. A
	// run-tree value: set on the top-level plan by pinRunTreeLimits and copied
	// into each child's start plan by childStartPlan. omitzero, so a plan
	// without it encodes exactly as it did before the field existed.
	RecordRootRun bool `json:"record_root_run,omitzero"`

	// RootRunIndexed says the cluster indexes a run's root
	// (task.EnsureRunRootSearchAttribute), so a child start may stamp it; a
	// start stamping an attribute the cluster does not have is refused. It is
	// the submitting engine's boot answer, pinned because workflow code may not
	// ask, and a run-tree value like RecordRootRun, so every child of a plan
	// has the one answer. omitzero, as RecordRootRun is.
	RootRunIndexed bool `json:"root_run_indexed,omitzero"`

	// Shared is the decision that admitted this run's definition from another
	// namespace (internal/authz): set only on the plan of a child run whose
	// Orkflow another namespace shared through composition, naming the
	// shared_with entry that admitted it. It is a record of why the run was
	// allowed, decided at the parent's submit; workflow code never reads it.
	// omitzero, so every other plan encodes exactly as before.
	Shared *authz.Decision `json:"shared,omitzero"`

	// RunContextSHA256 names the run tree's run-context object (the run's
	// origin, Orkflow and composed lineage, written once at submit under the
	// root run's prefix), so every task's input carries a run context that
	// references it (ExecuteTaskInput.run_context). Set at submit on the
	// top-level plan and copied into each child's start plan by
	// childStartPlan, as RunContextMaxBytes is. Empty means the run's tasks
	// are given no run context.
	RunContextSHA256 string `json:"run_context_sha256,omitzero"`
	// RunContextIndex is this run's entry in the run-context object: 0 for
	// the root, then a pre-order walk of the composed children, task ids in
	// order. Set at submit on each nested child plan.
	RunContextIndex int32 `json:"run_context_index,omitzero"`
	// RunContextMaxBytes bounds a task's run context, the one part of its
	// input that may take it past the 256 KiB payload bound (AGENTS.md §2.2):
	// DefaultRunContextMaxBytes, pinned per run.
	RunContextMaxBytes int64 `json:"run_context_max_bytes,omitzero"`
	// Identity is set at submit on the plan of every run of a tree that is
	// admitted (docs/TASK_IDENTITY.md, "The admission record"), and nil on
	// every other plan, so their plans encode exactly as before. It is
	// derived from the same value as the run's admission record, so the two
	// cannot disagree. omitzero.
	Identity *IdentityPlan `json:"identity,omitzero"`
}

// IdentityPlan is what a run's plan carries of its admission.
type IdentityPlan struct {
	// AdmissionSHA256 names the run's admission record, which an engine
	// wrote under the run's id before the run was started. Until the start
	// it is a secret: unguessable for the record's nonce, and written
	// nowhere but here and in the record's own name. Never logged.
	AdmissionSHA256 string `json:"admission_sha256"`
	// Issuer is the issuer the run was admitted under: the only one whose
	// tokens its tasks are given.
	Issuer string `json:"issuer"`
	// TokenTTLSeconds is identity.token.ttl as admitted: the life of a token
	// for an identity that names none.
	TokenTTLSeconds int32 `json:"token_ttl_seconds"`
	// SpecVersion is the projection the run's tasks' spec digests use
	// (task.IdentitySpecDigest).
	SpecVersion uint32 `json:"spec_v"`
	// AttemptsExtra is the attempts past a task's retry policy that may hold
	// an identity, resolved at submit with the run's namespace in hand.
	AttemptsExtra int32 `json:"attempts_extra"`
	// Grants are the run's grants as admitted, by name; nil when it names
	// none.
	Grants map[string]artifacts.GrantSelection `json:"grants,omitempty"`
}

// AttemptDeadlineSeconds is the deadline the interpreter gives the cluster
// for one attempt of t, the task's timeout, its grace and the plan's
// transfer budget: the attempt_deadline_seconds a task's admission pins.
func AttemptDeadlineSeconds(t *orkstackv1.Task, transferBudgetSeconds int32) int64 {
	return int64(timeoutSecondsFor(t)) + int64(graceSecondsFor(t)) + int64(transferBudgetSeconds)
}

// OverlayPolicy is a run's snapshot-overlay thresholds, pinned per run
// (DefaultOverlay* at submit), because MinRows decides which local activity
// workflow code runs for an event's snapshot. A run uses overlays when its
// declared rows — its tasks plus every fan-out's max_items — exceed MinRows.
// MaxChain and MaxRows act inside that activity only, and their outcome is
// recorded: a full snapshot is written after MaxChain overlays in a row, and
// whenever more than MaxRows rows differ from the base.
type OverlayPolicy struct {
	MinRows  int64 `json:"min_rows"`
	MaxChain int64 `json:"max_chain"`
	MaxRows  int64 `json:"max_rows"`
}

// valid is the structural check workflow code makes before it uses a
// policy: a policy that fails it is never used, and every snapshot of the
// run is full. Only signs, never a tunable constant.
func (p *OverlayPolicy) valid() bool {
	return p != nil && p.MinRows >= 0 && p.MaxChain >= 1 && p.MaxRows >= 0
}

// DefinitionRef is a run's planned definition stored by reference: the
// whole object's SHA-256, its stored and decoded sizes, and each gzip
// member's place in it. Plain Go, mirroring orkstack.v1.ContinuationObject
// without its URI.
type DefinitionRef struct {
	SHA256    string              `json:"sha256"`
	SizeBytes int64               `json:"size_bytes"`
	RawBytes  int64               `json:"raw_bytes"`
	Parts     []DefinitionPartRef `json:"parts"`
}

// DefinitionPartRef is one gzip member of a definition object.
type DefinitionPartRef struct {
	Offset    int64  `json:"offset"`
	SizeBytes int64  `json:"size_bytes"`
	RawBytes  int64  `json:"raw_bytes"`
	SHA256    string `json:"sha256"`
}

// ContinuationBudget is a run's per-generation budget: how much a generation
// grows before it continues as new at its next moment with nothing in
// flight, and every number the hold and the next generation's bootstrap
// decide with. Every field is pinned per run (DefaultContinuation* at
// submit), because each one decides whether, which, how many or where
// workflow code issues a command.
type ContinuationBudget struct {
	Events             int64         `json:"events"`               // growth per generation, in history events
	Bytes              int64         `json:"bytes"`                // growth per generation, in history bytes
	CheckpointMaxBytes int64         `json:"checkpoint_max_bytes"` // = Bytes: the largest copy a generation reads, its saved state and definition together, under one cap
	RetryEvents        int64         `json:"retry_events"`         // Events/4: growth before a failed switch or a given-up hold is retried
	RetryBytes         int64         `json:"retry_bytes"`          // Bytes/4
	HoldPercent        int64         `json:"hold_percent"`         // the hold line, % of the cluster limit
	MaxGenerations     int64         `json:"max_generations"`      // no switch into a generation at or past it
	ChunkBytes         int64         `json:"chunk_bytes"`          // one bootstrap read
	ChunksPerTask      int64         `json:"chunks_per_task"`      // reads per workflow task before a yield
	Restore            RestorePolicy `json:"restore"`
	// ItemInputFloorBytes is the per-item input floor of a fan-out total
	// above 1,024 items (eachBounds): DefaultItemInputFloorBytes, set with
	// continuation, which every run declaring more than 1,000 items needs.
	// Zero in every plan built before it.
	ItemInputFloorBytes int64 `json:"item_input_floor_bytes,omitzero"`
}

// RestorePolicy is the bootstrap's read deadline and the Temporal retry
// policy of each read. Pinned so that the history bound validated for this
// run (RestoreHistoryBound) holds for its whole life.
type RestorePolicy struct {
	TimeoutSeconds       int64 `json:"timeout_seconds"`        // engine.continuation.restore_timeout
	AttemptSeconds       int64 `json:"attempt_seconds"`        // one read attempt
	BackoffInitialMillis int64 `json:"backoff_initial_millis"` // the first retry's backoff
	BackoffMaxSeconds    int64 `json:"backoff_max_seconds"`    // the largest retry backoff
}

// InFlightLimit caps the activities, child runs and pins one run keeps
// pending. Max is engine.run_max_in_flight; HardStop is absolute, an open
// pin's closure included.
//
// PublishWindow and DispatchBytes pace the detached publish path
// (TaskPublishChange, task_publish.go): a plain task's TASK_SCHEDULED and
// TASK_STARTED are published without the loop waiting for them, at most
// PublishWindow outstanding, and one workflow task schedules at most
// DispatchBytes of plain tasks' activity input. Both zero in every plan built
// before the path, which then takes the awaited path and asks no version.
type InFlightLimit struct {
	Max           int32 `json:"max"`
	HardStop      int32 `json:"hard_stop"`
	PublishWindow int32 `json:"publish_window,omitzero"`
	DispatchBytes int64 `json:"dispatch_bytes,omitzero"`
	PublishEvents int64 `json:"publish_events,omitzero"`
}

// PublishReserveEvents is the history the plan's publish window can still
// append when every one of its publishes is outstanding: PublishWindow ×
// PublishEvents, or zero for a plan without the window. The flat-stage
// bound reserves it (ShapeMaxTasks). Pure.
func (l InFlightLimit) PublishReserveEvents() int64 {
	if l.PublishWindow <= 0 || l.PublishEvents <= 0 {
		return 0
	}
	return int64(l.PublishWindow) * l.PublishEvents
}

// continuationChunkCeiling is the largest chunk a bootstrap may read: the
// 128 KiB that AGENTS.md §2.2's saved-state exception states. A contract
// constant fixed by AGENTS.md, not a tunable default.
const continuationChunkCeiling = 128 << 10

// valid is the structural check every reader of a budget makes first. It
// reads no tunable constant: only positivity, 100%, and
// continuationChunkCeiling. A budget pinRunTreeLimits built always passes; a
// hand-built or forged one that fails never continues and is never
// restored.
func (b *ContinuationBudget) valid() bool {
	if b == nil {
		return false
	}
	r := b.Restore
	return b.Events > 0 && b.Bytes > 0 && b.CheckpointMaxBytes > 0 &&
		b.RetryEvents > 0 && b.RetryBytes > 0 &&
		b.HoldPercent > 0 && b.HoldPercent <= 100 &&
		b.MaxGenerations >= 1 &&
		b.ChunkBytes > 0 && b.ChunkBytes <= continuationChunkCeiling &&
		b.ChunksPerTask >= 1 &&
		r.TimeoutSeconds > 0 && r.AttemptSeconds > 0 && r.BackoffInitialMillis > 0 && r.BackoffMaxSeconds > 0
}

// HistoryLimit is the cluster's per-execution history limits, as the
// deployment declared them (engine.history.max_events / max_bytes), and the
// reserve a run keeps back below them so its own failure path can still be
// recorded (history_guard.go, docs/DEPLOY.md §1).
type HistoryLimit struct {
	MaxEvents int64          `json:"max_events"` // the cluster's per-execution event limit
	MaxBytes  int64          `json:"max_bytes"`  // the cluster's per-execution history-size limit
	Reserve   HistoryReserve `json:"reserve"`
}

// HistoryReserve is an upper bound on what the run's own failure path
// appends, plus the byte charges noteCommand applies to commands not yet
// recorded. Every number is pinned per run (DefaultHistoryReserve at submit),
// because each one decides where the guard crosses — a replay-visible
// command.
type HistoryReserve struct {
	Fixed                  int64 `json:"fixed"`                     // drain, terminal emit, close, marker, uncounted per-task commands
	PerInFlight            int64 `json:"per_in_flight"`             // cancel request, cancelled, task share, terminal emit (the dispatched task's own)
	PerPending             int64 `json:"per_pending"`               // a PENDING (never dispatched) task's markCanceled emit
	PerAwait               int64 `json:"per_await"`                 // an open await's resolution or cancel
	PerOpenPin             int64 `json:"per_open_pin"`              // ReleasePin + CompleteSession + tasks
	PerFanOut              int64 `json:"per_fan_out"`               // finishFanOutOnRunEnd's emit
	PerHandler             int64 `json:"per_handler"`               // one failure handler (plain, await or composed)
	BytesPerEvent          int64 `json:"bytes_per_event"`           // the bytes charged per reserved event
	PerInFlightBytes       int64 `json:"per_in_flight_bytes"`       // one completion's result
	CommandBytes           int64 `json:"command_bytes"`             // attributes around one scheduled input or chunk
	PublishBytes           int64 `json:"publish_bytes"`             // one detached item publish
	SessionBytes           int64 `json:"session_bytes"`             // one pin's session creation
	ChildStartBytes        int64 `json:"child_start_bytes"`         // one child start, four payloads of ≤ 256 KiB
	AwaitHandlerStartBytes int64 `json:"await_handler_start_bytes"` // one await failure handler's start
}

// CachePlan is the plan-side caching decision (docs/DSL.md "Caching"). Plain
// Go, like RunPlan.
type CachePlan struct {
	// MaxTTLSeconds is ORK_CACHE_MAX_TTL at submit, in whole seconds, >= 1.
	MaxTTLSeconds int64 `json:"max_ttl_seconds"`
	// Refresh is SubmitRunRequest.no_cache.
	Refresh bool `json:"refresh,omitzero"`
}

// EffectiveCacheTTL is the TTL a cached task's lookup honours: the cap when
// the task set none (0), else the smaller of the two. Pure.
func EffectiveCacheTTL(ttlSeconds int32, capSeconds int64) int64 {
	if ttlSeconds <= 0 || int64(ttlSeconds) > capSeconds {
		return capSeconds
	}
	return int64(ttlSeconds)
}

// AwaitCredential is one callback await's verification material, the shape
// pkg/triggers keeps for a trigger: a per-await random salt and
// sha256(salt || token), both hex. encoding/json sorts map keys, so a plan
// encodes to the same bytes every time.
type AwaitCredential struct {
	Salt   string `json:"salt"`
	Digest string `json:"digest"`
}

// ImagePolicySelection is the plan-side copy of
// orkstack.v1.ImagePolicySelection, a plain Go type for the reason
// TelemetrySelection is one: nesting a proto message inside RunPlan would
// change how the whole plan is encoded.
type ImagePolicySelection struct {
	RequireDigest     bool     `json:"require_digest,omitzero"`
	AllowedRegistries []string `json:"allowed_registries,omitempty"`
	RequireSignedBy   string   `json:"require_signed_by,omitempty"`
	SigningKeyPEM     string   `json:"signing_key_pem,omitempty"`
}

// Proto is nil-safe: a nil selection yields a nil message, so
// ExecuteTaskInput.ImagePolicy stays unset for a run with no policy.
func (s *ImagePolicySelection) Proto() *orkstackv1.ImagePolicySelection {
	if s == nil {
		return nil
	}
	return &orkstackv1.ImagePolicySelection{
		RequireDigest:     s.RequireDigest,
		AllowedRegistries: append([]string(nil), s.AllowedRegistries...),
		RequireSignedBy:   s.RequireSignedBy,
		SigningKeyPem:     s.SigningKeyPEM,
	}
}

// ImagePolicySelectionFromProto is Proto's inverse, nil for nil.
func ImagePolicySelectionFromProto(p *orkstackv1.ImagePolicySelection) *ImagePolicySelection {
	if p == nil {
		return nil
	}
	return &ImagePolicySelection{
		RequireDigest:     p.GetRequireDigest(),
		AllowedRegistries: append([]string(nil), p.GetAllowedRegistries()...),
		RequireSignedBy:   p.GetRequireSignedBy(),
		SigningKeyPEM:     p.GetSigningKeyPem(),
	}
}

// RunOrigin is the plan-side copy of orkstack.v1.RunOrigin. It is a plain Go
// type for the reason TelemetrySelection is: the data converter encodes a
// top-level protobuf message with protojson and everything else with
// encoding/json, so nesting a proto message here would silently change how
// the whole plan is encoded. Kind is the enum's proto name
// ("RUN_ORIGIN_KIND_TRIGGER"), as TelemetrySelection.TaskLogs is.
type RunOrigin struct {
	Kind        string `json:"kind"`
	ID          string `json:"id,omitempty"`
	Reference   string `json:"reference,omitempty"`
	Description string `json:"description,omitempty"`
	Principal   string `json:"principal,omitempty"`
	RootRunID   string `json:"root_run_id,omitempty"`
	// PrincipalVerified and AuthMethod say how Principal was known: set
	// only for an identity the API verified, absent for an asserted one,
	// so a plan recorded before them encodes and decodes unchanged.
	PrincipalVerified bool   `json:"principal_verified,omitzero"`
	AuthMethod        string `json:"auth_method,omitempty"`
}

// Proto renders the origin as the wire message the snapshot carries.
// Nil-safe: a nil origin produces a nil message, so Run.origin stays unset
// exactly as it was before this field existed. An unrecognized kind name
// falls back to UNSPECIFIED rather than panicking, as taskLogExportProto
// does, because a plan is data a harness can hand-build.
func (o *RunOrigin) Proto() *orkstackv1.RunOrigin {
	if o == nil {
		return nil
	}
	kind := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED
	if v, ok := orkstackv1.RunOriginKind_value[o.Kind]; ok {
		kind = orkstackv1.RunOriginKind(v)
	}
	return &orkstackv1.RunOrigin{
		Kind:        kind,
		Id:          o.ID,
		Reference:   o.Reference,
		Description: o.Description,
		Principal:   o.Principal,
		RootRunId:   o.RootRunID,

		PrincipalVerified: o.PrincipalVerified,
		AuthMethod:        o.AuthMethod,
	}
}

// OriginFromProto is Proto's inverse, used by the API to build the plan
// from the RunOrigin it decided. Nil in, nil out.
func OriginFromProto(o *orkstackv1.RunOrigin) *RunOrigin {
	if o == nil {
		return nil
	}
	return &RunOrigin{
		Kind:        o.GetKind().String(),
		ID:          o.GetId(),
		Reference:   o.GetReference(),
		Description: o.GetDescription(),
		Principal:   o.GetPrincipal(),
		RootRunID:   o.GetRootRunId(),

		PrincipalVerified: o.GetPrincipalVerified(),
		AuthMethod:        o.GetAuthMethod(),
	}
}

// WorkspaceLimits is the plan-side copy of orkstack.v1.WorkspaceLimits.
//
// It is a plain Go type for the same reason RunPlan is: the data converter
// encodes a top-level protobuf message with protojson and everything else with
// encoding/json, so nesting a proto message inside this struct would silently
// change how the whole plan is encoded.
type WorkspaceLimits struct {
	ValuesMaxBytes   int64 `json:"values_max_bytes"`
	ArtifactMaxCount int32 `json:"artifact_max_count"`
	ArtifactMaxBytes int64 `json:"artifact_max_bytes"`
	LogMaxBytes      int64 `json:"log_max_bytes"`
	CommitMaxBytes   int64 `json:"commit_max_bytes"`
}

// Default contract limits (docs/OCI_ACTIVITY_CONTRACT.md §2.2). They are the values a deployment
// gets with no configuration, and they are what task.json reports so a
// workload can respect them instead of discovering them by failing.
const (
	DefaultValuesMaxBytes   int64 = 262144     // 256 KiB
	DefaultArtifactMaxCount int32 = 256        //
	DefaultArtifactMaxBytes int64 = 5368709120 // 5 GiB
	DefaultLogMaxBytes      int64 = 16777216   // 16 MiB per stream per attempt
	DefaultCommitMaxBytes   int64 = 4194304    // 4 MiB
)

// DefaultLimits returns the contract limits with every field populated.
func DefaultLimits() WorkspaceLimits {
	return WorkspaceLimits{
		ValuesMaxBytes:   DefaultValuesMaxBytes,
		ArtifactMaxCount: DefaultArtifactMaxCount,
		ArtifactMaxBytes: DefaultArtifactMaxBytes,
		LogMaxBytes:      DefaultLogMaxBytes,
		CommitMaxBytes:   DefaultCommitMaxBytes,
	}
}

// Proto renders the plan's limits as the wire message the Orker receives.
// A zero field falls back to its documented default, so a plan built by a
// harness that set none still produces a container with real bounds.
func (l WorkspaceLimits) Proto() *orkstackv1.WorkspaceLimits {
	filled := DefaultLimits()
	if l.ValuesMaxBytes > 0 {
		filled.ValuesMaxBytes = l.ValuesMaxBytes
	}
	if l.ArtifactMaxCount > 0 {
		filled.ArtifactMaxCount = l.ArtifactMaxCount
	}
	if l.ArtifactMaxBytes > 0 {
		filled.ArtifactMaxBytes = l.ArtifactMaxBytes
	}
	if l.LogMaxBytes > 0 {
		filled.LogMaxBytes = l.LogMaxBytes
	}
	if l.CommitMaxBytes > 0 {
		filled.CommitMaxBytes = l.CommitMaxBytes
	}
	return &orkstackv1.WorkspaceLimits{
		ValuesMaxBytes:   filled.ValuesMaxBytes,
		ArtifactMaxCount: filled.ArtifactMaxCount,
		ArtifactMaxBytes: filled.ArtifactMaxBytes,
		LogMaxBytes:      filled.LogMaxBytes,
		CommitMaxBytes:   filled.CommitMaxBytes,
	}
}

// TelemetrySelection is the plan-side copy of orkstack.v1.TelemetrySelection.
//
// It is a plain Go type for the same reason WorkspaceLimits is: the data
// converter encodes a top-level protobuf message with protojson and
// everything else with encoding/json, so nesting a proto message inside
// RunPlan would silently change how the whole plan is encoded.
//
// TaskLogs is the enum's Go name ("TASK_LOG_EXPORT_ENABLED", ...) rather than
// the orkstackv1 enum type, for the same reason: an enum value nested here
// would still be a proto-generated type, and JSON-marshaling one directly
// depends on protobuf details this plain struct is meant to stay clear of.
// Empty means TASK_LOG_EXPORT_UNSPECIFIED.
type TelemetrySelection struct {
	Sink       string            `json:"sink,omitempty"`
	TaskLogs   string            `json:"task_logs,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Proto renders the selection as the wire message the Orker and the run
// snapshot carry. Nil-safe: a nil selection (no policy, no labels) produces a
// nil message, so ExecuteTaskInput.Telemetry and Run.Telemetry stay unset
// exactly as they were before this field existed.
func (s *TelemetrySelection) Proto() *orkstackv1.TelemetrySelection {
	if s == nil {
		return nil
	}
	return &orkstackv1.TelemetrySelection{
		Sink:       s.Sink,
		TaskLogs:   taskLogExportProto(s.TaskLogs),
		Attributes: s.Attributes,
	}
}

// taskLogExportProto maps the plan's plain-string enum name back to the wire
// enum. An unrecognized name — which validation should never let through —
// falls back to UNSPECIFIED rather than panicking: a plan is data a harness
// can hand-build, and a permissive read matches how WorkspaceLimits.Proto
// treats zero fields as "not set" rather than refusing them.
func taskLogExportProto(name string) orkstackv1.TaskLogExport {
	if name == "" {
		return orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED
	}
	if v, ok := orkstackv1.TaskLogExport_value[name]; ok {
		return orkstackv1.TaskLogExport(v)
	}
	return orkstackv1.TaskLogExport_TASK_LOG_EXPORT_UNSPECIFIED
}

// itemInputFloor is the per-item input floor this run pinned (eachBounds),
// 0 for a plan without one: every plan built before it, and every run
// without continuation.
func (p RunPlan) itemInputFloor() int64 {
	if p.Continuation == nil || p.Continuation.ItemInputFloorBytes < 0 {
		return 0
	}
	return p.Continuation.ItemInputFloorBytes
}

// Route answers where one pool's tasks go.
//
// A pool with no route is an error, never a fallback. Falling back would run a
// gpu task on the shared pool — the wrong machines, silently, which is worse
// than not running it. The error names the pool and the routes that do exist,
// because the only way this happens is a plan built for a different definition.
//
// A route onto one of the engine's own queues is refused for the same reason
// task.QueueForPool refuses it: ork-engine-events carries the engine's
// projection activities, and an Orker polling it would take engine work off
// the queue the engine is waiting on. The resolver cannot produce such a plan
// (task.RouteForPool refuses first) and `ork` will not boot on that channel;
// the check is here because this is the last place the plan is read, and a
// plan is data a harness can hand-build.
func (p RunPlan) Route(pool string) (string, error) {
	queue := p.Routes[pool]
	if queue == "" {
		return "", fmt.Errorf("pool %q has no route in this run's plan (planned pools: %s)",
			pool, p.pools())
	}
	if queue == task.EngineEventQueue {
		return "", fmt.Errorf("%w: this run's plan routes pool %q to %q, which carries "+
			"the engine's own projection activities", task.ErrReservedQueue, pool, queue)
	}
	return queue, nil
}

func (p RunPlan) pools() string {
	if len(p.Routes) == 0 {
		return "none"
	}
	names := make([]string, 0, len(p.Routes))
	for pool := range p.Routes {
		names = append(names, pool)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// InterruptionBudgets are a run's interruption budgets (E92, design §4.3),
// pinned in its plan at submit.
type InterruptionBudgets struct {
	// Lost is how many times a task re-runs after its Orker stopped answering.
	Lost int32 `json:"lost"`
	// LostRunCap bounds the run's lost-Orker events across all its tasks.
	LostRunCap int32 `json:"run_cap"`
	// HandedBack is how many times a task re-runs after a stopping Orker
	// handed it back.
	HandedBack int32 `json:"handed_back"`
	// NeverStarted is how many times a task re-runs, with no wait, after a
	// stopping Orker returned it before it started; past it, with a back-off.
	NeverStarted int32 `json:"never_started"`
	// NeverStartedFaults is how many consecutive never-started faults
	// (object storage refusing the attempt's record, a drifted profile) a
	// task survives before they spend its own attempts.
	NeverStartedFaults int32 `json:"faults"`
	// NeverStartedBackoffMillis is the initial wait of the never-started
	// back-off, doubling (exponent clamped at 6, wait at 60 s).
	NeverStartedBackoffMillis int64 `json:"backoff_ms"`
	// NeverStartedFaultFloorMillis floors the fault back-off's initial wait
	// at the time an Orker's store-outage cordon takes to trip (its store
	// check's interval times its failures, at the Orker's defaults), so a
	// one-attempt task outlives a store blip until the cordon can act (design
	// §4.8 4a). 0, in a plan from before the field, floors nothing.
	NeverStartedFaultFloorMillis int64 `json:"fault_floor_ms,omitzero"`
}

// NamedArtifactPlan is orkstackv1.NamedArtifact on the plan: one artifact a
// run received, by the name of the input it binds, with the run and task that
// recorded it.
type NamedArtifactPlan struct {
	Name     string          `json:"name"`
	RunID    string          `json:"run_id,omitzero"`
	TaskID   string          `json:"task_id,omitzero"`
	Artifact ArtifactRefPlan `json:"artifact"`
}

// ArtifactRefPlan is orkstackv1.ArtifactRef on the plan.
type ArtifactRefPlan struct {
	Name        string            `json:"name"`
	URI         string            `json:"uri"`
	ContentType string            `json:"content_type,omitzero"`
	SizeBytes   int64             `json:"size_bytes,omitzero"`
	SHA256      string            `json:"sha256,omitzero"`
	Metadata    map[string]string `json:"metadata,omitzero"`
}

// ArtifactClaimPolicy is how a run tree passes artifacts: how long one
// attempt of a claim local activity may take and how many it makes before it
// fails its run's start (CHILD_INPUT_UNAVAILABLE), and how many bytes each
// artifacts: binding may add to a child's start (CHILD_START_TOO_LARGE),
// which the submit reserved inside the tree's 256 KiB plan bound.
type ArtifactClaimPolicy struct {
	TimeoutSeconds      int32 `json:"timeout_seconds"`
	MaxAttempts         int32 `json:"max_attempts"`
	ReserveBytesPerBind int32 `json:"reserve_bytes_per_binding"`
}

// ArtifactRefPlanOf copies an ArtifactRef onto the plan.
func ArtifactRefPlanOf(ref *orkstackv1.ArtifactRef) ArtifactRefPlan {
	out := ArtifactRefPlan{Name: ref.GetName(), URI: ref.GetUri(), ContentType: ref.GetContentType(),
		SizeBytes: ref.GetSizeBytes(), SHA256: ref.GetSha256()}
	if len(ref.GetMetadata()) > 0 {
		out.Metadata = make(map[string]string, len(ref.GetMetadata()))
		for k, v := range ref.GetMetadata() {
			out.Metadata[k] = v
		}
	}
	return out
}

// Proto is the ArtifactRef the plan holds.
func (a ArtifactRefPlan) Proto() *orkstackv1.ArtifactRef {
	out := &orkstackv1.ArtifactRef{Name: a.Name, Uri: a.URI, ContentType: a.ContentType,
		SizeBytes: a.SizeBytes, Sha256: a.SHA256}
	if len(a.Metadata) > 0 {
		out.Metadata = make(map[string]string, len(a.Metadata))
		for k, v := range a.Metadata {
			out.Metadata[k] = v
		}
	}
	return out
}

// Proto is the NamedArtifact the plan holds.
func (n NamedArtifactPlan) Proto() *orkstackv1.NamedArtifact {
	return &orkstackv1.NamedArtifact{Name: n.Name, RunId: n.RunID, TaskId: n.TaskID, Artifact: n.Artifact.Proto()}
}

// namedArtifactsProto converts a plan list for a snapshot or a row.
func namedArtifactsProto(in []NamedArtifactPlan) []*orkstackv1.NamedArtifact {
	if len(in) == 0 {
		return nil
	}
	out := make([]*orkstackv1.NamedArtifact, 0, len(in))
	for _, n := range in {
		out = append(out, n.Proto())
	}
	return out
}
