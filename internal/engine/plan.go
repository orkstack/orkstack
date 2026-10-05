package engine

import (
	"fmt"
	"sort"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
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

	// Limits are the deployment-wide CONTRACT limits (design §2.2
	// WorkspaceLimits): values, artifact count and size, log and commit
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
	// omitempty, so a plan with none encodes exactly as it did before the
	// field existed.
	Telemetry *TelemetrySelection `json:"telemetry,omitempty"`

	// ImagePolicy is the image policy in force for this run, or nil when the
	// deployment configured none and the namespace names no signer. Decided
	// at submit (internal/api.prepare) from the ImagePolicy document and the
	// namespace, for the reason Limits and Telemetry are: both are state a
	// deterministic workflow may not read (AGENTS.md §2.1), and every
	// attempt of one task must be judged by one rule. It is copied onto
	// every ExecuteTaskInput and is otherwise not read by workflow code.
	// omitempty, so a plan with none encodes exactly as it did before the
	// field existed.
	ImagePolicy *ImagePolicySelection `json:"image_policy,omitempty"`

	// Origin is what started this run, or nil for a plan built before origins
	// existed. It is decided at SUBMIT for the reason CommitRoot and
	// Telemetry are: the door that accepted the submission is the only thing
	// that knows, and workflow code may read neither a request header nor a
	// trigger record (AGENTS.md §2.1). omitempty, so a plan with none encodes
	// exactly as it did before the field existed, and the golden history
	// fixture keeps matching without re-recording.
	Origin *RunOrigin `json:"origin,omitempty"`

	// Awaits holds, per task id, the callback credential of every `await:`
	// of this run that declares `callback: true`: a salt and the salted
	// SHA-256 of a token minted at SUBMIT (internal/api). NEVER the token.
	//
	// RunWorkflow does not read it. It rides the plan because the plan is the
	// run's replay-pinned, per-run record that any replica can read back from
	// the run's own start event (TemporalRunReader.RunStart) — which is what
	// lets the callback door verify a token with no table of pending
	// callbacks anywhere (AGENTS.md §13). omitempty, so a plan with none
	// encodes exactly as it did before the field existed and the golden
	// history fixture keeps matching.
	Awaits map[string]AwaitCredential `json:"awaits,omitempty"`

	// Children is, per composed task id, the child run that task starts: its
	// run id, its planned definition and its own plan, recursively. It is
	// decided at SUBMIT for the reason Routes is — which pools a child's
	// tasks resolve to, whether the namespace allows them, which image policy
	// holds, and what the child's run id is are store, cluster and process
	// facts workflow code may not read (AGENTS.md §2.1) — and it is the only
	// place a child's definition reaches workflow code. omitempty, so a plan
	// with none encodes exactly as it did before the field existed and the
	// golden history fixture keeps matching.
	Children map[string]*ChildRunPlan `json:"children,omitempty"`

	// Cache is this run's caching plan, or nil when its definition declares
	// no cached task. Decided at SUBMIT for the reason Limits is: the cap is
	// process configuration workflow code may not read (AGENTS.md §2.1), and
	// no_cache is a request field. omitempty, so a plan with none encodes
	// exactly as before and the golden history fixture keeps matching.
	Cache *CachePlan `json:"cache,omitempty"`
}

// CachePlan is the plan-side caching decision (docs/DSL.md "Caching"). Plain
// Go, like RunPlan.
type CachePlan struct {
	// MaxTTLSeconds is ORK_CACHE_MAX_TTL at submit, in whole seconds, >= 1.
	MaxTTLSeconds int64 `json:"max_ttl_seconds"`
	// Refresh is SubmitRunRequest.no_cache.
	Refresh bool `json:"refresh,omitempty"`
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
	RequireDigest     bool     `json:"require_digest,omitempty"`
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

// Default contract limits (design §2.2). They are the values a deployment
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
