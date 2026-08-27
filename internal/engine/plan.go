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
// (AGENTS.md §1.3). So the decision is made once, by the process that accepts
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
