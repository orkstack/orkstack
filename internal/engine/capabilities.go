package engine

import (
	"context"
	"sort"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

// Engine capabilities (docs/DEPLOY.md §5, "Engine capabilities and the fleet
// check"): what this build of `ork` can run that an older build could not,
// advertised where the cluster already lists every poller — the Temporal
// identity each process polls with — so a submit that needs a capability can
// ask the cluster's own poller list whether every process that could decide
// the run has it. Nothing is registered and nothing is stored: the list is
// read on the submit that needs it, never cached.
//
// The identity is an operator surface. It is `pid@host@` plus the suffix, and
// AGENTS.md §7.1 keeps host names off user surfaces, so a refusal names how
// many processes lack a capability, never which; the identities go to the
// engine's own log.

// CapabilityEachItems10k is what a run whose fan-outs declare more than
// LegacyEachMaxItems items needs: the per-item input floor (eachBounds), which
// an older build does not apply, so it would refuse an item this build
// schedules and the two would replay the run differently.
const CapabilityEachItems10k = "each-items-10k"

// LegacyEachMaxItems is the most items one Orkflow's fan-outs could declare
// before CapabilityEachItems10k: every build, of every release, runs that
// many. A run tree with a run declaring more needs the capability.
const LegacyEachMaxItems int32 = 1000

// CapabilityDefinitionRef is what a run whose planned definition travels by
// reference needs (RunPlan.Definition): a build that reads the definition
// object into its history before deciding anything. An older build ignores
// the plan field, finds a start definition with no tasks and ends the run
// INVALID_ORKFLOW, so a run it decides first can never be carried on.
const CapabilityDefinitionRef = "definition-ref"

// EngineCapabilities is what this build can run, in the order it advertises
// them. A later stage appends its own token; a token is never removed while a
// release that relies on it may still poll.
var EngineCapabilities = []string{CapabilityEachItems10k, CapabilityDefinitionRef}

// capabilitySuffix separates a process's SDK identity from what it advertises.
const capabilitySuffix = " ork-caps="

// EngineIdentity is the Temporal client identity an `ork` process polls with:
// base, the SDK's default identity (`pid@host@`), followed by
// " ork-caps=<comma-separated EngineCapabilities>". The run worker and the
// projection worker share the client, so both advertise it, and each process
// keeps one distinct identity, so the releases page's per-process count is
// unchanged.
func EngineIdentity(base string) string {
	return base + capabilitySuffix + strings.Join(EngineCapabilities, ",")
}

// IdentityCapabilities parses what identity advertises: the tokens after its
// last " ork-caps=", or none when it advertises nothing (an older release, an
// Orker, a test worker).
func IdentityCapabilities(identity string) []string {
	at := strings.LastIndex(identity, capabilitySuffix)
	if at < 0 {
		return nil
	}
	var out []string
	for _, token := range strings.Split(identity[at+len(capabilitySuffix):], ",") {
		if token = strings.TrimSpace(token); token != "" {
			out = append(out, token)
		}
	}
	return out
}

// identityHas reports whether identity advertises capability.
func identityHas(identity, capability string) bool {
	for _, token := range IdentityCapabilities(identity) {
		if token == capability {
			return true
		}
	}
	return false
}

// CapabilityScope is which pollers can decide a run that needs a capability.
type CapabilityScope int

const (
	// ScopeWorkflow is the engine's workflow channel: the builds a run started
	// now can land on (Stages 4 and 5: their risk is a workflow task decided by
	// an older build).
	ScopeWorkflow CapabilityScope = iota
	// ScopeWorkflowAndEvents also reads every poller of the projection
	// channel, whatever its build (Stage 6: the projection worker is
	// unversioned, so any process polling it may take this run's publish).
	ScopeWorkflowAndEvents
)

// CapabilityChecker answers the fleet check (internal/api's submit): which
// processes that could decide a run started now lack capability.
type CapabilityChecker interface {
	MissingCapability(ctx context.Context, capability string, scope CapabilityScope) ([]string, error)
}

var _ CapabilityChecker = (*TemporalReleaseCluster)(nil)

// MissingCapability returns the identity of every fresh poller that could
// decide a run started now and does not advertise capability; a checked
// poller with no identity is returned as "" and counts as lacking it. A
// failed describe is an error: the caller fails closed.
//
// Which pollers decide a run started now: those of the engine's workflow
// channel (the read engineReplicas makes, with its filters — fresh pollers
// only, another deployment's skipped). When the channel's routing names a
// current or a ramping version, only pollers of the current build (the
// unversioned ones when no current version is set) and of the ramping build
// are checked: a new run starts only there, and RunWorkflow is Pinned
// (AGENTS.md §7.2), so an older build still polling to drain its pinned runs
// never decides it. With no current or ramping version, every fresh poller is
// checked, because any of them may decide any workflow task.
//
// ScopeWorkflowAndEvents also describes the projection channel, as an
// ACTIVITY channel — it registers only PublishEvent, so a workflow-type
// describe of it lists no pollers and would pass vacuously — and checks every
// fresh poller there, whatever its build or deployment.
func (c *TemporalReleaseCluster) MissingCapability(ctx context.Context, capability string, scope CapabilityScope) ([]string, error) {
	described, err := c.describeChannel(ctx, c.engineQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, false)
	if err != nil {
		return nil, c.fail(ReleaseOpRead, "describe the engine's channel for the fleet check", "", err)
	}
	now, within := c.now(), c.freshness()
	builds, versioned := c.newRunBuilds(described.GetVersioningInfo())
	missing := map[string]bool{}
	var anonymous int
	note := func(poller *taskqueuepb.PollerInfo) {
		identity := poller.GetIdentity()
		if identity == "" {
			anonymous++
			return
		}
		if !identityHas(identity, capability) {
			missing[identity] = true
		}
	}
	for _, poller := range described.GetPollers() {
		if !freshPoller(poller, now, within) {
			continue
		}
		build := ""
		if opts := poller.GetDeploymentOptions(); opts != nil && opts.GetBuildId() != "" &&
			opts.GetWorkerVersioningMode() != enumspb.WORKER_VERSIONING_MODE_UNVERSIONED {
			if opts.GetDeploymentName() != c.deployment {
				continue // another deployment's engine shares this namespace
			}
			build = opts.GetBuildId()
		}
		if versioned && !builds[build] {
			continue // a draining build: it never decides a run started now
		}
		note(poller)
	}
	if scope == ScopeWorkflowAndEvents {
		events, err := c.describeChannel(ctx, EventQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, false)
		if err != nil {
			return nil, c.fail(ReleaseOpRead, "describe the projection channel for the fleet check", "", err)
		}
		for _, poller := range events.GetPollers() {
			if freshPoller(poller, now, within) {
				note(poller)
			}
		}
	}
	out := make([]string, 0, len(missing)+anonymous)
	for identity := range missing {
		out = append(out, identity)
	}
	sort.Strings(out)
	for i := 0; i < anonymous; i++ {
		out = append(out, "")
	}
	return out, nil
}

// newRunBuilds is the set of builds a run started now lands on, and whether
// the channel's routing names one at all. A current version names its build;
// none names the unversioned pollers (""); a ramping version adds its build.
func (c *TemporalReleaseCluster) newRunBuilds(info *taskqueuepb.TaskQueueVersioningInfo) (map[string]bool, bool) {
	current := info.GetCurrentDeploymentVersion().GetBuildId()
	ramping := info.GetRampingDeploymentVersion().GetBuildId()
	if current == "" && ramping == "" {
		return nil, false
	}
	builds := map[string]bool{current: true}
	if ramping != "" {
		builds[ramping] = true
	}
	return builds, true
}
