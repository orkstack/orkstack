package orker

import (
	"context"
	"fmt"
	"os"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// WorkRootVar overrides the parent directory for per-attempt workspaces.
//
// It MUST name a path the container daemon can bind-mount. On macOS with a
// VM-hosted daemon $HOME is shared and /var/folders is not, which is why
// `make up` points this at .data/work — and why an Orker probes the root at
// boot rather than failing every task later (§3.1).
const WorkRootVar = "ORK_ORKER_WORKDIR"

// OrkerIDVar overrides this Orker's identity. It labels every container the
// Orker creates and is the startup reaper's key, so two Orkers sharing an id
// would reap each other's live work (§5.5).
const OrkerIDVar = "ORK_ORKER_ID"

// BuildIDVar is the Worker Deployment build id recorded on every commit.
const BuildIDVar = "ORK_BUILD_ID"

// Options configures an Activities instance. The zero value reads the
// environment for everything.
type Options struct {
	// WorkRoot is the parent directory for per-attempt workspaces. Empty
	// means ORK_ORKER_WORKDIR, then the OS temp directory.
	WorkRoot string
	// EnvAllowlist limits which variables the `env` template helper reads.
	// Nil means ORK_ORKER_ENV_ALLOWLIST.
	EnvAllowlist []string
	// Logs is where a task attempt's output is streamed while it runs. Nil
	// leaves an Orker that executes tasks and streams nothing: logging is
	// observability, and losing it must never lose the work.
	Logs artifacts.LogStore
	// Log bounds that stream — flush size, flush interval and the per-attempt
	// cap. The zero value is the documented default in every field.
	Log LogOptions
	// Commits is the durable completion store. Nil disables the commit
	// protocol, which is a TEST-ONLY configuration: a deployment without it
	// cannot honor "a durably committed activity result cannot become pending
	// again" (AGENTS.md §15).
	Commits artifacts.CommitStore
	// Refs predicts upload URIs for the step-4 commit bound.
	Refs artifacts.RefPredictor
	// Runtime is the OCI runtime. Nil means DockerRuntime.
	Runtime ContainerRuntime
	// Policy is the Orker-local security and resource policy. The zero value
	// means SecurityPolicyFromEnv.
	Policy *SecurityPolicy
	// OrkerID and BuildID identify this Orker. Empty reads the environment.
	OrkerID string
	BuildID string
	// Pool is the pool this Orker serves, reported by DescribeOrker.
	Pool string
	// MaxConcurrent is reported by DescribeOrker.
	MaxConcurrent int32
	// LayoutRoot is where embedded OCI image layouts live (§4.6).
	LayoutRoot string
}

// Activities implements the engine<->Orker activity seam.
//
// It is now a thin shell around one OCIDispatcher. The plugin registry, the
// name lookup and the four builtin plugins are gone: any OCI image is an
// activity, and there is exactly one implementation that runs one (§6.1).
type Activities struct {
	dispatcher *OCIDispatcher
	pool       string
	maxConc    int32
}

// NewActivities builds the activity set with environment-derived defaults.
func NewActivities(store artifacts.Store) *Activities {
	return NewActivitiesWithOptions(store, Options{})
}

// NewActivitiesWithOptions builds the activity set with explicit options.
func NewActivitiesWithOptions(store artifacts.Store, opts Options) *Activities {
	workRoot := opts.WorkRoot
	if workRoot == "" {
		workRoot = os.Getenv(WorkRootVar)
	}
	allowlist := map[string]struct{}{}
	if opts.EnvAllowlist == nil {
		allowlist = parseAllowlist(os.Getenv(EnvAllowlistVar))
	} else {
		for _, name := range opts.EnvAllowlist {
			allowlist[name] = struct{}{}
		}
	}
	runtime := opts.Runtime
	if runtime == nil {
		runtime = NewDockerRuntime()
	}
	policy := SecurityPolicyFromEnv()
	if opts.Policy != nil {
		policy = *opts.Policy
	}
	orkerID := opts.OrkerID
	if orkerID == "" {
		orkerID = defaultOrkerID()
	}
	buildID := opts.BuildID
	if buildID == "" {
		buildID = os.Getenv(BuildIDVar)
	}

	return &Activities{
		dispatcher: &OCIDispatcher{
			Runtime:      runtime,
			Store:        store,
			Commits:      opts.Commits,
			Refs:         opts.Refs,
			Logs:         opts.Logs,
			LogOpts:      opts.Log,
			Policy:       policy,
			WorkRoot:     workRoot,
			OrkerID:      orkerID,
			BuildID:      buildID,
			EnvAllowlist: allowlist,
			LayoutRoot:   opts.LayoutRoot,
		},
		pool:    opts.Pool,
		maxConc: opts.MaxConcurrent,
	}
}

// Dispatcher exposes the dispatcher for boot-time work — the container reaper
// and the bind probe — and for tests.
func (a *Activities) Dispatcher() *OCIDispatcher { return a.dispatcher }

// ExecuteTask runs one activity: check the commit, build the workspace, stage
// inputs, pull the image, run the container, validate its outputs, upload
// them, and write the completion manifest.
//
// The argument is the proto message, not a Go struct. That is deliberate and
// it is the reason §11.4's one-time replay-fixture reset exists: Temporal's
// converter encodes a top-level protobuf message with protojson and everything
// else with encoding/json, so making ExecuteTaskInput a proto message flips the
// recorded payload encoding (§2.5).
func (a *Activities) ExecuteTask(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
) (*orkstackv1.ExecuteTaskResult, error) {
	return a.dispatcher.Execute(ctx, in)
}

// DescribeOrker reports this Orker's identity.
//
// It replaces ListPlugins one-for-one as the pool-liveness probe (§6.5).
// ListPlugins was never only a plugin list: a successful probe is how a pool
// proves it is alive, and proof/M11.6 fixed a bug in exactly that path. An
// Orker no longer has a plugin list, but a pool still has to prove it is
// alive, so the probe stays and only its payload changes.
func (a *Activities) DescribeOrker(ctx context.Context) (*orkstackv1.DescribeOrkerResult, error) {
	result := &orkstackv1.DescribeOrkerResult{
		OrkerId:         a.dispatcher.OrkerID,
		BuildId:         a.dispatcher.BuildID,
		Pool:            a.pool,
		SecurityProfile: a.dispatcher.Policy.ProfileID(),
		MaxConcurrent:   a.maxConc,
	}
	if a.dispatcher.Runtime != nil {
		info, err := a.dispatcher.Runtime.Describe(ctx)
		if err != nil {
			// The probe's job is to prove the Orker is ALIVE. An Orker whose
			// runtime cannot be described is still polling and still able to
			// answer, so the identity is returned with the runtime fields it
			// could not fill rather than failing the pool.
			result.RuntimeName = info.Name
			return result, nil
		}
		result.RuntimeName = info.Name
		result.RuntimeVersion = info.Version
		result.Rootless = info.Rootless
	}
	return result, nil
}

// Boot performs the checks an Orker must pass before it accepts work.
//
// Refusing to start is the only honest response to either failure. An Orker
// that starts anyway would fail every task later with an error naming the
// container rather than the cause, or — worse, for the conditional-write
// probe — would run a deployment that silently lost the immutability
// guarantee (§3.1, §5.3).
func (a *Activities) Boot(ctx context.Context) error {
	root := a.dispatcher.WorkRoot
	if root == "" {
		root = os.TempDir()
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("%s: create the workspace root %s: %w", ReasonWorkdirFailed, root, err)
	}
	if prober, ok := a.dispatcher.Runtime.(interface {
		ProbeBindable(context.Context, string) error
	}); ok {
		if err := prober.ProbeBindable(ctx, root); err != nil {
			return fmt.Errorf("%s: %w", ReasonWorkdirNotBindable, err)
		}
	}
	// Reap this Orker's own previous life's containers. An Orker crash does
	// not stop its containers (§14.17), so a restarted Orker inherits them.
	if _, err := a.dispatcher.Runtime.Reap(ctx, a.dispatcher.OrkerID); err != nil {
		return fmt.Errorf("reap orphaned containers: %w", err)
	}
	return nil
}

// defaultOrkerID derives a stable-per-process identity.
func defaultOrkerID() string {
	if explicit := strings.TrimSpace(os.Getenv(OrkerIDVar)); explicit != "" {
		return explicit
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "orker"
	}
	return fmt.Sprintf("%s-%d", safeNameComponent(host), os.Getpid())
}
