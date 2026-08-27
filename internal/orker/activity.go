package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	// Logger records Orker lifecycle decisions such as durable commit adoption.
	// Nil uses slog.Default.
	Logger *slog.Logger
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
	// identityLock is held for the process's lifetime once Boot claims this
	// Orker's identity, so no second live Orker can reap by the same label.
	identityLock *os.File
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
		orkerID = defaultOrkerID(workRoot)
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
			Logger:       opts.Logger,
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
// Refusing to start is the only honest response to any of these failures. An
// Orker that starts anyway would fail every task later with an error naming
// the container rather than the cause, or — worse, for the conditional-write
// probe — would run a deployment that silently lost the immutability
// guarantee (§3.1, §5.3).
func (a *Activities) Boot(ctx context.Context) error {
	// An operator typo in a resource cap must stop the process here. Left to
	// dispatch time an unreadable cap used to REMOVE the ceiling rather than
	// refuse, so a workload could be granted any size it asked for (§9.3,
	// AGENTS.md §6).
	if err := a.dispatcher.Policy.Validate(); err != nil {
		return fmt.Errorf("%s: %w", ReasonResourceRefused, err)
	}
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
	// Claim the identity before reaping by it.
	//
	// The reaper force-removes every container labelled with this Orker's id,
	// on the premise that they belong to its own dead predecessor. That
	// premise only holds while no OTHER live process answers to the same id —
	// and a stable identity makes sharing one possible, where the old
	// pid-derived identity made it impossible by making reaping useless. An
	// exclusive lock decides the question instead of assuming it: if the
	// lock is free, any previous holder is gone and its containers really are
	// orphans; if it is held, a peer is alive and this Orker refuses rather
	// than destroying that peer's running work.
	if err := a.claimIdentity(); err != nil {
		return err
	}
	// Reap this Orker's own previous life's containers. An Orker crash does
	// not stop its containers (§14.17), so a restarted Orker inherits them.
	if _, err := a.dispatcher.Runtime.Reap(ctx, a.dispatcher.OrkerID); err != nil {
		return fmt.Errorf("reap orphaned containers: %w", err)
	}
	return nil
}

// claimIdentity takes the process-lifetime lock that makes reaping by a stable
// identity safe. The lock lives in a host directory independent of the
// workspace: one identity must have one holder even when two processes use
// different work roots. Otherwise two Orkers explicitly given the same
// ORK_ORKER_ID could each acquire a different lock and force-remove the other's
// running containers at boot.
func (a *Activities) claimIdentity() error {
	dir := filepath.Join(os.TempDir(), "orkstack-orker-identity-locks")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%s: create the orker identity lock directory %s: %w", ReasonWorkdirFailed, dir, err)
	}
	sum := sha256.Sum256([]byte(a.dispatcher.OrkerID))
	path := filepath.Join(dir, hex.EncodeToString(sum[:])+".lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("%s: open the orker identity lock %s: %w", ReasonWorkdirFailed, path, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return fmt.Errorf(
			"another live orker already holds the identity %q; a second orker sharing one identity "+
				"would force-remove the first one's RUNNING containers at boot (§5.5). "+
				"Give this orker its own identity with %s",
			a.dispatcher.OrkerID, OrkerIDVar)
	}
	a.identityLock = file
	return nil
}

// Close releases the identity claim. A process that exits releases it anyway —
// that is the point of a lock rather than a pid file — so this exists for tests
// and for an embedded Orker that outlives one Activities.
func (a *Activities) Close() {
	if a.identityLock != nil {
		_ = syscall.Flock(int(a.identityLock.Fd()), syscall.LOCK_UN)
		_ = a.identityLock.Close()
		a.identityLock = nil
	}
}

// defaultOrkerID derives an identity that is STABLE across a restart on the
// same host, because the startup reaper's only key is this string.
//
// It used to end in os.Getpid(), which made every restart a different Orker:
// the replacement filtered `label=ork.orker=<new id>`, matched none of its dead
// predecessor's containers, and reaped nothing — exactly the scenario §5.5
// measure 2 exists for. The pid is gone.
//
// The work root replaces it as the discriminator. It is what makes two Orkers
// deliberately run side by side on one host distinct — they cannot share
// per-attempt execution directories anyway (§6.4) — while a restart of the SAME
// Orker keeps its configured root and therefore its identity. Two Orkers that
// do share a root are caught by the boot lock rather than silently reaping each
// other.
func defaultOrkerID(workRoot string) string {
	if explicit := strings.TrimSpace(os.Getenv(OrkerIDVar)); explicit != "" {
		return explicit
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "orker"
	}
	root := strings.TrimSpace(workRoot)
	if root == "" {
		root = os.TempDir()
	}
	abs, err := filepath.Abs(root)
	if err == nil {
		root = abs
		// A restart may spell one existing directory through a symlink. Resolve
		// that alias before hashing so it retains the predecessor's reaper key.
		// EvalSymlinks legitimately fails before Boot creates a new root, so the
		// absolute path remains the deterministic fallback.
		if canonical, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
			root = canonical
		}
	}
	sum := sha256.Sum256([]byte(root))
	return fmt.Sprintf("%s-%s", safeNameComponent(host), hex.EncodeToString(sum[:4]))
}
