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
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
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
	// Attempts is where the immutable per-attempt record is written at the end
	// of every attempt. Nil leaves an Orker that executes tasks and records no
	// retry history: like Logs, it is observability, and losing it must never
	// lose the work.
	Attempts artifacts.AttemptStore
	// Commits is the durable completion store. Nil disables the commit
	// protocol, which is a TEST-ONLY configuration: a deployment without it
	// cannot honor "a durably committed activity result cannot become pending
	// again" (AGENTS.md §15).
	Commits artifacts.CommitStore
	// Refs predicts upload URIs for the step-4 commit bound.
	Refs artifacts.RefPredictor
	// Memos is where cached tasks' memos live. Nil means this Orker reuses
	// nothing and writes no memo: every cached attempt is a MISS
	// (MEMO_STORE_ABSENT) and runs, exactly like Commits being nil disables
	// adoption.
	Memos artifacts.MemoStore
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
	// MaxPins is reported by DescribeOrker. Zero — the default — means this
	// Orker hosts no pins, and a submit that pins a task to its pool is refused
	// at submit rather than left waiting for a slot that does not exist
	// (docs/DEPLOY.md "Pin hosting is a pool-wide setting").
	MaxPins int32
	// LayoutRoot is where embedded OCI image layouts live (§4.6).
	LayoutRoot string
	// Signatures verifies a task's cosign signature when its image policy
	// names a signer (docs/DEPLOY.md "Image supply-chain policy"). cmd/orker always
	// builds one; nil only in tests, where such a task fails
	// IMAGE_SIGNATURE_UNAVAILABLE rather than passing.
	Signatures *dsl.SignatureVerifier
	// HeartbeatInterval is how often an attempt's heartbeat loop ticks, and
	// therefore how long a running container waits to observe a cancellation.
	// Zero means DefaultHeartbeatInterval. The caller MUST cap its task
	// worker's MaxHeartbeatThrottleInterval to the same value — heartbeat.go's
	// pairing rule.
	HeartbeatInterval time.Duration
	// TaskLogs exports every framed log line of every attempt, beside the
	// durable chunk store. Nil exports nothing.
	TaskLogs TaskLogExporter
	// Phases observes one attempt's progress through the §7.1 sequence — the
	// seam a tracer or a metrics recorder plugs into. Nil observes nothing.
	Phases PhaseObserver
	// TraceContext extracts the W3C trace carrier for the optional `trace`
	// key in task.json (§3.2). Nil means task.json never carries one.
	TraceContext func(ctx context.Context) map[string]string
	// Logger records Orker lifecycle decisions such as durable commit adoption.
	// Nil uses slog.Default.
	Logger *slog.Logger
}

// Activities implements the engine<->Orker activity seam.
//
// It wraps one OCIDispatcher: any OCI image is an activity, and there is
// exactly one implementation that runs one (§6.1).
type Activities struct {
	dispatcher *OCIDispatcher
	pool       string
	maxConc    int32
	maxPins    int32
	// identityLock is held for the process's lifetime once Boot claims this
	// Orker's identity, so no second live Orker can reap by the same label.
	identityLock *os.File
	// cordon is this process's cordon flag (cordon.go): the only place it
	// exists.
	cordon *Cordon
	// running counts the ExecuteTask activities in flight, measured by the
	// cordon interceptor at the SDK seam (running_tasks).
	running atomic.Int32
	// pins is the set of sessions this Orker accepted and has not completed
	// (open_pins), maintained by the same interceptor.
	pins *openPins
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
			Runtime:           runtime,
			Store:             store,
			Commits:           opts.Commits,
			Refs:              opts.Refs,
			Logs:              opts.Logs,
			LogOpts:           opts.Log,
			Attempts:          opts.Attempts,
			Memos:             opts.Memos,
			Policy:            policy,
			WorkRoot:          workRoot,
			OrkerID:           orkerID,
			BuildID:           buildID,
			EnvAllowlist:      allowlist,
			LayoutRoot:        opts.LayoutRoot,
			Signatures:        opts.Signatures,
			HeartbeatInterval: opts.HeartbeatInterval,
			TaskLogs:          opts.TaskLogs,
			Phases:            opts.Phases,
			TraceContext:      opts.TraceContext,
			Logger:            opts.Logger,
		},
		pool:    opts.Pool,
		maxConc: opts.MaxConcurrent,
		maxPins: opts.MaxPins,
		cordon:  NewCordon(),
		pins:    newOpenPins(),
	}
}

// Dispatcher exposes the dispatcher for boot-time work — the container reaper
// and the bind probe — and for tests.
func (a *Activities) Dispatcher() *OCIDispatcher { return a.dispatcher }

// ExecuteTask runs one activity: check the commit, build the workspace, stage
// inputs, pull the image, run the container, validate its outputs, upload
// them, and write the completion manifest.
//
// The argument is the proto message, not a Go struct. That is deliberate:
// Temporal's converter encodes a top-level protobuf message with protojson and
// everything else with encoding/json, so the recorded payload encoding is
// protojson and the base replay fixture pins it
// (internal/engine/testdata/history/README.md).
func (a *Activities) ExecuteTask(
	ctx context.Context,
	in *orkstackv1.ExecuteTaskInput,
) (*orkstackv1.ExecuteTaskResult, error) {
	return a.dispatcher.Execute(ctx, in)
}

// DescribeOrker reports this Orker's identity.
//
// It is the pool-liveness probe (§6.5): a successful call is how a pool
// proves it is alive, so it must answer even when there is nothing else to
// report.
func (a *Activities) DescribeOrker(ctx context.Context) (*orkstackv1.DescribeOrkerResult, error) {
	result := &orkstackv1.DescribeOrkerResult{
		OrkerId:           a.dispatcher.OrkerID,
		BuildId:           a.dispatcher.BuildID,
		Pool:              a.pool,
		SecurityProfile:   a.dispatcher.Policy.ProfileID(),
		MaxConcurrent:     a.maxConc,
		MaxConcurrentPins: a.maxPins,
		RunningTasks:      a.running.Load(),
		// This build implements fan-out items and their fan-in staging; an
		// Orker built before it answers false, and submit refuses a fan-out
		// on its pool (docs/DEPLOY.md §1).
		EachSupported: true,
		// The memo step (§5.4.1): this Orker reads
		// ExecuteTaskInput.cache, so submit may send a cached task to its pool.
		CacheSupported: true,
		// Join policies (docs/DSL.md "Joins and failure handlers"): this Orker reads
		// ExecuteTaskInput.join, so submit may send a failure handler to its
		// pool.
		JoinSupported: true,
	}
	// The cordon and the load: one lock read, one atomic load and — only when
	// a pin is open — one ReadDir of the work root.
	if cordoned, reason, since := a.cordon.State(); cordoned {
		result.Cordoned = true
		result.CordonReason = reason
		result.CordonedAt = timestamppb.New(since)
	}
	result.Pins = a.describePins()
	result.OpenPins = int32(len(result.Pins))
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

// ReleasePin removes one pin's shared directory (§6.4).
//
// The engine schedules it on the pin's OWN session, so it lands on the host
// that owns the directory or nowhere at all — there is no branch here that
// could remove another host's anything. It is idempotent: an absent directory
// is a success, because release genuinely races the other two ways a pin ends
// (a replacement Orker's boot reap, the lease sweep), and its failure is logged
// by the engine's coroutine rather than failing the run. Cleanup is not
// orchestration.
//
// The instance id is validated before any filesystem call, inside
// ReleasePinDirectory: a malformed one is refused rather than rewritten, and it
// is refused BEFORE anything is stat'ed, so a bad id can never be an
// accidental removal.
func (a *Activities) ReleasePin(ctx context.Context, in *orkstackv1.ReleasePinInput) error {
	removed, err := ReleasePinDirectory(
		a.dispatcher.WorkRoot, in.GetRunId(), in.GetPin().GetName(), in.GetPin().GetInstanceId())
	if err != nil {
		return err
	}
	if removed == "" {
		// The boring path. Nothing happened, so nothing is logged: an absent
		// directory is the expected outcome whenever a reap or a sweep got
		// there first, and logging it would make the common case look like an
		// event.
		return nil
	}
	a.dispatcher.logger().Info("pin released",
		"run", in.GetRunId(),
		"pin", in.GetPin().GetName(),
		"instance", in.GetPin().GetInstanceId(),
		"path", removed)
	return nil
}

// PinLeaseSweepInterval is how often SweepPinLeases ticks (§6.4).
const PinLeaseSweepInterval = time.Minute

// SweepPinLeases removes expired pin directories until ctx is done, and is the
// third and last of §6.4's three ways a pin's directory ends.
//
// It runs UNCONDITIONALLY — whether or not this Orker hosts pins. Three reasons,
// and the third is the one that makes it more than tidiness:
//
//   - It is cheap: one ReadDir of a directory the Orker already owns, once a
//     minute, and on a non-pin-hosting Orker it finds nothing every time.
//   - An operator who sets ORK_ORKER_MAX_PINS back to 0 restarts the process,
//     and the boot reap (which is also unconditional) has already removed the
//     previous life's directories — but a sweep gated on max_pins would be the
//     one piece of that story that silently stopped, for no gain.
//   - AcquirePin is driven by ExecuteTaskInput.pin, not by this Orker's own
//     setting. A pinned input that reaches a max_pins=0 Orker's ORDINARY queue
//     — which the engine will not send, but which nothing in the Orker refuses
//     — still creates a directory, and a gated sweeper would be the only thing
//     that could have collected it.
//
// The caller passes the process's own cancellable context, so the loop stops at
// shutdown rather than outliving it.
func (a *Activities) SweepPinLeases(ctx context.Context) {
	ticker := time.NewTicker(PinLeaseSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			a.sweepPinLeasesOnce(now)
		}
	}
}

// sweepPinLeasesOnce is one tick, split out so a test can drive it without a
// clock.
func (a *Activities) sweepPinLeasesOnce(now time.Time) {
	root := a.dispatcher.WorkRoot
	if root == "" {
		root = os.TempDir()
	}
	removed, err := SweepExpiredPinLeases(root, now)
	for _, path := range removed {
		a.dispatcher.logger().Info("pin lease expired", "path", path)
	}
	if err != nil {
		// Warn, never fatal: the sweep is a backstop for a directory nothing
		// will read again, and the next tick tries the same paths.
		a.dispatcher.logger().Warn("pin lease sweep", "error", err)
	}
}

// reapPinDirectories removes every pin directory found at boot (§6.4).
//
// It is UNCONDITIONAL, and deliberately so: a directory left behind by a
// PREVIOUS life that did host pins must still be cleaned up by a boot that does
// not, or turning ORK_ORKER_MAX_PINS back to 0 would strand the last run's
// shared trees on disk with nothing left that would ever look for them.
//
// A removal failure is logged and does NOT fail Boot. The directory is dead by
// construction — a fresh session-resource id is minted per worker start, so no
// pin survives a restart — and refusing to accept work over a tree nothing will
// ever read again is worse than starting; the lease sweep will try again.
func (a *Activities) reapPinDirectories(root string) {
	removed, err := ReapPinDirectories(root)
	for _, path := range removed {
		a.dispatcher.logger().Info("pin reaped", "path", path)
	}
	if err != nil {
		a.dispatcher.logger().Warn("pin boot reap", "error", err)
	}
}

// Boot performs the checks an Orker must pass before it accepts work.
//
// Refusing to start is the only honest response to any of these failures. An
// Orker that starts anyway would fail every task later with an error naming
// the container rather than the cause, or — worse, for the conditional-write
// probe — would run a deployment that silently lost the immutability
// guarantee (§3.1, §5.3).
func (a *Activities) Boot(ctx context.Context) error {
	// An operator typo in a resource cap must stop the process here rather
	// than surface per request at dispatch time: a cap that cannot be read is
	// not a ceiling, and a workload must never be granted a size nobody set
	// (§9.3, AGENTS.md §6).
	if err := a.dispatcher.Policy.Validate(); err != nil {
		return fmt.Errorf("%s: %w", ReasonResourceRefused, err)
	}
	// An id is what names this Orker's control channel (task.QueueForOrker),
	// so an id that cannot name one refuses boot here, naming the variable,
	// rather than leaving an Orker nobody can cordon.
	if err := task.ValidOrkerID(a.dispatcher.OrkerID); err != nil {
		return fmt.Errorf("ORKER_ID_INVALID: %s=%q must be 1–200 bytes with no whitespace "+
			"or control characters: %w", OrkerIDVar, a.dispatcher.OrkerID, err)
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
	// And its own previous life's pin directories, beside the containers, for
	// the same reason and with a stronger guarantee: a pin cannot outlive the
	// worker process that opened its session, so every ork-pin-* under the work
	// root is dead by construction rather than by heuristic (§6.4).
	a.reapPinDirectories(root)
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
// It deliberately carries no os.Getpid(): an identity ending in the pid would
// make every restart a different Orker — the replacement would filter
// `label=ork.orker=<new id>`, match none of its dead predecessor's containers,
// and reap nothing — exactly the scenario §5.5 measure 2 exists for.
//
// The work root is the discriminator instead. It is what makes two Orkers
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
