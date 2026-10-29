package orker

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// ContainerID is a runtime-assigned container handle. It is opaque: nothing
// outside the runtime implementation may parse it.
type ContainerID string

// Signal is the subset of Unix signals the dispatcher sends. Ordinary signal
// handling is the whole cancellation API a workload needs (AGENTS.md §3.3), so
// there are exactly two.
type Signal string

const (
	// SIGTERM asks a workload to clean up and exit.
	SIGTERM Signal = "TERM"
	// SIGKILL ends it after the grace period.
	SIGKILL Signal = "KILL"
)

// ExitStatus is what a container's own process reported.
//
// It carries no interpretation. Every status Wait returns is the WORKLOAD's,
// and is ActivityFailed when non-zero, whatever the number: 125, 126 and 127
// carry no runtime guarantee (§8.2, measured in §14.13). "The runtime refused"
// is distinguished by Create or Start returning an error, never by a number.
type ExitStatus struct {
	// Code is the process exit status.
	Code int32
	// OOMKilled reports that the kernel killed the container for exceeding its
	// memory limit. It is reported so an operator can see it; it does not
	// change the category, because the workload still failed.
	OOMKilled bool
}

// PullInfo is what the runtime learned about an image it made available.
type PullInfo struct {
	// PlatformDigest is the manifest digest actually selected for this host —
	// the platform manifest inside a multi-platform index. It is recorded as
	// audit data and is NEVER compared for commit identity, because it varies
	// legitimately across a mixed-architecture pool (§5.4).
	PlatformDigest string
	// Platform is "os/arch[/variant]".
	Platform string
	// Cached reports that the image was already present and no bytes moved.
	Cached bool
	// Delivery is "registry" when the image was pulled and "embedded" when it
	// was imported from a local OCI image layout (§4.6). An operator debugging
	// a stale builtin needs to know the pull did not happen.
	Delivery string
}

// Delivery values recorded in ImageRef.delivery.
const (
	DeliveryRegistry = "registry"
	DeliveryEmbedded = "embedded"
)

// Mount is one bind the runtime attaches to a container.
//
// A Mount's Source is ALWAYS a path the Orker created for this attempt.
// Arbitrary host bind mounts are forbidden without exception (§9.1) and there
// is deliberately no configuration key that could express one — a guarantee by
// construction is stronger than a validation rule.
type Mount struct {
	Source      string
	Destination string
	ReadOnly    bool
}

// ContainerSpec is the complete description of one container to create.
//
// Everything §9.2 enforces on every container is either a field here with no
// "off" value — ReadOnlyRootfs, DropAllCapabilities, NoNewPrivileges, Init —
// or is supplied by the security profile when the spec is built. Nothing here
// can express anything from §9.1: there is no privileged flag, no capability
// list to add to, no host-namespace selector, and no device list.
type ContainerSpec struct {
	// Image is the pinned reference — "{registry}/{repository}@sha256:…". A
	// tag never reaches the runtime.
	Image string
	// Command overrides the image ENTRYPOINT/CMD. Empty means the image
	// decides. It is argv, never a shell string.
	Command []string
	// Env is workload-supplied environment only. The platform puts nothing
	// here: required platform state travels as files (AGENTS.md §3).
	Env map[string]string
	// WorkDir is the container's working directory, always /orkstack.
	WorkDir string
	// Mounts are the Orker-created binds of §9.2.
	Mounts []Mount
	// TmpfsPath and TmpfsBytes describe the small tmpfs at /tmp, separate from
	// the workload's /orkstack/tmp scratch bind.
	TmpfsPath  string
	TmpfsBytes int64
	// Network is "none" or "default", always explicit, never implicit.
	Network string
	// RunAsOrker is the §3.1 readability escape hatch: run the workload as
	// the Orker's own user, so the Orker can read every file it writes
	// whatever its umask. It is an intent, not a uid: the runtime renders it
	// in its own spelling, and the boot probe checks that the rendering lands
	// as the Orker, because under a user namespace the Orker's uid inside the
	// container is another user on the host. False is the default: forcing a
	// user breaks images that need their baked-in identity, which would
	// violate "any OCI image can become an activity".
	RunAsOrker bool
	// CPUs and Memory are the effective resources after Orker policy. Both are
	// ALWAYS set: no workload ever receives unlimited resources, including one
	// that requested nothing (§9.3).
	CPUs   string
	Memory string
	// PidsLimit bounds process creation. Always set.
	PidsLimit int64
	// Labels carry ork.run, ork.task, ork.attempt and ork.orker. The last is
	// the startup reaper's key: it is how an Orker recognizes its own previous
	// life's containers (§5.5).
	Labels map[string]string
	// ReadOnlyRootfs, DropAllCapabilities, NoNewPrivileges, SeccompProfile and
	// Init are the §9.2 constants. They are fields rather than implicit so a
	// reader of a spec sees the whole security posture, and the runtime
	// refuses a spec that weakens any of them. A read-only root filesystem
	// includes /run and /var/tmp, which some runtimes leave writable: the
	// writable paths are the spec's binds and its TmpfsPath, and nothing else.
	ReadOnlyRootfs      bool
	DropAllCapabilities bool
	NoNewPrivileges     bool
	SeccompProfile      string
	Init                bool
	// StopTimeout is the daemon-side grace period between SIGTERM and SIGKILL
	// for a container the Orker is no longer watching. It is the
	// runtime-owned lifetime bound of §5.5 measure 3: a reaper cannot reach
	// the containers of an Orker that never comes back.
	StopTimeoutSeconds int
}

// readOnlyRootfsPaths are the paths of a read-only root filesystem that some
// runtimes leave writable on their own — Podman's --read-only keeps /run and
// /var/tmp as writable tmpfs, bounded only by the memory limit — so the boot
// probe checks each by name rather than trusting "/" to stand for them.
var readOnlyRootfsPaths = []string{"/run", "/var/tmp"}

// ContainerRuntime is the portability boundary between OrkStack and an OCI
// runtime. It is a boundary, not a framework: exactly one implementation
// ships, and the interface exists so Podman, containerd, Kubernetes Jobs, ECS
// or Nomad remain POSSIBLE, not because any of them is planned (AGENTS.md §5).
//
// The interface split is also the failure classification. Create and Start
// return error; Wait returns (ExitStatus, error). A container that never
// started never reaches Wait. So "the runtime refused" (RUNTIME_REFUSED) and
// "the workload failed" (CONTAINER_EXIT_NONZERO) are distinguished by WHICH
// CALL failed rather than by inspecting a number — which matters because exit
// codes 125, 126 and 127 carry no runtime guarantee (§6.1, §8.2, §14.13).
//
// No Docker-specific detail may appear outside runtime_docker.go.
type ContainerRuntime interface {
	// Pull makes the image at the pinned reference available locally and
	// reports what it resolved. pinned MUST be digest-addressed; a runtime
	// that is handed a tag returns an error rather than pulling it, because a
	// running execution must never silently switch images (AGENTS.md §5).
	Pull(ctx context.Context, pinned string) (PullInfo, error)
	// LoadLayout imports the image addressed by digest from a local OCI image
	// layout — the embedded fallback of §4.6 — and returns the PullInfo a Pull
	// would have. A digest the layout does not hold returns an error matching
	// ErrNotInLayout.
	LoadLayout(ctx context.Context, layoutPath, digest string) (PullInfo, error)
	// Create builds a container from spec without starting it.
	Create(ctx context.Context, spec ContainerSpec) (ContainerID, error)
	// Start runs the container and streams its stdout and stderr to the two
	// writers, independently and byte-exact. The streams are never merged on
	// disk (§3.6). Start returns once the streams are attached, not once the
	// container exits.
	Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error
	// Signal delivers sig to the container's PID 1.
	Signal(ctx context.Context, id ContainerID, sig Signal) error
	// Wait blocks until the container exits and reports its status.
	Wait(ctx context.Context, id ContainerID) (ExitStatus, error)
	// Remove force-removes the container. It is called on a DETACHED context,
	// because cleanup must survive the cancellation that caused it.
	Remove(ctx context.Context, id ContainerID) error
	// Reap force-removes every container this Orker labelled in a previous
	// life, and reports how many it removed. An Orker crash does not stop its
	// containers: after SIGKILL of the managing process the labelled container
	// was still running three seconds later, and the --rm control was also
	// still running, because --rm fires on container EXIT (§5.5, §14.17).
	//
	// It tries every container it finds. When some could not be removed, the
	// error is a *ReapIncompleteError carrying each failure, and the count is
	// what WAS removed; any other error means the containers could not even be
	// listed.
	Reap(ctx context.Context, orkerID string) (int, error)
	// Describe reports the runtime's identity for the DescribeOrker pool
	// liveness probe (§6.5).
	Describe(ctx context.Context) (RuntimeInfo, error)
}

// RuntimeInfo is a runtime's self-description, for DescribeOrker.
type RuntimeInfo struct {
	// Name is the engine that runs the containers ("docker", "podman"), not
	// the CLI that asks it to.
	Name    string
	Version string
	// CLI is the command-line grammar the runtime speaks to the engine.
	CLI      string
	Rootless bool
	// Seccomp is how the runtime spells the default seccomp profile:
	// SeccompBuiltin by name, or SeccompEngineDefault where the engine has
	// no name for it and is given none.
	Seccomp string
}

// ErrNotInLayout reports that an OCI image layout does not hold the requested
// digest. LoadLayout returns an error matching it, so the caller can fall back
// or fail with IMAGE_PULL_FAILED rather than guessing.
var ErrNotInLayout = errors.New("digest not present in OCI image layout")

// ErrNotBindable reports that the container daemon cannot bind-mount a host
// path. On macOS with a VM-hosted daemon $HOME is shared and /var/folders is
// not, so this is a real, common, and completely undiagnosable-by-accident
// host misconfiguration (§3.1). It is surfaced as WORKDIR_NOT_BINDABLE at
// boot rather than as a mysterious failure of every task later.
var ErrNotBindable = errors.New("path is not bind-mountable by the container runtime")

// ErrProbeImageUnavailable reports that the boot-time bind probe could not
// obtain the image it runs. It is distinct from ErrNotBindable: a registry
// that is slow, unreachable or rate-limited says nothing about whether the
// work root can be bound.
var ErrProbeImageUnavailable = errors.New("the bind probe image is unavailable")

// ErrPostureRefused reports that the container runtime did not give the boot
// probe's container the posture every task's container is owed (§9.2): it
// refused to run that container, which is built exactly as a task's is, or it
// ran it and what the container saw from inside was weaker. Either way every
// task would be refused, or would run weaker than the contract says, so the
// Orker does not start.
var ErrPostureRefused = errors.New("the container runtime does not give a task's container its posture")

// ErrRunAsOrkerUnavailable reports that the §3.1 escape hatch does not land as
// the Orker on this host: a file the boot probe's container wrote as the
// Orker's user is not one the Orker can read. A task that asks for the hatch
// fails with it rather than running as a user that would not help.
var ErrRunAsOrkerUnavailable = errors.New("running a container as the Orker's user does not land as the Orker on this host")

// ErrProbeTimedOut reports that the container daemon was too slow for the
// boot-time bind probe: it did not answer the probe's image inspect within the
// probe's budget, or it did not start the probe's container within it on every
// attempt it was given, whether it said nothing or said its own deadline had
// passed. It is distinct from ErrNotBindable: a daemon that is slow under load
// says nothing about whether the work root can be bound, which only the
// daemon's refusal of the bind, or a probe container that exited non-zero
// within its budget, can say.
var ErrProbeTimedOut = errors.New("the bind probe timed out")

// ReapIncompleteError is Reap's report that it removed every orphan it could
// and some it could not. Boot logs each failure and proceeds: one container
// stuck in removal must not stop an Orker from starting.
type ReapIncompleteError struct {
	Failures []error
}

func (e *ReapIncompleteError) Error() string {
	return fmt.Sprintf("%d orphaned container(s) could not be removed: %v",
		len(e.Failures), errors.Join(e.Failures...))
}

// DefaultImageLayoutRoot is where the embedded OCI image layouts live when
// ORK_ORKER_IMAGE_LAYOUT says nothing (§4.6).
//
// An absent directory is not an error: the fallback is for an air-gapped or
// rate-limited Orker, and the overwhelmingly common deployment pulls from a
// registry and never reads this path at all.
const DefaultImageLayoutRoot = "/var/lib/orkstack/images"
