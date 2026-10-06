package orker

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The container-side layout. It is fixed, and it is also written into
// task.json's `paths` so a workload that reads it never hardcodes a path and a
// future layout change stays a data change (§3.1, §3.2).
const (
	ContainerRoot           = "/orkstack"
	ContainerInput          = ContainerRoot + "/input"
	ContainerTaskJSON       = ContainerInput + "/task.json"
	ContainerConfigJSON     = ContainerInput + "/config.json"
	ContainerRunJSON        = ContainerInput + "/run.json"
	ContainerSecrets        = ContainerInput + "/secrets"
	ContainerInputArtifacts = ContainerInput + "/artifacts"
	ContainerUpstream       = ContainerInput + "/upstream"
	ContainerOutput         = ContainerRoot + "/output"
	ContainerValuesJSON     = ContainerOutput + "/values.json"
	ContainerArtifacts      = ContainerOutput + "/artifacts"
	// ContainerShared is the pin's shared directory, and it is present ONLY for
	// a pinned task (§3.1). It is the one mount whose lifetime is not the
	// attempt's: every task of one pin, and every attempt of every such task,
	// sees this path with the same contents for the life of the pin. For an
	// unpinned task the mount does not exist and task.json omits paths.shared.
	ContainerShared = ContainerRoot + "/shared"
	ContainerTmp    = ContainerRoot + "/tmp"
	// ContainerScratchTmp is the small tmpfs at /tmp, separate from the
	// workload's /orkstack/tmp scratch bind. The container root filesystem is
	// read-only (§9.2), so a workload that writes to /tmp out of habit needs
	// somewhere for those bytes to go.
	ContainerScratchTmp = "/tmp"
)

// Workspace file and directory modes (§3.1).
//
// The rationale is one sentence with two halves. The host per-attempt
// directory is 0700 and owned by the Orker's user, so no other host user can
// traverse into a 1777 child; and inside the container the workload may run as
// ANY uid the image chooses — the Orker does not force --user by default — so
// every directory it must write is world-writable with the sticky bit and
// every file it must read is 0444.
const (
	// ModeAttempt is the per-attempt root. 0700 is what makes the 1777
	// children safe.
	ModeAttempt os.FileMode = 0o700
	// ModeInputDir is a read-only input directory.
	ModeInputDir os.FileMode = 0o555
	// ModeInputFile is a read-only input file.
	ModeInputFile os.FileMode = 0o444
	// ModeWritableDir is an output or scratch directory: world-writable with
	// the sticky bit, so any container uid can create in it but only the
	// owner of a file can remove it.
	//
	// The sticky bit is os.ModeSticky and NOT the octal 0o1000. Go's FileMode
	// keeps the type bits above the permission bits, so writing 0o1777 here
	// sets 0777 plus a bit chmod(2) never receives — the directory ends up
	// world-writable with NO sticky bit, and any container uid can delete
	// another's outputs. Caught by TestWorkspace_PermissionsMatchTheLayout.
	ModeWritableDir os.FileMode = 0o777 | os.ModeSticky
	// ModePinLease is a pin's lease.json. It is written by the Orker and read
	// by the Orker; no container ever sees it, because it lives in the 0700 pin
	// root BESIDE the bind-mounted shared/ child rather than inside it (§8.2).
	ModePinLease os.FileMode = 0o600
)

// TaskJSONSchemaVersion is the major version of task.json this Orker writes.
//
// Within one major version the platform MUST NOT remove a field, retype a
// field, or change a field's meaning; it may only add optional fields. A
// workload that reads a schemaVersion it does not know must fail loudly rather
// than proceed on guessed semantics — a wrong result committed durably is
// worse than a clean failure (§3.2).
const TaskJSONSchemaVersion = 1

// Workspace is one attempt's host-side directory tree.
type Workspace struct {
	// Root is the per-attempt directory, mode 0700.
	Root string
	// Input is bind-mounted read-only at /orkstack/input.
	Input string
	// Secrets is a SEPARATE bind at /orkstack/input/secrets. It is separate so
	// it can be created, populated, mounted read-only and removed on every
	// exit path independently of the rest of the input tree (§3.1, §9.4).
	Secrets string
	// Output is bind-mounted read-write at /orkstack/output.
	Output string
	// Artifacts is Output/artifacts.
	Artifacts string
	// Tmp is bind-mounted read-write at /orkstack/tmp.
	Tmp string
	// Shared is the PIN's shared directory, bind-mounted read-write at
	// /orkstack/shared. It is empty for an unpinned attempt, which is the whole
	// of the conditional: an unpinned attempt renders exactly the four mounts
	// it rendered before pins existed.
	//
	// It is deliberately NOT created, sealed or removed by this type. It is
	// created by AcquirePin, it outlives this attempt, and Remove must never
	// touch it — the next task of the same pin is entitled to find it intact
	// (§7.5, §8.1).
	Shared string
}

// NewWorkspace creates the per-attempt tree with the §3.1 modes.
//
// Directories are created writable and sealed later by Seal, because the Orker
// has to write task.json, config.json, run.json and the staged inputs into a
// tree that the container will then see as 0555/0444.
func NewWorkspace(root string) (*Workspace, error) {
	ws := &Workspace{
		Root:      root,
		Input:     filepath.Join(root, "input"),
		Secrets:   filepath.Join(root, "secrets"),
		Output:    filepath.Join(root, "output"),
		Artifacts: filepath.Join(root, "output", "artifacts"),
		Tmp:       filepath.Join(root, "tmp"),
	}
	for _, dir := range []string{
		ws.Input,
		filepath.Join(ws.Input, "artifacts"),
		filepath.Join(ws.Input, "upstream"),
		// The MOUNTPOINT for the secrets bind, inside the input tree.
		//
		// /orkstack/input/secrets is a separate bind nested inside the
		// /orkstack/input bind (§3.1), and a nested bind needs its target to
		// already exist in the parent's source. Without this directory the
		// runtime tries to create the mountpoint in the container's rootfs,
		// which --read-only makes impossible, and EVERY container is refused
		// before it starts:
		//
		//   error mounting ".../secrets" to rootfs at "/orkstack/input/secrets":
		//   make mountpoint: mkdirat ...: read-only file system
		//
		// Found by TestIntegration_DispatcherRunsAReferenceImageEndToEnd.
		filepath.Join(ws.Input, "secrets"),
		ws.Secrets,
		ws.Output,
		ws.Artifacts,
		ws.Tmp,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create workspace directory %s: %w", dir, err)
		}
	}
	// The output and scratch trees are what the workload writes into, and it
	// may be any uid. Chmod rather than MkdirAll's mode argument, because
	// MkdirAll applies the process umask and 1777 is exactly the mode a
	// restrictive umask would quietly reduce.
	for _, dir := range []string{ws.Output, ws.Artifacts, ws.Tmp} {
		if err := os.Chmod(dir, ModeWritableDir); err != nil {
			return nil, fmt.Errorf("set mode on %s: %w", dir, err)
		}
	}
	return ws, nil
}

// Seal makes the input tree read-only, and is called AFTER every input file is
// written and BEFORE the container starts.
//
// Sealing before the container starts is load-bearing rather than tidy: §3.1's
// permission model depends on input/ being 0555/0444 while the workload runs,
// and it is why §7.1 pulls the image BEFORE rendering — task.json carries
// image.platformDigest, and rewriting an input file after the pull would make
// a sealed file mutable during setup.
func (w *Workspace) Seal() error {
	// Files first, then their directories: a 0555 directory cannot be
	// traversed for a chmod on macOS as reliably as one that is still
	// writable, and sealing depth-last keeps the walk simple.
	var files []string
	var dirs []string
	err := filepath.WalkDir(w.Input, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("seal input tree: %w", err)
	}
	for _, path := range files {
		if err := os.Chmod(path, ModeInputFile); err != nil {
			return fmt.Errorf("seal input file %s: %w", path, err)
		}
	}
	// Deepest first, so a parent is still traversable while its children are
	// being sealed.
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, path := range dirs {
		if err := os.Chmod(path, ModeInputDir); err != nil {
			return fmt.Errorf("seal input directory %s: %w", path, err)
		}
	}
	return nil
}

// SealSecrets makes the populated secrets directory read-only.
func (w *Workspace) SealSecrets() error {
	entries, err := os.ReadDir(w.Secrets)
	if err != nil {
		return fmt.Errorf("seal secrets: %w", err)
	}
	for _, entry := range entries {
		path := filepath.Join(w.Secrets, entry.Name())
		if err := os.Chmod(path, ModeInputFile); err != nil {
			return fmt.Errorf("seal secret %s: %w", entry.Name(), err)
		}
	}
	return os.Chmod(w.Secrets, ModeInputDir)
}

// Mounts renders the §9.2 bind set. Every source is a path this Orker created
// for this attempt OR for this attempt's pin; there is no branch that could
// produce a host path from a definition.
//
// The pin half of that promise is what §7.5 widened the sentence for, and it is
// no weaker: AcquirePin builds its path from a run id, a compiler-enforced slug
// and an instance id validated before it becomes a path segment, so a
// definition still cannot name a host path (§9.1).
func (w *Workspace) Mounts() []Mount {
	mounts := []Mount{
		{Source: w.Input, Destination: ContainerInput, ReadOnly: true},
		{Source: w.Secrets, Destination: ContainerSecrets, ReadOnly: true},
		{Source: w.Output, Destination: ContainerOutput},
		{Source: w.Tmp, Destination: ContainerTmp},
	}
	// Read-WRITE, and only when this attempt is pinned. The whole point of the
	// directory is that the next task of the pin finds what this one wrote.
	if w.Shared != "" {
		mounts = append(mounts, Mount{Source: w.Shared, Destination: ContainerShared})
	}
	return mounts
}

// Remove deletes the whole per-attempt tree.
//
// It deletes w.Root only. w.Shared belongs to the PIN and outlives every
// attempt of it, so a pinned attempt's cleanup must not reach it — the mount's
// source is a sibling of this tree, not a child of it (§8.1).
//
// Called on EVERY exit path: success, failure, panic and cancellation alike
// (§6.2).
func (w *Workspace) Remove() error {
	return removeSealedTree(w.Root)
}

// removeSealedTree deletes a tree the Orker itself made unremovable.
//
// It exists because os.RemoveAll CANNOT do it. Seal leaves input/ at 0555, and
// unlinking a file needs write and execute on its DIRECTORY, not on the file:
// a plain RemoveAll fails with "permission denied" and silently leaves the
// attempt's workspace — including any staged secret — on disk forever. Caught
// by TestWorkspace_SealMakesInputReadOnlyBeforeTheContainerStarts, whose own
// cleanup could not delete what the test had sealed.
//
// So every directory is made writable on the way down before anything is
// unlinked. A pin directory needs the identical walk for a different reason: a
// workload writing into the 1777 shared/ tree as its own uid may have left a
// 0500 directory behind, which no removal can traverse either (§8.4). One walk
// serves both, because two removal implementations would mean one of them
// eventually stops matching the other.
func removeSealedTree(root string) error {
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			// A path that cannot be walked cannot be chmod'ed either;
			// RemoveAll below reports what actually remains.
			return nil //nolint:nilerr // best-effort unseal; RemoveAll is the real gate
		}
		if entry.IsDir() {
			_ = os.Chmod(path, 0o700)
		}
		return nil
	})
	return os.RemoveAll(root)
}

// RemoveSecrets deletes the per-attempt secrets directory. It is called on
// EVERY exit path — success, failure, panic and cancellation — because secret
// bytes must not outlive the attempt that needed them (§9.4).
func (w *Workspace) RemoveSecrets() error {
	// The directory was sealed 0555, which on some systems prevents unlinking
	// its children. Restore write permission before removing.
	_ = os.Chmod(w.Secrets, 0o700)
	if entries, err := os.ReadDir(w.Secrets); err == nil {
		for _, entry := range entries {
			_ = os.Chmod(filepath.Join(w.Secrets, entry.Name()), 0o600)
		}
	}
	return os.RemoveAll(w.Secrets)
}

// ------------------------------------------------------------- pin workspace

// pinDirPrefix is what every pin directory's name starts with, and therefore
// what the boot reap and the lease sweep list by (§8.2).
const pinDirPrefix = "ork-pin-"

// pinLeaseFileName is the lease written beside — never inside — the shared
// directory, so nothing a workload can reach can forge or extend it.
const pinLeaseFileName = "lease.json"

// pinSharedDirName is the child that gets bind-mounted at /orkstack/shared.
const pinSharedDirName = "shared"

// pinReleaseBudget mirrors the engine's constant of the same name
// (internal/engine/workflow.go): the slice of a pin's lifetime reserved for the
// ReleasePin activity. It is duplicated rather than imported because the Orker
// never imports the engine, and it is duplicated as a NUMBER the lease only
// adds slack on top of — nothing here has to agree with the engine to be
// correct, only to be generous.
const pinReleaseBudget = 60 * time.Second

// pinLeaseSlack is how much longer a pin's lease outlives the pin's own
// timeout: the release budget plus five minutes (§8.4).
//
// The lease is a pure BACKSTOP and never the primary removal path. The session's
// ExecutionTimeout already bounds the pin's life and is measured from session
// CREATION, which strictly precedes the first attempt's arrival here — so by
// the time a lease could expire, Temporal has provably already declared the
// session dead and neither ReleasePin nor a boot reap nor anything else will
// ever use the directory again. The slack exists only so that the backstop can
// never fire on a pin that is merely slow.
const pinLeaseSlack = pinReleaseBudget + 5*time.Minute

// PinWorkspace is one opening of one pin, on the host that hosts it.
//
// It is the second, longer-lived kind of ephemeral execution directory (§8.1):
// still bounded, local, disposable and reconstructible by re-running, but with
// the PIN's lifetime rather than one attempt's. It is neither an input tree (it
// is never sealed) nor an output tree (nothing in it is scanned, hashed,
// uploaded or committed), and no secret is ever written under it.
type PinWorkspace struct {
	// Root is ork-pin-<run>-<pin>-<instance>, mode 0700 and owned by the
	// Orker's user — which is what makes the 1777 child safe, exactly as it is
	// for a per-attempt root.
	Root string
	// Shared is Root/shared, mode 1777 with the sticky bit. This is the path
	// bind-mounted read-write at /orkstack/shared.
	Shared string
	// LeasePath is Root/lease.json, mode 0600 (§8.4).
	LeasePath string
}

// pinLease is what lease.json holds.
//
// expiresAt is RFC 3339 with nanosecond precision, in UTC. A timestamp rather
// than a duration or a unix integer because the only readers are this Orker's
// own sweep and an operator looking at a directory wondering when it dies, and
// both are better served by a string that says so.
type pinLease struct {
	ExpiresAt string `json:"expiresAt"`
	RunID     string `json:"runId"`
	Pin       string `json:"pin"`
}

// AcquirePin resolves or creates the shared directory of one opening of one pin
// (§8.2, §8.4). Every member attempt of the pin calls it, and they all get the
// same paths back — that sharing IS the feature.
//
// It is therefore IDEMPOTENT by construction rather than by a lock: a second
// task of the pin, a second concurrent member, and a retry of a member that
// already ran all call it with the same (run, pin, instance) triple, and none
// of them may fail because the directory already exists. Every step is an
// MkdirAll or a conditional write.
//
// The instance id is VALIDATED, never sanitized, because it becomes a path
// segment — the same discipline ValidSecretName applies for the same reason.
// The engine mints it as a uuid through workflow.SideEffect, so a rejection
// here means the wire was not carrying what the engine sends, and rewriting it
// would be the one way to turn that into a silently wrong directory.
func AcquirePin(workRoot, runID, pinName, instanceID string, pinTimeoutSeconds int32) (*PinWorkspace, error) {
	return acquirePinAt(workRoot, runID, pinName, instanceID, pinTimeoutSeconds, time.Now())
}

// acquirePinAt is AcquirePin with the clock injected, so a test can assert what
// the lease says without waiting for it.
func acquirePinAt(
	workRoot, runID, pinName, instanceID string,
	pinTimeoutSeconds int32,
	now time.Time,
) (*PinWorkspace, error) {
	root, err := pinDirectory(workRoot, runID, pinName, instanceID)
	if err != nil {
		return nil, err
	}
	pin := &PinWorkspace{
		Root:      root,
		Shared:    filepath.Join(root, pinSharedDirName),
		LeasePath: filepath.Join(root, pinLeaseFileName),
	}
	if err := os.MkdirAll(pin.Root, ModeAttempt); err != nil {
		return nil, fmt.Errorf("create pin directory %s: %w", pin.Root, err)
	}
	if err := os.MkdirAll(pin.Shared, ModeWritableDir); err != nil {
		return nil, fmt.Errorf("create pin shared directory %s: %w", pin.Shared, err)
	}
	// Chmod rather than trusting MkdirAll's mode argument, and for both
	// directories, exactly as NewWorkspace does: MkdirAll applies the process
	// umask, 1777 is the mode a restrictive umask quietly reduces, and 0700 is
	// what makes the 1777 child safe from other host users. On a second call
	// these also REASSERT the modes, so a workload that chmod'ed its own view
	// of shared/ cannot leave the next member locked out.
	if err := os.Chmod(pin.Root, ModeAttempt); err != nil {
		return nil, fmt.Errorf("set mode on %s: %w", pin.Root, err)
	}
	if err := os.Chmod(pin.Shared, ModeWritableDir); err != nil {
		return nil, fmt.Errorf("set mode on %s: %w", pin.Shared, err)
	}
	if err := writePinLease(pin, runID, pinName, pinTimeoutSeconds, now); err != nil {
		return nil, err
	}
	return pin, nil
}

// writePinLease writes lease.json on FIRST arrival and leaves it alone on every
// later one.
//
// The lease is anchored to when the pin opened, NOT to the most recent
// activity, because the session's own ExecutionTimeout is likewise anchored to
// its creation rather than to recent use. Re-arming the lease on every
// AcquirePin call would let a directory outlive a session Temporal has already
// declared dead — which is the exact leak the lease exists to close, so
// extending it per call would quietly cost the feature its backstop.
func writePinLease(pin *PinWorkspace, runID, pinName string, pinTimeoutSeconds int32, now time.Time) error {
	if raw, err := os.ReadFile(pin.LeasePath); err == nil {
		var existing pinLease
		if json.Unmarshal(raw, &existing) == nil && existing.ExpiresAt != "" {
			return nil
		}
		// A lease that does not decode is rewritten rather than trusted. The
		// sweep reads an undecodable lease as abandoned, and a LIVE pin whose
		// lease write was torn by a dying host must not be swept out from
		// under the member that is about to arrive.
	}
	// A non-positive timeout leaves the slack as the whole lease. The engine
	// always sends a positive one (Pin.timeout_seconds or the derived sum), so
	// this only decides what a malformed wire gets: the shortest safe lease,
	// never a lease already in the past.
	if pinTimeoutSeconds < 0 {
		pinTimeoutSeconds = 0
	}
	expires := now.Add(time.Duration(pinTimeoutSeconds)*time.Second + pinLeaseSlack)
	encoded, err := json.MarshalIndent(pinLease{
		ExpiresAt: expires.UTC().Format(time.RFC3339Nano),
		RunID:     runID,
		Pin:       pinName,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode pin lease: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(pin.LeasePath, encoded, ModePinLease); err != nil {
		return fmt.Errorf("write pin lease %s: %w", pin.LeasePath, err)
	}
	// WriteFile's mode applies only when it CREATES the file, and the umask
	// reduces it either way; the chmod is what actually holds 0600.
	if err := os.Chmod(pin.LeasePath, ModePinLease); err != nil {
		return fmt.Errorf("set mode on %s: %w", pin.LeasePath, err)
	}
	return nil
}

// ReleasePinDirectory removes one pin's directory, and it is the ONE removal
// path all three of §8.4's ways to end a pin go through.
//
// It returns the path it removed, or "" when the directory was already gone —
// which is what lets a caller log a real removal and stay silent on the boring
// one. An absent directory is a SUCCESS, not an error: release genuinely races
// the other two ways a pin ends, and an Orker that restarted between the last
// member finishing and the release landing has already reaped it.
func ReleasePinDirectory(workRoot, runID, pinName, instanceID string) (string, error) {
	root, err := pinDirectory(workRoot, runID, pinName, instanceID)
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("stat pin directory %s: %w", root, err)
	}
	if err := removeSealedTree(root); err != nil {
		return "", fmt.Errorf("remove pin directory %s: %w", root, err)
	}
	return root, nil
}

// ReapPinDirectories removes EVERY pin directory under the work root, and is
// the boot reap of §8.4.
//
// It is not a heuristic. A pin lives inside a Temporal Worker Session whose
// resource id is a fresh uuid minted per worker start, so no session — and
// therefore no pin — can survive an Orker restart: every ork-pin-* found at
// boot is dead by construction.
//
// It returns what it removed and joins whatever it could not, so the caller can
// log both. A failure here must never fail a boot: cleanup is not orchestration
// (§8.5), the lease sweep will try again, and refusing to start over a
// directory nothing will ever read again is worse than starting.
func ReapPinDirectories(workRoot string) ([]string, error) {
	dirs, err := pinDirectories(workRoot)
	if err != nil {
		return nil, err
	}
	var removed []string
	var failures []error
	for _, dir := range dirs {
		if err := removeSealedTree(dir); err != nil {
			failures = append(failures, fmt.Errorf("remove pin directory %s: %w", dir, err))
			continue
		}
		removed = append(removed, dir)
	}
	return removed, errors.Join(failures...)
}

// SweepExpiredPinLeases removes every pin directory whose lease has expired as
// of now, and is the third way a pin ends (§8.4).
//
// It covers the one leak the other two cannot: an Orker that stays alive while
// its run is terminated from outside, where Temporal frees the session but no
// OrkStack code runs at all.
func SweepExpiredPinLeases(workRoot string, now time.Time) ([]string, error) {
	dirs, err := pinDirectories(workRoot)
	if err != nil {
		return nil, err
	}
	var removed []string
	var failures []error
	for _, dir := range dirs {
		if !pinLeaseExpired(dir, now) {
			continue
		}
		if err := removeSealedTree(dir); err != nil {
			failures = append(failures, fmt.Errorf("remove expired pin directory %s: %w", dir, err))
			continue
		}
		removed = append(removed, dir)
	}
	return removed, errors.Join(failures...)
}

// PinLeaseInfo is what one pin directory's lease says: whose pin it is and
// when its backstop fires.
type PinLeaseInfo struct {
	RunID     string
	Pin       string
	ExpiresAt time.Time
}

// ListOpenPinLeases reads every pin directory's lease under the work root, for
// DescribeOrker to decorate the open pins it counts with a name and an expiry.
//
// A directory whose lease is missing or undecodable is SKIPPED: this is a
// reader, and the sweep's own grace rule (pinLeaseExpired) decides that
// directory's fate. The count of open pins never comes from here — a session no
// member has reached yet has no directory and is still open.
func ListOpenPinLeases(workRoot string) ([]PinLeaseInfo, error) {
	dirs, err := pinDirectories(workRoot)
	if err != nil {
		return nil, err
	}
	var leases []PinLeaseInfo
	for _, dir := range dirs {
		raw, err := os.ReadFile(filepath.Join(dir, pinLeaseFileName))
		if err != nil {
			continue
		}
		var lease pinLease
		if json.Unmarshal(raw, &lease) != nil || lease.ExpiresAt == "" {
			continue
		}
		expires, err := time.Parse(time.RFC3339Nano, lease.ExpiresAt)
		if err != nil {
			continue
		}
		leases = append(leases, PinLeaseInfo{RunID: lease.RunID, Pin: lease.Pin, ExpiresAt: expires})
	}
	return leases, nil
}

// pinLeaseExpired decides whether one pin directory is dead.
//
// A lease that decodes answers directly. A MISSING or UNDECODABLE one is the
// judgment call, and it is resolved as "abandoned, after a grace period".
// AcquirePin writes a valid lease before it returns, so a directory without one
// either lost the race between its own MkdirAll and that write — in which case
// the attempt that created it failed — or had its lease truncated by a host
// that died mid-write. Neither is ever repaired, because nothing else writes a
// lease, so skipping it forever would leak exactly the directory the sweep
// exists to collect.
//
// The grace period is what makes that safe rather than merely tidy. The window
// between MkdirAll and the lease write is sub-millisecond, but it is not zero,
// and a sweep tick landing inside it must not delete a pin that is about to be
// used. pinLeaseSlack is a generous multiple of that window and is already the
// constant this file reasons about pin lifetimes with, so it is reused rather
// than a second number nobody would keep in step.
func pinLeaseExpired(dir string, now time.Time) bool {
	if raw, err := os.ReadFile(filepath.Join(dir, pinLeaseFileName)); err == nil {
		var lease pinLease
		if err := json.Unmarshal(raw, &lease); err == nil && lease.ExpiresAt != "" {
			if expires, err := time.Parse(time.RFC3339Nano, lease.ExpiresAt); err == nil {
				return now.After(expires)
			}
		}
	}
	info, err := os.Stat(dir)
	if err != nil {
		// Unstattable is not evidence of anything. Leave it for the boot reap,
		// which removes every pin directory unconditionally.
		return false
	}
	return now.Sub(info.ModTime()) >= pinLeaseSlack
}

// pinDirectories lists the pin directories under a work root. One lister serves
// the boot reap and the lease sweep, so the two can never disagree about what
// counts as a pin directory.
func pinDirectories(workRoot string) ([]string, error) {
	root := workRoot
	if root == "" {
		root = os.TempDir()
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// A root that does not exist holds no pin directories. Boot creates
			// it moments later; nothing here needs to.
			return nil, nil
		}
		return nil, fmt.Errorf("list the workspace root %s: %w", root, err)
	}
	var dirs []string
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), pinDirPrefix) {
			continue
		}
		dirs = append(dirs, filepath.Join(root, entry.Name()))
	}
	return dirs, nil
}

// pinDirectory renders ork-pin-<run>-<pin>-<instance> under the work root, and
// is the single place that name is spelled.
//
// safeComponent is defensive on both of its arguments rather than load-bearing:
// a pin name is a compiler-enforced slug and is already safe, and a run id is
// Orker-facing but engine-minted. The instance id is the one component that is
// checked and refused instead, because it is the one the directory's identity
// depends on.
func pinDirectory(workRoot, runID, pinName, instanceID string) (string, error) {
	if !validPinInstanceID(instanceID) {
		return "", fmt.Errorf(
			"pin instance id %q must match ^[0-9a-f-]{36}$; it becomes a path component and is "+
				"validated rather than rewritten, because a rewritten id would name a DIFFERENT "+
				"pin's shared directory", instanceID)
	}
	root := workRoot
	if root == "" {
		root = os.TempDir()
	}
	name := pinDirPrefix + safeComponent(runID) + "-" + safeComponent(pinName) + "-" + instanceID
	return filepath.Join(root, name), nil
}

// validPinInstanceID holds an instance id to ^[0-9a-f-]{36}$ (§8.2), the shape
// of the lowercase uuid the engine mints through workflow.SideEffect.
//
// Hand-rolled for the same reason ValidSecretName is: the grammar is the
// contract, and reading it as a loop makes it obvious that "." and ".." and
// every separator are outside it rather than merely unlikely.
func validPinInstanceID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f', r == '-':
		default:
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------- task.json

// taskJSON is the §3.2 schema. It is spelled out as a struct rather than
// assembled from a map because task.json is a CONTRACT: a field that silently
// changed type or disappeared would break every workload reading it, and the
// table in §3.2 is the specification a reader is entitled to rely on.
//
// Key casing is lowerCamelCase, matching every other JSON surface OrkStack
// emits (docs/JSON.md).
type taskJSON struct {
	SchemaVersion  int              `json:"schemaVersion"`
	IdempotencyKey string           `json:"idempotencyKey"`
	Run            taskJSONRun      `json:"run"`
	Task           taskJSONTask     `json:"task"`
	Image          taskJSONImage    `json:"image"`
	Limits         taskJSONLimits   `json:"limits"`
	Resources      *taskJSONRes     `json:"resources,omitempty"`
	Network        string           `json:"network"`
	Paths          taskJSONPaths    `json:"paths"`
	Inputs         taskJSONInputs   `json:"inputs"`
	Secrets        []taskJSONSecret `json:"secrets"`
	// Trace is present ONLY when this deployment exports traces. It is
	// additive and informational, never ABI: it does not bump SchemaVersion,
	// and a workload that ignores it loses nothing (internal/telemetry is the
	// only OpenTelemetry-aware code in the repository; this package never
	// imports it, and fills the field from a plain func the caller supplies).
	Trace *taskJSONTrace `json:"trace,omitempty"`
	// Each is present ONLY for an item of a fan-out task, absent as a key
	// otherwise — the precedent of task.pin: an added optional key keeps
	// SchemaVersion at 1 and costs a workload that never heard of fan-out
	// nothing.
	Each *taskJSONEach `json:"each,omitempty"`
	// Join is present ONLY for a failure handler, absent as a key otherwise
	// — the precedent of task.pin and each: an added optional key keeps
	// SchemaVersion at 1 and costs a workload that never heard of joins
	// nothing.
	Join *taskJSONJoin `json:"join,omitempty"`
}

// taskJSONJoin tells a failure handler how each of its needs ended, in
// needs order (ExecuteTaskInput.join, docs/DSL.md "Joins and failure
// handlers").
type taskJSONJoin struct {
	Policy string                `json:"policy"` // "any_failed" | "all_done"
	Needs  []taskJSONNeedOutcome `json:"needs"`
}

type taskJSONNeedOutcome struct {
	TaskID string             `json:"taskId"`
	Status string             `json:"status"` // SUCCEEDED | FAILED | SKIPPED | CANCELED
	Error  *taskJSONNeedError `json:"error,omitempty"`
}

type taskJSONNeedError struct {
	Code     string `json:"code"`
	Category string `json:"category,omitempty"` // ActivityFailed | InfrastructureError | ContractViolation | TimedOut | Cancelled
	Message  string `json:"message"`
	Details  string `json:"details,omitempty"`
}

// taskJSONJoinOf renders ExecuteTaskInput.join for task.json: the status
// name without STATUS_, the AGENTS.md §4 category name (omitted for
// UNSPECIFIED), and the join word. Nil for a task that is not a failure
// handler.
func taskJSONJoinOf(join *orkstackv1.JoinContext) *taskJSONJoin {
	if join == nil {
		return nil
	}
	out := &taskJSONJoin{Policy: dsl.JoinWord(join.GetPolicy()), Needs: []taskJSONNeedOutcome{}}
	for _, need := range join.GetNeeds() {
		outcome := taskJSONNeedOutcome{
			TaskID: need.GetTaskId(),
			Status: strings.TrimPrefix(need.GetStatus().String(), "STATUS_"),
		}
		if e := need.GetError(); e != nil {
			outcome.Error = &taskJSONNeedError{
				Code:     e.GetCode(),
				Category: failureCategoryWord(e.GetCategory()),
				Message:  e.GetMessage(),
				Details:  e.GetDetails(),
			}
		}
		out.Needs = append(out.Needs, outcome)
	}
	return out
}

// failureCategoryWord is the AGENTS.md §4 name of a category — the
// ApplicationError type string — or "" for UNSPECIFIED.
func failureCategoryWord(c orkstackv1.FailureCategory) string {
	switch c {
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED:
		return task.CategoryActivityFailed
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR:
		return task.CategoryInfrastructureError
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION:
		return task.CategoryContractViolation
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT:
		return task.CategoryTimedOut
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED:
		return task.CategoryCancelled
	}
	return ""
}

// taskJSONEach is one item's coordinates and value. Item is the element's
// exact JSON text for a task source (key order, number spelling and a
// 20-digit integer preserved) and the protojson rendering of the run input's
// value for an input source.
type taskJSONEach struct {
	Index int32           `json:"index"`
	Count int32           `json:"count"`
	As    string          `json:"as"`
	Item  json.RawMessage `json:"item"`
}

// taskJSONEachUpstream says a fan-out upstream exists and how many items it
// staged, so a workload can tell "zero items" from "no such upstream".
type taskJSONEachUpstream struct {
	TaskID string `json:"taskId"`
	Count  int32  `json:"count"`
}

// taskJSONTrace is the W3C trace carrier, in the same form `traceparent` and
// `tracestate` HTTP headers use. A workload that wants to join the trace its
// activity ran under propagates these verbatim; one that does not recognize
// the key ignores it, per §3.2's unknown-field rule.
type taskJSONTrace struct {
	Traceparent string `json:"traceparent"`
	Tracestate  string `json:"tracestate,omitempty"`
}

type taskJSONRun struct {
	ID        string          `json:"id"`
	Namespace string          `json:"namespace"`
	Orkflow   taskJSONOrkflow `json:"orkflow"`
}

type taskJSONOrkflow struct {
	Name             string `json:"name"`
	Version          string `json:"version"`
	DefinitionDigest string `json:"definitionDigest"`
}

type taskJSONTask struct {
	ID      string `json:"id"`
	Attempt int32  `json:"attempt"`
	Pool    string `json:"pool"`
	// RunAsOrkerUser is present and true only when the Orker overrode the
	// image's own USER, so a workload can see WHY its uid is not the one its
	// image declared. omitempty is the "absent = false" the table specifies.
	RunAsOrkerUser bool `json:"runAsOrkerUser,omitempty"`
	// Pin is present ONLY for a pinned task, absent as a key rather than
	// written empty — which is what makes the addition additive under the rule
	// above (a major version may only ADD optional fields), keeps
	// SchemaVersion at 1, and costs a workload that has never heard of pins
	// nothing (§3.2).
	Pin *taskJSONPin `json:"pin,omitempty"`
}

// taskJSONPin names the pin this task belongs to and which opening of it.
//
// InstanceID is what the Orker keys the shared directory by; to the workload it
// is informational, and it is the value an operator correlates a run's
// directories and Temporal history with.
type taskJSONPin struct {
	Name       string `json:"name"`
	InstanceID string `json:"instanceId"`
}

type taskJSONImage struct {
	Reference      string `json:"reference"`
	Pinned         string `json:"pinned"`
	IndexDigest    string `json:"indexDigest"`
	PlatformDigest string `json:"platformDigest,omitempty"`
	Platform       string `json:"platform,omitempty"`
	Delivery       string `json:"delivery,omitempty"`
}

type taskJSONLimits struct {
	TimeoutSeconds                 int32 `json:"timeoutSeconds"`
	CancellationGracePeriodSeconds int32 `json:"cancellationGracePeriodSeconds"`
	ValuesMaxBytes                 int64 `json:"valuesMaxBytes"`
	ArtifactMaxCount               int32 `json:"artifactMaxCount"`
	ArtifactMaxBytes               int64 `json:"artifactMaxBytes"`
	LogMaxBytes                    int64 `json:"logMaxBytes"`
	CommitMaxBytes                 int64 `json:"commitMaxBytes"`
}

type taskJSONRes struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type taskJSONPaths struct {
	Input     string `json:"input"`
	Config    string `json:"config"`
	RunInput  string `json:"runInput"`
	Secrets   string `json:"secrets"`
	Output    string `json:"output"`
	Values    string `json:"values"`
	Artifacts string `json:"artifacts"`
	Tmp       string `json:"tmp"`
	// Shared is /orkstack/shared, present only for a pinned task — the mount
	// itself does not exist for an unpinned one, so naming its path would be a
	// lie rather than a convenience (§3.1).
	Shared string `json:"shared,omitempty"`
}

type taskJSONInputs struct {
	Artifacts []taskJSONInputArtifact `json:"artifacts"`
	Upstream  []taskJSONUpstream      `json:"upstream"`
	// EachUpstream is present only when a fan-out upstream exists.
	EachUpstream []taskJSONEachUpstream `json:"eachUpstream,omitempty"`
}

type taskJSONInputArtifact struct {
	Producer string `json:"producer"`
	// Name is present only when the exact logical name is valid UTF-8.
	Name string `json:"name,omitempty"`
	// NameB64 is ALWAYS present: RawURL base64 of the exact name bytes, and
	// authoritative. It exists because `path` is produced by safeComponent,
	// which rewrites separators, dot segments and long names and therefore
	// cannot carry the logical name — a workload must never have to reverse
	// that rule.
	NameB64     string `json:"nameB64"`
	Path        string `json:"path"`
	SizeBytes   int64  `json:"sizeBytes"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"contentType,omitempty"`
	// Item is present only for a file one item of a fan-out produced.
	Item *int32 `json:"item,omitempty"`
}

type taskJSONUpstream struct {
	TaskID     string `json:"taskId"`
	ValuesPath string `json:"valuesPath"`
	// Item is present only for one item of a fan-out upstream.
	Item *int32 `json:"item,omitempty"`
}

type taskJSONSecret struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// TaskContext is everything the workspace needs that is not on the wire.
type TaskContext struct {
	Attempt        int32
	Pool           string
	Namespace      string
	RunAsOrkerUser bool
	EffectiveCPU   string
	EffectiveMem   string
	Network        string
	Image          *orkstackv1.ImageRef
	StagedInputs   []taskJSONInputArtifact
	StagedUpstream []taskJSONUpstream
	StagedSecrets  []taskJSONSecret
	// Trace carries "traceparent" and optionally "tracestate" in W3C form.
	// Nil or an empty "traceparent" means this deployment does not trace, and
	// task.json omits the whole `trace` key (§3.2).
	Trace map[string]string
	// Pin is the pin context off ExecuteTaskInput, or nil for an unpinned task.
	// Nil is what keeps paths.shared and task.pin out of an unpinned task's
	// task.json entirely.
	Pin *orkstackv1.PinContext
	// Each is the item context off ExecuteTaskInput, or nil for an attempt
	// that is not an item; EachItem is the resolved element's JSON text.
	Each     *orkstackv1.EachContext
	EachItem json.RawMessage
	// StagedEachUpstream lists every fan-out upstream staged for a direct
	// dependent, with its item count.
	StagedEachUpstream []taskJSONEachUpstream
}

// WriteTaskJSON renders /orkstack/input/task.json.
//
// Every field a workload could otherwise want from an environment variable
// lives here, because environment variables are not part of the ABI
// (AGENTS.md §3). An absent array is written as [], never null; an absent
// optional scalar is omitted, never written as "" — a workload distinguishes
// "no content type" from "empty content type" by presence (§3.2).
func (w *Workspace) WriteTaskJSON(in *orkstackv1.ExecuteTaskInput, tc TaskContext) error {
	limits := in.GetLimits()
	doc := taskJSON{
		SchemaVersion:  TaskJSONSchemaVersion,
		IdempotencyKey: in.GetIdempotencyKey(),
		Run: taskJSONRun{
			ID:        in.GetRunId(),
			Namespace: tc.Namespace,
			Orkflow: taskJSONOrkflow{
				Name:             in.GetOrkflowName(),
				Version:          in.GetOrkflowVersion(),
				DefinitionDigest: in.GetOrkflowDefinitionDigest(),
			},
		},
		Task: taskJSONTask{
			ID:             in.GetTaskId(),
			Attempt:        tc.Attempt,
			Pool:           tc.Pool,
			RunAsOrkerUser: tc.RunAsOrkerUser,
		},
		Image: taskJSONImage{
			Reference:      tc.Image.GetReference(),
			Pinned:         tc.Image.GetPinned(),
			IndexDigest:    tc.Image.GetIndexDigest(),
			PlatformDigest: tc.Image.GetPlatformDigest(),
			Platform:       tc.Image.GetPlatform(),
			Delivery:       tc.Image.GetDelivery(),
		},
		Limits: taskJSONLimits{
			TimeoutSeconds:                 in.GetTimeoutSeconds(),
			CancellationGracePeriodSeconds: in.GetCancellationGracePeriodSeconds(),
			ValuesMaxBytes:                 limits.GetValuesMaxBytes(),
			ArtifactMaxCount:               limits.GetArtifactMaxCount(),
			ArtifactMaxBytes:               limits.GetArtifactMaxBytes(),
			LogMaxBytes:                    limits.GetLogMaxBytes(),
			CommitMaxBytes:                 limits.GetCommitMaxBytes(),
		},
		Network: tc.Network,
		Paths: taskJSONPaths{
			Input:     ContainerInput,
			Config:    ContainerConfigJSON,
			RunInput:  ContainerRunJSON,
			Secrets:   ContainerSecrets,
			Output:    ContainerOutput,
			Values:    ContainerValuesJSON,
			Artifacts: ContainerArtifacts,
			Tmp:       ContainerTmp,
		},
		Inputs: taskJSONInputs{
			Artifacts:    tc.StagedInputs,
			Upstream:     tc.StagedUpstream,
			EachUpstream: tc.StagedEachUpstream,
		},
		Secrets: tc.StagedSecrets,
	}
	// The resources block reports the EFFECTIVE values after Orker policy,
	// which is exactly what §3.2 says it means: "as requested/capped; absent
	// means Orker default".
	if tc.EffectiveCPU != "" || tc.EffectiveMem != "" {
		doc.Resources = &taskJSONRes{CPU: tc.EffectiveCPU, Memory: tc.EffectiveMem}
	}
	// trace is written only when there is a real traceparent to carry
	// (§3.2): an empty or absent Trace map must never produce a `trace: {}`
	// or a `trace: {"traceparent": ""}` key for a deployment that never
	// traces.
	if traceparent := tc.Trace["traceparent"]; traceparent != "" {
		doc.Trace = &taskJSONTrace{Traceparent: traceparent, Tracestate: tc.Trace["tracestate"]}
	}
	// paths.shared and task.pin exist only for a pinned task, and are set
	// TOGETHER: the path is meaningful exactly when the mount is there, and the
	// mount is there exactly when this attempt belongs to a pin (§3.1, §3.2).
	if tc.Pin != nil {
		doc.Paths.Shared = ContainerShared
		doc.Task.Pin = &taskJSONPin{Name: tc.Pin.GetName(), InstanceID: tc.Pin.GetInstanceId()}
	}
	// each exists only for an item of a fan-out (§3.2), as task.pin does.
	if e := tc.Each; e != nil {
		item := tc.EachItem
		if len(item) == 0 {
			item = json.RawMessage("null")
		}
		doc.Each = &taskJSONEach{Index: e.GetIndex(), Count: e.GetCount(), As: e.GetAs(), Item: item}
	}
	// join exists only for a failure handler (§3.2), read straight off the
	// wire: TaskContext does not carry it.
	doc.Join = taskJSONJoinOf(in.GetJoin())
	// Never null.
	if doc.Inputs.Artifacts == nil {
		doc.Inputs.Artifacts = []taskJSONInputArtifact{}
	}
	if doc.Inputs.Upstream == nil {
		doc.Inputs.Upstream = []taskJSONUpstream{}
	}
	if doc.Secrets == nil {
		doc.Secrets = []taskJSONSecret{}
	}
	return w.writeInputFile("task.json", doc)
}

// WriteConfigJSON writes the RENDERED `with:` block.
func (w *Workspace) WriteConfigJSON(rendered map[string]any) error {
	return w.writeInputFile("config.json", configValue(rendered))
}

// configValue is what config.json encodes for a render: an empty object for
// none. The memo key hashes encodeInputFile(configValue(rendered)), the same
// bytes WriteConfigJSON writes.
func configValue(rendered map[string]any) map[string]any {
	if rendered == nil {
		return map[string]any{}
	}
	return rendered
}

// WriteRunJSON writes the run's input payload — the same data templates see as
// .inputs.
func (w *Workspace) WriteRunJSON(inputs map[string]any) error {
	if inputs == nil {
		inputs = map[string]any{}
	}
	return w.writeInputFile("run.json", inputs)
}

// encodeInputFile renders value as config.json and run.json are written:
// json.MarshalIndent(value, "", "  ") plus a trailing newline. It is the ONE
// encoder of both files, so a cached task's memo key (memo.go) hashes exactly
// the bytes its container would read; a build that changes this encoding
// changes every key, and every memo misses once — safe.
func encodeInputFile(value any) ([]byte, error) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func (w *Workspace) writeInputFile(name string, value any) error {
	encoded, err := encodeInputFile(value)
	if err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	path := filepath.Join(w.Input, name)
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}

// EncodeArtifactName renders the always-present nameB64 of §3.2.
func EncodeArtifactName(name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

// ---------------------------------------------------------------- output scan

// ScannedArtifact is one file found in output/artifacts/.
type ScannedArtifact struct {
	Name string
	Path string
	Size int64
}

// ScanOutputArtifacts implements §3.5.
//
// The scan is NOT recursive: a directory, symlink, device, socket or FIFO is a
// ContractViolation, and a directory hierarchy is archived by the workload as
// e.g. workspace.tar.gz. The artifact layer has no directory semantics
// (AGENTS.md §3.1).
//
// Ordering is by name bytes ascending, so two attempts of the same computation
// produce the same commit ordering.
//
// It runs on the SUCCESS path and on the FAILURE path. An earlier draft sent a
// non-zero exit straight to the failure path, which left these rules
// unenforced on exactly the path a broken or hostile image is most likely to
// take. What differs between the two is the CONSEQUENCE: on the failure path a
// violation is a note attached to the workload's error, never a replacement
// for it (§7.1 step 14).
func ScanOutputArtifacts(dir string, limits *orkstackv1.WorkspaceLimits) ([]ScannedArtifact, *Failure) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// A workload that produced no artifacts did not have to create
			// the directory. That is not a violation.
			return nil, nil
		}
		return nil, FailCause(ReasonOutputNotReadable, err,
			"cannot read the output artifact directory %s: %v", ContainerArtifacts, err)
	}

	return scanOutputArtifactEntries(dir, entries, limits)
}

// scanOutputArtifactEntries is the filesystem-independent half of the output
// scanner. Keeping the name validation here lets tests supply byte strings
// that the host filesystem itself may refuse to create.
func scanOutputArtifactEntries(dir string, entries []os.DirEntry, limits *orkstackv1.WorkspaceLimits) ([]ScannedArtifact, *Failure) {
	maxCount := limits.GetArtifactMaxCount()
	maxBytes := limits.GetArtifactMaxBytes()

	found := make([]ScannedArtifact, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()

		// Valid UTF-8 is a real restriction on a POSIX filesystem, and it is
		// imposed because the alternative is worse: ArtifactRef.name is a
		// proto string, and EVERY protobuf encoding — protojson and binary
		// alike — refuses invalid UTF-8 (§14.16). Such a name could be
		// staged, hashed and uploaded and would then fail at the commit,
		// after the bandwidth was spent. Rejecting at the scan makes it a
		// fast, terminal, actionable failure instead.
		if !utf8.ValidString(name) {
			return nil, Fail(ReasonArtifactNameNotUTF8,
				"output artifact name is not valid UTF-8: bytes %s in %s",
				hex.EncodeToString([]byte(name)), ContainerArtifacts)
		}

		info, err := entry.Info()
		if err != nil {
			return nil, FailCause(ReasonOutputNotReadable, err,
				"cannot stat output artifact %s/%s: %v", ContainerArtifacts, name, err)
		}
		if !info.Mode().IsRegular() {
			return nil, Fail(ReasonArtifactNotAFile,
				"%s/%s is %s, not a regular file; the artifact layer has no directory semantics — "+
					"archive a hierarchy yourself, e.g. as workspace.tar.gz",
				ContainerArtifacts, name, describeMode(info.Mode()))
		}
		if maxBytes > 0 && info.Size() > maxBytes {
			return nil, Fail(ReasonArtifactLimit,
				"output artifact %s/%s is %d bytes, over the %d-byte per-artifact limit",
				ContainerArtifacts, name, info.Size(), maxBytes)
		}
		found = append(found, ScannedArtifact{
			Name: name,
			Path: filepath.Join(dir, name),
			Size: info.Size(),
		})
	}

	if maxCount > 0 && int32(len(found)) > maxCount {
		return nil, Fail(ReasonArtifactLimit,
			"%d output artifacts exceed the limit of %d", len(found), maxCount)
	}

	sort.Slice(found, func(i, j int) bool { return found[i].Name < found[j].Name })
	return found, nil
}

// describeMode names a non-regular entry in the words a person uses.
func describeMode(mode os.FileMode) string {
	switch {
	case mode.IsDir():
		return "a directory"
	case mode&os.ModeSymlink != 0:
		return "a symlink"
	case mode&os.ModeDevice != 0:
		return "a device"
	case mode&os.ModeSocket != 0:
		return "a socket"
	case mode&os.ModeNamedPipe != 0:
		return "a FIFO"
	default:
		return "not a regular file"
	}
}

// CheckReadable opens a file the Orker must read, and fails terminally when it
// cannot.
//
// This is the §3.1 measure, and it exists because READABILITY IS NOT
// GUARANTEED BY THE MOUNT. 1777 guarantees the workload can CREATE a file; it
// says nothing about the mode that file lands with. A workload running under
// umask 077 writes values.json as 0600 owned by its own uid, and an Orker
// running as a different non-root user then cannot read the bytes it must hash
// and upload. Both outcomes reproduce on one machine: on a native filesystem
// the Orker gets Permission denied, and through a VM-shared bind mount the
// same write is ownership-remapped and reads fine (§14.8). No mount
// configuration can be relied on, so the Orker verifies rather than assumes.
//
// stat succeeds even when open fails, which is what lets the message always be
// specific — it names the file, its mode, its owner and the fix.
//
// The classification is deliberate. OUTPUT_NOT_READABLE is a ContractViolation
// and is never retried: the image will do the same thing on every attempt.
// Classifying it as ARTIFACT_UPLOAD_FAILED — infrastructure, retryable — would
// loop forever against deterministic image behavior, which AGENTS.md §11
// forbids.
func CheckReadable(path, containerPath string) *Failure {
	file, err := os.Open(path)
	if err == nil {
		_ = file.Close()
		return nil
	}
	detail := describeUnreadable(path)
	return FailCause(ReasonOutputNotReadable, err,
		"the orker cannot read %s (%s): %v — the workload wrote it with permissions the orker's "+
			"user cannot open. Set `umask 022` in the image's entry script, or set "+
			"runAsOrkerUser: true on this task",
		containerPath, detail, err)
}

// describeUnreadable renders "mode=600 owner=65534:65534" for the message.
func describeUnreadable(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return "unstattable"
	}
	described := fmt.Sprintf("mode=%o", info.Mode().Perm())
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		described += fmt.Sprintf(" owner=%d:%d", stat.Uid, stat.Gid)
	}
	return described
}

// ---------------------------------------------------------------- values.json

// ReadValues validates and returns the workload's values.json bytes (§3.4).
//
// The bytes are returned VERBATIM. The Orker parses to validate and never
// re-serializes: key order, spacing and number formatting are preserved, and
// the committed sha256 is the sha256 of exactly what the workload wrote. The
// same exact bytes are what travels inline as ExecuteTaskResult.values_json,
// which is why that field is bytes and not a Struct-backed Payload (§14.9).
func ReadValues(path string, limits *orkstackv1.WorkspaceLimits) ([]byte, *Failure) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, Fail(ReasonValuesMissing,
				"the container exited 0 but wrote no %s; a workload with no result writes {}",
				ContainerValuesJSON)
		}
		return nil, FailCause(ReasonOutputNotReadable, err,
			"cannot stat %s: %v", ContainerValuesJSON, err)
	}
	if !info.Mode().IsRegular() {
		return nil, Fail(ReasonValuesMalformed,
			"%s is %s, not a regular file", ContainerValuesJSON, describeMode(info.Mode()))
	}
	// Size is checked before the read so an enormous file costs no memory.
	if max := limits.GetValuesMaxBytes(); max > 0 && info.Size() > max {
		return nil, Fail(ReasonValuesTooLarge,
			"%s is %d bytes, over the %d-byte limit", ContainerValuesJSON, info.Size(), max)
	}
	if failure := CheckReadable(path, ContainerValuesJSON); failure != nil {
		return nil, failure
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, FailCause(ReasonOutputNotReadable, err,
			"cannot read %s: %v", ContainerValuesJSON, err)
	}
	if !utf8.Valid(raw) {
		return nil, Fail(ReasonValuesMalformed, "%s is not valid UTF-8", ContainerValuesJSON)
	}
	// A top-level object, specifically. An array, a scalar or null is a
	// violation: downstream templates address .tasks.<id>.output.<field>, and
	// there is no field on an array.
	var probe any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, FailCause(ReasonValuesMalformed, err,
			"%s is not valid JSON: %v", ContainerValuesJSON, err)
	}
	if _, ok := probe.(map[string]any); !ok {
		return nil, Fail(ReasonValuesMalformed,
			"%s must be a JSON object at the top level, not %s", ContainerValuesJSON, jsonKind(probe))
	}
	return raw, nil
}

func jsonKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	default:
		return "an unexpected value"
	}
}

// InlineValuesLimit is the size at or below which values.json is copied inline
// into ExecuteTaskResult.values_json.
//
// values.json's own budget is 256 KiB because it is an object-store artifact,
// and an object-store budget is not the Temporal payload budget. The Temporal
// budget is enforced separately, and this is it (§3.4).
const InlineValuesLimit = 4096

// HashFile streams a file and returns its lowercase hex sha256 and size.
func HashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(digest.Sum(nil)), size, nil
}

// sniffFileContentType resolves the content type the artifact store would
// record for this file, by reading exactly the bytes the store's own sniffer
// inspects and applying the store's own rule.
//
// It exists so §5.4 step 4 can PREDICT a ref whose ContentType matches the one
// step 5 stores. RefPredictor.RefFor uses its contentType verbatim and cannot
// sniff bytes it was never handed, so the caller resolves it once here and
// passes the same value to both sides.
func sniffFileContentType(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	head := make([]byte, artifacts.SniffLen)
	n, err := io.ReadFull(file, head)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	return artifacts.SniffContentType(head[:n]), nil
}

// HashBytes returns the lowercase hex sha256 of a byte slice.
func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ValidSecretName holds a secret name to the §3.2 grammar,
// ^[A-Za-z0-9._-]{1,128}$. A name is validated rather than sanitized, so it
// can never become a path segment that escapes the secrets directory.
func ValidSecretName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-':
		default:
			return false
		}
	}
	// "." and ".." pass the character test and are still path segments.
	return name != "." && name != ".."
}

// SplitSecretSource splits "env:DB_PASSWORD" into its scheme and locator.
func SplitSecretSource(source string) (scheme, locator string) {
	scheme, locator, found := strings.Cut(source, ":")
	if !found {
		return "", source
	}
	return scheme, locator
}
