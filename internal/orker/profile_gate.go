package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"slices"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Step 3c and the recheck before create (design §4.4, §4.11): a task that
// names profiles runs here only with each one this Orker offers, with the
// identity its run pinned at submit, and with every device as the boot audit
// found it. A device changed since is drift: it latches, holds the cordon
// until the Orker restarts, and is never adopted.

// The step-3c reasons. Each is an InfrastructureError retried under the
// task's policy: another Orker of the pool may hold the profile as the run
// pinned it.
const (
	// ReasonProfileNotOffered: this Orker does not offer a profile the task
	// names.
	ReasonProfileNotOffered = "PROFILE_NOT_OFFERED"
	// ReasonProfileChanged: this Orker offers the profile with another
	// identity than the run pinned at submit, or the run pinned none.
	ReasonProfileChanged = "PROFILE_CHANGED"
	// ReasonProfileDrifted: a device of the profile no longer matches what
	// this Orker audited at boot.
	ReasonProfileDrifted = "PROFILE_DRIFTED"
)

// auditedDevice is one device of a profile and the host node its boot audit
// found behind it.
type auditedDevice struct {
	ProfileDevice
	Host hostDevice
}

// profileAudit is one profile as its boot audit found it.
type profileAudit struct {
	// identity is the default contract's, the profile's own.
	identity string
	// devices are every contract's host devices, each once: what the drift
	// checks and the host-view probe cover. A container path two of the
	// name's contracts both give is replaced, for the probe alone, by one of
	// its own (probeContainerPath).
	devices []auditedDevice
	// contracts are a devices profile's contracts by label: each one's
	// identity and the devices it passes a task. A gpu profile's one contract
	// carries its identity and no devices.
	contracts map[string]contractAudit
	// def is the label of the default contract.
	def string
	// gpu is a gpu profile's CDI audit (cdi_audit.go): the audit set's
	// digests, re-hashed at 3c and before create. Nil for a devices profile,
	// and for a gpu profile whose spec was absent at boot.
	gpu *gpuSpecAudit
	// hookUnknown is why a gpu profile has no spec audit though its specs
	// were found: the hook binary behind them is a build the table lacks
	// (HOOK_BUILD_UNKNOWN). The profile is held unavailable for it alone.
	hookUnknown string
	// gpuNodes are a gpu profile's unit nodes, stat'ed as devices are; they
	// reach a container through CDI (dispatch_units.go), never as a mapping.
	gpuNodes []auditedDevice
	// unitNodes are gpuNodes by unit, in the profile's unit order, for the
	// unit probes.
	unitNodes [][]auditedDevice
}

// contractAudit is one contract of a profile as its boot audit found it.
type contractAudit struct {
	identity string
	accepts  []string
	devices  []auditedDevice
}

// setAudited records the boot audit. It is set once, before the Orker polls.
func (s *profileState) setAudited(audits map[string]profileAudit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = maps.Clone(audits)
}

// audit is one profile's boot audit, and whether it has one.
func (s *profileState) audit(name string) (profileAudit, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.audits[name]
	return a, ok
}

// gate is step 3c's devices: those of the contracts serving the task's
// profiles (gateSelect), or why it cannot run here.
func (s *profileState) gate(refs []*orkstackv1.ProfileRef, inspect func(string) (hostDevice, error), now time.Time) ([]DeviceMapping, *Failure) {
	devices, _, failure := s.gateSelect(refs, inspect, now)
	return devices, failure
}

// recheck is the device comparison again, just before the container is
// created (beforeCreate) and again just before it is started. The two checks
// that cost a call to the host's engine or a read of every spec file run once
// per attempt, in the first only: the engine's default runtime, asked with the
// attempt's context, and the spec directories rescanned for a spec of a gpu
// profile's kind that the audit did not read. The runtime not being able to say
// is a never-started RUNTIME_STATE_UNKNOWN, the fault an attempt that could not
// read the runtime's state is already given, so the task runs again without
// spending its own attempt; a spec file that cannot be read or parsed is a
// never-started PROFILE_NOT_OFFERED with nothing latched, the file being
// settled by the next attempt.
func (s *profileState) recheck(ctx context.Context, refs []*orkstackv1.ProfileRef, inspect func(string) (hostDevice, error), now time.Time, beforeCreate bool) *Failure {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		audit, ok := s.audit(ref.GetName())
		if !ok {
			return Fail(ReasonProfileNotOffered, "profile %q is not offered by this Orker", ref.GetName())
		}
		if failure := s.checkDevices(ref.GetName(), audit, inspect, now); failure != nil {
			return failure
		}
		names = append(names, ref.GetName())
	}
	if !beforeCreate {
		return nil
	}
	if failure := s.checkRuntime(ctx, names, now); failure != nil {
		return neverStarted(failure)
	}
	for _, name := range names {
		audit, _ := s.audit(name)
		if failure := s.checkSpecDirs(name, audit, now); failure != nil {
			return neverStarted(failure)
		}
	}
	return nil
}

// checkDevices is step 3c's and the recheck's device check: a drifted or
// unavailable profile fails, and otherwise its devices are stat'ed. Both run
// before the container is created, so every failure here (a drift or an
// unavailable profile) is marked never-started; only PROFILE_DRIFTED is an
// interruption the Orker may emit on that mark (emission.go), and the mark on
// PROFILE_NOT_OFFERED decides nothing today.
func (s *profileState) checkDevices(name string, audit profileAudit, inspect func(string) (hostDevice, error), now time.Time) *Failure {
	return neverStarted(s.compareDevices(name, audit, inspect, now))
}

// compareDevices is checkDevices without the never-started mark.
func (s *profileState) compareDevices(name string, audit profileAudit, inspect func(string) (hostDevice, error), now time.Time) *Failure {
	if s.drifted(name) {
		return Fail(ReasonProfileDrifted,
			"profile %q drifted on this Orker since its boot audit; it stays out of service until the Orker restarts", name)
	}
	if s.unavailable(name) {
		// Held unavailable: a device is not there, or no container has been
		// shown to see the audited devices (host_view_probe.go), so none is
		// started with them.
		return Fail(ReasonProfileNotOffered,
			"profile %q is unavailable on this Orker until its devices and its host-view probe pass again; "+
				"the task runs on an Orker of its pool that offers it", name)
	}
	if failure := s.checkSpecBytes(name, audit, now); failure != nil {
		return failure
	}
	return s.statDevices(name, audit, inspect, now)
}

// checkSpecBytes re-checks a gpu profile's audit set (design §4.11, at 3c and
// just before create): a file that is gone, or that cannot be read, holds the
// profile unavailable (SPEC_ABSENT); one whose bytes differ latches a drift
// (SPEC_CHANGED). So does a mount source that is no longer the file the audit
// found (MOUNT_SOURCE_CHANGED, in the drift's detail), before the hook
// binaries are hashed again. (A spec file of the kind that appeared is
// checkSpecDirs's, once per attempt.)
func (s *profileState) checkSpecBytes(name string, audit profileAudit, now time.Time) *Failure {
	if audit.gpu == nil {
		return nil
	}
	drifted := func(detail, what string) *Failure {
		s.setDrift(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED, detail, now)
		return Fail(ReasonProfileDrifted,
			"profile %q drifted on this Orker: %s; it stays out of service until the Orker restarts", name, what)
	}
	for _, path := range slices.Sorted(maps.Keys(audit.gpu.files)) {
		raw, err := os.ReadFile(path)
		if err != nil {
			s.setUnavailable(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT,
				fmt.Sprintf("%s: %v", path, err), now)
			return Fail(ReasonProfileNotOffered,
				"profile %q is unavailable on this Orker: a CDI spec it was audited from cannot be read; "+
					"the task runs on an Orker of its pool that offers it", name)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != audit.gpu.files[path] {
			return drifted(path+" differs from the bytes this Orker audited", "a CDI spec it was audited from changed")
		}
	}
	inspect := s.inspectMount
	if inspect == nil {
		inspect = inspectHostMount
	}
	for _, src := range slices.Sorted(maps.Keys(audit.gpu.mounts)) {
		want := audit.gpu.mounts[src]
		if src == nvidiaVoidSource && want.Resolved == nvidiaVoidSource {
			continue // the placeholder, fixed by the grammar
		}
		got, err := inspect(src)
		switch {
		case err != nil:
			return drifted("MOUNT_SOURCE_CHANGED: "+src+" cannot be inspected again: "+err.Error(), "a mount source it was audited with changed")
		case got.Resolved != want.Resolved || got.Kind != want.Kind:
			return drifted(fmt.Sprintf("MOUNT_SOURCE_CHANGED: %s is now %s %s, not %s %s", src, got.Kind, got.Resolved, want.Kind, want.Resolved),
				"a mount source it was audited with changed")
		case got.Untrusted != "":
			return drifted("MOUNT_SOURCE_CHANGED: "+src+" is no longer root's: "+got.Untrusted, "a mount source it was audited with changed")
		}
	}
	return s.checkHookBinaries(name, audit, now)
}

// checkSpecDirs rescans the spec directories before create for a spec of the
// profile's kind that the audit did not read. A successfully parsed one latches
// drift (SPEC_CHANGED). A file that cannot be read or parsed is only reported
// (never-started PROFILE_NOT_OFFERED, nothing latched): it may be another
// vendor's half-written file, and it cannot be resolved until it is whole, when
// the next attempt reads it again.
func (s *profileState) checkSpecDirs(name string, audit profileAudit, now time.Time) *Failure {
	kind := s.gpuKind(name)
	if audit.gpu == nil || kind == "" {
		return nil
	}
	found, err := specFilesOfKind(s.cdiDirs, kind)
	if err != nil {
		return Fail(ReasonProfileNotOffered,
			"profile %q cannot be checked now: a CDI spec file could not be read; the task runs again, "+
				"and on an Orker of its pool that offers the profile", name)
	}
	for _, path := range slices.Sorted(maps.Keys(found)) {
		if _, audited := audit.gpu.files[path]; !audited {
			s.setDrift(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED,
				path+" declares "+kind+" and was not in the audit set", now)
			return Fail(ReasonProfileDrifted,
				"profile %q drifted on this Orker: a CDI spec of its kind appeared after the audit; "+
					"it stays out of service until the Orker restarts", name)
		}
	}
	return nil
}

// gpuKind is the CDI kind of a declared gpu profile, or "".
func (s *profileState) gpuKind(name string) string {
	for _, p := range s.declared.List {
		if p.Name == name && p.Kind == orkstackv1.ProfileKind_PROFILE_KIND_GPU && p.GPU != nil {
			return p.GPU.CDIKind
		}
	}
	return ""
}

// useRuntime gives the state the engine's default-runtime question and what it
// answered at boot, which every later answer is compared with.
func (s *profileState) useRuntime(asker defaultRuntimeAsker, boot runtimeFacts) {
	s.runtime, s.bootRuntime = asker, boot
}

// runtimeDrift asks the engine's default runtime again and says how it differs
// from boot, or why it cannot say. changed is false for a question that could
// not be answered: that is no drift, an engine that did not answer.
func (s *profileState) runtimeDrift(ctx context.Context) (detail string, changed bool, err error) {
	if s.runtime == nil {
		return "", false, nil
	}
	now, err := s.runtime.DefaultRuntime(ctx)
	if err != nil {
		return "", false, err
	}
	if err := checkDefaultRuntime(now); err != nil {
		return DetailDefaultRuntimeRefused + ": " + err.Error(), true, nil
	}
	if now != s.bootRuntime {
		return fmt.Sprintf("%s: the default runtime is now %q at %q, not %q at %q", DetailDefaultRuntimeRefused,
			now.Name, now.Path, s.bootRuntime.Name, s.bootRuntime.Path), true, nil
	}
	return "", false, nil
}

// checkRuntime is the pre-create default-runtime check, once for the attempt's
// profiles: a runtime that is not what the boot audit found latches a drift on
// each (HOST_VIEW_CHANGED, the runtime being part of the host's view); one that
// cannot be asked is RUNTIME_STATE_UNKNOWN, with nothing latched.
func (s *profileState) checkRuntime(ctx context.Context, names []string, now time.Time) *Failure {
	ctx, cancel := context.WithTimeout(ctx, controlTimeout)
	defer cancel()
	detail, changed, err := s.runtimeDrift(ctx)
	switch {
	case err != nil:
		return FailCause(ReasonRuntimeStateUnknown, err,
			"the container engine did not say which runtime it uses, so the profiles %v cannot be held to runc or crun: %v", names, err)
	case changed:
		for _, name := range names {
			s.setDrift(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_VIEW_CHANGED, detail, now)
		}
		return Fail(ReasonProfileDrifted,
			"profiles %v drifted on this Orker: its container engine's default runtime changed; "+
				"they stay out of service until the Orker restarts", names)
	}
	return nil
}

// auditRuntimeOnce is the periodic default-runtime check: every profile that
// has not drifted latches when the runtime changed.
//
// An engine that cannot answer is no drift, but it is not silent either: the
// error is returned for the caller to log, so a daemon that stops answering
// shows in the Orker's log at every audit.
func (s *profileState) auditRuntimeOnce(ctx context.Context, now time.Time) (map[string]string, error) {
	detail, changed, err := s.runtimeDrift(ctx)
	if err != nil {
		return nil, err
	}
	if !changed {
		return nil, nil
	}
	drifted := map[string]string{}
	for _, p := range s.declared.List {
		if s.drifted(p.Name) {
			continue
		}
		drifted[p.Name] = detail
		s.setDrift(p.Name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_VIEW_CHANGED, detail, now)
	}
	return drifted, nil
}

// checkHookBinaries hashes a gpu profile's hook binaries again, at 3c and just
// before create: a binary that is another build than the audited one, or that
// cannot be read, latches drift (HOOK_BINARY_CHANGED).
//
// This reads and hashes the whole file where the design (§4.11) compares dev,
// inode, size and ctime of a descriptor held for the Orker's life. It is
// deliberately the stricter of the two: the binary is small (about 7 MiB, a
// few milliseconds to hash), and a full hash cannot be fooled by an edit that
// keeps the size and restores the ctime, which the stat comparison cannot see.
func (s *profileState) checkHookBinaries(name string, audit profileAudit, now time.Time) *Failure {
	hash := s.hookBinary
	if hash == nil {
		hash = hashHostHook
	}
	for _, p := range slices.Sorted(maps.Keys(audit.gpu.hooks)) {
		bin, err := hash(p)
		if err == nil && bin.SHA256 == audit.gpu.hooks[p] {
			continue
		}
		detail := p + " is not the build this Orker audited"
		if err != nil {
			detail = p + " cannot be audited again: " + err.Error()
		}
		s.setDrift(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOOK_BINARY_CHANGED, detail, now)
		return Fail(ReasonProfileDrifted,
			"profile %q drifted on this Orker: a hook binary it was audited with changed; "+
				"it stays out of service until the Orker restarts", name)
	}
	return nil
}

// statDevices compares every device of a profile with its audit. A device
// that is not there holds the profile unavailable (DEVICE_ABSENT) until it is
// back unchanged; one that differs latches a drift.
func (s *profileState) statDevices(name string, audit profileAudit, inspect func(string) (hostDevice, error), now time.Time) *Failure {
	for _, d := range slices.Concat(audit.devices, audit.gpuNodes) {
		detail, absent := deviceDrift(d, inspect)
		switch {
		case detail == "":
			continue
		case absent:
			s.setUnavailable(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_ABSENT, detail, now)
			// What differed names host paths and device numbers: it stays in
			// the cordon's detail, which only the control channel carries,
			// never in a failure a run's readers see.
			return Fail(ReasonProfileNotOffered,
				"profile %q is unavailable on this Orker: a device it passes is not there; it returns to service "+
					"once the device is back unchanged, and the task runs on an Orker of its pool that offers it", name)
		default:
			s.setDrift(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED, detail, now)
			return Fail(ReasonProfileDrifted,
				"profile %q drifted on this Orker: a device it passes differs from its boot audit; "+
					"it stays out of service until the Orker restarts", name)
		}
	}
	return nil
}

// deviceDrift is how a device differs from its audit now, or "", and whether
// the difference is that the device is not there: its path, or the node it
// leads to, does not exist.
func deviceDrift(d auditedDevice, inspect func(string) (hostDevice, error)) (string, bool) {
	now, err := inspect(d.Path)
	if err != nil {
		return fmt.Sprintf("%s: %v", d.Path, err), errors.Is(err, fs.ErrNotExist)
	}
	was := d.Host
	switch {
	case now.Resolved != was.Resolved:
		return fmt.Sprintf("%s now leads to %s, not %s", d.Path, now.Resolved, was.Resolved), false
	case !now.Char:
		return fmt.Sprintf("%s is no longer a character device", d.Path), false
	case now.Major != was.Major || now.Minor != was.Minor:
		return fmt.Sprintf("%s is now %d:%d, not %d:%d", d.Path, now.Major, now.Minor, was.Major, was.Minor), false
	case now.GID != was.GID:
		return fmt.Sprintf("%s now has gid %d, not %d", d.Path, now.GID, was.GID), false
	}
	return "", false
}

// deviceInspector is what the attempt asks of a device path: the host, or a test's
// seam (inspectDevice).
func (d *OCIDispatcher) deviceInspector() func(string) (hostDevice, error) {
	if d.inspectDevice != nil {
		return d.inspectDevice
	}
	return inspectHostDevice
}

// profileMemoEntries is the profiles a cached task's memo key covers: each it
// names, with the identity of the contract serving it here (in a uniform
// single-contract pool, the one its run pinned) and this Orker's revision.
// Nil for a task naming none.
func (d *OCIDispatcher) profileMemoEntries(in *orkstackv1.ExecuteTaskInput) []ProfileMemoEntry {
	if len(in.GetDevices()) == 0 {
		return nil
	}
	revisions := map[string]string{}
	if d.profiles != nil {
		for _, p := range d.profiles.declared.List {
			revisions[p.Name] = p.Revision
		}
	}
	entries := make([]ProfileMemoEntry, 0, len(in.GetDevices()))
	for _, ref := range in.GetDevices() {
		identity := ref.GetIdentityDigest()
		if d.profiles != nil {
			identity = d.profiles.memoIdentity(ref)
		}
		entries = append(entries, ProfileMemoEntry{Name: ref.GetName(), Identity: identity, Revision: revisions[ref.GetName()]})
	}
	return entries
}
