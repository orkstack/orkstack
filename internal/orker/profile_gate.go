package orker

import (
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
	identity string
	devices  []auditedDevice
	// gpu is a gpu profile's CDI audit (cdi_audit.go): the audit set's
	// digests, re-hashed at 3c and before create. Nil for a devices profile,
	// and for a gpu profile whose spec was absent at boot.
	gpu *gpuSpecAudit
	// gpuNodes are a gpu profile's unit nodes, stat'ed as devices are; they
	// reach a container through CDI (dispatch_units.go), never as a mapping.
	gpuNodes []auditedDevice
	// unitNodes are gpuNodes by unit, in the profile's unit order, for the
	// unit probes.
	unitNodes [][]auditedDevice
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

// gate is step 3c: the devices the task's profiles pass, or why it cannot run
// here. inspect is what the host says a path is now (inspectHostDevice).
func (s *profileState) gate(refs []*orkstackv1.ProfileRef, inspect func(string) (hostDevice, error), now time.Time) ([]DeviceMapping, *Failure) {
	var devices []DeviceMapping
	for _, ref := range refs {
		name := ref.GetName()
		audit, ok := s.audit(name)
		switch {
		case !ok:
			return nil, Fail(ReasonProfileNotOffered,
				"profile %q is not offered by this Orker; the task runs on an Orker of its pool that offers it", name)
		case ref.GetIdentityDigest() == "":
			return nil, Fail(ReasonProfileChanged,
				"profile %q: the task carries no identity for it, so it cannot be held to what its run pinned", name)
		case ref.GetIdentityDigest() != audit.identity:
			return nil, Fail(ReasonProfileChanged,
				"profile %q is defined on this Orker otherwise than when the run was submitted; "+
					"every Orker of a pool must define it the same way", name)
		}
		if failure := s.checkDevices(name, audit, inspect, now); failure != nil {
			return nil, failure
		}
		for _, d := range audit.devices {
			devices = append(devices, DeviceMapping{HostPath: d.Host.Resolved, ContainerPath: d.ContainerPath, Permissions: d.Permissions})
		}
	}
	return devices, nil
}

// recheck is the device comparison again, just before the container is
// created, and again just before it is started.
func (s *profileState) recheck(refs []*orkstackv1.ProfileRef, inspect func(string) (hostDevice, error), now time.Time) *Failure {
	for _, ref := range refs {
		audit, ok := s.audit(ref.GetName())
		if !ok {
			return Fail(ReasonProfileNotOffered, "profile %q is not offered by this Orker", ref.GetName())
		}
		if failure := s.checkDevices(ref.GetName(), audit, inspect, now); failure != nil {
			return failure
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

// checkSpecBytes re-hashes a gpu profile's audit set (design §4.11, at 3c and
// just before create): a file that is gone, or that cannot be read, holds the
// profile unavailable (SPEC_ABSENT); one whose bytes differ latches a drift
// (SPEC_CHANGED).
func (s *profileState) checkSpecBytes(name string, audit profileAudit, now time.Time) *Failure {
	if audit.gpu == nil {
		return nil
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
			s.setDrift(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED,
				path+" differs from the bytes this Orker audited", now)
			return Fail(ReasonProfileDrifted,
				"profile %q drifted on this Orker: a CDI spec it was audited from changed; "+
					"it stays out of service until the Orker restarts", name)
		}
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

// profileGate is execute's STEP 3c. A task naming no profile passes with no
// device and no read of the host.
func (d *OCIDispatcher) profileGate(in *orkstackv1.ExecuteTaskInput, beat *Heartbeater) ([]DeviceMapping, *Failure) {
	if len(in.GetDevices()) == 0 {
		return nil, nil
	}
	beat.Phase("profiles")
	if d.profiles == nil {
		return nil, Fail(ReasonProfileNotOffered, "task %q names profiles, and this Orker offers none", in.GetTaskId())
	}
	return d.profiles.gate(in.GetDevices(), d.deviceInspector(), time.Now())
}

// profileRecheck is the device comparison just before the container is
// created, and again just before it is started.
func (d *OCIDispatcher) profileRecheck(in *orkstackv1.ExecuteTaskInput) *Failure {
	if len(in.GetDevices()) == 0 {
		return nil
	}
	if d.profiles == nil {
		return Fail(ReasonProfileNotOffered, "task %q names profiles, and this Orker offers none", in.GetTaskId())
	}
	return d.profiles.recheck(in.GetDevices(), d.deviceInspector(), time.Now())
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
// names, with the identity its run pinned and this Orker's revision. Nil for
// a task naming none.
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
		entries = append(entries, ProfileMemoEntry{Name: ref.GetName(), Identity: ref.GetIdentityDigest(), Revision: revisions[ref.GetName()]})
	}
	return entries
}
