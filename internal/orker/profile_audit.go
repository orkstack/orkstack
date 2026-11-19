package orker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// DetailDeviceClassRefused is the ORKER_PROFILE_INVALID detail of a device a
// profile names that is not of a device class this Orker passes, or that is
// not there to be classified (design §4.13).
const DetailDeviceClassRefused = "DEVICE_CLASS_REFUSED"

// procDevicesPath is where the host lists its drivers by major.
const procDevicesPath = "/proc/devices"

// auditProfiles is the boot audit of every declared devices profile (design
// §4.11): each host device inspected on this host and classified against its
// /proc/devices once, the host node behind it recorded, and each profile's
// contracts described with their structural identities. A device that is not
// of a class, or not there, refuses the boot, as does a container path two
// profiles would give: a task may hold both names, but only one contract of
// each, so two contracts of one name may share a path. inspect is what the
// host says a path is (inspectHostDevice).
func auditProfiles(declared Profiles, procDevices string, inspect func(string) (hostDevice, error)) (map[string]profileAudit, []*orkstackv1.OrkerProfile, error) {
	drivers := parseProcDevices(procDevices)
	// The probe is asked about each host device of each name once, and its
	// paths are one set across every name: every real container path is
	// taken before any path is given for the probe alone.
	asked := 0
	probePaths := map[string]bool{}
	for _, p := range declared.List {
		if p.Kind != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES {
			continue
		}
		contracts, _ := p.contractList()
		hosts := map[string]bool{}
		for _, c := range contracts {
			for _, d := range c.Devices {
				hosts[d.Path] = true
				probePaths[d.ContainerPath] = true
			}
		}
		asked += len(hosts)
	}
	if asked > maxProbePaths {
		return nil, nil, fmt.Errorf("the profiles pass %d host devices to their probe, more than the %d one host-view probe is asked about", asked, maxProbePaths)
	}
	audits := map[string]profileAudit{}
	claimed := map[string]string{}
	found := map[string]hostDevice{}
	for _, p := range declared.List {
		if p.Kind != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES {
			continue
		}
		contracts, def := p.contractList()
		audit := profileAudit{contracts: map[string]contractAudit{}, def: def}
		mine := map[string]bool{}
		for _, c := range contracts {
			for _, d := range c.Devices {
				if other, taken := claimed[d.ContainerPath]; taken && other != p.Name {
					return nil, nil, fmt.Errorf("profiles %s and %s both pass a device at %s, which a task holding both could not be given",
						other, p.Name, d.ContainerPath)
				}
				claimed[d.ContainerPath] = p.Name
			}
		}
		given := map[string]bool{}
		for _, c := range contracts {
			ca := contractAudit{accepts: c.Accepts}
			for _, d := range c.Devices {
				host, seen := found[d.Path]
				if !seen {
					var err error
					if host, err = inspect(d.Path); err != nil {
						return nil, nil, fmt.Errorf("profile %s (%s): %w", p.Name, DetailDeviceClassRefused, err)
					}
					if _, err := classifyDevice(host, drivers); err != nil {
						return nil, nil, fmt.Errorf("profile %s (%s): %w", p.Name, DetailDeviceClassRefused, err)
					}
					found[d.Path] = host
				}
				ca.devices = append(ca.devices, auditedDevice{ProfileDevice: d, Host: host})
				if !mine[d.Path] {
					mine[d.Path] = true
					probe := d
					probe.ContainerPath = probeContainerPath(d.ContainerPath, given, probePaths)
					audit.devices = append(audit.devices, auditedDevice{ProfileDevice: probe, Host: host})
				}
			}
			audit.contracts[c.Label] = ca
		}
		audits[p.Name] = audit
	}
	// The identities from the same inspection: a device is classified once,
	// so each identity describes exactly what was audited.
	classify := func(path string) (string, error) {
		host, ok := found[path]
		if !ok {
			return "", fmt.Errorf("%s was not audited", path)
		}
		return classifyDevice(host, drivers)
	}
	described, err := declared.Describe(classify)
	if err != nil {
		return nil, nil, err
	}
	for _, d := range described {
		a, ok := audits[d.GetName()]
		if !ok {
			continue
		}
		a.identity = d.GetIdentityDigest()
		for _, c := range d.GetContracts() {
			ca := a.contracts[c.GetLabel()]
			ca.identity = c.GetIdentityDigest()
			a.contracts[c.GetLabel()] = ca
		}
		audits[d.GetName()] = a
	}
	return audits, described, nil
}

// probeContainerPath is path for the host-view probe, or, when another host
// device of the same profile already takes it there (given), a path of the
// probe's own: two contracts of one name may give different devices one
// container path, and the probe sees every device of every name at once. A
// path of its own is none in taken, which holds every real container path of
// the audit and every path already given, so it is unique across names.
func probeContainerPath(path string, given, taken map[string]bool) string {
	if !given[path] {
		given[path] = true
		return path
	}
	out := path
	for n := 1; taken[out]; n++ {
		out = fmt.Sprintf("/dev/ork-probe-%d", n)
	}
	taken[out], given[out] = true, true
	return out
}

// auditGPUProfiles is the boot audit of every declared gpu profile: its CDI
// specs audited (cdi_audit.go) from dirs, its structural identity recorded.
// A profile with no spec of its kind is returned in absent, for SPEC_ABSENT,
// with its identity and no spec audit; any other refusal refuses the boot.
func auditGPUProfiles(declared Profiles, dirs []string, host cdiHost) (map[string]profileAudit, []string, error) {
	audits := map[string]profileAudit{}
	var absent []string
	for _, p := range declared.List {
		if p.Kind != orkstackv1.ProfileKind_PROFILE_KIND_GPU || p.GPU == nil {
			continue
		}
		identity, err := p.StructuralIdentity(nil)
		if err != nil {
			return nil, nil, err
		}
		audit := profileAudit{identity: identity}
		found, err := auditGPUSpecs(dirs, p.GPU.CDIKind, p.GPU.Units, host)
		switch {
		case errors.Is(err, errCDISpecAbsent):
			absent = append(absent, p.Name)
		case err != nil:
			return nil, nil, fmt.Errorf("profile %s: %w", p.Name, err)
		default:
			audit = gpuAuditOf(identity, found)
		}
		audit.contracts, audit.def = gpuContracts(p, identity)
		audits[p.Name] = audit
	}
	return audits, absent, nil
}

// gpuContracts is a gpu profile's one contract, for its audit: its identity
// is the profile's, since a gpu identity holds no device of a contract.
func gpuContracts(p Profile, identity string) (map[string]contractAudit, string) {
	c, _ := p.DefaultContract()
	return map[string]contractAudit{c.Label: {identity: identity, accepts: c.Accepts}}, c.Label
}

// gpuAuditOf is a gpu profile's audit record from what its CDI audit found.
func gpuAuditOf(identity string, found gpuSpecAudit) profileAudit {
	audit := profileAudit{identity: identity, gpu: &found}
	for _, unit := range found.units {
		var nodes []auditedDevice
		for _, n := range unit.nodes {
			nodes = append(nodes, auditedDevice{
				ProfileDevice: ProfileDevice{Path: n.hostPath, ContainerPath: n.path, Permissions: n.permissions},
				Host:          n.host,
			})
		}
		audit.unitNodes = append(audit.unitNodes, nodes)
		audit.gpuNodes = append(audit.gpuNodes, nodes...)
	}
	return audit
}

// auditGPUOnce is the periodic full audit of every gpu profile that has not
// drifted (design §4.11): its CDI specs read and validated again. A profile
// whose spec was absent at boot is adopted by the first audit that finds it,
// which starts its lifetime. Specs gone hold it unavailable (SPEC_ABSENT);
// back as audited, that ends. Specs the rules now refuse, or that differ from
// the audit, latch a drift (SPEC_CHANGED). It returns the drifts it found.
func (s *profileState) auditGPUOnce(host cdiHost, now time.Time) map[string]string {
	const (
		absent  = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT
		changed = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED
	)
	drifted := map[string]string{}
	for _, p := range s.declared.List {
		if p.Kind != orkstackv1.ProfileKind_PROFILE_KIND_GPU || p.GPU == nil || s.drifted(p.Name) {
			continue
		}
		old, ok := s.audit(p.Name)
		if !ok {
			continue
		}
		found, err := auditGPUSpecs(s.cdiDirs, p.GPU.CDIKind, p.GPU.Units, host)
		switch {
		case errors.Is(err, errCDISpecAbsent):
			s.setUnavailable(p.Name, absent, err.Error(), now)
		case err != nil:
			drifted[p.Name] = err.Error()
			s.setDrift(p.Name, changed, err.Error(), now)
		case old.gpu == nil:
			adopted := gpuAuditOf(old.identity, found)
			adopted.contracts, adopted.def = old.contracts, old.def
			s.adoptAudit(p.Name, adopted)
			s.clearUnavailable(p.Name, absent)
		case !sameGPUAudit(old, gpuAuditOf(old.identity, found)):
			detail := "the CDI specs or the nodes they pass differ from this Orker's audit"
			drifted[p.Name] = detail
			s.setDrift(p.Name, changed, detail, now)
		default:
			s.clearUnavailable(p.Name, absent)
		}
	}
	return drifted
}

// sameGPUAudit reports whether two audits of one gpu profile found the same
// spec bytes and the same nodes.
func sameGPUAudit(a, b profileAudit) bool {
	return a.gpu != nil && b.gpu != nil && maps.Equal(a.gpu.files, b.gpu.files) &&
		slices.EqualFunc(a.unitNodes, b.unitNodes, func(x, y []auditedDevice) bool { return slices.Equal(x, y) })
}

// adoptAudit records a gpu profile's first audit after boot.
func (s *profileState) adoptAudit(name string, audit profileAudit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.audits == nil {
		s.audits = map[string]profileAudit{}
	}
	s.audits[name] = audit
}

// useCDI sets what a gpu profile's audit reads: the trusted spec directories
// (orker.cdi.spec_dirs) and the container runtime's socket. Called before the
// boot audit.
func (s *profileState) useCDI(dirs []string, socket string) {
	s.cdiDirs = slices.Clone(dirs)
	s.cdiSocket = socket
}

// bootAuditProfiles runs the boot audit against this host and records it.
// With no profile declared it reads nothing.
func (s *profileState) bootAuditProfiles() error {
	if len(s.declared.List) == 0 {
		return nil
	}
	raw, err := os.ReadFile(procDevicesPath)
	if err != nil {
		return fmt.Errorf("profiles (%s): read %s: %w; device profiles need a Linux host", DetailDeviceClassRefused, procDevicesPath, err)
	}
	audits, described, err := auditProfiles(s.declared, string(raw), inspectHostDevice)
	if err != nil {
		return err
	}
	gpuAudits, absent, err := auditGPUProfiles(s.declared, s.cdiDirs, hostCDI(parseProcDevices(string(raw)), s.cdiSocket))
	if err != nil {
		return err
	}
	maps.Copy(audits, gpuAudits)
	for _, name := range absent {
		s.setUnavailable(name, orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT,
			fmt.Sprintf("no CDI spec of its kind in %s", strings.Join(s.cdiDirs, ", ")), time.Now())
	}
	s.setAudited(audits)
	s.setDescribed(described)
	return nil
}

// auditOnce is one periodic audit (design §4.11): every audited profile that
// has not drifted has its devices stat'ed again, held unavailable or not. A
// device that differs latches a drift and one that is not there holds the
// profile unavailable (statDevices). It returns the drifts this audit found,
// by profile and with what differed, and the profiles whose devices are all
// as audited.
func (s *profileState) auditOnce(inspect func(string) (hostDevice, error), now time.Time) (drifted map[string]string, present []string) {
	s.mu.Lock()
	names := slices.Sorted(maps.Keys(s.audits))
	s.mu.Unlock()
	for _, name := range names {
		if s.drifted(name) {
			continue
		}
		audit, _ := s.audit(name)
		failure := s.statDevices(name, audit, inspect, now)
		switch {
		case failure == nil:
			present = append(present, name)
		case failure.Reason == ReasonProfileDrifted:
			if drifted == nil {
				drifted = map[string]string{}
			}
			drifted[name] = s.cordonDetail(name)
		}
	}
	return drifted, present
}

// RunProfileAudit audits this Orker's profiles until ctx is done: every
// device stat'ed again and the host-view probe run again, every
// audit_interval, and every recovery_interval while a profile is held
// unavailable, so a healthy one clears after profileRecoveryChecks good
// checks. The boot audit has just run, so the first is one interval in. A
// drift is logged once, when it is found; the cordon it holds and the
// DescribeOrker answer carry it from then on.
func (a *Activities) RunProfileAudit(ctx context.Context) {
	s := a.profiles
	if s == nil || len(s.declared.List) == 0 {
		return
	}
	root := a.probeRoot()
	go a.awaitGPULock(ctx, gpuLockRetry)
	go a.runUnitRecovery(ctx, root, s.declared.ProbeImage, s.declared.RecoveryInterval)
	good := map[string]int{}
	timer := time.NewTimer(s.nextAuditDelay())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		now := time.Now()
		drifted, present := s.auditOnce(inspectHostDevice, now)
		if raw, err := os.ReadFile(procDevicesPath); err == nil {
			if drifted == nil {
				drifted = map[string]string{}
			}
			maps.Copy(drifted, s.auditGPUOnce(hostCDI(parseProcDevices(string(raw)), s.cdiSocket), now))
		}
		for _, name := range slices.Sorted(maps.Keys(drifted)) {
			a.dispatcher.logger().Error("a device profile drifted: it is out of service on this Orker until it restarts",
				"profile", name, "reason", ReasonProfileDrifted, "detail", drifted[name])
		}
		a.hostViewOnce(ctx, root, now, present, good)
		a.probeUnitsOnce(ctx, root, s.declared.ProbeImage)
		timer.Reset(s.nextAuditDelay())
	}
}
