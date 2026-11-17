package orker

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
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
// §4.11): each device inspected on this host and classified against its
// /proc/devices, the host node behind it recorded, and each profile described
// with its structural identity. A device that is not of a class, or not there,
// refuses the boot, as do two profiles that would give one container path.
// inspect is what the host says a path is (inspectHostDevice).
func auditProfiles(declared Profiles, procDevices string, inspect func(string) (hostDevice, error)) (map[string]profileAudit, []*orkstackv1.OrkerProfile, error) {
	drivers := parseProcDevices(procDevices)
	total := 0
	for _, p := range declared.List {
		total += len(p.Devices)
	}
	if total > maxProbePaths {
		return nil, nil, fmt.Errorf("the profiles pass %d devices, more than the %d one host-view probe is asked about", total, maxProbePaths)
	}
	audits := map[string]profileAudit{}
	claimed := map[string]string{}
	found := map[string]hostDevice{}
	for _, p := range declared.List {
		if p.Kind != orkstackv1.ProfileKind_PROFILE_KIND_DEVICES {
			continue
		}
		audit := profileAudit{}
		for _, d := range p.Devices {
			if other, taken := claimed[d.ContainerPath]; taken {
				return nil, nil, fmt.Errorf("profiles %s and %s both pass a device at %s, which a task holding both could not be given",
					other, p.Name, d.ContainerPath)
			}
			claimed[d.ContainerPath] = p.Name
			host, err := inspect(d.Path)
			if err != nil {
				return nil, nil, fmt.Errorf("profile %s (%s): %w", p.Name, DetailDeviceClassRefused, err)
			}
			if _, err := classifyDevice(host, drivers); err != nil {
				return nil, nil, fmt.Errorf("profile %s (%s): %w", p.Name, DetailDeviceClassRefused, err)
			}
			found[d.Path] = host
			audit.devices = append(audit.devices, auditedDevice{ProfileDevice: d, Host: host})
		}
		audits[p.Name] = audit
	}
	// The identities from the same inspection: a device is classified once,
	// so the identity describes exactly what was audited.
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
		if a, ok := audits[d.GetName()]; ok {
			a.identity = d.GetIdentityDigest()
			audits[d.GetName()] = a
		}
	}
	return audits, described, nil
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
		for _, name := range slices.Sorted(maps.Keys(drifted)) {
			a.dispatcher.logger().Error("a device profile drifted: it is out of service on this Orker until it restarts",
				"profile", name, "reason", ReasonProfileDrifted, "detail", drifted[name])
		}
		a.hostViewOnce(ctx, root, now, present, good)
		timer.Reset(s.nextAuditDelay())
	}
}
