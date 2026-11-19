package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// viewProbeRuntime is a container runtime whose probe container reports, for
// each device it is given, the node a daemon would make of it: the numbers in
// seen, keyed by host path, or the host's own where seen has none. It is a
// fake because a probe container seeing other devices than the Orker audited
// (a daemon on another host) cannot be produced on demand.
type viewProbeRuntime struct {
	layoutProbeRuntime
	seen     map[string]viewEntry
	label    string
	noLabel  bool
	runErr   error
	stdout   string
	local    error
	pullErr  error
	specs    []ContainerSpec
	hostView map[string]viewEntry
}

func (r *viewProbeRuntime) RunBootProbe(_ context.Context, spec ContainerSpec) (BootProbeRun, error) {
	r.specs = append(r.specs, spec)
	if r.runErr != nil {
		return BootProbeRun{}, r.runErr
	}
	if r.stdout != "" {
		return BootProbeRun{Container: "c1", Stdout: r.stdout}, nil
	}
	var view []viewEntry
	for _, d := range spec.Devices {
		v, ok := r.seen[d.HostPath]
		if !ok {
			v = r.hostView[d.HostPath]
		}
		v.Path = d.ContainerPath
		view = append(view, v)
	}
	var out bytes.Buffer
	if err := writeProbeView(&out, view); err != nil {
		return BootProbeRun{}, err
	}
	return BootProbeRun{Container: "c1", Stdout: out.String()}, nil
}

func (r *viewProbeRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{}, r.pullErr
}

func (r *viewProbeRuntime) StartBootProbe(context.Context, ContainerSpec) (BootProbeRun, error) {
	return BootProbeRun{}, errors.New("not used")
}
func (*viewProbeRuntime) bindProbeTimeout() time.Duration { return time.Second }
func (r *viewProbeRuntime) ImageLabel(_ context.Context, _, key string) (string, bool, error) {
	if key != LabelBuild || r.noLabel {
		return "", false, nil
	}
	return r.label, true, nil
}
func (r *viewProbeRuntime) LocalDaemon() error { return r.local }

const viewProbeImage = "registry.test/orker@sha256:" + testDigest

// viewProbeFixture is an Orker built from commit "c0ffee" with one devices
// profile, sensor, audited as the char node 188:199 of group 20.
func viewProbeFixture(t *testing.T, rt *viewProbeRuntime) *Activities {
	t.Helper()
	policy := DefaultSecurityPolicy()
	declared := Profiles{
		ProbeImage: viewProbeImage, AuditInterval: time.Minute, RecoveryInterval: time.Second,
		List: []Profile{{Name: "sensor", Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}},
	}
	if rt.label == "" && !rt.noLabel {
		rt.label = "c0ffee"
	}
	rt.hostView = map[string]viewEntry{"/dev/ttyUSB199": {Kind: viewChar, Major: 188, Minor: 199, GID: 20}}
	a := NewActivitiesWithOptions(nil, Options{
		Policy: &policy, Runtime: rt, OrkerID: "o1", WorkRoot: t.TempDir(), Profiles: declared, Build: "c0ffee",
	})
	a.profiles.setAudited(map[string]profileAudit{"sensor": {identity: "id", devices: []auditedDevice{{
		ProfileDevice: ProfileDevice{Path: "/dev/serial/by-id/x", ContainerPath: "/dev/sensor", Permissions: "rw"},
		Host:          hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199, GID: 20},
	}}}})
	return a
}

// TestProbeHostView judges the probe container's report against the audit:
// a native daemon passes, another view names the profile, and a probe that
// cannot be judged (not run, not this build's image, unreadable) holds the
// profile unavailable instead.
func TestProbeHostView(t *testing.T) {
	const (
		unavailable = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE
		mismatch    = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH
	)
	for _, tc := range []struct {
		name        string
		rt          *viewProbeRuntime
		build       string
		unavailable orkstackv1.ProfileCordonReason
		differs     string
		// ranNone: the image is judged before any container runs.
		ranNone bool
	}{
		{name: "a native daemon", rt: &viewProbeRuntime{}},
		{name: "another node", rt: &viewProbeRuntime{seen: map[string]viewEntry{"/dev/ttyUSB199": {Kind: viewChar, Major: 4, Minor: 64, GID: 20}}}, differs: "188:199"},
		{name: "another group", rt: &viewProbeRuntime{seen: map[string]viewEntry{"/dev/ttyUSB199": {Kind: viewChar, Major: 188, Minor: 199, GID: 5}}}, differs: "gid"},
		{name: "not there", rt: &viewProbeRuntime{seen: map[string]viewEntry{"/dev/ttyUSB199": {Error: "no such file"}}}, differs: "could not stat"},
		{name: "the image did not run", rt: &viewProbeRuntime{runErr: ErrProbeImageUnavailable}, unavailable: unavailable},
		{name: "the image could not be pulled", rt: &viewProbeRuntime{pullErr: errors.New("registry unavailable")}, unavailable: unavailable, ranNone: true},
		{name: "another build", rt: &viewProbeRuntime{label: "deadbeef"}, unavailable: mismatch, ranNone: true},
		{name: "no build label", rt: &viewProbeRuntime{noLabel: true}, unavailable: mismatch, ranNone: true},
		{name: "an Orker of no known build", rt: &viewProbeRuntime{}, build: "unknown", unavailable: mismatch, ranNone: true},
		{name: "an unreadable report", rt: &viewProbeRuntime{stdout: "hello"}, unavailable: unavailable},
		{name: "a report of what was not asked", rt: &viewProbeRuntime{
			stdout: `{"view":[{"path":"/dev/sensor","kind":"char","major":188,"minor":199,"gid":20},{"path":"/etc","kind":"dir"}]}`,
		}, unavailable: unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := viewProbeFixture(t, tc.rt)
			build := "c0ffee"
			if tc.build != "" {
				build = tc.build
			}
			f, err := a.dispatcher.probeHostView(t.Context(), tc.rt, a.probeRoot(), viewProbeImage, build, a.profiles.hostViewAudits())
			if err != nil {
				t.Fatalf("probeHostView: %v", err)
			}
			if f.Unavailable != tc.unavailable {
				t.Fatalf("unavailable = %v (%s), want %v", f.Unavailable, f.Detail, tc.unavailable)
			}
			if tc.ranNone && len(tc.rt.specs) != 0 {
				t.Fatalf("an image that was not judged its own ran %d containers", len(tc.rt.specs))
			}
			switch got := f.Differs["sensor"]; {
			case tc.differs == "" && got != "":
				t.Fatalf("differs = %q, want none", got)
			case tc.differs != "" && !strings.Contains(got, tc.differs):
				t.Fatalf("differs = %q, want it to name %q", got, tc.differs)
			}
		})
	}
}

// TestHostViewProbeSpec gives the probe container exactly the profiles'
// devices, the image's own entrypoint with `probe view` of their container
// paths, no network, and the Orker's label for its reap.
func TestHostViewProbeSpec(t *testing.T) {
	rt := &viewProbeRuntime{}
	a := viewProbeFixture(t, rt)
	if _, err := a.dispatcher.probeHostView(t.Context(), rt, a.probeRoot(), viewProbeImage, "c0ffee", a.profiles.hostViewAudits()); err != nil {
		t.Fatal(err)
	}
	if len(rt.specs) != 1 {
		t.Fatalf("ran %d probe containers, want 1", len(rt.specs))
	}
	spec := rt.specs[0]
	if spec.Image != viewProbeImage || spec.Network != NetworkNone {
		t.Fatalf("image %q network %q", spec.Image, spec.Network)
	}
	if want := []string{"probe", "view", "/dev/sensor"}; !slices.Equal(spec.Command, want) {
		t.Fatalf("command = %q, want %q", spec.Command, want)
	}
	if want := []DeviceMapping{{HostPath: "/dev/ttyUSB199", ContainerPath: "/dev/sensor", Permissions: "rw"}}; !slices.Equal(spec.Devices, want) {
		t.Fatalf("devices = %v, want %v", spec.Devices, want)
	}
	if spec.Labels["ork.orker"] != "o1" || spec.Labels["ork.task"] != hostViewProbeTaskID {
		t.Fatalf("labels = %v", spec.Labels)
	}
}

// TestBootHostView refuses a boot whose container sees other devices or
// whose CLI reaches another daemon, and boots, unavailable, where the probe
// could not judge.
func TestBootHostView(t *testing.T) {
	t.Run("another view refuses", func(t *testing.T) {
		rt := &viewProbeRuntime{seen: map[string]viewEntry{"/dev/ttyUSB199": {Kind: viewChar, Major: 4, Minor: 64, GID: 20}}}
		a := viewProbeFixture(t, rt)
		err := a.bootHostView(t.Context(), a.probeRoot())
		if err == nil || !strings.Contains(err.Error(), ReasonProfileInvalid) || !strings.Contains(err.Error(), DetailHostViewMismatch) ||
			!strings.Contains(err.Error(), "sensor") {
			t.Fatalf("bootHostView = %v", err)
		}
	})
	t.Run("a remote daemon refuses before any probe", func(t *testing.T) {
		rt := &viewProbeRuntime{local: errors.New("DOCKER_HOST names a daemon that is not a unix socket on this host")}
		a := viewProbeFixture(t, rt)
		err := a.bootHostView(t.Context(), a.probeRoot())
		if err == nil || !strings.Contains(err.Error(), DetailHostViewMismatch) || len(rt.specs) != 0 {
			t.Fatalf("bootHostView = %v after %d probes", err, len(rt.specs))
		}
	})
	t.Run("a probe that cannot judge boots unavailable", func(t *testing.T) {
		rt := &viewProbeRuntime{label: "deadbeef"}
		a := viewProbeFixture(t, rt)
		if err := a.bootHostView(t.Context(), a.probeRoot()); err != nil {
			t.Fatalf("bootHostView = %v", err)
		}
		if cordoned, _, _ := a.cordon.State(); !a.profiles.unavailable("sensor") || !cordoned {
			t.Fatal("the profile is not held unavailable, or the Orker not cordoned")
		}
		if got := a.profiles.nextAuditDelay(); got != time.Second {
			t.Fatalf("next check in %v, want recovery_interval", got)
		}
	})
	t.Run("a native daemon boots", func(t *testing.T) {
		rt := &viewProbeRuntime{}
		a := viewProbeFixture(t, rt)
		if err := a.bootHostView(t.Context(), a.probeRoot()); err != nil || a.profiles.unavailable("sensor") {
			t.Fatalf("bootHostView = %v", err)
		}
	})
}

// TestHostViewOnce is the periodic probe: an unavailable profile clears only
// after profileRecoveryChecks good checks in a row, and a view that changes
// later is a drift, which no good check clears.
func TestHostViewOnce(t *testing.T) {
	rt := &viewProbeRuntime{label: "deadbeef"}
	a := viewProbeFixture(t, rt)
	now := time.Unix(1_800_000_000, 0)
	present := []string{"sensor"}
	good := map[string]int{}
	a.hostViewOnce(t.Context(), a.probeRoot(), now, present, good)
	if good["sensor"] != 0 || !a.profiles.unavailable("sensor") {
		t.Fatalf("good = %v, unavailable = %v", good, a.profiles.unavailable("sensor"))
	}
	rt.label = "c0ffee"
	for i := 1; i < profileRecoveryChecks; i++ {
		a.hostViewOnce(t.Context(), a.probeRoot(), now, present, good)
		if good["sensor"] != i || !a.profiles.unavailable("sensor") {
			t.Fatalf("after %d good checks: good = %v, unavailable = %v", i, good, a.profiles.unavailable("sensor"))
		}
	}
	if a.hostViewOnce(t.Context(), a.probeRoot(), now, present, good); a.profiles.unavailable("sensor") || len(good) != 0 {
		t.Fatalf("after %d good checks the profile is still unavailable (good %v)", profileRecoveryChecks, good)
	}
	if cordoned, _, _ := a.cordon.State(); cordoned {
		t.Fatal("the Orker is still cordoned")
	}

	rt.seen = map[string]viewEntry{"/dev/ttyUSB199": {Kind: viewChar, Major: 4, Minor: 64, GID: 20}}
	a.hostViewOnce(t.Context(), a.probeRoot(), now, present, good)
	if !a.profiles.drifted("sensor") {
		t.Fatal("a changed view did not drift the profile")
	}
	var r orkstackv1.DescribeOrkerResult
	a.profiles.report(&r)
	if len(r.GetProfileCordons()) != 1 || r.GetProfileCordons()[0].GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOST_VIEW_CHANGED {
		t.Fatalf("cordons = %v", r.GetProfileCordons())
	}
	probes := len(rt.specs)
	rt.seen = nil
	a.hostViewOnce(t.Context(), a.probeRoot(), now, present, good)
	if !a.profiles.drifted("sensor") || len(rt.specs) != probes {
		t.Fatal("a drifted profile was probed again or cleared")
	}
}

// TestProfileAudit_UnpluggedDevice is a device unplugged after the boot audit:
// the profile is unavailable (DEVICE_ABSENT), never drifted. Plugged back as
// the same node it is available again after profileRecoveryChecks good
// checks; plugged back as another node it has drifted, until a restart.
func TestProfileAudit_UnpluggedDevice(t *testing.T) {
	audited := hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199, GID: 20}
	unplugged := func(string) (hostDevice, error) {
		return hostDevice{}, fmt.Errorf("resolve /dev/serial/by-id/x: %w", fs.ErrNotExist)
	}
	check := func(t *testing.T, a *Activities, inspect func(string) (hostDevice, error), good map[string]int) {
		t.Helper()
		drifted, present := a.profiles.auditOnce(inspect, time.Now())
		if len(drifted) != 0 {
			t.Fatalf("drifted = %v", drifted)
		}
		a.hostViewOnce(t.Context(), a.probeRoot(), time.Now(), present, good)
	}
	unplug := func(t *testing.T) (*Activities, map[string]int) {
		t.Helper()
		a := viewProbeFixture(t, &viewProbeRuntime{})
		good := map[string]int{}
		check(t, a, unplugged, good)
		var r orkstackv1.DescribeOrkerResult
		a.profiles.report(&r)
		if a.profiles.drifted("sensor") || len(r.GetProfileCordons()) != 1 ||
			r.GetProfileCordons()[0].GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_ABSENT {
			t.Fatalf("an unplugged device: cordons %v", r.GetProfileCordons())
		}
		refs := []*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: "id"}}
		if _, failure := a.profiles.gate(refs, unplugged, time.Now()); failure == nil || failure.Reason != ReasonProfileNotOffered {
			t.Fatalf("gate = %v, want %s", failure, ReasonProfileNotOffered)
		}
		return a, good
	}

	t.Run("back as the same node", func(t *testing.T) {
		a, good := unplug(t)
		same := func(string) (hostDevice, error) { return audited, nil }
		for i := 1; i < profileRecoveryChecks; i++ {
			check(t, a, same, good)
			if !a.profiles.unavailable("sensor") {
				t.Fatalf("available after %d good checks", i)
			}
		}
		check(t, a, unplugged, good)
		for i := 0; i < profileRecoveryChecks; i++ {
			check(t, a, same, good)
		}
		if a.profiles.unavailable("sensor") || a.profiles.drifted("sensor") {
			t.Fatal("the device is back unchanged and the profile is still out of service")
		}
	})
	t.Run("back as another node", func(t *testing.T) {
		a, _ := unplug(t)
		other := audited
		other.Resolved, other.Minor = "/dev/ttyUSB0", 0
		drifted, _ := a.profiles.auditOnce(func(string) (hostDevice, error) { return other, nil }, time.Now())
		if drifted["sensor"] == "" || !a.profiles.drifted("sensor") {
			t.Fatalf("a device back as another node: drifted %v", drifted)
		}
	})
}

// TestProfileGate_RefusesAnUnavailableProfile: a task polled before its
// Orker cordoned is not given devices no container has been shown to see.
func TestProfileGate_RefusesAnUnavailableProfile(t *testing.T) {
	rt := &viewProbeRuntime{}
	a := viewProbeFixture(t, rt)
	a.profiles.setUnavailable("sensor", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_MISMATCH, "x", time.Now())
	refs := []*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: "id"}}
	inspect := func(string) (hostDevice, error) {
		return hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 199, GID: 20}, nil
	}
	if _, failure := a.profiles.gate(refs, inspect, time.Now()); failure == nil || failure.Reason != ReasonProfileNotOffered {
		t.Fatalf("gate = %v, want %s", failure, ReasonProfileNotOffered)
	}
	if failure := a.profiles.recheck(refs, inspect, time.Now()); failure == nil || failure.Reason != ReasonProfileNotOffered {
		t.Fatalf("recheck = %v, want %s", failure, ReasonProfileNotOffered)
	}
}

// TestAuditProfiles_RefusesMoreDevicesThanOneProbe: a probe is asked about at
// most maxProbePaths paths, so more devices refuse the boot instead of
// leaving every profile unavailable for good.
func TestAuditProfiles_RefusesMoreDevicesThanOneProbe(t *testing.T) {
	var declared Profiles
	for i := 0; i <= maxProbePaths/16; i++ {
		p := Profile{Name: fmt.Sprintf("p%d", i), Kind: orkstackv1.ProfileKind_PROFILE_KIND_DEVICES}
		for j := 0; j < 16; j++ {
			p.Devices = append(p.Devices, ProfileDevice{Path: fmt.Sprintf("/dev/ttyUSB%d", i*16+j), ContainerPath: fmt.Sprintf("/dev/s%d-%d", i, j), Permissions: "r"})
		}
		declared.List = append(declared.List, p)
	}
	_, _, err := auditProfiles(declared, "Character devices:\n188 ttyUSB\n", func(string) (hostDevice, error) {
		t.Fatal("inspected a device")
		return hostDevice{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "more than") {
		t.Fatalf("auditProfiles = %v", err)
	}
}

// TestProfileGate_FailuresNameNoHostPath: what differed about a device names
// host paths and device numbers, so it stays in the cordon's detail (control
// channel only) and never reaches a run's readers in the attempt's failure.
func TestProfileGate_FailuresNameNoHostPath(t *testing.T) {
	refs := []*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: "id"}}
	for name, inspect := range map[string]func(string) (hostDevice, error){
		"absent": func(string) (hostDevice, error) {
			return hostDevice{}, fmt.Errorf("resolve /dev/serial/by-id/x: %w", fs.ErrNotExist)
		},
		"changed": func(string) (hostDevice, error) {
			return hostDevice{Resolved: "/dev/ttyUSB0", Char: true, Major: 188, Minor: 0, GID: 20}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			a := viewProbeFixture(t, &viewProbeRuntime{})
			_, failure := a.profiles.gate(refs, inspect, time.Now())
			if failure == nil || strings.Contains(failure.Message, "/dev/") || strings.Contains(failure.Message, "188") {
				t.Fatalf("gate = %+v; want a failure naming no host path or device number", failure)
			}
			if !strings.Contains(a.profiles.cordonDetail("sensor"), "/dev/") {
				t.Fatalf("the cordon's detail %q does not say what was observed", a.profiles.cordonDetail("sensor"))
			}
		})
	}
}
