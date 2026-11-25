package orker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

const (
	hookUnknown = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOOK_BUILD_UNKNOWN
	hookChanged = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_HOOK_BINARY_CHANGED
)

func nvidiaProfiles() Profiles {
	return Profiles{List: []Profile{{Name: "nv", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
		GPU: &GPUProfile{CDIKind: nvidiaKind, Units: []string{"all"}}}}}
}

// nvidiaState is a profile state whose nvidia profile was audited from the
// captured admitted spec, with the host used and the spec directory.
func nvidiaState(t *testing.T) (*profileState, cdiHost, string) {
	t.Helper()
	dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaAdmitted(t)})
	h := nvidiaTestHost(t)
	s := newProfileState(nvidiaProfiles(), "")
	s.useCDI([]string{dir}, "")
	audits, absent, err := auditGPUProfiles(s.declared, []string{dir}, h)
	if err != nil || len(absent) != 0 || audits["nv"].gpu == nil {
		t.Fatalf("auditGPUProfiles = %v, absent %v, err %v", audits, absent, err)
	}
	s.setAudited(audits)
	s.inspectMount = h.inspectMount
	return s, h, dir
}

func unknownHost(h cdiHost) cdiHost {
	h.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: strings.Repeat("cd", 32)}, nil }
	return h
}

// TestAuditGPUProfiles_UnknownHookBuildDisablesTheProfileAlone: a hook binary
// the table lacks refuses nothing at boot: the profile is audited with no
// spec audit (no units), and held unavailable HOOK_BUILD_UNKNOWN, naming the
// digest. The Orker is not cordoned and its other profiles are untouched.
func TestAuditGPUProfiles_UnknownHookBuildDisablesTheProfileAlone(t *testing.T) {
	dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaAdmitted(t)})
	s := newProfileState(nvidiaProfiles(), "")
	audits, absent, err := auditGPUProfiles(s.declared, []string{dir}, unknownHost(nvidiaTestHost(t)))
	if err != nil || len(absent) != 0 {
		t.Fatalf("auditGPUProfiles = %v, absent %v, err %v; an unknown build refuses nothing", audits, absent, err)
	}
	a := audits["nv"]
	if a.identity == "" || a.gpu != nil || !strings.Contains(a.hookUnknown, DetailHookBuildUnknown) ||
		!strings.Contains(a.hookUnknown, strings.Repeat("cd", 32)) {
		t.Fatalf("audit = %+v, want its identity, no spec audit and the HOOK_BUILD_UNKNOWN detail", a)
	}
	s.setAudited(audits)
	s.holdUnknownBuilds(audits, time.Now())
	if !s.unavailableFor("nv", hookUnknown) || s.drifted("nv") {
		t.Fatal("the profile is not held HOOK_BUILD_UNKNOWN")
	}
	r := &orkstackv1.DescribeOrkerResult{}
	s.report(r)
	if got := r.GetProfileCordons(); len(got) != 1 || got[0].GetReason() != hookUnknown ||
		got[0].GetCause() != orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE {
		t.Fatalf("cordons = %v", got)
	}
}

// TestAuditGPUProfiles_ARefusedHookRefusesTheBoot: every other hook finding is
// a refusal, as a spec's other edits are.
func TestAuditGPUProfiles_ARefusedHookRefusesTheBoot(t *testing.T) {
	dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaFixture(t, "nvidia-1.20.2-stock.yaml")})
	_, _, err := auditGPUProfiles(nvidiaProfiles(), []string{dir}, nvidiaTestHost(t))
	if err == nil || errors.Is(err, errHookBuildUnknown) || !strings.Contains(err.Error(), DetailCDIHookRefused) {
		t.Fatalf("auditGPUProfiles = %v, want %s", err, DetailCDIHookRefused)
	}
}

// TestAuditGPUOnce_HookBinaryChangedIsDrift: the periodic audit hashes the
// hook binary again: another build, listed or not, is drift
// (HOOK_BINARY_CHANGED), latched until a restart.
func TestAuditGPUOnce_HookBinaryChangedIsDrift(t *testing.T) {
	s, h, _ := nvidiaState(t)
	if drifted := s.auditGPUOnce(h, time.Now()); len(drifted) != 0 || s.unavailable("nv") {
		t.Fatalf("unchanged: drifted %v", drifted)
	}
	if drifted := s.auditGPUOnce(unknownHost(h), time.Now()); drifted["nv"] == "" || !s.drifted("nv") {
		t.Fatalf("an unlisted build: drifted %v", drifted)
	}
	r := &orkstackv1.DescribeOrkerResult{}
	s.report(r)
	if got := r.GetProfileCordons(); len(got) != 1 || got[0].GetReason() != hookChanged {
		t.Fatalf("cordons = %v, want HOOK_BINARY_CHANGED", got)
	}
}

// TestAuditGPUOnce_AnotherListedBuildIsDriftToo: a listed build is not the
// one audited, whatever the table says of it.
func TestAuditGPUOnce_AnotherListedBuildIsDriftToo(t *testing.T) {
	s, h, _ := nvidiaState(t)
	other := h
	other.hookBinary = func(string) (hostHook, error) {
		for _, b := range cdiHookTable {
			if b.Arch == "amd64" && b.BinarySHA256 != knownTestBuild(t).BinarySHA256 {
				return hostHook{SHA256: b.BinarySHA256}, nil
			}
		}
		t.Fatal("no second amd64 build")
		return hostHook{}, nil
	}
	if drifted := s.auditGPUOnce(other, time.Now()); drifted["nv"] == "" {
		t.Fatalf("drifted = %v", drifted)
	}
}

// TestAuditGPUOnce_AdoptsAnUnknownBuildOnceListed: a profile held
// HOOK_BUILD_UNKNOWN at boot stays so while the build is unlisted and is
// adopted, with the reason ended, by the first audit that finds a listed one.
func TestAuditGPUOnce_AdoptsAnUnknownBuildOnceListed(t *testing.T) {
	dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaAdmitted(t)})
	h := nvidiaTestHost(t)
	s := newProfileState(nvidiaProfiles(), "")
	s.useCDI([]string{dir}, "")
	audits, _, err := auditGPUProfiles(s.declared, []string{dir}, unknownHost(h))
	if err != nil {
		t.Fatal(err)
	}
	s.setAudited(audits)
	s.holdUnknownBuilds(audits, time.Now())
	if drifted := s.auditGPUOnce(unknownHost(h), time.Now()); len(drifted) != 0 || !s.unavailableFor("nv", hookUnknown) || s.drifted("nv") {
		t.Fatalf("still unlisted: drifted %v, unavailable %v", drifted, s.unavailableFor("nv", hookUnknown))
	}
	s.auditGPUOnce(h, time.Now())
	a, _ := s.audit("nv")
	if s.unavailable("nv") || a.gpu == nil || len(a.unitNodes) != 1 {
		t.Fatalf("a listed build was not adopted: unavailable %v, audit %+v", s.unavailable("nv"), a)
	}
}

// TestProfileGate_HookBinaryChangedIsDrift: before a container is created the
// gate hashes the hook binary again (design §4.11, at 3c and before create): a
// build other than the audited one latches drift, a binary that is gone does
// too (a hook that cannot run is a changed one), and the attempt never starts.
func TestProfileGate_HookBinaryChangedIsDrift(t *testing.T) {
	for name, hook := range map[string]func(string) (hostHook, error){
		"another build": func(string) (hostHook, error) { return hostHook{SHA256: strings.Repeat("ef", 32)}, nil },
		"a gone binary": func(p string) (hostHook, error) { return hostHook{}, errors.New(p + ": no such file") },
	} {
		t.Run(name, func(t *testing.T) {
			s, h, _ := nvidiaState(t)
			s.hookBinary = hook
			a, _ := s.audit("nv")
			refs := []*orkstackv1.ProfileRef{{Name: "nv", IdentityDigest: a.identity}}
			_, failure := s.gate(refs, h.inspect, time.Now())
			if failure == nil || failure.Reason != ReasonProfileDrifted || !failure.NeverStarted || !s.drifted("nv") {
				t.Fatalf("gate = %v, want a never-started PROFILE_DRIFTED", failure)
			}
			r := &orkstackv1.DescribeOrkerResult{}
			s.report(r)
			if got := r.GetProfileCordons(); len(got) != 1 || got[0].GetReason() != hookChanged {
				t.Fatalf("cordons = %v", got)
			}
		})
	}
	t.Run("the audited build passes", func(t *testing.T) {
		s, h, _ := nvidiaState(t)
		s.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
		a, _ := s.audit("nv")
		if _, failure := s.gate([]*orkstackv1.ProfileRef{{Name: "nv", IdentityDigest: a.identity}}, h.inspect, time.Now()); failure != nil {
			t.Fatalf("gate = %v", failure)
		}
	})
}

// TestParseGPU_AdmitsNVIDIAKinds: the profile parser no longer refuses
// nvidia.com kinds; a CDI device name of one is a unit's, `all` still never.
func TestParseGPU_AdmitsNVIDIAKinds(t *testing.T) {
	if err := checkCDIDevice("nvidia.com/gpu=0"); err != nil {
		t.Errorf("checkCDIDevice(nvidia.com/gpu=0) = %v", err)
	}
	if err := checkCDIDevice("nvidia.com/gpu=GPU-0a1b2c3d-0000-4000-8000-000000000000"); err != nil {
		t.Errorf("checkCDIDevice(uuid) = %v", err)
	}
	for _, bad := range []string{"nvidia.com/gpu=all", "nvidia.com/gpu", "nvidia.com/gpu=", "/dev/nvidia0"} {
		if err := checkCDIDevice(bad); err == nil {
			t.Errorf("checkCDIDevice(%q) = nil", bad)
		}
	}
}

func TestParseProfiles_AdmitsAnNVIDIAGPUProfile(t *testing.T) {
	got, err := ParseProfiles([]byte("profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest +
		"\n  x:\n    kind: gpu\n    cdi:\n      kind: nvidia.com/gpu\n      units:\n        - \"0\"\n        - GPU-0a1b2c3d-0000-4000-8000-000000000000\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.List) != 1 || got.List[0].GPU == nil || got.List[0].GPU.CDIKind != "nvidia.com/gpu" || len(got.List[0].GPU.Units) != 2 {
		t.Fatalf("profiles = %+v", got.List)
	}
	// `all` is still no unit, for NVIDIA as for every kind.
	if _, err := ParseProfiles([]byte("profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest +
		"\n  x:\n    kind: gpu\n    cdi:\n      kind: nvidia.com/gpu\n      units:\n        - all\n")); err == nil || !strings.Contains(err.Error(), "all") {
		t.Fatalf("units: all = %v", err)
	}
}

// TestNVIDIASpecsDrift: the seam the emulation test judges drift with says an
// identical audit is no drift, and a changed spec byte or hook build is.
func TestNVIDIASpecsDrift(t *testing.T) {
	good := knownTestBuild(t).BinarySHA256
	spec := nvidiaAdmitted(t)
	for name, tc := range map[string]struct {
		later, hook string
		drift       bool
	}{
		"nothing changed":        {spec, good, false},
		"a spec byte":            {spec + "# edited\n", good, true},
		"another hook build":     {spec, strings.Repeat("ab", 32), true},
		"a mount source differs": {strings.Replace(spec, "libcuda.so.999.88.77\n", "libcuda.so.999.88.78\n", 1), good, true},
	} {
		t.Run(name, func(t *testing.T) {
			later := tc.hook
			if later != good {
				// An unlisted build is refused by the audit, not compared.
				_, err := NVIDIASpecsDriftForTest(t, spec, tc.later, "amd64", good, later, "all")
				if !errors.Is(err, errHookBuildUnknown) {
					t.Fatalf("err = %v", err)
				}
				return
			}
			got, err := NVIDIASpecsDriftForTest(t, spec, tc.later, "amd64", good, later, "all")
			if err != nil || got != tc.drift {
				t.Fatalf("drift = %v, %v; want %v", got, err, tc.drift)
			}
		})
	}
}

func gateOf(t *testing.T, s *profileState, h cdiHost) *Failure {
	t.Helper()
	a, _ := s.audit("nv")
	_, failure := s.gate([]*orkstackv1.ProfileRef{{Name: "nv", IdentityDigest: a.identity}}, h.inspect, time.Now())
	return failure
}

func cordonOf(t *testing.T, s *profileState) *orkstackv1.ProfileCordon {
	t.Helper()
	r := &orkstackv1.DescribeOrkerResult{}
	s.report(r)
	if got := r.GetProfileCordons(); len(got) == 1 {
		return got[0]
	}
	t.Fatalf("cordons = %v, want one", r.GetProfileCordons())
	return nil
}

// TestProfileGate_MountSourceChangedIsDrift: before create the gate inspects
// every recorded mount source again: another resolution, another kind, or a
// source that is no longer root's latches drift (SPEC_CHANGED, detail
// MOUNT_SOURCE_CHANGED), as does a source that cannot be inspected.
func TestProfileGate_MountSourceChangedIsDrift(t *testing.T) {
	for name, change := range map[string]func(string, hostMount) (hostMount, error){
		"resolves elsewhere": func(p string, m hostMount) (hostMount, error) { m.Resolved = "/usr/lib/other.so"; return m, nil },
		"another kind":       func(p string, m hostMount) (hostMount, error) { m.Kind = mountDir; return m, nil },
		"no longer root's": func(p string, m hostMount) (hostMount, error) {
			m.Untrusted = "owned by uid 1000, not root"
			return m, nil
		},
		"gone": func(p string, m hostMount) (hostMount, error) { return hostMount{}, errors.New(p + ": no such file") },
	} {
		t.Run(name, func(t *testing.T) {
			s, h, _ := nvidiaState(t)
			s.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
			s.inspectMount = func(p string) (hostMount, error) { return change(p, mustMount(t, h, p)) }
			failure := gateOf(t, s, h)
			if failure == nil || failure.Reason != ReasonProfileDrifted || !failure.NeverStarted {
				t.Fatalf("gate = %v, want a never-started PROFILE_DRIFTED", failure)
			}
			c := cordonOf(t, s)
			if c.GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED || !strings.Contains(c.GetDetail(), "MOUNT_SOURCE_CHANGED") {
				t.Fatalf("cordon = %v", c)
			}
		})
	}
	t.Run("unchanged passes", func(t *testing.T) {
		s, h, _ := nvidiaState(t)
		s.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
		s.inspectMount = h.inspectMount
		if failure := gateOf(t, s, h); failure != nil {
			t.Fatalf("gate = %v", failure)
		}
	})
}

func mustMount(t *testing.T, h cdiHost, p string) hostMount {
	t.Helper()
	m, err := h.inspectMount(p)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestProfileGate_ASpecOfTheKindAppearingIsDrift: a file of the profile's kind
// added to a spec directory after the boot audit is caught before the next
// create, not only by the periodic audit; so is a file that no longer parses.
func TestProfileGate_ASpecOfTheKindAppearingIsDrift(t *testing.T) {
	preCreate := func(t *testing.T, s *profileState, h cdiHost) *Failure {
		t.Helper()
		a, _ := s.audit("nv")
		return s.recheck(context.Background(), []*orkstackv1.ProfileRef{{Name: "nv", IdentityDigest: a.identity}}, h.inspect, time.Now(), true)
	}
	t.Run("a second file of the kind", func(t *testing.T) {
		s, h, dir := nvidiaState(t)
		s.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
		body := "cdiVersion: 0.5.0\nkind: nvidia.com/gpu\ndevices:\n  - name: extra\n    containerEdits:\n      deviceNodes:\n        - {path: /dev/nvidia1, hostPath: /dev/nvidia1}\n"
		if err := os.WriteFile(filepath.Join(dir, "later.yaml"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		failure := preCreate(t, s, h)
		if failure == nil || failure.Reason != ReasonProfileDrifted || !failure.NeverStarted || !s.drifted("nv") {
			t.Fatalf("pre-create = %v, want a never-started PROFILE_DRIFTED", failure)
		}
		if c := cordonOf(t, s); c.GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED {
			t.Fatalf("cordon = %v", c)
		}
		// 3c and the recheck before the start do not scan (once per attempt).
		s2, h2, dir2 := nvidiaState(t)
		s2.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
		if err := os.WriteFile(filepath.Join(dir2, "later.yaml"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		if failure := gateOf(t, s2, h2); failure != nil {
			t.Fatalf("3c rescanned the directories: %v", failure)
		}
	})
	// A file that cannot be read or parsed, such as another vendor's
	// half-written spec, is no drift: the attempt is a never-started
	// PROFILE_NOT_OFFERED, nothing is latched, and once the file is whole the
	// next attempt passes.
	t.Run("another vendor's half-written file", func(t *testing.T) {
		s, h, dir := nvidiaState(t)
		s.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
		file := filepath.Join(dir, "other.yaml")
		if err := os.WriteFile(file, []byte("cdiVersion: 0.5.0\nkind: other.example/gpu\ndevices:\n  - name: x\n    containerEdits:\n      env: [A"), 0o644); err != nil {
			t.Fatal(err)
		}
		failure := preCreate(t, s, h)
		if failure == nil || failure.Reason != ReasonProfileNotOffered || !failure.NeverStarted || s.drifted("nv") || s.unavailable("nv") {
			t.Fatalf("pre-create = %v (drifted %v), want a never-started PROFILE_NOT_OFFERED with nothing latched", failure, s.drifted("nv"))
		}
		whole := "cdiVersion: 0.5.0\nkind: other.example/gpu\ndevices:\n  - name: x\n    containerEdits:\n      env: [A=b]\n"
		if err := os.WriteFile(file, []byte(whole), 0o644); err != nil {
			t.Fatal(err)
		}
		if failure := preCreate(t, s, h); failure != nil {
			t.Fatalf("the file whole: %v", failure)
		}
	})
	// A file of another kind is none of the profile's business.
	s, h, dir := nvidiaState(t)
	s.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: knownTestBuild(t).BinarySHA256}, nil }
	s.inspectMount = h.inspectMount
	other := "cdiVersion: 0.5.0\nkind: other.example/gpu\ndevices:\n  - name: x\n    containerEdits:\n      env: [A=b]\n"
	if err := os.WriteFile(filepath.Join(dir, "other.yaml"), []byte(other), 0o644); err != nil {
		t.Fatal(err)
	}
	if failure := gateOf(t, s, h); failure != nil {
		t.Fatalf("gate = %v", failure)
	}
}

// TestAuditGPUOnce_DriftLabels: a spec edited to add a hook behind an
// unlisted build is spec drift (SPEC_CHANGED); a hook binary that cannot be
// held to root's after boot, or that is another build, is HOOK_BINARY_CHANGED.
func TestAuditGPUOnce_DriftLabels(t *testing.T) {
	changed := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED
	t.Run("a hook added to a hookless spec, behind an unlisted build", func(t *testing.T) {
		dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaFixture(t, "nvidia-1.20.2-hookless.yaml")})
		h := nvidiaTestHost(t)
		s := newProfileState(nvidiaProfiles(), "")
		s.useCDI([]string{dir}, "")
		audits, _, err := auditGPUProfiles(s.declared, []string{dir}, h)
		if err != nil || audits["nv"].gpu == nil {
			t.Fatalf("boot: %v", err)
		}
		s.setAudited(audits)
		if err := os.WriteFile(filepath.Join(dir, "nvidia.yaml"), []byte(nvidiaAdmitted(t)), 0o644); err != nil {
			t.Fatal(err)
		}
		if drifted := s.auditGPUOnce(unknownHost(h), time.Now()); drifted["nv"] == "" {
			t.Fatal("no drift")
		}
		if c := cordonOf(t, s); c.GetReason() != changed {
			t.Fatalf("reason = %v, want SPEC_CHANGED", c.GetReason())
		}
	})
	t.Run("a binary that is no longer root's", func(t *testing.T) {
		s, h, _ := nvidiaState(t)
		bad := h
		bad.hookBinary = func(p string) (hostHook, error) { return hostHook{}, errors.New(p + " is owned by uid 1000, not root") }
		if drifted := s.auditGPUOnce(bad, time.Now()); drifted["nv"] == "" {
			t.Fatal("no drift")
		}
		if c := cordonOf(t, s); c.GetReason() != hookChanged {
			t.Fatalf("reason = %v, want HOOK_BINARY_CHANGED", c.GetReason())
		}
	})
	t.Run("an unreadable binary where none was audited is spec drift, and no panic", func(t *testing.T) {
		dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaAdmitted(t)})
		h := nvidiaTestHost(t)
		s := newProfileState(nvidiaProfiles(), "")
		s.useCDI([]string{dir}, "")
		audits, _, _ := auditGPUProfiles(s.declared, []string{dir}, unknownHost(h)) // unlisted at boot: no spec audit
		s.setAudited(audits)
		bad := h
		bad.hookBinary = func(p string) (hostHook, error) { return hostHook{}, errors.New("gone") }
		if drifted := s.auditGPUOnce(bad, time.Now()); drifted["nv"] == "" {
			t.Fatal("no drift")
		}
	})
}

// TestAuditGPUProfiles_HardRefusalsBeatAnUnlistedBuild: the whole audit
// finishes before HOOK_BUILD_UNKNOWN is reported, so a refusal in a later unit
// is the answer, not the unlisted build; and adopting a listed build later
// latches no drift on the next pass.
func TestAuditGPUProfiles_HardRefusalsBeatAnUnlistedBuild(t *testing.T) {
	spec := `cdiVersion: 0.5.0
kind: nvidia.com/gpu
devices:
  - name: "0"
    containerEdits:
      deviceNodes:
        - {path: /dev/nvidia0, hostPath: /dev/nvidia0, permissions: rwm}
      hooks:
        - {hookName: createContainer, path: /usr/bin/nvidia-cdi-hook, args: [nvidia-cdi-hook, disable-device-node-modification], env: [NVIDIA_CTK_DEBUG=false]}
  - name: "1"
    containerEdits:
      deviceNodes:
        - {path: /dev/nvidia1, hostPath: /dev/nvidia1, permissions: rwm}
      env: [LD_PRELOAD=/x.so]
`
	h := unknownHost(nvidiaTestHost(t))
	_, err := auditGPUSpecs([]string{specDir(t, map[string]string{"n.yaml": spec})}, nvidiaKind, []string{"0", "1"}, h)
	if err == nil || errors.Is(err, errHookBuildUnknown) || !strings.Contains(err.Error(), DetailCDIEditRefused) {
		t.Fatalf("auditGPUSpecs = %v, want the later unit's %s, not HOOK_BUILD_UNKNOWN", err, DetailCDIEditRefused)
	}
	// Without the refusal the unlisted build is what is reported.
	clean := strings.Replace(spec, "      env: [LD_PRELOAD=/x.so]\n", "", 1)
	if _, err := auditGPUSpecs([]string{specDir(t, map[string]string{"n.yaml": clean})}, nvidiaKind, []string{"0", "1"}, h); !errors.Is(err, errHookBuildUnknown) {
		t.Fatalf("auditGPUSpecs = %v, want HOOK_BUILD_UNKNOWN", err)
	}

	// Adopted later: the next pass is no drift.
	s, host, _ := nvidiaState(t)
	_ = s
	dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaAdmitted(t)})
	st := newProfileState(nvidiaProfiles(), "")
	st.useCDI([]string{dir}, "")
	audits, _, err := auditGPUProfiles(st.declared, []string{dir}, unknownHost(host))
	if err != nil {
		t.Fatal(err)
	}
	st.setAudited(audits)
	st.holdUnknownBuilds(audits, time.Now())
	st.auditGPUOnce(host, time.Now()) // adopts
	if drifted := st.auditGPUOnce(host, time.Now()); len(drifted) != 0 || st.drifted("nv") {
		t.Fatalf("the pass after the adoption drifted: %v", drifted)
	}
}

// TestActivities_AuditCDISpecDirs: the engine's CDI spec directories must all
// be held by orker.cdi.spec_dirs; a gpu profile asks, and an engine that cannot
// report them is held to the documented defaults.
func TestActivities_AuditCDISpecDirs(t *testing.T) {
	type dirs = struct {
		dirs     []string
		reported bool
		err      error
	}
	for name, tc := range map[string]struct {
		engine  dirs
		ours    []string
		profile Profiles
		want    string
	}{
		"equal":                              {dirs{[]string{"/etc/cdi", "/var/run/cdi"}, true, nil}, []string{"/etc/cdi", "/var/run/cdi"}, gpuProfiles(), ""},
		"ours a superset":                    {dirs{[]string{"/etc/cdi"}, true, nil}, []string{"/etc/cdi", "/var/run/cdi"}, gpuProfiles(), ""},
		"the engine reads one we do not":     {dirs{[]string{"/etc/cdi", "/srv/cdi"}, true, nil}, []string{"/etc/cdi", "/var/run/cdi"}, gpuProfiles(), DetailCDISpecDirsMismatch},
		"unreported defaults, ours narrower": {dirs{[]string{"/etc/cdi", "/var/run/cdi"}, false, nil}, []string{"/etc/cdi"}, gpuProfiles(), DetailCDISpecDirsMismatch},
		"cannot ask":                         {dirs{nil, false, errors.New("daemon gone")}, []string{"/etc/cdi"}, gpuProfiles(), DetailCDISpecDirsMismatch},
		"no gpu profile":                     {dirs{[]string{"/srv/cdi"}, true, nil}, []string{"/etc/cdi"}, Profiles{}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			a := newCordonActivities(t, t.TempDir())
			a.profiles = newProfileState(tc.profile, "")
			a.dispatcher.Policy.CDISpecDirs = tc.ours
			a.dispatcher.Runtime = specDirsRuntime{dirs: tc.engine.dirs, reported: tc.engine.reported, err: tc.engine.err}
			err := a.auditCDISpecDirs(context.Background())
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("= %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), ReasonProfileInvalid)):
				t.Fatalf("= %v, want %s", err, tc.want)
			}
		})
	}
	a := newCordonActivities(t, t.TempDir())
	a.profiles = newProfileState(gpuProfiles(), "")
	if err := a.auditCDISpecDirs(context.Background()); err == nil || !strings.Contains(err.Error(), DetailCDISpecDirsMismatch) {
		t.Errorf("a runtime that cannot be asked: %v", err)
	}
}

type specDirsRuntime struct {
	ContainerRuntime
	dirs     []string
	reported bool
	err      error
}

func (r specDirsRuntime) CDISpecDirs(context.Context) ([]string, bool, error) {
	return r.dirs, r.reported, r.err
}
