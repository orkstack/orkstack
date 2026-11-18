package orker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func gpuProfiles() Profiles {
	return Profiles{List: []Profile{{Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
		GPU: &GPUProfile{CDIKind: testKind, Units: []string{"amd"}}}}}
}

// gpuState is a profile state whose gpu profile was audited from a spec
// directory holding amdSpec, and that directory.
func gpuState(t *testing.T) (*profileState, string) {
	t.Helper()
	dir := specDir(t, map[string]string{"gpu.yaml": amdSpec})
	s := newProfileState(gpuProfiles(), "")
	audits, absent, err := auditGPUProfiles(s.declared, []string{dir}, testCDIHost())
	if err != nil || len(absent) != 0 {
		t.Fatalf("auditGPUProfiles = %v, absent %v", err, absent)
	}
	s.setAudited(audits)
	return s, dir
}

func gpuRef(s *profileState) []*orkstackv1.ProfileRef {
	a, _ := s.audit("amd")
	return []*orkstackv1.ProfileRef{{Name: "amd", IdentityDigest: a.identity}}
}

// TestAuditGPUProfiles_RecordsTheUnitsAndTheSpecs: the gpu audit records the
// structural identity, the audit set's digests and each unit's nodes, and a
// gpu profile passes 3c with no raw device mapping (its units come by CDI).
func TestAuditGPUProfiles_RecordsTheUnitsAndTheSpecs(t *testing.T) {
	s, _ := gpuState(t)
	a, ok := s.audit("amd")
	if !ok || a.identity == "" || a.gpu == nil || len(a.gpu.files) != 1 || len(a.gpuNodes) != 2 {
		t.Fatalf("audit = %+v", a)
	}
	devices, failure := s.gate(gpuRef(s), testCDIHost().inspect, time.Now())
	if failure != nil || len(devices) != 0 {
		t.Fatalf("gate = %v, %v; want a pass with no device mapping", devices, failure)
	}
}

// TestProfileGate_GPUSpecChangedIsDrift: a spec file of the audit set whose
// bytes differ from the audit is drift (SPEC_CHANGED), latched.
func TestProfileGate_GPUSpecChangedIsDrift(t *testing.T) {
	s, dir := gpuState(t)
	if err := os.WriteFile(filepath.Join(dir, "gpu.yaml"), []byte(strings.Replace(amdSpec, "rw", "r", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	_, failure := s.gate(gpuRef(s), testCDIHost().inspect, time.Now())
	if failure == nil || failure.Reason != ReasonProfileDrifted || !failure.NeverStarted {
		t.Fatalf("gate = %v, want a never-started PROFILE_DRIFTED", failure)
	}
	r := &orkstackv1.DescribeOrkerResult{}
	s.report(r)
	if got := r.GetProfileCordons(); len(got) != 1 || got[0].GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED {
		t.Fatalf("cordons = %v, want SPEC_CHANGED", got)
	}
}

// TestProfileGate_GPUSpecRemovedIsAbsent: a spec file of the audit set that
// is gone makes the profile unavailable (SPEC_ABSENT), not drifted.
func TestProfileGate_GPUSpecRemovedIsAbsent(t *testing.T) {
	s, dir := gpuState(t)
	if err := os.Remove(filepath.Join(dir, "gpu.yaml")); err != nil {
		t.Fatal(err)
	}
	_, failure := s.gate(gpuRef(s), testCDIHost().inspect, time.Now())
	if failure == nil || failure.Reason != ReasonProfileNotOffered || s.drifted("amd") ||
		!s.unavailableFor("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT) {
		t.Fatalf("gate = %v, drifted %v; want PROFILE_NOT_OFFERED and SPEC_ABSENT", failure, s.drifted("amd"))
	}
}

// TestAuditGPUProfiles_NoSpecIsAbsentNotRefused: a gpu profile with no spec
// at boot is reported absent, for SPEC_ABSENT, and refuses nothing.
func TestAuditGPUProfiles_NoSpecIsAbsentNotRefused(t *testing.T) {
	audits, absent, err := auditGPUProfiles(gpuProfiles(), []string{specDir(t, nil)}, testCDIHost())
	if err != nil || len(absent) != 1 || absent[0] != "amd" {
		t.Fatalf("auditGPUProfiles = %v, %v, %v", audits, absent, err)
	}
	if a := audits["amd"]; a.identity == "" || a.gpu != nil {
		t.Fatalf("an absent profile's audit = %+v, want its identity and no spec audit", a)
	}
}

// TestAuditGPUOnce_TheFullAuditEachPass: each audit pass reads and validates
// the gpu specs again (design §4.11). The same specs leave the profile alone;
// specs the rules now refuse, or that differ from the boot audit, latch
// SPEC_CHANGED; a spec gone holds it unavailable (SPEC_ABSENT) until it is
// back unchanged.
func TestAuditGPUOnce_TheFullAuditEachPass(t *testing.T) {
	now := time.Now()
	s, dir := gpuState(t)
	s.useCDI([]string{dir}, "")
	if drifted := s.auditGPUOnce(testCDIHost(), now); len(drifted) != 0 || s.unavailable("amd") {
		t.Fatalf("unchanged specs: drifted %v, unavailable %v", drifted, s.unavailable("amd"))
	}
	spec := filepath.Join(dir, "gpu.yaml")
	if err := os.Rename(spec, spec+".off"); err != nil {
		t.Fatal(err)
	}
	s.auditGPUOnce(testCDIHost(), now)
	absent := orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT
	if !s.unavailableFor("amd", absent) || s.drifted("amd") {
		t.Fatal("a spec gone did not hold the profile SPEC_ABSENT")
	}
	if err := os.Rename(spec+".off", spec); err != nil {
		t.Fatal(err)
	}
	s.auditGPUOnce(testCDIHost(), now)
	if s.unavailable("amd") {
		t.Fatal("the spec back unchanged did not end SPEC_ABSENT")
	}
	if err := os.WriteFile(spec, []byte(strings.ReplaceAll(amdSpec, "- 987", "- 0")), 0o644); err != nil {
		t.Fatal(err)
	}
	if drifted := s.auditGPUOnce(testCDIHost(), now); drifted["amd"] == "" || !s.drifted("amd") {
		t.Fatalf("a spec the rules refuse: drifted %v", drifted)
	}
}

// TestAuditGPUOnce_AdoptsASpecAbsentAtBoot: a gpu profile whose spec was
// absent at boot is adopted by the first pass that finds it: that audit
// starts its lifetime.
func TestAuditGPUOnce_AdoptsASpecAbsentAtBoot(t *testing.T) {
	dir := specDir(t, nil)
	s := newProfileState(gpuProfiles(), "")
	s.useCDI([]string{dir}, "")
	audits, absent, err := auditGPUProfiles(s.declared, []string{dir}, testCDIHost())
	if err != nil || len(absent) != 1 {
		t.Fatalf("boot: %v, absent %v", err, absent)
	}
	s.setAudited(audits)
	s.setUnavailable("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT, "none at boot", time.Now())
	if err := os.WriteFile(filepath.Join(dir, "gpu.yaml"), []byte(amdSpec), 0o644); err != nil {
		t.Fatal(err)
	}
	s.auditGPUOnce(testCDIHost(), time.Now())
	a, _ := s.audit("amd")
	if s.unavailable("amd") || a.gpu == nil || len(a.unitNodes) != 1 {
		t.Fatalf("the spec that appeared was not adopted: unavailable %v, audit %+v", s.unavailable("amd"), a)
	}
}
