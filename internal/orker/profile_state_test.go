package orker

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestProfileState_DriftLatchesAndUnavailableClears(t *testing.T) {
	t0 := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	s := newProfileState(Profiles{}, "")
	reported := func() *orkstackv1.DescribeOrkerResult {
		t.Helper()
		r := &orkstackv1.DescribeOrkerResult{}
		s.report(r)
		return r
	}

	if r := reported(); r.GetCordoned() || len(r.GetProfileCordons()) != 0 {
		t.Fatalf("a fresh state holds no cordon: %v", r)
	}

	s.setUnavailable("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE, "probe image not pulled", t0)
	r := reported()
	if !r.GetCordoned() || len(r.GetProfileCordons()) != 1 ||
		r.GetProfileCordons()[0].GetCause() != orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE {
		t.Fatalf("unavailable not reported: %v", r)
	}

	// A later observation of the same cause keeps the first instant.
	s.setUnavailable("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY, "gone", t0.Add(time.Minute))
	if got := reported().GetProfileCordons()[0]; !got.GetSince().AsTime().Equal(t0) ||
		got.GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY {
		t.Errorf("since moved or reason not updated: %v", got)
	}

	s.clearUnavailable("amd")
	if r := reported(); r.GetCordoned() || len(r.GetProfileCordons()) != 0 {
		t.Fatalf("unavailable did not clear: %v", r)
	}

	s.setDrift("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED, "device replaced", t0)
	// Nothing clears a drift, and an unavailable observation never replaces it.
	s.clearUnavailable("amd")
	s.setUnavailable("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY, "gone", t0)
	s.setDrift("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED, "later", t0.Add(time.Hour))
	got := reported().GetProfileCordons()
	if len(got) != 1 || got[0].GetCause() != orkstackv1.ProfileCordon_CAUSE_DRIFT ||
		got[0].GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED ||
		got[0].GetDetail() != "device replaced" {
		t.Fatalf("drift did not latch its first observation: %v", got)
	}
	if !s.drifted("amd") || s.drifted("other") {
		t.Error("drifted reports the wrong profiles")
	}
}

func TestProfileState_ReportIsSortedAndCopied(t *testing.T) {
	t0 := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	s := newProfileState(Profiles{}, "")
	s.setUnavailable("b", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_ABSENT, "", t0)
	s.setDrift("a", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_SPEC_CHANGED, "", t0)
	s.setDescribed([]*orkstackv1.OrkerProfile{{Name: "a"}, {Name: "b"}})

	r := &orkstackv1.DescribeOrkerResult{}
	s.report(r)
	if r.GetProfileCordons()[0].GetProfile() != "a" || r.GetProfileCordons()[1].GetProfile() != "b" {
		t.Fatalf("cordons not sorted by profile: %v", r.GetProfileCordons())
	}
	// The answer is a copy: changing it changes nothing the state holds.
	r.GetProfiles()[0].Name = "mutated"
	r.GetProfileCordons()[0].Detail = "mutated"
	again := &orkstackv1.DescribeOrkerResult{}
	s.report(again)
	if again.GetProfiles()[0].GetName() != "a" || again.GetProfileCordons()[0].GetDetail() != "" {
		t.Fatalf("report shares state: %v", again)
	}
}

func TestProfileState_NothingDeclaredReportsNothing(t *testing.T) {
	// An Orker with no profiles adds only the kinds its build implements,
	// a capability an engine built before profiles ignores: no profile, no
	// cause, no cordon.
	r := &orkstackv1.DescribeOrkerResult{OrkerId: "o"}
	newProfileState(Profiles{}, "").report(r)
	want := &orkstackv1.DescribeOrkerResult{OrkerId: "o", ProfileKindsSupported: SupportedProfileKinds()}
	if !proto.Equal(r, want) {
		t.Fatalf("report added fields: %v", r)
	}
}

func TestBoot_RefusesUnimplementedProfileKind(t *testing.T) {
	declared, err := ParseProfiles([]byte(validProfilesDoc))
	if err != nil {
		t.Fatal(err)
	}
	policy := DefaultSecurityPolicy()
	a := NewActivitiesWithOptions(nil, Options{
		Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o1", WorkRoot: t.TempDir(), Profiles: declared,
	})
	err = a.Boot(t.Context())
	if err == nil {
		t.Fatal("Boot accepted profiles of kinds this build does not implement")
	}
	for _, want := range []string{ReasonProfileInvalid, "amd (kind gpu)"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Boot error %q does not name %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "sensor (kind") {
		t.Fatalf("Boot refused the devices kind, which this build implements: %v", err)
	}
}

// TestBoot_RefusesADeviceOfNoClass: a devices profile naming a node that is
// not of a device class refuses the boot, before anything polls.
func TestBoot_RefusesADeviceOfNoClass(t *testing.T) {
	declared, err := ParseProfiles([]byte("profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest +
		"\n  sensor:\n    kind: devices\n    devices:\n      - path: /dev/ttyUSB0\n        container_path: /dev/s\n        permissions: r\n"))
	if err != nil {
		t.Fatal(err)
	}
	// /dev/ttyUSB0 is absent on most hosts and, where present, a real
	// adapter: either way the test reads the refusal, never a pass, so it
	// points the audit at /dev/null through the declared path instead.
	declared.List[0].Devices[0].Path = "/dev/null"
	policy := DefaultSecurityPolicy()
	a := NewActivitiesWithOptions(nil, Options{
		Policy: &policy, Runtime: &layoutProbeRuntime{}, OrkerID: "o1", WorkRoot: t.TempDir(), Profiles: declared,
	})
	err = a.Boot(t.Context())
	if err == nil || !strings.Contains(err.Error(), ReasonProfileInvalid) || !strings.Contains(err.Error(), DetailDeviceClassRefused) {
		t.Fatalf("Boot = %v; want %s naming %s", err, ReasonProfileInvalid, DetailDeviceClassRefused)
	}
}
