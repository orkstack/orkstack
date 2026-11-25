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

	// A later observation of the same reason keeps its first instant and
	// takes the latest detail.
	s.setUnavailable("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE, "still not pulled", t0.Add(time.Minute))
	if got := reported().GetProfileCordons(); len(got) != 1 || !got[0].GetSince().AsTime().Equal(t0) || got[0].GetDetail() != "still not pulled" {
		t.Errorf("since moved or detail not updated: %v", got)
	}

	s.clearUnavailable("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_PROBE_IMAGE_UNAVAILABLE)
	if r := reported(); r.GetCordoned() || len(r.GetProfileCordons()) != 0 {
		t.Fatalf("unavailable did not clear: %v", r)
	}

	s.setDrift("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED, "device replaced", t0)
	// Nothing clears a drift, and an unavailable observation never replaces it.
	s.clearUnavailable("amd", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_ALL_UNITS_UNHEALTHY)
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

// TestProfileState_ReasonsClearIndependently: a profile holds every
// unavailable reason that stands, each cleared only by its own check, so
// clearing one never ends another (AGENTS.md §7). The cause stands while any
// reason does, is reported one entry per reason in value order, and keeps the
// instant its first reason was seen.
func TestProfileState_ReasonsClearIndependently(t *testing.T) {
	const (
		lockHeld = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD
		foreign  = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_FOREIGN_UNITS_HELD
		absent   = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_ABSENT
	)
	t0 := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	c := NewCordon()
	s := newProfileState(Profiles{}, "")
	s.attach(c)
	s.setUnavailable("amd", foreign, "container abc runs unit 0", t0)
	s.setUnavailable("amd", lockHeld, "held by pid 42 (orker)", t0.Add(time.Second))

	r := &orkstackv1.DescribeOrkerResult{}
	s.report(r)
	if got := r.GetProfileCordons(); len(got) != 2 || got[0].GetReason() != lockHeld || got[1].GetReason() != foreign {
		t.Fatalf("both reasons not reported in value order: %v", got)
	}

	// The lock is taken: its reason clears, the foreign container's stands,
	// and the Orker stays cordoned.
	s.clearUnavailable("amd", lockHeld)
	if !s.unavailable("amd") || !s.unavailableFor("amd", foreign) || s.unavailableFor("amd", lockHeld) {
		t.Fatal("clearing the lock reason ended the foreign-units reason")
	}
	if cordoned, _, _ := c.State(); !cordoned {
		t.Fatal("a standing reason left the Orker uncordoned")
	}
	// Clearing a reason that does not stand changes nothing.
	s.clearUnavailable("amd", absent)
	if !s.unavailableFor("amd", foreign) {
		t.Fatal("clearing an absent reason ended another")
	}
	s.clearUnavailable("amd", foreign)
	if s.unavailable("amd") {
		t.Fatal("the last reason cleared, still unavailable")
	}
	if cordoned, reason, _ := c.State(); cordoned {
		t.Fatalf("no reason stands, still cordoned: %q", reason)
	}
}
