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
	s := newProfileState(Profiles{})
	reported := func() *orkstackv1.DescribeOrkerResult {
		t.Helper()
		r := &orkstackv1.DescribeOrkerResult{}
		s.report(r)
		return r
	}

	if r := reported(); r.GetCordoned() || len(r.GetProfileCordons()) != 0 {
		t.Fatalf("a fresh state holds no cordon: %v", r)
	}

	s.setUnavailable("github-only", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_POSITIVE_CONTROL_FAILED, "api.github.com:443 refused", t0)
	r := reported()
	if !r.GetCordoned() || len(r.GetProfileCordons()) != 1 ||
		r.GetProfileCordons()[0].GetCause() != orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE {
		t.Fatalf("unavailable not reported: %v", r)
	}

	// A later observation of the same cause keeps the first instant.
	s.setUnavailable("github-only", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GATEWAY_ABSENT, "gone", t0.Add(time.Minute))
	if got := reported().GetProfileCordons()[0]; !got.GetSince().AsTime().Equal(t0) ||
		got.GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GATEWAY_ABSENT {
		t.Errorf("since moved or reason not updated: %v", got)
	}

	s.clearUnavailable("github-only")
	if r := reported(); r.GetCordoned() || len(r.GetProfileCordons()) != 0 {
		t.Fatalf("unavailable did not clear: %v", r)
	}

	s.setDrift("github-only", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_NETWORK_CHANGED, "network replaced", t0)
	// Nothing clears a drift, and an unavailable observation never replaces it.
	s.clearUnavailable("github-only")
	s.setUnavailable("github-only", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GATEWAY_ABSENT, "gone", t0)
	s.setDrift("github-only", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GATEWAY_CHANGED, "later", t0.Add(time.Hour))
	got := reported().GetProfileCordons()
	if len(got) != 1 || got[0].GetCause() != orkstackv1.ProfileCordon_CAUSE_DRIFT ||
		got[0].GetReason() != orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_NETWORK_CHANGED ||
		got[0].GetDetail() != "network replaced" {
		t.Fatalf("drift did not latch its first observation: %v", got)
	}
	if !s.drifted("github-only") || s.drifted("other") {
		t.Error("drifted reports the wrong profiles")
	}
}

func TestProfileState_ReportIsSortedAndCopied(t *testing.T) {
	t0 := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	s := newProfileState(Profiles{})
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
	// An Orker with no profiles adds no field to its answer, so an engine
	// built before profiles decodes it unchanged.
	r := &orkstackv1.DescribeOrkerResult{OrkerId: "o"}
	newProfileState(Profiles{}).report(r)
	if !proto.Equal(r, &orkstackv1.DescribeOrkerResult{OrkerId: "o"}) {
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
	for _, want := range []string{ReasonProfileInvalid, "amd", "github-only", "sensor"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Boot error %q does not name %q", err, want)
		}
	}
}
