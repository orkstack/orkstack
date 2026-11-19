package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestResolveConsent: a digest given is sent as it is; --consent with no
// value shows what would be consented to and its digest and sends that
// digest only on a yes at a terminal; anywhere else it needs a value.
func TestResolveConsent(t *testing.T) {
	asks := &orkstackv1.AsksFor{Digest: "c1:" + strings.Repeat("a", 64), Audiences: []string{"sts"}, Grants: []string{"deployer"}}
	given := &orkstackv1.ConsentView{Digest: "c1:" + strings.Repeat("b", 64), Audiences: []string{"sts"}}
	for _, tc := range []struct {
		name     string
		flag     string
		terminal bool
		in       string
		changes  bool
		asks     *orkstackv1.AsksFor
		want     string
		err      string
		shows    []string
	}{
		{name: "a digest given", flag: "c1:x", want: "c1:x"},
		{name: "none", flag: "", want: ""},
		{name: "asked, yes", flag: consentAsk, terminal: true, in: "yes\n", asks: asks, want: asks.GetDigest(),
			shows: []string{"sts", "deployer", asks.GetDigest(), "now also asks for grant deployer"}},
		{name: "asked, no", flag: consentAsk, terminal: true, in: "no\n", asks: asks, err: "cancelled"},
		{name: "asked, not a terminal", flag: consentAsk, asks: asks, err: "needs a digest"},
		{name: "asked, unknown", flag: consentAsk, terminal: true, in: "yes\n", err: "could not be read"},
		{name: "asked, the version changes", flag: consentAsk, terminal: true, in: "yes\n", asks: asks, changes: true, err: "changes the version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			var stderr bytes.Buffer
			cmd.SetIn(strings.NewReader(tc.in))
			cmd.SetErr(&stderr)
			g := &globalFlags{terminal: func() bool { return tc.terminal }}
			got, err := g.resolveConsent(cmd, tc.flag, "schedule update nightly", tc.asks, given, tc.changes)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("resolveConsent = %q, %v; want %q", got, err, tc.want)
			}
			for _, s := range tc.shows {
				if !strings.Contains(stderr.String(), s) {
					t.Errorf("the prompt does not show %q:\n%s", s, stderr.String())
				}
			}
		})
	}
}

// TestApplyConsents: each --consent KIND/NAME=DIGEST reaches the request of
// the step that saves that object and nothing else; a malformed entry, one
// given twice, and one naming an object the apply does not save refuse the
// apply before anything is written.
func TestApplyConsents(t *testing.T) {
	plan := func() *orkstackv1.ApplyReport {
		return &orkstackv1.ApplyReport{Automation: []*orkstackv1.AutomationDocumentReport{{Plan: &orkstackv1.PlanAutomationResponse{
			Steps: []*orkstackv1.AutomationStep{
				{Action: orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE, Kind: orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_SCHEDULE, Name: "nightly",
					Request: &orkstackv1.AutomationStep_CreateSchedule{CreateSchedule: &orkstackv1.CreateScheduleRequest{Name: "nightly"}}},
				{Action: orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE, Kind: orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_TRIGGER, Name: "nightly",
					Request: &orkstackv1.AutomationStep_UpdateTrigger{UpdateTrigger: &orkstackv1.UpdateTriggerRequest{Name: "nightly"}}},
				{Action: orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED, Kind: orkstackv1.AutomationObjectKind_AUTOMATION_OBJECT_KIND_SCHEDULE, Name: "weekly"},
			},
		}}}}
	}
	report := plan()
	r := &applyRun{report: report, opts: applyOptions{consents: []string{"schedule/nightly=c1:a"}}}
	if refusals := r.attachConsents(); len(refusals) != 0 {
		t.Fatalf("refusals = %v", refusals)
	}
	steps := report.GetAutomation()[0].GetPlan().GetSteps()
	if steps[0].GetCreateSchedule().GetConsent() != "c1:a" || steps[1].GetUpdateTrigger().GetConsent() != "" {
		t.Fatalf("consents went to %q and %q", steps[0].GetCreateSchedule().GetConsent(), steps[1].GetUpdateTrigger().GetConsent())
	}
	for _, tc := range []struct{ entry, want string }{
		{"schedule/weekly=c1:a", "this apply saves no schedule"},
		{"webhook/nightly=c1:a", "want KIND/NAME=DIGEST"},
		{"schedule/nightly", "want KIND/NAME=DIGEST"},
		{"schedule=c1:a", "want KIND/NAME=DIGEST"},
	} {
		r := &applyRun{report: plan(), opts: applyOptions{consents: []string{tc.entry}}}
		if refusals := r.attachConsents(); len(refusals) != 1 || !strings.Contains(refusals[0], tc.want) {
			t.Errorf("%s: refusals = %v, want %q", tc.entry, refusals, tc.want)
		}
	}
	r = &applyRun{report: plan(), opts: applyOptions{consents: []string{"trigger/nightly=c1:a", "trigger/nightly=c1:b"}}}
	if refusals := r.attachConsents(); len(refusals) != 1 || !strings.Contains(refusals[0], "more than once") {
		t.Errorf("twice: refusals = %v", refusals)
	}
}

// TestRenderPromotion_StaleConsent: a promotion's answer names each object
// it leaves refused an identity, with the command that consents.
func TestRenderPromotion_StaleConsent(t *testing.T) {
	var out bytes.Buffer
	digest := "c1:" + strings.Repeat("c", 64)
	if err := renderPromotion(&out, &orkstackv1.SetCurrentOrkflowVersionResponse{Name: "deploy", CurrentVersion: "2", Namespace: "default",
		StaleConsent: []*orkstackv1.StaleConsent{{Kind: "schedule", Name: "nightly", Digest: digest}}}); err != nil {
		t.Fatal(err)
	}
	if want := "schedule nightly: orkctl schedule update nightly --consent=" + digest; !strings.Contains(out.String(), want) {
		t.Fatalf("promotion output:\n%s\nlacks %q", out.String(), want)
	}
}

// TestRenderIssuedTokens: one line a token, its claims and never a token.
func TestRenderIssuedTokens(t *testing.T) {
	var out bytes.Buffer
	item := int32(3)
	if err := renderIssuedTokens(&out, []*orkstackv1.IssuedTokenRecord{{TaskId: "apply", Item: &item, Attempt: 2, OrkerId: "orker-1",
		Tokens: []*orkstackv1.IssuedTokenEntry{{Name: "aws", Audience: "sts", Subject: "ork:namespace:ns:orkflow:d:task:apply", Jti: "j1"}}}}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"apply", "3", "aws", "sts", "ork:namespace:ns:orkflow:d:task:apply", "j1", "orker-1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	if err := renderIssuedTokens(&out, nil); err != nil || !strings.Contains(out.String(), "No token") {
		t.Fatalf("empty: %q, %v", out.String(), err)
	}
}

// TestLiveFooters_Identities: inspect names each identity an attempt holds,
// its state, when it was minted and when it expires.
func TestLiveFooters_Identities(t *testing.T) {
	live := &orkstackv1.LiveAttempt{TaskId: "apply", HeartbeatAvailable: true, Identity: []*orkstackv1.LiveIdentity{
		{Name: "aws", State: orkstackv1.LiveIdentityState_LIVE_IDENTITY_STATE_FRESH, LastRefresh: timestamppb.Now(), ExpiresAt: timestamppb.Now()}}}
	lines := liveFooters("", "ork-1", nil, live, time.Now())
	if len(lines) == 0 || !strings.Contains(lines[0], "identity aws fresh") {
		t.Fatalf("lines = %v", lines)
	}
}
