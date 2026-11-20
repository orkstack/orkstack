package api_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const consentAutomationDoc = `apiVersion: orkstack/v1
kind: Automation
namespace: payments
schedules:
  nightly:
    orkflow: deploy
    cron: "0 2 * * *"
`

func consentWarnings(plan *orkstackv1.PlanAutomationResponse) []*orkstackv1.ValidationIssue {
	var out []*orkstackv1.ValidationIssue
	for _, issue := range plan.GetIssues() {
		if strings.Contains(issue.GetMessage(), "consent") {
			out = append(out, issue)
		}
	}
	return out
}

// TestPlanAutomation_ConsentIsAWarningNotAStep: a plan that creates a
// schedule whose Orkflow asks for an identity warns that apply needs that
// schedule's digest; once applied with it and the Orkflow moves on, the plan
// of the unchanged document has no step, one warning with the new digest,
// and the digest of the plan without it.
func TestPlanAutomation_ConsentIsAWarningNotAStep(t *testing.T) {
	bindings, err := artifacts.NewScheduleBindingStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	h := newAutomationHarness(t, func(cfg *api.Config) { cfg.ScheduleBindings = bindings })
	ctx := context.Background()
	put := func(version, audience string) {
		def := roleOrkflow(version, audience)
		def.Namespace = "payments"
		_, _, err := h.orkflows.Put(ctx, def, artifacts.HeadExpectation{})
		must.OK(t, err)
		_, err = h.orkflows.SetCurrentVersion(ctx, "payments", "deploy", version, artifacts.CurrentExpectation{})
		must.OK(t, err)
	}
	put("1", "sts")
	plan, err := h.plan(t, consentAutomationDoc, false)
	must.OK(t, err)
	warnings := consentWarnings(plan)
	if len(warnings) != 1 || warnings[0].GetField() != "schedules.nightly" ||
		warnings[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		!strings.Contains(warnings[0].GetMessage(), "--consent schedule/nightly=c1:") {
		t.Fatalf("plan warnings = %v, want one naming schedule/nightly's digest", warnings)
	}
	msg := warnings[0].GetMessage()
	first := msg[strings.Index(msg, "schedule/nightly=")+len("schedule/nightly="):][:67]

	step := plan.GetSteps()[0].GetCreateSchedule()
	step.Consent = first
	_, err = h.svc.CreateSchedule(ctx, connect.NewRequest(step))
	must.OK(t, err)

	put("2", "vault")
	again, err := h.plan(t, consentAutomationDoc, false)
	must.OK(t, err)
	warnings = consentWarnings(again)
	if len(again.GetSteps()) != 1 || again.GetSteps()[0].GetAction() != orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED {
		t.Fatalf("a stale consent is a step: %v", again.GetSteps())
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].GetMessage(), "stale") || strings.Contains(warnings[0].GetMessage(), first) {
		t.Fatalf("stale warnings = %v", warnings)
	}
	third, err := h.plan(t, consentAutomationDoc, false)
	must.OK(t, err)
	if third.GetPlanDigest() != again.GetPlanDigest() {
		t.Fatal("two plans of an unchanged document differ")
	}
}
