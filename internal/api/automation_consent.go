package api

import (
	"context"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// automationConsentIssues are the plan's consent warnings
// (workload-identity §3.2, "orkctl automation apply"): a step that binds an
// object whose Orkflow asks for an identity needs that object's digest on
// the command (--consent KIND/NAME=DIGEST), and an object the plan leaves as
// it is whose consent is stale is reported with the digest to consent to.
// Neither is a step, so neither is in the plan's digest. What cannot be read
// is not warned about: the save itself refuses or records nothing.
func (s *Service) automationConsentIssues(ctx context.Context, ns string, live automation.Live, steps []*orkstackv1.AutomationStep) []*orkstackv1.ValidationIssue {
	memo := asksMemo{}
	var issues []*orkstackv1.ValidationIssue
	binds := map[string]bool{}
	need := func(kind, section, name, orkflow, version string) {
		binds[kind+"/"+name] = true
		asks, err := s.asksForMemo(ctx, memo, ns, orkflow, version)
		switch {
		case err != nil && !unknownAsks(err):
			// Not "asks for nothing": the save will decide, and refuse if
			// it needs a consent the apply does not carry.
			issues = append(issues, consentWarning(section+"."+name, fmt.Sprintf(
				"what Orkflow %s asks for could not be read (%v); if it asks for an identity, this apply needs --consent %s/%s=DIGEST",
				orkflow, err, kind, name)))
			return
		case err != nil, asks.Empty():
			return
		}
		issues = append(issues, consentWarning(section+"."+name, fmt.Sprintf(
			"Orkflow %s asks for an identity (audiences %s; grants %s), and this apply binds the %s: it needs --consent %s/%s=%s",
			orkflow, listOrNone(asks.Audiences), listOrNone(asks.Grants), kind, kind, name, asks.Digest)))
	}
	for _, step := range steps {
		switch r := step.GetRequest().(type) {
		case *orkstackv1.AutomationStep_CreateSchedule:
			req := r.CreateSchedule
			need("schedule", "schedules", req.GetName(), req.GetOrkflowName(), req.GetOrkflowVersion())
		case *orkstackv1.AutomationStep_UpdateSchedule:
			req := r.UpdateSchedule
			if have := liveSchedule(live, req.GetName()); have != nil &&
				(have.GetOrkflowVersion() != req.GetOrkflowVersion() || !sameScheduleInput(have.GetInput(), req.GetInput())) {
				need("schedule", "schedules", req.GetName(), have.GetOrkflowName(), req.GetOrkflowVersion())
			}
		case *orkstackv1.AutomationStep_CreateTrigger:
			req := r.CreateTrigger
			need("trigger", "triggers", req.GetName(), req.GetOrkflowName(), req.GetOrkflowVersion())
		case *orkstackv1.AutomationStep_UpdateTrigger:
			req := r.UpdateTrigger
			if have := liveTrigger(live, req.GetName()); have != nil && have.GetOrkflowVersion() != req.GetOrkflowVersion() {
				need("trigger", "triggers", req.GetName(), have.GetOrkflowName(), req.GetOrkflowVersion())
			}
		case *orkstackv1.AutomationStep_CreateEventSource:
			src := r.CreateEventSource.GetEventSource()
			need("event-source", "event_sources", src.GetName(), src.GetOrkflowName(), src.GetOrkflowVersion())
		case *orkstackv1.AutomationStep_UpdateEventSource:
			src := r.UpdateEventSource.GetEventSource()
			if have := liveSource(live, src.GetName()); have != nil && eventsources.ChangesWhatItSubmits(have, src) {
				need("event-source", "event_sources", src.GetName(), src.GetOrkflowName(), src.GetOrkflowVersion())
			}
		}
	}
	stale := func(kind, section, name string, given *orkstackv1.ConsentView, asks *orkstackv1.AsksFor) {
		if name == "" || binds[kind+"/"+name] || !consentStale(given, asks) {
			return
		}
		issues = append(issues, consentWarning(section+"."+name, fmt.Sprintf(
			"the %s's consent is stale: %s. Consent with --consent %s/%s=%s",
			kind, consent.Difference(given, consent.Asks{Digest: asks.GetDigest(), Audiences: asks.GetAudiences(), Grants: asks.GetGrants()}),
			kind, name, asks.GetDigest())))
	}
	for _, sch := range live.Schedules {
		stale("schedule", "schedules", sch.GetName(), sch.GetConsented(), sch.GetAsksFor())
	}
	for _, tr := range live.Triggers {
		stale("trigger", "triggers", tr.GetName(), tr.GetConsented(), tr.GetAsksFor())
	}
	for _, src := range live.EventSources {
		view := s.sourceView(ctx, memo, src)
		stale("event-source", "event_sources", src.GetName(), view.GetConsented(), view.GetAsksFor())
	}
	return issues
}

func consentWarning(field, message string) *orkstackv1.ValidationIssue {
	return &orkstackv1.ValidationIssue{Field: field, Message: message, Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING}
}

func liveSchedule(live automation.Live, name string) *orkstackv1.Schedule {
	for _, s := range live.Schedules {
		if s.GetName() == name {
			return s
		}
	}
	return nil
}

func liveTrigger(live automation.Live, name string) *orkstackv1.Trigger {
	for _, t := range live.Triggers {
		if t.GetName() == name {
			return t
		}
	}
	return nil
}

func liveSource(live automation.Live, name string) *orkstackv1.EventSource {
	for _, s := range live.EventSources {
		if s.GetName() == name {
			return s
		}
	}
	return nil
}

func listOrNone(s []string) string {
	if len(s) == 0 {
		return "(none)"
	}
	out := s[0]
	for _, v := range s[1:] {
		out += ", " + v
	}
	return out
}

// sameScheduleInput compares two inputs as a schedule's binding record does:
// by the digest of their canonical JSON, so the plan says a save binds
// exactly when the save does.
func sameScheduleInput(a, b *orkstackv1.Payload) bool {
	da, errA := scheduleInputSHA256(a)
	db, errB := scheduleInputSHA256(b)
	return errA == nil && errB == nil && da == db
}
