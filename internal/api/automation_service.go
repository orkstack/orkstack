package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// AutomationService plans and exports a namespace's automation document
// (docs/AUTOMATION.md). It WRITES NOTHING: no store Put, Create, Delete or
// Update, no schedule-controller mutation, no run row, no cluster call but
// describes and lists. An apply is the CLIENT sending each actionable step's
// carried request to the existing write RPC it names — the same validated,
// audited call a person could make by hand.

// automationConfigured refuses both RPCs on a service with no schedule
// controller: a plan that silently skipped schedules would read as "no
// schedules" and, with --prune, as nothing to delete.
func (s *Service) automationConfigured() error {
	if s.schedules == nil {
		return connect.NewError(connect.CodeFailedPrecondition,
			errors.New("no schedule controller is configured; automation plans over every service"))
	}
	return nil
}

// PlanAutomation parses the document, reads what its namespace holds, and
// answers with every step an apply would take. Problems with the document
// are issues; an unreachable store or cluster is an RPC error, never an empty
// plan.
func (s *Service) PlanAutomation(
	ctx context.Context,
	req *connect.Request[orkstackv1.PlanAutomationRequest],
) (*connect.Response[orkstackv1.PlanAutomationResponse], error) {
	if err := s.automationConfigured(); err != nil {
		return nil, err
	}
	doc, issues := automation.Parse([]byte(req.Msg.GetYaml()))
	if automation.HasErrors(issues) || doc == nil {
		return connect.NewResponse(&orkstackv1.PlanAutomationResponse{Issues: issues}), nil
	}
	ns := artifacts.ResolveNamespace(doc.Namespace)
	if requested := req.Msg.GetNamespace(); requested != "" && artifacts.ResolveNamespace(requested) != ns {
		issues = append(issues, &orkstackv1.ValidationIssue{
			Field:    "namespace",
			Message:  fmt.Sprintf("the document describes namespace %q but this request is for %q", ns, artifacts.ResolveNamespace(requested)),
			Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR,
		})
		return connect.NewResponse(&orkstackv1.PlanAutomationResponse{Issues: issues}), nil
	}

	live, err := s.readAutomationLive(ctx, ns, doc.Schedules != nil, doc.Triggers != nil, doc.Webhooks != nil)
	if err != nil {
		return nil, err
	}
	live.Orkflows = map[automation.OrkflowRef]bool{}
	for _, ref := range doc.References() {
		_, err := s.orkflows.Get(ctx, ns, ref.Name, ref.Version)
		switch {
		case err == nil:
			live.Orkflows[ref] = true
		case errors.Is(err, artifacts.ErrNotFound):
			live.Orkflows[ref] = false
		default:
			return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q", ref.Name, ref.Version, ns)
		}
	}
	for _, pending := range req.Msg.GetPendingOrkflows() {
		if artifacts.ResolveNamespace(pending.GetNamespace()) != ns {
			continue
		}
		live.Orkflows[automation.OrkflowRef{Name: pending.GetName(), Version: pending.GetVersion()}] = true
		live.Orkflows[automation.OrkflowRef{Name: pending.GetName()}] = true
	}

	steps, planIssues := automation.Plan(doc, live, automation.Options{Prune: req.Msg.GetPrune()})
	issues = append(issues, planIssues...)
	targetIssues, err := s.validateCarriedRequests(ctx, live.Namespace, steps)
	if err != nil {
		return nil, err
	}
	issues = append(issues, targetIssues...)
	return connect.NewResponse(&orkstackv1.PlanAutomationResponse{
		Namespace:  ns,
		Managed:    doc.Managed(),
		Steps:      steps,
		Issues:     issues,
		PlanDigest: automation.Digest(ns, steps),
	}), nil
}

// ExportAutomation renders the document a namespace's current automation
// would be applied from. It refuses rather than omit.
func (s *Service) ExportAutomation(
	ctx context.Context,
	req *connect.Request[orkstackv1.ExportAutomationRequest],
) (*connect.Response[orkstackv1.ExportAutomationResponse], error) {
	if err := s.automationConfigured(); err != nil {
		return nil, err
	}
	stored, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	ns := stored.GetName()
	live, err := s.readAutomationLive(ctx, ns, true, true, true)
	if err != nil {
		return nil, err
	}
	yaml, unmanaged, err := automation.Export(ns, live)
	var refused *automation.ExportError
	if errors.As(err, &refused) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&orkstackv1.ExportAutomationResponse{Yaml: yaml, Unmanaged: unmanaged}), nil
}

// readAutomationLive reads what namespace ns holds, for the sections asked
// for. Every read that fails is an RPC error, never an empty section: an
// empty section would plan every declared object as a create and, with
// --prune, every live one as nothing to delete. The namespace itself is
// always read; an absent one is Live.Namespace == nil, and nothing can be
// stored in a namespace that does not exist, so its schedules and triggers
// are not read.
func (s *Service) readAutomationLive(ctx context.Context, ns string, schedules, triggers, webhooks bool) (automation.Live, error) {
	var live automation.Live
	stored, err := s.namespace(ctx, ns)
	switch {
	case err == nil:
		live.Namespace = stored
	case connect.CodeOf(err) == connect.CodeNotFound:
	default:
		return live, err
	}
	if live.Namespace != nil && schedules {
		views, err := s.schedules.List(ctx, ns)
		if err != nil {
			return live, scheduleConnectError(err, ns, "")
		}
		for _, view := range views {
			live.Schedules = append(live.Schedules, scheduleProto(view))
		}
	}
	if live.Namespace != nil && triggers {
		list, err := s.triggers.List(ctx, ns, "")
		if err != nil {
			return live, automationReadError(err, "list the triggers of namespace %q", ns)
		}
		for _, trigger := range list {
			live.Triggers = append(live.Triggers, triggerProto(trigger))
		}
	}
	if webhooks {
		hooks, err := s.webhooks.ListContext(ctx)
		if err != nil {
			return live, automationReadError(err, "list webhooks")
		}
		for _, hook := range hooks {
			// Only subscriptions scoped to this namespace: a deployment-wide
			// one belongs to no namespace document and is never read into a
			// plan, so no namespace's --prune can reach it.
			if hook.Namespace != "" && artifacts.ResolveNamespace(hook.Namespace) == ns {
				live.Webhooks = append(live.Webhooks, webhookProto(hook))
			}
		}
	}
	return live, nil
}

// automationReadError maps a registry read failure: a cancelled or timed-out
// request keeps its code, and anything else is the store being unreachable.
func automationReadError(err error, format string, args ...any) error {
	subject := fmt.Sprintf(format, args...)
	switch {
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", subject, err))
}

// validateCarriedRequests runs the target RPCs' own validators over the
// requests the plan carries, so an apply fails at plan time, before any
// write, wherever the engine can know it. A refusal is an ERROR anchored to
// the document field; a validator that could not run (a store or a policy
// unreachable) is an RPC error.
func (s *Service) validateCarriedRequests(ctx context.Context, stored *orkstackv1.Namespace, steps []*orkstackv1.AutomationStep) ([]*orkstackv1.ValidationIssue, error) {
	var issues []*orkstackv1.ValidationIssue
	refuse := func(field string, err error) error {
		if code := connect.CodeOf(err); code != connect.CodeInvalidArgument && code != connect.CodeUnknown {
			return err
		}
		issues = append(issues, &orkstackv1.ValidationIssue{
			Field: field, Message: connectMessage(err), Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR,
		})
		return nil
	}
	for _, step := range steps {
		switch req := step.GetRequest().(type) {
		case *orkstackv1.AutomationStep_CreateNamespace:
			spec := req.CreateNamespace.GetNamespace()
			if err := s.validNamespaceForPlan(ctx, spec, nil, refuse); err != nil {
				return nil, err
			}
		case *orkstackv1.AutomationStep_UpdateNamespace:
			spec := req.UpdateNamespace.GetNamespace()
			if err := s.validNamespaceForPlan(ctx, spec, stored, refuse); err != nil {
				return nil, err
			}
		case *orkstackv1.AutomationStep_CreateSchedule:
			if err := s.validScheduleForPlan(step.GetName(), req.CreateSchedule.GetSpec(), req.CreateSchedule.GetDescription(), refuse); err != nil {
				return nil, err
			}
		case *orkstackv1.AutomationStep_UpdateSchedule:
			if err := s.validScheduleForPlan(step.GetName(), req.UpdateSchedule.GetSpec(), req.UpdateSchedule.GetDescription(), refuse); err != nil {
				return nil, err
			}
		}
	}
	return issues, nil
}

// validNamespaceForPlan is CreateNamespace's (stored nil) or UpdateNamespace's
// validation, in their order: the sink and the signer are checked on update
// only when they change, exactly as UpdateNamespace does.
func (s *Service) validNamespaceForPlan(ctx context.Context, spec, stored *orkstackv1.Namespace, refuse func(string, error) error) error {
	if err := validNamespaceName(spec.GetName()); err != nil {
		if rerr := refuse("namespace", err); rerr != nil {
			return rerr
		}
	}
	if err := s.validNamespaceRetention(ctx, spec); err != nil {
		if rerr := refuse("settings.run_retention", err); rerr != nil {
			return rerr
		}
	}
	if err := validNamespaceTelemetryDocument(spec); err != nil {
		if rerr := refuse("settings.telemetry", err); rerr != nil {
			return rerr
		}
	}
	if sink := spec.GetTelemetry().GetSink(); stored == nil || sink != stored.GetTelemetry().GetSink() {
		if err := s.validNamespaceTelemetrySink(sink); err != nil {
			if rerr := refuse("settings.telemetry", err); rerr != nil {
				return rerr
			}
		}
	}
	if signer := spec.GetRequireSignedBy(); stored == nil || signer != stored.GetRequireSignedBy() {
		if err := s.validNamespaceSigner(ctx, spec); err != nil {
			if rerr := refuse("settings.require_signed_by", err); rerr != nil {
				return rerr
			}
		}
	}
	if err := artifacts.ValidateNamespaceSpec(spec); err != nil {
		return refuse(namespaceSpecField(err), connect.NewError(connect.CodeInvalidArgument, err))
	}
	return nil
}

// namespaceSpecField anchors one of ValidateNamespaceSpec's refusals to the
// settings key it is about.
func namespaceSpecField(err error) string {
	message := err.Error()
	for _, probe := range []struct{ fragment, field string }{
		{"budget", "settings.budget"},
		{"allowed_pools", "settings.allowed_pools"},
		{"default_pool", "settings.default_pool"},
		// validNamespacePools names the default as "default pool" and an
		// allow-list entry as "allowed pool"; both must come before the
		// generic probe, or a bad default is anchored to the allow-list.
		{"default pool", "settings.default_pool"},
		{"allowed pool", "settings.allowed_pools"},
		{"pool", "settings.allowed_pools"},
		{"run retention", "settings.run_retention"},
		{"run_retention", "settings.run_retention"},
		{"telemetry", "settings.telemetry"},
		{"allowed_secrets", "settings.allowed_secrets"},
		{"secret", "settings.allowed_secrets"},
		{"require_signed_by", "settings.require_signed_by"},
		{"max_running", "settings.max_running"},
	} {
		if strings.Contains(message, probe.fragment) {
			return probe.field
		}
	}
	return "settings"
}

// validScheduleForPlan is CreateSchedule's and UpdateSchedule's validation of
// the authored spec and description, anchored to the entry's field.
func (s *Service) validScheduleForPlan(name string, spec *orkstackv1.ScheduleSpec, description string, refuse func(string, error) error) error {
	field := "schedules." + name
	if err := ValidateScheduleSpec(spec, s.now()); err != nil {
		at, message := field, err.Error()
		if rest, ok := strings.CutPrefix(message, "spec."); ok {
			if key, tail, found := strings.Cut(rest, ": "); found {
				at, message = field+"."+key, tail
			}
		} else if rest, ok := strings.CutPrefix(message, "spec: "); ok {
			message = rest
		}
		if rerr := refuse(at, connect.NewError(connect.CodeInvalidArgument, errors.New(message))); rerr != nil {
			return rerr
		}
	}
	if err := validateScheduleText("description", description); err != nil {
		return refuse(field+".description", connect.NewError(connect.CodeInvalidArgument, err))
	}
	return nil
}
