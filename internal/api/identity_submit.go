package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// reasonStoreUnscoped refuses a run that would hold an identity while a
// live Orker's storage credential reaches the control plane or has not been
// checked (docs/REASONS.md).
const reasonStoreUnscoped = "IDENTITY_STORE_UNSCOPED"

// checkIdentityAtSubmit is task identity's part of a submit's preparation,
// for one definition of the run's tree as it runs in namespace ns
// (workload-identity §10): every audience its tasks ask for is one the
// namespace allows now; with identities_current_only, it is the version
// its owner's current pointer names. A definition that asks for nothing
// reads nothing. Every refusal comes before a run id is minted.
func (s *Service) checkIdentityAtSubmit(ctx context.Context, ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow, owner string) error {
	if !identity.AsksForIdentity(wf) {
		return nil
	}
	p, err := s.namespacePolicy(ctx, ns)
	if err != nil {
		return policyReadError(ns.GetName(), err)
	}
	if err := identity.CheckAudiences(ns.GetName(), p.Audiences, wf); err != nil {
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if p.IdentitiesCurrentOnly {
		current, err := s.orkflows.Get(ctx, owner, wf.GetName(), "")
		if err != nil {
			return orkflowStoreError(err, "the current version of orkflow %q in namespace %q", wf.GetName(), owner)
		}
		if current.GetVersion() != wf.GetVersion() {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"%s: namespace %q lets only an Orkflow's current version hold identities, and %s@%s is not current (%s is)",
				identity.ReasonNotPermitted, ns.GetName(), wf.GetName(), wf.GetVersion(), current.GetVersion()))
		}
	}
	return nil
}

// checkStoreScopedForIdentity refuses, IDENTITY_STORE_UNSCOPED, a run that
// would hold an identity while any live Orker of the deployment reports a
// storage credential that reaches the control plane, or no verdict; with
// identity.unscoped_orkers: report the Orkers are only reported (the
// engine's own report logs them). An inventory that cannot be read is
// unavailable, never "every Orker is scoped".
func (s *Service) checkStoreScopedForIdentity(ctx context.Context) error {
	if s.identityUnscopedOrkers == identity.UnscopedReport {
		return nil
	}
	if s.fleet == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: this engine reads no Orker inventory, so it cannot confirm that every Orker's storage credential is scoped", reasonStoreUnscoped))
	}
	fleet, err := engine.ReadFleet(ctx, s.fleet)
	if err != nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("the Orker inventory could not be read: %w", err))
	}
	unscoped := engine.UnscopedOrkers(fleet)
	if len(unscoped) == 0 {
		return nil
	}
	names := make([]string, 0, len(unscoped))
	for _, u := range unscoped {
		names = append(names, u.OrkerID+" ("+u.Verdict+")")
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s: a run that holds an identity is refused while an Orker's storage credential is not confirmed scoped: %s "+
			"(docs/SECURITY_MODEL.md; identity.unscoped_orkers: report reports them instead)",
		reasonStoreUnscoped, strings.Join(names, ", ")))
}

// treeAsksForIdentity reports whether wf or any Orkflow it composes holds
// an identity. A child that is not there answers false: planChildren
// refuses the run with that reason, which is the one to give. A store that
// does not answer is an error, never "asks for nothing".
func (s *Service) treeAsksForIdentity(ctx context.Context, ns string, wf *orkstackv1.Orkflow) (bool, error) {
	if identity.AsksForIdentity(wf) {
		return true, nil
	}
	// Only a definition that composes reads anything more: the hot path of
	// a submit of a plain Orkflow stays as it was.
	composes := false
	for _, t := range wf.GetTasks() {
		composes = composes || t.GetOrkflow() != nil
	}
	if !composes {
		return false, nil
	}
	asks, err := s.asksFor(ctx, ns, wf.GetName(), wf.GetVersion())
	switch {
	case errors.Is(err, consent.ErrChildMissing), connect.CodeOf(err) == connect.CodeNotFound:
		return false, nil
	case unknownAsks(err):
		// A tree the projection cannot follow is held to every check.
		return true, nil
	case err != nil:
		return false, err
	}
	return !asks.Empty(), nil
}

// The refusals of a run nobody submitted that would hold an identity
// (workload-identity §3.2; docs/REASONS.md).
const (
	reasonStarterNotRecorded  = "IDENTITY_STARTER_NOT_RECORDED"
	reasonStarterNotPermitted = "IDENTITY_STARTER_NOT_PERMITTED"
	reasonStarterNotConsented = "IDENTITY_STARTER_NOT_CONSENTED"
)

// starter is what a schedule's or a trigger's record says of who bound it.
type starter struct {
	kind, name string // the orkctl noun and the object's handle
	binder     string
	verified   bool
	binding    *artifacts.BinderBinding
	consent    consentRecord
}

// checkStarter holds a run a schedule or a trigger starts, that would hold
// an identity, to the three checks of workload-identity §3.2: a verified
// binder is recorded with the binding that let it bind; that binding still
// grants it run.submit on the Orkflow (no group or claim is read again);
// and its consent is to what the version the run resolves asks for now.
// Each is a refusal only when the fact was read: a record, an access
// document or a definition that could not be read is unavailable, so a
// fire tries again and an invoke answers 503.
func (s *Service) checkStarter(ctx context.Context, ns *orkstackv1.Namespace, msg *orkstackv1.SubmitRunRequest, origin *orkstackv1.RunOrigin) error {
	st, err := s.readStarter(ctx, ns.GetName(), msg, origin)
	if err != nil {
		return err
	}
	orkflow := msg.GetOrkflowName()
	if !st.verified || st.binder == "" || st.binding == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: %s %s has no verified binder recorded with the binding that let it bind; rebind it as a principal who may submit Orkflow %s: `orkctl %s update %s --rebind`",
			reasonStarterNotRecorded, st.kind, st.name, orkflow, st.kind, st.name))
	}
	s.access.init()
	policy, unavailable := s.access.policy(ctx)
	if unavailable {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("the access document could not be read to judge %s %s's binder", st.kind, st.name))
	}
	res, err := s.withIDs(ctx, policy, authz.Resource{Namespace: ns.GetName(), Orkflow: orkflow})
	if err != nil {
		return err
	}
	recorded := authz.BinderBinding{Name: st.binding.Name, Selector: st.binding.Selector, Role: st.binding.Role, Scope: st.binding.Scope}
	if ok, why := policy.StillGrants(recorded, st.binder, authz.PermRunSubmit, res); !ok {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf(
			"%s: %s %s's binder %s may no longer submit Orkflow %s: %s; rebind it as a principal who may",
			reasonStarterNotPermitted, st.kind, st.name, st.binder, orkflow, why))
	}
	asks, err := s.asksFor(ctx, ns.GetName(), orkflow, msg.GetOrkflowVersion())
	if err != nil {
		return err
	}
	if st.consent.Digest != asks.Digest {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"%s: what Orkflow %s asks for is not what %s %s's consent covers: %s. Consent with `orkctl %s update %s --consent=%s`",
			reasonStarterNotConsented, orkflow, st.kind, st.name, consent.Difference(st.consent.view(), asks), st.kind, st.name, asks.Digest))
	}
	return nil
}

// readStarter reads the record of the object that starts the run. A record
// that cannot be read is unavailable; one that holds no entry for what the
// run submits is a starter with no binder.
func (s *Service) readStarter(ctx context.Context, ns string, msg *orkstackv1.SubmitRunRequest, origin *orkstackv1.RunOrigin) (starter, error) {
	switch origin.GetKind() {
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE:
		st := starter{kind: "schedule", name: origin.GetDescription()}
		if s.scheduleBindings == nil {
			return st, nil
		}
		doc, err := s.scheduleBindings.Get(ctx, ns, origin.GetId())
		if err != nil {
			return st, connect.NewError(connect.CodeUnavailable, fmt.Errorf("schedule %s's record could not be read: %w", st.name, err))
		}
		held, err := scheduleSubmission(&orkstackv1.ScheduleRecord{OrkflowName: msg.GetOrkflowName(), OrkflowVersion: msg.GetOrkflowVersion(), Input: msg.GetInput()})
		if err != nil {
			return st, connect.NewError(connect.CodeInternal, err)
		}
		if e, ok := doc.Find(held); ok {
			st.binder, st.verified, st.binding, st.consent = e.Binder, e.BinderVerified, e.BinderBinding, scheduleConsentOf(e)
		}
		return st, nil
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER:
		t, found, err := s.triggers.Get(ctx, origin.GetId())
		if err != nil {
			return starter{}, connect.NewError(connect.CodeUnavailable, fmt.Errorf("trigger %s's record could not be read: %w", origin.GetId(), err))
		}
		st := starter{kind: "trigger", name: origin.GetId()}
		if !found {
			return st, nil
		}
		b := t.Binding
		st.binder, st.verified, st.consent = b.Binder, b.Verified, triggerConsentOf(b)
		if b.BinderBindingName != "" {
			st.binding = &artifacts.BinderBinding{Name: b.BinderBindingName, Selector: b.BinderBindingSelector, Role: b.BinderBindingRole, Scope: b.BinderBindingScope}
		}
		return st, nil
	}
	return starter{kind: "starter", name: origin.GetId()}, nil
}

// startedByAnObject reports a run a schedule or a trigger starts: the
// runs nobody submitted that checkStarter judges.
func startedByAnObject(origin *orkstackv1.RunOrigin) bool {
	switch origin.GetKind() {
	case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE, orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER:
		return true
	}
	return false
}
