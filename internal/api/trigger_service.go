package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/orkstack/orkstack/internal/consent"
	"log/slog"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// TriggerService manages inbound run starters. It is the management half of
// the capability; the invoke half is the plain-HTTP adapter in triggers.go.
//
// The shape mirrors webhook_service.go, with one addition a webhook has no use
// for: a trigger names an orkflow, so a create resolves that orkflow before it
// writes anything.

// CreateTrigger stores one trigger bound to one orkflow. Its bearer token
// appears only in this response; every later read maps to a Trigger, which has
// no secret field at all.
func (s *Service) CreateTrigger(
	ctx context.Context,
	req *connect.Request[orkstackv1.CreateTriggerRequest],
) (*connect.Response[orkstackv1.CreateTriggerResponse], error) {
	name := req.Msg.GetOrkflowName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("orkflow_name is required"))
	}
	if err := ownNamespaceTarget("trigger", name); err != nil {
		return nil, err
	}
	// The trigger's own name is checked before anything is resolved: a name
	// no automation document could address is a bad request whatever the
	// namespace holds.
	if err := triggers.ValidateName(req.Msg.GetName()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	// The namespace is resolved BEFORE the orkflow is read, and the orkflow is
	// read before anything is written. These are the same two checks
	// SubmitRun's own preparation performs, in the same order and returning
	// the same errors, so a trigger can never be created against a namespace
	// or an orkflow version that a submission would refuse. A refused create
	// writes nothing: a trigger must never silently point at nothing.
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	version := req.Msg.GetOrkflowVersion()
	// Reading what the version asks for is the check that it exists, and a
	// create binds, so it needs a consent when that is anything
	// (consent_save.go).
	asks, unknown, err := s.asksForSave(ctx, ns.GetName(), name, version)
	if err != nil {
		return nil, err
	}
	who := authn.Recorded(ctx, req.Header())
	granted, err := s.binderBindingFor(ctx, ns.GetName(), name)
	if err != nil {
		return nil, err
	}
	given, err := decideConsent(consentSave{Kind: "trigger", Name: req.Msg.GetName(), Orkflow: name, Binds: true,
		Given: req.Msg.GetConsent(), Asks: asks, Unknown: unknown, By: recordedBinder(who.ID), Now: s.now().UTC()})
	if err != nil {
		return nil, err
	}

	trigger, secret, err := s.triggers.Create(ctx, triggers.Registration{
		Namespace:      ns.GetName(),
		OrkflowName:    name,
		OrkflowVersion: version,
		Description:    req.Msg.GetDescription(),
		Name:           req.Msg.GetName(),
		// A create binds its caller (docs/ORKFLOW_TRIGGERS.md, "Who a
		// trigger runs as").
		Binding: s.triggerBoundBy(who, granted, given),
	})
	if errors.Is(err, triggers.ErrNameTaken) {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
			"trigger name %q is already in use in namespace %q", req.Msg.GetName(), ns.GetName()))
	}
	if err != nil {
		return nil, triggerConnectError(err)
	}
	// The one and only disclosure of the plaintext token: it was generated in
	// this process a moment ago and is not in the record that was stored.
	return connect.NewResponse(&orkstackv1.CreateTriggerResponse{
		Trigger: s.triggerProtoWith(ctx, trigger),
		Secret:  secret,
	}), nil
}

// ListTriggers returns one namespace's triggers, narrowed by filters. It reads
// the shared store on every call, so a replica answers for a trigger another
// replica created. A single orkflow_name equals filter narrows what the store
// reads; the whole set then applies to every trigger read.
func (s *Service) ListTriggers(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListTriggersRequest],
) (*connect.Response[orkstackv1.ListTriggersResponse], error) {
	set, err := compileFilters(req.Msg)
	if err != nil {
		return nil, err
	}
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	orkflow, _ := set.Equality("orkflow_name")
	list, err := s.triggers.List(ctx, ns.GetName(), orkflow)
	if err != nil {
		return nil, triggerConnectError(err)
	}
	out := make([]*orkstackv1.Trigger, 0, len(list))
	memo := asksMemo{}
	for _, trigger := range list {
		if row := s.triggerProtoMemo(ctx, memo, trigger); set.Match(row) {
			out = append(out, row)
		}
	}
	return connect.NewResponse(&orkstackv1.ListTriggersResponse{Triggers: out}), nil
}

// DeleteTrigger removes exactly one trigger, addressed by id and namespace. A
// trigger of another namespace is NotFound and is not deleted.
func (s *Service) DeleteTrigger(
	ctx context.Context,
	req *connect.Request[orkstackv1.DeleteTriggerRequest],
) (*connect.Response[orkstackv1.DeleteTriggerResponse], error) {
	id := req.Msg.GetId()
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	// An id that is not shaped like a generated one names no record, so it is
	// answered as absent rather than handed to the store, which would report
	// a malformed key as a storage fault.
	if !triggers.ValidID(id) {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("trigger %q not found in namespace %q", id, ns.GetName()))
	}
	ok, err := s.triggers.Delete(ctx, id, ns.GetName())
	if err != nil {
		return nil, triggerConnectError(err)
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("trigger %q not found in namespace %q", id, ns.GetName()))
	}
	return connect.NewResponse(&orkstackv1.DeleteTriggerResponse{}), nil
}

// UpdateTrigger replaces a trigger's name, pinned version and description in
// place. The id, namespace, orkflow and bearer token are untouched, so the URL
// and the token an external caller holds keep working. It runs CreateTrigger's
// checks in CreateTrigger's order: namespace, then the trigger, then the pin.
func (s *Service) UpdateTrigger(
	ctx context.Context,
	req *connect.Request[orkstackv1.UpdateTriggerRequest],
) (*connect.Response[orkstackv1.UpdateTriggerResponse], error) {
	id := req.Msg.GetId()
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	notFound := connect.NewError(connect.CodeNotFound,
		fmt.Errorf("trigger %q not found in namespace %q", id, ns.GetName()))
	if !triggers.ValidID(id) {
		return nil, notFound
	}
	if err := triggers.ValidateName(req.Msg.GetName()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	current, found, err := s.triggers.Get(ctx, id)
	if err != nil {
		return nil, triggerConnectError(err)
	}
	if !found || current.Namespace != ns.GetName() {
		return nil, notFound
	}
	// A pin must exist at update exactly as at create: a trigger must never
	// be re-pointed at a version a submission would refuse. Reading what it
	// asks for is that check, and the consent rule's input.
	// What the version asks for is read only for a save that binds or gives
	// a consent: a rename or a new description of a trigger needs no read of
	// its Orkflow. It is also the check that a new pin exists.
	version := req.Msg.GetOrkflowVersion()
	var asks consent.Asks
	var unknown, asked bool
	ask := func() error {
		if asked {
			return nil
		}
		var err error
		asks, unknown, err = s.asksForSave(ctx, ns.GetName(), current.OrkflowName, version)
		asked = err == nil
		return err
	}
	if current.OrkflowVersion != version || req.Msg.GetConsent() != "" {
		if err := ask(); err != nil {
			return nil, err
		}
	}
	who := authn.Recorded(ctx, req.Header())
	if req.Msg.GetRebind() && who.ID == "" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"--rebind records you as the trigger's binder, and this request names nobody: send Ork-Principal, or a credential the engine verifies"))
	}
	// The granting binding is read before the record's compare-and-swap,
	// which may run again, for every save: whether it binds is decided
	// inside, against the record as the swap reads it.
	granted, err := s.binderBindingFor(ctx, ns.GetName(), current.OrkflowName)
	if err != nil {
		return nil, err
	}
	var consentErr error
	updated, found, err := s.triggers.Update(ctx, id, ns.GetName(), func(t *triggers.Trigger) error {
		// The caller binds when the save changes the version the trigger
		// starts or asks to rebind; a new name or description rebinds
		// nothing. A rebind keeps the consent; a save that binds needs one
		// when the Orkflow asks for an identity (consent_save.go).
		consentErr = nil
		binds := t.OrkflowVersion != version
		if binds || req.Msg.GetConsent() != "" {
			if err := ask(); err != nil {
				consentErr = err
				return err
			}
		}
		given, err := decideConsent(consentSave{Kind: "trigger", Name: t.Name, Orkflow: t.OrkflowName, Binds: binds,
			Given: req.Msg.GetConsent(), Asks: asks, Unknown: unknown, Prior: triggerConsentOf(t.Binding), By: recordedBinder(who.ID), Now: s.now().UTC()})
		if err != nil {
			consentErr = err
			return err
		}
		switch {
		case binds || req.Msg.GetRebind():
			t.Binding = s.triggerBoundBy(who, granted, given)
		case req.Msg.GetConsent() != "":
			t.Binding = withTriggerConsent(t.Binding, given)
		}
		t.Name = req.Msg.GetName()
		t.OrkflowVersion = req.Msg.GetOrkflowVersion()
		t.Description = req.Msg.GetDescription()
		return nil
	})
	switch {
	case consentErr != nil:
		return nil, consentErr
	case errors.Is(err, triggers.ErrNameTaken):
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
			"trigger name %q is already in use in namespace %q", req.Msg.GetName(), ns.GetName()))
	case errors.Is(err, triggers.ErrUpdateContention):
		return nil, connect.NewError(connect.CodeUnavailable, err)
	case err != nil:
		return nil, triggerConnectError(err)
	case !found:
		return nil, notFound
	}
	return connect.NewResponse(&orkstackv1.UpdateTriggerResponse{Trigger: s.triggerProtoWith(ctx, updated)}), nil
}

// triggerProto renders one record on the wire. orkstack.v1.Trigger has no
// secret field, so there is nothing here to strip: the verification material
// simply has no wire representation.
func triggerProto(trigger triggers.Trigger) *orkstackv1.Trigger {
	return &orkstackv1.Trigger{
		Id:             trigger.ID,
		Namespace:      trigger.Namespace,
		OrkflowName:    trigger.OrkflowName,
		OrkflowVersion: trigger.OrkflowVersion,
		Description:    trigger.Description,
		CreatedAt:      timestamppb.New(trigger.CreatedAt),
		Name:           trigger.Name,
		Consented:      triggerConsentOf(trigger.Binding).view(),
	}
}

// triggerProtoWith renders trigger with what its Orkflow asks for now, and
// whether its consent is stale. A definition that cannot be read leaves both
// unset: "could not tell" is never "asks for nothing".
func (s *Service) triggerProtoWith(ctx context.Context, trigger triggers.Trigger) *orkstackv1.Trigger {
	return s.triggerProtoMemo(ctx, nil, trigger)
}

// triggerProtoMemo is triggerProtoWith reading each Orkflow version once
// through memo.
func (s *Service) triggerProtoMemo(ctx context.Context, memo asksMemo, trigger triggers.Trigger) *orkstackv1.Trigger {
	out := triggerProto(trigger)
	asks, err := s.asksForMemo(ctx, memo, trigger.Namespace, trigger.OrkflowName, trigger.OrkflowVersion)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("what a trigger's Orkflow asks for could not be read; it is not shown",
				slog.String("namespace", trigger.Namespace), slog.String("trigger", trigger.ID), slog.String("error", err.Error()))
		}
		return out
	}
	out.AsksFor = &orkstackv1.AsksFor{Digest: asks.Digest, Audiences: asks.Audiences, Grants: asks.Grants}
	out.ConsentStale = consentStale(out.GetConsented(), out.GetAsksFor())
	return out
}

// triggerConsentOf is the consent a trigger's binding recorded.
func triggerConsentOf(b triggers.Binding) consentRecord {
	return consentRecord{Digest: b.Consent, At: b.ConsentAt, By: b.ConsentBy, Audiences: b.ConsentAudiences, Grants: b.ConsentGrants}
}

// withTriggerConsent records c in b.
func withTriggerConsent(b triggers.Binding, c consentRecord) triggers.Binding {
	b.Consent, b.ConsentAt, b.ConsentBy, b.ConsentAudiences, b.ConsentGrants = c.Digest, c.At.UTC(), c.By, c.Audiences, c.Grants
	return b
}

// triggerBoundBy is who as a trigger's binder, with the consent the save
// decided; an asserted caller stays asserted.
func (s *Service) triggerBoundBy(who authn.RecordedPrincipal, granted *artifacts.BinderBinding, c consentRecord) triggers.Binding {
	b := triggers.Binding{Binder: recordedBinder(who.ID), Verified: who.Verified && who.ID != "", BoundAt: s.now().UTC()}
	if b.Verified {
		b.Method = recordedBinder(who.Method)
		if granted != nil {
			b.BinderBindingName, b.BinderBindingSelector, b.BinderBindingRole, b.BinderBindingScope = granted.Name, granted.Selector, granted.Role, granted.Scope
		}
	}
	return withTriggerConsent(b, c)
}

// triggerConnectError maps a store failure onto a Connect code, the way
// webhookConnectError does for the webhook registry.
func triggerConnectError(err error) error {
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	// Remaining errors are durable-store failures and must never masquerade as
	// a successful empty registry or a successful delete.
	return connect.NewError(connect.CodeInternal, err)
}
