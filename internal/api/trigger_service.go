package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
	if _, err := s.orkflows.Get(ctx, ns.GetName(), name, version); err != nil {
		return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
			name, version, ns.GetName())
	}

	trigger, secret, err := s.triggers.Create(ctx, triggers.Registration{
		Namespace:      ns.GetName(),
		OrkflowName:    name,
		OrkflowVersion: version,
		Description:    req.Msg.GetDescription(),
		Name:           req.Msg.GetName(),
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
		Trigger: triggerProto(trigger),
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
	for _, trigger := range list {
		if row := triggerProto(trigger); set.Match(row) {
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
	// be re-pointed at a version a submission would refuse.
	if version := req.Msg.GetOrkflowVersion(); version != "" {
		if _, err := s.orkflows.Get(ctx, ns.GetName(), current.OrkflowName, version); err != nil {
			return nil, orkflowStoreError(err, "orkflow %q version %q in namespace %q",
				current.OrkflowName, version, ns.GetName())
		}
	}
	updated, found, err := s.triggers.Update(ctx, id, ns.GetName(), func(t *triggers.Trigger) error {
		t.Name = req.Msg.GetName()
		t.OrkflowVersion = req.Msg.GetOrkflowVersion()
		t.Description = req.Msg.GetDescription()
		return nil
	})
	switch {
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
	return connect.NewResponse(&orkstackv1.UpdateTriggerResponse{Trigger: triggerProto(updated)}), nil
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
	}
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
