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
	})
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

// ListTriggers returns one namespace's triggers, narrowed to one orkflow when
// orkflow_name is set. It reads the shared store on every call, so a replica
// answers for a trigger another replica created.
func (s *Service) ListTriggers(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListTriggersRequest],
) (*connect.Response[orkstackv1.ListTriggersResponse], error) {
	ns, err := s.namespace(ctx, req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	list, err := s.triggers.List(ctx, ns.GetName(), req.Msg.GetOrkflowName())
	if err != nil {
		return nil, triggerConnectError(err)
	}
	out := make([]*orkstackv1.Trigger, 0, len(list))
	for _, trigger := range list {
		out = append(out, triggerProto(trigger))
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
