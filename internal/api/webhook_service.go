package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RegisterWebhook creates one subscription, deployment-wide unless it names a
// scope. Its signing secret appears only in this response; every later read
// maps to Webhook. A scope is checked for shape, never for existence: a
// subscription may be registered before the orkflow it names is applied.
func (s *Service) RegisterWebhook(ctx context.Context, req *connect.Request[orkstackv1.RegisterWebhookRequest]) (*connect.Response[orkstackv1.RegisterWebhookResponse], error) {
	events, err := webhookEventsFromProto(req.Msg.GetEvents())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if _, err := webhooks.ValidateURL(req.Msg.GetUrl()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := webhooks.ValidateScope(req.Msg.GetNamespace(), req.Msg.GetOrkflowName(), req.Msg.GetTaskId(), events); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := webhooks.ValidateName(req.Msg.GetName(), req.Msg.GetNamespace()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	hook, err := s.webhooks.RegisterContext(ctx, webhooks.Registration{
		URL: req.Msg.GetUrl(), Events: events, Secret: req.Msg.GetSecret(),
		Namespace: req.Msg.GetNamespace(), OrkflowName: req.Msg.GetOrkflowName(), TaskID: req.Msg.GetTaskId(),
		Name: req.Msg.GetName(),
	})
	if errors.Is(err, webhooks.ErrNameTaken) {
		return nil, connect.NewError(connect.CodeAlreadyExists, webhookNameTaken(req.Msg.GetName(), req.Msg.GetNamespace()))
	}
	if err != nil {
		return nil, webhookConnectError(err)
	}
	return connect.NewResponse(&orkstackv1.RegisterWebhookResponse{Webhook: webhookProto(hook), Secret: hook.Secret}), nil
}

// ListWebhooks lists the deployment's subscriptions, narrowed by filters
// applied to every subscription read. `namespace=` (empty) selects the
// deployment-wide ones.
func (s *Service) ListWebhooks(ctx context.Context, req *connect.Request[orkstackv1.ListWebhooksRequest]) (*connect.Response[orkstackv1.ListWebhooksResponse], error) {
	set, err := compileFilters(req.Msg)
	if err != nil {
		return nil, err
	}
	hooks, err := s.webhooks.ListContext(ctx)
	if err != nil {
		return nil, webhookConnectError(err)
	}
	out := make([]*orkstackv1.Webhook, 0, len(hooks))
	for _, hook := range hooks {
		if row := webhookProto(hook); set.Match(row) {
			out = append(out, row)
		}
	}
	return connect.NewResponse(&orkstackv1.ListWebhooksResponse{Webhooks: out}), nil
}

func (s *Service) DeleteWebhook(ctx context.Context, req *connect.Request[orkstackv1.DeleteWebhookRequest]) (*connect.Response[orkstackv1.DeleteWebhookResponse], error) {
	ok, err := s.webhooks.DeleteContext(ctx, req.Msg.GetId())
	if err != nil {
		return nil, webhookConnectError(err)
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("webhook %q not found", req.Msg.GetId()))
	}
	return connect.NewResponse(&orkstackv1.DeleteWebhookResponse{}), nil
}

// UpdateWebhook replaces a subscription's name, URL, events and scope in
// place, on the register path's own checks in the register path's order. The
// id and the signing secret are untouched, so a receiver keeps verifying and
// the subscription's delivery history stays continuous.
func (s *Service) UpdateWebhook(ctx context.Context, req *connect.Request[orkstackv1.UpdateWebhookRequest]) (*connect.Response[orkstackv1.UpdateWebhookResponse], error) {
	msg := req.Msg
	events, err := webhookEventsFromProto(msg.GetEvents())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	target, err := webhooks.ValidateURL(msg.GetUrl())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := webhooks.ValidateScope(msg.GetNamespace(), msg.GetOrkflowName(), msg.GetTaskId(), events); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := webhooks.ValidateName(msg.GetName(), msg.GetNamespace()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	hook, found, err := s.webhooks.UpdateContext(ctx, msg.GetId(), func(w *webhooks.Webhook) error {
		w.Name = msg.GetName()
		w.URL = target
		w.Events = events
		w.Namespace = msg.GetNamespace()
		w.OrkflowName = msg.GetOrkflowName()
		w.TaskID = msg.GetTaskId()
		return nil
	})
	switch {
	case errors.Is(err, webhooks.ErrNameTaken):
		return nil, connect.NewError(connect.CodeAlreadyExists, webhookNameTaken(msg.GetName(), msg.GetNamespace()))
	case errors.Is(err, webhooks.ErrUpdateContention):
		return nil, connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, artifacts.ErrInvalidWebhookID):
		// An id no store can address is absent, not a storage fault.
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("webhook %q not found", msg.GetId()))
	case err != nil:
		return nil, webhookConnectError(err)
	case !found:
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("webhook %q not found", msg.GetId()))
	}
	return connect.NewResponse(&orkstackv1.UpdateWebhookResponse{Webhook: webhookProto(hook)}), nil
}

// webhookNameTaken is the AlreadyExists message for a name another
// subscription of the namespace scope holds.
func webhookNameTaken(name, namespace string) error {
	return fmt.Errorf("webhook name %q is already in use in namespace %q", name, namespace)
}

func webhookEventsFromProto(events []orkstackv1.EventType) ([]webhooks.Event, error) {
	out := make([]webhooks.Event, 0, len(events))
	for _, event := range events {
		parsed, err := webhooks.ParseEvent(event.String())
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func webhookProto(hook webhooks.Webhook) *orkstackv1.Webhook {
	events := make([]orkstackv1.EventType, 0, len(hook.Events))
	for _, event := range hook.Events {
		if value, ok := orkstackv1.EventType_value["EVENT_TYPE_"+string(event)]; ok {
			events = append(events, orkstackv1.EventType(value))
		}
	}
	return &orkstackv1.Webhook{
		Id: hook.ID, Url: hook.URL, Events: events, CreatedAt: timestamppb.New(hook.CreatedAt),
		Namespace: hook.Namespace, OrkflowName: hook.OrkflowName, TaskId: hook.TaskID,
		Name: hook.Name,
	}
}

func webhookConnectError(err error) error {
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	// Remaining errors are durable-store failures and must never masquerade as
	// a successful empty registry.
	return connect.NewError(connect.CodeInternal, err)
}
