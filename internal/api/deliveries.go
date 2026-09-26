package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The delivery-record RPCs arrive with the proto; their store and handlers
// land in the change that records deliveries. Until then each is refused
// explicitly rather than answered with an empty listing.
var errDeliveriesNotImplemented = errors.New("delivery records are not implemented by this build")

// ListDeliveries is not implemented by this build.
func (s *Service) ListDeliveries(context.Context, *connect.Request[orkstackv1.ListDeliveriesRequest]) (*connect.Response[orkstackv1.ListDeliveriesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errDeliveriesNotImplemented)
}

// GetDelivery is not implemented by this build.
func (s *Service) GetDelivery(context.Context, *connect.Request[orkstackv1.GetDeliveryRequest]) (*connect.Response[orkstackv1.GetDeliveryResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errDeliveriesNotImplemented)
}

// RedeliverWebhook is not implemented by this build.
func (s *Service) RedeliverWebhook(context.Context, *connect.Request[orkstackv1.RedeliverWebhookRequest]) (*connect.Response[orkstackv1.RedeliverWebhookResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errDeliveriesNotImplemented)
}
