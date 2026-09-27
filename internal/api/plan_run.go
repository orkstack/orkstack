package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// PlanRun is not implemented yet; the proto lands first.
func (s *Service) PlanRun(
	_ context.Context,
	_ *connect.Request[orkstackv1.PlanRunRequest],
) (*connect.Response[orkstackv1.PlanRunResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("PlanRun is not implemented"))
}
