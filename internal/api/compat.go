package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// CompareInterfaces is not implemented yet (interface-compatibility-gate).
func (s *Service) CompareInterfaces(
	context.Context, *connect.Request[orkstackv1.CompareInterfacesRequest],
) (*connect.Response[orkstackv1.CompareInterfacesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("CompareInterfaces is not implemented"))
}

// ListOrkflowCallers is not implemented yet (interface-compatibility-gate).
func (s *Service) ListOrkflowCallers(
	context.Context, *connect.Request[orkstackv1.ListOrkflowCallersRequest],
) (*connect.Response[orkstackv1.ListOrkflowCallersResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ListOrkflowCallers is not implemented"))
}
