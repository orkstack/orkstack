package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ListOrkers answers Unimplemented until the Orker fleet seam is wired.
func (s *Service) ListOrkers(
	context.Context, *connect.Request[orkstackv1.ListOrkersRequest],
) (*connect.Response[orkstackv1.ListOrkersResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ListOrkers is not implemented yet"))
}

// CordonOrker answers Unimplemented until the Orker fleet seam is wired.
func (s *Service) CordonOrker(
	context.Context, *connect.Request[orkstackv1.CordonOrkerRequest],
) (*connect.Response[orkstackv1.CordonOrkerResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("CordonOrker is not implemented yet"))
}
