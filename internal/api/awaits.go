package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ResolveAwait completes or rejects an awaited task of a running run.
func (s *Service) ResolveAwait(context.Context, *connect.Request[orkstackv1.ResolveAwaitRequest]) (*connect.Response[orkstackv1.ResolveAwaitResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("awaited tasks are not implemented yet"))
}
