package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// DiffRuns is declared by the proto contract; the comparison lands in the next
// commit. Until then it answers Unimplemented, exactly as an engine built
// before the RPC existed would.
func (s *Service) DiffRuns(
	_ context.Context,
	_ *connect.Request[orkstackv1.DiffRunsRequest],
) (*connect.Response[orkstackv1.DiffRunsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("DiffRuns is not implemented yet"))
}
