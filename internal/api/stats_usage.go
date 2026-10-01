package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// GetUsage is declared by the proto before its implementation lands; until
// then it answers Unimplemented, as an older replica does.
func (h *StatsHandler) GetUsage(
	context.Context,
	*connect.Request[orkstackv1.GetUsageRequest],
) (*connect.Response[orkstackv1.GetUsageResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("GetUsage is not implemented yet"))
}
