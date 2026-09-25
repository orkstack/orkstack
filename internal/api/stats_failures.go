package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// GetFailureBreakdown is declared by the proto; its implementation follows.
func (h *StatsHandler) GetFailureBreakdown(
	context.Context,
	*connect.Request[orkstackv1.GetFailureBreakdownRequest],
) (*connect.Response[orkstackv1.GetFailureBreakdownResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented,
		errors.New("GetFailureBreakdown is not implemented yet"))
}
