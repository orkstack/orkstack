package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// GetFlakeIndex is not implemented yet.
func (h *StatsHandler) GetFlakeIndex(
	context.Context,
	*connect.Request[orkstackv1.GetFlakeIndexRequest],
) (*connect.Response[orkstackv1.GetFlakeIndexResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("GetFlakeIndex is not implemented"))
}
