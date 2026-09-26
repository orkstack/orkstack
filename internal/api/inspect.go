package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// InspectRun is declared by the proto; the handler lands with the engine's
// live-attempt reader.
func (s *Service) InspectRun(
	context.Context,
	*connect.Request[orkstackv1.InspectRunRequest],
) (*connect.Response[orkstackv1.InspectRunResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("InspectRun is not implemented yet"))
}
