package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ExportRunEvidence is declared by the proto; the handler lands with the
// evidence package.
func (s *Service) ExportRunEvidence(context.Context, *connect.Request[orkstackv1.ExportRunEvidenceRequest], *connect.ServerStream[orkstackv1.EvidenceChunk]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.New("ExportRunEvidence is not implemented"))
}
