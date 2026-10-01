package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ListReleases is not implemented yet.
func (s *Service) ListReleases(context.Context, *connect.Request[orkstackv1.ListReleasesRequest]) (*connect.Response[orkstackv1.ListReleasesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("releases are not implemented yet"))
}

// RampRelease is not implemented yet.
func (s *Service) RampRelease(context.Context, *connect.Request[orkstackv1.RampReleaseRequest]) (*connect.Response[orkstackv1.RampReleaseResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("releases are not implemented yet"))
}

// PromoteRelease is not implemented yet.
func (s *Service) PromoteRelease(context.Context, *connect.Request[orkstackv1.PromoteReleaseRequest]) (*connect.Response[orkstackv1.PromoteReleaseResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("releases are not implemented yet"))
}
