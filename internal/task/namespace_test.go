package task

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

// fakeNamespaceService records the calls EnsureNamespace makes and replays
// canned errors, so namespace provisioning is testable without a cluster.
type fakeNamespaceService struct {
	describeErr error
	registerErr error

	described       []string
	registeredNames []string
}

func (f *fakeNamespaceService) DescribeNamespace(
	_ context.Context,
	in *workflowservice.DescribeNamespaceRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	f.described = append(f.described, in.GetNamespace())
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &workflowservice.DescribeNamespaceResponse{}, nil
}

func (f *fakeNamespaceService) RegisterNamespace(
	_ context.Context,
	in *workflowservice.RegisterNamespaceRequest,
	_ ...grpc.CallOption,
) (*workflowservice.RegisterNamespaceResponse, error) {
	f.registeredNames = append(f.registeredNames, in.GetNamespace())
	if f.registerErr != nil {
		return nil, f.registerErr
	}
	return &workflowservice.RegisterNamespaceResponse{}, nil
}

// A fresh Temporal dev cluster has no ork-main namespace. DescribeNamespace
// then fails with *serviceerror.NamespaceNotFound, which exposes its gRPC
// status via Status() rather than GRPCStatus(); classifying that as "unknown"
// makes every binary refuse to boot against the default config.
func TestEnsureNamespace_RegistersAMissingNamespace(t *testing.T) {
	svc := &fakeNamespaceService{describeErr: serviceerror.NewNamespaceNotFound(DefaultNamespace)}

	if err := ensureNamespace(context.Background(), svc, DefaultNamespace); err != nil {
		t.Fatalf("ensureNamespace: %v", err)
	}
	if len(svc.registeredNames) != 1 || svc.registeredNames[0] != DefaultNamespace {
		t.Fatalf("registered = %v, want [%s]", svc.registeredNames, DefaultNamespace)
	}
}

func TestEnsureNamespace_ExistingNamespaceIsNotReRegistered(t *testing.T) {
	svc := &fakeNamespaceService{}

	if err := ensureNamespace(context.Background(), svc, DefaultNamespace); err != nil {
		t.Fatalf("ensureNamespace: %v", err)
	}
	if len(svc.registeredNames) != 0 {
		t.Fatalf("registered = %v, want none", svc.registeredNames)
	}
}

// Two binaries booting at once race to register; losing the race is fine.
func TestEnsureNamespace_ToleratesAConcurrentRegistration(t *testing.T) {
	svc := &fakeNamespaceService{
		describeErr: serviceerror.NewNamespaceNotFound(DefaultNamespace),
		registerErr: serviceerror.NewNamespaceAlreadyExists("already exists"),
	}

	if err := ensureNamespace(context.Background(), svc, DefaultNamespace); err != nil {
		t.Fatalf("ensureNamespace: %v", err)
	}
}

func TestEnsureNamespace_ReportsAnUnexpectedDescribeFailure(t *testing.T) {
	boom := errors.New("boom")
	svc := &fakeNamespaceService{describeErr: boom}

	err := ensureNamespace(context.Background(), svc, DefaultNamespace)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
	if len(svc.registeredNames) != 0 {
		t.Fatalf("registered = %v, want none", svc.registeredNames)
	}
}
