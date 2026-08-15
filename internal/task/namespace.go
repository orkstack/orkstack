package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// namespaceRetention is the workflow-execution retention period used when
// EnsureNamespace registers a missing namespace.
const namespaceRetention = 72 * time.Hour

// namespaceRegistrar is the slice of Temporal's WorkflowService that namespace
// provisioning needs. Narrowing it keeps EnsureNamespace testable without a
// live cluster.
type namespaceRegistrar interface {
	DescribeNamespace(
		ctx context.Context,
		in *workflowservice.DescribeNamespaceRequest,
		opts ...grpc.CallOption,
	) (*workflowservice.DescribeNamespaceResponse, error)
	RegisterNamespace(
		ctx context.Context,
		in *workflowservice.RegisterNamespaceRequest,
		opts ...grpc.CallOption,
	) (*workflowservice.RegisterNamespaceResponse, error)
}

// EnsureNamespace makes sure the configured Temporal namespace exists on the
// cluster, registering it when it is missing. The default ORK_TEMPORAL_NAMESPACE
// is ork-main, which a fresh dev cluster does not have; provisioning it at boot
// keeps `make infra-up` + `make run-ork`/`make run-orker` working without a
// manual `temporal operator namespace create` step. Namespaces that already
// exist (including client.DefaultNamespace) are a no-op.
//
// It runs in the binary's main, never inside workflow code, and uses the
// already-dialed client's WorkflowService, so it inherits the client's
// connection and credentials.
func EnsureNamespace(ctx context.Context, temporalClient client.Client, namespace string) error {
	if namespace == "" || namespace == client.DefaultNamespace {
		return nil
	}
	return ensureNamespace(ctx, temporalClient.WorkflowService(), namespace)
}

func ensureNamespace(ctx context.Context, svc namespaceRegistrar, namespace string) error {
	if _, err := svc.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}); err == nil {
		return nil
	} else if grpcCode(err) != codes.NotFound {
		return fmt.Errorf("describe temporal namespace %q: %w", namespace, err)
	}
	if _, err := svc.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      "OrkStack default namespace (provisioned at binary startup)",
		WorkflowExecutionRetentionPeriod: durationpb.New(namespaceRetention),
	}); err != nil && grpcCode(err) != codes.AlreadyExists {
		return fmt.Errorf("register temporal namespace %q: %w", namespace, err)
	}
	return nil
}

// grpcCode reports the gRPC status code carried by err.
//
// Temporal's serviceerror types (*serviceerror.NamespaceNotFound,
// *serviceerror.NamespaceAlreadyExists, …) expose their status through a
// Status() method, NOT the GRPCStatus() method google.golang.org/grpc/status
// looks for. A bare status.Code() therefore reports Unknown for exactly the
// errors this file classifies, so check the Temporal shape first.
func grpcCode(err error) codes.Code {
	var serviceErr interface{ Status() *status.Status }
	if errors.As(err, &serviceErr) {
		if st := serviceErr.Status(); st != nil {
			return st.Code()
		}
	}
	return status.Code(err)
}
