package engine

import (
	"context"
	"errors"
	commonpb "go.temporal.io/api/common/v1"
	"testing"
	"time"

	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeWorkflowServiceAdmin stands in for Temporal's WorkflowService, narrowed
// to exactly the four calls workflowServiceAdmin declares — the same style
// internal/task/namespace_test.go's fakeNamespaceService uses for
// EnsureNamespace, so every admin read and write is testable with no live
// cluster.
type fakeWorkflowServiceAdmin struct {
	describeErr         error
	describeRetention   time.Duration
	describeNoRetention bool

	updateErr        error
	updateConfirmed  time.Duration
	updateNoResponse bool

	describedNamespaces []string
	updateRequests      []*workflowservice.UpdateNamespaceRequest

	describeExecutionErr   error
	describeExecutionFound bool
	describeExecutionClose time.Time
	describeExecutionRunID string
	describeExecutionCalls []*workflowservice.DescribeWorkflowExecutionRequest

	deleteExecutionErr   error
	deleteExecutionCalls []*workflowservice.DeleteWorkflowExecutionRequest
	// deleteGone names executions already deleted (expired): NotFound.
	deleteGone map[string]bool
}

func (f *fakeWorkflowServiceAdmin) DescribeNamespace(
	_ context.Context,
	in *workflowservice.DescribeNamespaceRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	f.describedNamespaces = append(f.describedNamespaces, in.GetNamespace())
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	if f.describeNoRetention {
		return &workflowservice.DescribeNamespaceResponse{Config: &namespacepb.NamespaceConfig{}}, nil
	}
	return &workflowservice.DescribeNamespaceResponse{
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(f.describeRetention),
		},
	}, nil
}

func (f *fakeWorkflowServiceAdmin) UpdateNamespace(
	_ context.Context,
	in *workflowservice.UpdateNamespaceRequest,
	_ ...grpc.CallOption,
) (*workflowservice.UpdateNamespaceResponse, error) {
	f.updateRequests = append(f.updateRequests, in)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	if f.updateNoResponse {
		return &workflowservice.UpdateNamespaceResponse{Config: &namespacepb.NamespaceConfig{}}, nil
	}
	return &workflowservice.UpdateNamespaceResponse{
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(f.updateConfirmed),
		},
	}, nil
}

func (f *fakeWorkflowServiceAdmin) DescribeWorkflowExecution(
	_ context.Context,
	in *workflowservice.DescribeWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.describeExecutionCalls = append(f.describeExecutionCalls, in)
	if f.describeExecutionErr != nil {
		return nil, f.describeExecutionErr
	}
	if !f.describeExecutionFound {
		return nil, serviceerror.NewNotFound("gone")
	}
	info := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: in.GetExecution().GetWorkflowId(),
			RunId:      f.describeExecutionRunID,
		},
	}
	if !f.describeExecutionClose.IsZero() {
		info.CloseTime = timestamppb.New(f.describeExecutionClose)
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}, nil
}

func (f *fakeWorkflowServiceAdmin) DeleteWorkflowExecution(
	_ context.Context,
	in *workflowservice.DeleteWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DeleteWorkflowExecutionResponse, error) {
	f.deleteExecutionCalls = append(f.deleteExecutionCalls, in)
	if f.deleteExecutionErr != nil {
		return nil, f.deleteExecutionErr
	}
	if f.deleteGone[in.GetWorkflowExecution().GetRunId()] {
		return nil, serviceerror.NewNotFound("execution already deleted")
	}
	return &workflowservice.DeleteWorkflowExecutionResponse{}, nil
}

func readerWithRetention(fake *fakeWorkflowServiceAdmin) *TemporalRunReader {
	return &TemporalRunReader{admin: fake}
}

func TestNamespaceRetention_ReadsTheClustersCurrentValue(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{describeRetention: 72 * time.Hour}
	reader := readerWithRetention(fake)

	got, err := reader.NamespaceRetention(context.Background(), "ork-main")
	if err != nil {
		t.Fatalf("NamespaceRetention: %v", err)
	}
	if got != 72*time.Hour {
		t.Errorf("retention = %s, want 72h", got)
	}
	if len(fake.describedNamespaces) != 1 || fake.describedNamespaces[0] != "ork-main" {
		t.Errorf("described = %v, want [ork-main]", fake.describedNamespaces)
	}
}

func TestNamespaceRetention_ReportsATemporalFailureUntouched(t *testing.T) {
	boom := errors.New("temporal unavailable")
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{describeErr: boom})

	_, err := reader.NamespaceRetention(context.Background(), "ork-main")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}

func TestNamespaceRetention_WithNoClientReportsItsOwnMisconfiguration(t *testing.T) {
	reader := &TemporalRunReader{}
	if _, err := reader.NamespaceRetention(context.Background(), "ork-main"); err == nil {
		t.Fatal("NamespaceRetention with no client succeeded, want an error")
	}
}

func TestSetNamespaceRetention_WritesThroughUpdateNamespaceAndReturnsWhatTemporalConfirmed(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{updateConfirmed: 168 * time.Hour}
	reader := readerWithRetention(fake)

	got, err := reader.SetNamespaceRetention(context.Background(), "ork-main", 168*time.Hour)
	if err != nil {
		t.Fatalf("SetNamespaceRetention: %v", err)
	}
	if got != 168*time.Hour {
		t.Errorf("confirmed retention = %s, want 168h", got)
	}
	if len(fake.updateRequests) != 1 {
		t.Fatalf("update requests = %d, want 1", len(fake.updateRequests))
	}
	req := fake.updateRequests[0]
	if req.GetNamespace() != "ork-main" {
		t.Errorf("update namespace = %q, want ork-main", req.GetNamespace())
	}
	if got := req.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration(); got != 168*time.Hour {
		t.Errorf("requested retention on the wire = %s, want 168h", got)
	}
}

// TestSetNamespaceRetention_RefusesANonPositiveDurationBeforeAnyCall is the
// engine half of "a non-positive duration is rejected before any call leaves
// the process": the API layer also refuses it, but the engine method refuses
// it too and never asks the fake at all.
func TestSetNamespaceRetention_RefusesANonPositiveDurationBeforeAnyCall(t *testing.T) {
	for _, retention := range []time.Duration{0, -time.Hour} {
		fake := &fakeWorkflowServiceAdmin{}
		reader := readerWithRetention(fake)

		if _, err := reader.SetNamespaceRetention(context.Background(), "ork-main", retention); err == nil {
			t.Fatalf("SetNamespaceRetention(%s) succeeded, want a refusal", retention)
		}
		if len(fake.updateRequests) != 0 {
			t.Fatalf("SetNamespaceRetention(%s) called UpdateNamespace, want no call for a non-positive duration", retention)
		}
	}
}

// TestSetNamespaceRetention_SurfacesTemporalsOwnRejectionUntouched is the
// "Temporal's own server-side minimum-retention rejection" acceptance case:
// a serviceerror.InvalidArgument from UpdateNamespace comes back as-is, never
// swallowed into a default, a zero value, or a fabricated success.
func TestSetNamespaceRetention_SurfacesTemporalsOwnRejectionUntouched(t *testing.T) {
	rejection := serviceerror.NewInvalidArgument("retention period is below the cluster's configured minimum")
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{updateErr: rejection})

	got, err := reader.SetNamespaceRetention(context.Background(), "ork-main", time.Hour)
	if !errors.Is(err, rejection) {
		t.Fatalf("err = %v, want it to wrap %v", err, rejection)
	}
	if got != 0 {
		t.Errorf("confirmed retention on a failed write = %s, want the zero value", got)
	}
}

func TestSetNamespaceRetention_WithNoClientReportsItsOwnMisconfiguration(t *testing.T) {
	reader := &TemporalRunReader{}
	if _, err := reader.SetNamespaceRetention(context.Background(), "ork-main", time.Hour); err == nil {
		t.Fatal("SetNamespaceRetention with no client succeeded, want an error")
	}
}
