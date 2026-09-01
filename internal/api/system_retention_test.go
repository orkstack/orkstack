package api_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// fakeRetention stands in for engine.TemporalRunReader's retention seam: the
// deployment-wide Temporal cluster setting, with no cluster in this process.
// It records what it was asked so a test can prove the handler asked at all
// (or, for the refused-before-any-call case, that it did NOT).
type fakeRetention struct {
	current  time.Duration
	readErr  error
	confirm  time.Duration
	writeErr error

	readNamespaces  []string
	writeNamespaces []string
	writeRequests   []time.Duration
}

func (f *fakeRetention) NamespaceRetention(_ context.Context, namespace string) (time.Duration, error) {
	f.readNamespaces = append(f.readNamespaces, namespace)
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.current, nil
}

func (f *fakeRetention) SetNamespaceRetention(
	_ context.Context,
	namespace string,
	retention time.Duration,
) (time.Duration, error) {
	f.writeNamespaces = append(f.writeNamespaces, namespace)
	f.writeRequests = append(f.writeRequests, retention)
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.confirm, nil
}

// retentionFixture wires a Service with a fake reader (satisfying the router
// binding's other seams) and the given retention admin, over one fixed
// Temporal namespace — exactly how `ork` wires Config.TemporalNamespace and
// Config.Retention (cmd/ork/main.go).
func retentionFixture(t *testing.T, retention *fakeRetention) *api.Service {
	t.Helper()
	return api.NewService(api.Config{
		Reader:            &fakeReader{},
		Retention:         retention,
		TemporalNamespace: "ork-main",
	})
}

// TestGetExecutionRetention_ReadsLiveFromTheCluster is the successful-read
// case: the handler answers with what the fake reported, naming the TEMPORAL
// namespace it asked about, through the same binding GetRun uses.
func TestGetExecutionRetention_ReadsLiveFromTheCluster(t *testing.T) {
	retention := &fakeRetention{current: 72 * time.Hour}
	svc := retentionFixture(t, retention)

	resp, err := svc.GetExecutionRetention(context.Background(),
		connect.NewRequest(&orkstackv1.GetExecutionRetentionRequest{}))
	if err != nil {
		t.Fatalf("GetExecutionRetention: %v", err)
	}
	if got := resp.Msg.GetTemporalNamespace(); got != "ork-main" {
		t.Errorf("temporal_namespace = %q, want ork-main", got)
	}
	if got := resp.Msg.GetRetention().AsDuration(); got != 72*time.Hour {
		t.Errorf("retention = %s, want 72h", got)
	}
	if len(retention.readNamespaces) != 1 || retention.readNamespaces[0] != "ork-main" {
		t.Errorf("read namespaces = %v, want [ork-main]", retention.readNamespaces)
	}
}

// TestGetExecutionRetention_UnavailableClusterIsNotSwallowed proves a read
// failure surfaces rather than answering a fabricated zero value.
func TestGetExecutionRetention_UnavailableClusterIsNotSwallowed(t *testing.T) {
	boom := errors.New("temporal unreachable")
	svc := retentionFixture(t, &fakeRetention{readErr: boom})

	_, err := svc.GetExecutionRetention(context.Background(),
		connect.NewRequest(&orkstackv1.GetExecutionRetentionRequest{}))
	if err == nil {
		t.Fatal("GetExecutionRetention succeeded, want the cluster failure surfaced")
	}
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want it to wrap %v", err, boom)
	}
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("code = %v, want Unavailable", connect.CodeOf(err))
	}
}

// TestUpdateExecutionRetention_WritesThroughToTheCluster is the
// successful-write case, and it also proves the response's retention is the
// value the CLUSTER confirmed, not merely the request echoed back: they
// differ here on purpose.
func TestUpdateExecutionRetention_WritesThroughToTheCluster(t *testing.T) {
	retention := &fakeRetention{confirm: 200 * time.Hour}
	svc := retentionFixture(t, retention)

	resp, err := svc.UpdateExecutionRetention(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateExecutionRetentionRequest{
			Retention: durationpb.New(168 * time.Hour),
		}))
	if err != nil {
		t.Fatalf("UpdateExecutionRetention: %v", err)
	}
	if got := resp.Msg.GetTemporalNamespace(); got != "ork-main" {
		t.Errorf("temporal_namespace = %q, want ork-main", got)
	}
	if got := resp.Msg.GetRetention().AsDuration(); got != 200*time.Hour {
		t.Errorf("retention = %s, want the cluster-confirmed 200h, not the requested 168h", got)
	}
	if len(retention.writeRequests) != 1 || retention.writeRequests[0] != 168*time.Hour {
		t.Errorf("write requests = %v, want [168h]", retention.writeRequests)
	}
	if len(retention.writeNamespaces) != 1 || retention.writeNamespaces[0] != "ork-main" {
		t.Errorf("write namespaces = %v, want [ork-main]", retention.writeNamespaces)
	}
}

// TestUpdateExecutionRetention_ARejectedWriteIsSurfacedNotSwallowed is
// acceptance's "a rejected write (Temporal error surfaced, not swallowed)":
// Temporal's own server-side minimum-retention rejection comes back to the
// caller as InvalidArgument, carrying Temporal's own message, never a
// fabricated success and never a silently accepted default.
func TestUpdateExecutionRetention_ARejectedWriteIsSurfacedNotSwallowed(t *testing.T) {
	rejection := serviceerror.NewInvalidArgument("retention period is below the cluster's configured minimum")
	svc := retentionFixture(t, &fakeRetention{writeErr: rejection})

	resp, err := svc.UpdateExecutionRetention(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateExecutionRetentionRequest{
			Retention: durationpb.New(time.Hour),
		}))
	if err == nil {
		t.Fatalf("UpdateExecutionRetention succeeded with response %v, want Temporal's rejection surfaced", resp)
	}
	if !errors.Is(err, rejection) {
		t.Errorf("err = %v, want it to wrap %v", err, rejection)
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument (Temporal's own classification)", connect.CodeOf(err))
	}
}

// TestUpdateExecutionRetention_RefusesANonPositiveDurationBeforeAnyCall is
// acceptance's other write refusal: a non-positive duration never reaches the
// cluster at all.
func TestUpdateExecutionRetention_RefusesANonPositiveDurationBeforeAnyCall(t *testing.T) {
	for name, retention := range map[string]*durationpb.Duration{
		"zero":     durationpb.New(0),
		"negative": durationpb.New(-time.Hour),
		"unset":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeRetention{}
			svc := retentionFixture(t, fake)

			_, err := svc.UpdateExecutionRetention(context.Background(),
				connect.NewRequest(&orkstackv1.UpdateExecutionRetentionRequest{Retention: retention}))
			if err == nil {
				t.Fatal("UpdateExecutionRetention succeeded, want a refusal")
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", connect.CodeOf(err))
			}
			if len(fake.writeRequests) != 0 {
				t.Errorf("SetNamespaceRetention was called with %v; a non-positive duration must never leave the process",
					fake.writeRequests)
			}
		})
	}
}

// TestExecutionRetention_WithNoRetentionAdminAnswersUnimplemented is what an
// engine that never wired Config.Retention honestly knows: neither RPC
// fabricates a value.
func TestExecutionRetention_WithNoRetentionAdminAnswersUnimplemented(t *testing.T) {
	svc := api.NewService(api.Config{Reader: &fakeReader{}, TemporalNamespace: "ork-main"})

	if _, err := svc.GetExecutionRetention(context.Background(),
		connect.NewRequest(&orkstackv1.GetExecutionRetentionRequest{})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("GetExecutionRetention code = %v, want Unimplemented", connect.CodeOf(err))
	}
	if _, err := svc.UpdateExecutionRetention(context.Background(),
		connect.NewRequest(&orkstackv1.UpdateExecutionRetentionRequest{
			Retention: durationpb.New(time.Hour),
		})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Errorf("UpdateExecutionRetention code = %v, want Unimplemented", connect.CodeOf(err))
	}
}

var _ engine.RetentionAdmin = (*fakeRetention)(nil)
