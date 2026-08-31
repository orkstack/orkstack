//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

func applyDeletionTestOrkflow(
	t *testing.T,
	ctx context.Context,
	svc *api.Service,
	name string,
) *orkstackv1.Orkflow {
	t.Helper()
	response, err := svc.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: helloYAMLNamed(t, name),
	}))
	if err != nil {
		t.Fatalf("ApplyOrkflow(%s): %v", name, err)
	}
	return response.Msg.GetOrkflow()
}

func isNonTerminalRun(run *orkstackv1.Run, runID string) bool {
	if run.GetId() != runID {
		return false
	}
	switch run.GetStatus() {
	case orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_CANCELED,
		orkstackv1.Status_STATUS_SKIPPED:
		return false
	default:
		return true
	}
}

func waitForDeletionBlocker(
	t *testing.T,
	ctx context.Context,
	reader *engine.TemporalRunReader,
	namespace, orkflowName, runID string,
) {
	t.Helper()
	deadline := time.NewTimer(45 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		listed, err := reader.ListRuns(ctx, engine.RunQuery{
			Namespace:   namespace,
			OrkflowName: orkflowName,
			Limit:       engine.DefaultRunPageSize,
		})
		if err != nil {
			t.Fatalf("TemporalRunReader.ListRuns: %v", err)
		}
		for _, run := range listed {
			if isNonTerminalRun(run, runID) {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("real Temporal visibility never reported non-terminal run %s for orkflow %s", runID, orkflowName)
		case <-poll.C:
		case <-ctx.Done():
			t.Fatalf("waiting for Temporal visibility: %v", ctx.Err())
		}
	}
}

// TestIntegration_DeleteOrkflowRefusesARealTemporalRun proves the deletion
// safety boundary against the two production systems it crosses: definitions
// live in S3, while run existence comes from Temporal visibility. The run has
// deliberately no worker, so it remains non-terminal without a sleep or a
// workload image and the real TemporalRunReader must find it before deletion.
func TestIntegration_DeleteOrkflowRefusesARealTemporalRun(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	temporalNamespace := fmt.Sprintf("%s-orkflow-delete-%d-%d", baseNamespace(), os.Getpid(),
		time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialRoutingCluster(t, ctx, address, temporalNamespace)
	reader := engine.NewTemporalRunReader(temporalClient)
	store := newDurableOrkflowStore(t)
	svc := api.NewService(api.Config{
		Orkflows: store,
		Reader:   reader,
	})

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	blockedName := "delete-blocked-" + suffix
	emptyName := "delete-empty-" + suffix
	blocked := applyDeletionTestOrkflow(t, ctx, svc, blockedName)
	applyDeletionTestOrkflow(t, ctx, svc, emptyName)

	runID := fmt.Sprintf("ork-delete-blocker-%d", time.Now().UnixNano())
	starter := engine.NewTemporalStarter(temporalClient, task.DefaultQueue)
	if err := starter.StartRun(ctx, runID, blocked, nil, engine.RunPlan{
		Namespace: blocked.GetNamespace(),
		Routes:    map[string]string{task.DefaultPool: task.DefaultQueue},
	}); err != nil {
		t.Fatalf("StartRun(%s): %v", runID, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = temporalClient.TerminateWorkflow(cleanupCtx, runID, "", "orkflow deletion integration cleanup")
	})

	// Temporal visibility is eventually consistent. Wait until the production
	// reader observes the exact run before testing the deletion boundary.
	waitForDeletionBlocker(t, ctx, reader, blocked.GetNamespace(), blockedName, runID)

	_, err = svc.DeleteOrkflow(ctx, connect.NewRequest(&orkstackv1.DeleteOrkflowRequest{Name: blockedName}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("DeleteOrkflow(%s) error = %v (code %v), want FailedPrecondition", blockedName, err, connect.CodeOf(err))
	}
	if message := err.Error(); !strings.Contains(message, blockedName) || !strings.Contains(message, "1 run(s) block deletion") {
		t.Fatalf("DeleteOrkflow refusal = %q, want orkflow name and blocking-run count", message)
	}
	if _, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: blockedName})); err != nil {
		t.Fatalf("GetOrkflow after refusal: %v; refusal partially deleted the definition", err)
	}

	if _, err := svc.DeleteOrkflow(ctx, connect.NewRequest(&orkstackv1.DeleteOrkflowRequest{Name: emptyName})); err != nil {
		t.Fatalf("DeleteOrkflow(%s without runs): %v", emptyName, err)
	}
	if _, err := svc.GetOrkflow(ctx, connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: emptyName})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetOrkflow after deletion error = %v (code %v), want NotFound", err, connect.CodeOf(err))
	}
	if _, err := svc.ListOrkflowVersions(ctx, connect.NewRequest(&orkstackv1.ListOrkflowVersionsRequest{Name: emptyName})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("ListOrkflowVersions after deletion error = %v (code %v), want NotFound", err, connect.CodeOf(err))
	}
}
