//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestIntegration_SetNamespaceRetentionReachesTheRealCluster proves the WRITE
// half of the deployment-wide execution-retention seam against a live
// Temporal, the way acceptance requires: the change is verified through a
// SEPARATE, direct DescribeNamespace call on a namespaceRegistrar-free client
// dialed independently of the TemporalRunReader under test — never merely by
// re-reading through the same reader's own NamespaceRetention — so this test
// cannot pass on a write that only updated in-process state.
func TestIntegration_SetNamespaceRetentionReachesTheRealCluster(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	namespace := fmt.Sprintf("%s-retention-%d-%d", baseNamespace(), os.Getpid(),
		time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialRoutingCluster(t, ctx, address, namespace)

	reader := engine.NewTemporalRunReader(temporalClient)

	// A second, independent handle on the SAME cluster — dialed fresh, never
	// shared with the reader under test — is what makes the later check
	// prove the write left this process, rather than proving only that the
	// reader can echo its own last write back to itself.
	independent, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		t.Fatalf("dial independent verification client: %v", err)
	}
	t.Cleanup(independent.Close)

	describeIndependently := func() time.Duration {
		t.Helper()
		resp, err := independent.WorkflowService().DescribeNamespace(ctx,
			&workflowservice.DescribeNamespaceRequest{Namespace: namespace})
		if err != nil {
			t.Fatalf("independent DescribeNamespace(%s): %v", namespace, err)
		}
		ttl := resp.GetConfig().GetWorkflowExecutionRetentionTtl()
		if ttl == nil {
			t.Fatalf("independent DescribeNamespace(%s) reported no retention", namespace)
		}
		return ttl.AsDuration()
	}

	// EnsureNamespace (dialRoutingCluster) provisioned this namespace at
	// task.namespaceRetention (72h). Confirmed independently before this test
	// changes anything, so the assertion below is a real transition and not
	// a value that was already there.
	before := describeIndependently()
	if before != 72*time.Hour {
		t.Fatalf("freshly provisioned namespace %q retention = %s, want 72h (this test's "+
			"baseline assumption); EnsureNamespace's default may have changed", namespace, before)
	}

	const want = 2 * time.Hour
	confirmed, err := reader.SetNamespaceRetention(ctx, namespace, want)
	if err != nil {
		t.Fatalf("SetNamespaceRetention(%s, %s): %v", namespace, want, err)
	}
	if confirmed != want {
		t.Fatalf("SetNamespaceRetention reported %s, want %s", confirmed, want)
	}

	// THE assertion: read back through a client this test dialed itself,
	// never through `reader`.
	after := describeIndependently()
	if after != want {
		t.Fatalf("independent DescribeNamespace(%s) after the write reports %s, want %s: "+
			"the write did not reach the cluster", namespace, after, want)
	}
	t.Logf("EVIDENCE namespace=%s before=%s requested=%s confirmed_by_writer=%s "+
		"confirmed_independently=%s", namespace, before, want, confirmed, after)

	// And the refusal Temporal itself owns is not swallowed: a non-positive
	// duration never leaves this process (engine-side refusal, no call), and
	// SetNamespaceRetention's own contract is that a duration Temporal DOES
	// reject comes back untouched. This cluster's dev-server minimum is not
	// asserted here — only that the engine refuses locally before asking.
	if _, err := reader.SetNamespaceRetention(ctx, namespace, 0); err == nil {
		t.Fatal("SetNamespaceRetention(0) succeeded, want a local refusal")
	}
	unchanged := describeIndependently()
	if unchanged != want {
		t.Fatalf("retention after a refused write = %s, want it unchanged at %s", unchanged, want)
	}
}
