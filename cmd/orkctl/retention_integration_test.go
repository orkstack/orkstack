//go:build integration

package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// TestIntegration_CLISetsExecutionRetentionOnTheRealCluster is acceptance 7's
// end-to-end case: it goes through the SAME path an operator does —
// `orkctl admin retention get/set`, over Connect, into a real `ork` process,
// into engine.TemporalRunReader.SetNamespaceRetention, into Temporal's real
// UpdateNamespace RPC — and it verifies the change through a client this test
// dials ITSELF and never hands to `orkctl` or `ork`, so it cannot pass on a
// write that only updated in-process state.
//
// internal/engine/retention_integration_test.go covers the same write
// directly against engine.TemporalRunReader, with no process boundary, to
// isolate the engine seam from the Connect/CLI wiring. This test is the
// other half: it never calls SetNamespaceRetention itself, only the CLI.
func TestIntegration_CLISetsExecutionRetentionOnTheRealCluster(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalNamespace := isolatedTemporalNamespace("ork-retention-cli")
	t.Logf("temporal namespace %s", temporalNamespace)
	// Provisions the namespace at task.namespaceRetention's documented
	// default (72h) and closes its own bootstrap client — this test's later
	// verification client is dialed fresh, below.
	dialIsolatedNamespace(ctx, t, address, temporalNamespace)

	root := repoRoot(t)
	binDir := t.TempDir()
	orkctlPath := buildBinary(t, root, binDir, "orkctl")
	enginePath := buildBinary(t, root, binDir, "ork")

	pool := privateTaskPool()
	engineAddr := startNamespacedEngine(ctx, t, enginePath, address, temporalNamespace, pool)

	cli := func(args ...string) (string, string, int) {
		return runBinary(ctx, t, orkctlPath, root,
			append(args, "--server", "http://"+engineAddr)...)
	}
	waitForEngine(t, cli)

	// A verification handle dialed independently of `ork` and of `orkctl`:
	// nothing under test ever touches this client. It is the only thing
	// that gets to say whether the write reached the cluster.
	independent, err := client.DialContext(ctx, client.Options{
		HostPort: address, Namespace: temporalNamespace,
	})
	if err != nil {
		t.Fatalf("dial independent verification client: %v", err)
	}
	t.Cleanup(independent.Close)
	describeIndependently := func() time.Duration {
		t.Helper()
		resp, err := independent.WorkflowService().DescribeNamespace(ctx,
			&workflowservice.DescribeNamespaceRequest{Namespace: temporalNamespace})
		if err != nil {
			t.Fatalf("independent DescribeNamespace(%s): %v", temporalNamespace, err)
		}
		ttl := resp.GetConfig().GetWorkflowExecutionRetentionTtl()
		if ttl == nil {
			t.Fatalf("independent DescribeNamespace(%s) reported no retention", temporalNamespace)
		}
		return ttl.AsDuration()
	}

	// ---- orkctl admin retention get: the baseline EnsureNamespace set -----
	getStdout, getStderr, code := cli("--json", "admin", "retention", "get")
	if code != 0 {
		t.Fatalf("admin retention get exit = %d\nstdout:\n%s\nstderr:\n%s", code, getStdout, getStderr)
	}
	baseline := decodeExecutionRetention(t, getStdout)
	if baseline.TemporalNamespace != temporalNamespace {
		t.Errorf("temporal_namespace = %q, want %q", baseline.TemporalNamespace, temporalNamespace)
	}
	if got := baseline.retention(t); got != 72*time.Hour {
		// EnsureNamespace's documented default is 72h. Fail loudly if the
		// baseline assumption this test rests on has changed elsewhere.
		t.Fatalf("baseline retention = %s, want 72h; EnsureNamespace's default may have changed", got)
	}
	independentBefore := describeIndependently()
	if independentBefore != 72*time.Hour {
		t.Fatalf("independent baseline retention = %s, want 72h", independentBefore)
	}

	// ---- orkctl admin retention set: the write under test, via the CLI ----
	const want = 5 * time.Hour
	setStdout, setStderr, code := cli("--json", "admin", "retention", "set", want.String())
	if code != 0 {
		t.Fatalf("admin retention set exit = %d\nstdout:\n%s\nstderr:\n%s", code, setStdout, setStderr)
	}
	confirmed := decodeExecutionRetention(t, setStdout)
	if confirmed.TemporalNamespace != temporalNamespace {
		t.Errorf("temporal_namespace = %q, want %q", confirmed.TemporalNamespace, temporalNamespace)
	}
	confirmedDuration := confirmed.retention(t)
	if confirmedDuration != want {
		t.Fatalf("admin retention set reported %s, want %s", confirmedDuration, want)
	}

	// THE assertion: a SEPARATE, directly-dialed DescribeNamespace call
	// confirms the cluster's own state changed. This is the check that
	// cannot pass on a write that only updated something in-process — it
	// never reuses `independent`'s earlier read, `ork`'s reader, or
	// `orkctl`'s own echoed response.
	independentAfter := describeIndependently()
	if independentAfter != want {
		t.Fatalf("independent DescribeNamespace(%s) after `orkctl admin retention set` reports %s, "+
			"want %s: the CLI write did not reach the cluster", temporalNamespace, independentAfter, want)
	}
	t.Logf("EVIDENCE namespace=%s before=%s cli_set=%s cli_confirmed=%s independent_after=%s",
		temporalNamespace, independentBefore, want, confirmedDuration, independentAfter)

	// ---- orkctl admin retention get again: the CLI read agrees -----------
	reGetStdout, reGetStderr, code := cli("--json", "admin", "retention", "get")
	if code != 0 {
		t.Fatalf("admin retention get (after set) exit = %d\nstdout:\n%s\nstderr:\n%s",
			code, reGetStdout, reGetStderr)
	}
	afterGet := decodeExecutionRetention(t, reGetStdout)
	afterGetDuration := afterGet.retention(t)
	if afterGetDuration != want {
		t.Fatalf("admin retention get after the write reports %s, want %s", afterGetDuration, want)
	}

	// ---- a non-positive duration is refused before it reaches Temporal ---
	_, setZeroStderr, code := cli("admin", "retention", "set", "0s")
	if code == 0 {
		t.Fatal("admin retention set 0s succeeded, want a refusal")
	}
	if !strings.Contains(setZeroStderr, "invalid_argument") {
		t.Errorf("the refusal is not an invalid argument:\n%s", setZeroStderr)
	}
	unchanged := describeIndependently()
	if unchanged != want {
		t.Fatalf("retention after a refused CLI write = %s, want it unchanged at %s", unchanged, want)
	}
}

// executionRetentionJSON is `orkctl admin retention get/set --json`, decoded.
// Retention is left as a raw string because durationpb's protojson rendering
// (e.g. "18000s") is what the CLI actually prints; retention() below turns it
// into a time.Duration for comparison.
type executionRetentionJSON struct {
	TemporalNamespace string `json:"temporalNamespace"`
	Retention         string `json:"retention"`
}

func decodeExecutionRetention(t *testing.T, stdout string) executionRetentionJSON {
	t.Helper()
	var decoded executionRetentionJSON
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("admin retention --json is not valid JSON: %v\n%s", err, stdout)
	}
	return decoded
}

// retention parses protojson's Duration rendering ("18000s", "1.5s"), which
// is Go's time.ParseDuration syntax restricted to a trailing "s" and
// (optionally) a fractional part — time.ParseDuration accepts exactly that
// shape.
func (e executionRetentionJSON) retention(t *testing.T) time.Duration {
	t.Helper()
	if e.Retention == "" {
		t.Fatalf("empty retention in %+v", e)
	}
	d, err := time.ParseDuration(e.Retention)
	if err != nil {
		t.Fatalf("parse retention %q: %v", e.Retention, err)
	}
	return d
}
