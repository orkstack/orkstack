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

	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// TestIntegration_CLISetsMaximumRunRetentionOnTheRealCluster is the
// end-to-end case: it goes through the SAME path an operator does —
// `orkctl admin retention get/set`, over Connect, into a real `ork` process,
// into archive.RetentionController, into engine.TemporalRunReader's
// SetNamespaceRetention, into Temporal's real UpdateNamespace RPC — and it
// verifies the underlying cluster's backstop TTL through a client this test
// dials ITSELF and never hands to `orkctl` or `ork`, so it cannot pass on a
// write that only updated in-process state.
//
// internal/engine/retention_integration_test.go covers the same underlying
// write directly against engine.TemporalRunReader, with no process boundary,
// to isolate the engine seam from the Connect/CLI wiring. This test is the
// other half: it never calls SetNamespaceRetention itself, only the CLI.
//
// The CLI's own output NEVER names the underlying Temporal namespace or its
// TTL (AGENTS.md §7): what it reports is the deployment's maximum run
// retention. The independent check below is what proves the underlying
// mechanism, precisely because the CLI itself must never be asked to.
func TestIntegration_CLISetsMaximumRunRetentionOnTheRealCluster(t *testing.T) {
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
	orkctlPath := testbin.Path(t, "./cmd/orkctl")
	enginePath := testbin.Path(t, "./cmd/ork")

	pool := privateTaskPool()
	engineAddr := startNamespacedEngine(ctx, t, enginePath, address, temporalNamespace, pool)

	cli := func(args ...string) (string, string, int) {
		return runBinary(ctx, t, orkctlPath, root,
			append(args, "--server", "http://"+engineAddr)...)
	}
	waitForEngine(t, cli)

	// A verification handle dialed independently of `ork` and of `orkctl`:
	// nothing under test ever touches this client. It is the only thing
	// that gets to say whether the underlying cluster's own TTL changed —
	// which `ork` derives automatically as maximum + its enforcement margin
	// (ORK_CLEANUP_RETENTION_BUFFER, default 24h), never the raw requested
	// value.
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
	const margin = 24 * time.Hour // ork's ORK_CLEANUP_RETENTION_BUFFER default

	// ---- orkctl admin retention get: whatever `ork` currently reports -----
	//
	// The maximum run retention is a deployment-wide document
	// (metadata/settings/retention-policy.json) in the SAME shared bucket
	// every integration test in this package runs against — it is not scoped
	// to this test's own isolated Temporal namespace the way the namespace's
	// own provisioned TTL is (internal/engine/retention_integration_test.go
	// covers that isolated case). So this test asserts the INVARIANT
	// (independent backstop == whatever the maximum is, plus the margin) and
	// the DELTA a CLI-driven write produces, never a specific starting
	// number: a prior integration run against this same bucket may have left
	// the maximum at anything.
	getStdout, getStderr, code := cli("--json", "admin", "retention", "get")
	if code != 0 {
		t.Fatalf("admin retention get exit = %d\nstdout:\n%s\nstderr:\n%s", code, getStdout, getStderr)
	}
	baseline := decodeRetentionPolicy(t, getStdout).maxRunRetention(t)
	independentBefore := describeIndependently()
	if independentBefore != baseline+margin {
		t.Fatalf("independent backstop TTL = %s, want the reported maximum %s + %s margin", independentBefore, baseline, margin)
	}

	// ---- orkctl admin retention set: the write under test, via the CLI ----
	// Guaranteed to differ from baseline (never equal, always positive), so
	// the assertions below prove a real transition rather than a no-op that
	// happened to already match.
	want := baseline + time.Hour
	setStdout, setStderr, code := cli("--json", "admin", "retention", "set", want.String(), "--yes")
	if code != 0 {
		t.Fatalf("admin retention set exit = %d\nstdout:\n%s\nstderr:\n%s", code, setStdout, setStderr)
	}
	confirmed := decodeRetentionPolicy(t, setStdout)
	confirmedDuration := confirmed.maxRunRetention(t)
	if confirmedDuration != want {
		t.Fatalf("admin retention set reported %s, want %s", confirmedDuration, want)
	}

	// THE assertion: a SEPARATE, directly-dialed DescribeNamespace call
	// confirms the underlying cluster's own backstop TTL moved to want+margin
	// — this is the check that cannot pass on a write that only updated
	// something in-process — it never reuses `independent`'s earlier read,
	// `ork`'s reader, or `orkctl`'s own echoed response.
	independentAfter := describeIndependently()
	if independentAfter != want+margin {
		t.Fatalf("independent DescribeNamespace(%s) after `orkctl admin retention set` reports %s, "+
			"want %s (%s + %s margin): the CLI write did not reach the cluster",
			temporalNamespace, independentAfter, want+margin, want, margin)
	}
	t.Logf("EVIDENCE namespace=%s before=%s cli_set=%s cli_confirmed=%s independent_after=%s",
		temporalNamespace, independentBefore, want, confirmedDuration, independentAfter)

	// ---- orkctl admin retention get again: the CLI read agrees -----------
	reGetStdout, reGetStderr, code := cli("--json", "admin", "retention", "get")
	if code != 0 {
		t.Fatalf("admin retention get (after set) exit = %d\nstdout:\n%s\nstderr:\n%s",
			code, reGetStdout, reGetStderr)
	}
	afterGet := decodeRetentionPolicy(t, reGetStdout)
	afterGetDuration := afterGet.maxRunRetention(t)
	if afterGetDuration != want {
		t.Fatalf("admin retention get after the write reports %s, want %s", afterGetDuration, want)
	}

	// ---- a non-positive duration is refused before it reaches the store --
	// This refusal now happens LOCALLY in the CLI, before any RPC leaves
	// the process (cmd/orkctl/admin.go), so the message is the CLI's own
	// plain validation error, not a Connect InvalidArgument round-tripped
	// from the server — there is no server round trip to round-trip from.
	_, setZeroStderr, code := cli("admin", "retention", "set", "0s")
	if code == 0 {
		t.Fatal("admin retention set 0s succeeded, want a refusal")
	}
	if !strings.Contains(setZeroStderr, "positive duration") {
		t.Errorf("the refusal does not explain the positive-duration requirement:\n%s", setZeroStderr)
	}
	unchanged := describeIndependently()
	if unchanged != want+margin {
		t.Fatalf("backstop TTL after a refused CLI write = %s, want it unchanged at %s", unchanged, want+margin)
	}
}

// retentionPolicyJSON is `orkctl admin retention get/set --json`, decoded.
// maxRunRetention is left as a raw string because durationpb's protojson
// rendering (e.g. "18000s") is what the CLI actually prints; maxRunRetention()
// below turns it into a time.Duration for comparison.
type retentionPolicyJSON struct {
	Policy struct {
		MaxRunRetention string `json:"maxRunRetention"`
	} `json:"policy"`
	NamespaceRetentionEnforced bool `json:"namespaceRetentionEnforced"`
}

func decodeRetentionPolicy(t *testing.T, stdout string) retentionPolicyJSON {
	t.Helper()
	var decoded retentionPolicyJSON
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("admin retention --json is not valid JSON: %v\n%s", err, stdout)
	}
	return decoded
}

// maxRunRetention parses protojson's Duration rendering ("18000s", "1.5s"),
// which is Go's time.ParseDuration syntax restricted to a trailing "s" and
// (optionally) a fractional part — time.ParseDuration accepts exactly that
// shape.
func (p retentionPolicyJSON) maxRunRetention(t *testing.T) time.Duration {
	t.Helper()
	if p.Policy.MaxRunRetention == "" {
		t.Fatalf("empty maxRunRetention in %+v", p)
	}
	d, err := time.ParseDuration(p.Policy.MaxRunRetention)
	if err != nil {
		t.Fatalf("parse maxRunRetention %q: %v", p.Policy.MaxRunRetention, err)
	}
	return d
}
