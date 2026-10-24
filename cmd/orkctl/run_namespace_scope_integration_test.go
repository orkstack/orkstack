//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// TestIntegration_ARunIDOfOneNamespaceIsNotReadableOrCancellableFromAnother is
// the tenancy boundary on a run id, through the real binaries.
//
// A run id is an address inside a namespace, not a capability over the
// deployment: GetRun, StreamEvents and CancelRun require the namespace, and an
// id alone reads and cancels nothing. An id is not a secret: submit prints it,
// tickets quote it, CI transcripts echo it. Cancel is the half that matters
// most, because it is a write.
//
// The unit gate
// (TestRunID_IsAnAddressInsideANamespaceNotACapabilityOverTheDeployment)
// measures the service in memory. This one measures what an operator can
// actually do at a terminal: the flag orkctl sends, the code the engine
// answers with, and the run still running afterwards.
//
// The task sleeps so the assertions are about the scope and not about a race
// with the run's own completion: a cancel refused because the run had already
// succeeded would pass this test for the wrong reason.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace and its own pool.
func TestIntegration_ARunIDOfOneNamespaceIsNotReadableOrCancellableFromAnother(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalNamespace := isolatedTemporalNamespace("ork-namespace-cli")
	dialIsolatedNamespace(ctx, t, address, temporalNamespace)

	pool := privateTaskPool()
	taskQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map test pool: %v", err)
	}

	repoRoot := repoRoot(t)
	orkctlPath := testbin.Path(t, "./cmd/orkctl")
	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")

	startNamespacedOrker(ctx, t, orkerPath, address, temporalNamespace, taskQueue)
	engineAddr := startNamespacedEngine(ctx, t, enginePath, address, temporalNamespace, taskQueue)

	configPath := filepath.Join(t.TempDir(), "config.json")
	cli := func(args ...string) (string, string, int) {
		full := append(append([]string(nil), args...), "--server", "http://"+engineAddr)
		return runBinaryWithEnv(ctx, t, orkctlPath, repoRoot,
			[]string{configEnvVar + "=" + configPath, namespaceEnvVar + "="}, full...)
	}
	waitForEngine(t, cli)

	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	alpha := "scope-alpha-" + token
	bravo := "scope-bravo-" + token
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		removeNamespaceObjects(t, cleanupCtx, alpha, bravo)
	})

	for _, ns := range []string{alpha, bravo} {
		if stdout, stderr, code := cli("namespace", "create", ns,
			"--description", "run-id scope", "--default-pool", pool); code != 0 {
			t.Fatalf("namespace create %s exit = %d\nstdout:\n%s\nstderr:\n%s",
				ns, code, stdout, stderr)
		}
	}

	const orkflowName = "slow-deploy"
	document := writeSleepingOrkflow(t, orkflowName, pool)
	if stdout, stderr, code := cli("apply", document, "-n", alpha); code != 0 {
		t.Fatalf("apply into %s exit = %d\nstdout:\n%s\nstderr:\n%s", alpha, code, stdout, stderr)
	}
	waitForPoolLive(t, cli, alpha, pool)

	stdout, stderr, code := cli("run", "submit", orkflowName, "-n", alpha)
	if code != 0 {
		t.Fatalf("run submit -n %s exit = %d\nstdout:\n%s\nstderr:\n%s",
			alpha, code, stdout, stderr)
	}
	runID := parseRunID(t, stdout)

	// Its own namespace reads it. Anything else in this test would be measuring
	// a broken deployment rather than a scope.
	if stdout, stderr, code = cli("run", "get", runID, "-n", alpha); code != 0 {
		t.Fatalf("run get %s -n %s exit = %d\nstdout:\n%s\nstderr:\n%s",
			runID, alpha, code, stdout, stderr)
	}

	// The other namespace does not — for a read, for a stream, or for the
	// write. `--from-sequence` past the backlog keeps the watch from blocking
	// on a live run: what is under test is the refusal, which the bridge and
	// the RPC both make before a single event is sent.
	for _, tc := range []struct {
		what string
		args []string
	}{
		{"run get", []string{"run", "get", runID, "-n", bravo}},
		{"run watch", []string{"run", "watch", runID, "-n", bravo, "--from-sequence", "9999"}},
		{"run cancel", []string{"run", "cancel", runID, "-n", bravo}},
	} {
		stdout, stderr, code := cli(tc.args...)
		if code == 0 {
			t.Fatalf("`orkctl %s` from namespace %s succeeded against %s's run %s:\n%s",
				tc.what, bravo, alpha, runID, stdout)
		}
		if !strings.Contains(stderr, "not_found") {
			t.Errorf("`orkctl %s` from %s refused with %q, want not_found: "+
				"a caller who cannot name the namespace must not be able to tell "+
				"\"not yours\" from \"no such run\"", tc.what, bravo, stderr)
		}
	}

	// The refusals refused: the run is still there, and its own namespace can
	// still stop it. A scope that also broke the owner would pass every
	// assertion above and be useless.
	if stdout, stderr, code = cli("run", "cancel", runID, "-n", alpha); code != 0 {
		t.Fatalf("run cancel %s -n %s exit = %d\nstdout:\n%s\nstderr:\n%s",
			runID, alpha, code, stdout, stderr)
	}
}

// writeSleepingOrkflow writes a one-task orkflow that runs long enough to be
// cancelled: the test needs a run that is still executing when it asks.
func writeSleepingOrkflow(t *testing.T, name, pool string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".ork.yaml")
	document := fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: 1.0.0
description: run-id scope
tasks:
  - id: wait
    uses: shell
    pool: %s
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "sleep 60"]
`, name, pool)
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write orkflow %s: %v", name, err)
	}
	return path
}
