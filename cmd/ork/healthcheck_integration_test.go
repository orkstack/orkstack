//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/bootwait"
	"github.com/orkstack/orkstack/internal/testsupport/listenaddr"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// healthcheckResult is one run of `ork --healthcheck`.
type healthcheckResult struct {
	code           int
	stdout, stderr string
}

// runHealthcheck runs `binary --healthcheck` with environ and returns its exit
// status and its two streams.
func runHealthcheck(ctx context.Context, t *testing.T, binary string, environ []string) healthcheckResult {
	t.Helper()
	cmd := exec.CommandContext(ctx, binary, "--healthcheck")
	cmd.Env = environ
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	result := healthcheckResult{stdout: stdout.String(), stderr: stderr.String()}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		result.code = exit.ExitCode()
	default:
		t.Fatalf("run %s --healthcheck: %v", binary, err)
	}
	return result
}

// TestIntegration_HealthcheckFollowsReadiness: `ork --healthcheck` is a
// question to the real engine's own admin listener, and its answer is the
// engine's readiness. Nothing listens before the engine starts, so the check
// fails; once the engine is ready the check exits 0 and the API it vouches
// for answers at once; with the admin address unset the check fails naming
// ORK_ADMIN_ADDR; and after the engine stops the check fails again.
func TestIntegration_HealthcheckFollowsReadiness(t *testing.T) {
	temporal := requireTemporal(t)
	requireObjectStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	ns := fmt.Sprintf("%s-hc-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	binary := buildOrk(t)
	// The check runs in the environment a container's health check would:
	// the daemon's own settings, here only the admin address. Before the
	// engine starts it is an address nothing listens on; once it has started,
	// the one its admin listener bound, read from its own log.
	checkAt := func(addr string) []string {
		return append(testbin.StackEnv(os.Environ()), telemetry.AdminAddrVar+"="+addr)
	}

	transcript := &strings.Builder{}
	fmt.Fprintf(transcript, "# ork --healthcheck follows the engine's readiness, namespace %s\n\n", ns)
	check := func(label string, environ []string) healthcheckResult {
		t.Helper()
		result := runHealthcheck(ctx, t, binary, environ)
		fmt.Fprintf(transcript, "$ ork --healthcheck   # %s\n%s%s(exit %d)\n\n", label, result.stdout, result.stderr, result.code)
		return result
	}

	// Nothing has started: there is no listener to ask.
	before := check("before the engine starts", checkAt(listenaddr.Closed))
	if before.code != 1 || before.stdout != "" || !strings.HasPrefix(before.stderr, "ork: not ready: ") ||
		strings.Count(before.stderr, "\n") != 1 {
		t.Fatalf("before the engine starts: exit %d, stdout %q, stderr %q; want exit 1 and one line on stderr",
			before.code, before.stdout, before.stderr)
	}

	process := startEngine(ctx, t, binary, temporal, "engine-hc",
		"ORK_TEMPORAL_NAMESPACE="+ns, telemetry.AdminAddrVar+"="+listenaddr.Loopback)
	adminAddr, err := process.log.Addr(ctx, "engine-hc", listenaddr.Admin, process.done, bootwait.Deadline)
	must.OK(t, err)
	fmt.Fprintf(transcript, "# admin listener %s\n\n", adminAddr)
	checkEnv := checkAt(adminAddr)

	// Poll with a deadline, not a sleep: the engine is ready when the check says
	// so, and every answer on the way there is exit 1 with a one-line reason.
	var ready healthcheckResult
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for ready.stdout == "" {
		select {
		case <-process.done:
			t.Fatalf("the engine exited before it was ready:\n%s", process.log.String())
		case <-deadline.C:
			t.Fatalf("the engine was not ready within 90s; the last check exited %d: %q\nlog:\n%s",
				ready.code, ready.stderr, process.log.String())
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
		result := check("polling for readiness", checkEnv)
		switch result.code {
		case 0:
			ready = result
		case 1:
			if strings.Count(result.stderr, "\n") != 1 || !strings.HasPrefix(result.stderr, "ork: not ready: ") {
				t.Fatalf("a failing check must say why in one line, got %q", result.stderr)
			}
		default:
			t.Fatalf("the check exited %d; only 0 and 1 are answers: %q", result.code, result.stderr)
		}
	}
	if !strings.HasPrefix(ready.stdout, "ork: ready") || ready.stderr != "" {
		t.Fatalf("ready check: stdout %q, stderr %q; want one \"ork: ready\" line", ready.stdout, ready.stderr)
	}

	// Ready means the API serves: the very first call after the check passes
	// is answered, with no retry.
	apiAddr, err := process.apiAddr(ctx, "engine-hc")
	must.OK(t, err)
	client := orkflowClient(t, apiAddr)
	if _, err := client.ListOrkflows(ctx, connect.NewRequest(&orkstackv1.ListOrkflowsRequest{})); err != nil {
		t.Fatalf("the check said ready, but the API did not answer: %v", err)
	}
	// And the check is the listener's own /readyz, asked of the same address.
	response, err := http.Get("http://" + adminAddr + telemetry.ReadyzPath)
	must.OKf(t, err, "GET /readyz")
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("the check said ready, but /readyz answers %d", response.StatusCode)
	}

	// The admin address unset is a configuration the check cannot answer for.
	unset := check("ORK_ADMIN_ADDR unset", testbin.StackEnv(os.Environ()))
	if unset.code != 1 || unset.stdout != "" || !strings.Contains(unset.stderr, telemetry.AdminAddrVar) ||
		strings.Count(unset.stderr, "\n") != 1 {
		t.Fatalf("admin address unset: exit %d, stdout %q, stderr %q; want exit 1 and one line naming ORK_ADMIN_ADDR",
			unset.code, unset.stdout, unset.stderr)
	}

	// The engine stops, and the check follows it down.
	stopEngine(t, "engine-hc")
	after := check("after the engine stops", checkEnv)
	if after.code != 1 || after.stdout != "" || !strings.HasPrefix(after.stderr, "ork: not ready: ") ||
		strings.Count(after.stderr, "\n") != 1 {
		t.Fatalf("after the engine stops: exit %d, stdout %q, stderr %q; want exit 1 and one line on stderr",
			after.code, after.stdout, after.stderr)
	}

	proof.Write(t, "ork", "healthcheck.txt", []byte(transcript.String()))
}
