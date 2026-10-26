//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// syncBuffer is a log an exec.Cmd writes to while a test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// orkerProcess is one real orker binary the test started.
type orkerProcess struct {
	cmd  *exec.Cmd
	log  *syncBuffer
	done chan struct{}
}

// stop signals the process and waits for it to exit, killing it if it will not.
func (p *orkerProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// requireTemporal returns the Temporal address, skipping when it is not up
// (failing instead under ORK_REQUIRE_INFRA; see infraAbsent).
func requireTemporal(t *testing.T) string {
	t.Helper()
	address := os.Getenv("ORK_TEMPORAL_ADDRESS")
	if address == "" {
		address = client.DefaultHostPort
	}
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v; run `make infra-up`", address, err)
	}
	_ = conn.Close()
	return address
}

// requireObjectStore skips when this binary's bucket (internal/testsupport/
// s3bucket, made in TestMain) cannot be reached; ORK_REQUIRE_INFRA makes that
// a failure instead (see infraAbsent).
func requireObjectStore(t *testing.T) {
	t.Helper()
	cfg := artifacts.ConfigFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := s3bucket.Client(ctx, cfg)
	if err == nil {
		_, err = store.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.S3Bucket)})
	}
	if err != nil {
		infraAbsent(t, "object store bucket %s at %s is not reachable: %v; run `make infra-up`", cfg.S3Bucket, cfg.S3Endpoint, err)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

// ownWorkRoot is this test's own Orker work root, one the container runtime can
// bind: a fresh directory under ORK_TEST_CONTAINER_TMPDIR, ORK_ORKER_WORKDIR or
// the repository's .data/work, never that directory itself (AGENTS.md §11.1).
func ownWorkRoot(t *testing.T) string {
	t.Helper()
	parent := os.Getenv("ORK_TEST_CONTAINER_TMPDIR")
	if parent == "" {
		parent = os.Getenv("ORK_ORKER_WORKDIR")
	}
	if parent == "" {
		parent = filepath.Join("..", "..", ".data", "work")
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatalf("create work root parent %s: %v", parent, err)
	}
	root, err := os.MkdirTemp(parent, proof.WorkRootPattern("orker-hc"))
	if err != nil {
		t.Fatalf("create work root under %s: %v", parent, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	absolute, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	return absolute
}

// withoutEnv is environ minus the named variables, so a value the shell running
// the tests exports cannot stand in for the one the test sets.
func withoutEnv(environ []string, names ...string) []string {
	var kept []string
next:
	for _, entry := range environ {
		for _, name := range names {
			if strings.HasPrefix(entry, name+"=") {
				continue next
			}
		}
		kept = append(kept, entry)
	}
	return kept
}

type healthcheckResult struct {
	code           int
	stdout, stderr string
}

// runHealthcheck runs `binary --healthcheck` with environ.
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

// awaitActivityPoller waits, within a deadline, for the cluster to list an
// activity poller on queue: the fact an Orker's readiness stands for.
func awaitActivityPoller(ctx context.Context, t *testing.T, c client.Client, queue string, within time.Duration) {
	t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	var lastErr error
	for {
		description, err := c.DescribeTaskQueueEnhanced(ctx, client.DescribeTaskQueueEnhancedOptions{
			TaskQueue:      queue,
			TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeActivity},
			ReportPollers:  true,
			Versions:       &client.TaskQueueVersionSelection{AllActive: true, Unversioned: true},
		})
		lastErr = err
		if err == nil {
			for _, version := range description.VersionsInfo {
				if len(version.TypesInfo[client.TaskQueueTypeActivity].Pollers) > 0 {
					return
				}
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("no activity poller on %s within %s (last error: %v)", queue, within, lastErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}

// TestIntegration_HealthcheckFollowsReadiness: `orker --healthcheck` is a
// question to the real Orker's own admin listener, and its answer is the
// Orker's readiness. Nothing listens before the Orker starts, so the check
// fails; once the Orker's workers poll, the check exits 0; with the admin
// address unset it fails naming ORK_ADMIN_ADDR; and after the Orker stops the
// check fails again.
func TestIntegration_HealthcheckFollowsReadiness(t *testing.T) {
	temporal := requireTemporal(t)
	requireObjectStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	ns := fmt.Sprintf("%s-orkerhc-%s", base, suffix)
	queue := "ork-orkerhc-" + suffix
	adminAddr := freeAddress(t)
	binary := testbin.Path(t, "./cmd/orker")

	// The check runs in the environment a container's health check would: the
	// daemon's own settings, here only the admin address.
	checkEnv := append(withoutEnv(os.Environ(), telemetry.AdminAddrVar, "ORK_CONFIG_FILE"),
		telemetry.AdminAddrVar+"="+adminAddr)
	daemonEnv := append(withoutEnv(os.Environ(), telemetry.AdminAddrVar, "ORK_CONFIG_FILE"),
		telemetry.AdminAddrVar+"="+adminAddr,
		"ORK_TEMPORAL_ADDRESS="+temporal,
		"ORK_TEMPORAL_NAMESPACE="+ns,
		"ORK_ORKER_QUEUE="+queue,
		"ORK_ORKER_ID="+proof.OrkerID("hc"),
		"ORK_ORKER_WORKDIR="+ownWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=s3",
	)

	transcript := &strings.Builder{}
	fmt.Fprintf(transcript, "# orker --healthcheck follows the Orker's readiness, admin listener %s, namespace %s\n\n", adminAddr, ns)
	check := func(label string, environ []string) healthcheckResult {
		t.Helper()
		result := runHealthcheck(ctx, t, binary, environ)
		fmt.Fprintf(transcript, "$ orker --healthcheck   # %s\n%s%s(exit %d)\n\n", label, result.stdout, result.stderr, result.code)
		return result
	}
	oneLine := func(s string) bool { return strings.Count(s, "\n") == 1 }

	// Nothing has started: there is no listener to ask.
	before := check("before the orker starts", checkEnv)
	if before.code != 1 || before.stdout != "" || !strings.HasPrefix(before.stderr, "orker: not ready: ") || !oneLine(before.stderr) {
		t.Fatalf("before the orker starts: exit %d, stdout %q, stderr %q; want exit 1 and one line on stderr",
			before.code, before.stdout, before.stderr)
	}

	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = daemonEnv
	log := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start orker: %v", err)
	}
	process := &orkerProcess{cmd: cmd, log: log, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(process.done) }()
	t.Cleanup(func() {
		process.stop(t)
		t.Logf("orker log:\n%s", log.String())
	})

	// Poll with a deadline, not a sleep: the Orker is ready when the check says
	// so, and every answer on the way there is exit 1 with a one-line reason.
	var ready healthcheckResult
	deadline := time.NewTimer(2 * time.Minute)
	defer deadline.Stop()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for ready.stdout == "" {
		select {
		case <-process.done:
			t.Fatalf("the orker exited before it was ready:\n%s", log.String())
		case <-deadline.C:
			t.Fatalf("the orker was not ready within 2m; the last check exited %d: %q\nlog:\n%s",
				ready.code, ready.stderr, log.String())
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
		result := check("polling for readiness", checkEnv)
		switch result.code {
		case 0:
			ready = result
		case 1:
			if !oneLine(result.stderr) || !strings.HasPrefix(result.stderr, "orker: not ready: ") {
				t.Fatalf("a failing check must say why in one line, got %q", result.stderr)
			}
		default:
			t.Fatalf("the check exited %d; only 0 and 1 are answers: %q", result.code, result.stderr)
		}
	}
	if !strings.HasPrefix(ready.stdout, "orker: ready") || ready.stderr != "" {
		t.Fatalf("ready check: stdout %q, stderr %q; want one \"orker: ready\" line", ready.stdout, ready.stderr)
	}

	// Ready means the Orker is polling: the cluster lists its activity poller.
	control, err := client.DialContext(ctx, client.Options{HostPort: temporal, Namespace: ns, Identity: "orker-hc-control"})
	if err != nil {
		t.Fatalf("dial %s: %v", temporal, err)
	}
	defer control.Close()
	awaitActivityPoller(ctx, t, control, queue, time.Minute)
	// And the check is the listener's own /readyz, asked of the same address.
	response, err := http.Get("http://" + adminAddr + telemetry.ReadyzPath)
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("the check said ready, but /readyz answers %d", response.StatusCode)
	}

	// The admin address unset is a configuration the check cannot answer for.
	unset := check("ORK_ADMIN_ADDR unset", withoutEnv(os.Environ(), telemetry.AdminAddrVar, "ORK_CONFIG_FILE"))
	if unset.code != 1 || unset.stdout != "" || !strings.Contains(unset.stderr, telemetry.AdminAddrVar) || !oneLine(unset.stderr) {
		t.Fatalf("admin address unset: exit %d, stdout %q, stderr %q; want exit 1 and one line naming ORK_ADMIN_ADDR",
			unset.code, unset.stdout, unset.stderr)
	}

	// The Orker stops, and the check follows it down.
	process.stop(t)
	after := check("after the orker stops", checkEnv)
	if after.code != 1 || after.stdout != "" || !strings.HasPrefix(after.stderr, "orker: not ready: ") || !oneLine(after.stderr) {
		t.Fatalf("after the orker stops: exit %d, stdout %q, stderr %q; want exit 1 and one line on stderr",
			after.code, after.stdout, after.stderr)
	}

	proof.Write(t, "orker", "healthcheck.txt", []byte(transcript.String()))
}
