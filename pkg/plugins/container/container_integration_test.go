//go:build integration

package containerplugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// These tests drive a real Docker daemon. They skip cleanly when one is not
// reachable — or fail, under ORK_REQUIRE_INFRA, so a proof run cannot report
// green having run none of them — in line with the repository's convention (AGENTS.md
// §5), and they leave the host as they found it: every container is removed and
// every image they pull is an image they also make sure is present at the end.

// testImage is small, has a shell, and is stable. ORK_TEST_CONTAINER_IMAGE
// overrides it for hosts with a private mirror.
func testImage() string {
	if image := strings.TrimSpace(os.Getenv("ORK_TEST_CONTAINER_IMAGE")); image != "" {
		return image
	}
	return "alpine:3.20"
}

func requireDocker(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").CombinedOutput()
	if err != nil {
		infraAbsent(t, "docker daemon is not reachable: %v: %s", err, strings.TrimSpace(string(out)))
	}
	t.Logf("docker server %s", strings.TrimSpace(string(out)))
}

// requireImage makes sure the image is on the host, pulling it once if needed.
func requireImage(t *testing.T, image string) {
	t.Helper()
	if code, _ := docker(t, 30*time.Second, "image", "inspect", "--format", "{{.Id}}", image); code == 0 {
		return
	}
	code, out := docker(t, 5*time.Minute, "pull", image)
	if code != 0 {
		t.Skipf("cannot pull %s (no registry access?): %s", image, out)
	}
}

// docker runs a docker command and returns its exit code plus combined output.
func docker(t *testing.T, timeout time.Duration, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var combined bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, combined.String()
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), combined.String()
	default:
		t.Fatalf("docker %s: %v", strings.Join(args, " "), err)
		return -1, ""
	}
}

// assertNoLeftoverContainers proves the plugin's cleanup: nothing labelled for
// this task may survive the run, whatever its outcome.
func assertNoLeftoverContainers(t *testing.T, taskID string) {
	t.Helper()
	code, out := docker(t, 30*time.Second,
		"ps", "--all", "--quiet", "--filter", "label=ork.task="+taskID)
	if code != 0 {
		t.Fatalf("docker ps: %s", out)
	}
	if ids := strings.Fields(out); len(ids) != 0 {
		for _, id := range ids {
			docker(t, 30*time.Second, "rm", "--force", id)
		}
		t.Fatalf("run left %d container(s) behind: %v", len(ids), ids)
	}
}

func newIntegrationContext(t *testing.T, taskID string, config map[string]any) *plugin.ExecContext {
	t.Helper()
	return &plugin.ExecContext{
		TaskID:  taskID,
		Config:  config,
		WorkDir: bindableTempDir(t),
		Log:     func(format string, args ...any) { t.Logf(format, args...) },
	}
}

// bindableTempDir returns a scratch directory the Docker daemon can bind-mount.
//
// It is not always os.TempDir(). A daemon inside a VM (colima, Lima, Rancher,
// Docker Desktop) only sees the paths that VM shares, and on macOS the default
// share is the user's home, not /var/folders. This is the same constraint an
// operator meets through ORK_ORKER_WORKDIR, so the tests resolve it the same
// way rather than pretending it does not exist.
func bindableTempDir(t *testing.T) string {
	t.Helper()
	root := bindableRoot(t)
	if root == "" {
		t.Skipf("no directory on this host can be bind-mounted by the docker daemon; " +
			"set ORK_TEST_CONTAINER_TMPDIR to one the daemon shares")
	}
	dir, err := os.MkdirTemp(root, "ork-container-test-*")
	if err != nil {
		t.Fatalf("create work dir under %s: %v", root, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

var (
	bindableRootOnce sync.Once
	bindableRootDir  string
)

// bindableRoot picks the first candidate directory the daemon can actually
// bind-mount, once per test binary.
func bindableRoot(t *testing.T) string {
	t.Helper()
	bindableRootOnce.Do(func() {
		if explicit := strings.TrimSpace(os.Getenv("ORK_TEST_CONTAINER_TMPDIR")); explicit != "" {
			bindableRootDir = explicit
			return
		}
		home, _ := os.UserHomeDir()
		candidates := []string{os.TempDir()}
		if home != "" {
			candidates = append(candidates, filepath.Join(home, ".ork-test-workdirs"))
		}
		for _, candidate := range candidates {
			if err := os.MkdirAll(candidate, 0o755); err != nil {
				continue
			}
			if daemonCanBind(t, candidate) {
				bindableRootDir = candidate
				t.Logf("work directories for these tests live under %s", candidate)
				return
			}
			t.Logf("the docker daemon cannot bind-mount %s; trying the next candidate", candidate)
		}
	})
	return bindableRootDir
}

// daemonCanBind reports whether the daemon can bind-mount a path under root.
// `--mount` refuses a source it cannot see, which is exactly the probe needed.
func daemonCanBind(t *testing.T, root string) bool {
	t.Helper()
	probe, err := os.MkdirTemp(root, "ork-bind-probe-*")
	if err != nil {
		return false
	}
	defer func() { _ = os.RemoveAll(probe) }()

	image := testImage()
	requireImage(t, image)
	code, out := docker(t, 2*time.Minute, "run", "--rm",
		"--mount", "type=bind,source="+probe+",target=/ork-probe,readonly", image, "true")
	if code != 0 {
		t.Logf("bind probe for %s: %s", root, strings.TrimSpace(out))
	}
	return code == 0
}

// hostResolver stands in for the Orker's upstream resolver: it hands back a
// path that already exists on the host, which is exactly what the Orker does
// after materializing an artifact into the workdir.
type hostResolver struct{ paths map[string]string }

func (h hostResolver) Resolve(_ context.Context, ref string) (string, error) {
	path, ok := h.paths[ref]
	if !ok {
		return "", fmt.Errorf("no artifact %q", ref)
	}
	return path, nil
}

func TestIntegration_ContainerRunsAndCapturesBothStreamsAsArtifacts(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	const taskID = "capture-streams"
	ec := newIntegrationContext(t, taskID, map[string]any{
		"image": image,
		"env": map[string]any{
			"RELEASE": "v1.4.2",
			"ARCHIVE": "orkstack-${RELEASE}.tar.gz",
		},
		"command":           `echo "archive=$ARCHIVE"; echo "a warning" >&2`,
		"capture_stdout_as": "run-log",
		"capture_stderr_as": "run-warnings",
	})

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got := result.Output["exit_code"]; got != float64(0) {
		t.Errorf("exit_code = %v, want 0", got)
	}
	if got := result.Output["stdout"]; got != "archive=orkstack-v1.4.2.tar.gz\n" {
		t.Errorf("stdout = %q, want the expanded environment variable", got)
	}
	if got := result.Output["timed_out"]; got != false {
		t.Errorf("timed_out = %v, want false", got)
	}

	artifacts := map[string]string{}
	for _, produced := range result.Artifacts {
		raw, err := os.ReadFile(produced.LocalPath)
		if err != nil {
			t.Fatalf("read artifact %q: %v", produced.Name, err)
		}
		artifacts[produced.Name] = string(raw)
	}
	if artifacts["run-log"] != "archive=orkstack-v1.4.2.tar.gz\n" {
		t.Errorf("run-log artifact = %q", artifacts["run-log"])
	}
	if artifacts["run-warnings"] != "a warning\n" {
		t.Errorf("run-warnings artifact = %q", artifacts["run-warnings"])
	}
	assertNoLeftoverContainers(t, taskID)
}

func TestIntegration_ContainerMountsAnUpstreamArtifactReadOnly(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	const contents = "slideshow: Sample Slide Show\n"
	// The Orker materializes an upstream artifact into the task workdir, so
	// the stand-in puts it somewhere the daemon can bind-mount too.
	upstream := filepath.Join(bindableTempDir(t), "body")
	if err := os.WriteFile(upstream, []byte(contents), 0o644); err != nil {
		t.Fatalf("write upstream artifact: %v", err)
	}

	const taskID = "mount-artifact"
	ec := newIntegrationContext(t, taskID, map[string]any{
		"image": image,
		"mounts": []any{
			map[string]any{"artifact": "fetch/body", "path": "/input/body.txt"},
		},
		// Reading must work; writing must not.
		"command": `cat /input/body.txt; if echo tampered > /input/body.txt 2>/dev/null; ` +
			`then echo "WRITABLE"; else echo "read-only confirmed"; fi`,
	})
	ec.Artifacts = hostResolver{paths: map[string]string{"fetch/body": upstream}}

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stdout, _ := result.Output["stdout"].(string)
	if !strings.Contains(stdout, contents) {
		t.Errorf("the container did not read the mounted artifact: stdout = %q", stdout)
	}
	if !strings.Contains(stdout, "read-only confirmed") {
		t.Errorf("the mounted artifact was writable inside the container: stdout = %q", stdout)
	}
	if after, err := os.ReadFile(upstream); err != nil || string(after) != contents {
		t.Errorf("the upstream artifact changed on the host: %q (err %v)", after, err)
	}
	assertNoLeftoverContainers(t, taskID)
}

func TestIntegration_ContainerGetsAWritableEphemeralMountThatIsRemovedAfterwards(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	const taskID = "ephemeral-mount"
	ec := newIntegrationContext(t, taskID, map[string]any{
		"image": image,
		"mounts": []any{
			map[string]any{"ephemeral": "scratch", "path": "/scratch"},
		},
		"command": `echo built > /scratch/report.txt && cat /scratch/report.txt && ls /scratch`,
	})
	scratch := filepath.Join(ec.WorkDir, internalDir, ephemeralDir, "scratch")

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stdout, _ := result.Output["stdout"].(string)
	if !strings.Contains(stdout, "built") || !strings.Contains(stdout, "report.txt") {
		t.Errorf("the container could not use the ephemeral mount: stdout = %q", stdout)
	}
	if _, err := os.Stat(scratch); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ephemeral mount %s survived the run (stat err = %v)", scratch, err)
	}
	assertNoLeftoverContainers(t, taskID)
}

func TestIntegration_ContainerTimeoutKillsTheContainerAndFailsTheTask(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	const taskID = "timeout"
	ec := newIntegrationContext(t, taskID, map[string]any{
		"image":           image,
		"command":         "echo working; sleep 120",
		"timeout_seconds": 3,
	})

	started := time.Now()
	_, err := New().Execute(context.Background(), ec)
	elapsed := time.Since(started)

	if err == nil {
		t.Fatalf("Execute succeeded, want the timeout to fail the task")
	}
	if !strings.Contains(err.Error(), "exceeded its timeout") {
		t.Errorf("error %q does not report the timeout", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("exit code %d", exitKilled)) {
		t.Errorf("error %q does not report the killed exit code %d", err, exitKilled)
	}
	var retryable *plugin.RetryableError
	if !errors.As(err, &retryable) || retryable.Retryable {
		t.Errorf("a blown budget produced %v, want a permanent failure", err)
	}
	if elapsed > 60*time.Second {
		t.Errorf("the timeout took %s to take effect", elapsed)
	}
	t.Logf("container killed after %s: %v", elapsed.Round(time.Millisecond), err)
	assertNoLeftoverContainers(t, taskID)
}

func TestIntegration_ContainerFailsTheTaskWithTheContainerExitCode(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	const taskID = "exit-code"
	ec := newIntegrationContext(t, taskID, map[string]any{
		"image":   image,
		"command": `echo "boom" >&2; exit 17`,
	})

	_, err := New().Execute(context.Background(), ec)
	if err == nil {
		t.Fatalf("Execute succeeded, want the container's failure to fail the task")
	}
	if !strings.Contains(err.Error(), "exited with code 17") {
		t.Errorf("error %q does not carry the container's exit code", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %q does not carry the container's stderr", err)
	}
	assertNoLeftoverContainers(t, taskID)
}

// The pull policy is the difference between "runs on a warm host" and "runs
// anywhere", so it is proven against a real registry: the image is removed, a
// run brings it back, and the next run finds it locally.
func TestIntegration_ContainerPullsAMissingImageThenReusesTheLocalOne(t *testing.T) {
	requireDocker(t)
	image := testImage()
	requireImage(t, image)

	if code, out := docker(t, 60*time.Second, "image", "rm", "--force", image); code != 0 {
		t.Skipf("cannot remove %s to prove a pull (in use?): %s", image, out)
	}
	// Whatever happens next, leave the host with the image it started with.
	t.Cleanup(func() { requireImage(t, image) })

	const taskID = "pull-missing"
	first, err := New().Execute(context.Background(), newIntegrationContext(t, taskID, map[string]any{
		"image":   image,
		"pull":    "missing",
		"command": "echo pulled",
	}))
	if err != nil {
		t.Skipf("cannot pull %s from its registry: %v", image, err)
	}
	if first.Output["image_pulled"] != true {
		t.Errorf("first run image_pulled = %v, want true", first.Output["image_pulled"])
	}
	if got := first.Output["stdout"]; got != "pulled\n" {
		t.Errorf("first run stdout = %q, want the container's output", got)
	}

	second, err := New().Execute(context.Background(), newIntegrationContext(t, taskID, map[string]any{
		"image":   image,
		"pull":    "missing",
		"command": "echo local",
	}))
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if second.Output["image_pulled"] != false {
		t.Errorf("second run image_pulled = %v, want false: the image is already local",
			second.Output["image_pulled"])
	}
	assertNoLeftoverContainers(t, taskID)
}

func TestIntegration_ContainerRefusesToFetchWhenPullIsNever(t *testing.T) {
	requireDocker(t)
	const absent = "orkstack.invalid/definitely-not-here:0.0.1"

	_, err := New().Execute(context.Background(), newIntegrationContext(t, "pull-never", map[string]any{
		"image":   absent,
		"pull":    "never",
		"command": "true",
	}))
	if err == nil {
		t.Fatalf("Execute succeeded, want `pull: never` to refuse a missing image")
	}
	if !strings.Contains(err.Error(), "`pull: never`") {
		t.Errorf("error %q does not explain the policy", err)
	}
	var retryable *plugin.RetryableError
	if !errors.As(err, &retryable) || retryable.Retryable {
		t.Errorf("error %v is not permanent; no retry can make a forbidden pull succeed", err)
	}
}
