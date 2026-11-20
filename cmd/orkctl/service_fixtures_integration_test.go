//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// The event-source fixtures under `orkctl service test`
// (docs/EVENT_SOURCE_IMAGES.md, "Testing a source image"): the Bash, Go and
// Python sources keep the whole profile and pass every check; each hostile
// image does one thing a contract-keeping image never does and fails the
// check about it, and the two that attack the answer filesystem's bound
// pass only because the bound held (they end themselves when it does not).

// servicePublish is the --publish-exec every fixture is tested with: one
// message into its directory broker, through the container's own root.
const servicePublish = `b=$ORK_SERVICE_ROOT/tmp/broker; echo hello > "$b/m1" && mv "$b/m1" "$b/ready/m1"`

// The seven checks, by the names the report gives them, in its order.
const (
	checkLabels   = "labels and interface"
	checkStatus   = "a status within the startup bound, ready and healthy"
	checkQuiet    = "a quiet poll is answered empty.json within the wait"
	checkAccepted = "a message is handed over and acknowledged"
	checkReleased = "a released message is delivered again, counted"
	checkStop     = "stop: the notice, SIGTERM, an exit within stopSeconds"
	checkBeat     = "a lost beat: the container ends itself"
)

var allServiceChecks = []string{checkLabels, checkStatus, checkQuiet, checkAccepted, checkReleased, checkStop, checkBeat}

// serviceFixture is one image to build and what service test must say of it.
type serviceFixture struct {
	name string
	dir  string // under test/fixtures
	mode string // the hostile MODE, or ""
	// fails is exactly the checks that must fail, the one about the hostile
	// act first; nil means every check passes. A failure inside the
	// conversation leaves its exchange open, so the checks after it on the
	// same lane fail with it.
	fails []string
	// details is a fragment of the first check's details, naming why.
	details string
}

func TestIntegration_ServiceTest_Fixtures(t *testing.T) {
	if runtime.GOOS != "linux" {
		infraAbsent(t, "service test runs on Linux only")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "docker is not on PATH: %v", err)
	}
	registry := localcatalog.Registry()
	requireRegistry(t, registry)
	ctx := context.Background()
	root := repoRoot(t)
	orkctlPath := testbin.Path(t, "./cmd/orkctl")

	fixtures := []serviceFixture{
		{name: "bash", dir: "source-bash"},
		{name: "go", dir: "source-go"},
		{name: "python", dir: "source-python"},
		{name: "hostile-fill", dir: "source-hostile", mode: "fill"},
		{name: "hostile-nest", dir: "source-hostile", mode: "nest"},
		{name: "hostile-link-swap", dir: "source-hostile", mode: "link-swap",
			fails: []string{checkAccepted, checkReleased}, details: "a link"},
		{name: "hostile-silent-lane", dir: "source-hostile", mode: "silent-lane",
			fails: []string{checkQuiet, checkAccepted, checkReleased}, details: "deadline"},
		{name: "hostile-ignore-term", dir: "source-hostile", mode: "ignore-term",
			fails: []string{checkStop}, details: "killed"},
		{name: "hostile-ignore-beat", dir: "source-hostile", mode: "ignore-beat",
			fails: []string{checkBeat}, details: "still running"},
		{name: "hostile-billion-limit", dir: "source-hostile", mode: "billion-limit",
			fails: []string{checkStatus, checkBeat}, details: "OBJECT_INVALID broker.maxDeliveries"},
		{name: "hostile-ack-no-verdict", dir: "source-hostile", mode: "ack-no-verdict",
			fails: []string{checkAccepted, checkReleased}, details: "settled without a verdict"},
	}
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			image := buildServiceFixture(ctx, t, root, registry, f)
			report, stderr, code := runServiceTest(ctx, t, orkctlPath, root, image)
			assertServiceReport(t, f, report, stderr, code)
		})
	}
}

// buildServiceFixture builds one fixture and pushes it under a tag of this
// process's own (AGENTS.md §11.1), removed from the daemon when the test
// ends; `service test` runs it by digest, as an Orker does.
func buildServiceFixture(ctx context.Context, t *testing.T, root, registry string, f serviceFixture) string {
	t.Helper()
	tag := fmt.Sprintf("%s/orkstack-test/service-%s:%s", registry, f.name, exampleImageTag())
	dockerfile := filepath.Join(root, "test", "fixtures", f.dir, "Dockerfile")
	buildContext := filepath.Join(root, "test", "fixtures", f.dir)
	if f.dir == "source-go" {
		// The Go fixture is one static binary, built from this tree.
		buildContext = t.TempDir()
		build := exec.CommandContext(ctx, "go", "build", "-o", filepath.Join(buildContext, "source-go"), "./test/fixtures/source-go")
		build.Dir, build.Env = root, append(os.Environ(), "CGO_ENABLED=0")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build the Go fixture: %v\n%s", err, out)
		}
	}
	args := []string{"build", "-q", "-f", dockerfile, "-t", tag}
	if f.mode != "" {
		args = append(args, "--build-arg", "MODE="+f.mode)
	}
	docker(ctx, t, append(args, buildContext)...)
	t.Cleanup(func() { _ = exec.Command("docker", "rmi", "-f", tag).Run() })
	docker(ctx, t, "push", "-q", tag)
	return tag
}

func docker(ctx context.Context, t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// runServiceTest runs `orkctl service test --json` on image and returns
// the report it printed, its stderr and its exit code.
func runServiceTest(ctx context.Context, t *testing.T, orkctlPath, root, image string) (serviceTestReport, string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, orkctlPath, "service", "test", image, "--json", "--publish-exec", servicePublish)
	cmd.Dir, cmd.Env = root, testbin.StackEnv(os.Environ())
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	code := 0
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		code = exit.ExitCode()
	case err != nil:
		t.Fatalf("orkctl service test %s: %v\n%s", image, err, stderr.String())
	}
	var report serviceTestReport
	if uerr := json.Unmarshal(stdout.Bytes(), &report); uerr != nil {
		t.Fatalf("orkctl service test %s printed no report (exit %d): %v\nstdout:\n%s\nstderr:\n%s",
			image, code, uerr, stdout.String(), stderr.String())
	}
	return report, stderr.String(), code
}

func assertServiceReport(t *testing.T, f serviceFixture, report serviceTestReport, stderr string, code int) {
	t.Helper()
	results := map[string]serviceCheck{}
	for _, c := range report.Checks {
		results[c.Name] = c
	}
	dump := func() string {
		var b strings.Builder
		for _, c := range report.Checks {
			fmt.Fprintf(&b, "  %-55s %-7s %s\n", c.Name, c.Result, c.Details)
		}
		return b.String() + "stderr: " + stderr
	}
	if f.fails == nil {
		if !report.OK || code != 0 {
			t.Fatalf("%s must keep the whole profile (exit %d, ok %v):\n%s", f.name, code, report.OK, dump())
		}
		for _, name := range allServiceChecks {
			if c, ok := results[name]; !ok || c.Result != "pass" {
				t.Fatalf("%s: check %q did not pass:\n%s", f.name, name, dump())
			}
		}
		return
	}
	if report.OK || code == 0 {
		t.Fatalf("%s passed service test (exit %d); it must fail %q:\n%s", f.name, code, f.fails[0], dump())
	}
	first, ok := results[f.fails[0]]
	if !ok || first.Result != "fail" || !strings.Contains(first.Details, f.details) {
		t.Fatalf("%s: check %q did not fail for %q:\n%s", f.name, f.fails[0], f.details, dump())
	}
	want := map[string]bool{}
	for _, name := range f.fails {
		want[name] = true
	}
	for _, c := range report.Checks {
		if (c.Result == "fail") != want[c.Name] {
			t.Fatalf("%s: check %q ended %s; the checks that must fail are %q:\n%s", f.name, c.Name, c.Result, f.fails, dump())
		}
	}
	// Positive control: every check before the hostile act passed, so the
	// failure is the hostile act's and not a broken harness.
	for _, name := range allServiceChecks {
		if name == f.fails[0] {
			break
		}
		if r, ok := results[name]; !ok || r.Result != "pass" {
			t.Fatalf("%s: check %q, before the one under test, did not pass:\n%s", f.name, name, dump())
		}
	}
}
