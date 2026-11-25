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
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
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
		// The late unbidden settle: after the verdict exists, naming none.
		{name: "hostile-late-settle", dir: "source-hostile", mode: "late-settle",
			fails: []string{checkAccepted, checkReleased}, details: "not answering this lane's verdict"},
		// The same under the one settle that names no verdict, the close at
		// the hold bound, before that bound.
		{name: "hostile-early-close", dir: "source-hostile", mode: "early-close",
			fails: []string{checkAccepted, checkReleased}, details: "before its hold bound"},
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

// TestIntegration_ServiceTest_FixturesGiveBack: a message held without a
// verdict when its container is stopped, or loses its Orker's beat, goes
// back to the broker with no settled.json (there is no verdict to name), and
// is delivered again; a settle written there is refused, and nothing is
// recorded for it (docs/EVENT_SOURCE_IMAGES.md, "Stopping, and a lost
// beat"). The containers are plain ones whose broker and answer directories
// are the test's own, because both are looked at once the container is gone;
// the Orker's side is its own lane driver.
func TestIntegration_ServiceTest_FixturesGiveBack(t *testing.T) {
	if runtime.GOOS != "linux" {
		infraAbsent(t, "service images run on Linux only")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "docker is not on PATH: %v", err)
	}
	registry := localcatalog.Registry()
	requireRegistry(t, registry)
	root := repoRoot(t)
	for _, f := range []serviceFixture{
		{name: "giveback-bash", dir: "source-bash"},
		{name: "giveback-go", dir: "source-go"},
		{name: "giveback-python", dir: "source-python"},
		{name: "giveback-hostile", dir: "source-hostile", mode: "stop-settle", details: "settled without a verdict"},
	} {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			image := buildServiceFixture(ctx, t, root, registry, f)
			t.Run("stop", func(t *testing.T) { giveBack(ctx, t, f, image, 120, "0", (*fixtureRun).terminate) })
			t.Run("lost beat", func(t *testing.T) { giveBack(ctx, t, f, image, 20, "3", (*fixtureRun).loseBeat) })
		})
	}
}

// giveBack ends a container that holds a message. orphanSeconds is far above
// any stall of the test's own beat, so only a beat stopped on purpose is lost.
func giveBack(ctx context.Context, t *testing.T, f serviceFixture, image string, orphanSeconds int, wantExit string, end func(*fixtureRun, context.Context, *testing.T)) {
	broker := t.TempDir()
	first := startFixture(ctx, t, image, broker, orphanSeconds)
	first.door.setWait(true)
	must.OK(t, os.WriteFile(filepath.Join(broker, "broker", "m1"), []byte("hello"), 0o644))
	must.OK(t, os.Rename(filepath.Join(broker, "broker", "m1"), filepath.Join(broker, "broker", "ready", "m1")))
	held, err := first.poll(ctx)
	must.OKf(t, err, "the message")
	if held.Kind != sourcehost.ExchangeHeld {
		t.Fatalf("the message ended kind %d, not held", held.Kind)
	}
	end(first, ctx, t)
	if code := strings.TrimSpace(first.docker(ctx, t, "wait", first.id)); code != wantExit {
		t.Fatalf("the container exited %s; want %s\n%s", code, wantExit, first.docker(ctx, t, "logs", first.id))
	}
	if _, err := os.Stat(filepath.Join(broker, "broker", "ready", "m1")); err != nil {
		t.Fatalf("the message was not given back to the broker: %v", err)
	}
	_, settledErr := os.Stat(filepath.Join(first.answer, sourcehost.LanePath(0, sourcehost.StepSettled)))
	closed, err := first.driver.HoldExpired(ctx, held.Message)
	if f.details != "" {
		release := sourcehost.Decision{Disposition: sourcehost.Wait, Action: sourcehost.ActionRelease,
			Receipt: sourcehost.Receipt{Outcome: artifacts.ReceiptReleased}}
		_, verdictErr := first.driver.Verdict(ctx, held.Message, release)
		for _, err := range []error{err, verdictErr} {
			var v *servicehost.ViolationError
			if !errors.As(err, &v) || !strings.Contains(v.Error(), f.details) {
				t.Errorf("a settle with no verdict: %v; want a violation holding %q", err, f.details)
			}
		}
		if settledErr != nil || closed || len(first.recorded.receipts) != 0 {
			t.Errorf("settled.json %v, closed %v, receipts %+v; want the settle there and nothing recorded for it", settledErr, closed, first.recorded.receipts)
		}
		return
	}
	if !errors.Is(settledErr, os.ErrNotExist) || closed || err != nil || len(first.recorded.receipts) != 0 {
		t.Fatalf("settled.json %v, closed %v (%v), receipts %+v; want no settle and nothing recorded", settledErr, closed, err, first.recorded.receipts)
	}
	second := startFixture(ctx, t, image, broker, orphanSeconds)
	again, err := second.poll(ctx)
	must.OKf(t, err, "the message again")
	if again.Kind != sourcehost.ExchangeSettled || again.Message.ID != held.Message.ID || again.Message.DeliveryCount != held.Message.DeliveryCount+1 ||
		len(second.recorded.receipts) != 1 || second.recorded.receipts[0].Outcome != artifacts.ReceiptAccepted {
		t.Fatalf("after a give-back of %q count %d: kind %d, %q count %d, receipts %+v", held.Message.ID, held.Message.DeliveryCount,
			again.Kind, again.Message.ID, again.Message.DeliveryCount, second.recorded.receipts)
	}
}

// fixtureRun is one fixture container over directories of the test's own,
// and the Orker's side of its one lane.
type fixtureRun struct {
	id       string
	answer   string
	door     *acceptingDoor
	driver   *sourcehost.LaneDriver
	recorded *keptReceipts
	stopBeat func()
}

// keptReceipts keeps what a lane records; one goroutine uses it.
type keptReceipts struct{ receipts []sourcehost.Receipt }

func (k *keptReceipts) Record(_ context.Context, _ sourcehost.HandedMessage, _ int64, r sourcehost.Receipt) error {
	k.receipts = append(k.receipts, r)
	return nil
}

func startFixture(ctx context.Context, t *testing.T, image, broker string, orphanSeconds int) *fixtureRun {
	t.Helper()
	dir := t.TempDir()
	input, ask, answer := filepath.Join(dir, "input"), filepath.Join(dir, "ask"), filepath.Join(dir, "answer")
	for _, d := range []string{input, ask, answer} {
		must.OK(t, os.Mkdir(d, 0o755))
	}
	must.OK(t, os.WriteFile(filepath.Join(input, "service.json"), fmt.Appendf(nil, `{"limits":{"holdSeconds":960,"orphanSeconds":%d}}`, orphanSeconds), 0o644))
	asks, err := servicehost.OpenAskDir(ask)
	must.OK(t, err)
	t.Cleanup(func() { _ = asks.Close() })
	steps, err := servicehost.OpenAnswerDir(answer)
	must.OK(t, err)
	t.Cleanup(func() { _ = steps.Close() })
	r := &fixtureRun{answer: answer, door: &acceptingDoor{}, recorded: &keptReceipts{}}
	var beat servicehost.Beat
	must.OK(t, asks.WriteJSON("alive.json", beat.Next()))
	beating, beaten := make(chan struct{}), make(chan struct{})
	r.stopBeat = sync.OnceFunc(func() { close(beating); <-beaten })
	go func() {
		defer close(beaten)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-beating:
				return
			case <-tick.C:
				_ = asks.WriteJSON("alive.json", beat.Next())
			}
		}
	}()
	t.Cleanup(r.stopBeat)
	judge := sourcehost.NewJudge(&orkstackv1.EventSource{Id: "src", Name: "giveback", Namespace: "default", JudgingRevision: 1}, 1, r.door, noStager{}, time.Now)
	r.driver = &sourcehost.LaneDriver{Index: 0, Steps: steps, Asks: asks, Judge: judge, Recorder: r.recorded, Now: time.Now,
		Limits: sourcehost.LaneLimits{PollWait: time.Second, Answer: 30 * time.Second, Settle: 30 * time.Second, Hold: 960 * time.Second,
			LeaseSeconds: 60, MaxBodyBytes: 1 << 20}}
	r.id = strings.TrimSpace(r.docker(ctx, t, "run", "-d", "--init", "--network", "none", "--read-only", "--cap-drop", "ALL",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "--label", fmt.Sprintf("ork.service-test=%d", os.Getpid()),
		"-v", input+":/orkstack/input:ro", "-v", ask+":"+servicehost.PathAsk+":ro", "-v", answer+":"+servicehost.PathAnswer,
		"-v", broker+":/tmp", image))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", r.id).Run() })
	for {
		if _, err := steps.ReadStep(servicehost.StatusName, servicehost.MaxStatusBytes); err == nil {
			return r
		}
		if running := r.docker(ctx, t, "inspect", "--format", "{{.State.Running}}", r.id); strings.TrimSpace(running) != "true" {
			t.Fatalf("the fixture exited before it wrote a status:\n%s", r.docker(ctx, t, "logs", r.id))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the fixture wrote no status:\n%s", r.docker(context.WithoutCancel(ctx), t, "logs", r.id))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (r *fixtureRun) poll(ctx context.Context) (sourcehost.Exchange, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return r.driver.Poll(ctx, 1)
}

func (r *fixtureRun) terminate(ctx context.Context, t *testing.T) {
	r.docker(ctx, t, "kill", "--signal", "TERM", r.id)
}

func (r *fixtureRun) loseBeat(context.Context, *testing.T) { r.stopBeat() }

func (r *fixtureRun) docker(ctx context.Context, t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
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
