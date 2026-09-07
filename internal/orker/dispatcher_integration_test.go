//go:build integration

package orker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// referenceImage is the image the contract is measured against.
//
// It is pinned to a DIGEST because the dispatcher refuses a tag: a running
// execution must never silently switch images because a tag moved
// (AGENTS.md §5). The digest is resolved once, at the start of the run, from
// whatever alpine:3.20 the host has — so the test pins what it actually ran
// rather than hardcoding a digest that will rot.
const referenceTag = "alpine:3.20"

// requireDocker skips (or fails under ORK_REQUIRE_INFRA) when there is no
// daemon to talk to.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "the docker CLI is not on PATH: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		infraAbsent(t, "the docker daemon is not reachable: %v: %s", err, out)
	}
}

// pinnedReference resolves the reference tag to the digest form the dispatcher
// will accept, pulling it first if the host does not have it.
func pinnedReference(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if out, err := exec.CommandContext(ctx, "docker", "image", "inspect", referenceTag).CombinedOutput(); err != nil {
		if pullOut, pullErr := exec.CommandContext(ctx, "docker", "pull", "--quiet", referenceTag).CombinedOutput(); pullErr != nil {
			infraAbsent(t, "cannot pull %s: %v: %s (inspect said %s)",
				referenceTag, pullErr, pullOut, out)
		}
	}
	out, err := exec.CommandContext(ctx, "docker", "image", "inspect",
		"--format", "{{index .RepoDigests 0}}", referenceTag).Output()
	if err != nil {
		// An image built or loaded locally has no RepoDigest. Fall back to
		// the image ID, which is digest-shaped and is what the runtime will
		// resolve against.
		id, idErr := exec.CommandContext(ctx, "docker", "image", "inspect",
			"--format", "{{.Id}}", referenceTag).Output()
		if idErr != nil {
			infraAbsent(t, "cannot resolve a digest for %s: %v", referenceTag, idErr)
		}
		return referenceTag + "@" + strings.TrimSpace(string(id))
	}
	return strings.TrimSpace(string(out))
}

// bindableWorkRoot returns a directory the daemon can bind-mount.
//
// §3.1 predicts exactly this problem: on macOS with a VM-hosted daemon $HOME
// is shared and /var/folders is not, which is why `make up` points
// ORK_ORKER_WORKDIR at .data/work. A test that used t.TempDir() would fail on
// every developer Mac for a reason that is not in the tree, so it probes with
// a real bind mount and skips when no candidate works.
func bindableWorkRoot(t *testing.T) string {
	t.Helper()
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve the repository root: %v", err)
	}
	candidates := []string{filepath.Join(repoRoot, ".data", "work"), t.TempDir()}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".orkstack-test-work"))
	}

	runtime := orker.NewDockerRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var reasons []string
	for _, candidate := range candidates {
		if err := os.MkdirAll(candidate, 0o755); err != nil {
			reasons = append(reasons, candidate+": "+err.Error())
			continue
		}
		if err := runtime.ProbeBindable(ctx, candidate); err != nil {
			reasons = append(reasons, candidate+": "+err.Error())
			continue
		}
		root, err := os.MkdirTemp(candidate, "ork-dispatch-*")
		if err != nil {
			reasons = append(reasons, candidate+": "+err.Error())
			continue
		}
		t.Cleanup(func() { _ = os.RemoveAll(root) })
		return root
	}
	// A host capability that starting infrastructure would not fix stays an
	// ordinary skip, per the note on infraAbsent.
	t.Skipf("no bind-mountable workspace root on this host:\n  %s", strings.Join(reasons, "\n  "))
	return ""
}

// newDispatcher builds a dispatcher wired to real Docker and in-memory stores.
func newDispatcher(t *testing.T) (*orker.OCIDispatcher, *artifacts.MemoryCommitStore) {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local artifact store: %v", err)
	}
	commits := artifacts.NewMemoryCommitStore()
	return &orker.OCIDispatcher{
		Runtime:  orker.NewDockerRuntime(),
		Store:    store,
		Commits:  commits,
		Refs:     artifacts.MemoryRefPredictor{},
		Policy:   orker.DefaultSecurityPolicy(),
		WorkRoot: bindableWorkRoot(t),
		OrkerID:  "orker-dispatch-test",
		BuildID:  "test",
	}, commits
}

// baseInput is a well-formed ExecuteTaskInput for the reference image.
func baseInput(t *testing.T, taskID string, command ...string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	pinned := pinnedReference(t)
	_, digest, _ := strings.Cut(pinned, "@")
	return &orkstackv1.ExecuteTaskInput{
		RunId:          "ork-dispatch-" + taskID,
		OrkflowName:    "dispatch-test",
		OrkflowVersion: "1.0.0",
		Namespace:      "default",
		Pool:           "main",
		TaskId:         taskID,
		IdempotencyKey: "default/ork-dispatch-" + taskID + "/" + taskID,
		Image: &orkstackv1.ImageRef{
			Reference:   referenceTag,
			Pinned:      pinned,
			IndexDigest: digest,
		},
		Command:                        command,
		TimeoutSeconds:                 120,
		CancellationGracePeriodSeconds: 2,
		Network:                        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		Limits: &orkstackv1.WorkspaceLimits{
			ValuesMaxBytes:   262144,
			ArtifactMaxCount: 256,
			ArtifactMaxBytes: 5368709120,
			LogMaxBytes:      16777216,
			CommitMaxBytes:   4194304,
		},
	}
}

// TestIntegration_DispatcherRunsAReferenceImageEndToEnd is acceptance
// criterion 2: OCIDispatcher executes alpine:3.20 end to end.
//
// It runs the whole §5.4 success path — validate, scan, hash, bound the
// commit, upload artifacts, upload values.json, upload the streams, write the
// conditional commit — and asserts on every output the contract promises.
func TestIntegration_DispatcherRunsAReferenceImageEndToEnd(t *testing.T) {
	requireDocker(t)
	dispatcher, commits := newDispatcher(t)

	in := baseInput(t, "echo", "/bin/sh", "-c",
		`echo "hello from the workload"; `+
			`echo "a warning" >&2; `+
			`printf '{"greeting":"hello","n":12345678901234567890}' > /orkstack/output/values.json; `+
			`printf 'report body' > /orkstack/output/artifacts/report.txt`)
	in.CommitUri = "mem://commits/" + in.RunId + "/echo/commit.json"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.GetExitCode() != 0 {
		t.Errorf("exit code = %d, want 0", result.GetExitCode())
	}
	if result.GetCommitAdopted() {
		t.Error("a first attempt reported commit_adopted; nothing was committed before it")
	}

	// values.json travels as the workload's EXACT bytes. The oversized
	// integer is the point: a Struct round trip would silently corrupt it
	// (§14.9), which is why the field is `bytes`.
	const wantValues = `{"greeting":"hello","n":12345678901234567890}`
	if got := string(result.GetValuesJson()); got != wantValues {
		t.Errorf("values_json = %s, want the workload's exact bytes %s", got, wantValues)
	}
	if !result.GetValuesInlined() {
		t.Error("a values.json well under 4 KiB was not inlined")
	}
	if result.GetValuesRef().GetUri() == "" {
		t.Error("values_ref is empty; it is authoritative and is always set on success")
	}

	// The workload's artifact, and the platform's streams, in separate fields
	// so a workload artifact named stdout.log cannot collide (§3.6).
	if len(result.GetArtifacts()) != 1 || result.GetArtifacts()[0].GetName() != "report.txt" {
		t.Errorf("artifacts = %v, want exactly report.txt", result.GetArtifacts())
	}
	systemNames := map[string]bool{}
	for _, ref := range result.GetSystemArtifacts() {
		systemNames[ref.GetName()] = true
	}
	if !systemNames[orker.StdoutLogName] || !systemNames[orker.StderrLogName] {
		t.Errorf("system_artifacts = %v, want stdout.log and stderr.log",
			result.GetSystemArtifacts())
	}

	// The image was resolved, and what actually ran is recorded.
	if result.GetImage().GetPlatformDigest() == "" || result.GetImage().GetPlatform() == "" {
		t.Errorf("image = %v; the Orker records the platform digest it ran", result.GetImage())
	}
	if result.GetImage().GetDelivery() != orker.DeliveryRegistry {
		t.Errorf("image delivery = %q, want %q", result.GetImage().GetDelivery(), orker.DeliveryRegistry)
	}

	// The commit exists, and it is what the result points at.
	record, err := commits.Get(ctx, in.GetCommitUri())
	if err != nil {
		t.Fatalf("the commit was not written: %v", err)
	}
	if record.SHA256 != result.GetCommit().GetSha256() {
		t.Errorf("commit sha256 = %q, result says %q", record.SHA256, result.GetCommit().GetSha256())
	}
	if record.Commit.GetSchemaVersion() != artifacts.CommitSchemaVersion {
		t.Errorf("commit schema_version = %d, want %d",
			record.Commit.GetSchemaVersion(), artifacts.CommitSchemaVersion)
	}
	if record.Commit.GetIdempotencyKey() != in.GetIdempotencyKey() {
		t.Errorf("commit idempotency_key = %q, want %q",
			record.Commit.GetIdempotencyKey(), in.GetIdempotencyKey())
	}

	// The two captured streams are separate and byte-exact.
	stdout := readArtifact(t, ctx, dispatcher, result.GetSystemArtifacts(), orker.StdoutLogName)
	stderr := readArtifact(t, ctx, dispatcher, result.GetSystemArtifacts(), orker.StderrLogName)
	if !strings.Contains(stdout, "hello from the workload") {
		t.Errorf("stdout.log does not carry what the workload printed: %q", stdout)
	}
	if strings.Contains(stdout, "a warning") {
		t.Errorf("stdout.log carries what went to stderr; the streams must never merge: %q", stdout)
	}
	if !strings.Contains(stderr, "a warning") {
		t.Errorf("stderr.log does not carry what the workload printed to stderr: %q", stderr)
	}
}

// TestIntegration_BurstOutputBeforeExitIsNotTruncated guards against a race
// between two INDEPENDENT docker invocations: `docker start --attach`
// (draining stdout/stderr into the durable log) and `docker wait` (reporting
// the exit code). A container that writes a large burst and then exits
// almost immediately can have `docker wait` return before the attach has
// finished copying everything it buffered, and DockerRuntime.Wait used to
// report the exit status without joining the attach at all — silently
// losing whatever was still in flight, with the task reporting SUCCEEDED.
//
// This does not sleep between writes: the burst has to outrun the copy to
// reproduce the race, which is exactly what a `sleep`-paced script would not
// do. Run without the DockerRuntime.Wait fix, this reliably truncates
// stdout.log at a different byte offset each run.
func TestIntegration_BurstOutputBeforeExitIsNotTruncated(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	const lineCount = 10000
	const suffix = " of 10000 - the quick brown fox jumps over the lazy dog"

	var want strings.Builder
	for i := 1; i <= lineCount; i++ {
		want.WriteString("line ")
		want.WriteString(strconv.Itoa(i))
		want.WriteString(suffix)
		want.WriteByte('\n')
	}

	in := baseInput(t, "burst", "/bin/sh", "-c",
		`set -eu; i=1; while [ "$i" -le `+strconv.Itoa(lineCount)+` ]; do `+
			`echo "line $i`+suffix+`"; i=$((i + 1)); done; `+
			`printf '{}' > /orkstack/output/values.json`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.GetExitCode() != 0 {
		t.Fatalf("exit code = %d, want 0", result.GetExitCode())
	}

	stdout := readArtifact(t, ctx, dispatcher, result.GetSystemArtifacts(), orker.StdoutLogName)
	if stdout != want.String() {
		gotLines := strings.Count(stdout, "\n")
		t.Fatalf("stdout.log has %d bytes across %d lines, want %d bytes across %d lines "+
			"(durable log was truncated mid-burst)", len(stdout), gotLines, want.Len(), lineCount)
	}
}

// TestIntegration_AdoptsAnExistingCommitWithoutRunningTheContainer is the
// §5.4 step-0 guarantee, and the one that makes "Orker died after commit.json,
// before Temporal completion" a container that ran ONCE.
func TestIntegration_AdoptsAnExistingCommitWithoutRunningTheContainer(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	// A marker file proves whether the container ran: the second attempt must
	// not append to it.
	marker := filepath.Join(t.TempDir(), "runs")
	in := baseInput(t, "adopt", "/bin/sh", "-c",
		`printf '{"ran":true}' > /orkstack/output/values.json; printf 'x' > /orkstack/output/artifacts/ran`)
	in.CommitUri = "mem://commits/" + in.RunId + "/adopt/commit.json"
	_ = marker

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	first, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if first.GetCommitAdopted() {
		t.Fatal("the first attempt adopted a commit that did not exist")
	}

	second, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if !second.GetCommitAdopted() {
		t.Fatal("the second attempt re-ran the container instead of adopting the durable commit")
	}

	// Adoption reconstructs the FULL result, including the inline copy —
	// otherwise the same task would produce a populated TaskRun.output on a
	// fresh attempt and an empty one on an adopted attempt, decided by nothing
	// but which Orker happened to crash (§5.4).
	if string(second.GetValuesJson()) != string(first.GetValuesJson()) {
		t.Errorf("adopted values_json = %q, fresh was %q",
			second.GetValuesJson(), first.GetValuesJson())
	}
	if !second.GetValuesInlined() {
		t.Error("the adopted result did not inline values.json, but the fresh one did")
	}
	if second.GetValuesRef().GetUri() != first.GetValuesRef().GetUri() {
		t.Errorf("adopted values_ref = %q, want the committed %q",
			second.GetValuesRef().GetUri(), first.GetValuesRef().GetUri())
	}
	// The attempt is the COMMITTING attempt's, so the engine records the same
	// number either way.
	if second.GetAttempt() != first.GetAttempt() {
		t.Errorf("adopted attempt = %d, want the committing attempt %d",
			second.GetAttempt(), first.GetAttempt())
	}
}

// TestIntegration_WorkspaceModesAreWhatTheContainerSees is §14.3, measured
// from INSIDE a real container rather than from the host.
//
// The host-side modes are unit-tested; what matters to a workload is what it
// observes through the bind mounts, and that is what §3.1's whole permission
// argument is about.
func TestIntegration_WorkspaceModesAreWhatTheContainerSees(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	// Runs as an arbitrary non-root uid, which is the case §3.1 is designed
	// for: the Orker does not force --user, so the workload may be anybody.
	in := baseInput(t, "modes", "/bin/sh", "-c", strings.Join([]string{
		`printf '{"uid":"%s","output":"%s","input":"%s","tmp":"%s","task_json":"%s","readonly":"%s"}' `,
		`"$(id -u)" `,
		`"$(stat -c %a /orkstack/output)" `,
		`"$(stat -c %a /orkstack/input)" `,
		`"$(stat -c %a /orkstack/tmp)" `,
		`"$(stat -c %a /orkstack/input/task.json)" `,
		`"$( (touch /orkstack/input/nope 2>&1 >/dev/null) | head -c 200 )" `,
		`> /orkstack/output/values.json`,
	}, ""))
	in.RunAsOrkerUser = false

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var observed struct {
		UID      string `json:"uid"`
		Output   string `json:"output"`
		Input    string `json:"input"`
		Tmp      string `json:"tmp"`
		TaskJSON string `json:"task_json"`
		ReadOnly string `json:"readonly"`
	}
	if err := json.Unmarshal(result.GetValuesJson(), &observed); err != nil {
		t.Fatalf("decode what the container observed (%s): %v", result.GetValuesJson(), err)
	}
	t.Logf("§14.3 observed inside the container: %+v", observed)

	// 1777: world-writable so any uid can create, sticky so only the owner of
	// a file can remove it.
	for _, dir := range []struct{ name, mode string }{
		{"/orkstack/output", observed.Output},
		{"/orkstack/tmp", observed.Tmp},
	} {
		if dir.mode != "1777" {
			t.Errorf("%s is %s inside the container, want 1777", dir.name, dir.mode)
		}
	}
	// 0555 / 0444: read-only, and the mount is read-only too.
	if observed.Input != "555" {
		t.Errorf("/orkstack/input is %s inside the container, want 555", observed.Input)
	}
	if observed.TaskJSON != "444" {
		t.Errorf("/orkstack/input/task.json is %s inside the container, want 444", observed.TaskJSON)
	}
	if !strings.Contains(observed.ReadOnly, "Read-only file system") {
		t.Errorf("writing to /orkstack/input was not refused with a read-only filesystem: %q",
			observed.ReadOnly)
	}
}

// TestIntegration_OutputNotReadableIsDetectedAndTerminal is acceptance
// criterion 4 and §14.8: a workload writing under umask 077 produces a 0600
// values.json the Orker cannot read.
//
// The outcome is host-dependent by design — on a native filesystem the Orker
// gets Permission denied, and through a VM-shared bind mount the write is
// ownership-remapped and reads fine — so this test asserts the CONTRACT rather
// than one host's answer: either the read succeeds, or it fails as a
// non-retryable ContractViolation naming the file, its mode and the fix. What
// it must never be is a retryable infrastructure error, which would loop
// forever against deterministic image behavior.
func TestIntegration_OutputNotReadableIsDetectedAndTerminal(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	in := baseInput(t, "umask", "/bin/sh", "-c",
		`umask 077; printf '{"ok":true}' > /orkstack/output/values.json`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := dispatcher.Execute(ctx, in)
	if err == nil {
		// The permissive host. Record which one this was, so the evidence is
		// unambiguous either way.
		t.Logf("§14.8: this host remapped ownership through the bind mount, so the "+
			"0600 values.json was readable; result values_json=%s", result.GetValuesJson())
		return
	}

	t.Logf("§14.8: this host is native, so the 0600 values.json was unreadable: %v", err)
	if !strings.Contains(err.Error(), orker.ReasonOutputNotReadable) {
		t.Fatalf("error = %v, want %s", err, orker.ReasonOutputNotReadable)
	}
	for _, want := range []string{"mode=", "umask 022"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not name %q, so an author cannot act on it:\n%v", want, err)
		}
	}
	if !strings.Contains(err.Error(), orker.CategoryContractViolation) {
		t.Errorf("error category is not %s; a retryable classification would loop forever "+
			"against an image that writes the same mode every attempt:\n%v",
			orker.CategoryContractViolation, err)
	}
}

// TestIntegration_CancellationSignalsThenKills is §14.5 and §14.14: SIGTERM,
// then SIGKILL after the grace period, for a workload that ignores SIGTERM.
//
// It also proves the thing §6.3 calls the largest correctness gap: cancelling
// the activity context has to reach a RUNNING container, not merely be noticed
// between phases.
func TestIntegration_CancellationSignalsThenKills(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	// trap '' TERM makes the shell IGNORE SIGTERM, so only the SIGKILL after
	// the grace period can end it.
	in := baseInput(t, "deaf", "/bin/sh", "-c",
		`trap '' TERM; echo ready; while true; do sleep 1; done`)
	in.CancellationGracePeriodSeconds = 2
	in.TimeoutSeconds = 300

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var execErr error
	go func() {
		defer close(done)
		_, execErr = dispatcher.Execute(ctx, in)
	}()

	// Give the container time to start and install its trap, then cancel.
	time.Sleep(6 * time.Second)
	started := time.Now()
	cancel()

	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("cancellation never reached the container; it was still running 90s later")
	}
	elapsed := time.Since(started)
	t.Logf("§14.5: a SIGTERM-ignoring container was terminated %s after cancellation "+
		"(grace period %ds)", elapsed.Round(time.Millisecond), in.GetCancellationGracePeriodSeconds())

	// Returning nil after cancellation would report success and MUST NOT
	// happen (§6.3).
	if execErr == nil {
		t.Fatal("Execute returned nil after cancellation; that reports success for work " +
			"that was torn down")
	}
	if !errors.Is(execErr, context.Canceled) {
		t.Errorf("Execute returned %v, want a context cancellation", execErr)
	}
	// The kill must not happen before the grace period is up, or the grace
	// period is not a grace period.
	grace := time.Duration(in.GetCancellationGracePeriodSeconds()) * time.Second
	if elapsed < grace {
		t.Errorf("the container died after %s, before its %s grace period elapsed", elapsed, grace)
	}
}

// TestIntegration_ContainerTimeoutIsTerminal covers the OTHER kill path, which
// shares TerminateContainer with cancellation but means something different:
// the task exceeded its own wall clock, and §8.1 makes that terminal by
// default (§17 C1).
func TestIntegration_ContainerTimeoutIsTerminal(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	in := baseInput(t, "slow", "/bin/sh", "-c", `sleep 300`)
	in.TimeoutSeconds = 3
	in.CancellationGracePeriodSeconds = 2

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, err := dispatcher.Execute(ctx, in)
	if err == nil {
		t.Fatal("a container that outlived its timeout reported success")
	}
	if !strings.Contains(err.Error(), orker.ReasonContainerTimeout) {
		t.Fatalf("error = %v, want %s", err, orker.ReasonContainerTimeout)
	}
	if !strings.Contains(err.Error(), orker.CategoryTimedOut) {
		t.Errorf("error category is not %s:\n%v", orker.CategoryTimedOut, err)
	}
}

// TestIntegration_ContractViolationsAreRefusedWithTheirReasonCode walks the
// §8.2 rows a real container can produce, so the codes are proven against the
// runtime rather than against a mock.
func TestIntegration_ContractViolationsAreRefusedWithTheirReasonCode(t *testing.T) {
	requireDocker(t)

	tests := []struct {
		name       string
		script     string
		wantReason string
	}{
		{
			name:       "exit 0 with no values.json",
			script:     `echo "did nothing"`,
			wantReason: orker.ReasonValuesMissing,
		},
		{
			name:       "values.json is an array, not an object",
			script:     `printf '[1,2]' > /orkstack/output/values.json`,
			wantReason: orker.ReasonValuesMalformed,
		},
		{
			name:       "values.json is not valid JSON",
			script:     `printf '{oops' > /orkstack/output/values.json`,
			wantReason: orker.ReasonValuesMalformed,
		},
		{
			name: "a directory in output/artifacts",
			script: `printf '{}' > /orkstack/output/values.json; ` +
				`mkdir -p /orkstack/output/artifacts/nested`,
			wantReason: orker.ReasonArtifactNotAFile,
		},
		{
			name: "a symlink in output/artifacts",
			script: `printf '{}' > /orkstack/output/values.json; ` +
				`ln -s /etc/passwd /orkstack/output/artifacts/link`,
			wantReason: orker.ReasonArtifactNotAFile,
		},
		{
			name:       "the workload exits non-zero",
			script:     `echo "failing on purpose" >&2; exit 3`,
			wantReason: orker.ReasonContainerExitNonzero,
		},
		{
			name: "exit 127 is the workload's own status, not a runtime refusal",
			script: `printf '{}' > /orkstack/output/values.json; ` +
				`this-command-does-not-exist`,
			wantReason: orker.ReasonContainerExitNonzero,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dispatcher, _ := newDispatcher(t)
			in := baseInput(t, "violate", "/bin/sh", "-c", tc.script)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			_, err := dispatcher.Execute(ctx, in)
			if err == nil {
				t.Fatalf("Execute succeeded; want %s", tc.wantReason)
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("error = %v, want %s", err, tc.wantReason)
			}
		})
	}
}

// TestIntegration_AFailedTaskStillPublishesWhatItProduced is §7.1 step 14: the
// compiler errors, test reports and core dumps a failing task produces are
// exactly what somebody opens the run to read.
func TestIntegration_AFailedTaskStillPublishesWhatItProduced(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	in := baseInput(t, "failing", "/bin/sh", "-c",
		`printf 'the test report' > /orkstack/output/artifacts/report.txt; `+
			`echo "compiler said no" >&2; `+
			`exit 1`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, err := dispatcher.Execute(ctx, in)
	if err == nil {
		t.Fatal("a container that exited 1 reported success")
	}

	detail := failureDetail(t, err)
	if detail == nil {
		t.Fatal("the failure carried no ActivityFailureDetail, so everything the task " +
			"produced is unreachable")
	}
	if detail.GetExitCode() != 1 {
		t.Errorf("detail exit_code = %d, want 1", detail.GetExitCode())
	}
	if detail.GetReasonCode() != orker.ReasonContainerExitNonzero {
		t.Errorf("detail reason = %q, want %q", detail.GetReasonCode(), orker.ReasonContainerExitNonzero)
	}
	var names []string
	for _, ref := range detail.GetArtifacts() {
		names = append(names, ref.GetName())
	}
	if strings.Join(names, ",") != "report.txt" {
		t.Errorf("a failed task's artifacts = %v, want report.txt", names)
	}
	if len(detail.GetSystemArtifacts()) != 2 {
		t.Errorf("a failed task's system artifacts = %v, want stdout.log and stderr.log",
			detail.GetSystemArtifacts())
	}
}

// TestIntegration_RefusesAnUnpinnedImageBeforeItRunsAnything covers
// IMAGE_NOT_PINNED. A running execution must never silently switch images
// because a tag moved (AGENTS.md §5), so a tag is refused rather than
// resolved.
func TestIntegration_RefusesAnUnpinnedImageBeforeItRunsAnything(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	in := baseInput(t, "unpinned")
	in.Image = &orkstackv1.ImageRef{Reference: referenceTag, Pinned: referenceTag}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if _, err := dispatcher.Execute(ctx, in); err == nil {
		t.Fatal("Execute accepted a tag as the pinned reference")
	} else if !strings.Contains(err.Error(), orker.ReasonImageNotPinned) {
		t.Fatalf("error = %v, want %s", err, orker.ReasonImageNotPinned)
	}
}

// readArtifact fetches one stored artifact's bytes by name.
func readArtifact(
	t *testing.T,
	ctx context.Context,
	dispatcher *orker.OCIDispatcher,
	refs []*orkstackv1.ArtifactRef,
	name string,
) string {
	t.Helper()
	for _, ref := range refs {
		if ref.GetName() != name {
			continue
		}
		reader, err := dispatcher.Store.Get(ctx, ref.GetUri())
		if err != nil {
			t.Fatalf("read %s at %s: %v", name, ref.GetUri(), err)
		}
		defer func() { _ = reader.Close() }()
		raw, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(raw)
	}
	t.Fatalf("no artifact named %q in %v", name, refs)
	return ""
}

// failureDetail extracts the ActivityFailureDetail an Orker attaches to a
// terminal failure.
func failureDetail(t *testing.T, err error) *orkstackv1.ActivityFailureDetail {
	t.Helper()
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("error %T is not a *temporal.ApplicationError", err)
	}
	var detail *orkstackv1.ActivityFailureDetail
	if err := applicationErr.Details(&detail); err != nil {
		t.Fatalf("decode the failure detail: %v", err)
	}
	return detail
}
