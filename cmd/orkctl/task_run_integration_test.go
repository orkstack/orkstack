//go:build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The `orkctl task run` integration scenarios: a real engine and a
// real Orker against the real Temporal and MinIO (or the local backend)
// produce a run, and the BUILT orkctl replays one of its tasks on this
// machine's container runtime, through the Orker's own dispatcher.

// taskRunFixtureDoc is task-run-fixture: `produce` writes payload.txt holding
// a random marker and reports the run's `target` input as its value; `consume`
// prints its config.json and task.json between marker lines, then exits 1
// unless the upstream value equals `expect` and the staged artifact holds the
// marker. Submitted with target=staging, expect=prod, the run fails at
// consume. strict=false removes the comparison (the edited-document case).
func taskRunFixtureDoc(name, marker string, strict bool) string {
	comparison := `if [ "{{ .tasks.produce.output.value }}" != "{{ .inputs.expect }}" ]; then
          echo "mismatch: {{ .tasks.produce.output.value }} != {{ .inputs.expect }}" >&2
          exit 1
        fi
        `
	if !strict {
		comparison = ""
	}
	return fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
description: orkctl task run fixture.
inputs:
  target:
    type: string
  expect:
    type: string
tasks:
  - id: produce
    uses: shell
    timeout_seconds: 120
    with:
      op: eval
      command: ["sh", "-c", "printf '%%s\n' '%s' > /orkstack/output/artifacts/payload.txt && printf '%%s' '{{ .inputs.target }}'"]
  - id: consume
    uses: shell
    needs: [produce]
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        echo '<<<CONFIG'
        cat /orkstack/input/config.json
        echo 'CONFIG>>>'
        echo '<<<TASK'
        cat /orkstack/input/task.json
        echo 'TASK>>>'
        %sgrep -q '%s' '{{ artifact "produce/payload.txt" }}' || exit 1
`, name, marker, comparison, marker)
}

// taskRunStack is a timing stack (one replica, one Orker, MinIO) plus the
// directory the built orkctl's sessions land in.
type taskRunStack struct {
	*timingStack
	sessions string
}

func newTaskRunStack(ctx context.Context, t *testing.T) *taskRunStack {
	t.Helper()
	s := newTimingStack(ctx, t, 1)
	s.startOrker()
	sessions := filepath.Join(bindableOrkerWorkRoot(t), "task-run-sessions")
	if err := os.MkdirAll(sessions, 0o755); err != nil {
		t.Fatal(err)
	}
	return &taskRunStack{timingStack: s, sessions: sessions}
}

// taskRun runs the built orkctl's `task run` against the replica, with its
// session root pointed at a directory this test owns.
func (s *taskRunStack) taskRun(args ...string) (string, string, int) {
	s.t.Helper()
	return runTaskRunBinary(s.ctx, s.t, s.orkctlPath, s.root, s.sessions, "http://"+s.replicas[0].addr, &s.transcript, args...)
}

func runTaskRunBinary(ctx context.Context, t *testing.T, orkctl, dir, sessions, server string, transcript *strings.Builder, args ...string) (string, string, int) {
	t.Helper()
	full := append(append([]string{"task", "run"}, args...), "--server", server)
	cmd := exec.CommandContext(ctx, orkctl, full...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), orker.WorkRootVar+"="+sessions)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("run orkctl: %v", err)
	}
	fmt.Fprintf(transcript, "$ orkctl %s\n%s", strings.Join(full[:len(full)-2], " "), stdout.String())
	if stderr.Len() > 0 {
		fmt.Fprintf(transcript, "(stderr) %s", stderr.String())
	}
	fmt.Fprintf(transcript, "(exit %d)\n\n", code)
	return stdout.String(), stderr.String(), code
}

var keptAt = regexp.MustCompile(`(?m)^workspace kept at (.+)$`)

func keptSession(t *testing.T, stdout string) string {
	t.Helper()
	match := keptAt.FindStringSubmatch(stdout)
	if match == nil {
		t.Fatalf("no `workspace kept at` line:\n%s", stdout)
	}
	t.Cleanup(func() { _ = (&orker.Workspace{Root: match[1]}).Remove() })
	return match[1]
}

// failedFixtureRun applies task-run-fixture and runs it to its failure at
// consume; it returns the orkflow name, the marker and the finished run.
func (s *taskRunStack) failedFixtureRun() (string, string, *orkstackv1.Run) {
	s.t.Helper()
	name := fmt.Sprintf("task-run-fixture-%d-%d", os.Getpid(), time.Now().UnixNano())
	marker := fmt.Sprintf("marker-%d", time.Now().UnixNano())
	s.apply(taskRunFixtureDoc(name, marker, true))
	runID := s.submit(name, "--input", "target=staging", "--input", "expect=prod")
	run := awaitTerminalOnB(s.ctx, s.t, s.replicas[0], runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		s.t.Fatalf("run %s finished %s, want FAILED at consume: %v", runID, run.GetStatus(), run.GetError())
	}
	return name, marker, run
}

func taskRunNamed(run *orkstackv1.Run, id string) *orkstackv1.TaskRun {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == id {
			return tr
		}
	}
	return nil
}

// productionStdout reads a task's recorded stdout.log through ReadArtifact.
func productionStdout(t *testing.T, runs orkstackv1connect.RunServiceClient, ctx context.Context, runID, taskID string) string {
	t.Helper()
	var buf bytes.Buffer
	if _, err := readRecordedArtifact(ctx, runs, &orkstackv1.ReadArtifactRequest{
		RunId: runID, TaskId: taskID, Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT, Name: orker.StdoutLogName,
	}, &buf); err != nil {
		t.Fatalf("read %s/%s stdout.log: %v", runID, taskID, err)
	}
	return buf.String()
}

// between returns the text between a "<<<TAG" line and a "TAG>>>" line.
func between(t *testing.T, text, tag string) string {
	t.Helper()
	start := strings.Index(text, "<<<"+tag+"\n")
	end := strings.Index(text, tag+">>>\n")
	if start < 0 || end < start {
		t.Fatalf("no %s block in:\n%s", tag, text)
	}
	return text[start+len("<<<"+tag+"\n") : end]
}

func withoutIdentity(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("task.json: %v\n%s", err, raw)
	}
	delete(doc, "idempotencyKey")
	if run, ok := doc["run"].(map[string]any); ok {
		delete(run, "id")
	}
	return doc
}

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// replayFailedConsume is the replay assertion set against a failed fixture run. It
// returns the two invocations' stdout.
// It returns the local run ids of the two task runs it made.
func (s *taskRunStack) replayFailedConsume(run *orkstackv1.Run) []string {
	t := s.t
	stdout, stderr, code := s.taskRun("--from-run", run.GetId(), "--task", "consume", "--keep")
	if code != exitRunFailed || !strings.Contains(stdout, "FAILED  CONTAINER_EXIT_NONZERO (ActivityFailed)") {
		t.Fatalf("replay exit = %d, want %d with CONTAINER_EXIT_NONZERO\nstdout:\n%s\nstderr:\n%s", code, exitRunFailed, stdout, stderr)
	}
	session := keptSession(t, stdout)
	localIDs := []string{localRunID(t, stdout)}

	production := productionStdout(t, s.replicas[0].runs, s.ctx, run.GetId(), "consume")
	keptConfig, err := os.ReadFile(filepath.Join(session, "input", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(keptConfig) != between(t, production, "CONFIG") {
		t.Fatalf("config.json differs from the production attempt's\nlocal:\n%s\nproduction:\n%s", keptConfig, between(t, production, "CONFIG"))
	}
	t.Logf("EVIDENCE config.json byte-identical to the production attempt's (%d bytes)", len(keptConfig))
	proof.Write(t, "task-run", "config-identical.txt", []byte(fmt.Sprintf(
		"run %s task consume\nproduction config.json (between the attempt's own markers, read back via ReadArtifact STDOUT):\n%s"+
			"local input/config.json:\n%s"+"byte-identical: %t (%d bytes)\n",
		run.GetId(), between(t, production, "CONFIG"), keptConfig, string(keptConfig) == between(t, production, "CONFIG"), len(keptConfig))))

	keptTask, err := os.ReadFile(filepath.Join(session, "input", "task.json"))
	if err != nil {
		t.Fatal(err)
	}
	local, remote := withoutIdentity(t, keptTask), withoutIdentity(t, []byte(between(t, production, "TASK")))
	localJSON, _ := json.Marshal(local)
	remoteJSON, _ := json.Marshal(remote)
	if !bytes.Equal(localJSON, remoteJSON) {
		t.Fatalf("task.json differs beyond run.id and idempotencyKey\nlocal:  %s\nremote: %s", localJSON, remoteJSON)
	}
	t.Logf("EVIDENCE task.json equal to the production attempt's after removing run.id and idempotencyKey")

	produce := taskRunNamed(run, "produce")
	if got := fileSHA(t, filepath.Join(session, "input", "upstream", "produce", "values.json")); got != produce.GetValuesRef().GetSha256() {
		t.Fatalf("staged values.json hashes to %s, the run recorded %s", got, produce.GetValuesRef().GetSha256())
	}
	if got := fileSHA(t, filepath.Join(session, "input", "artifacts", "produce", "payload.txt")); got != produce.GetArtifacts()[0].GetSha256() {
		t.Fatalf("staged payload.txt hashes to %s, the run recorded %s", got, produce.GetArtifacts()[0].GetSha256())
	}
	t.Logf("EVIDENCE staged upstream bytes hash to the run's recorded digests (values %s, payload %s)",
		produce.GetValuesRef().GetSha256(), produce.GetArtifacts()[0].GetSha256())

	stdout, stderr, code = s.taskRun("--from-run", run.GetId(), "--task", "consume", "--input", "expect=staging")
	if code != exitOK || !regexp.MustCompile(`(?m)^SUCCEEDED$`).MatchString(stdout) || !regexp.MustCompile(`(?m)^values {7}\{`).MatchString(stdout) {
		t.Fatalf("replay with --input expect=staging exit = %d, want 0 with values\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	t.Logf("EVIDENCE --input expect=staging: SUCCEEDED, exit 0")
	return append(localIDs, localRunID(t, stdout))
}

// TestIntegration_TaskRunReplaysAFailedTaskAgainstItsRunsBytes.
func TestIntegration_TaskRunReplaysAFailedTaskAgainstItsRunsBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTaskRunStack(ctx, t)
	_, _, run := s.failedFixtureRun()
	s.replayFailedConsume(run)
	s.writeTranscript("task-run-replay.txt")
}

// bucketListing is the artifact store's listing, one line per object.
func bucketListing(t *testing.T, ctx context.Context, cfg artifacts.Config) []string {
	t.Helper()
	store, err := artifacts.NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatalf("open the bucket: %v", err)
	}
	refs, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("list the bucket: %v", err)
	}
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, fmt.Sprintf("%s %d %s", ref.URI, ref.SizeBytes, ref.SHA256))
	}
	sort.Strings(out)
	return out
}

// TestIntegration_TaskRunWritesNothingToTheDeployment.
func TestIntegration_TaskRunWritesNothingToTheDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTaskRunStack(ctx, t)
	_, _, run := s.failedFixtureRun()

	// "Before" is taken once the source run's archive record is readable, so
	// the engine's own terminal writes are complete.
	archive, err := artifacts.NewRunArchive(s.artifactCfg)
	if err != nil {
		t.Fatalf("open the run archive: %v", err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if _, err := archive.GetRun(ctx, run.GetId()); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("the source run's archive record never became readable: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	// Every run the deployment lists, paged at the 200-run maximum.
	listRuns := func() []string {
		var ids []string
		token := ""
		for {
			res, err := s.replicas[0].runs.ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{
				PageSize: 200, PageToken: token,
			}))
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			for _, r := range res.Msg.GetRuns() {
				ids = append(ids, r.GetId())
			}
			if token = res.Msg.GetNextPageToken(); token == "" {
				break
			}
		}
		sort.Strings(ids)
		return ids
	}
	attempts := func() string {
		res, err := s.replicas[0].runs.ListTaskAttempts(ctx, connect.NewRequest(&orkstackv1.ListTaskAttemptsRequest{
			RunId: run.GetId(), TaskId: "consume",
		}))
		if err != nil {
			t.Fatalf("ListTaskAttempts: %v", err)
		}
		raw, _ := json.Marshal(res.Msg.GetAttempts())
		return string(raw)
	}
	runsBefore, bucketBefore, attemptsBefore := listRuns(), bucketListing(t, ctx, s.artifactCfg), attempts()

	localIDs := s.replayFailedConsume(run)

	runsAfter, bucketAfter, attemptsAfter := listRuns(), bucketListing(t, ctx, s.artifactCfg), attempts()
	if strings.Join(runsAfter, ",") != strings.Join(runsBefore, ",") {
		t.Errorf("ListRuns changed:\nbefore %v\nafter  %v", runsBefore, runsAfter)
	}
	if strings.Join(bucketAfter, "\n") != strings.Join(bucketBefore, "\n") {
		t.Errorf("the bucket's artifact listing changed: %d objects before, %d after", len(bucketBefore), len(bucketAfter))
	}
	if attemptsAfter != attemptsBefore {
		t.Errorf("ListTaskAttempts(%s, consume) changed:\nbefore %s\nafter  %s", run.GetId(), attemptsBefore, attemptsAfter)
	}
	commits, err := artifacts.NewS3CommitStore(ctx, s.artifactCfg)
	if err != nil {
		t.Fatalf("open the commit store: %v", err)
	}
	root, err := artifacts.CommitRoot(s.artifactCfg)
	if err != nil {
		t.Fatal(err)
	}
	key, err := artifacts.CommitKey("default", run.GetId(), "consume")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := commits.Get(ctx, root+"/"+key); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("consume's commit key answered %v after a locally SUCCESSFUL replay, want ErrNotFound", err)
	}
	for _, localID := range localIDs {
		requireTaskRunSettled(ctx, t, s.sessions, localID)
	}
	report := fmt.Sprintf("source run %s (archive record readable before the snapshot)\n"+
		"ListRuns ids before: %d, after: %d, identical: %t\n"+
		"bucket artifact listing before: %d objects, after: %d objects, identical: %t\n"+
		"ListTaskAttempts(%s, consume) identical: %t\n"+
		"commit %s/%s after the local success: ErrNotFound\n"+
		"task runs %v: no container labelled ork.run, no lock file left\n",
		run.GetId(), len(runsBefore), len(runsAfter), strings.Join(runsAfter, ",") == strings.Join(runsBefore, ","),
		len(bucketBefore), len(bucketAfter), strings.Join(bucketAfter, "\n") == strings.Join(bucketBefore, "\n"),
		run.GetId(), attemptsAfter == attemptsBefore, root, key, localIDs)
	t.Logf("EVIDENCE\n%s", report)
	proof.Write(t, "task-run", "writes-nothing.txt", []byte(report))
	s.writeTranscript("task-run-writes-nothing.txt")
}

// TestIntegration_TaskRunRunsAnEditedDocumentAgainstTheRunsBytes.
func TestIntegration_TaskRunRunsAnEditedDocumentAgainstTheRunsBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTaskRunStack(ctx, t)
	name, marker, run := s.failedFixtureRun()
	versions := func() string {
		res, err := s.replicas[0].flows.ListOrkflowVersions(ctx, connect.NewRequest(&orkstackv1.ListOrkflowVersionsRequest{Name: name}))
		if err != nil {
			t.Fatalf("ListOrkflowVersions: %v", err)
		}
		raw, _ := json.Marshal(res.Msg)
		return string(raw)
	}
	before := versions()
	edited := filepath.Join(t.TempDir(), "edited.ork.yaml")
	doc := strings.Replace(taskRunFixtureDoc(name, marker, false), `version: "0.1.0"`, `version: "0.1.1"`, 1)
	if err := os.WriteFile(edited, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := s.taskRun("--file", edited, "--from-run", run.GetId(), "--task", "consume")
	if code != exitOK {
		t.Fatalf("edited replay exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	digests := regexp.MustCompile(`sha256:[0-9a-f]{64}`)
	definition := digests.FindString(regexp.MustCompile(`(?m)^definition .*$`).FindString(stdout))
	source := digests.FindString(regexp.MustCompile(`(?m)^source run .*$`).FindString(stdout))
	if definition == "" || source == "" || definition == source {
		t.Fatalf("the header does not show two different digests (definition %q, source run %q):\n%s", definition, source, stdout)
	}
	if after := versions(); after != before {
		t.Fatalf("task run --file stored something:\nbefore %s\nafter  %s", before, after)
	}
	t.Logf("EVIDENCE edited document ran: definition %s, source run %s, nothing stored", definition, source)
	s.writeTranscript("task-run-edited.txt")
}

// TestIntegration_TaskRunSecretIsExplicitAndNeverKept.
func TestIntegration_TaskRunSecretIsExplicitAndNeverKept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTaskRunStack(ctx, t)
	name := fmt.Sprintf("task-run-secret-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: %s
version: "0.1.0"
tasks:
  - id: secretive
    uses: shell
    timeout_seconds: 120
    secrets:
      - name: tok
        source: "env:NOT_ON_THIS_ORKER"
    with:
      op: exec
      command: ["sh", "-c", "sha256sum /orkstack/input/secrets/tok"]
`, name))
	runID := s.submit(name)
	run := awaitTerminalOnB(ctx, t, s.replicas[0], runID)
	if tr := taskRunNamed(run, "secretive"); tr.GetError().GetCode() != orker.ReasonSecretUnresolved {
		t.Fatalf("the run's secretive failed with %v, want %s", tr.GetError(), orker.ReasonSecretUnresolved)
	}
	stdout, stderr, code := s.taskRun("--from-run", runID, "--task", "secretive")
	if code != exitError || !strings.Contains(stderr, `task "secretive" declares secret "tok"; supply it with --secret tok=file:PATH`) {
		t.Fatalf("an unaccounted secret exit = %d, want 1\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if strings.Contains(stdout, "fetched") {
		t.Fatal("an unaccounted secret fetched")
	}
	marker := fmt.Sprintf("secret-marker-%d", time.Now().UnixNano())
	secretFile := filepath.Join(t.TempDir(), "tok")
	if err := os.WriteFile(secretFile, []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code = s.taskRun("--from-run", runID, "--task", "secretive", "--secret", "tok=file:"+secretFile, "--keep")
	if code != exitOK {
		t.Fatalf("supplied secret exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	want := fileSHA(t, secretFile)
	if !strings.Contains(stdout, want+"  /orkstack/input/secrets/tok") {
		t.Fatalf("the task did not see the supplied secret (want sha256 %s):\n%s", want, stdout)
	}
	if strings.Contains(stdout, marker) || strings.Contains(stderr, marker) {
		t.Fatal("the secret's value reached the transcript")
	}
	session := keptSession(t, stdout)
	_ = filepath.WalkDir(session, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if raw, readErr := os.ReadFile(path); readErr == nil && bytes.Contains(raw, []byte(marker)) {
			t.Errorf("kept file %s contains the secret", path)
		}
		return nil
	})
	t.Logf("EVIDENCE secret staged (sha256 %s seen by the task), no kept file holds it", want)
	s.writeTranscript("task-run-secret.txt")
}

// TestIntegration_TaskRunInterruptStopsTheContainer.
func TestIntegration_TaskRunInterruptStopsTheContainer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTaskRunStack(ctx, t)
	doc := filepath.Join(t.TempDir(), "interrupt.ork.yaml")
	if err := os.WriteFile(doc, []byte(fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: task-run-interrupt-%d
version: "0.1.0"
tasks:
  - id: looper
    uses: shell
    timeout_seconds: 300
    cancellation_grace_period_seconds: 5
    with:
      op: script
      interpreter: sh
      script: |
        trap 'echo trapped-TERM; exit 143' TERM
        echo ready
        while true; do sleep 1; done
`, time.Now().UnixNano())), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, s.orkctlPath, "task", "run", "--file", doc, "--task", "looper",
		"--server", "http://"+s.replicas[0].addr)
	cmd.Dir = s.root
	cmd.Env = append(os.Environ(), orker.WorkRootVar+"="+s.sessions)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	ready := make(chan struct{})
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		scanner := bufio.NewScanner(pipe)
		signalled := false
		for scanner.Scan() {
			out.WriteString(scanner.Text() + "\n")
			if !signalled && strings.Contains(scanner.Text(), "│ ready") {
				signalled = true
				close(ready)
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(3 * time.Minute):
		_ = cmd.Process.Kill()
		t.Fatalf("the container never printed its ready line\nstderr:\n%s", stderr.String())
	}
	interrupted := time.Now()
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { <-copied; waited <- cmd.Wait() }()
	var waitErr error
	select {
	case waitErr = <-waited:
	case <-time.After(5*time.Second + 30*time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("orkctl did not exit within the grace period plus 30s\nstdout:\n%s", out.String())
	}
	elapsed := time.Since(interrupted)
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != exitRunCanceled {
		t.Fatalf("orkctl exited %v, want %d\nstdout:\n%s\nstderr:\n%s", waitErr, exitRunCanceled, out.String(), stderr.String())
	}
	if !strings.Contains(out.String(), "trapped-TERM") {
		t.Fatalf("the workload's TERM trap never ran:\n%s", out.String())
	}
	interruptedID := localRunID(t, out.String())
	requireTaskRunSettled(ctx, t, s.sessions, interruptedID)
	fmt.Fprintf(&s.transcript, "$ orkctl task run --file %s --task looper   (SIGINT after the ready line)\n%s(stderr) %s(exit %d after %s)\n",
		doc, out.String(), stderr.String(), exitRunCanceled, elapsed.Round(time.Millisecond))
	t.Logf("EVIDENCE SIGINT -> trapped-TERM, exit 3 after %s, %s left no container and no lock file", elapsed.Round(time.Millisecond), interruptedID)
	s.writeTranscript("task-run-interrupt.txt")
}

// localRunID is the local run id a task run's header named.
func localRunID(t *testing.T, stdout string) string {
	t.Helper()
	match := localRunLine.FindStringSubmatch(stdout)
	if match == nil {
		t.Fatalf("no `local run` header line:\n%s", stdout)
	}
	return match[1]
}

// requireTaskRunSettled: the task run localID left no container (labelled
// ork.run=localID) and no lock file of its identity under sessions, which
// is what a run that removed its own container leaves (task_run_owner.go).
func requireTaskRunSettled(ctx context.Context, t *testing.T, sessions, localID string) {
	t.Helper()
	if id, label, running, present := taskRunContainer(ctx, t, localID); present {
		t.Errorf("task run %s left container %s (ork.orker=%s, running %t)", localID, id, label, running)
	}
	lock := filepath.Join(sessions, taskRunOrkerPrefix+strings.TrimPrefix(localID, "local-")+taskRunLockSuffix)
	if _, err := os.Stat(lock); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("task run %s left its lock file %s (%v)", localID, lock, err)
	}
}

// localRunLine is the header line that names a task run's local run id.
var localRunLine = regexp.MustCompile(`(?m)^local run\s+(local-[0-9a-f]+)`)

// startedTaskRun is one `orkctl task run` process the test started and whose
// container printed its ready line.
type startedTaskRun struct {
	cmd     *exec.Cmd
	localID string
	copied  chan struct{}
	stderr  *bytes.Buffer

	mu  sync.Mutex
	out bytes.Buffer
}

// output is what the run printed so far.
func (r *startedTaskRun) output() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.out.String()
}

// startLooper starts `orkctl task run --task looper` and returns once its
// container printed "ready", with the local run id its header named.
func (s *taskRunStack) startLooper(doc string) *startedTaskRun {
	s.t.Helper()
	cmd := exec.CommandContext(s.ctx, s.orkctlPath, "task", "run", "--file", doc, "--task", "looper",
		"--server", "http://"+s.replicas[0].addr)
	cmd.Dir = s.root
	cmd.Env = append(os.Environ(), orker.WorkRootVar+"="+s.sessions)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		s.t.Fatal(err)
	}
	run := &startedTaskRun{cmd: cmd, copied: make(chan struct{}), stderr: &bytes.Buffer{}}
	cmd.Stderr = run.stderr
	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	// A failed test still stops what it started and removes its container,
	// by the run's own label once the header named it.
	s.t.Cleanup(func() {
		_ = cmd.Process.Kill()
		if match := localRunLine.FindStringSubmatch(run.output()); match != nil {
			ids, _ := exec.Command(localContainerCLI(), "ps", "-a", "-q", "--filter", "label=ork.run="+match[1]).Output()
			for _, id := range strings.Fields(string(ids)) {
				_ = exec.Command(localContainerCLI(), "rm", "--force", id).Run()
			}
		}
	})
	ready := make(chan struct{})
	go func() {
		defer close(run.copied)
		scanner := bufio.NewScanner(pipe)
		scanner.Buffer(make([]byte, 64<<10), 4<<20)
		signalled := false
		for scanner.Scan() {
			run.mu.Lock()
			run.out.WriteString(scanner.Text() + "\n")
			run.mu.Unlock()
			if !signalled && strings.Contains(scanner.Text(), "│ ready") {
				signalled = true
				close(ready)
			}
		}
		_, _ = io.Copy(io.Discard, pipe)
	}()
	select {
	case <-ready:
	case <-run.copied:
		_ = cmd.Wait()
		s.t.Fatalf("the looper exited before its ready line\nstdout:\n%s\nstderr:\n%s", run.output(), run.stderr.String())
	case <-time.After(3 * time.Minute):
		_ = cmd.Process.Kill()
		s.t.Fatalf("the looper never printed its ready line\nstdout:\n%s", run.output())
	}
	run.localID = localRunID(s.t, run.output())
	return run
}

// taskRunContainer is the one container labelled ork.run=localID, its
// ork.orker label and whether it is running; ok is false when there is none.
func taskRunContainer(ctx context.Context, t *testing.T, localID string) (id, orkerLabel string, running, ok bool) {
	t.Helper()
	raw, err := exec.CommandContext(ctx, localContainerCLI(), "ps", "-a", "-q", "--filter", "label=ork.run="+localID).Output()
	if err != nil {
		t.Fatalf("list the containers labelled ork.run=%s: %v", localID, err)
	}
	ids := strings.Fields(string(raw))
	switch len(ids) {
	case 0:
		return "", "", false, false
	case 1:
	default:
		t.Fatalf("%d containers are labelled ork.run=%s, want at most 1: %v", len(ids), localID, ids)
	}
	answer, err := exec.CommandContext(ctx, localContainerCLI(), "inspect", "--format",
		`{{index .Config.Labels "ork.orker"}} {{.State.Running}}`, ids[0]).Output()
	if err != nil {
		t.Fatalf("inspect container %s: %v", ids[0], err)
	}
	fields := strings.Fields(string(answer))
	if len(fields) != 2 {
		t.Fatalf("inspect container %s answered %q", ids[0], answer)
	}
	return ids[0], fields[0], fields[1] == "true", true
}

// TestIntegration_TaskRunTouchesOnlyItsOwnContainers: two `orkctl task run`
// processes on one host label their containers with identities of their own,
// so one run's identity never names the other's container. A run killed with
// SIGKILL leaves its container running; the next task run removes that one,
// because its owner is gone, and never the container of a run still alive.
func TestIntegration_TaskRunTouchesOnlyItsOwnContainers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newTaskRunStack(ctx, t)
	doc := filepath.Join(t.TempDir(), "owners.ork.yaml")
	if err := os.WriteFile(doc, []byte(fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: task-run-owners-%d
version: "0.1.0"
tasks:
  - id: looper
    uses: shell
    timeout_seconds: 300
    cancellation_grace_period_seconds: 5
    with:
      op: script
      interpreter: sh
      script: |
        echo ready
        while true; do sleep 1; done
  - id: hello
    uses: shell
    timeout_seconds: 120
    with:
      op: eval
      command: ["sh", "-c", "echo hello"]
`, time.Now().UnixNano())), 0o644); err != nil {
		t.Fatal(err)
	}

	live := s.startLooper(doc)
	abandoned := s.startLooper(doc)
	_, liveOrker, liveRunning, ok := taskRunContainer(ctx, t, live.localID)
	if !ok || !liveRunning {
		t.Fatalf("the live run's container (ork.run=%s) is not running", live.localID)
	}
	abandonedContainer, abandonedOrker, abandonedRunning, ok := taskRunContainer(ctx, t, abandoned.localID)
	if !ok || !abandonedRunning {
		t.Fatalf("the second run's container (ork.run=%s) is not running", abandoned.localID)
	}
	if liveOrker == abandonedOrker {
		t.Errorf("both runs labelled their containers ork.orker=%s: one run's identity names the other's "+
			"container, so removing one run's containers removes the other's", liveOrker)
	}

	// SIGKILL runs nothing of orkctl's: the container stays, running, with
	// nobody watching it.
	if err := abandoned.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-abandoned.copied
	_ = abandoned.cmd.Wait()
	if _, _, running, ok := taskRunContainer(ctx, t, abandoned.localID); !ok || !running {
		t.Fatalf("the killed run's container %s stopped by itself (present %t, running %t); "+
			"the test needs one that outlives its run", abandonedContainer, ok, running)
	}

	stdout, stderr, code := s.taskRun("--file", doc, "--task", "hello")
	if code != exitOK {
		t.Fatalf("the third run exited %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitOK, stdout, stderr)
	}
	thirdID := localRunID(t, stdout)
	if _, _, _, present := taskRunContainer(ctx, t, abandoned.localID); present {
		t.Errorf("the killed run's container %s (ork.orker=%s) is still there after the next task run; "+
			"a container whose run is gone must be removed", abandonedContainer, abandonedOrker)
	}
	_, _, liveRunningAfter, livePresent := taskRunContainer(ctx, t, live.localID)
	if !livePresent || !liveRunningAfter {
		t.Fatalf("the live run's container (ork.run=%s, ork.orker=%s) was removed or stopped by the next "+
			"task run (present %t, running %t)", live.localID, liveOrker, livePresent, liveRunningAfter)
	}
	if !strings.Contains(stdout, abandonedOrker) {
		t.Errorf("the third run did not say it removed the killed run's container (ork.orker=%s):\n%s", abandonedOrker, stdout)
	}

	// The live run is still the one in charge of its container.
	if err := live.cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { <-live.copied; waited <- live.cmd.Wait() }()
	var waitErr error
	select {
	case waitErr = <-waited:
	case <-time.After(5*time.Second + 30*time.Second):
		_ = live.cmd.Process.Kill()
		t.Fatalf("the live run did not exit within its grace period plus 30s\nstdout:\n%s", live.output())
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != exitRunCanceled {
		t.Fatalf("the live run exited %v, want %d\nstdout:\n%s\nstderr:\n%s", waitErr, exitRunCanceled, live.output(), live.stderr.String())
	}
	// Every run is settled: no container, and no lock file left behind.
	for _, localID := range []string{live.localID, abandoned.localID, thirdID} {
		requireTaskRunSettled(ctx, t, s.sessions, localID)
	}
	report := fmt.Sprintf("live run %s: ork.orker=%s, running before and after the third run\n"+
		"killed run %s: ork.orker=%s, running after SIGKILL, removed by the third run %s\n"+
		"third run output:\n%s",
		live.localID, liveOrker, abandoned.localID, abandonedOrker, thirdID, stdout)
	t.Logf("EVIDENCE\n%s", report)
	proof.Write(t, "task-run", "own-containers.txt", []byte(report))
	fmt.Fprintf(&s.transcript, "$ orkctl task run --file %s --task looper   (live, SIGINT at the end)\n%s(stderr) %s\n",
		doc, live.output(), live.stderr.String())
	fmt.Fprintf(&s.transcript, "$ orkctl task run --file %s --task looper   (SIGKILL after the ready line)\n%s(stderr) %s\n",
		doc, abandoned.output(), abandoned.stderr.String())
	s.writeTranscript("task-run-own-containers.txt")
}

// TestIntegration_TaskRunRendersOnlyWithoutACreatedContainer.
func TestIntegration_TaskRunRendersOnlyWithoutACreatedContainer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newTaskRunStack(ctx, t)
	_, _, run := s.failedFixtureRun()
	stdout, stderr, code := s.taskRun("--json", "--from-run", run.GetId(), "--task", "consume", "--render-only")
	if code != exitOK {
		t.Fatalf("--render-only exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	var report taskRunReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("--json report: %v\n%s", err, stdout)
	}
	if report.Outcome != taskRunRendered || len(report.Config) == 0 || len(report.TaskJSON) == 0 {
		t.Fatalf("report outcome %q (config %d bytes, task.json %d bytes), want RENDERED with both", report.Outcome, len(report.Config), len(report.TaskJSON))
	}
	containers, err := exec.CommandContext(ctx, localContainerCLI(), "ps", "-a", "-q", "--filter", "label=ork.run="+report.LocalRunID).Output()
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	if strings.TrimSpace(string(containers)) != "" {
		t.Fatalf("containers labelled ork.run=%s exist: %s", report.LocalRunID, containers)
	}
	t.Logf("EVIDENCE --render-only: RENDERED, no container labelled ork.run=%s", report.LocalRunID)
	s.writeTranscript("task-run-render-only.txt")
}

// treeListing is every regular file under dir with its sha256.
func treeListing(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	_ = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, path)
		out = append(out, rel+" "+fileSHA(t, path))
		return nil
	})
	sort.Strings(out)
	return out
}

// TestIntegration_TaskRunReplaysALocalBackendRun: the same replay
// against an engine and Orker on the LOCAL artifact backend, so the recorded
// URIs are file:// and replayStore serves them.
func TestIntegration_TaskRunReplaysALocalBackendRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	address := temporalAddress()
	requireTemporal(t, address)
	pool := privateTaskPool()
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatal(err)
	}
	root := repoRoot(t)
	orkctlPath := testbin.Path(t, "./cmd/orkctl")
	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")
	namespace := isolatedNamespace()
	_ = dialIsolatedNamespace(ctx, t, address, namespace)
	startOrker(ctx, t, orkerPath, address, namespace, queue)
	engineAddr, engineExited := startEngine(ctx, t, enginePath, address, namespace, queue)
	server := "http://" + engineAddr
	var transcript strings.Builder
	cli := func(args ...string) (string, string, int) {
		return runBinary(ctx, t, orkctlPath, root, append(args, "--server", server)...)
	}
	waitForEngine(t, engineExited, cli)
	waitForPoolLive(t, cli, "default", pool)
	sessions := filepath.Join(bindableOrkerWorkRoot(t), "task-run-sessions")
	if err := os.MkdirAll(sessions, 0o755); err != nil {
		t.Fatal(err)
	}

	name := fmt.Sprintf("task-run-local-%d-%d", os.Getpid(), time.Now().UnixNano())
	marker := fmt.Sprintf("marker-%d", time.Now().UnixNano())
	doc := filepath.Join(t.TempDir(), "fixture.ork.yaml")
	if err := os.WriteFile(doc, []byte(taskRunFixtureDoc(name, marker, true)), 0o644); err != nil {
		t.Fatal(err)
	}
	if stdout, stderr, code := cli("apply", doc); code != 0 {
		t.Fatalf("apply exit = %d\n%s\n%s", code, stdout, stderr)
	}
	stdout, stderr, code := cli("run", "submit", name, "--input", "target=staging", "--input", "expect=prod")
	if code != 0 {
		t.Fatalf("submit exit = %d\n%s\n%s", code, stdout, stderr)
	}
	runID := parseRunID(t, stdout)
	httpClient := h2cClient(t)
	runs := orkstackv1connect.NewRunServiceClient(httpClient, server)
	replica := &engineReplica{name: "ork-local", addr: engineAddr, runs: runs}
	run := awaitTerminalOnB(ctx, t, replica, runID)
	produce := taskRunNamed(run, "produce")
	if !strings.HasPrefix(produce.GetValuesRef().GetUri(), "file://") {
		t.Fatalf("the local backend recorded %s, want a file:// URI", produce.GetValuesRef().GetUri())
	}
	shared := sharedLocalArtifactDir(t)
	// "Before" is taken once the source run's archive record is readable, so
	// the engine's own terminal writes into the shared directory are done.
	archive, err := artifacts.NewRunArchive(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: shared})
	if err != nil {
		t.Fatalf("open the local run archive: %v", err)
	}
	archived := time.Now().Add(2 * time.Minute)
	for {
		if _, err := archive.GetRun(ctx, runID); err == nil {
			break
		} else if time.Now().After(archived) {
			t.Fatalf("the source run's archive record never became readable: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	before := treeListing(t, shared)

	stdout, stderr, code = runTaskRunBinary(ctx, t, orkctlPath, root, sessions, server, &transcript,
		"--from-run", runID, "--task", "consume", "--keep")
	if code != exitRunFailed {
		t.Fatalf("local-backend replay exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitRunFailed, stdout, stderr)
	}
	session := keptSession(t, stdout)
	production := productionStdout(t, runs, ctx, runID, "consume")
	keptConfig, err := os.ReadFile(filepath.Join(session, "input", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(keptConfig) != between(t, production, "CONFIG") {
		t.Fatalf("config.json differs from the production attempt's\nlocal:\n%s\nproduction:\n%s", keptConfig, between(t, production, "CONFIG"))
	}
	if after := treeListing(t, shared); strings.Join(after, "\n") != strings.Join(before, "\n") {
		t.Fatalf("the shared artifact directory changed: %d files before, %d after\nbefore:\n%s\nafter:\n%s",
			len(before), len(after), strings.Join(before, "\n"), strings.Join(after, "\n"))
	}
	report := fmt.Sprintf("local backend run %s: values %s\nconfig.json byte-identical: true (%d bytes)\nshared artifact directory: %d files before, identical after\n",
		runID, produce.GetValuesRef().GetUri(), len(keptConfig), len(before))
	t.Logf("EVIDENCE\n%s", report)
	proof.Write(t, "task-run", "local-backend.txt", []byte(report+"\n"+transcript.String()))
}

// localContainerCLI is the container CLI `orkctl task run` drives under this
// environment, for a test that asks the runtime what it left behind:
// ORK_DOCKER_BIN, then podman for ORK_DOCKER_CLI=podman, then docker.
func localContainerCLI() string {
	if bin := strings.TrimSpace(os.Getenv(orker.DockerBinVar)); bin != "" {
		return bin
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv(orker.CLIVar)), orker.CLIPodman) {
		return orker.CLIPodman
	}
	return orker.DefaultDockerBin
}
