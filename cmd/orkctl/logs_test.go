package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// newLogFixture is the CLI fixture with a task log store attached, and a poll
// interval short enough that a test does not spend seconds waiting for the
// engine's default backoff.
func newLogFixture(t *testing.T) (*fixture, *cliLogStore) {
	t.Helper()
	logs := &cliLogStore{}
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Logs = logs
		cfg.LogPoll = api.LogPollOptions{Min: 5 * time.Millisecond, Max: 25 * time.Millisecond}
	})
	return f, logs
}

// quietGap is a silence long enough to outlast many of the engine's poll
// cycles (5–25 ms below). Every "it is still following" claim in this file is
// measured across one, because without it the test races the engine and a
// follower that quietly stopped passes whenever the next write happens to
// land inside a single poll interval.
const quietGap = 300 * time.Millisecond

// cliLogStore is the shared bucket as the CLI's engine sees it.
type cliLogStore struct {
	mu     sync.Mutex
	chunks []artifacts.LogChunk
}

func (s *cliLogStore) PutLogChunk(_ context.Context, chunk artifacts.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := chunk
	stored.Data = append([]byte(nil), chunk.Data...)
	s.chunks = append(s.chunks, stored)
	return nil
}

func (s *cliLogStore) ReadLogChunks(
	_ context.Context, ref artifacts.LogRef, afterSeq int64, limit int,
) ([]artifacts.LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []artifacts.LogChunk
	for _, chunk := range s.chunks {
		if chunk.LogRef != ref || chunk.Seq <= afterSeq {
			continue
		}
		out = append(out, chunk)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *cliLogStore) LatestLogAttempt(_ context.Context, runID, taskID string) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	latest := int32(0)
	for _, chunk := range s.chunks {
		if chunk.RunID == runID && chunk.TaskID == taskID && chunk.Attempt > latest {
			latest = chunk.Attempt
		}
	}
	if latest == 0 {
		return 0, artifacts.ErrNotFound
	}
	return latest, nil
}

func (s *cliLogStore) write(runID, taskID string, seq int64, body string) {
	if err := s.PutLogChunk(context.Background(), artifacts.LogChunk{
		LogRef: artifacts.LogRef{RunID: runID, TaskID: taskID, Attempt: 1},
		Seq:    seq, Data: []byte(body),
	}); err != nil {
		panic(err)
	}
}

// seedLoggedRun puts a run with the named tasks into the read model.
func seedLoggedRun(t *testing.T, f *fixture, runID string, status orkstackv1.Status, taskIDs ...string) {
	t.Helper()
	taskRuns := make([]*orkstackv1.TaskRun, 0, len(taskIDs))
	for _, id := range taskIDs {
		taskRuns = append(taskRuns, &orkstackv1.TaskRun{
			TaskId: id, Plugin: "shell", Status: status, Attempt: 1,
		})
	}
	f.runs.Apply(&orkstackv1.Run{
		Id:             runID,
		OrkflowName:    "chatty",
		OrkflowVersion: "0.1.0",
		Status:         status,
		TaskRuns:       taskRuns,
	})
}

func TestRunLogs_PrintsAFinishedTasksOutput(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-1", orkstackv1.Status_STATUS_SUCCEEDED, "build")
	logs.write("run-logs-1", "build", 1, "compiling\n")
	logs.write("run-logs-1", "build", 2, "done\n")

	out := f.run("run", "logs", "run-logs-1", "--task", "build").requireExit(t, 0)
	if out.stdout != "compiling\ndone\n" {
		t.Fatalf("stdout = %q, want the task's output verbatim", out.stdout)
	}
}

// Without --task the whole run is followed, and each line says which task
// wrote it — otherwise a two-task run reads as one shuffled log.
func TestRunLogs_WithoutATaskPrintsEveryTaskPrefixed(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-2", orkstackv1.Status_STATUS_SUCCEEDED, "build", "test")
	logs.write("run-logs-2", "build", 1, "building\n")
	logs.write("run-logs-2", "test", 1, "testing\n")

	out := f.run("run", "logs", "run-logs-2").requireExit(t, 0)
	for _, want := range []string{"[build] building", "[test] testing"} {
		if !strings.Contains(out.stdout, want) {
			t.Errorf("stdout = %q, want it to carry %q", out.stdout, want)
		}
	}
}

// The follow claim: -f keeps printing while the run is live and returns when
// it reaches a terminal state. The chunk written after the command started is
// the one that proves the following.
func TestRunLogs_FollowPrintsWhileTheRunIsLiveAndExitsOnTheTerminalState(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-3", orkstackv1.Status_STATUS_RUNNING, "build")
	logs.write("run-logs-3", "build", 1, "first\n")

	out := &syncBuffer{}
	done := make(chan result, 1)
	go func() { done <- f.runTo(out, "run", "logs", "run-logs-3", "--task", "build", "-f") }()

	awaitOutput(t, out, "first")

	// A QUIET GAP, longer than many of the engine's poll cycles. It is what
	// makes the claim below a measurement rather than a race: a follower that
	// ended at the first sweep finding nothing has already exited, and the
	// line written after it could never be printed.
	time.Sleep(quietGap)
	select {
	case outcome := <-done:
		t.Fatalf("the command exited after %s while the run was still RUNNING: %+v",
			quietGap, outcome)
	default:
	}

	// Written long after the follower attached: this is the live half.
	logs.write("run-logs-3", "build", 2, "second\n")
	awaitOutput(t, out, "second")

	time.Sleep(quietGap)

	// The run finishes: the command must return on its own.
	seedLoggedRun(t, f, "run-logs-3", orkstackv1.Status_STATUS_SUCCEEDED, "build")
	select {
	case outcome := <-done:
		if outcome.code != 0 {
			t.Fatalf("exit code = %d, want 0\nstderr: %s", outcome.code, outcome.stderr)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("the follower never exited after the run finished; it printed %q", out.String())
	}
	if got := out.String(); !strings.Contains(got, "first") || !strings.Contains(got, "second") {
		t.Fatalf("the followed log reads %q, want both lines", got)
	}
}

// Resume is the CLI's half of the same contract: --from-seq skips what the
// caller already has.
func TestRunLogs_FromSeqResumes(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-4", orkstackv1.Status_STATUS_SUCCEEDED, "build")
	for seq := int64(1); seq <= 4; seq++ {
		logs.write("run-logs-4", "build", seq, fmt.Sprintf("line %d\n", seq))
	}

	out := f.run("run", "logs", "run-logs-4", "--task", "build", "--from-seq", "3").
		requireExit(t, 0)
	if out.stdout != "line 4\n" {
		t.Fatalf("stdout = %q, want only the chunk after seq 3", out.stdout)
	}
}

// Truncation must reach the terminal: a cut log that printed as if complete is
// the failure this flag exists to prevent.
func TestRunLogs_ReportsTruncation(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-5", orkstackv1.Status_STATUS_SUCCEEDED, "chatty")
	logs.write("run-logs-5", "chatty", 1, "kept\n")
	if err := logs.PutLogChunk(context.Background(), artifacts.LogChunk{
		LogRef:    artifacts.LogRef{RunID: "run-logs-5", TaskID: "chatty", Attempt: 1},
		Seq:       2,
		Data:      []byte("[ork] log truncated: this task attempt passed its limit\n"),
		Truncated: true,
	}); err != nil {
		t.Fatalf("PutLogChunk: %v", err)
	}

	out := f.run("run", "logs", "run-logs-5", "--task", "chatty").requireExit(t, 0)
	if !strings.Contains(out.stdout, "truncated") {
		t.Fatalf("stdout = %q, want the truncation notice", out.stdout)
	}
}

// --json is one object per chunk, one per line, so a follower can be piped
// into a processor without waiting for the stream to end.
func TestRunLogs_JSONIsOneChunkPerLine(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-6", orkstackv1.Status_STATUS_SUCCEEDED, "build")
	logs.write("run-logs-6", "build", 1, "one\n")
	logs.write("run-logs-6", "build", 2, "two\n")

	out := f.run("run", "logs", "run-logs-6", "--task", "build", "--json").requireExit(t, 0)
	lines := strings.Split(strings.TrimSpace(out.stdout), "\n")
	if len(lines) < 2 {
		t.Fatalf("stdout = %q, want one JSON object per chunk", out.stdout)
	}
	for i, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("line %d (%q) is not JSON: %v", i, line, err)
		}
		if _, ok := decoded["seq"]; !ok && i < len(lines)-1 {
			t.Errorf("line %d (%q) carries no seq", i, line)
		}
	}
}

func TestRunLogs_UnknownRunFails(t *testing.T) {
	f, _ := newLogFixture(t)
	out := f.run("run", "logs", "nope").requireExit(t, 1)
	if !strings.Contains(out.stderr, "not found") {
		t.Fatalf("stderr = %q, want a not-found error", out.stderr)
	}
}

func TestRunLogs_UnknownTaskNamesTheOnesThatExist(t *testing.T) {
	f, _ := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-7", orkstackv1.Status_STATUS_SUCCEEDED, "build", "test")

	out := f.run("run", "logs", "run-logs-7", "--task", "deploy").requireExit(t, 1)
	if !strings.Contains(out.stderr, "build") || !strings.Contains(out.stderr, "test") {
		t.Fatalf("stderr = %q, want it to name the tasks the run has", out.stderr)
	}
}

// ---- helpers ----------------------------------------------------------------

// syncBuffer is a bytes.Buffer a test can read while a command writes it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
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

var _ io.Writer = (*syncBuffer)(nil)

func awaitOutput(t *testing.T, out *syncBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%q never appeared in the followed output; it holds %q", want, out.String())
}
