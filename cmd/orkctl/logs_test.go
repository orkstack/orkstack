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
	// grow, when set, appends one more chunk after every read this store
	// serves: the task never goes quiet, exactly as a chatty running task
	// does not.
	grow *artifacts.LogRef
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
	_ context.Context, ref artifacts.LogRef, afterSeq int64, limit int, maxBytes int64,
) ([]artifacts.LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []artifacts.LogChunk
	read := int64(0)
	for _, chunk := range s.chunks {
		if chunk.LogRef != ref || chunk.Seq <= afterSeq {
			continue
		}
		out = append(out, chunk)
		read += int64(len(chunk.Data))
		// Both bounds, as the contract states them: the first chunk is always
		// returned whole, and a page stops at whichever bound is met first
		// (pkg/artifacts/logs.go).
		if limit > 0 && len(out) == limit {
			break
		}
		if maxBytes > 0 && read >= maxBytes {
			break
		}
	}
	if s.grow != nil {
		s.appendLocked(*s.grow)
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
	s.writeAttempt(runID, taskID, 1, seq, body)
}

// writeAttempt stores one chunk of a named TRY. A retry writes under its own
// attempt with its own sequence starting again at 1, which is the shape a
// resume across a retry has to cope with.
func (s *cliLogStore) writeAttempt(runID, taskID string, attempt int32, seq int64, body string) {
	if err := s.PutLogChunk(context.Background(), artifacts.LogChunk{
		LogRef: artifacts.LogRef{RunID: runID, TaskID: taskID, Attempt: attempt},
		Seq:    seq, Data: []byte(body),
	}); err != nil {
		panic(err)
	}
}

// keepWriting makes the named task chatty for as long as anyone reads it.
func (s *cliLogStore) keepWriting(runID, taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grow = &artifacts.LogRef{RunID: runID, TaskID: taskID, Attempt: 1}
}

func (s *cliLogStore) appendLocked(ref artifacts.LogRef) {
	next := int64(0)
	for _, chunk := range s.chunks {
		if chunk.LogRef == ref && chunk.Seq > next {
			next = chunk.Seq
		}
	}
	next++
	s.chunks = append(s.chunks, artifacts.LogChunk{
		LogRef: ref, Seq: next, Data: []byte(fmt.Sprintf("still writing %d\n", next)),
	})
}

// seedLoggedRun puts a run with the named tasks into the read model.
func seedLoggedRun(t *testing.T, f *fixture, runID string, status orkstackv1.Status, taskIDs ...string) {
	t.Helper()
	taskRuns := make([]*orkstackv1.TaskRun, 0, len(taskIDs))
	for _, id := range taskIDs {
		taskRuns = append(taskRuns, &orkstackv1.TaskRun{
			TaskId: id, Status: status, Attempt: 1,
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

// Without -f the command is a REPLAY: it prints what the store holds and
// returns, even though the run is still going. The store here never goes
// quiet — it appends another chunk after every read it serves, as a chatty
// task does — so a replay that waited for silence would never return, and the
// deadline below says exactly that instead of hanging the suite.
func TestRunLogs_WithoutFollowReturnsWhileTheRunIsStillRunning(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-8", orkstackv1.Status_STATUS_RUNNING, "build")
	logs.write("run-logs-8", "build", 1, "stored\n")
	logs.keepWriting("run-logs-8", "build")

	out := &syncBuffer{}
	done := make(chan result, 1)
	go func() { done <- f.runTo(out, "run", "logs", "run-logs-8", "--task", "build") }()

	select {
	case outcome := <-done:
		if outcome.code != 0 {
			t.Fatalf("exit code = %d, want 0\nstderr: %s", outcome.code, outcome.stderr)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("`orkctl run logs` without -f never returned against a task that keeps "+
			"writing; a replay became a follow. It printed %s", preview(out.String()))
	}
	if got := out.String(); got != "stored\n" {
		t.Fatalf("the replay printed %s, want the one chunk stored when it ran", preview(got))
	}
}

// preview trims a follower's output for a failure message. Against a task
// that never stops writing, printing the whole buffer buries the failure in
// megabytes of the log it is complaining about.
func preview(text string) string {
	const limit = 200
	if len(text) <= limit {
		return fmt.Sprintf("%q", text)
	}
	return fmt.Sprintf("%q… (%d bytes in total)", text[:limit], len(text))
}

// Resume is the CLI's half of the same contract: --from-seq skips what the
// caller already has, and it names the attempt it counted — a cursor is half
// of one without it.
func TestRunLogs_FromSeqResumes(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-4", orkstackv1.Status_STATUS_SUCCEEDED, "build")
	for seq := int64(1); seq <= 4; seq++ {
		logs.write("run-logs-4", "build", seq, fmt.Sprintf("line %d\n", seq))
	}

	out := f.run("run", "logs", "run-logs-4", "--task", "build",
		"--from-seq", "3", "--from-attempt", "1").requireExit(t, 0)
	if out.stdout != "line 4\n" {
		t.Fatalf("stdout = %q, want only the chunk after seq 3", out.stdout)
	}
}

// THE RESUME THAT CROSSES A RETRY. A chunk sequence is dense per ATTEMPT and
// starts again at 1, so a cursor of 5 taken from try 1 means nothing to try 2.
// The engine drops a foreign cursor and delivers the resolved attempt from its
// first chunk — but only when it is told which attempt the cursor counted
// (internal/api/logs.go, StreamTaskLogsRequest.from_attempt).
//
// A CLI that sent the number alone would have try 1's cursor applied to try 2
// and would print nothing here, while exiting 0 as if the log had been read:
// a retry is usually shorter than the try that failed, which is what makes
// this loss silent.
func TestRunLogs_ResumeAcrossARetryDeliversTheNewAttemptFromItsFirstChunk(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-9", orkstackv1.Status_STATUS_SUCCEEDED, "flaky")
	// Try 1 talked for five chunks and died; try 2 wrote two and succeeded.
	for seq := int64(1); seq <= 5; seq++ {
		logs.writeAttempt("run-logs-9", "flaky", 1, seq, fmt.Sprintf("try one line %d\n", seq))
	}
	for seq := int64(1); seq <= 2; seq++ {
		logs.writeAttempt("run-logs-9", "flaky", 2, seq, fmt.Sprintf("try two line %d\n", seq))
	}

	out := f.run("run", "logs", "run-logs-9", "--task", "flaky",
		"--from-seq", "5", "--from-attempt", "1").requireExit(t, 0)
	want := "try two line 1\ntry two line 2\n"
	if out.stdout != want {
		t.Fatalf("stdout = %q, want %q: the cursor belongs to attempt 1, so attempt 2 is "+
			"delivered from its first chunk", out.stdout, want)
	}
}

// A cursor with no attempt is REFUSED, not guessed. Applying it to whichever
// try is latest is the silent loss above; the CLI cannot recover the missing
// half, so it says so and exits non-zero instead of printing a short log that
// looks complete.
func TestRunLogs_FromSeqWithoutAnAttemptIsRefused(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-10", orkstackv1.Status_STATUS_SUCCEEDED, "build")
	logs.write("run-logs-10", "build", 1, "one\n")

	out := f.run("run", "logs", "run-logs-10", "--task", "build", "--from-seq", "1").
		requireExit(t, 1)
	for _, want := range []string{"--from-attempt", "--attempt"} {
		if !strings.Contains(out.stderr, want) {
			t.Errorf("stderr = %q, want it to name %s", out.stderr, want)
		}
	}
	if out.stdout != "" {
		t.Fatalf("stdout = %q, want nothing printed for a cursor that cannot be honoured",
			out.stdout)
	}
}

// A pinned --attempt is itself the cursor's other half: one try's transcript
// resumed inside that try needs no second flag.
func TestRunLogs_APinnedAttemptSuppliesTheCursorsAttempt(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-11", orkstackv1.Status_STATUS_SUCCEEDED, "flaky")
	for seq := int64(1); seq <= 3; seq++ {
		logs.writeAttempt("run-logs-11", "flaky", 1, seq, fmt.Sprintf("try one line %d\n", seq))
	}
	logs.writeAttempt("run-logs-11", "flaky", 2, 1, "try two line 1\n")

	out := f.run("run", "logs", "run-logs-11", "--task", "flaky",
		"--attempt", "1", "--from-seq", "2").requireExit(t, 0)
	if out.stdout != "try one line 3\n" {
		t.Fatalf("stdout = %q, want the rest of attempt 1", out.stdout)
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

// A run SubmitRun has just accepted has no task runs yet: they are filled in
// by the run's first workflow step, a moment later. A follower that attaches
// in that window is not asking about a typo, so it must wait for the task to
// appear rather than exit — the window is exactly when a CLI user who
// submitted and then followed would land.
func TestRunLogs_FollowWaitsForAJustAcceptedRunsTasksToAppear(t *testing.T) {
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-new", orkstackv1.Status_STATUS_PENDING)

	out := &syncBuffer{}
	done := make(chan result, 1)
	go func() { done <- f.runTo(out, "run", "logs", "run-logs-new", "--task", "build", "-f") }()

	time.Sleep(quietGap)
	select {
	case outcome := <-done:
		t.Fatalf("the follower gave up on a PENDING run with no tasks yet: %+v", outcome)
	default:
	}

	seedLoggedRun(t, f, "run-logs-new", orkstackv1.Status_STATUS_RUNNING, "build")
	logs.write("run-logs-new", "build", 1, "hello\n")
	awaitOutput(t, out, "hello")

	seedLoggedRun(t, f, "run-logs-new", orkstackv1.Status_STATUS_SUCCEEDED, "build")
	select {
	case outcome := <-done:
		if outcome.code != 0 {
			t.Fatalf("exit code = %d, want 0\nstderr: %s", outcome.code, outcome.stderr)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("the follower never exited after the run finished; it printed %q", out.String())
	}
}

// Once a run has its tasks, a name that is not among them IS a typo, and a
// follower says so at once rather than waiting for a task that cannot come.
func TestRunLogs_FollowRejectsAnUnknownTaskOnceTheRunHasTasks(t *testing.T) {
	f, _ := newLogFixture(t)
	seedLoggedRun(t, f, "run-logs-typo", orkstackv1.Status_STATUS_RUNNING, "build")

	out := f.run("run", "logs", "run-logs-typo", "--task", "biuld", "-f").requireExit(t, 1)
	if !strings.Contains(out.stderr, `no task "biuld"`) {
		t.Fatalf("stderr = %q, want the unknown-task error", out.stderr)
	}
}
