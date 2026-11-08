package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// fastResumes shrinks the resume waits, so a test never waits on a clock and
// still walks the doubling and its cap.
func fastResumes(t *testing.T) {
	t.Helper()
	delay, maxDelay := watchResumeDelay, watchResumeMaxDelay
	watchResumeDelay, watchResumeMaxDelay = time.Nanosecond, 4*time.Nanosecond
	t.Cleanup(func() { watchResumeDelay, watchResumeMaxDelay = delay, maxDelay })
}

// logEnvelope is a TaskLogChunk as one Connect streaming envelope.
func logEnvelope(t *testing.T, chunk *orkstackv1.TaskLogChunk) []byte {
	t.Helper()
	body, err := proto.Marshal(chunk)
	if err != nil {
		t.Fatal(err)
	}
	return framed(0, body)
}

// logChunks is attempt's chunks from..to as the store's own text, enveloped.
func logChunks(t *testing.T, run, task string, attempt int32, from, to int64) []byte {
	t.Helper()
	var wire []byte
	for seq := from; seq <= to; seq++ {
		wire = append(wire, logEnvelope(t, &orkstackv1.TaskLogChunk{RunId: run, TaskId: task, Attempt: attempt, Seq: seq,
			Data: fmt.Appendf(nil, "try %d line %d\n", attempt, seq)})...)
	}
	return wire
}

// endStream is a Connect end message carrying err.
func endStream(code, message string) []byte {
	return framed(2, fmt.Appendf(nil, `{"error":{"code":%q,"message":%q}}`, code, message))
}

func framed(flags byte, body []byte) []byte {
	prefix := []byte{flags, byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}
	return append(prefix, body...)
}

// firstStreamsAnswered answers the first n StreamTaskLogs calls with answer and
// leaves every later one to the real service. It returns the count of calls.
func firstStreamsAnswered(f *fixture, n int64, answer func(w http.ResponseWriter, r *http.Request)) *atomic.Int64 {
	calls := &atomic.Int64{}
	f.interceptWith(func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/StreamTaskLogs") {
			return false
		}
		if calls.Add(1) > n {
			return false
		}
		answer(w, r)
		return true
	})
	return calls
}

// finOn writes wire to the stream and, once its last whole chunk has printed,
// ends the connection with a FIN under it, what a replica that dies leaves a
// client. The wait matters: on an end that races the response's headers, Go's
// HTTP/2 client drops what it had read.
func finOn(f *fixture, wire []byte, printed <-chan struct{}) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		rawAnswer(w, r, wire)
		<-printed
		f.listener.closeWrite()
		<-r.Context().Done()
	}
}

// followPrinting runs `run logs` with signal closing once line has printed.
func followPrinting(f *fixture, signal map[string]chan struct{}, args ...string) result {
	f.t.Helper()
	out := &liveWriter{triggers: map[string]func(){}}
	for line, printed := range signal {
		out.triggers[line] = func() { close(printed) }
	}
	outcome := f.runTo(out, args...)
	outcome.stdout = out.buf.String()
	return outcome
}

func seedTries(f *fixture, logs *cliLogStore, run, task string, chunksPerTry ...int64) {
	seedLoggedRun(f.t, f, run, orkstackv1.Status_STATUS_SUCCEEDED, task)
	for i, count := range chunksPerTry {
		attempt := int32(i + 1)
		for seq := int64(1); seq <= count; seq++ {
			logs.writeAttempt(run, task, attempt, seq, fmt.Sprintf("try %d line %d\n", attempt, seq))
		}
	}
}

func triesText(chunksPerTry ...int64) string {
	var b strings.Builder
	for i, count := range chunksPerTry {
		for seq := int64(1); seq <= count; seq++ {
			fmt.Fprintf(&b, "try %d line %d\n", i+1, seq)
		}
	}
	return b.String()
}

func logResumeLines(stderr string) []string {
	var lines []string
	for _, line := range strings.Split(stderr, "\n") {
		if strings.HasPrefix(line, "read task logs: the stream of task ") {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestRunLogs_FollowResumesAfterTheLastChunk: wherever a connection or a
// stopping replica cuts the stream, each line prints exactly once, in order.
func TestRunLogs_FollowResumesAfterTheLastChunk(t *testing.T) {
	const run, task = "run-logs-cut", "build"
	for _, tc := range []struct {
		name string
		said string
		cut  func(t *testing.T, f *fixture, printed <-chan struct{}) func(http.ResponseWriter, *http.Request)
	}{
		{"a FIN between chunks", "internal: protocol error: incomplete envelope: unexpected EOF", func(t *testing.T, f *fixture, printed <-chan struct{}) func(http.ResponseWriter, *http.Request) {
			return finOn(f, logChunks(t, run, task, 1, 1, 2), printed)
		}},
		{"a FIN inside a chunk", "unknown: read enveloped message: unexpected EOF", func(t *testing.T, f *fixture, printed <-chan struct{}) func(http.ResponseWriter, *http.Request) {
			return finOn(f, append(logChunks(t, run, task, 1, 1, 2), 0, 0, 0, 0, 100, 1, 2, 3), printed)
		}},
		{"a stopping replica's answer", "unavailable: this engine replica is shutting down", func(t *testing.T, f *fixture, _ <-chan struct{}) func(http.ResponseWriter, *http.Request) {
			wire := append(logChunks(t, run, task, 1, 1, 2), endStream("unavailable", "this engine replica is shutting down")...)
			return func(w http.ResponseWriter, r *http.Request) { rawAnswer(w, r, wire) }
		}},
		{"a clean end before `last`", "the stream ended before the task's log did", func(t *testing.T, f *fixture, _ <-chan struct{}) func(http.ResponseWriter, *http.Request) {
			wire := append(logChunks(t, run, task, 1, 1, 2), framed(2, []byte("{}"))...)
			return func(w http.ResponseWriter, r *http.Request) { rawAnswer(w, r, wire) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fastResumes(t)
			f, logs := newLogFixture(t)
			seedTries(f, logs, run, task, 5)
			printed := make(chan struct{})
			calls := firstStreamsAnswered(f, 1, tc.cut(t, f, printed))

			out := followPrinting(f, map[string]chan struct{}{"try 1 line 2": printed}, "run", "logs", run, "--task", task, "-f").requireExit(t, exitOK)
			if want := triesText(5); out.stdout != want {
				t.Fatalf("stdout = %q, want %q: nothing repeated, nothing skipped", out.stdout, want)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("streams opened = %d, want 2", got)
			}
			lines := logResumeLines(out.stderr)
			if len(lines) != 1 || !strings.Contains(lines[0], "("+tc.said+")") ||
				!strings.Contains(lines[0], "resuming after chunk 2 of attempt 1") {
				t.Fatalf("resume lines = %v, want one saying %q after chunk 2 of attempt 1", lines, tc.said)
			}
		})
	}
}

// TestRunLogs_FollowResumeKeepsEveryAttempt: the engine drops a cursor of
// another attempt, so a retry during the outage would lose the unread tail of
// the try being read and all of any try between. Each prints whole, in order.
func TestRunLogs_FollowResumeKeepsEveryAttempt(t *testing.T) {
	const run, task = "run-logs-retried", "flaky"
	for _, tc := range []struct {
		name  string
		tries []int64
	}{
		{"one retry", []int64{5, 2}},
		{"two retries", []int64{5, 3, 2}},
		{"a retry that wrote nothing", []int64{5, 0, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fastResumes(t)
			f, logs := newLogFixture(t)
			seedTries(f, logs, run, task, tc.tries...)
			printed := make(chan struct{})
			firstStreamsAnswered(f, 1, finOn(f, logChunks(t, run, task, 1, 1, 2), printed))

			out := followPrinting(f, map[string]chan struct{}{"try 1 line 2": printed}, "run", "logs", run, "--task", task, "-f").requireExit(t, exitOK)
			if want := triesText(tc.tries...); out.stdout != want {
				t.Fatalf("stdout = %q, want %q", out.stdout, want)
			}
		})
	}
}

// TestRunLogs_FollowResumeSurvivesACutDuringTheCatchUp: the read of the old
// attempt's tail is cut in its turn, and still nothing repeats or goes missing.
func TestRunLogs_FollowResumeSurvivesACutDuringTheCatchUp(t *testing.T) {
	fastResumes(t)
	const run, task = "run-logs-twice", "flaky"
	f, logs := newLogFixture(t)
	seedTries(f, logs, run, task, 5, 2)
	first, second := make(chan struct{}), make(chan struct{})
	var cuts atomic.Int64
	f.interceptWith(func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/StreamTaskLogs") {
			return false
		}
		switch cuts.Add(1) {
		case 1: // the follow, cut after two chunks of try 1
			finOn(f, logChunks(t, run, task, 1, 1, 2), first)(w, r)
			return true
		case 3: // the catch-up of try 1, cut after one more
			finOn(f, logChunks(t, run, task, 1, 3, 3), second)(w, r)
			return true
		}
		return false
	})
	out := followPrinting(f, map[string]chan struct{}{"try 1 line 2": first, "try 1 line 3": second},
		"run", "logs", run, "--task", task, "-f").requireExit(t, exitOK)
	if want := triesText(5, 2); out.stdout != want {
		t.Fatalf("stdout = %q, want %q", out.stdout, want)
	}
}

// TestRunLogs_FollowResumeIsBounded: resumes in a row that deliver nothing are
// bounded as a watch's are, and a chunk resets the count.
func TestRunLogs_FollowResumeIsBounded(t *testing.T) {
	const run, task = "run-logs-bound", "build"
	t.Run("nothing delivered", func(t *testing.T) {
		fastResumes(t)
		f, logs := newLogFixture(t)
		seedTries(f, logs, run, task, 2)
		calls := firstStreamsAnswered(f, 1000, func(w http.ResponseWriter, r *http.Request) {
			rawAnswer(w, r, endStream("unavailable", "no replica"))
		})
		out := f.run("run", "logs", run, "--task", task, "-f").requireExit(t, exitError)
		if got := calls.Load(); got != 1+watchResumeLimit {
			t.Fatalf("streams opened = %d, want %d", got, 1+watchResumeLimit)
		}
		if lines := logResumeLines(out.stderr); len(lines) != watchResumeLimit {
			t.Fatalf("resume lines = %v, want %d", lines, watchResumeLimit)
		}
		if want := "Error: read task logs: unavailable: no replica"; !strings.Contains(out.stderr, want) {
			t.Fatalf("stderr = %q, want %q", out.stderr, want)
		}
	})
	t.Run("a chunk resets the count", func(t *testing.T) {
		fastResumes(t)
		const total = 2 * watchResumeLimit
		f, logs := newLogFixture(t)
		seedTries(f, logs, run, task, total+1)
		var seq atomic.Int64
		firstStreamsAnswered(f, total, func(w http.ResponseWriter, r *http.Request) {
			at := seq.Add(1)
			rawAnswer(w, r, append(logChunks(t, run, task, 1, at, at), endStream("unavailable", "again")...))
		})
		out := f.run("run", "logs", run, "--task", task, "-f").requireExit(t, exitOK)
		if want := triesText(total + 1); out.stdout != want {
			t.Fatalf("stdout = %q, want %q", out.stdout, want)
		}
	})
}

// TestRunLogs_FollowDoesNotResumeAnEngineError: an error the engine reports,
// whatever its code, ends the follow, and so does a stream ended or reset on a
// live connection by the engine itself, partway through a chunk.
func TestRunLogs_FollowDoesNotResumeAnEngineError(t *testing.T) {
	const run, task = "run-logs-refused", "build"
	for _, tc := range []struct {
		name string
		wire func(t *testing.T) []byte
		said string
	}{
		{"internal", func(t *testing.T) []byte {
			return append(logChunks(t, run, task, 1, 1, 1), endStream("internal", "no")...)
		}, "Error: read task logs: internal: no"},
		{"not_found", func(t *testing.T) []byte { return endStream("not_found", "no") }, "Error: read task logs: not_found: no"},
		{"a cut prefix on a live connection", func(t *testing.T) []byte {
			return append(logChunks(t, run, task, 1, 1, 1), 0, 0)
		}, "Error: read task logs: internal: protocol error: incomplete envelope: unexpected EOF"},
		{"a cut chunk on a live connection", func(t *testing.T) []byte {
			return append(logChunks(t, run, task, 1, 1, 1), 0, 0, 0, 0, 100, 1, 2, 3)
		}, "Error: read task logs: internal: protocol error: promised 100 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fastResumes(t)
			f, logs := newLogFixture(t)
			seedTries(f, logs, run, task, 3)
			calls := firstStreamsAnswered(f, 1, func(w http.ResponseWriter, r *http.Request) { rawAnswer(w, r, tc.wire(t)) })
			out := f.run("run", "logs", run, "--task", task, "-f").requireExit(t, exitError)
			if !strings.Contains(out.stderr, tc.said) {
				t.Fatalf("stderr = %q, want %q", out.stderr, tc.said)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("streams opened = %d, want 1", got)
			}
			if lines := logResumeLines(out.stderr); len(lines) != 0 {
				t.Fatalf("resume lines = %v, want none", lines)
			}
		})
	}
}

// Without -f a read is one read: its failure ends it, as it always has.
func TestRunLogs_AReplayIsNotResumed(t *testing.T) {
	fastResumes(t)
	const run, task = "run-logs-replay", "build"
	f, logs := newLogFixture(t)
	seedTries(f, logs, run, task, 3)
	calls := firstStreamsAnswered(f, 1, func(w http.ResponseWriter, r *http.Request) {
		rawAnswer(w, r, append(logChunks(t, run, task, 1, 1, 1), endStream("unavailable", "no replica")...))
	})
	out := f.run("run", "logs", run, "--task", task).requireExit(t, exitError)
	if got := calls.Load(); got != 1 {
		t.Fatalf("streams opened = %d, want 1", got)
	}
	if !strings.Contains(out.stderr, "read task logs: unavailable: no replica") {
		t.Fatalf("stderr = %q", out.stderr)
	}
}

// --json prints one line per chunk with one `last`, whatever the cuts.
func TestRunLogs_FollowResumeKeepsTheJSONLines(t *testing.T) {
	fastResumes(t)
	const run, task = "run-logs-json", "flaky"
	f, logs := newLogFixture(t)
	seedTries(f, logs, run, task, 3, 1)
	printed := make(chan struct{})
	firstStreamsAnswered(f, 1, finOn(f, logChunks(t, run, task, 1, 1, 1), printed))
	out := followPrinting(f, map[string]chan struct{}{`"seq":"1"`: printed}, "run", "logs", run, "--task", task, "-f", "--json").requireExit(t, exitOK)
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(out.stdout), "\n") {
		var chunk struct {
			Attempt int    `json:"attempt"`
			Seq     string `json:"seq"`
			Last    bool   `json:"last"`
		}
		if err := json.Unmarshal([]byte(line), &chunk); err != nil {
			t.Fatalf("line %q: %v", line, err)
		}
		got = append(got, fmt.Sprintf("%d/%s/%v", chunk.Attempt, chunk.Seq, chunk.Last))
	}
	want := []string{"1/1/false", "1/2/false", "1/3/false", "2/1/false", "2/1/true"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("chunks = %v, want %v", got, want)
	}
}

// A whole-run follow rides out its own discovery read as well as its streams.
func TestRunLogs_WholeRunFollowRidesOutARunRead(t *testing.T) {
	fastResumes(t)
	const run = "run-logs-whole"
	f, logs := newLogFixture(t)
	seedLoggedRun(t, f, run, orkstackv1.Status_STATUS_RUNNING, "build")
	logs.write(run, "build", 1, "line 1\n")
	var reads atomic.Int64
	f.interceptWith(func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/GetRun") {
			return false
		}
		switch reads.Add(1) {
		case 1:
			return false
		case 2:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write(fmt.Appendf(nil, `{"code":%q,"message":"no replica"}`, connect.CodeUnavailable.String()))
			return true
		default:
			seedLoggedRun(t, f, run, orkstackv1.Status_STATUS_SUCCEEDED, "build")
			return false
		}
	})
	out := f.run("run", "logs", run, "-f").requireExit(t, exitOK)
	if !strings.Contains(out.stdout, "[build] line 1\n") {
		t.Fatalf("stdout = %q", out.stdout)
	}
}

// A pinned attempt is the whole read: a resume never prints another, whatever
// attempt the cursor it was given counted.
func TestRunLogs_FollowResumeStaysOnAPinnedAttempt(t *testing.T) {
	fastResumes(t)
	const run, task = "run-logs-pinned", "flaky"
	f, logs := newLogFixture(t)
	seedTries(f, logs, run, task, 3, 2, 2)
	firstStreamsAnswered(f, 1, func(w http.ResponseWriter, r *http.Request) {
		rawAnswer(w, r, endStream("unavailable", "no replica"))
	})
	out := f.run("run", "logs", run, "--task", task, "-f", "--attempt", "3",
		"--from-attempt", "1", "--from-seq", "2").requireExit(t, exitOK)
	if want := "try 3 line 1\ntry 3 line 2\n"; out.stdout != want {
		t.Fatalf("stdout = %q, want %q", out.stdout, want)
	}
}
