package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// `run watch` resumes a stream the engine ended as unavailable — the answer a
// replica gives when its history read of a live run fails — from the last
// event it printed, as the UI's run page does, a bounded number of times in a
// row, and says so on stderr.

const resumeRunID = "ork-resume-1"

// resumeUnavailable is what the engine answered when its history read failed
// under the stream.
var resumeUnavailable = connect.NewError(connect.CodeUnavailable, errors.New(
	`stream run "ork-resume-1": engine: read history of run ork-resume-1: `+
		`Current and request branch tokens, or current and request versioned transitions, don't match`))

// resumeEvent is sequence seq of a five-event run whose fifth is RUN_SUCCEEDED.
func resumeEvent(seq int64) *orkstackv1.Event {
	event := &orkstackv1.Event{RunId: resumeRunID, Sequence: seq, Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		Status: orkstackv1.Status_STATUS_RUNNING, Message: "event " + strconv.FormatInt(seq, 10)}
	if seq == 5 {
		event.Type, event.Status, event.Message = orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
			orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded"
	}
	return event
}

// scriptedStream is one StreamEvents answer: the sequences it sends, then
// the error it ends with (nil for a clean end). A stream that stalls holds
// the call open after sending until the client goes away; one that resets
// resets the connection under the call first.
type scriptedStream struct {
	send []int64
	// events are sent after send, as they are.
	events []*orkstackv1.Event
	end    error
	stall  bool
	reset  bool
	// raw answers the call at the HTTP layer instead, for an end the Connect
	// handler cannot produce. It may be only the script's first entry.
	raw func(s *resumingRuns, w http.ResponseWriter, r *http.Request)
}

// resumingRuns answers StreamEvents from a script, one entry per call (the
// last entry repeats), and GetRun with a succeeded run. froms records each
// call's from_sequence.
type resumingRuns struct {
	orkstackv1connect.UnimplementedRunServiceHandler
	script []scriptedStream
	// reset resets every connection the server holds, closeWrite ends each
	// with a FIN, and goAway sends a GOAWAY on each and then ends it with a
	// FIN; watchAgainst sets all three.
	reset, closeWrite, goAway func()
	// runStatuses are GetRun's answers, one per call (the last repeats); none
	// is one succeeded run. A status of unspecified answers with getRunErr.
	runStatuses []orkstackv1.Status
	getRunErr   error

	mu       sync.Mutex
	froms    []int64
	getCalls int
}

// serveRaw answers the first call from the script's raw entry, when it has one.
func (s *resumingRuns) serveRaw(w http.ResponseWriter, r *http.Request) bool {
	s.mu.Lock()
	first := len(s.froms) == 0 && s.script[0].raw != nil
	if first {
		s.froms = append(s.froms, 0)
	}
	s.mu.Unlock()
	if first {
		s.script[0].raw(s, w, r)
	}
	return first
}

func (s *resumingRuns) StreamEvents(
	ctx context.Context,
	req *connect.Request[orkstackv1.StreamEventsRequest],
	stream *connect.ServerStream[orkstackv1.Event],
) error {
	s.mu.Lock()
	call := len(s.froms)
	s.froms = append(s.froms, req.Msg.GetFromSequence())
	s.mu.Unlock()
	answer := s.script[min(call, len(s.script)-1)]
	for _, seq := range answer.send {
		if err := stream.Send(resumeEvent(seq)); err != nil {
			return err
		}
	}
	for _, event := range answer.events {
		if err := stream.Send(event); err != nil {
			return err
		}
	}
	if answer.reset {
		s.reset()
	}
	if answer.reset || answer.stall {
		<-ctx.Done()
		return ctx.Err()
	}
	return answer.end
}

func (s *resumingRuns) GetRun(
	context.Context,
	*connect.Request[orkstackv1.GetRunRequest],
) (*connect.Response[orkstackv1.GetRunResponse], error) {
	s.mu.Lock()
	status := orkstackv1.Status_STATUS_SUCCEEDED
	if len(s.runStatuses) > 0 {
		status = s.runStatuses[min(s.getCalls, len(s.runStatuses)-1)]
	}
	s.getCalls++
	s.mu.Unlock()
	if status == orkstackv1.Status_STATUS_UNSPECIFIED {
		return nil, s.getRunErr
	}
	return connect.NewResponse(&orkstackv1.GetRunResponse{Run: &orkstackv1.Run{
		Id: resumeRunID, Status: status}}), nil
}

func (s *resumingRuns) calls() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.froms...)
}

// watchAgainst runs `orkctl run watch` against handler with the resume waits
// shrunk to nanoseconds — 1ns doubling to at most 4ns — so the test never
// waits on a clock and still sees the doubling and its cap.
func watchAgainst(t *testing.T, handler *resumingRuns, args ...string) result {
	t.Helper()
	var stdout bytes.Buffer
	outcome := watchAgainstTo(t, handler, &stdout, args...)
	outcome.stdout = stdout.String()
	return outcome
}

// watchAgainstTo is watchAgainst writing stdout into out, so a test can act
// on a line while the watch still runs. The server listens on a
// resetListener, which handler.reset resets, and a watch that has not ended
// within 30 s fails the test.
func watchAgainstTo(t *testing.T, handler *resumingRuns, out io.Writer, args ...string) result {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	delay, maxDelay := watchResumeDelay, watchResumeMaxDelay
	watchResumeDelay, watchResumeMaxDelay = time.Nanosecond, 4*time.Nanosecond
	t.Cleanup(func() { watchResumeDelay, watchResumeMaxDelay = delay, maxDelay })

	path, h := orkstackv1connect.NewRunServiceHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/StreamEvents") && handler.serveRaw(w, r) {
			return
		}
		h.ServeHTTP(w, r)
	}))
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	listener := &resetListener{Listener: server.Listener, goAwaySeen: make(chan struct{})}
	server.Listener = listener
	handler.reset, handler.closeWrite = listener.reset, listener.closeWrite
	handler.goAway = func() {
		go func() { _ = server.Config.Shutdown(context.Background()) }()
		<-listener.goAwaySeen
		listener.closeWrite()
	}
	server.Start()
	t.Cleanup(server.Close)

	full := append(append([]string{"run", "watch", resumeRunID}, args...), "--server", server.URL)
	done := make(chan result, 1)
	go func() {
		var stderr bytes.Buffer
		code := Execute(full, out, &stderr)
		done <- result{stderr: stderr.String(), code: code}
	}()
	select {
	case outcome := <-done:
		return outcome
	case <-time.After(30 * time.Second):
		t.Fatalf("run watch did not end within 30 s")
		return result{}
	}
}

// printedSequences reads the sequences of a human watch's event lines.
func printedSequences(t *testing.T, stdout string) []int64 {
	t.Helper()
	var seqs []int64
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if seq, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
			seqs = append(seqs, seq)
		}
	}
	return seqs
}

// jsonSequences reads the sequences of a --json watch's document, which must
// parse whole.
func jsonSequences(t *testing.T, stdout string) []int64 {
	t.Helper()
	var doc struct {
		Events []struct {
			Sequence string `json:"sequence"`
		} `json:"events"`
		Run *struct {
			Status string `json:"status"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("--json watch is not one JSON document: %v\n%s", err, stdout)
	}
	if doc.Run == nil || doc.Run.Status != "SUCCEEDED" {
		t.Fatalf("--json watch's run = %+v, want the succeeded run\n%s", doc.Run, stdout)
	}
	var seqs []int64
	for _, e := range doc.Events {
		seq, err := strconv.ParseInt(e.Sequence, 10, 64)
		if err != nil {
			t.Fatalf("event sequence %q: %v", e.Sequence, err)
		}
		seqs = append(seqs, seq)
	}
	return seqs
}

func requireSequences(t *testing.T, got []int64, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("printed sequences %v, want %v: each once, in order", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("printed sequences %v, want %v: each once, in order", got, want)
		}
	}
}

func resumeLines(stderr string) []string {
	var lines []string
	for _, line := range strings.Split(stderr, "\n") {
		if strings.HasPrefix(line, "watch run: the event stream was interrupted") {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestRunWatch_ResumesAnUnavailableStreamFromTheLastEvent(t *testing.T) {
	for _, mode := range []string{"human", "json"} {
		t.Run(mode, func(t *testing.T) {
			handler := &resumingRuns{script: []scriptedStream{
				{send: []int64{1, 2, 3}, end: resumeUnavailable},
				// A replica that repeats an event already printed: the watch
				// prints it once all the same.
				{send: []int64{3, 4, 5}},
			}}
			var args []string
			if mode == "json" {
				args = []string{"--json"}
			}
			outcome := watchAgainst(t, handler, args...)
			outcome.requireExit(t, exitOK)
			if mode == "json" {
				requireSequences(t, jsonSequences(t, outcome.stdout), 1, 2, 3, 4, 5)
			} else {
				requireSequences(t, printedSequences(t, outcome.stdout), 1, 2, 3, 4, 5)
				if !strings.Contains(outcome.stdout, "finished with status") {
					t.Fatalf("stdout has no final status:\n%s", outcome.stdout)
				}
			}
			if got := handler.calls(); len(got) != 2 || got[0] != 0 || got[1] != 3 {
				t.Fatalf("from_sequence per stream = %v, want [0 3]: the resume starts after the last event printed", got)
			}
			lines := resumeLines(outcome.stderr)
			if len(lines) != 1 {
				t.Fatalf("stderr = %q, want one resume line", outcome.stderr)
			}
			for _, want := range []string{"unavailable", "branch tokens", "resuming after event 3", "(1 of 6)"} {
				if !strings.Contains(lines[0], want) {
					t.Fatalf("resume line %q does not say %q", lines[0], want)
				}
			}
			if strings.Contains(outcome.stdout, "interrupted") {
				t.Fatalf("the resume note reached stdout:\n%s", outcome.stdout)
			}
		})
	}
}

func TestRunWatch_ResumeIsBoundedByStreamsThatDeliverNothing(t *testing.T) {
	handler := &resumingRuns{script: []scriptedStream{
		{send: []int64{1, 2}, end: resumeUnavailable},
		{end: resumeUnavailable},
	}}
	outcome := watchAgainst(t, handler)
	outcome.requireExit(t, exitError)
	if !strings.Contains(outcome.stderr, "Error: watch run: unavailable: ") {
		t.Fatalf("stderr = %q, want the engine's error once the resumes are spent", outcome.stderr)
	}
	calls := handler.calls()
	if len(calls) != 1+watchResumeLimit {
		t.Fatalf("streams opened = %d, want %d: the first and %d resumes", len(calls), 1+watchResumeLimit, watchResumeLimit)
	}
	for i, from := range calls[1:] {
		if from != 2 {
			t.Fatalf("resume %d asked from %d, want 2", i+1, from)
		}
	}
	lines := resumeLines(outcome.stderr)
	if len(lines) != watchResumeLimit {
		t.Fatalf("resume lines = %d, want %d:\n%s", len(lines), watchResumeLimit, outcome.stderr)
	}
	// The wait doubles from the first and stops at its cap.
	for i, want := range []string{"in 1ns (1 of 6)", "in 2ns (2 of 6)", "in 4ns (3 of 6)", "in 4ns (4 of 6)", "in 4ns (5 of 6)", "in 4ns (6 of 6)"} {
		if !strings.Contains(lines[i], want) {
			t.Fatalf("resume line %d = %q, want %q", i+1, lines[i], want)
		}
	}
	requireSequences(t, printedSequences(t, outcome.stdout), 1, 2)
}

// An event resets the bound: a stream that keeps making progress is resumed
// more times than the bound allows in a row.
func TestRunWatch_AnEventResetsTheResumeBound(t *testing.T) {
	script := []scriptedStream{}
	for seq := int64(1); seq < 5; seq++ {
		script = append(script, scriptedStream{end: resumeUnavailable},
			scriptedStream{send: []int64{seq}, end: resumeUnavailable})
	}
	script = append(script, scriptedStream{send: []int64{5}})
	handler := &resumingRuns{script: script}
	outcome := watchAgainst(t, handler)
	outcome.requireExit(t, exitOK)
	requireSequences(t, printedSequences(t, outcome.stdout), 1, 2, 3, 4, 5)
	if got, want := len(resumeLines(outcome.stderr)), len(script)-1; got != want || want <= watchResumeLimit {
		t.Fatalf("resume lines = %d, want %d (more than the bound of %d)", got, want, watchResumeLimit)
	}
}

func TestRunWatch_OtherStreamErrorsAreNotResumed(t *testing.T) {
	// INTERNAL and UNKNOWN are the codes a connection's end reaches orkctl
	// with: said by the engine in the stream's end message, they end the watch.
	for _, code := range []connect.Code{connect.CodeInternal, connect.CodeUnknown, connect.CodeNotFound, connect.CodeDeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			handler := &resumingRuns{script: []scriptedStream{
				{send: []int64{1}, end: connect.NewError(code, errors.New("no"))},
				{send: []int64{2, 3, 4, 5}},
			}}
			outcome := watchAgainst(t, handler)
			outcome.requireExit(t, exitError)
			if want := "Error: watch run: " + code.String() + ": no"; !strings.Contains(outcome.stderr, want) {
				t.Fatalf("stderr = %q, want %q", outcome.stderr, want)
			}
			if got := handler.calls(); len(got) != 1 {
				t.Fatalf("streams opened = %d, want 1: only an interrupted engine (engineInterrupted) is resumed", len(got))
			}
			if lines := resumeLines(outcome.stderr); len(lines) != 0 {
				t.Fatalf("resume lines = %v, want none", lines)
			}
		})
	}
}

// A replica shutting down ends its streams cleanly on a run that has not
// ended. The run's status says so, and the watch resumes rather than report
// a live run as finished.
func TestRunWatch_ResumesAStreamThatEndsWhileTheRunIsLive(t *testing.T) {
	for _, status := range []orkstackv1.Status{orkstackv1.Status_STATUS_RUNNING, orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_AWAITING} {
		t.Run(status.String(), func(t *testing.T) {
			handler := &resumingRuns{
				script:      []scriptedStream{{send: []int64{1, 2}}, {send: []int64{3, 4, 5}}},
				runStatuses: []orkstackv1.Status{status, orkstackv1.Status_STATUS_SUCCEEDED},
			}
			outcome := watchAgainst(t, handler)
			outcome.requireExit(t, exitOK)
			requireSequences(t, printedSequences(t, outcome.stdout), 1, 2, 3, 4, 5)
			if got := handler.calls(); len(got) != 2 || got[1] != 2 {
				t.Fatalf("from_sequence per stream = %v, want [0 2]", got)
			}
			if strings.Contains(outcome.stdout, statusLabel(status)) {
				t.Fatalf("the watch reported the live run as finished:\n%s", outcome.stdout)
			}
			lines := resumeLines(outcome.stderr)
			if len(lines) != 1 || !strings.Contains(lines[0], "the stream ended with the run still "+statusLabel(status)) {
				t.Fatalf("resume lines = %v", lines)
			}
		})
	}
}

// An engine whose streams keep ending on a live run is reported, not retried
// for ever, and --json still prints one whole document.
func TestRunWatch_LiveRunResumeIsBounded(t *testing.T) {
	handler := &resumingRuns{
		script:      []scriptedStream{{send: nil}},
		runStatuses: []orkstackv1.Status{orkstackv1.Status_STATUS_RUNNING},
	}
	outcome := watchAgainst(t, handler, "--json")
	outcome.requireExit(t, exitError)
	if got := handler.calls(); len(got) != 1+watchResumeLimit {
		t.Fatalf("streams opened = %d, want %d", len(got), 1+watchResumeLimit)
	}
	if want := "Error: watch run: the stream ended with the run still RUNNING"; !strings.Contains(outcome.stderr, want) {
		t.Fatalf("stderr = %q, want %q", outcome.stderr, want)
	}
	if !json.Valid([]byte(outcome.stdout)) {
		t.Fatalf("stdout does not parse:\n%s", outcome.stdout)
	}
}

// The final GetRun meets the same engine going away: an interrupted answer is
// resumed, and any other ends the watch naming it.
func TestRunWatch_FinalGetRunAnswer(t *testing.T) {
	t.Run("interrupted", func(t *testing.T) {
		handler := &resumingRuns{
			script:      []scriptedStream{{send: []int64{1, 2, 3, 4, 5}}},
			runStatuses: []orkstackv1.Status{orkstackv1.Status_STATUS_UNSPECIFIED, orkstackv1.Status_STATUS_SUCCEEDED},
			getRunErr:   resumeUnavailable,
		}
		outcome := watchAgainst(t, handler)
		outcome.requireExit(t, exitOK)
		requireSequences(t, printedSequences(t, outcome.stdout), 1, 2, 3, 4, 5)
		if lines := resumeLines(outcome.stderr); len(lines) != 1 {
			t.Fatalf("resume lines = %v, want one", lines)
		}
	})
	t.Run("refused", func(t *testing.T) {
		handler := &resumingRuns{
			script:      []scriptedStream{{send: []int64{1, 2, 3, 4, 5}}},
			runStatuses: []orkstackv1.Status{orkstackv1.Status_STATUS_UNSPECIFIED},
			getRunErr:   connect.NewError(connect.CodeNotFound, errors.New("no such run")),
		}
		outcome := watchAgainst(t, handler)
		outcome.requireExit(t, exitError)
		if want := "Error: get run: not_found: no such run"; !strings.Contains(outcome.stderr, want) {
			t.Fatalf("stderr = %q, want %q", outcome.stderr, want)
		}
		if lines := resumeLines(outcome.stderr); len(lines) != 0 {
			t.Fatalf("resume lines = %v, want none", lines)
		}
	})
}

// TestRunWatch_ShowsAGapAndReachesTheRunsEnd: numbers the run never published
// are said, in the human table and the JSON document alike, and the watch goes
// on to the run's end and exits with its outcome.
func TestRunWatch_ShowsAGapAndReachesTheRunsEnd(t *testing.T) {
	for _, mode := range []string{"human", "json"} {
		t.Run(mode, func(t *testing.T) {
			var args []string
			if mode == "json" {
				args = []string{"--json"}
			}
			outcome := watchAgainst(t, &resumingRuns{script: []scriptedStream{{events: []*orkstackv1.Event{
				resumeEvent(1), resumeEvent(2),
				{RunId: resumeRunID, Sequence: 4, Type: orkstackv1.EventType_EVENT_TYPE_STREAM_GAP,
					Message: "events 3–4 of this run were never published"},
				resumeEvent(5),
			}}}}, args...)
			outcome.requireExit(t, exitOK)
			if mode == "json" {
				requireSequences(t, jsonSequences(t, outcome.stdout), 1, 2, 4, 5)
				if !strings.Contains(outcome.stdout, `"STREAM_GAP"`) {
					t.Fatalf("the document does not say STREAM_GAP:\n%s", outcome.stdout)
				}
			} else {
				requireSequences(t, printedSequences(t, outcome.stdout), 1, 2, 4, 5)
				if !strings.Contains(outcome.stdout, "STREAM_GAP") || !strings.Contains(outcome.stdout, "finished with status") {
					t.Fatalf("stdout does not show the gap and the run's end:\n%s", outcome.stdout)
				}
			}
			if !strings.Contains(outcome.stdout, "events 3–4 of this run were never published") {
				t.Fatalf("stdout does not name the numbers:\n%s", outcome.stdout)
			}
			if resumes := resumeLines(outcome.stderr); len(resumes) != 0 {
				t.Fatalf("the watch resumed over a gap: %q", resumes)
			}
		})
	}
}
