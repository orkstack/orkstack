package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
// the error it ends with (nil for a clean end).
type scriptedStream struct {
	send []int64
	end  error
}

// resumingRuns answers StreamEvents from a script, one entry per call (the
// last entry repeats), and GetRun with a succeeded run. froms records each
// call's from_sequence.
type resumingRuns struct {
	orkstackv1connect.UnimplementedRunServiceHandler
	script []scriptedStream

	mu    sync.Mutex
	froms []int64
}

func (s *resumingRuns) StreamEvents(
	_ context.Context,
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
	return answer.end
}

func (s *resumingRuns) GetRun(
	context.Context,
	*connect.Request[orkstackv1.GetRunRequest],
) (*connect.Response[orkstackv1.GetRunResponse], error) {
	return connect.NewResponse(&orkstackv1.GetRunResponse{Run: &orkstackv1.Run{
		Id: resumeRunID, Status: orkstackv1.Status_STATUS_SUCCEEDED}}), nil
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
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	delay, maxDelay := watchResumeDelay, watchResumeMaxDelay
	watchResumeDelay, watchResumeMaxDelay = time.Nanosecond, 4*time.Nanosecond
	t.Cleanup(func() { watchResumeDelay, watchResumeMaxDelay = delay, maxDelay })

	path, h := orkstackv1connect.NewRunServiceHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	full := append(append([]string{"run", "watch", resumeRunID}, args...), "--server", server.URL)
	code := Execute(full, &stdout, &stderr)
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
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
	for _, code := range []connect.Code{connect.CodeInternal, connect.CodeNotFound, connect.CodeDeadlineExceeded} {
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
				t.Fatalf("streams opened = %d, want 1: only unavailable is resumed", len(got))
			}
			if lines := resumeLines(outcome.stderr); len(lines) != 0 {
				t.Fatalf("resume lines = %v, want none", lines)
			}
		})
	}
}
