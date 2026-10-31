package main

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// A connection the engine resets under a call reaches orkctl as CANCELED from
// connect-go v1.21.1 on, where it was UNAVAILABLE (engineInterrupted). These
// tests reset real TCP connections, over the h2c orkctl dials, and hold that
// `run watch` and `run inspect --watch` ride the reset out — and that
// orkctl's own cancel, a Ctrl-C, still ends them with no resume.

// resetListener remembers every connection it accepted, so a test can reset
// them all at once: SO_LINGER 0 and a close send the peer a TCP RST, what a
// client sees when the engine replica serving it dies.
type resetListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *resetListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, conn)
		l.mu.Unlock()
	}
	return conn, err
}

func (l *resetListener) reset() {
	l.mu.Lock()
	conns := l.conns
	l.conns = nil
	l.mu.Unlock()
	for _, conn := range conns {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	}
}

func TestRunWatch_ResumesAStreamWhoseConnectionWasReset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script []scriptedStream
		// resetOn is the printed text that resets the connection; empty,
		// the stream resets it as the call arrives.
		resetOn string
		froms   []int64
	}{
		{
			name:   "before any event",
			script: []scriptedStream{{reset: true}, {send: []int64{1, 2, 3, 4, 5}}},
			froms:  []int64{0, 0},
		},
		{
			name:    "mid-stream",
			script:  []scriptedStream{{send: []int64{1, 2}, stall: true}, {send: []int64{3, 4, 5}}},
			resetOn: "event 2",
			froms:   []int64{0, 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := &resumingRuns{script: tc.script}
			out := &liveWriter{triggers: map[string]func(){}}
			if tc.resetOn != "" {
				out.triggers[tc.resetOn] = func() { handler.reset() }
			}
			outcome := watchAgainstTo(t, handler, out)
			outcome.requireExit(t, exitOK)
			requireSequences(t, printedSequences(t, out.buf.String()), 1, 2, 3, 4, 5)
			if got := handler.calls(); len(got) != len(tc.froms) || got[0] != tc.froms[0] || got[1] != tc.froms[1] {
				t.Fatalf("from_sequence per stream = %v, want %v: a reset stream is resumed after the last event printed",
					got, tc.froms)
			}
			lines := resumeLines(outcome.stderr)
			if len(lines) != 1 || !strings.Contains(lines[0], "canceled") {
				t.Fatalf("stderr = %q, want one resume line naming the reset's canceled", outcome.stderr)
			}
		})
	}
}

func TestRunWatch_InterruptEndsTheWatchWithoutAResume(t *testing.T) {
	keepAliveOnInterrupt(t)
	handler := &resumingRuns{script: []scriptedStream{
		{send: []int64{1}, stall: true},
		{send: []int64{2, 3, 4, 5}},
	}}
	out := &liveWriter{triggers: map[string]func(){"event 1": func() { interruptSelf(t) }}}
	outcome := watchAgainstTo(t, handler, out)
	outcome.requireExit(t, exitInterrupted)
	if got := handler.calls(); len(got) != 1 {
		t.Fatalf("streams opened = %d, want 1: orkctl's own cancel is never resumed", len(got))
	}
	if lines := resumeLines(outcome.stderr); len(lines) != 0 {
		t.Fatalf("resume lines = %v, want none after a Ctrl-C", lines)
	}
}

// inspectWatchFrames is a run polled once running, then failed.
func inspectWatchFrames() []inspectFrame {
	running := trainRun(orkstackv1.Status_STATUS_RUNNING)
	failed := trainRun(orkstackv1.Status_STATUS_FAILED)
	failed.TaskRuns[0].Status = orkstackv1.Status_STATUS_FAILED
	return []inspectFrame{
		{run: running, live: engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{fitStarted()}}},
		{run: failed},
	}
}

// inspectWatch runs `run inspect --watch` against f, failing the test when it
// has not ended within 30 s.
func inspectWatch(t *testing.T, f *fixture) result {
	t.Helper()
	done := make(chan result, 1)
	go func() { done <- f.run("run", "inspect", "ork-train-1", "--watch", "--interval", "1s") }()
	select {
	case res := <-done:
		return res
	case <-time.After(30 * time.Second):
		t.Fatalf("run inspect --watch did not end within 30 s")
		return result{}
	}
}

func TestRunInspect_WatchRidesOutAResetConnection(t *testing.T) {
	f := newInspectFixture(t, inspectAt, inspectWatchFrames()...)
	var first sync.Once
	f.interceptWith(func(_ http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/InspectRun") {
			return false
		}
		reset := false
		first.Do(func() { reset = true })
		if !reset {
			return false
		}
		f.resetConnections()
		<-r.Context().Done()
		return true
	})
	res := inspectWatch(t, f)
	if res.code != exitRunFailed {
		t.Fatalf("exit = %d, want %d (failed): the reset poll is ridden out; stderr %s", res.code, exitRunFailed, res.stderr)
	}
	if got := strings.Count(res.stderr, "inspect unavailable: "); got != 1 {
		t.Fatalf("stderr = %q, want one inspect unavailable line for the reset poll", res.stderr)
	}
	if !strings.Contains(res.stdout, "run FAILED; nothing is live") {
		t.Fatalf("the watch did not poll on to the run's end:\n%s", res.stdout)
	}
}

func TestRunInspect_InterruptEndsTheWatchWithoutAResume(t *testing.T) {
	keepAliveOnInterrupt(t)
	f := newInspectFixture(t, inspectAt, inspectWatchFrames()...)
	f.interceptWith(func(_ http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/InspectRun") {
			return false
		}
		interruptSelf(t)
		<-r.Context().Done()
		return true
	})
	res := inspectWatch(t, f)
	if res.code != exitInterrupted {
		t.Fatalf("exit = %d, want %d; stderr %s", res.code, exitInterrupted, res.stderr)
	}
	if strings.Contains(res.stderr, "inspect unavailable") {
		t.Fatalf("stderr = %q, want no inspect unavailable line after a Ctrl-C", res.stderr)
	}
}
