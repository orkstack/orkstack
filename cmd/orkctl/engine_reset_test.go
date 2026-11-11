package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// A connection the engine resets under a call reaches orkctl as CANCELED from
// connect-go v1.21.1 on when orkctl's read is the first to meet the reset, and
// as the connection's end when its own write is (engineInterrupted). These
// tests end real TCP connections, over the h2c orkctl dials, and hold that
// `run watch` and `run inspect --watch` ride that out, that an HTTP stream the
// engine ends itself on a connection that stays up is not taken for it — and
// that orkctl's own cancel, a Ctrl-C, still ends them with no resume.

// resetListener remembers every connection it accepted, so a test can reset
// them all at once: SO_LINGER 0 and a close send the peer a TCP RST, what a
// client sees when the engine replica serving it dies.
type resetListener struct {
	net.Listener
	// goAwaySeen, when set, closes once a server has written a GOAWAY.
	goAwaySeen chan struct{}
	goAwayOnce sync.Once
	mu         sync.Mutex
	conns      []net.Conn
}

func (l *resetListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.conns = append(l.conns, conn)
	l.mu.Unlock()
	return &goAwayConn{Conn: conn, listener: l}, nil
}

// goAwayConn tells its listener when the server writes a GOAWAY: the frame's
// header, with an 8-byte payload, on stream 0. A write that split it would
// fail the test by its deadline, never pass it.
type goAwayConn struct {
	net.Conn
	listener *resetListener
}

var goAwayFrameHeader = []byte{0, 0, 8, 7, 0, 0, 0, 0, 0}

func (c *goAwayConn) Write(data []byte) (int, error) {
	n, err := c.Conn.Write(data)
	if c.listener.goAwaySeen != nil && bytes.Contains(data[:n], goAwayFrameHeader) {
		c.listener.goAwayOnce.Do(func() { close(c.listener.goAwaySeen) })
	}
	return n, err
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

// closeWrite ends every connection's server side with a FIN, leaving it open
// for the client to close.
func (l *resetListener) closeWrite() {
	l.mu.Lock()
	conns := l.conns
	l.mu.Unlock()
	for _, conn := range conns {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
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
		// ways are how the reset can reach orkctl. Linux hands an RST to the
		// first syscall that meets it, as ECONNRESET, and a read after that
		// sees the connection end. A read that meets it is CANCELED
		// (connect-go's isClientDisconnected). A write meets it first only
		// when it is one whose error Go's HTTP/2 client drops — the SETTINGS
		// acknowledgement (processSettings), or a flow-control update once a
		// response is being read — and the read loop then sees the end,
		// io.ErrUnexpectedEOF, which before any response is UNAVAILABLE
		// (connect-go's makeRequest).
		ways []resetWay
	}{
		{
			name:   "before any event",
			script: []scriptedStream{{reset: true}, {send: []int64{1, 2, 3, 4, 5}}},
			froms:  []int64{0, 0},
			// The server resets once it has read the request, and the
			// client's read loop may not yet have read the server's
			// SETTINGS: on a loaded host it reads them after the reset,
			// and the acknowledgement's write meets the reset first.
			ways: []resetWay{resetRead, resetAfterDroppedWrite},
		},
		{
			name:    "mid-stream",
			script:  []scriptedStream{{send: []int64{1, 2}, stall: true}, {send: []int64{3, 4, 5}}},
			resetOn: "event 2",
			froms:   []int64{0, 2},
			// Event 2 printed, so the client had read every frame before
			// it, the server's SETTINGS first, and it writes nothing after
			// it: the events are far below the 4 KiB a flow-control update
			// waits for, and orkctl sends no pings. Its read meets the reset.
			ways: []resetWay{resetRead},
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
			if len(lines) != 1 || !slices.ContainsFunc(tc.ways, func(way resetWay) bool { return way.saidBy(lines[0]) }) {
				t.Fatalf("stderr = %q, want one resume line saying one of %v", outcome.stderr, tc.ways)
			}
		})
	}
}

// resetWay is one way a reset reaches orkctl: the code its resume line
// names. The cause after the code is the OS's and the HTTP/2 client's own
// wording, which neither orkctl nor this test owns.
type resetWay string

const (
	resetRead              resetWay = "canceled"
	resetAfterDroppedWrite resetWay = "unavailable"
)

// saidBy reports whether line is the first resume of a stream interrupted
// with w's code.
func (w resetWay) saidBy(line string) bool {
	said, ok := strings.CutPrefix(line, "watch run: the event stream was interrupted ("+string(w)+": ")
	return ok && strings.HasSuffix(said, " (1 of "+strconv.Itoa(watchResumeLimit)+")")
}

// streamEnvelope is one Connect streaming envelope holding event.
func streamEnvelope(t *testing.T, event *orkstackv1.Event) []byte {
	t.Helper()
	body, err := proto.Marshal(event)
	must.OK(t, err)
	prefix := []byte{0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(prefix[1:], uint32(len(body)))
	return append(prefix, body...)
}

// rawAnswer opens a StreamEvents response at the HTTP layer and writes wire to
// it, flushed, so a test can end the stream where the Connect handler cannot.
func rawAnswer(w http.ResponseWriter, r *http.Request, wire []byte) {
	w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(wire)
	w.(http.Flusher).Flush()
}

// oneEventThen is the first event's envelope followed by the bytes of a
// stream cut at the place named.
func oneEventThen(t *testing.T, rest ...byte) []byte {
	t.Helper()
	return append(streamEnvelope(t, resumeEvent(1)), rest...)
}

// A prefix promising 100 bytes, and 3 of them.
var cutInsideAMessage = []byte{0, 0, 0, 0, 100, 1, 2, 3}

// TestRunWatch_ResumesAConnectionThatEndsMidStream: an engine that closes its
// side (a FIN, what a replica that exits sends) ends the stream as INTERNAL
// between messages and inside the prefix, and as UNKNOWN inside a message
// (connect-go's envelopeReader.Read). Wherever it cuts, the watch resumes after
// the last whole event. CloseWrite sends the FIN whatever the server left
// unread, where a Close could send an RST instead.
func TestRunWatch_ResumesAConnectionThatEndsMidStream(t *testing.T) {
	for _, tc := range []struct {
		name string
		rest []byte
		said string
	}{
		{name: "between messages", said: "internal: protocol error: incomplete envelope: unexpected EOF"},
		{name: "inside the prefix", rest: []byte{0, 0}, said: "internal: protocol error: incomplete envelope: unexpected EOF"},
		{name: "inside a message", rest: cutInsideAMessage, said: "unknown: read enveloped message: unexpected EOF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The FIN waits for the event to print: on an end that races the
			// response's headers, Go's HTTP/2 client drops what it had read.
			printed := make(chan struct{})
			handler := &resumingRuns{script: []scriptedStream{
				{raw: func(s *resumingRuns, w http.ResponseWriter, r *http.Request) {
					rawAnswer(w, r, oneEventThen(t, tc.rest...))
					<-printed
					s.closeWrite()
					<-r.Context().Done()
				}},
				{send: []int64{2, 3, 4, 5}},
			}}
			out := &liveWriter{triggers: map[string]func(){"event 1": func() { close(printed) }}}
			outcome := watchAgainstTo(t, handler, out)
			outcome.requireExit(t, exitOK)
			requireSequences(t, printedSequences(t, out.buf.String()), 1, 2, 3, 4, 5)
			if got := handler.calls(); len(got) != 2 || got[1] != 1 {
				t.Fatalf("from_sequence per stream = %v, want [0 1]", got)
			}
			lines := resumeLines(outcome.stderr)
			if len(lines) != 1 || !strings.Contains(lines[0], "interrupted ("+tc.said+")") {
				t.Fatalf("resume lines = %v, want one saying %q", lines, tc.said)
			}
		})
	}
}

// TestRunWatch_ResumesAfterAGoAwayAndAClose: a server that said GOAWAY and then
// closed the connection with the stream open, as a replica past its shutdown
// budget or a load balancer does. The server here has stopped listening, so
// every resume after the first finds no engine, and the watch gives up on that.
func TestRunWatch_ResumesAfterAGoAwayAndAClose(t *testing.T) {
	handler := &resumingRuns{script: []scriptedStream{{send: []int64{1, 2}, stall: true}}}
	out := &liveWriter{triggers: map[string]func(){"event 2": func() { handler.goAway() }}}
	outcome := watchAgainstTo(t, handler, out)
	outcome.requireExit(t, exitError)
	requireSequences(t, printedSequences(t, out.buf.String()), 1, 2)
	lines := resumeLines(outcome.stderr)
	if len(lines) != watchResumeLimit {
		t.Fatalf("resume lines = %v, want %d", lines, watchResumeLimit)
	}
	if !strings.Contains(lines[0], goAwayEnded) || !strings.Contains(lines[0], "resuming after event 2") {
		t.Fatalf("first resume line = %q, want the GOAWAY's end, resuming after event 2", lines[0])
	}
}

// TestRunWatch_DoesNotResumeAStreamEndedOnALiveConnection: what connect-go
// reports for these has the code, and for the cut prefix the text, of a
// connection's end, but the connection is up and the engine ended the stream
// itself: cleanly with no end message, or by resetting it (a handler's panic).
// Each ends the watch.
func TestRunWatch_DoesNotResumeAStreamEndedOnALiveConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		rest []byte
		// abort resets the stream instead of ending it cleanly.
		abort bool
		said  string
	}{
		{name: "clean end between messages", said: "watch run: internal: protocol error: unexpected EOF"},
		{name: "clean end inside the prefix", rest: []byte{0, 0}, said: "watch run: internal: protocol error: incomplete envelope: unexpected EOF"},
		{name: "clean end inside a message", rest: cutInsideAMessage, said: "watch run: internal: protocol error: promised 100 bytes in enveloped message, got 3 bytes: unexpected EOF"},
		{name: "a handler's panic", abort: true, said: "watch run: internal: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A reset waits for the event to print, as the FIN above does.
			printed := make(chan struct{})
			handler := &resumingRuns{script: []scriptedStream{
				{raw: func(_ *resumingRuns, w http.ResponseWriter, r *http.Request) {
					rawAnswer(w, r, oneEventThen(t, tc.rest...))
					if tc.abort {
						<-printed
						panic(http.ErrAbortHandler)
					}
				}},
				{send: []int64{2, 3, 4, 5}},
			}}
			out := &liveWriter{triggers: map[string]func(){"event 1": func() { close(printed) }}}
			outcome := watchAgainstTo(t, handler, out)
			outcome.requireExit(t, exitError)
			if !strings.Contains(outcome.stderr, "Error: "+tc.said) {
				t.Fatalf("stderr = %q, want %q", outcome.stderr, tc.said)
			}
			requireSequences(t, printedSequences(t, out.buf.String()), 1)
			if got := handler.calls(); len(got) != 1 {
				t.Fatalf("streams opened = %d, want 1", len(got))
			}
			if lines := resumeLines(outcome.stderr); len(lines) != 0 {
				t.Fatalf("resume lines = %v, want none", lines)
			}
		})
	}
}

// TestConnectionEnded: only the failures of the connection under a read count,
// and the mark keeps the cause and its text.
func TestConnectionEnded(t *testing.T) {
	read := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ETIMEDOUT}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"the connection closed", io.ErrUnexpectedEOF, true},
		{"a wrapped close", fmt.Errorf("body: %w", io.ErrUnexpectedEOF), true},
		{"closed after a GOAWAY", errors.New(goAwayEnded + `; LastStreamID=1, ErrCode=NO_ERROR, debug=""`), true},
		{"a failed network read", read, true},
		{"a failed network write", &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}, false},
		{"a clean end", io.EOF, false},
		{"a peer's stream reset", errors.New("stream error: stream ID 1; INTERNAL_ERROR; received from peer"), false},
		{"a cancel", context.Canceled, false},
		{"a closed body", http.ErrBodyReadAfterClose, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectionEnded(tc.err); got != tc.want {
				t.Fatalf("connectionEnded(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
	marked := &connectionEndedError{err: read}
	if marked.Error() != read.Error() || !errors.Is(marked, syscall.ETIMEDOUT) {
		t.Fatalf("the mark changed its cause: %v", marked)
	}
}

// TestEngineInterrupted_AConnectionsEndAfterACtrlC: connect-go codes a read
// failing after its context ended as CANCELED around the same cause, and a
// Ctrl-C ends the watch whichever came first.
func TestEngineInterrupted_AConnectionsEndAfterACtrlC(t *testing.T) {
	ended := connect.NewError(connect.CodeInternal, &connectionEndedError{err: io.ErrUnexpectedEOF})
	if !engineInterrupted(context.Background(), ended) {
		t.Fatal("a connection's end under a live context is an interrupted engine")
	}
	done, cancel := context.WithCancel(context.Background())
	cancel()
	if engineInterrupted(done, ended) {
		t.Fatal("a Ctrl-C is never resumed")
	}
	genuine := connect.NewError(connect.CodeInternal, io.ErrUnexpectedEOF)
	if engineInterrupted(context.Background(), genuine) {
		t.Fatal("an error with no mark is not a connection's end")
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

// TestCloseIdleConnections_ReleasesTheEnginesConnection: orkctl lets its
// sockets go when a command ends, through the mark connectionEndTransport puts
// on a body as through a bare transport.
func TestCloseIdleConnections_ReleasesTheEnginesConnection(t *testing.T) {
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	g := &globalFlags{server: server.URL}
	response, err := g.httpClient().Get(server.URL)
	must.OK(t, err)
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()

	g.closeIdleConnections()
	select {
	case <-closed:
	case <-time.After(30 * time.Second):
		t.Fatal("the engine's connection was still open 30 s after closeIdleConnections")
	}
}
