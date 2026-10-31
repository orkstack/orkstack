package api_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The API listener's bounds, shrunk so a test outlasts them in about a
// second rather than in minutes. NewServerWith sets the real ones
// (TestNewServer_BoundsHeadersAndIdleConnectionsAndNothingElse), and
// serveBounded replaces them on the very server it built, so what is proven
// here is what those kinds of bound do, not a copy of them.
const (
	testHeaderTimeout = 150 * time.Millisecond
	testIdleTimeout   = 250 * time.Millisecond
	// outlast is how long a long-lived stream sits silent: several times
	// both bounds over.
	outlast = 4 * testIdleTimeout
	// closeDeadline is how long a test waits for a bound to close a
	// connection before it says the bound is missing.
	closeDeadline = 10 * time.Second
)

// serveBounded serves handler from NewServerWith with its bounds shrunk, and
// returns the address it listens on.
func serveBounded(t *testing.T, handler http.Handler) string {
	t.Helper()
	server := api.NewServerWith("127.0.0.1:0", handler, api.ServerOptions{IdleTimeout: testIdleTimeout})
	server.ReadHeaderTimeout = testHeaderTimeout
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	return listener.Addr().String()
}

// dialRaw opens a plain TCP connection, for a client that speaks HTTP by hand.
func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// requireClosedByBound reads r until the server closes conn, and fails when
// it is still open after closeDeadline (the bound is missing) or closed
// sooner than bound after start (something else closed it). start must be
// taken before what starts the server's own timer, so the client's elapsed
// time is never shorter than the server's however the host schedules either.
func requireClosedByBound(t *testing.T, conn net.Conn, r io.Reader, start time.Time, bound time.Duration) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(closeDeadline)); err != nil {
		t.Fatal(err)
	}
	_, err := io.Copy(io.Discard, r)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("the connection was still open %s later: nothing bounds it", closeDeadline)
	}
	if elapsed := time.Since(start); elapsed < bound {
		t.Fatalf("the connection closed after %s, before its %s bound", elapsed, bound)
	}
}

// openIdleConnection makes one request on a raw keep-alive connection and
// leaves it idle, returning the connection, its reader and a time no later
// than when the server began counting it idle: before the request was sent.
func openIdleConnection(t *testing.T, addr, path string) (net.Conn, *bufio.Reader, time.Time) {
	t.Helper()
	conn := dialRaw(t, addr)
	sent := time.Now()
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\n\r\n", path, addr); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.Close {
		t.Fatalf("the server closed a keep-alive connection after one request (%s)", response.Status)
	}
	return conn, reader, sent
}

// openSlowHeader starts a request whose headers never end, returning a time
// no later than when the server began waiting for them: before the dial.
func openSlowHeader(t *testing.T, addr string) (net.Conn, time.Time) {
	t.Helper()
	start := time.Now()
	conn := dialRaw(t, addr)
	if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nX-Slow: ", addr); err != nil {
		t.Fatal(err)
	}
	return conn, start
}

// HTTP/2 frame types and flags the h2c probes below write and read.
const (
	h2FrameData     = 0x0
	h2FrameHeaders  = 0x1
	h2FrameSettings = 0x4
	h2FrameGoAway   = 0x7
	h2FlagAck       = 0x1
	h2FlagEndStream = 0x1
	h2FlagEndHeader = 0x4
)

// h2Request is a request's header block: :method GET, :scheme http, :path /
// (HPACK static table 2, 6, 4), and :authority localhost as a literal that
// is never indexed, so every request can reuse it.
var h2Request = []byte{0x82, 0x86, 0x84, 0x01, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't'}

// h2Frame encodes one HTTP/2 frame.
func h2Frame(kind, flags byte, stream uint32, payload []byte) []byte {
	header := []byte{byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)), kind, flags,
		byte(stream>>24) & 0x7f, byte(stream >> 16), byte(stream >> 8), byte(stream)}
	return append(header, payload...)
}

// readH2Frame reads one HTTP/2 frame's header and discards its payload.
func readH2Frame(r io.Reader) (kind, flags byte, stream uint32, err error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, 0, err
	}
	size := int64(header[0])<<16 | int64(header[1])<<8 | int64(header[2])
	if _, err := io.CopyN(io.Discard, r, size); err != nil {
		return 0, 0, 0, err
	}
	stream = uint32(header[5]&0x7f)<<24 | uint32(header[6])<<16 | uint32(header[7])<<8 | uint32(header[8])
	return header[3], header[4], stream, nil
}

// openH2c opens an h2c connection with prior knowledge and reads the
// server's SETTINGS, which a client acknowledges before it sends anything
// else; the caller writes that acknowledgement with its first frames. It
// returns a time no later than when the server began counting the
// connection idle: before the dial.
func openH2c(t *testing.T, addr string) (net.Conn, time.Time) {
	t.Helper()
	start := time.Now()
	conn := dialRaw(t, addr)
	if _, err := conn.Write(append([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"), h2Frame(h2FrameSettings, 0, 0, nil)...)); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(closeDeadline)); err != nil {
		t.Fatal(err)
	}
	kind, flags, _, err := readH2Frame(conn)
	if err != nil {
		t.Fatalf("read the server's first frame: %v", err)
	}
	if kind != h2FrameSettings || flags&h2FlagAck != 0 {
		t.Fatalf("the server's first frame is type %d flags %d, want SETTINGS", kind, flags)
	}
	return conn, start
}

// openSlowH2Header opens an h2c connection and starts its first request
// with a header block that never ends: a HEADERS frame without
// END_HEADERS, and no CONTINUATION after it. It returns a time no later
// than when the server began counting the connection idle: before the dial.
func openSlowH2Header(t *testing.T, addr string) (net.Conn, time.Time) {
	t.Helper()
	conn, start := openH2c(t, addr)
	stalled := append(h2Frame(h2FrameSettings, h2FlagAck, 0, nil), h2Frame(h2FrameHeaders, 0, 1, h2Request)...)
	if _, err := conn.Write(stalled); err != nil {
		t.Fatal(err)
	}
	return conn, start
}

// The listener bounds what a client may make it wait for and hold, and
// nothing that would end a request already being answered: no whole-request
// read deadline and no write deadline, either of which would cut a run's
// event stream, a task's live log or a WebSocket mid-way.
func TestNewServer_BoundsHeadersAndIdleConnectionsAndNothingElse(t *testing.T) {
	handler := http.NotFoundHandler()
	server := api.NewServer("127.0.0.1:0", handler)
	if server.ReadHeaderTimeout != api.ReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %s, want %s", server.ReadHeaderTimeout, api.ReadHeaderTimeout)
	}
	if server.IdleTimeout != api.DefaultIdleTimeout {
		t.Errorf("IdleTimeout = %s, want %s", server.IdleTimeout, api.DefaultIdleTimeout)
	}
	if server.MaxHeaderBytes != api.MaxHeaderBytes {
		t.Errorf("MaxHeaderBytes = %d, want %d", server.MaxHeaderBytes, api.MaxHeaderBytes)
	}
	if server.ReadTimeout != 0 || server.WriteTimeout != 0 {
		t.Errorf("ReadTimeout = %s, WriteTimeout = %s; want neither: each would end a long-lived stream",
			server.ReadTimeout, server.WriteTimeout)
	}
	if !server.Protocols.HTTP1() || !server.Protocols.UnencryptedHTTP2() {
		t.Errorf("protocols = %s, want HTTP/1.1 and h2c", server.Protocols)
	}

	tuned := api.NewServerWith("127.0.0.1:0", handler, api.ServerOptions{IdleTimeout: 11 * time.Minute})
	if tuned.IdleTimeout != 11*time.Minute {
		t.Errorf("IdleTimeout = %s, want the 11m it was given", tuned.IdleTimeout)
	}
	if tuned.ReadHeaderTimeout != api.ReadHeaderTimeout || tuned.MaxHeaderBytes != api.MaxHeaderBytes {
		t.Errorf("an option changed a fixed bound: %s, %d", tuned.ReadHeaderTimeout, tuned.MaxHeaderBytes)
	}
}

func TestAPIListener_ClosesASlowHeaderAnIdleConnectionAndRefusesOversizedHeaders(t *testing.T) {
	addr := serveBounded(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	t.Run("a request whose headers never end", func(t *testing.T) {
		conn, start := openSlowHeader(t, addr)
		requireClosedByBound(t, conn, conn, start, testHeaderTimeout)
	})

	t.Run("a keep-alive connection left idle", func(t *testing.T) {
		conn, reader, idleSince := openIdleConnection(t, addr, "/")
		requireClosedByBound(t, conn, reader, idleSince, testIdleTimeout)
	})

	// Go's HTTP/2 server does not apply ReadHeaderTimeout. A first request
	// whose header block never ends opens no stream, so the connection
	// carries none, and the idle bound closes it: GOAWAY at the bound, and
	// the connection a second later.
	t.Run("an h2c request whose header block never ends", func(t *testing.T) {
		conn, start := openSlowH2Header(t, addr)
		requireClosedByBound(t, conn, conn, start, testIdleTimeout)
	})

	t.Run("headers over the bound", func(t *testing.T) {
		conn := dialRaw(t, addr)
		big := strings.Repeat("a", api.MaxHeaderBytes+8<<10)
		if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nX-Big: %s\r\n\r\n", addr, big); err != nil {
			t.Fatal(err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(closeDeadline)); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
			t.Fatalf("headers of %d bytes: %s, want 431", len(big), response.Status)
		}
	})

	t.Run("headers under the bound", func(t *testing.T) {
		conn := dialRaw(t, addr)
		// What a gateway forwards: a few kilobytes of cookie and token.
		cookie := strings.Repeat("c", 24<<10)
		if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nCookie: session=%s\r\n\r\n", addr, cookie); err != nil {
			t.Fatal(err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(closeDeadline)); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("a 24 KiB cookie: %s, want 204", response.Status)
		}
	})
}

// longStream is one long-lived answer the engine streams: next blocks until
// its next message and describes it.
type longStream struct {
	name string
	next func() (string, error)
}

// Every long-lived stream the API serves outlives the listener's bounds: it
// sits silent for several times both of them over, and then still delivers
// what happens next on the connection it opened. In the same silence, a
// keep-alive connection and a request whose headers never end are closed on
// each server, so the bounds were in force while the streams survived them.
// Go's HTTP/2 server bounds an h2c connection by the idle bound alone, which
// counts only while it carries no stream: a header block left unfinished
// beside an open stream is bounded by nothing until that stream ends
// (docs/DEPLOY.md, "The API listener's bounds"). Over the window the idle
// bound and its grace would have closed the connection in, the open stream
// keeps answering, so the connection was live and the server was serving;
// once the stream ends, the idle bound closes the connection.
func TestAPIListener_AnH2cHeaderBlockBesideAnOpenStreamWaitsForThatStream(t *testing.T) {
	release := make(chan struct{})
	addr := serveBounded(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ticker := time.NewTicker(testIdleTimeout / 5)
		defer ticker.Stop()
		for {
			select {
			case <-release:
				return
			case <-r.Context().Done():
				return
			case <-ticker.C:
				_, _ = w.Write([]byte("tick"))
				w.(http.Flusher).Flush()
			}
		}
	}))
	conn, _ := openH2c(t, addr)
	frames := append(h2Frame(h2FrameSettings, h2FlagAck, 0, nil),
		h2Frame(h2FrameHeaders, h2FlagEndHeader|h2FlagEndStream, 1, h2Request)...)
	frames = append(frames, h2Frame(h2FrameHeaders, 0, 3, h2Request)...)
	opened := time.Now()
	if _, err := conn.Write(frames); err != nil {
		t.Fatal(err)
	}

	// The idle bound sends GOAWAY at testIdleTimeout and closes the
	// connection a second later; the window outlasts both.
	window := opened.Add(testIdleTimeout + time.Second + outlast)
	if err := conn.SetReadDeadline(window.Add(closeDeadline)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	late := 0
	for time.Now().Before(window) {
		kind, _, stream, err := readH2Frame(reader)
		if err != nil {
			t.Fatalf("the connection ended %s after the stalled header block: %v", time.Since(opened), err)
		}
		if kind == h2FrameGoAway {
			t.Fatalf("GOAWAY %s after the stalled header block, while a stream was open", time.Since(opened))
		}
		if kind == h2FrameData && stream == 1 && time.Since(opened) > testIdleTimeout+time.Second {
			late++
		}
	}
	if late == 0 {
		t.Fatal("the open stream sent nothing after the idle bound's grace: the window proves nothing")
	}

	released := time.Now()
	close(release)
	requireClosedByBound(t, conn, reader, released, testIdleTimeout)
}

func TestAPIListener_LongLivedStreamsOutliveItsBounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A live run whose events reach every stream from the in-process bus.
	const runID = "run-long"
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_RUNNING})
	publish := func(eventType orkstackv1.EventType, taskID string) {
		bus.Publish(&orkstackv1.Event{
			RunId: runID, Type: eventType, TaskId: taskID,
			Status: orkstackv1.Status_STATUS_RUNNING, Timestamp: timestamppb.Now(),
		}, "")
	}
	publish(orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, "")
	eventsAddr := serveBounded(t, api.NewMux(api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Runs: runs, Bus: bus, Starter: &fakeStarter{},
	}), api.MuxOptions{}))

	// A running task whose log chunks an Orker writes to the bucket.
	logRun := runningRun("run-long-logs", "build")
	logs := &memoryLogStore{}
	ref := artifacts.LogRef{RunID: logRun.GetId(), TaskID: "build", Attempt: 1}
	logs.write(ref, 1, "before\n")
	logsAddr := serveBounded(t, api.NewMux(api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Reader: &fakeReader{
			runs:   map[string]*orkstackv1.Run{logRun.GetId(): logRun},
			events: map[string][]*orkstackv1.Event{logRun.GetId(): {}},
		},
		Runs: engine.NewRunStore(), Bus: engine.NewEventBus(), Starter: &fakeStarter{},
		Logs:    logs,
		LogPoll: api.LogPollOptions{Min: 5 * time.Millisecond, Max: 20 * time.Millisecond},
	}), api.MuxOptions{}))

	h2c := &http.Transport{Protocols: new(http.Protocols)}
	h2c.Protocols.SetUnencryptedHTTP2(true)
	http1 := &http.Transport{}
	t.Cleanup(func() {
		h2c.CloseIdleConnections()
		http1.CloseIdleConnections()
	})
	h2cClient, http1Client := &http.Client{Transport: h2c}, &http.Client{Transport: http1}

	eventStream := func(client *http.Client, options ...connect.ClientOption) func() (string, error) {
		stream, err := orkstackv1connect.NewRunServiceClient(client, "http://"+eventsAddr, options...).
			StreamEvents(ctx, connect.NewRequest(&orkstackv1.StreamEventsRequest{RunId: runID}))
		if err != nil {
			t.Fatalf("StreamEvents: %v", err)
		}
		t.Cleanup(func() { _ = stream.Close() })
		return func() (string, error) {
			if !stream.Receive() {
				return "", fmt.Errorf("the stream ended: %v", stream.Err())
			}
			return stream.Msg().GetType().String(), nil
		}
	}
	logStream := func(client *http.Client) func() (string, error) {
		stream, err := orkstackv1connect.NewRunServiceClient(client, "http://"+logsAddr).StreamTaskLogs(ctx,
			connect.NewRequest(&orkstackv1.StreamTaskLogsRequest{RunId: logRun.GetId(), TaskId: "build", Follow: true}))
		if err != nil {
			t.Fatalf("StreamTaskLogs: %v", err)
		}
		t.Cleanup(func() { _ = stream.Close() })
		return func() (string, error) {
			for stream.Receive() {
				if data := stream.Msg().GetData(); len(data) > 0 {
					return string(data), nil
				}
			}
			return "", fmt.Errorf("the stream ended: %v", stream.Err())
		}
	}
	socket := func(url string) *websocket.Conn {
		conn, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatalf("dial %s: %v", url, err)
		}
		t.Cleanup(func() { _ = conn.CloseNow() })
		return conn
	}
	eventSocket := func(url string) func() (string, error) {
		conn := socket(url)
		return func() (string, error) {
			_, frame, err := conn.Read(ctx)
			return string(frame), err
		}
	}
	logSocket := func(url string) func() (string, error) {
		conn := socket(url)
		return func() (string, error) {
			for {
				_, frame, err := conn.Read(ctx)
				if err != nil {
					return "", err
				}
				chunk := &orkstackv1.TaskLogChunk{}
				if err := api.UnmarshalJSON(frame, chunk); err != nil {
					return "", fmt.Errorf("decode %q: %w", frame, err)
				}
				if data := chunk.GetData(); len(data) > 0 {
					return string(data), nil
				}
			}
		}
	}

	streams := []longStream{
		{"run events, Connect over h2c (proto)", eventStream(h2cClient)},
		{"run events, Connect over HTTP/1.1 (JSON)", eventStream(http1Client, connect.WithProtoJSON())},
		{"run events, WebSocket", eventSocket("ws://" + eventsAddr + "/ws/runs/" + runID)},
		{"task log, Connect over h2c", logStream(h2cClient)},
		{"task log, Connect over HTTP/1.1", logStream(http1Client)},
		{"task log, WebSocket", logSocket("ws://" + logsAddr + "/ws/runs/" + logRun.GetId() + "/tasks/build/logs?follow=true")},
	}
	opened := time.Now()
	for _, s := range streams {
		got, err := s.next()
		if err != nil {
			t.Fatalf("%s: first message: %v", s.name, err)
		}
		if !strings.Contains(got, "RUN_STARTED") && !strings.Contains(got, "before") {
			t.Fatalf("%s: first message %q", s.name, got)
		}
	}

	// The silence. It lasts outlast, and it is held by watching the bounds
	// act rather than by a sleep: on each server, a keep-alive connection
	// and a request whose headers never end are opened and closed by the
	// bounds, round after round, until the silence has outlasted them. Every
	// moment the streams must survive is one in which the listener was seen
	// enforcing its bounds, so a slow host cannot make the test pass.
	rounds := 0
	for deadline := opened.Add(outlast); rounds == 0 || time.Now().Before(deadline); rounds++ {
		for _, addr := range []string{eventsAddr, logsAddr} {
			slow, slowSince := openSlowHeader(t, addr)
			idle, idleReader, idleSince := openIdleConnection(t, addr, "/ws/")
			requireClosedByBound(t, slow, slow, slowSince, testHeaderTimeout)
			requireClosedByBound(t, idle, idleReader, idleSince, testIdleTimeout)
		}
	}
	t.Logf("the bounds closed %d rounds of connections during %s of silence", rounds, time.Since(opened).Round(time.Millisecond))

	publish(orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, "fetch")
	logs.write(ref, 2, "after\n")
	for _, s := range streams {
		got, err := s.next()
		if err != nil {
			t.Fatalf("%s: after %s of silence: %v", s.name, time.Since(opened).Round(time.Millisecond), err)
		}
		if !strings.Contains(got, "TASK_SCHEDULED") && !strings.Contains(got, "after") {
			t.Fatalf("%s: after the silence got %q", s.name, got)
		}
	}
}
