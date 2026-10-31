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

	// The silence. On each server, a keep-alive connection and a request
	// whose headers never end are closed by the bounds while it lasts.
	for _, addr := range []string{eventsAddr, logsAddr} {
		slow, slowSince := openSlowHeader(t, addr)
		idle, idleReader, idleSince := openIdleConnection(t, addr, "/ws/")
		requireClosedByBound(t, slow, slow, slowSince, testHeaderTimeout)
		requireClosedByBound(t, idle, idleReader, idleSince, testIdleTimeout)
	}
	if rest := outlast - time.Since(opened); rest > 0 {
		time.Sleep(rest)
	}

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
