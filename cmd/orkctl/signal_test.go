package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// keepAliveOnInterrupt registers the test's own SIGINT receiver for its
// duration, so a SIGINT this test sends itself can never kill the test
// binary — not even before orkctl has registered for it.
func keepAliveOnInterrupt(t *testing.T) {
	t.Helper()
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, os.Interrupt)
	t.Cleanup(func() { signal.Stop(ch) })
}

func interruptSelf(t *testing.T) {
	t.Helper()
	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Errorf("send SIGINT: %v", err)
	}
}

// TestInterrupt_JSONWatchStaysParseableAndExits130: Ctrl-C during `run
// watch --json` still closes the JSON document — stdout parses — and the
// command exits 130 with "Error: interrupted".
func TestInterrupt_JSONWatchStaysParseableAndExits130(t *testing.T) {
	keepAliveOnInterrupt(t)
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork").requireExit(t, exitOK)

	out := &liveWriter{triggers: map[string]func(){}}
	// The first event is on stdout: the watch is streaming, and orkctl's
	// signal handling is in place.
	out.triggers["RUN_CREATED"] = func() { interruptSelf(t) }

	done := make(chan result, 1)
	go func() { done <- f.runTo(out, "--json", "run", "watch", "run-001") }()
	var outcome result
	select {
	case outcome = <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("run watch did not stop on SIGINT; output so far:\n%s", out.buf.String())
	}
	outcome.requireExit(t, exitInterrupted)
	if !strings.Contains(outcome.stderr, "Error: interrupted") {
		t.Fatalf("stderr = %q, want Error: interrupted", outcome.stderr)
	}
	var doc struct {
		Events []map[string]any `json:"events"`
		Run    any              `json:"run"`
	}
	if err := json.Unmarshal(out.buf.Bytes(), &doc); err != nil {
		t.Fatalf("interrupted --json watch is not valid JSON: %v\n%s", err, out.buf.String())
	}
	if len(doc.Events) != 1 {
		t.Fatalf("events = %d, want the one printed before the interrupt\n%s", len(doc.Events), out.buf.String())
	}
}

// stallingArtifacts sends one chunk of an artifact, announces it, and then
// stalls until the client goes away.
type stallingArtifacts struct {
	orkstackv1connect.UnimplementedRunServiceHandler
	sent chan struct{}
	// release ends the stall when the test is over, so a server that was
	// never disconnected cannot hang its cleanup.
	release chan struct{}
}

func (s *stallingArtifacts) ReadArtifact(
	ctx context.Context,
	_ *connect.Request[orkstackv1.ReadArtifactRequest],
	stream *connect.ServerStream[orkstackv1.ArtifactChunk],
) error {
	if err := stream.Send(&orkstackv1.ArtifactChunk{Data: []byte("partial bytes")}); err != nil {
		return err
	}
	close(s.sent)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return connect.NewError(connect.CodeUnavailable, context.Canceled)
	}
}

// TestInterrupt_ArtifactDownloadLeavesNoTemporaryFile: Ctrl-C during `run
// artifact download` removes the partial temporary file and creates no
// output, and the command exits 130.
func TestInterrupt_ArtifactDownloadLeavesNoTemporaryFile(t *testing.T) {
	keepAliveOnInterrupt(t)
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	handler := &stallingArtifacts{sent: make(chan struct{}), release: make(chan struct{})}
	path, h := orkstackv1connect.NewRunServiceHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(handler.release) })

	dir := t.TempDir()
	output := filepath.Join(dir, "report.bin")
	done := make(chan result, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		code := Execute([]string{"run", "artifact", "download", "run-1", "--task", "build", "--kind", "output",
			"--name", "report.bin", "--output", output, "--server", server.URL}, &stdout, &stderr)
		done <- result{stdout: stdout.String(), stderr: stderr.String(), code: code}
	}()
	select {
	case <-handler.sent:
	case <-time.After(15 * time.Second):
		t.Fatal("the download never started")
	}
	interruptSelf(t)
	var outcome result
	select {
	case outcome = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("run artifact download did not stop on SIGINT")
	}
	outcome.requireExit(t, exitInterrupted)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read output dir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("an interrupted download left %v behind", names)
	}
}
