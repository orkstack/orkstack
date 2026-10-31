//go:build integration

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// listenerNamespace is a Temporal namespace of this test's own.
func listenerNamespace(label string) string {
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	return fmt.Sprintf("%s-%s-%d-%d", base, label, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// paddedDoc is a valid Orkflow padded with a comment to padding bytes more.
func paddedDoc(padding int) string {
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: padded\ntasks:\n  - id: one\n    uses: shell\n" +
		"    with: {op: exec, command: [echo, ok]}\n# " + strings.Repeat("x", padding) + "\n"
}

// statusUnder is the status of GET path on address with the Host header host.
func statusUnder(t *testing.T, address, host, path string) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, "http://"+address+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = host
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET %s with Host %q: %v", path, host, err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return response.StatusCode
}

// TestIntegration_EngineBoundsItsAPIListener: the real ork binary takes its
// read cap, its idle timeout and its Host allowlist from
// ORK_REQUEST_MAX_BYTES, ORK_HTTP_IDLE_TIMEOUT and ORK_ALLOWED_HOSTS. A
// request over the cap is refused resource_exhausted naming it, and the next
// request on the same client is served; a keep-alive connection is closed
// once idle for the timeout; a Host that is neither one the engine always
// admits nor listed is refused 403 on every route; and a cap outside its
// bounds or an allowed host that is not a name stops the boot naming the
// variable.
func TestIntegration_EngineBoundsItsAPIListener(t *testing.T) {
	temporal := requireTemporal(t)
	_ = requireObjectStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	binary := buildOrk(t)
	const idle = 2 * time.Second

	address, orkflows, log := bootEngine(ctx, t, binary, temporal, "engine-listener",
		"ORK_TEMPORAL_NAMESPACE="+listenerNamespace("listener"),
		"ORK_REQUEST_MAX_BYTES=1048576", "ORK_HTTP_IDLE_TIMEOUT="+idle.String(),
		"ORK_ALLOWED_HOSTS=orkstack.example.com, *.corp.example")
	if !strings.Contains(log.String(), "request_max_bytes=1048576") || !strings.Contains(log.String(), "allowed_hosts") {
		t.Fatalf("the boot log does not state the listener's settings:\n%s", log.String())
	}

	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host string
		want int
	}{
		{address, http.StatusOK}, {"localhost:" + port, http.StatusOK}, {"app.localhost:" + port, http.StatusOK},
		{"ork:" + port, http.StatusOK}, {"orkstack.example.com", http.StatusOK}, {"ci.corp.example:443", http.StatusOK},
		{"evil.example:" + port, http.StatusForbidden}, {"corp.example", http.StatusForbidden},
		{"ork.orkstack.svc:" + port, http.StatusForbidden},
	} {
		for _, path := range []string{"/", "/v1/webhooks"} {
			if got := statusUnder(t, address, tc.host, path); got != tc.want {
				t.Errorf("GET %s with Host %q: %d, want %d", path, tc.host, got, tc.want)
			}
		}
	}

	_, err = orkflows.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: paddedDoc(2 << 20)}))
	if connect.CodeOf(err) != connect.CodeResourceExhausted || !strings.Contains(err.Error(), "configured max 1048576") {
		t.Fatalf("a 2 MiB request under a 1 MiB cap: %v, want resource_exhausted naming the cap", err)
	}
	if _, err := orkflows.ValidateOrkflow(ctx, connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: paddedDoc(512 << 10)})); err != nil {
		t.Fatalf("a 512 KiB request after the refusal: %v", err)
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// Taken before the request is sent, so it is never later than the
	// moment the engine starts counting the connection idle.
	idleSince := time.Now()
	if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\n\r\n", address); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, reader)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("a keep-alive connection idle for 30s was still open under ORK_HTTP_IDLE_TIMEOUT=%s", idle)
	}
	if elapsed := time.Since(idleSince); elapsed < idle {
		t.Fatalf("the idle connection closed after %s, before ORK_HTTP_IDLE_TIMEOUT=%s", elapsed, idle)
	}

	_, refused := startEngine(ctx, t, binary, temporal, "engine-listener-refused",
		"ORK_TEMPORAL_NAMESPACE="+listenerNamespace("listener-refused"), "ORK_REQUEST_MAX_BYTES=1024")
	select {
	case <-refused.done:
	case <-ctx.Done():
		t.Fatalf("an engine with ORK_REQUEST_MAX_BYTES=1024 did not stop:\n%s", refused.log.String())
	}
	if code := refused.cmd.ProcessState.ExitCode(); code == 0 || !strings.Contains(refused.log.String(), "ORK_REQUEST_MAX_BYTES=1024 must be between") {
		t.Fatalf("an engine with ORK_REQUEST_MAX_BYTES=1024 exited %d; want a refusal naming it:\n%s", code, refused.log.String())
	}

	_, refusedHosts := startEngine(ctx, t, binary, temporal, "engine-listener-hosts",
		"ORK_TEMPORAL_NAMESPACE="+listenerNamespace("listener-hosts"), "ORK_ALLOWED_HOSTS=https://orkstack.example.com")
	select {
	case <-refusedHosts.done:
	case <-ctx.Done():
		t.Fatalf("an engine with a URL in ORK_ALLOWED_HOSTS did not stop:\n%s", refusedHosts.log.String())
	}
	if code := refusedHosts.cmd.ProcessState.ExitCode(); code == 0 ||
		!strings.Contains(refusedHosts.log.String(), "ORK_ALLOWED_HOSTS entry") ||
		!strings.Contains(refusedHosts.log.String(), "https://orkstack.example.com") {
		t.Fatalf("an engine with a URL in ORK_ALLOWED_HOSTS exited %d; want a refusal naming it:\n%s", code, refusedHosts.log.String())
	}
}
