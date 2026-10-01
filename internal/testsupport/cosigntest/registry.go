package cosigntest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// PrivateRegistry is a `registry:3` container a test starts for itself, on a
// loopback port it chose, with deletion enabled so a signature can be removed
// (Client.Unsign). A private registry is what lets a suite remove a
// signature, or stop the registry, without touching the shared one every
// other suite on this host reads (deploy/docker-compose.yml's `registry`).
//
// The container is the test's own: it is stopped and removed by its ID in
// cleanup, never by a name pattern.
type PrivateRegistry struct {
	// Host is "localhost:<port>" — loopback, so both OrkStack's resolver and
	// the Docker daemon speak plain HTTP to it.
	Host string
	id   string
}

// StartRegistry starts a private registry, waits for it to answer, and
// removes it at test end. It calls skip (the suite's infraAbsent) when Docker
// or the registry:3 image is unavailable.
func StartRegistry(t testing.TB, skip func(format string, args ...any)) *PrivateRegistry {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a registry port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "run", "-d",
		"-p", fmt.Sprintf("127.0.0.1:%d:5000", port),
		"-e", "REGISTRY_STORAGE_DELETE_ENABLED=true",
		"--label", "orkstack.test=image-policy",
		"registry:3").CombinedOutput()
	if err != nil {
		skip("cannot start a private registry:3 container: %v: %s", err, out)
		return nil
	}
	r := &PrivateRegistry{Host: fmt.Sprintf("localhost:%d", port), id: strings.TrimSpace(string(out))}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", r.id).Run()
	})
	r.WaitReady(t)
	return r
}

// WaitReady waits (bounded) for /v2/ to answer.
func (r *PrivateRegistry) WaitReady(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + r.Host + "/v2/")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("private registry %s did not answer /v2/ within 60s", r.Host)
}

// Stop stops the container (the registry outage); Start brings it back on the
// same port with the same content.
func (r *PrivateRegistry) Stop(t testing.TB) {
	t.Helper()
	if out, err := exec.Command("docker", "stop", r.id).CombinedOutput(); err != nil {
		t.Fatalf("docker stop %s: %v: %s", r.id, err, out)
	}
}

// Start restarts a stopped registry and waits for it.
func (r *PrivateRegistry) Start(t testing.TB) {
	t.Helper()
	if out, err := exec.Command("docker", "start", r.id).CombinedOutput(); err != nil {
		t.Fatalf("docker start %s: %v: %s", r.id, err, out)
	}
	r.WaitReady(t)
}
