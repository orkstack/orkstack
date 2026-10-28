package cosigntest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// composeFile is the development stack's compose file, relative to the
// module root. Its `registry` service is the one image pin this package runs.
const composeFile = "deploy/docker-compose.yml"

// digestPinned is a reference proof-fetch would pull: a name, optionally a
// tag, and a lower-case sha256 digest. An empty digest, a short one and an
// interpolated name (`$`) do not match.
var digestPinned = regexp.MustCompile(`^[^\s@$]+@sha256:[0-9a-f]{64}$`)

// PrivateRegistry is a registry container (the image deploy/docker-compose.yml
// pins for the stack's own `registry`) a test starts for itself, on a
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

// RegistryImage is the registry image deploy/docker-compose.yml pins for the
// stack's `registry`, exactly as written there (`registry:3@sha256:...`). The
// compose file is found by walking up from the working directory to go.mod.
//
// It is the one reference this package runs. The stack's images are pulled by
// digest (the CI proof host's proof-fetch, `make up`), and Docker drops the
// tag when it pulls a tag@digest reference, so a host that has the stack has
// no local `registry:3`; running the floating tag would pull from Docker Hub
// at test time, outside every setup retry and bound.
func RegistryImage() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("cosigntest: working directory: %w", err)
	}
	path, err := composePath(wd)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("cosigntest: read %s: %w", composeFile, err)
	}
	return registryImageFrom(data)
}

// composePath is the compose file of the module that holds start.
func composePath(start string) (string, error) {
	abs, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("cosigntest: resolve %s: %w", start, err)
	}
	for dir := abs; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, filepath.FromSlash(composeFile)), nil
		}
		if filepath.Dir(dir) == dir {
			return "", fmt.Errorf("cosigntest: no go.mod above %s, so no %s", abs, composeFile)
		}
	}
}

// registryImageFrom reads the `registry` service's image out of a compose
// file and refuses anything proof-fetch would not pull: a floating tag, a bare
// name, an interpolated reference.
func registryImageFrom(data []byte) (string, error) {
	var doc struct {
		Services map[string]struct {
			Image string `yaml:"image"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return "", fmt.Errorf("cosigntest: parse %s: %w", composeFile, err)
	}
	svc, ok := doc.Services["registry"]
	if !ok {
		return "", fmt.Errorf("cosigntest: %s has no registry service", composeFile)
	}
	if svc.Image == "" {
		return "", fmt.Errorf("cosigntest: the registry service of %s names no image", composeFile)
	}
	if !digestPinned.MatchString(svc.Image) {
		return "", fmt.Errorf("cosigntest: the registry image %q of %s is not pinned by digest", svc.Image, composeFile)
	}
	return svc.Image, nil
}

// StartRegistry starts a private registry from the image deploy/docker-compose.yml
// pins (RegistryImage), waits for it to answer, and removes it at test end.
// It never pulls: it calls skip (the suite's infraAbsent) when Docker is
// unavailable or the pinned image is not on the host, which `make up` and the
// CI proof host's setup put there. A compose file it cannot read, or whose pin
// is not a digest, is a defect of the repository and fails the test.
func StartRegistry(t testing.TB, skip func(format string, args ...any)) *PrivateRegistry {
	t.Helper()
	image, err := RegistryImage()
	if err != nil {
		t.Fatalf("resolve the registry image: %v", err)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		skip("the docker CLI is unavailable, so no private registry can start: %v", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", image).CombinedOutput(); err != nil {
		skip("the registry image %s is not on this host (or the docker daemon is unreachable): %v: %s; "+
			"`make up` pulls it, and the CI proof host's setup (proof-fetch) pulls it before any suite runs",
			image, err, strings.TrimSpace(string(out)))
		return nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a registry port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	out, err := exec.CommandContext(ctx, "docker", registryRunArgs(image, port)...).CombinedOutput()
	if err != nil {
		skip("cannot start a private registry from %s: %v: %s", image, err, out)
		return nil
	}
	r := &PrivateRegistry{Host: fmt.Sprintf("localhost:%d", port), id: strings.TrimSpace(string(out))}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", r.id).Run()
	})
	r.WaitReady(t)
	return r
}

// registryRunArgs is the `docker` command line that starts the registry: from
// the pinned image, never pulling it, published on loopback only, with
// deletion enabled.
func registryRunArgs(image string, port int) []string {
	return []string{"run", "-d", "--pull", "never",
		"-p", fmt.Sprintf("127.0.0.1:%d:5000", port),
		"-e", "REGISTRY_STORAGE_DELETE_ENABLED=true",
		"--label", "orkstack.test=image-policy",
		image}
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
