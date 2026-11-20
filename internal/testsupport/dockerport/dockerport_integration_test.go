//go:build integration

package dockerport

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
)

// image is one the proof host already holds: the Orker's bind probe.
const image = orker.BindProbeImage

// docker skips, or under ORK_REQUIRE_INFRA fails, when no daemon or image is
// at hand.
func docker(t *testing.T, ctx context.Context) {
	t.Helper()
	absent := func(why string) {
		if os.Getenv("ORK_REQUIRE_INFRA") == "1" {
			t.Fatal(why)
		}
		t.Skip(why)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		absent("docker is not on PATH")
	}
	if out, err := exec.CommandContext(ctx, "docker", "image", "inspect", image).CombinedOutput(); err != nil {
		absent(fmt.Sprintf("the image %s is not present: %s", image, out))
	}
}

func run(t *testing.T, ctx context.Context, publish string) (string, []byte, error) {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--pull", "never",
		"--label", fmt.Sprintf("ork.test=dockerport-%d", os.Getpid()),
		"-p", publish, image, "sleep", "60").CombinedOutput()
	id := strings.TrimSpace(string(out))
	if err == nil {
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	}
	return id, out, err
}

// A container published with Publish is reached at the port Addr reads back,
// and that port is one Docker holds: binding it again fails.
func TestIntegration_AddrReadsThePortDockerHolds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docker(t, ctx)
	id, out, err := run(t, ctx, Publish("5000"))
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	addr, err := Addr(ctx, id, "5000")
	if err != nil {
		t.Fatal(err)
	}
	port, err := Port(ctx, id, "5000")
	if err != nil || !strings.HasSuffix(addr, fmt.Sprintf(":%d", port)) {
		t.Fatalf("Addr %s, Port %d, %v", addr, port, err)
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatalf("Addr = %s; want loopback", addr)
	}
	if ln, err := net.Listen("tcp", addr); err == nil {
		ln.Close()
		t.Fatalf("%s could be bound again: Docker does not hold the port it published", addr)
	}
}

// This host's Docker refuses a port another process holds with a message
// AddressInUse knows, which is what RunPinned retries on.
func TestIntegration_AddressInUseKnowsThisDockersRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docker(t, ctx)
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	_, out, err := run(t, ctx, held.Addr().String()+":5000")
	if err == nil {
		t.Fatalf("docker run published %s, which this test holds", held.Addr())
	}
	if !AddressInUse(out) {
		t.Fatalf("Docker refused the held port with a message AddressInUse does not know: %s", out)
	}
}
