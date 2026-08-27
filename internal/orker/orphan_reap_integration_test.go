//go:build integration

package orker_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestIntegration_ReplacementOrkerReapsLabelledOrphan(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "docker CLI unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := "phase2-reaper-" + strings.ReplaceAll(t.Name(), "/", "-")
	cmd := exec.CommandContext(ctx, "docker", "run", "-d", "--label", "ork.orker="+id, "--label", "ork.run=phase2-orphan", "alpine:3.20", "sleep", "300")
	out, err := cmd.CombinedOutput()
	if err != nil {
		infraAbsent(t, "create orphan container: %v: %s", err, out)
	}
	container := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	workRoot, err := os.MkdirTemp("../../.data/work", "phase2-reap-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workRoot) })
	activities := orker.NewActivitiesWithOptions(store, orker.Options{Runtime: orker.NewDockerRuntime(), OrkerID: id, WorkRoot: workRoot})
	if err := activities.Boot(ctx); err != nil {
		t.Fatalf("replacement Boot: %v", err)
	}
	check := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "label=ork.orker="+id)
	remaining, err := check.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(remaining)) != "" {
		t.Fatalf("orphan survives replacement boot: %s", remaining)
	}
}
