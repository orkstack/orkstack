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
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func TestIntegration_ReplacementOrkerReapsLabelledOrphan(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "docker CLI unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// Unique to this process: an identity is host-wide, and a second process
	// running this test would otherwise boot into the first one's lock.
	id := proof.OrkerID("phase2-reaper-" + strings.ReplaceAll(t.Name(), "/", "-"))
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
	workRoot, err := os.MkdirTemp("../../.data/work", proof.WorkRootPattern("phase2-reap"))
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

// TestIntegration_ReplacementOrkerReapsUnderTheDefaultIdentity is the same
// guarantee through the path production actually takes.
//
// The test above injects OrkerID, so it proves the label-matching MECHANISM and
// never exercises the default. That matters: a default that embedded
// os.Getpid() would mint a new identity on every real restart and reap
// nothing, while this suite stayed green. Here nothing is injected — the identity is
// whatever an Orker started with no ORK_ORKER_ID gets, exactly as
// scripts/stack.sh and cmd/orker produce it.
func TestIntegration_ReplacementOrkerReapsUnderTheDefaultIdentity(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "docker CLI unavailable: %v", err)
	}
	t.Setenv(orker.OrkerIDVar, "") // no injected identity, no inherited one
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	workRoot, err := os.MkdirTemp("../../.data/work", proof.WorkRootPattern("phase2-default-reap"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workRoot) })

	newLife := func() *orker.Activities {
		store, err := artifacts.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return orker.NewActivitiesWithOptions(store, orker.Options{
			Runtime: orker.NewDockerRuntime(), WorkRoot: workRoot,
		})
	}

	// Life one: boots, claims its identity, then "crashes" — Close releases the
	// claim the way process death would, but leaves the container running,
	// which is what an Orker crash really does (§14.17).
	first := newLife()
	if err := first.Boot(ctx); err != nil {
		infraAbsent(t, "first Boot: %v", err)
	}
	id := first.Dispatcher().OrkerID
	first.Close()

	out, err := exec.CommandContext(ctx, "docker", "run", "-d",
		"--label", "ork.orker="+id, "--label", "ork.run=phase2-default-orphan",
		"alpine:3.20", "sleep", "300").CombinedOutput()
	if err != nil {
		infraAbsent(t, "create orphan container: %v: %s", err, out)
	}
	container := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })

	// Life two: a replacement process, same host, same work root, no injected
	// identity. It must recognise and remove its predecessor's container.
	second := newLife()
	if got := second.Dispatcher().OrkerID; got != id {
		t.Fatalf("the default identity changed across a restart: %q then %q", id, got)
	}
	if err := second.Boot(ctx); err != nil {
		t.Fatalf("replacement Boot: %v", err)
	}
	t.Cleanup(second.Close)

	remaining, err := exec.CommandContext(ctx, "docker", "ps", "-aq",
		"--filter", "label=ork.orker="+id).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(remaining)) != "" {
		t.Fatalf("the predecessor's container survived a default-identity boot: %s", remaining)
	}
}
