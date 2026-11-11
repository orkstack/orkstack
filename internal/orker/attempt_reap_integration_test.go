//go:build integration

package orker_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// These tests are about what a killed Orker leaves on the host's disk, with a
// real container and a real SIGKILL: its attempt's directory, which holds the
// secret values staged for it (docs/DEPLOY.md, "What removes a task's
// container").

// stagedSecretVar is the source of the secret the attempts below stage.
const stagedSecretVar = "ATTEMPT_DIRS_TEST_STAGED_SECRET"

// secretInput is removalInput with a secret staged from the environment the
// held Orker inherits.
func secretInput(t *testing.T, name, value string, command ...string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	t.Setenv(stagedSecretVar, value)
	in := removalInput(t, name, command...)
	in.Secrets = []*orkstackv1.SecretRef{{Name: "db", Source: "env:" + stagedSecretVar}}
	return in
}

// attemptDirectoryOf is the one directory an attempt of run has under workRoot,
// and fails unless there is exactly one.
func attemptDirectoryOf(t *testing.T, workRoot, run string) string {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(workRoot, "ork-"+run+"-*"))
	must.OK(t, err)
	if len(dirs) != 1 {
		t.Fatalf("attempt directories of %s under %s: %v, want one", run, workRoot, dirs)
	}
	return dirs[0]
}

// requireStagedSecret holds an attempt's directory to still having its secret
// on disk, in the clear, which is the leak these tests are about.
func requireStagedSecret(t *testing.T, dir, value string) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, "secrets", "db"))
	must.OKf(t, err, "the attempt's staged secret")
	if string(got) != value {
		t.Fatalf("the attempt's staged secret reads %q, want %q", got, value)
	}
}

// bootLogged is removalLife with the boot's log kept.
func bootLogged(ctx context.Context, t *testing.T, orkerID, workRoot string) (*orker.Activities, *bytes.Buffer) {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OK(t, err)
	t.Cleanup(func() { _ = store.Close() })
	var logged bytes.Buffer
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Runtime: orker.NewDockerRuntime(), OrkerID: orkerID, WorkRoot: workRoot,
		Commits: artifacts.NewMemoryCommitStore(), Refs: artifacts.MemoryRefPredictor{},
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	})
	orker.SetRemovalRetryEvery(activities, time.Hour)
	must.OKf(t, activities.Boot(ctx), "Boot")
	t.Cleanup(activities.Close)
	return activities, &logged
}

// TestIntegration_AttemptDirectories_AKilledOrkersSecretsGoWithItsReplacementsBoot
// is the finding: an Orker killed during a task leaves the attempt's directory,
// the staged secret in it in the clear, and only a replacement under the same
// identity can say the directory is its own.
func TestIntegration_AttemptDirectories_AKilledOrkersSecretsGoWithItsReplacementsBoot(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	id := proof.OrkerID("attempt-dirs-killed")
	workRoot := removalWorkRoot(t, "attempt-dirs-killed")
	const secret = "a-value-staged-for-one-attempt"
	in := secretInput(t, "dirs-killed", secret, "/bin/sh", "-c", `while :; do echo tick; sleep 1; done`)
	_, kill := startHeldOrker(ctx, t, id, workRoot, in)
	kill()

	dir := attemptDirectoryOf(t, workRoot, in.GetRunId())
	requireStagedSecret(t, dir, secret)

	_, logged := bootLogged(ctx, t, id, workRoot)
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Fatalf("the killed attempt's directory survived its replacement's boot: %v\n%s", err, logged)
	}
	if left := containersLabelled(ctx, t, "ork.run="+in.GetRunId()); len(left) != 0 {
		t.Errorf("the killed attempt's containers survived its replacement's boot: %v", left)
	}
}

// TestIntegration_AttemptDirectories_ALiveOrkerOnTheSameWorkRootKeepsItsOwn is
// what identity makes safe, on the one shape where it has to: two Orkers on one
// work root, which only explicit identities allow. Each boot removes only its
// own identity's directories, and the live Orker's workload goes on reading its
// secret past the other's boot. What makes the window mean something: the same
// boot did remove its own predecessor's directory, and the live workload's own
// ticks say it was running throughout.
func TestIntegration_AttemptDirectories_ALiveOrkerOnTheSameWorkRootKeepsItsOwn(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	workRoot := removalWorkRoot(t, "attempt-dirs-shared")
	const liveSecret = "the-live-orkers-secret"
	live := secretInput(t, "dirs-live", liveSecret, "/bin/sh", "-c",
		`while :; do cat /orkstack/input/secrets/db >/dev/null && echo tick; sleep 1; done`)
	// Past this test's own deadline: a task that timed out would have its Orker
	// remove the directory itself, which says nothing about the other's boot.
	live.TimeoutSeconds = 3600
	liveContainer, killLive := startHeldOrker(ctx, t, proof.OrkerID("attempt-dirs-live"), workRoot, live)
	defer killLive()
	liveDir := attemptDirectoryOf(t, workRoot, live.GetRunId())

	// The other identity's previous life, killed on the same work root. Its own
	// boot, with the live Orker's attempt already running, is the first test of
	// the rule; its replacement's is the second.
	otherID := proof.OrkerID("attempt-dirs-other")
	other := removalInput(t, "dirs-other", "/bin/sh", "-c", `while :; do echo tick; sleep 1; done`)
	_, killOther := startHeldOrker(ctx, t, otherID, workRoot, other)
	killOther()
	otherDir := attemptDirectoryOf(t, workRoot, other.GetRunId())

	before := tickCount(ctx, t, liveContainer)
	_, logged := bootLogged(ctx, t, otherID, workRoot)

	if _, err := os.Lstat(otherDir); !os.IsNotExist(err) {
		t.Fatalf("the boot left its own identity's directory, so it proves nothing about the other: %v\n%s", err, logged)
	}
	if got := containerState(ctx, t, liveContainer); got != "running" {
		t.Fatalf("the live Orker's container is %q after another identity's boot", got)
	}
	requireStagedSecret(t, liveDir, liveSecret)
	// Still reading its secret after the boot. A workload that lost it prints
	// no tick.
	awaitTicks(ctx, t, liveContainer, before+3)
	requireStagedSecret(t, liveDir, liveSecret)
	if !strings.Contains(logged.String(), liveDir) {
		t.Errorf("the boot did not name the directory it left:\n%s", logged)
	}
}

// tickCount is how many ticks a container has printed so far.
func tickCount(ctx context.Context, t *testing.T, id string) int {
	t.Helper()
	out, err := exec.CommandContext(ctx, containerCLI(t), "logs", id).CombinedOutput()
	if err != nil {
		t.Fatalf("read the logs of container %s: %v: %s", id, err, out)
	}
	return strings.Count(string(out), "tick")
}

// TestIntegration_AttemptDirectories_WhatTheOrkerCannotRemoveDoesNotKeepTheSecrets
// is a killed attempt whose workload ran as root and left a directory a
// non-root Orker cannot empty. The rest goes, the secret first; the boot says
// what stayed and goes on.
func TestIntegration_AttemptDirectories_WhatTheOrkerCannotRemoveDoesNotKeepTheSecrets(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	id := proof.OrkerID("attempt-dirs-held")
	workRoot := removalWorkRoot(t, "attempt-dirs-held")
	const secret = "a-value-under-a-root-owned-sibling"
	in := secretInput(t, "dirs-held", secret, "/bin/sh", "-c",
		`mkdir /orkstack/output/held && touch /orkstack/output/held/file; while :; do echo tick; sleep 1; done`)
	container, kill := startHeldOrker(ctx, t, id, workRoot, in)
	awaitTicks(ctx, t, container, 1)
	kill()

	dir := attemptDirectoryOf(t, workRoot, in.GetRunId())
	requireStagedSecret(t, dir, secret)
	// Registered after removalWorkRoot's own cleanup, so it runs before: a
	// container of the same uid empties what the test's own removal could not.
	t.Cleanup(func() {
		cli := containerCLI(t)
		out, err := exec.Command(cli, "run", "--rm", "--volume", workRoot+":/w", orker.BindProbeImage,
			"rm", "-rf", "/w/"+filepath.Base(dir)).CombinedOutput()
		if err != nil {
			t.Logf("empty the held directory: %v: %s", err, out)
		}
	})
	info, err := os.Stat(filepath.Join(dir, "output", "held", "file"))
	must.OKf(t, err, "the workload's file")

	_, logged := bootLogged(ctx, t, id, workRoot)

	if _, err := os.Lstat(filepath.Join(dir, "secrets")); !os.IsNotExist(err) {
		t.Fatalf("the secrets survived what the Orker could not remove: %v\n%s", err, logged)
	}
	if _, err := os.Lstat(filepath.Join(dir, "input")); !os.IsNotExist(err) {
		t.Errorf("the sealed input survived what the Orker could not remove: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "output", "held", "file")); err == nil {
		// Only where the workload's root is another user on the host. Where it
		// is the Orker (an engine running rootless, an Orker running as root),
		// nothing stood in the way and the branch below holds instead.
		if !strings.Contains(logged.String(), dir) {
			t.Errorf("the boot did not say what stayed:\n%s", logged)
		}
		t.Logf("observed: the workload's file stayed, and the secrets did not")
		return
	}
	t.Logf("observed: the workload's file (owner %v) was removable here, so nothing stayed", info.Sys())
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Errorf("the directory stayed with nothing holding it: %v", err)
	}
}
