//go:build integration

package orker_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// pinInstanceID is the shape the engine mints through workflow.SideEffect and
// the Orker validates before it becomes a path segment: a lowercase uuid.
const pinInstanceID = "7c1d4e2f-9a03-4b58-8e61-5d2f0a9b3c47"

// TestIntegration_TwoPinnedContainersShareOneDirectory is the load-bearing
// proof of the whole feature's promise: two REAL containers, on one Orker, in
// one pin, passing a file through /orkstack/shared.
//
// It is deliberately not a mock and not a path comparison. The unit test
// TestExecute_TwoPinnedAttemptsShareOneSharedDirectory already proves the Go
// side hands both attempts the same host path; only a real bind mount proves
// that the second container can actually READ what the first one WROTE, which
// is the sentence the feature exists to be able to say.
//
// The two tasks are dispatched sequentially through the full execute() path —
// validate, workspace, acquire pin, stage, pull, render, seal, run, validate,
// commit — so the per-attempt tree is built and destroyed twice around a
// directory that survives both.
func TestIntegration_TwoPinnedContainersShareOneDirectory(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	const runID = "ork-pin-share-1"
	const want = "written by clone, read by compile"

	// The writer. It creates a subdirectory and a file under /orkstack/shared,
	// which is the shape a real pin uses (clone into shared/src, build out of
	// it) rather than a bare file at the root.
	writer := baseInput(t, "clone", "/bin/sh", "-c",
		`set -e; mkdir -p /orkstack/shared/src; `+
			`printf '`+want+`' > /orkstack/shared/src/greeting.txt; `+
			`printf '{}' > /orkstack/output/values.json`)
	writer.RunId = runID
	writer.IdempotencyKey = "default/" + runID + "/clone"
	writer.Pin = &orkstackv1.PinContext{
		Name: "build", InstanceId: pinInstanceID, TimeoutSeconds: 600,
	}
	// Every task of a pin runs as the Orker's own user, which is what
	// docs/DSL.md recommends across a pin (rule 11's warning) and what keeps
	// the sticky-bit uid clash out of the measurement: two tasks
	// running as different uids in one 1777 directory is the author's problem
	// to avoid, not the platform's to prove.
	writer.RunAsOrkerUser = true

	// The reader. It fails if the file is not there, so a broken mount is a red
	// test rather than an empty assertion — and it copies what it read into
	// values.json so the assertion travels back through the ordinary result
	// path, proving the bytes made it all the way across.
	reader := baseInput(t, "compile", "/bin/sh", "-c",
		`set -e; test -f /orkstack/shared/src/greeting.txt; `+
			`printf '{"seen":"%s"}' "$(cat /orkstack/shared/src/greeting.txt)" > /orkstack/output/values.json`)
	reader.RunId = runID
	reader.IdempotencyKey = "default/" + runID + "/compile"
	reader.Pin = &orkstackv1.PinContext{
		Name: "build", InstanceId: pinInstanceID, TimeoutSeconds: 600,
	}
	reader.RunAsOrkerUser = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Whatever happens below, the pin's directory must not be left on this
	// host's disk: the run is over the moment this test is.
	t.Cleanup(func() {
		if _, err := orker.ReleasePinDirectory(
			dispatcher.WorkRoot, runID, "build", pinInstanceID); err != nil {
			t.Errorf("cleanup: ReleasePinDirectory: %v", err)
		}
	})

	if _, err := dispatcher.Execute(ctx, writer); err != nil {
		t.Fatalf("the writing task failed: %v", err)
	}
	result, err := dispatcher.Execute(ctx, reader)
	if err != nil {
		t.Fatalf("the reading task failed, so the shared mount did not carry the file: %v", err)
	}

	if got := string(result.GetValuesJson()); !strings.Contains(got, want) {
		t.Errorf("the second container's values.json = %s, want it to carry %q read back out "+
			"of /orkstack/shared", got, want)
	}

	// The directory itself, on the host, with the file still in it: the pin's
	// lifetime is neither attempt's.
	pinRoot := filepath.Join(dispatcher.WorkRoot, "ork-pin-"+runID+"-build-"+pinInstanceID)
	shared := filepath.Join(pinRoot, "shared", "src", "greeting.txt")
	content, err := os.ReadFile(shared)
	if err != nil {
		t.Fatalf("read %s on the host: %v", shared, err)
	}
	if string(content) != want {
		t.Errorf("the host copy reads %q, want %q", content, want)
	}
	// And the lease beside it, at 0600, which is what the sweep reads.
	info, err := os.Stat(filepath.Join(pinRoot, "lease.json"))
	if err != nil {
		t.Fatalf("stat lease.json: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("lease.json is %04o, want 0600", got)
	}

	// ReleasePin removes it, which is the normal end of a pin
	// (docs/OCI_ACTIVITY_CONTRACT.md §6.4) and
	// is asserted here rather than left to the cleanup above so a failure names
	// the release rather than a leaked directory.
	removed, err := orker.ReleasePinDirectory(dispatcher.WorkRoot, runID, "build", pinInstanceID)
	if err != nil {
		t.Fatalf("ReleasePinDirectory: %v", err)
	}
	if removed != pinRoot {
		t.Errorf("released %q, want %q", removed, pinRoot)
	}
	if _, err := os.Stat(pinRoot); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the pin directory survived its release: %v", err)
	}
}

// TestIntegration_AReplacementOrkerReapsAPinItsPredecessorLeft is the crash
// half of the boot reap (docs/OCI_ACTIVITY_CONTRACT.md §6.4), driven the way
// orphan_reap_integration_test.go drives the container half: one Activities
// "dies" (Close releases its identity claim, exactly as process death would)
// leaving real state behind, and a replacement pointed at the same work root
// must recognise and remove it.
//
// It simulates the restart rather than SIGKILLing a real `orker` binary. That
// is not a shortcut around the interesting part: the reap does not read a
// lease, a pid, a label or a clock — it removes every ork-pin-* it finds,
// because a fresh session-resource id is minted per worker start and therefore
// no pin can survive the process that opened it. What the reap has to get right
// is that it runs at all, that it finds the directory, and that it can remove a
// tree a workload made unremovable. All three are exercised here; killing a
// real process would only change how the directory came to be orphaned, and a
// Go test cannot drive a real `orker` binary's lifecycle without a cluster,
// a queue and a workflow to pin anything to.
//
// The 0500 subdirectory is the part a plain os.RemoveAll gets wrong, and it is
// written by a REAL container running as a real uid — which is how it happens
// in production and is what makes this an integration test rather than a
// t.TempDir exercise.
func TestIntegration_AReplacementOrkerReapsAPinItsPredecessorLeft(t *testing.T) {
	requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const runID = "ork-pin-reap-1"
	dispatcher, _ := newDispatcher(t)
	workRoot := dispatcher.WorkRoot
	pinRoot := filepath.Join(workRoot, "ork-pin-"+runID+"-build-"+pinInstanceID)
	t.Cleanup(func() { _, _ = orker.ReleasePinDirectory(workRoot, runID, "build", pinInstanceID) })

	// A real pinned task, which is what leaves the directory behind — including
	// a subdirectory whose mode no removal can traverse.
	in := baseInput(t, "clone", "/bin/sh", "-c",
		`set -e; mkdir -p /orkstack/shared/sealed; `+
			`printf 'left by a dead orker' > /orkstack/shared/sealed/build.log; `+
			`chmod 0500 /orkstack/shared/sealed; `+
			`printf '{}' > /orkstack/output/values.json`)
	in.RunId = runID
	in.IdempotencyKey = "default/" + runID + "/clone"
	in.Pin = &orkstackv1.PinContext{Name: "build", InstanceId: pinInstanceID, TimeoutSeconds: 600}
	in.RunAsOrkerUser = true

	if _, err := dispatcher.Execute(ctx, in); err != nil {
		t.Fatalf("the pinned task failed: %v", err)
	}
	if _, err := os.Stat(pinRoot); err != nil {
		t.Fatalf("the pinned task left no pin directory to reap: %v", err)
	}

	// The replacement Orker: a new process, same host, same work root. Its boot
	// reap must remove what its predecessor left, whether or not it hosts pins
	// itself — so this one deliberately does NOT (MaxPins is unset), which is
	// the case a max_pins-gated reap would get wrong.
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	replacement := orker.NewActivitiesWithOptions(store, orker.Options{
		Runtime:  orker.NewDockerRuntime(),
		WorkRoot: workRoot,
		OrkerID:  "orker-pin-reap-replacement",
	})
	if err := replacement.Boot(ctx); err != nil {
		t.Fatalf("replacement Boot: %v", err)
	}
	t.Cleanup(replacement.Close)

	if _, err := os.Stat(pinRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a dead predecessor's pin directory survived a replacement's boot: %v", err)
	}
}
