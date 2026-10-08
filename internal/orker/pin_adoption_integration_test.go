//go:build integration

package orker_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
)

// The two pin openings this file drives. Both are the shape the engine mints
// through workflow.SideEffect and the Orker validates before it becomes a path
// segment (a lowercase uuid), and they are DIFFERENT from each other and from
// pin_workspace_integration_test.go's so that a leaked directory can never be
// mistaken for this run's.
const (
	pinAdoptInstanceID  = "b41e7d38-2c65-4f0a-9d17-6ae5c8b02f91"
	pinResumeInstanceID = "0d9a5f74-13b8-4c62-a5e0-7f3c19d84b26"
)

// TestIntegration_APinnedAttemptThatDiedAfterCommitIsAdoptedNotRerun is
// AGENTS.md's existing chaos row — "Orker termination after commit.json but
// before Temporal completion" — asked of a PINNED task, which is the one shape
// the row was never measured in.
//
// The simulation is the same one
// TestIntegration_AdoptsAnExistingCommitWithoutRunningTheContainer uses, and
// deliberately so: Execute is called TWICE with the same input, and the first
// call's return is simply not treated as authoritative. That is exactly what an
// Orker that wrote commit.json and then died looks like from the retry's side —
// the durable record exists, Temporal never heard about it, and a replacement
// attempt arrives with the same idempotency key. Nothing here needs a real
// SIGKILL, because the only state that survives a real death IS the commit and
// the pin directory, and both are real below.
//
// Two things must hold at once, and the second is what pins are about:
//
//  1. the retry ADOPTS — same values, same artifacts, same committing attempt,
//     and the container does NOT run a second time; and
//  2. the pin's /orkstack/shared survives the whole thing, because it is keyed
//     by the pin's instance id and not by the attempt (a retry stays on the
//     pin's host and sees the same directory, docs/DSL.md "Pins").
//
// The container itself is the witness for (1): it appends a line to
// shared/runs.log every time it starts, so "the container did not run again" is
// read off the pin's own disk rather than inferred from a flag the dispatcher
// set.
func TestIntegration_APinnedAttemptThatDiedAfterCommitIsAdoptedNotRerun(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	const runID = "ork-pin-adopt-1"
	const stageMarker = "clone finished, then the host died"

	in := baseInput(t, "warm", "/bin/sh", "-c",
		`set -e; mkdir -p /orkstack/shared; `+
			`echo started >> /orkstack/shared/runs.log; `+
			`printf '`+stageMarker+`' > /orkstack/shared/stage.txt; `+
			`printf '{"stage":"warm"}' > /orkstack/output/values.json; `+
			`printf 'the report' > /orkstack/output/artifacts/report.txt`)
	in.RunId = runID
	in.IdempotencyKey = "default/" + runID + "/warm"
	in.CommitUri = "mem://commits/" + runID + "/warm/commit.json"
	in.Pin = &orkstackv1.PinContext{
		Name: "build", InstanceId: pinAdoptInstanceID, TimeoutSeconds: 600,
	}
	// Every task of a pin runs as the Orker's own user, which is what
	// docs/DSL.md recommends across a pin and what keeps a uid clash in the
	// 1777 shared directory out of the measurement.
	in.RunAsOrkerUser = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pinRoot := filepath.Join(dispatcher.WorkRoot, "ork-pin-"+runID+"-build-"+pinAdoptInstanceID)
	t.Cleanup(func() {
		if _, err := orker.ReleasePinDirectory(
			dispatcher.WorkRoot, runID, "build", pinAdoptInstanceID); err != nil {
			t.Errorf("cleanup: ReleasePinDirectory: %v", err)
		}
	})

	// ── attempt 1: runs, commits, and is then treated as never reported ──
	first, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("the first pinned attempt failed: %v", err)
	}
	if first.GetCommitAdopted() {
		t.Fatal("the first attempt adopted a commit that did not exist yet")
	}
	if first.GetExitCode() != 0 {
		t.Fatalf("the first attempt exited %d, want 0", first.GetExitCode())
	}
	if got := containerStarts(t, pinRoot); got != 1 {
		t.Fatalf("the container started %d time(s) on the first attempt, want 1", got)
	}

	// ── attempt 2: the retry a real Temporal redelivery would send ──
	second, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("the retry of the pinned attempt failed: %v", err)
	}
	if !second.GetCommitAdopted() {
		t.Fatal("the retry re-ran the container instead of adopting the pinned attempt's durable commit")
	}
	// The whole result is reconstructed, not a stub: a client must not be able
	// to tell an adopted pinned attempt from a fresh one (§5.4).
	if string(second.GetValuesJson()) != string(first.GetValuesJson()) {
		t.Errorf("adopted values_json = %q, the committing attempt's was %q",
			second.GetValuesJson(), first.GetValuesJson())
	}
	if !second.GetValuesInlined() {
		t.Error("the adopted result did not inline values.json, but the fresh one did")
	}
	if second.GetValuesRef().GetUri() != first.GetValuesRef().GetUri() {
		t.Errorf("adopted values_ref = %q, want the committed %q",
			second.GetValuesRef().GetUri(), first.GetValuesRef().GetUri())
	}
	if len(second.GetArtifacts()) != len(first.GetArtifacts()) {
		t.Errorf("adopted result carries %d artifact(s), the commit recorded %d",
			len(second.GetArtifacts()), len(first.GetArtifacts()))
	}
	for i, artifact := range second.GetArtifacts() {
		if want := first.GetArtifacts()[i]; artifact.GetUri() != want.GetUri() ||
			artifact.GetSha256() != want.GetSha256() {
			t.Errorf("adopted artifact %d = %s (%s), want %s (%s)",
				i, artifact.GetName(), artifact.GetSha256(), want.GetName(), want.GetSha256())
		}
	}
	if second.GetAttempt() != first.GetAttempt() {
		t.Errorf("adopted attempt = %d, want the COMMITTING attempt %d",
			second.GetAttempt(), first.GetAttempt())
	}

	// The pin's own disk is the witness: one container start, ever.
	if got := containerStarts(t, pinRoot); got != 1 {
		t.Errorf("the container started %d time(s) across both attempts, want 1 — "+
			"adoption must not re-run a pinned container", got)
	}

	// ── the pin-specific half: /orkstack/shared outlived the attempt ──
	//
	// The shared directory is keyed by (run, pin, instance), so an
	// attempt-level retry — adopted or not — must find it exactly as the
	// interrupted attempt left it. Read from the HOST, because that is where a
	// crash would have destroyed it.
	stage, err := os.ReadFile(filepath.Join(pinRoot, "shared", "stage.txt"))
	if err != nil {
		t.Fatalf("the pin's shared directory did not survive the retry: %v", err)
	}
	if string(stage) != stageMarker {
		t.Errorf("shared/stage.txt reads %q, want %q", stage, stageMarker)
	}
	// And the lease beside it, so the sweep can still collect this directory if
	// nothing ever releases it.
	if _, err := os.Stat(filepath.Join(pinRoot, "lease.json")); err != nil {
		t.Errorf("the pin lost its lease across the retry: %v", err)
	}

	// The per-ATTEMPT trees are gone while the pin's is not. That contrast is
	// the layout makes (docs/OCI_ACTIVITY_CONTRACT.md §6.4): one directory per
	// attempt, destroyed on every
	// exit, around one directory per pin that survives them all.
	if leaked := attemptDirectories(t, dispatcher.WorkRoot, runID); len(leaked) != 0 {
		t.Errorf("per-attempt workspaces survived: %v", leaked)
	}
}

// TestIntegration_APinnedRetrySeesTheSharedDirectoryItsFailedAttemptLeft is the
// other half of the same chaos row, and the one no commit is involved in.
//
// An attempt that died BEFORE writing commit.json leaves nothing durable, so
// the retry genuinely re-runs the container — in a FRESH per-attempt workspace,
// against the SAME /orkstack/shared (docs/DSL.md "Pins").
// That is the property a pin exists for, and it is worth proving separately
// from adoption because adoption never touches the mount at all.
//
// The proof is self-verifying rather than asserted from the outside: the
// container's own command fails on its first run after leaving a file in
// /orkstack/shared, and succeeds on the second ONLY by reading that file back
// and copying its contents into values.json. A shared directory that had been
// destroyed with the failed attempt would make the second run take the first
// branch again and fail exactly as the first did.
func TestIntegration_APinnedRetrySeesTheSharedDirectoryItsFailedAttemptLeft(t *testing.T) {
	requireDocker(t)
	dispatcher, _ := newDispatcher(t)

	const runID = "ork-pin-resume-1"
	const handoff = "stage 1 got this far before the host died"

	// No commit_uri at all: this models the attempt that died BEFORE the
	// durable write, which is the case where a retry has to really re-run.
	in := baseInput(t, "resume", "/bin/sh", "-c",
		`set -e; `+
			`if [ -f /orkstack/shared/stage1.done ]; then `+
			`printf '{"resumed_from":"%s"}' "$(cat /orkstack/shared/stage1.done)" `+
			`> /orkstack/output/values.json; exit 0; fi; `+
			`mkdir -p /orkstack/shared; `+
			`printf '`+handoff+`' > /orkstack/shared/stage1.done; `+
			// Died here: the shared work is done and nothing was reported.
			`exit 3`)
	in.RunId = runID
	in.IdempotencyKey = "default/" + runID + "/resume"
	in.Pin = &orkstackv1.PinContext{
		Name: "build", InstanceId: pinResumeInstanceID, TimeoutSeconds: 600,
	}
	in.RunAsOrkerUser = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pinRoot := filepath.Join(dispatcher.WorkRoot, "ork-pin-"+runID+"-build-"+pinResumeInstanceID)
	t.Cleanup(func() {
		if _, err := orker.ReleasePinDirectory(
			dispatcher.WorkRoot, runID, "build", pinResumeInstanceID); err != nil {
			t.Errorf("cleanup: ReleasePinDirectory: %v", err)
		}
	})

	// ── attempt 1: does its shared work, then dies ──
	if _, err := dispatcher.Execute(ctx, in); err == nil {
		t.Fatal("the first attempt succeeded; it was written to exit 3 so that the retry is real")
	} else if !strings.Contains(err.Error(), orker.ReasonContainerExitNonzero) {
		t.Fatalf("the first attempt failed for the wrong reason: %v", err)
	}
	// The handoff is on the pin's disk even though the attempt failed, and the
	// failed attempt's own workspace is gone.
	if raw, err := os.ReadFile(filepath.Join(pinRoot, "shared", "stage1.done")); err != nil {
		t.Fatalf("a failed pinned attempt took the pin's shared directory with it: %v", err)
	} else if string(raw) != handoff {
		t.Fatalf("shared/stage1.done reads %q, want %q", raw, handoff)
	}
	if leaked := attemptDirectories(t, dispatcher.WorkRoot, runID); len(leaked) != 0 {
		t.Errorf("the failed attempt's workspace survived: %v", leaked)
	}

	// ── attempt 2: the retry, on the same host, into the same pin ──
	second, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("the retry failed, so it could not see what its failed attempt left in "+
			"/orkstack/shared: %v", err)
	}
	if second.GetCommitAdopted() {
		t.Error("the retry reported an adopted commit; nothing was committed before it")
	}
	// The bytes travelled container -> shared mount -> container -> values.json
	// -> ExecuteTaskResult. Only a surviving shared directory can produce this.
	if got := string(second.GetValuesJson()); !strings.Contains(got, handoff) {
		t.Errorf("the retry's values.json = %s, want it to carry %q read back out of "+
			"/orkstack/shared", got, handoff)
	}
	// A FRESH workspace, not the failed attempt's: nothing is left behind from
	// either run.
	if leaked := attemptDirectories(t, dispatcher.WorkRoot, runID); len(leaked) != 0 {
		t.Errorf("per-attempt workspaces survived the retry: %v", leaked)
	}
}

// containerStarts counts the lines the workload appended to shared/runs.log,
// which is how many times its container actually started.
func containerStarts(t *testing.T, pinRoot string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(pinRoot, "shared", "runs.log"))
	if err != nil {
		t.Fatalf("read the container's own start log: %v", err)
	}
	return len(strings.Fields(string(raw)))
}

// attemptDirectories lists the per-attempt workspaces still on disk for one
// run, so a test can assert the contrast between the attempt's lifetime and the
// pin's. A pin directory is ork-pin-<run>-..., which is deliberately NOT
// matched: the prefix check below is anchored on the attempt shape.
func attemptDirectories(t *testing.T, workRoot, runID string) []string {
	t.Helper()
	entries, err := os.ReadDir(workRoot)
	if err != nil {
		t.Fatalf("read the work root %s: %v", workRoot, err)
	}
	var found []string
	prefix := "ork-" + runID + "-"
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			found = append(found, entry.Name())
		}
	}
	return found
}
