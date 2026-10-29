//go:build unix

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
)

// TestTaskRun_EachRunHasItsOwnIdentity: two task runs on one host label their
// containers with different identities, so removing one run's containers by
// its label can never remove the other's.
func TestTaskRun_EachRunHasItsOwnIdentity(t *testing.T) {
	fx := newTaskRunFixture(t)
	var labels []string
	for range 2 {
		rt := &fakeTaskRuntime{script: succeedWith(`{}`, "")}
		fx.runTaskRun(rt, "--from-run", taskRunSourceRun, "--task", "consume").requireExit(t, exitOK)
		if len(rt.specs) != 1 {
			t.Fatalf("the run created %d containers, want 1", len(rt.specs))
		}
		label := rt.specs[0].Labels["ork.orker"]
		if !strings.HasPrefix(label, taskRunOrkerPrefix) {
			t.Errorf("ork.orker=%q, want the prefix %q", label, taskRunOrkerPrefix)
		}
		if err := task.ValidOrkerID(label); err != nil {
			t.Errorf("ork.orker=%q is not a valid Orker id: %v", label, err)
		}
		labels = append(labels, label)
	}
	if labels[0] == labels[1] {
		t.Fatalf("both runs labelled their containers ork.orker=%s", labels[0])
	}
}

// lockFiles are the identity lock files under root.
func lockFiles(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), taskRunOrkerPrefix) {
			out = append(out, entry.Name())
		}
	}
	return out
}

// TestTaskRun_ASettledRunLeavesNoLockFile: a run that removed its container,
// and a --render-only run that never created one, delete their lock files;
// a run whose removal failed keeps its own, so a later run asks the runtime
// for its label again.
func TestTaskRun_ASettledRunLeavesNoLockFile(t *testing.T) {
	fx := newTaskRunFixture(t)
	fx.runTaskRun(&fakeTaskRuntime{script: succeedWith(`{}`, "")}, "--from-run", taskRunSourceRun, "--task", "consume").requireExit(t, exitOK)
	fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", taskRunSourceRun, "--task", "consume", "--render-only").requireExit(t, exitOK)
	if left := lockFiles(t, fx.workdir); len(left) != 0 {
		t.Fatalf("settled runs left lock files: %v", left)
	}

	failing := &fakeTaskRuntime{script: succeedWith(`{}`, ""), removeErr: errors.New("daemon hiccup")}
	fx.runTaskRun(failing, "--from-run", taskRunSourceRun, "--task", "consume")
	label := failing.specs[0].Labels["ork.orker"]
	if left := lockFiles(t, fx.workdir); !reflect.DeepEqual(left, []string{label + taskRunLockSuffix}) {
		t.Fatalf("a run whose removal failed left %v, want only its own %s%s", left, label, taskRunLockSuffix)
	}

	// The next run asks for that label, says what it removed, and settles it.
	next := &fakeTaskRuntime{script: succeedWith(`{}`, ""), reaped: map[string]int{label: 1}}
	got := fx.runTaskRun(next, "--from-run", taskRunSourceRun, "--task", "consume").requireExit(t, exitOK)
	if !reflect.DeepEqual(next.reapAsked, []string{label}) {
		t.Errorf("the next run asked Reap for %v, want %v", next.reapAsked, []string{label})
	}
	got.requireContains(t, "removed 1 container(s) of an earlier task run that is gone (ork.orker="+label+")")
	if left := lockFiles(t, fx.workdir); len(left) != 0 {
		t.Fatalf("lock files left after the next run settled the earlier one: %v", left)
	}

	// --render-only asks the runtime for nobody's containers.
	failing = &fakeTaskRuntime{script: succeedWith(`{}`, ""), removeErr: errors.New("daemon hiccup")}
	fx.runTaskRun(failing, "--from-run", taskRunSourceRun, "--task", "consume")
	rendering := &fakeTaskRuntime{}
	fx.runTaskRun(rendering, "--from-run", taskRunSourceRun, "--task", "consume", "--render-only").requireExit(t, exitOK)
	if len(rendering.reapAsked) != 0 {
		t.Errorf("--render-only asked Reap for %v", rendering.reapAsked)
	}
}

// reapRecorder answers Reap and records which identities it was asked for.
type reapRecorder struct {
	orker.ContainerRuntime
	mu      sync.Mutex
	asked   []string
	removed map[string]int
	fail    map[string]error
}

func (r *reapRecorder) Reap(_ context.Context, orkerID string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked = append(r.asked, orkerID)
	return r.removed[orkerID], r.fail[orkerID]
}

const testFingerprint = "host=test\nruntime=recorder\n"

func claimForTest(t *testing.T, root, hex, fingerprint string) *taskRunOwner {
	t.Helper()
	owner, err := claimTaskRunOwner(root, "local-"+hex, fingerprint)
	if err != nil {
		t.Fatalf("claim local-%s: %v", hex, err)
	}
	if owner.unlocked != nil {
		t.Fatalf("claim local-%s could not lock: %v", hex, owner.unlocked)
	}
	t.Cleanup(func() { owner.release(false) })
	return owner
}

// TestTaskRunOwner_ReapsOnlyRunsThatAreGone: a run removes the containers of
// an identity whose lock file is in its root, names its own host and
// endpoint, and whose lock is free; it deletes that lock file and touches
// nothing else: not itself, not a live run, not a run on another daemon, not
// another user's run, not a claim still in progress, and not a file that
// merely looks like a lock.
func TestTaskRunOwner_ReapsOnlyRunsThatAreGone(t *testing.T) {
	root := t.TempDir()
	self := claimForTest(t, root, "aaaaaaaaaaaaaaaa", testFingerprint)
	live := claimForTest(t, root, "bbbbbbbbbbbbbbbb", testFingerprint)
	gone := claimForTest(t, root, "cccccccccccccccc", testFingerprint)
	gone.release(false) // the process is gone: its lock is free, its file stays
	elsewhere := claimForTest(t, root, "9999999999999999", "host=test\nruntime=another-daemon\n")
	elsewhere.release(false)

	if self.id != "orkctl-task-run-aaaaaaaaaaaaaaaa" {
		t.Fatalf("identity %q, want orkctl-task-run-aaaaaaaaaaaaaaaa", self.id)
	}
	// What a scan must never read as a run that is gone.
	for name, mode := range map[string]os.FileMode{
		"orkctl-task-run-dddddddddddddddd.lock.new": 0o600, // a claim killed before its rename
		"orkctl-task-run-XYZ.lock":                  0o600, // not a local run id
		"orkctl-task-run-eeeeeeeeeeeeeeee.lock2":    0o600,
		"ork-task-run-ffffffffffffffff":             0o600, // a session directory's name
		"orkctl-task-run-8888888888888888.lock":     0o000, // another user's, in a shared root
	} {
		if err := os.WriteFile(filepath.Join(root, name), nil, mode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "orkctl-task-run-1111111111111111.lock"), 0o700); err != nil {
		t.Fatal(err)
	}

	rt := &reapRecorder{removed: map[string]int{gone.id: 2}}
	removed, err := reapGoneTaskRuns(context.Background(), rt, root, testFingerprint, self)
	if err != nil {
		t.Fatalf("reapGoneTaskRuns: %v", err)
	}
	if want := []string{gone.id}; !reflect.DeepEqual(rt.asked, want) {
		t.Errorf("Reap was asked for %v, want only %v", rt.asked, want)
	}
	if want := map[string]int{gone.id: 2}; !reflect.DeepEqual(removed, want) {
		t.Errorf("removed %v, want %v", removed, want)
	}
	if _, err := os.Stat(gone.path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the gone run's lock file is still there (%v)", err)
	}
	for _, kept := range []string{self.path, live.path, elsewhere.path} {
		if _, err := os.Stat(kept); err != nil {
			t.Errorf("lock file %s was removed: %v", kept, err)
		}
	}

	// Once the live run is gone, the next scan settles it too.
	live.release(false)
	rt = &reapRecorder{}
	if _, err := reapGoneTaskRuns(context.Background(), rt, root, testFingerprint, self); err != nil {
		t.Fatal(err)
	}
	if want := []string{live.id}; !reflect.DeepEqual(rt.asked, want) {
		t.Errorf("after the second run ended, Reap was asked for %v, want %v", rt.asked, want)
	}
	// A scan on the other daemon settles that one's run.
	rt = &reapRecorder{}
	if _, err := reapGoneTaskRuns(context.Background(), rt, root, "host=test\nruntime=another-daemon\n", self); err != nil {
		t.Fatal(err)
	}
	if want := []string{elsewhere.id}; !reflect.DeepEqual(rt.asked, want) {
		t.Errorf("a scan on the other daemon asked Reap for %v, want %v", rt.asked, want)
	}
}

// TestTaskRunOwner_AFailedRemovalKeepsTheLockFile: a container that could not
// be removed keeps its identity's lock file, so a later run tries again, and
// the error names the identity.
func TestTaskRunOwner_AFailedRemovalKeepsTheLockFile(t *testing.T) {
	root := t.TempDir()
	gone := claimForTest(t, root, "cccccccccccccccc", testFingerprint)
	gone.release(false)

	rt := &reapRecorder{fail: map[string]error{gone.id: errors.New("daemon unreachable")}}
	_, err := reapGoneTaskRuns(context.Background(), rt, root, testFingerprint, nil)
	if err == nil || !strings.Contains(err.Error(), gone.id) || !strings.Contains(err.Error(), "daemon unreachable") {
		t.Fatalf("error %v, want one naming %s and the cause", err, gone.id)
	}
	if _, statErr := os.Stat(gone.path); statErr != nil {
		t.Fatalf("the lock file of an identity whose containers are still there was removed: %v", statErr)
	}
	if _, err := reapGoneTaskRuns(context.Background(), &reapRecorder{}, root, testFingerprint, nil); err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(gone.path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("the retry left the lock file (%v)", statErr)
	}
}

// TestTaskRunOwner_ClaimIsHeldAndRefusesAForeignID: a claim holds its lock
// under its scanned name, names its fingerprint, leaves no staging file,
// deletes its file when it settled, and refuses an id that is not a local
// run id rather than build a label filter from it.
func TestTaskRunOwner_ClaimIsHeldAndRefusesAForeignID(t *testing.T) {
	root := t.TempDir()
	owner := claimForTest(t, root, "0123456789abcdef", testFingerprint)
	file, err := os.OpenFile(owner.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if err := lockTaskRunOwner(file); !errors.Is(err, errTaskRunOwnerHeld) {
		t.Fatalf("a second lock of a live claim answered %v, want errTaskRunOwnerHeld", err)
	}
	if named, _ := os.ReadFile(owner.path); string(named) != testFingerprint {
		t.Errorf("the lock file names %q, want %q", named, testFingerprint)
	}
	if _, err := os.Stat(owner.path + ".new"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the staging file is still there (%v)", err)
	}
	owner.release(true)
	if _, err := os.Stat(owner.path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a settled claim left its lock file (%v)", err)
	}
	for _, id := range []string{"ork-1790000000000000001-1", "local-../../x", "local-ABCDEF0123456789"} {
		if _, err := claimTaskRunOwner(root, id, testFingerprint); err == nil {
			t.Errorf("claimTaskRunOwner(%q) succeeded, want a refusal", id)
		}
	}
}
