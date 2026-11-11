package orker

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// sealedAttemptDir builds what a killed attempt leaves: the whole workspace
// tree, sealed, with a secret staged in it. It uses the dispatcher's own
// directory naming and the workspace's own seal, so a change to either reaches
// this test.
func sealedAttemptDir(t *testing.T, d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) string {
	t.Helper()
	dir, err := d.makeAttemptDir(in, 1)
	must.OKf(t, err, "makeAttemptDir")
	// A sealed tree is one t.TempDir's own cleanup cannot remove.
	t.Cleanup(func() { _ = removeSealedTree(dir) })
	ws, err := NewWorkspace(dir)
	must.OKf(t, err, "NewWorkspace")
	must.OK(t, os.WriteFile(filepath.Join(ws.Secrets, "db"), []byte("hunter2"), 0o600))
	must.OK(t, os.WriteFile(filepath.Join(ws.Input, "task.json"), []byte("{}"), 0o644))
	must.OKf(t, ws.Seal(), "Seal")
	must.OKf(t, ws.SealSecrets(), "SealSecrets")
	return dir
}

func attemptInput(run, task string) *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{RunId: run, TaskId: task}
}

// TestReapAttemptDirectories_RemovesWhatThisIdentityLeftAndNothingElse is the
// rule, entry by entry: a directory this identity's dispatcher made goes,
// secrets and sealed input included, and everything else under the work root
// stays, whatever it looks like.
func TestReapAttemptDirectories_RemovesWhatThisIdentityLeftAndNothingElse(t *testing.T) {
	root := t.TempDir()
	mine := &OCIDispatcher{WorkRoot: root, OrkerID: "orker-a"}
	peer := &OCIDispatcher{WorkRoot: root, OrkerID: "orker-b"}

	own := []string{
		sealedAttemptDir(t, mine, attemptInput("ork-one", "build")),
		// A task id and a run id may hold what the owner's segment looks like.
		sealedAttemptDir(t, mine, attemptInput("ork-"+attemptOwner("orker-b"), attemptOwner("orker-b"))),
		sealedAttemptDir(t, mine, &orkstackv1.ExecuteTaskInput{
			RunId: "ork-one", TaskId: "convert", Each: &orkstackv1.EachContext{Index: 97, Count: 100},
		}),
	}
	peers := []string{
		sealedAttemptDir(t, peer, attemptInput("ork-one", "build")),
		// The peer's run and task ids spell this identity's segment, which only
		// the name's end may decide.
		sealedAttemptDir(t, peer, attemptInput("ork-"+attemptOwner("orker-a")+"-1", attemptOwner("orker-a")+"-2")),
	}

	stays := append([]string{}, peers...)
	for _, name := range []string{
		// A directory of the shape every release before this one made: nothing
		// says whose it is.
		"ork-ork-one-build-1-123456789",
		// A pin's directory ends by its own lease, its release or its reap.
		"ork-pin-ork-one-build-0000002a-0000-4000-8000-000000000000",
		"ork-pin-ork-one-" + attemptOwner("orker-a") + "-123456789",
		"ork-bindprobe-123456789",
		SpoolDirName,
		StageDirName,
		// Nothing the Orker made.
		"operator-notes",
		"ork-",
		"ork-" + attemptOwner("orker-a"),
		"ork-" + attemptOwner("orker-a") + "-123",
		"ork-x-y-z-" + attemptOwner("orker-a") + "-123",
		"ork-x-y-1-" + attemptOwner("orker-a") + "-12x",
		"ork-x-y-1-" + attemptOwner("orker-a") + "-",
	} {
		path := filepath.Join(root, name)
		must.OK(t, os.Mkdir(path, 0o700))
		stays = append(stays, path)
	}
	// A file and a link named as this identity's directory: neither is a
	// directory it made, and a link's target is never reached.
	elsewhere := t.TempDir()
	keep := filepath.Join(elsewhere, "keep")
	must.OK(t, os.WriteFile(keep, []byte("x"), 0o600))
	file := filepath.Join(root, "ork-file-task-1-"+attemptOwner("orker-a")+"-5")
	must.OK(t, os.WriteFile(file, nil, 0o600))
	link := filepath.Join(root, "ork-link-task-1-"+attemptOwner("orker-a")+"-6")
	must.OK(t, os.Symlink(elsewhere, link))
	stays = append(stays, file, link, keep)

	report, err := ReapAttemptDirectories(root, "orker-a")
	must.OKf(t, err, "ReapAttemptDirectories")

	slices.Sort(own)
	slices.Sort(report.Removed)
	if !slices.Equal(report.Removed, own) {
		t.Errorf("removed %v, want exactly %v", report.Removed, own)
	}
	for _, dir := range own {
		if _, err := os.Lstat(dir); !os.IsNotExist(err) {
			t.Errorf("%s survived its identity's reap: %v", dir, err)
		}
	}
	for _, path := range stays {
		if _, err := os.Lstat(path); err != nil {
			t.Errorf("%s did not survive another identity's reap: %v", path, err)
		}
	}
	for _, dir := range peers {
		secret, err := os.ReadFile(filepath.Join(dir, "secrets", "db"))
		if err != nil || string(secret) != "hunter2" {
			t.Errorf("the peer's secret in %s is gone: %q, %v", dir, secret, err)
		}
	}
	// Only what has an attempt directory's shape is reported as left: the two
	// of the peer and the one of an earlier release.
	wantOthers := append([]string{filepath.Join(root, "ork-ork-one-build-1-123456789")}, peers...)
	slices.Sort(wantOthers)
	slices.Sort(report.Others)
	if !slices.Equal(report.Others, wantOthers) {
		t.Errorf("reported as left by others: %v, want %v", report.Others, wantOthers)
	}
}

// TestReapAttemptDirectories_NoIdentityRemovesNothing: an empty identity is what
// a dispatcher with none makes, whose directories carry no owner. It must not
// match anything by accident.
func TestReapAttemptDirectories_NoIdentityRemovesNothing(t *testing.T) {
	root := t.TempDir()
	d := &OCIDispatcher{WorkRoot: root}
	dir := sealedAttemptDir(t, d, attemptInput("ork-one", "build"))
	report, err := ReapAttemptDirectories(root, "")
	must.OKf(t, err, "ReapAttemptDirectories")
	if len(report.Removed) != 0 {
		t.Errorf("removed %v with no identity", report.Removed)
	}
	if _, err := os.Lstat(dir); err != nil {
		t.Errorf("the directory is gone: %v", err)
	}
}

func TestReapAttemptDirectories_AnAbsentRootHoldsNothing(t *testing.T) {
	report, err := ReapAttemptDirectories(filepath.Join(t.TempDir(), "absent"), "orker-a")
	if err != nil || len(report.Removed)+len(report.Others) != 0 {
		t.Fatalf("report %+v, err %v", report, err)
	}
}

// TestAttemptOwner_DiffersBetweenIdentities: the segment is what keeps one
// identity's reap off another's directories.
func TestAttemptOwner_DiffersBetweenIdentities(t *testing.T) {
	seen := map[string]string{}
	for _, id := range []string{"orker-a", "orker-b", "host-0123456789abcdef", "a", "A"} {
		owner := attemptOwner(id)
		if other, dup := seen[owner]; dup {
			t.Errorf("%q and %q share the owner segment %q", id, other, owner)
		}
		seen[owner] = id
		if got, attempt := attemptDirOwner("ork-run-task-1-" + owner + "-123"); !attempt || got != owner {
			t.Errorf("attemptDirOwner read %q, %v from %q's own name, want %q", got, attempt, id, owner)
		}
	}
}

// bootWithLog boots one Orker in workRoot under id, its log kept.
func bootWithLog(t *testing.T, workRoot, id string, runtime ContainerRuntime) (*Activities, *bytes.Buffer, error) {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OK(t, err)
	t.Cleanup(func() { _ = store.Close() })
	var logged bytes.Buffer
	activities := NewActivitiesWithOptions(store, Options{
		Runtime: runtime, WorkRoot: workRoot, OrkerID: id,
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	})
	err = activities.Boot(context.Background())
	if err == nil {
		t.Cleanup(activities.Close)
	}
	return activities, &logged, err
}

// TestBoot_ReapsThisIdentitysAttemptDirectories is the reap in its place: a
// replacement's boot removes what its predecessor left, says so, and leaves
// another identity's alone, saying that too.
func TestBoot_ReapsThisIdentitysAttemptDirectories(t *testing.T) {
	root := t.TempDir()
	id := "attempt-reap-" + t.Name()
	dead := sealedAttemptDir(t, &OCIDispatcher{WorkRoot: root, OrkerID: id}, attemptInput("ork-one", "build"))
	peer := sealedAttemptDir(t, &OCIDispatcher{WorkRoot: root, OrkerID: id + "-peer"}, attemptInput("ork-one", "build"))

	_, logged, err := bootWithLog(t, root, id, &reapRecorder{labelled: map[string]int{}})
	must.OKf(t, err, "Boot")
	if _, err := os.Lstat(dead); !os.IsNotExist(err) {
		t.Errorf("the predecessor's attempt directory survived the boot: %v", err)
	}
	if !strings.Contains(logged.String(), dead) {
		t.Errorf("the boot did not say what it removed:\n%s", logged)
	}
	if secret, err := os.ReadFile(filepath.Join(peer, "secrets", "db")); err != nil || string(secret) != "hunter2" {
		t.Errorf("another identity's staged secret: %q, %v", secret, err)
	}
	if !strings.Contains(logged.String(), peer) {
		t.Errorf("the boot did not say what it left:\n%s", logged)
	}
}

// TestBoot_ARefusedIdentityRemovesNoDirectory is how identity makes the reap
// safe: while a live Orker holds an identity, a second process claiming it
// stops at the lock, before it reaps anything by it. The live Orker's attempt,
// secret included, is untouched; once the lock is free, the same call removes it.
func TestBoot_ARefusedIdentityRemovesNoDirectory(t *testing.T) {
	root := t.TempDir()
	id := "attempt-reap-" + t.Name()
	live, _, err := bootWithLog(t, root, id, &reapRecorder{labelled: map[string]int{}})
	must.OKf(t, err, "first Boot")
	running := sealedAttemptDir(t, live.Dispatcher(), attemptInput("ork-one", "build"))

	if _, _, err := bootWithLog(t, root, id, &reapRecorder{labelled: map[string]int{}}); err == nil {
		t.Fatal("a second Orker booted on a live identity")
	}
	if secret, err := os.ReadFile(filepath.Join(running, "secrets", "db")); err != nil || string(secret) != "hunter2" {
		t.Fatalf("a refused boot reached the live Orker's attempt: %q, %v", secret, err)
	}

	// The positive control: nothing but the lock stood in the way.
	live.Close()
	if _, _, err := bootWithLog(t, root, id, &reapRecorder{labelled: map[string]int{}}); err != nil {
		t.Fatalf("replacement Boot: %v", err)
	}
	if _, err := os.Lstat(running); !os.IsNotExist(err) {
		t.Errorf("the replacement left its dead predecessor's directory: %v", err)
	}
}

// TestBoot_AnotherIdentitysBootLeavesALiveOrkersPinAndScratch: two identities on
// one work root, the shape only explicit identities allow. The first is alive, with
// a pin open and a download on its way, and the second's boot must touch neither.
// What makes that mean something: the same boot does remove its own predecessor's.
func TestBoot_AnotherIdentitysBootLeavesALiveOrkersPinAndScratch(t *testing.T) {
	root := t.TempDir()
	liveID := "scratch-reap-live-" + t.Name()
	otherID := "scratch-reap-other-" + t.Name()
	live, _, err := bootWithLog(t, root, liveID, &reapRecorder{labelled: map[string]int{}})
	must.OKf(t, err, "the live Orker's Boot")
	writeIn := func(dir, name string) string {
		t.Helper()
		must.OK(t, os.MkdirAll(dir, 0o700))
		path := filepath.Join(dir, name)
		must.OK(t, os.WriteFile(path, []byte("in use"), 0o600))
		return path
	}
	pin, err := AcquirePin(root, "ork-one", "build", testInstanceID, 3600, liveID)
	must.OKf(t, err, "AcquirePin")
	stays := []string{
		writeIn(pin.Shared, "held"),
		writeIn(live.Dispatcher().spoolDir(), "upload"),
		writeIn(live.Dispatcher().stagingCache().dir, "digest"),
	}

	// The second identity's dead predecessor left the same three.
	predecessor := &OCIDispatcher{WorkRoot: root, OrkerID: otherID}
	otherPin, err := AcquirePin(root, "ork-two", "build", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", 3600, otherID)
	must.OKf(t, err, "AcquirePin")
	goes := []string{
		writeIn(otherPin.Shared, "held"),
		writeIn(predecessor.spoolDir(), "upload"),
		writeIn(predecessor.stagingCache().dir, "digest"),
	}

	_, logged, err := bootWithLog(t, root, otherID, &reapRecorder{labelled: map[string]int{}})
	must.OKf(t, err, "the other Orker's Boot")
	for _, path := range stays {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("another identity's boot took the live Orker's %s: %v", path, err)
		}
	}
	for _, path := range goes {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("the boot left its own predecessor's %s: %v", path, err)
		}
	}
	if !strings.Contains(logged.String(), pin.Root) {
		t.Errorf("the boot did not say which pin directory it left:\n%s", logged)
	}
}

// TestBoot_ScratchFromBeforeIdentitiesStays: a file directly under a scratch
// directory is what a release that kept them per work root left, and nothing says
// whose. It may be a live Orker's of that release, so it stays, and the boot says so.
func TestBoot_ScratchFromBeforeIdentitiesStays(t *testing.T) {
	for _, name := range []string{SpoolDirName, StageDirName} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			old := filepath.Join(root, name, "from-an-earlier-release")
			must.OK(t, os.MkdirAll(filepath.Dir(old), 0o700))
			must.OK(t, os.WriteFile(old, []byte("x"), 0o600))
			_, logged, err := bootWithLog(t, root, "scratch-old-"+t.Name(), &reapRecorder{labelled: map[string]int{}})
			must.OKf(t, err, "Boot")
			if _, err := os.Stat(old); err != nil {
				t.Errorf("the boot took a file nothing says is its own: %v", err)
			}
			if !strings.Contains(logged.String(), old) {
				t.Errorf("the boot did not say what it left:\n%s", logged)
			}
		})
	}
}
