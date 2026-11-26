package identity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resumedTo writes the resume that runs tasks of ork-1 again under scope,
// and points the recorder's and the mint's checks at the real store.
func (w *recordWorld) resumedTo(scope, previous uint64, tasks ...string) {
	w.t.Helper()
	if err := w.store.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "ns", RunID: "ork-1", Scope: scope,
		PreviousScope: previous, Tasks: tasks}); err != nil {
		w.t.Fatal(err)
	}
	check := &FollowsCheck{Store: w.store, Spacing: fastSpacings}
	w.recorder.Follows, w.minter.Follows = check, check
}

// scopedRequest is the commit of an attempt dispatched under scope.
func (w *recordWorld) scopedRequest(scope uint64) CommitRequest {
	w.t.Helper()
	req := w.commitRequest()
	key, err := artifacts.ScopedCommitKey("ns", "ork-1", "build", scope)
	if err != nil {
		w.t.Fatal(err)
	}
	req.Scopes, req.CommitKey = Scopes{Resume: scope}, key
	req.Commit.IdempotencyKey = task.ScopedIdempotencyKeyFor("ns", "ork-1", "build", scope)
	return req
}

// TestRecord_AResumedTaskIsRecordedUnderItsScope: a task a resume runs again
// is recorded at that scope's key, whatever the base's says: the earlier
// record is neither answered nor touched, and it stands where it was.
func TestRecord_AResumedTaskIsRecordedUnderItsScope(t *testing.T) {
	w := newRecordWorld(t)
	first, err := w.record(w.commitRequest())
	if err != nil || !first.Created {
		t.Fatalf("the base's record = %+v, %v", first, err)
	}
	w.resumedTo(5, 0, "build")
	got, err := w.record(w.scopedRequest(5))
	if err != nil || !got.Created || got.Key != "metadata/attested/ns/ork-1/build/r00000000000000000005/commit.json" {
		t.Fatalf("the resumed task's record = created %v at %q, %v", got.Created, got.Key, err)
	}
	base, ok := w.recordedAt(w.commitRequest())
	if !ok || base.SHA256 != first.Record.SHA256 {
		t.Fatalf("the base's record = %v, %v; want it as it was", base.SHA256, ok)
	}
	again, err := w.record(w.scopedRequest(5))
	if err != nil || again.Created || again.Record.SHA256 != got.Record.SHA256 {
		t.Fatalf("a repeat = created %v, %v; want the resumed task's record", again.Created, err)
	}
}

// TestRecord_AScopeIsHeldToItsKeys: at a preflight and at a commit, an
// attempt that names a scope a later resume replaced, none after a resume,
// or one that does not run its task again is refused; and a commit under a
// scope names that scope's key and idempotency key and no other's. Nothing
// is recorded.
func TestRecord_AScopeIsHeldToItsKeys(t *testing.T) {
	base := func(w *recordWorld) CommitRequest { return w.commitRequest() }
	at := func(scope uint64) func(*recordWorld) CommitRequest {
		return func(w *recordWorld) CommitRequest { return w.scopedRequest(scope) }
	}
	for _, tc := range []struct {
		name   string
		resume func(*recordWorld)
		req    func(*recordWorld) CommitRequest
		edit   func(*CommitRequest)
		want   string
		// preflight is what the preflight is answered: "" is passed.
		preflight string
	}{
		{"none, after a resume", func(w *recordWorld) { w.resumedTo(5, 0, "build") }, base, nil, ReasonNotAdmitted, ReasonNotAdmitted},
		{"one a later resume replaced", func(w *recordWorld) { w.resumedTo(5, 0, "build"); w.resumedTo(9, 5, "build") }, at(5), nil, ReasonNotAdmitted, ReasonNotAdmitted},
		{"one no resume opened", func(w *recordWorld) { w.resumedTo(5, 0, "build") }, at(7), nil, ReasonNotAdmitted, ReasonNotAdmitted},
		{"one that keeps its task", func(w *recordWorld) { w.resumedTo(5, 0, "deploy") }, at(5), nil, ReasonNotAdmitted, ReasonNotAdmitted},
		{"a root's scope, named by the root", func(w *recordWorld) { w.resumedTo(5, 0, "build") }, at(5),
			func(r *CommitRequest) { r.Scopes.Root = 5 }, ReasonNotAdmitted, ReasonNotAdmitted},
		{"the base's commit key", func(w *recordWorld) { w.resumedTo(5, 0, "build") }, at(5),
			func(r *CommitRequest) { r.CommitKey, _ = artifacts.CommitKey("ns", "ork-1", "build") }, ReasonCommitIdentityMismatch, ""},
		{"another scope's commit key", func(w *recordWorld) { w.resumedTo(5, 0, "build") }, at(5),
			func(r *CommitRequest) { r.CommitKey, _ = artifacts.ScopedCommitKey("ns", "ork-1", "build", 6) }, ReasonCommitIdentityMismatch, ""},
		{"the base's idempotency key", func(w *recordWorld) { w.resumedTo(5, 0, "build") }, at(5),
			func(r *CommitRequest) { r.Commit.IdempotencyKey = task.IdempotencyKeyFor("ns", "ork-1", "build") }, ReasonCommitIdentityMismatch, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordWorld(t)
			tc.resume(w)
			req := tc.req(w)
			if tc.edit != nil {
				tc.edit(&req)
			}
			pre := req
			pre.Preflight, pre.Commit = true, nil
			if _, err := w.record(pre); (tc.preflight == "") != (err == nil) || (tc.preflight != "" && reasonOf(err) != tc.preflight) {
				t.Fatalf("the preflight = %v, want %q", err, tc.preflight)
			}
			got, err := w.record(req)
			if got.Created || reasonOf(err) != tc.want {
				t.Fatalf("the commit = created %v, %v; want %s", got.Created, err, tc.want)
			}
			for _, scope := range []uint64{0, 5, 6, 7} {
				if _, ok := w.recordedAt(w.scopedRequest(scope)); ok {
					t.Fatalf("a refused commit left a record at scope %d", scope)
				}
			}
		})
	}
}

// TestRecord_AResumedItemIsRecordedUnderItsScope: a fan-out's item too.
func TestRecord_AResumedItemIsRecordedUnderItsScope(t *testing.T) {
	w := newRecordWorld(t)
	row := w.adm.Tasks["build"]
	row.MaxItems = 4
	w.adm.Tasks["build"] = row
	w.sha = w.write(w.adm)
	w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: w.sha})
	w.facts.Pending[0].ActivityID = "ork/build/2"
	w.resumedTo(5, 0, "build")
	req := w.commitRequest()
	key, err := artifacts.ScopedItemCommitKey("ns", "ork-1", "build", 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	req.Item, req.Scopes, req.CommitKey = artifacts.Item{Index: 2, Set: true}, Scopes{Resume: 5}, key
	req.Commit.IdempotencyKey = task.ScopedItemIdempotencyKeyFor("ns", "ork-1", "build", 2, 5)
	want, err := artifacts.AttestedCommitKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := w.record(req); err != nil || !got.Created || got.Key != want {
		t.Fatalf("the resumed item's record = created %v at %q, %v; want %q", got.Created, got.Key, err, want)
	}
}

// TestMint_AResumeOpensItsOwnSlots: a resume the API accepted opens a new
// set of the attempts that may hold a task's identities, and nothing else
// does: the base's are spent, a resumed attempt holds one of its own, and
// an attempt under a scope that was replaced holds none.
func TestMint_AResumeOpensItsOwnSlots(t *testing.T) {
	w := newRecordWorld(t)
	ask := func(attempt int32, scopes Scopes) error {
		t.Helper()
		w.facts.Pending[0].Attempt = attempt
		w.facts.Pending[0].StartedAt = started.Add(time.Duration(attempt) * time.Second)
		req := w.request()
		req.Attempt, req.Scopes = attempt, scopes
		_, err := w.mint(req)
		return err
	}
	for attempt := int32(1); attempt <= 2; attempt++ {
		if err := ask(attempt, Scopes{}); err != nil {
			t.Fatalf("the base's attempt %d: %v", attempt, err)
		}
	}
	if err := ask(3, Scopes{}); reasonOf(err) != ReasonAttemptsExhausted {
		t.Fatalf("a third of two: %v", err)
	}
	w.resumedTo(5, 0, "build")
	if err := ask(3, Scopes{}); reasonOf(err) != ReasonNotAdmitted {
		t.Fatalf("an attempt that names no resume, after one: %v", err)
	}
	if err := ask(4, Scopes{Resume: 7}); reasonOf(err) != ReasonNotAdmitted {
		t.Fatalf("a scope no resume opened: %v", err)
	}
	for attempt := int32(1); attempt <= 2; attempt++ {
		if err := ask(attempt, Scopes{Resume: 5}); err != nil {
			t.Fatalf("the resumed attempt %d: %v", attempt, err)
		}
	}
	if err := ask(3, Scopes{Resume: 5}); reasonOf(err) != ReasonAttemptsExhausted {
		t.Fatalf("a third of the resume's two: %v", err)
	}
}

// TestMint_AScopeThisEngineCannotCheckIsRefused: a mint with no check of a
// run's resumes issues nothing to an attempt that names one, and spends no
// slot.
func TestMint_AScopeThisEngineCannotCheckIsRefused(t *testing.T) {
	w := newMintWorld(t)
	req := w.request()
	req.Scopes = Scopes{Resume: 5}
	if _, err := w.mint(req); reasonOf(err) != ReasonEngineRefused {
		t.Fatalf("mint = %v, want %s", err, ReasonEngineRefused)
	}
	slot, err := artifacts.AttemptSlotKey("ns", "ork-1", "build", artifacts.NoItem, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(w.dir, filepath.FromSlash(slot))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused mint took a slot: %v", err)
	}
}

// TestRecord_AChildOfAResumedRootKeepsItsOwnKeys: a run below a root names
// the root's scope, which is held against the root's resumes, and records at
// its own run's plain key: its id is its own.
func TestRecord_AChildOfAResumedRootKeepsItsOwnKeys(t *testing.T) {
	w := newRecordWorld(t)
	w.adm.RootRunID = "ork-root"
	w.adm.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-root", AdmissionSHA256: w.sha}
	if err := w.store.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "ns", RunID: "ork-root", Scope: 5, Tasks: []string{"sub"}}); err != nil {
		t.Fatal(err)
	}
	check := &FollowsCheck{Store: w.store, Spacing: fastSpacings}
	for _, tc := range []struct {
		scopes Scopes
		want   string
	}{{Scopes{Root: 5}, ""}, {Scopes{}, ReasonNotAdmitted}, {Scopes{Root: 4}, ReasonNotAdmitted}, {Scopes{Resume: 5, Root: 5}, ReasonNotAdmitted}} {
		err := check.CheckScope(context.Background(), w.adm, "build", tc.scopes)
		if (tc.want == "") != (err == nil) || (tc.want != "" && reasonOf(err) != tc.want) {
			t.Errorf("scopes %+v = %v, want %q", tc.scopes, err, tc.want)
		}
	}
	req := w.commitRequest()
	req.Scopes = Scopes{Root: 5}
	key, idempotency, err := commitCoordinates(req)
	if err != nil || key != req.CommitKey || idempotency != task.IdempotencyKeyFor("ns", "ork-1", "build") {
		t.Fatalf("a child's keys = %q, %q, %v; want its own run's plain ones", key, idempotency, err)
	}
}
