package engine

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordedVerify is the recordsWorld run verified as ResumeRun verifies an
// admitted one.
func (w *recordsWorld) recordedVerify(t *testing.T) ResumeVerification {
	t.Helper()
	local, err := artifacts.NewLocalCommitStore(w.dir)
	if err != nil {
		t.Fatal(err)
	}
	commits := &readCommits{CommitStore: local}
	w.commitReads = commits
	values, err := artifacts.NewLocalStore(w.dir)
	if err != nil {
		t.Fatal(err)
	}
	in := ResumePlanInput{Definition: w.ask.Definition, Base: w.ask.Base, Facts: ResumeBaseFacts{Dispatched: map[ResumeUnit]bool{}}}
	got, err := NewResumeVerifier(commits, values).VerifyRecorded(context.Background(), in, "file://"+w.dir, &ResumeRecordedRun{
		Store: w, AdmissionSHA256: w.ask.AdmissionSHA256, Resumes: w.ask.Resumes, End: w.ask.End, Spacings: w.ask.Spacings, Children: w.ask.Children, Received: w.ask.Received})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// readCommits is a commit store that keeps each uri it is asked for.
type readCommits struct {
	artifacts.CommitStore
	mu  sync.Mutex
	got []string
}

func (r *readCommits) Get(ctx context.Context, uri string) (artifacts.CommitRecord, error) {
	r.mu.Lock()
	r.got = append(r.got, uri)
	r.mu.Unlock()
	return r.CommitStore.Get(ctx, uri)
}

// identified edits a commit to name task's activity at scope, as an Orker's
// attempt does.
func identified(task string, scope uint64) func(*orkstackv1.ActivityCommit) {
	return func(c *orkstackv1.ActivityCommit) {
		c.IdempotencyKey = taskScopedKey(task, scope)
	}
}

func taskScopedKey(id string, scope uint64) string {
	return task.ScopedIdempotencyKeyFor("default", "ork-x", id, scope)
}

// TestVerifyRecorded_ReadsWhatTheEngineRecorded: an admitted run's kept
// container task is verified at the engine's record, the key its read-back
// reads, and an Orker's commit beside no record keeps nothing; its awaits and
// composed tasks are verified from their records, and what they follow is the
// evidence the planner holds them to.
func TestVerifyRecorded_ReadsWhatTheEngineRecorded(t *testing.T) {
	w := newRecordsWorld(t)
	build := w.commit("build", 0, `{"n":1}`, identified("build", 0))
	w.commit("alone", 0, `{"n":2}`, identified("alone", 0))
	w.resolution("approve", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
	w.composed(0, `{"digest":"d"}`, map[string]string{"approve": w.row("approve").GetValuesRef().GetSha256()}, nil)
	w.resolution("review", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
	got := w.recordedVerify(t)
	ev := got.Evidence
	for _, id := range []string{"build", "alone", "approve", "ship", "review"} {
		if ev.Kept[nodeUnit(id)] != KeptValid || got.Sizes[nodeUnit(id)] <= 0 {
			t.Errorf("%s = %v (%s, %v), size %d; want valid and measured", id, ev.Kept[nodeUnit(id)], ev.Why[nodeUnit(id)], got.Causes[nodeUnit(id)], got.Sizes[nodeUnit(id)])
		}
	}
	if !ev.Recorded || ev.ResultSHA256["build"] != build || len(ev.Follows["approve"]) != 1 || !ev.Resolutions["review"].Completed {
		t.Errorf("evidence = %+v", ev)
	}

	// What verification read is what the read-back reads: the engine's record,
	// and nothing where an Orker commits.
	entry, err := adoptEntryOf(w.ask.Definition, "file://"+w.dir, "ork-x", w.ask.Definition.GetTasks()[0], -1, 0, true)
	if err != nil || !artifacts.IsAttestedURI(entry.GetCommitUri()) {
		t.Fatalf("the read-back's entry = %v, %v", entry, err)
	}
	w.commitReads.mu.Lock()
	read := slices.Clone(w.commitReads.got)
	w.commitReads.mu.Unlock()
	if !slices.Contains(read, entry.GetCommitUri()) || slices.ContainsFunc(read, func(uri string) bool { return !artifacts.IsAttestedURI(uri) }) {
		t.Errorf("verification read %v; the read-back reads %s", read, entry.GetCommitUri())
	}

	// An Orker's commit beside no record of the engine's is no result.
	planted := newRecordsWorld(t)
	commits, err := artifacts.NewLocalCommitStore(planted.dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := ExpectedCommitIdentity(planted.ask.Definition, "ork-x", planted.ask.Definition.GetTasks()[0], -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	ref := planted.put(`{"n":1}`)
	if _, err := commits.Create(context.Background(), "file://"+filepath.Join(planted.dir, filepath.FromSlash(id.Key)), &orkstackv1.ActivityCommit{
		SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: "ork-x", TaskId: "build", Values: ref,
		IdempotencyKey: id.IdempotencyKey}); err != nil {
		t.Fatal(err)
	}
	if a := planted.recordedVerify(t).Evidence.Kept[nodeUnit("build")]; a != KeptGone {
		t.Errorf("an Orker's commit with no record = %v, want gone", a)
	}
}

// TestVerifyRecorded_HoldsTheStateToTheResumes: a kept result the state
// places at a scope a later resume replaced is unreadable, though a real
// record of it lies there.
func TestVerifyRecorded_HoldsTheStateToTheResumes(t *testing.T) {
	w := newRecordsWorld(t)
	w.commit("alone", 0, `{"n":2}`, identified("alone", 0))
	w.ask.Resumes = []artifacts.ResumeRecord{{Scope: 42, Tasks: []string{"alone"}}}
	if a := w.recordedVerify(t).Evidence.Kept[nodeUnit("alone")]; a != KeptUnreadable {
		t.Errorf("alone = %v, want unreadable", a)
	}
}

// TestVerifyRecorded_AnOpenAwaitsRecordUnread: an await answered on record and
// whose record could not be read, or not as its own, says so.
func TestVerifyRecorded_AnOpenAwaitsRecordUnread(t *testing.T) {
	w := newRecordsWorld(t)
	file := w.resolution("review", 0, `{"ok":true}`, nil, nil)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.Replace(string(raw), `"taskId": "review"`, `"taskId": "approve"`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	got := w.recordedVerify(t).Evidence
	if got.Kept[nodeUnit("review")] != KeptUnreadable || got.Resolutions["review"] != (ResolutionAnswer{}) {
		t.Errorf("review = %v, %+v", got.Kept[nodeUnit("review")], got.Resolutions["review"])
	}
}
