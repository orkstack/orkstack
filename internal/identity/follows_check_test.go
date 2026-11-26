package identity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var fastSpacings = &artifacts.RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond,
	ReaskMax: 2 * time.Millisecond, AttemptMargin: time.Millisecond}

// followsWorld is the check over the real local record store: build, then
// approve (which follows build), then deploy (which follows approve).
type followsWorld struct {
	t     *testing.T
	dir   string
	store *artifacts.RecordStore
	check *FollowsCheck
	adm   artifacts.Admission
	sha   string
}

func newFollowsWorld(t *testing.T) *followsWorld {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	w := &followsWorld{t: t, dir: dir, store: store, check: &FollowsCheck{Store: store, Spacing: fastSpacings}}
	w.adm, _ = admitted(t, "ork-1", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	w.adm.Follows = map[string][]artifacts.FollowRef{
		"approve": {{TaskID: "build", Kind: artifacts.FollowContainer}},
		"deploy":  {{TaskID: "approve", Kind: artifacts.FollowAwaited}},
		"report":  {{TaskID: "build", Kind: artifacts.FollowContainer}, {TaskID: "sub", Kind: artifacts.FollowComposed}},
	}
	w.sha = w.admission(w.adm)
	return w
}

func (w *followsWorld) put(key string, body []byte) {
	w.t.Helper()
	path := filepath.Join(w.dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		w.t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		w.t.Fatal(err)
	}
}

func (w *followsWorld) admission(a artifacts.Admission) string {
	w.t.Helper()
	raw, sha, err := artifacts.EncodeAdmission(a)
	if err != nil {
		w.t.Fatal(err)
	}
	key, err := artifacts.AdmissionKey(a.Namespace, a.RunID, sha)
	if err != nil {
		w.t.Fatal(err)
	}
	w.put(key, raw)
	return sha
}

// commit records task's result, whose values hash to values repeated.
func (w *followsWorld) commit(run, sha, task string, scope uint64, values string, edit func(*orkstackv1.ActivityCommit)) {
	w.t.Helper()
	c := &orkstackv1.ActivityCommit{SchemaVersion: 1, Namespace: "ns", RunId: run, TaskId: task,
		Values:   &orkstackv1.ArtifactRef{Sha256: strings.Repeat(values, 64)},
		Recorded: &orkstackv1.CommitRecording{AdmissionSha256: sha}}
	if edit != nil {
		edit(c)
	}
	key, err := artifacts.ScopedCommitKey("ns", run, task, scope)
	if err != nil {
		w.t.Fatal(err)
	}
	if key, err = artifacts.AttestedCommitKey(key); err != nil {
		w.t.Fatal(err)
	}
	raw, err := artifacts.MarshalCommit(c)
	if err != nil {
		w.t.Fatal(err)
	}
	w.put(key, raw)
}

// resolve records task's resolution, over followed (a result's values by
// task).
func (w *followsWorld) resolve(task string, scope uint64, rejected bool, followed map[string]string) {
	w.t.Helper()
	detail, err := artifacts.MarshalResolutionDetail(&orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "ns",
		RunId: "ork-1", TaskId: task, ResumeScope: scope, Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice"})
	if err != nil {
		w.t.Fatal(err)
	}
	sum := sha256.Sum256(detail)
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION,
		Namespace: "ns", RunId: "ork-1", TaskId: task, ResumeScope: scope, AdmissionSha256: w.sha,
		DefinitionDigest: "sha256:" + w.adm.DefinitionDigest, InputBinding: make([]byte, 32), Rejected: rejected,
		DetailSha256: hex.EncodeToString(sum[:])}
	if !rejected {
		rec.Values = &orkstackv1.ArtifactRef{Uri: "file:///none", Sha256: strings.Repeat("e", 64), SizeBytes: 5000}
	}
	for _, id := range slices.Sorted(maps.Keys(followed)) {
		rec.Follows = append(rec.Follows, &orkstackv1.RecordedSource{TaskId: id, RecordSha256: strings.Repeat("b", 64),
			ResultSha256: resultOf(w.t, followed[id])})
	}
	w.record(artifacts.RecordResolution, rec)
	key, err := artifacts.ResolutionDetailKey("ns", "ork-1", task, rec.DetailSha256)
	if err != nil {
		w.t.Fatal(err)
	}
	w.put(key, detail)
}

func (w *followsWorld) record(kind artifacts.RecordKind, rec *orkstackv1.RecordedValues) {
	w.t.Helper()
	raw, err := artifacts.MarshalRecordedValues(rec)
	if err != nil {
		w.t.Fatal(err)
	}
	key, err := artifacts.RecordKey(kind, rec.Namespace, rec.RunId, rec.TaskId, rec.ResumeScope)
	if err != nil {
		w.t.Fatal(err)
	}
	w.put(key, raw)
}

func (w *followsWorld) resume(scope, previous uint64, tasks, reopened []string) {
	w.t.Helper()
	if err := w.store.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "ns", RunID: "ork-1", Scope: scope,
		PreviousScope: previous, Tasks: tasks, ReopenedAwaits: reopened}); err != nil {
		w.t.Fatal(err)
	}
}

func (w *followsWorld) linkKey(scope uint64) string {
	key, err := artifacts.ResumeLinkKey("ns", "ork-1", scope)
	if err != nil {
		w.t.Fatal(err)
	}
	return key
}

// unlinked puts the record of plan n of scope, as a confirm that stalled
// before its link left it.
func (w *followsWorld) unlinked(scope uint64, n int, task string) {
	w.t.Helper()
	raw, err := artifacts.EncodeResumeRecord(artifacts.ResumeRecord{Namespace: "ns", RunID: "ork-1", Scope: scope, N: n, Tasks: []string{task}})
	if err != nil {
		w.t.Fatal(err)
	}
	w.put(w.resumeKey(scope, n), raw)
}

func (w *followsWorld) resumeKey(scope uint64, n int) string {
	key, err := artifacts.ResumeRecordKey("ns", "ork-1", scope, n)
	if err != nil {
		w.t.Fatal(err)
	}
	return key
}

func resultOf(t *testing.T, values string) string {
	t.Helper()
	sha, err := ResultSHA256(strings.Repeat(values, 64), nil)
	if err != nil {
		t.Fatal(err)
	}
	return sha
}

// TestCheckFollows_ARunNeverResumed: each followed task's record of success
// is read by its key, under this run's admission; one that is missing,
// rejected or another run's admission is refused, and one that cannot be
// trusted is asked again, never a miss.
func TestCheckFollows_ARunNeverResumed(t *testing.T) {
	for _, tc := range []struct {
		name, task string
		make       func(w *followsWorld)
		want       string
	}{
		{"nothing followed", "build", func(*followsWorld) {}, ""},
		{"a container's commit", "approve", func(w *followsWorld) { w.commit("ork-1", w.sha, "build", 0, "c", nil) }, ""},
		{"no commit", "approve", func(*followsWorld) {}, ReasonRequiredResultNotRecorded},
		{"a commit under a later scope only", "approve", func(w *followsWorld) { w.commit("ork-1", w.sha, "build", 7, "c", nil) }, ReasonRequiredResultNotRecorded},
		{"a commit of another admission", "approve", func(w *followsWorld) { w.commit("ork-1", otherSHA, "build", 0, "c", nil) }, ReasonUnavailable},
		{"a commit with no recording", "approve", func(w *followsWorld) {
			w.commit("ork-1", w.sha, "build", 0, "c", func(c *orkstackv1.ActivityCommit) { c.Recorded = nil })
		}, ReasonUnavailable},
		{"a commit of another task", "approve", func(w *followsWorld) {
			w.commit("ork-1", w.sha, "build", 0, "c", func(c *orkstackv1.ActivityCommit) { c.TaskId = "other" })
		}, ReasonUnavailable},
		{"a commit with no values", "approve", func(w *followsWorld) {
			w.commit("ork-1", w.sha, "build", 0, "c", func(c *orkstackv1.ActivityCommit) { c.Values = nil })
		}, ReasonUnavailable},
		{"a commit of a later format", "approve", func(w *followsWorld) {
			w.commit("ork-1", w.sha, "build", 0, "c", func(c *orkstackv1.ActivityCommit) { c.SchemaVersion = 2 })
		}, ReasonUnavailable},
		{"bytes that are no commit", "approve", func(w *followsWorld) {
			w.put("metadata/attested/ns/ork-1/build/commit.json", []byte("{"))
		}, ReasonUnavailable},
		{"a resolution", "deploy", func(w *followsWorld) { w.resolve("approve", 0, false, map[string]string{"build": "c"}) }, ""},
		{"no resolution", "deploy", func(*followsWorld) {}, ReasonRequiredResultNotRecorded},
		{"a rejection", "deploy", func(w *followsWorld) { w.resolve("approve", 0, true, map[string]string{"build": "c"}) }, ReasonRequiredResultNotRecorded},
		{"one of two", "report", func(w *followsWorld) { w.commit("ork-1", w.sha, "build", 0, "c", nil) }, ReasonRequiredResultNotRecorded},
		{"a composed task's record", "report", func(w *followsWorld) {
			w.commit("ork-1", w.sha, "build", 0, "c", nil)
			w.record(artifacts.RecordComposed, &orkstackv1.RecordedValues{SchemaVersion: 1,
				Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "ns", RunId: "ork-1", TaskId: "sub",
				AdmissionSha256: w.sha, InputBinding: make([]byte, 32), ChildRunId: "ork-2", ChildAdmissionSha256: otherSHA,
				ChildOutputsSha256: otherSHA, Values: &orkstackv1.ArtifactRef{Uri: "file:///none", Sha256: strings.Repeat("e", 64), SizeBytes: 5000}})
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newFollowsWorld(t)
			tc.make(w)
			rows, err := w.check.CheckFollows(context.Background(), w.adm, w.sha, tc.task, 0)
			w.holds(rows, err, tc.want, len(w.adm.Follows[tc.task]))
		})
	}
}

func (w *followsWorld) holds(rows []FollowedRecord, err error, want string, n int) {
	w.t.Helper()
	if want == "" {
		if err != nil || len(rows) != n {
			w.t.Fatalf("%d rows, %v; want %d", len(rows), err, n)
		}
		return
	}
	var m *MintError
	if !errors.As(err, &m) || m.Reason != want || m.Unavailable != (want == ReasonUnavailable) || rows != nil {
		w.t.Fatalf("rows %v, err %v; want %s", rows, err, want)
	}
}

// wavedReads answers no commit read before as many are waiting as one check
// makes at a time, or as are left: each answer is one round trip.
type wavedReads struct {
	FollowsStore
	mu            sync.Mutex
	left, waiting int
	wave          chan struct{}
	waves, most   int
}

func (r *wavedReads) ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error) {
	r.mu.Lock()
	r.waiting++
	r.most = max(r.most, r.waiting)
	wave := r.wave
	if r.waiting == min(followsParallel, r.left) {
		r.left -= r.waiting
		r.waiting, r.wave, r.waves = 0, make(chan struct{}), r.waves+1
		close(wave)
	}
	r.mu.Unlock()
	select {
	case <-wave:
	case <-ctx.Done():
		return artifacts.RecordRead[artifacts.CommitRecord]{}, ctx.Err()
	}
	return r.FollowsStore.ReadAttestedCommit(ctx, class, spacing, want)
}

// TestCheckFollows_AsManyRowsAsARecordHolds: a check of 180 records has sixteen
// reads in flight and is twelve round trips, and answers every row in order.
func TestCheckFollows_AsManyRowsAsARecordHolds(t *testing.T) {
	const rows = 180
	w := newFollowsWorld(t)
	for i := range rows {
		id := fmt.Sprintf("t%03d", i)
		w.adm.Follows["many"] = append(w.adm.Follows["many"], artifacts.FollowRef{TaskID: id, Kind: artifacts.FollowContainer})
	}
	w.sha = w.admission(w.adm)
	for i := range rows {
		w.commit("ork-1", w.sha, fmt.Sprintf("t%03d", i), 0, "c", nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	waved := &wavedReads{FollowsStore: w.check.Store, left: rows, wave: make(chan struct{})}
	w.check.Store = waved
	got, err := w.check.CheckFollows(ctx, w.adm, w.sha, "many", 0)
	if err != nil || len(got) != rows {
		t.Fatalf("%d rows, %v", len(got), err)
	}
	for i, row := range got {
		if want := fmt.Sprintf("t%03d", i); row.Ref.TaskID != want {
			t.Fatalf("row %d is of %s, want %s", i, row.Ref.TaskID, want)
		}
	}
	if waved.most != followsParallel || waved.waves != 12 {
		t.Fatalf("%d reads at once in %d round trips, want %d in 12", waved.most, waved.waves, followsParallel)
	}
}

// TestCheckFollows_TheRows: what is answered is each followed task's record
// digest and result digest, in the order followed.
func TestCheckFollows_TheRows(t *testing.T) {
	w := newFollowsWorld(t)
	w.commit("ork-1", w.sha, "build", 0, "c", func(c *orkstackv1.ActivityCommit) {
		c.Artifacts = []*orkstackv1.ArtifactRef{{Name: "app.tar", Sha256: strings.Repeat("f", 64)}}
		c.SystemArtifacts = []*orkstackv1.ArtifactRef{{Name: "stdout", Sha256: strings.Repeat("1", 64)}}
	})
	rows, err := w.check.CheckFollows(context.Background(), w.adm, w.sha, "approve", 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v, %v", rows, err)
	}
	want, err := ResultSHA256(strings.Repeat("c", 64), []ResultArtifact{{Name: "app.tar", SHA256: strings.Repeat("f", 64)}})
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Ref.TaskID != "build" || rows[0].ResultSHA256 != want || len(rows[0].RecordSHA256) != 64 {
		t.Fatalf("%+v; want result %s", rows[0], want)
	}
}

// TestCheckFollows_AResumedRoot: the newest resume that names a followed
// task, as run again or reopened, decides where its record counts, and one
// no resume names counts at the base; a record below the newest scope must
// still follow what is current.
func TestCheckFollows_AResumedRoot(t *testing.T) {
	base := func(w *followsWorld) {
		w.commit("ork-1", w.sha, "build", 0, "c", nil)
		w.resolve("approve", 0, false, map[string]string{"build": "c"})
	}
	for _, tc := range []struct {
		name  string
		scope uint64
		make  func(w *followsWorld)
		want  string
	}{
		{"an approval kept", 5, func(w *followsWorld) { base(w); w.resume(5, 0, []string{"deploy"}, nil) }, ""},
		{"an approval reopened and not given again", 5, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"deploy"}, []string{"approve"})
		}, ReasonRequiredResultNotRecorded},
		{"an approval reopened and given again", 5, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"deploy"}, []string{"approve"})
			w.resolve("approve", 5, false, map[string]string{"build": "c"})
		}, ""},
		{"reopened by the first of two resumes, given then, kept by the second", 9, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"deploy"}, []string{"approve"})
			w.resolve("approve", 5, false, map[string]string{"build": "c"})
			w.resume(9, 5, []string{"deploy"}, nil)
		}, ""},
		{"given under the first of two resumes, reopened by the second", 9, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"deploy"}, []string{"approve"})
			w.resolve("approve", 5, false, map[string]string{"build": "c"})
			w.resume(9, 5, []string{"deploy"}, []string{"approve"})
		}, ReasonRequiredResultNotRecorded},
		{"an approval kept over a result since replaced", 5, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"build", "deploy"}, nil)
			w.commit("ork-1", w.sha, "build", 5, "d", nil)
		}, ReasonRequiredResultNotRecorded},
		{"an approval kept over a result made again unchanged", 5, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"build", "deploy"}, nil)
			w.commit("ork-1", w.sha, "build", 5, "c", nil)
		}, ""},
		{"an approval kept over a task run again and not done", 5, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"build", "deploy"}, nil)
		}, ReasonRequiredResultNotRecorded},
		{"an approval that names nothing it follows", 5, func(w *followsWorld) {
			base(w)
			w.resolve("approve", 0, false, nil)
			w.resume(5, 0, []string{"deploy"}, nil)
		}, ReasonRequiredResultNotRecorded},
		{"a resume nothing links to", 5, func(w *followsWorld) { base(w) }, ReasonNotAdmitted},
		{"a resume whose record is gone", 5, func(w *followsWorld) {
			base(w)
			w.resume(5, 0, []string{"deploy"}, nil)
			at := filepath.Join(w.dir, filepath.FromSlash(w.resumeKey(5, 0)))
			if err := os.Rename(at, at+".gone"); err != nil {
				w.t.Fatal(err)
			}
		}, ReasonUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newFollowsWorld(t)
			tc.make(w)
			rows, err := w.check.CheckFollows(context.Background(), w.adm, w.sha, "deploy", tc.scope)
			w.holds(rows, err, tc.want, 1)
		})
	}
}

// TestCheckFollows_AChildRun: a child's task that follows nothing of its
// own run follows what the child's start follows, in the parent or above,
// each under its own run's admission, and the root's resumes decide there.
func TestCheckFollows_AChildRun(t *testing.T) {
	child := func(w *followsWorld, mutate func(*artifacts.Admission)) (artifacts.Admission, string) {
		a, _ := admitted(w.t, "ork-2", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-1",
			AdmissionSHA256: w.sha, TaskID: "sub"}, nil)
		a.RootRunID = "ork-1"
		a.StartFollows = []artifacts.FollowRef{{RunID: "ork-1", TaskID: "approve", Kind: artifacts.FollowAwaited}}
		if mutate != nil {
			mutate(&a)
		}
		return a, w.admission(a)
	}
	t.Run("the start's approval is there", func(t *testing.T) {
		w := newFollowsWorld(t)
		w.resolve("approve", 0, false, map[string]string{"build": "c"})
		a, sha := child(w, nil)
		rows, err := w.check.CheckFollows(context.Background(), a, sha, "build", 0)
		w.holds(rows, err, "", 1)
	})
	t.Run("the start's approval is not there", func(t *testing.T) {
		w := newFollowsWorld(t)
		a, sha := child(w, nil)
		rows, err := w.check.CheckFollows(context.Background(), a, sha, "build", 0)
		w.holds(rows, err, ReasonRequiredResultNotRecorded, 0)
	})
	t.Run("the root's resume reopened it", func(t *testing.T) {
		w := newFollowsWorld(t)
		w.resolve("approve", 0, false, map[string]string{"build": "c"})
		w.resume(5, 0, []string{"sub"}, []string{"approve"})
		a, sha := child(w, nil)
		rows, err := w.check.CheckFollows(context.Background(), a, sha, "build", 5)
		w.holds(rows, err, ReasonRequiredResultNotRecorded, 0)
	})
	t.Run("a grandparent's approval", func(t *testing.T) {
		w := newFollowsWorld(t)
		w.resolve("approve", 0, false, map[string]string{"build": "c"})
		_, mid := child(w, nil)
		a, _ := admitted(t, "ork-3", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-2", AdmissionSHA256: mid, TaskID: "sub"}, nil)
		a.RootRunID, a.StartFollows = "ork-1", []artifacts.FollowRef{{RunID: "ork-1", TaskID: "approve", Kind: artifacts.FollowAwaited}}
		rows, err := w.check.CheckFollows(context.Background(), a, w.admission(a), "build", 0)
		w.holds(rows, err, "", 1)
	})
	t.Run("a run that is not above it", func(t *testing.T) {
		w := newFollowsWorld(t)
		a, sha := child(w, func(a *artifacts.Admission) { a.StartFollows[0].RunID = "ork-9" })
		rows, err := w.check.CheckFollows(context.Background(), a, sha, "build", 0)
		w.holds(rows, err, ReasonNotAdmitted, 0)
	})
	t.Run("its own task's commit counts at the base, whatever the root's scope", func(t *testing.T) {
		w := newFollowsWorld(t)
		w.resume(5, 0, []string{"sub", "build"}, nil)
		a, sha := child(w, func(a *artifacts.Admission) {
			a.Follows = map[string][]artifacts.FollowRef{"deploy": {{TaskID: "build", Kind: artifacts.FollowContainer}}}
		})
		w.commit("ork-2", sha, "build", 0, "c", nil)
		rows, err := w.check.CheckFollows(context.Background(), a, sha, "deploy", 5)
		w.holds(rows, err, "", 1)
	})
}

// TestCheckScope: an attempt's scope is the newest of its root, by the
// link, and a root's own attempt is of a task that scope runs again.
func TestCheckScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		child  bool
		task   string
		scopes Scopes
		make   func(w *followsWorld)
		want   string
	}{
		{"never resumed", false, "deploy", Scopes{}, func(*followsWorld) {}, ""},
		{"resumed since", false, "deploy", Scopes{}, func(w *followsWorld) { w.resume(5, 0, []string{"deploy"}, nil) }, ReasonNotAdmitted},
		{"the newest, which runs it again", false, "deploy", Scopes{Resume: 5}, func(w *followsWorld) { w.resume(5, 0, []string{"deploy"}, nil) }, ""},
		{"the newest, which does not", false, "build", Scopes{Resume: 5}, func(w *followsWorld) { w.resume(5, 0, []string{"deploy"}, nil) }, ReasonNotAdmitted},
		{"an older one", false, "deploy", Scopes{Resume: 5}, func(w *followsWorld) {
			w.resume(5, 0, []string{"deploy"}, nil)
			w.resume(9, 5, []string{"deploy"}, nil)
		}, ReasonNotAdmitted},
		{"one no resume opened", false, "deploy", Scopes{Resume: 5}, func(*followsWorld) {}, ReasonNotAdmitted},
		{"a root that names a root's scope", false, "deploy", Scopes{Root: 5}, func(w *followsWorld) { w.resume(5, 0, []string{"deploy"}, nil) }, ReasonNotAdmitted},
		{"a link that does not decode", false, "deploy", Scopes{}, func(w *followsWorld) { w.put(w.linkKey(0), []byte("{")) }, ReasonUnavailable},
		{"a resume record that does not decode", false, "deploy", Scopes{Resume: 5}, func(w *followsWorld) {
			w.resume(5, 0, []string{"deploy"}, nil)
			w.put(w.resumeKey(5, 0), []byte("{"))
		}, ReasonUnavailable},
		{"a later plan of the scope, which no link names", false, "deploy", Scopes{Resume: 5}, func(w *followsWorld) {
			w.resume(5, 0, []string{"deploy"}, nil)
			w.unlinked(5, 1, "build")
		}, ""},
		{"a task only a plan no link names runs again", false, "build", Scopes{Resume: 5}, func(w *followsWorld) {
			w.resume(5, 0, []string{"deploy"}, nil)
			w.unlinked(5, 1, "build")
		}, ReasonNotAdmitted},
		{"a record with no link", false, "deploy", Scopes{Resume: 5}, func(w *followsWorld) { w.unlinked(5, 0, "deploy") }, ReasonNotAdmitted},
		{"a child of the newest", true, "build", Scopes{Root: 5}, func(w *followsWorld) { w.resume(5, 0, []string{"sub"}, nil) }, ""},
		{"a child of an older one", true, "build", Scopes{}, func(w *followsWorld) { w.resume(5, 0, []string{"sub"}, nil) }, ReasonNotAdmitted},
		{"a child that names a scope of its own", true, "build", Scopes{Resume: 5}, func(w *followsWorld) { w.resume(5, 0, []string{"sub"}, nil) }, ReasonNotAdmitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newFollowsWorld(t)
			tc.make(w)
			adm := w.adm
			if tc.child {
				adm, _ = admitted(t, "ork-2", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-1", AdmissionSHA256: w.sha, TaskID: "sub"}, nil)
				adm.RootRunID = "ork-1"
			}
			err := w.check.CheckScope(context.Background(), adm, tc.task, tc.scopes)
			if got := reasonOf(err); (tc.want == "" && err != nil) || (tc.want != "" && got != tc.want) {
				t.Fatalf("err = %v; want %q", err, tc.want)
			}
		})
	}
}

// downStore is a record store that does not answer.
type downStore struct{ FollowsStore }

var errDown = &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("bucket unreachable")}

func (downStore) ResumeChain(context.Context, string, string) (artifacts.RecordRead[[]artifacts.ResumeRecord], error) {
	return artifacts.RecordRead[[]artifacts.ResumeRecord]{}, errDown
}
func (downStore) ReadAttestedCommit(context.Context, artifacts.SlotClass, *artifacts.RecordSpacings, artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error) {
	return artifacts.RecordRead[artifacts.CommitRecord]{}, errDown
}

// TestCheckFollows_AStoreThatDoesNotAnswer: never a miss.
func TestCheckFollows_AStoreThatDoesNotAnswer(t *testing.T) {
	w := newFollowsWorld(t)
	w.check.Store = downStore{w.store}
	if err := w.check.CheckScope(context.Background(), w.adm, "deploy", Scopes{}); reasonOf(err) != ReasonUnavailable {
		t.Fatalf("scope: %v", err)
	}
	rows, err := w.check.CheckFollows(context.Background(), w.adm, w.sha, "approve", 0)
	w.holds(rows, err, ReasonUnavailable, 0)
}

// TestCheckFollows_ACancelledAsk: never a miss.
func TestCheckFollows_ACancelledAsk(t *testing.T) {
	w := newFollowsWorld(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows, err := w.check.CheckFollows(ctx, w.adm, w.sha, "approve", 0)
	w.holds(rows, err, ReasonUnavailable, 0)
}

// TestMint_WhatATaskFollows: step 6a holds the mint, before the attempt cap
// is spent, and lets it through once the followed task's result is recorded.
func TestMint_WhatATaskFollows(t *testing.T) {
	w := newMintWorld(t)
	adm := w.adm
	adm.Follows = map[string][]artifacts.FollowRef{"build": {{TaskID: "prep", Kind: artifacts.FollowContainer}}}
	req := w.request()
	req.AdmissionSHA256 = w.write(adm)
	w.starts.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: req.AdmissionSHA256})
	w.minter.Follows = &FollowsCheck{Store: w.store, Spacing: fastSpacings}
	if _, err := w.mint(req); reasonOf(err) != ReasonRequiredResultNotRecorded {
		t.Fatalf("mint = %v; want %s", err, ReasonRequiredResultNotRecorded)
	}
	// The refused mint took no slot: another attempt may still hold it.
	f := &followsWorld{t: t, dir: w.dir, store: w.store}
	f.commit("ork-1", req.AdmissionSHA256, "prep", 0, "c", nil)
	if tokens, err := w.mint(req); err != nil || len(tokens) != 1 {
		t.Fatalf("mint = %d tokens, %v", len(tokens), err)
	}
}

// TestCheckReceived: what a task stages of its run's start is what the records
// of the tasks that produced it say, two runs up as one, and never what the
// cluster carried; a failure handler's artifact whose producer left no record
// is left out, and any other missing one is refused.
func TestCheckReceived(t *testing.T) {
	ctx := context.Background()
	app := func(c *orkstackv1.ActivityCommit) {
		c.Artifacts = []*orkstackv1.ArtifactRef{{Name: "app.tar", Uri: "file:///app", Sha256: strings.Repeat("f", 64), SizeBytes: 7}}
	}
	// below is a child two runs below ork-1, which receives bundle from
	// ork-1's build and notes from ork-2's composed task sub.
	below := func(w *followsWorld, edit func(*artifacts.Admission)) (artifacts.Admission, string) {
		mid, _ := admitted(w.t, "ork-2", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-1", AdmissionSHA256: w.sha, TaskID: "sub"}, nil)
		mid.RootRunID = "ork-1"
		midSHA := w.admission(mid)
		a, _ := admitted(w.t, "ork-3", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-2", AdmissionSHA256: midSHA, TaskID: "inner"}, nil)
		a.RootRunID = "ork-1"
		a.StartArtifacts = map[string]artifacts.StartArtifact{
			"bundle": {RunID: "ork-1", TaskID: "build", Kind: artifacts.FollowContainer, Name: "app.tar"},
			"notes":  {RunID: "ork-2", TaskID: "sub", Kind: artifacts.FollowComposed, Name: "notes.md"},
		}
		row := a.Tasks["build"]
		row.Inputs = []string{"bundle", "notes"}
		a.Tasks["build"] = row
		w.record(artifacts.RecordComposed, &orkstackv1.RecordedValues{SchemaVersion: 1,
			Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "ns", RunId: "ork-2", TaskId: "sub",
			AdmissionSha256: midSHA, InputBinding: make([]byte, 32), ChildRunId: "ork-9", ChildAdmissionSha256: otherSHA,
			ChildOutputsSha256: otherSHA, Values: &orkstackv1.ArtifactRef{Uri: "file:///none", Sha256: strings.Repeat("e", 64), SizeBytes: 5000},
			Artifacts: []*orkstackv1.ArtifactRef{{Name: "notes.md", Uri: "file:///notes", Sha256: strings.Repeat("1", 64)}}})
		if edit != nil {
			edit(&a)
		}
		return a, w.admission(a)
	}
	t.Run("both", func(t *testing.T) {
		w := newFollowsWorld(t)
		w.commit("ork-1", w.sha, "build", 0, "c", app)
		a, sha := below(w, nil)
		got, err := w.check.CheckReceived(ctx, a, sha, "build", 0)
		if err != nil || len(got) != 2 || got[0].GetName() != "bundle" || got[0].GetUri() != "file:///app" || got[0].GetSizeBytes() != 7 ||
			got[1].GetName() != "notes" || got[1].GetSha256() != strings.Repeat("1", 64) {
			t.Fatalf("%v, %v", got, err)
		}
	})
	t.Run("a resume ran the producer again", func(t *testing.T) {
		w := newFollowsWorld(t)
		w.commit("ork-1", w.sha, "build", 0, "c", app)
		w.resume(5, 0, []string{"build", "sub"}, nil)
		w.commit("ork-1", w.sha, "build", 5, "d", func(c *orkstackv1.ActivityCommit) {
			app(c)
			c.Artifacts[0].Uri = "file:///app-again"
		})
		a, sha := below(w, nil)
		got, err := w.check.CheckReceived(ctx, a, sha, "build", 5)
		if err != nil || got[0].GetUri() != "file:///app-again" {
			t.Fatalf("%v, %v", got, err)
		}
	})
	t.Run("a producer left standing over a result since replaced", func(t *testing.T) {
		w := newFollowsWorld(t)
		w.adm.Follows["sub"] = []artifacts.FollowRef{{TaskID: "build", Kind: artifacts.FollowContainer}}
		w.sha = w.admission(w.adm)
		w.commit("ork-1", w.sha, "build", 0, "c", app)
		w.record(artifacts.RecordComposed, &orkstackv1.RecordedValues{SchemaVersion: 1,
			Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "ns", RunId: "ork-1", TaskId: "sub",
			AdmissionSha256: w.sha, InputBinding: make([]byte, 32), ChildRunId: "ork-9", ChildAdmissionSha256: otherSHA,
			ChildOutputsSha256: otherSHA, Values: &orkstackv1.ArtifactRef{Uri: "file:///none", Sha256: strings.Repeat("e", 64), SizeBytes: 5000},
			Artifacts: []*orkstackv1.ArtifactRef{{Name: "out", Uri: "file:///out", Sha256: strings.Repeat("1", 64)}},
			Follows:   []*orkstackv1.RecordedSource{{TaskId: "build", RecordSha256: strings.Repeat("b", 64), ResultSha256: resultOf(t, "c")}}})
		w.resume(5, 0, []string{"build"}, nil)
		for n := range 4 {
			// First while what it follows has no record, then while it has another.
			handler := n%2 == 1
			if n == 2 {
				w.commit("ork-1", w.sha, "build", 5, "d", app)
			}
			a, _ := admitted(t, "ork-2", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-1", AdmissionSHA256: w.sha, TaskID: "next"}, nil)
			a.RootRunID = "ork-1"
			a.StartArtifacts = map[string]artifacts.StartArtifact{"in": {RunID: "ork-1", TaskID: "sub", Kind: artifacts.FollowComposed, Name: "out", Handler: handler}}
			row := a.Tasks["build"]
			row.Inputs = []string{"in"}
			a.Tasks["build"] = row
			if got, err := w.check.CheckReceived(ctx, a, w.admission(a), "build", 5); reasonOf(err) != ReasonRequiredResultNotRecorded || got != nil {
				t.Fatalf("passed by a failure handler %v, replaced %v: %v, %v", handler, n >= 2, got, err)
			}
		}
	})
	refused := func(name string, edit func(*artifacts.Admission), make func(*followsWorld)) {
		t.Run(name, func(t *testing.T) {
			w := newFollowsWorld(t)
			make(w)
			a, sha := below(w, edit)
			got, err := w.check.CheckReceived(ctx, a, sha, "build", 0)
			var m *MintError
			if !errors.As(err, &m) || m.Reason != ReasonRequiredResultNotRecorded || m.Task != "build" || got != nil {
				t.Fatalf("%v, %v", got, err)
			}
		})
	}
	refused("no record of the producer", nil, func(*followsWorld) {})
	handlers := func(a *artifacts.Admission) {
		src := a.StartArtifacts["bundle"]
		src.Handler = true
		a.StartArtifacts["bundle"] = src
	}
	noArtifact := func(w *followsWorld) { w.commit("ork-1", w.sha, "build", 0, "c", nil) }
	refused("a record with no such artifact", nil, noArtifact)
	refused("a failure handler's, whose record names no such artifact", handlers, noArtifact)
	t.Run("a failure handler's, with no record", func(t *testing.T) {
		w := newFollowsWorld(t)
		a, sha := below(w, handlers)
		got, err := w.check.CheckReceived(ctx, a, sha, "build", 0)
		// Its name alone: the Orker takes no other unrecorded one.
		if err != nil || len(got) != 2 || got[0].GetName() != "bundle" || got[0].GetUri() != "" || got[0].GetSha256() != "" ||
			got[1].GetName() != "notes" || got[1].GetUri() == "" {
			t.Fatalf("%v, %v", got, err)
		}
	})
	t.Run("a store that does not answer", func(t *testing.T) {
		w := newFollowsWorld(t)
		a, sha := below(w, nil)
		w.check.Store = downStore{w.store}
		if got, err := w.check.CheckReceived(ctx, a, sha, "build", 0); reasonOf(err) != ReasonUnavailable || got != nil {
			t.Fatalf("%v, %v", got, err)
		}
	})
}
