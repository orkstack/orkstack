package engine

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const recordsAdmission = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// recordsWorld is an ended admitted run over a real local record store:
// build, approve (an await that follows build), ship (composed, which follows
// approve), fan (a fan-out of build) and review (an await that follows build
// through fan).
type recordsWorld struct {
	t     *testing.T
	dir   string
	store *artifacts.RecordStore
	ask   ResumeRecordsAsk
	// faults fail the read of a key that holds the part as a store that does
	// not answer fails it: a fault no real store produces on demand at one key
	// (AGENTS.md §11.1).
	faults map[string]error
	mu     sync.Mutex
	// commitReads keeps what a verification of the run read.
	commitReads *readCommits
	reads       []string
}

func newRecordsWorld(t *testing.T) *recordsWorld {
	t.Helper()
	dir := t.TempDir()
	store, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	await := func(id string, needs ...string) *orkstackv1.Task {
		return &orkstackv1.Task{Id: id, Needs: needs, Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}}
	}
	fan := ptask("fan", "build")
	fan.Each = &orkstackv1.EachSpec{MaxItems: 5}
	ship := &orkstackv1.Task{Id: "ship", Needs: []string{"approve"}, Uses: "orkflow:release@1", Orkflow: &orkstackv1.OrkflowRef{Name: "release", Version: "1"}}
	awaiting := func(id string, status orkstackv1.Status) *orkstackv1.TaskRun {
		row := prow(id, status)
		row.Await = &orkstackv1.TaskAwait{Deadline: timestamppb.New(time.Now().Add(time.Hour))}
		return row
	}
	w := &recordsWorld{t: t, dir: dir, store: store, faults: map[string]error{}}
	w.ask = ResumeRecordsAsk{
		Definition: &orkstackv1.Orkflow{Name: "wf", Tasks: []*orkstackv1.Task{ptask("build"), ptask("alone"), await("approve", "build"), ship, fan, await("review", "fan")}},
		Base: &orkstackv1.Run{Id: "ork-x", Namespace: "default", TaskRuns: []*orkstackv1.TaskRun{prow("build", sOK), prow("alone", sOK),
			awaiting("approve", sOK), prow("ship", sOK), prow("fan", sOK), awaiting("review", sWait)}},
		AdmissionSHA256: recordsAdmission, End: time.Now().Add(time.Minute),
		Spacings: &artifacts.RecordSpacings{MissConfirm: time.Millisecond, ReaskInitial: time.Millisecond, ReaskMax: time.Millisecond},
	}
	return w
}

func (w *recordsWorld) row(id string) *orkstackv1.TaskRun {
	for _, r := range w.ask.Base.GetTaskRuns() {
		if r.GetTaskId() == id {
			return r
		}
	}
	w.t.Fatalf("no row %s", id)
	return nil
}

func (w *recordsWorld) fault(key string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reads = append(w.reads, key)
	for part, err := range w.faults {
		if strings.Contains(key, part) {
			return err
		}
	}
	return nil
}

func (w *recordsWorld) ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, sp *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error) {
	if err := w.fault("/" + want.TaskID + "/commit"); err != nil {
		return artifacts.RecordRead[artifacts.CommitRecord]{}, err
	}
	return w.store.ReadAttestedCommit(ctx, class, sp, want)
}

func (w *recordsWorld) ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, sp *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error) {
	if err := w.fault(key); err != nil {
		return artifacts.RecordRead[*artifacts.RecordedFound]{}, err
	}
	return w.store.ReadRecordedValues(ctx, class, sp, key, want)
}

// commit records task's result of raw at scope, and answers its digest.
func (w *recordsWorld) commit(task string, scope uint64, raw string, edit func(*orkstackv1.ActivityCommit)) string {
	w.t.Helper()
	ref := w.put(raw)
	key, err := artifacts.ScopedCommitKey("default", "ork-x", task, scope)
	if err != nil {
		w.t.Fatal(err)
	}
	c := &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: "ork-x", TaskId: task, Values: ref,
		Artifacts: []*orkstackv1.ArtifactRef{{Name: "out", Uri: ref.GetUri(), Sha256: ref.GetSha256(), SizeBytes: ref.GetSizeBytes()}},
		Recorded:  &orkstackv1.CommitRecording{AdmissionSha256: recordsAdmission}}
	if edit != nil {
		edit(c)
	}
	if _, err := w.store.RecordCommit(context.Background(), key, c); err != nil {
		w.t.Fatal(err)
	}
	digest, err := identity.ResultSHA256(ref.GetSha256(), []identity.ResultArtifact{{Name: "out", SHA256: ref.GetSha256()}})
	if err != nil {
		w.t.Fatal(err)
	}
	return digest
}

func (w *recordsWorld) put(raw string) *orkstackv1.ArtifactRef {
	w.t.Helper()
	values, err := artifacts.NewLocalStore(w.dir)
	if err != nil {
		w.t.Fatal(err)
	}
	ref, err := values.Put(context.Background(), "values.json", strings.NewReader(raw), artifacts.PutOptions{})
	if err != nil {
		w.t.Fatal(err)
	}
	return &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}
}

// values records an awaited (kind RESOLUTION; a rejection when raw is empty)
// or a composed task's record at scope, which follows what follows says.
func (w *recordsWorld) values(task string, kind orkstackv1.RecordedValuesKind, scope uint64, raw string, follows map[string]string, edit func(*orkstackv1.RecordedValues)) (file string) {
	w.t.Helper()
	ctx := context.Background()
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: kind, Namespace: "default", RunId: "ork-x", TaskId: task, ResumeScope: scope,
		AdmissionSha256: recordsAdmission, InputBinding: make([]byte, 32), Rejected: raw == ""}
	if raw != "" {
		rec.Values = w.put(raw)
	}
	for id, digest := range follows {
		rec.Follows = append(rec.Follows, &orkstackv1.RecordedSource{TaskId: id, RecordSha256: strings.Repeat("b", 64), ResultSha256: digest})
	}
	of := artifacts.RecordComposed
	if kind == orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION {
		of = artifacts.RecordResolution
		detail, err := w.store.WriteResolutionDetail(ctx, &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "default", RunId: "ork-x", TaskId: task, ResumeScope: scope,
			Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice", PrincipalVerified: true})
		if err != nil {
			w.t.Fatal(err)
		}
		rec.DetailSha256 = detail
	} else {
		rec.ChildRunId, rec.ChildAdmissionSha256, rec.ChildOutputsSha256 = "ork-child", recordsAdmission, strings.Repeat("c", 64)
	}
	if edit != nil {
		edit(rec)
	}
	if _, err := w.store.WriteRecordedValues(ctx, rec); err != nil {
		w.t.Fatal(err)
	}
	if row := w.row(task); row.GetStatus() == sOK && rec.GetValues() != nil {
		row.ValuesRef = &orkstackv1.ArtifactRef{Name: "values.json", Uri: rec.GetValues().GetUri(), Sha256: rec.GetValues().GetSha256(), SizeBytes: rec.GetValues().GetSizeBytes()}
		if rec.GetChildRunId() != "" {
			row.Child = &orkstackv1.TaskChild{RunId: rec.GetChildRunId()}
		}
	}
	key, err := artifacts.RecordKey(of, "default", "ork-x", task, scope)
	if err != nil {
		w.t.Fatal(err)
	}
	return filepath.Join(w.dir, filepath.FromSlash(key))
}

func (w *recordsWorld) resolution(task string, scope uint64, raw string, follows map[string]string, edit func(*orkstackv1.RecordedValues)) string {
	return w.values(task, orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, scope, raw, follows, edit)
}

func (w *recordsWorld) composed(scope uint64, raw string, follows map[string]string, edit func(*orkstackv1.RecordedValues)) string {
	return w.values("ship", orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, scope, raw, follows, edit)
}

func (w *recordsWorld) read() map[string]ResumeRecord {
	w.t.Helper()
	got, err := ReadResumeRecords(context.Background(), w, w.ask)
	if err != nil {
		w.t.Fatal(err)
	}
	return got
}

func unansweredStore() error {
	return fmt.Errorf("read: %w", &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("no answer")})
}

// TestReadResumeRecords_WhatARunLeft: the digest of each task any record
// follows, what each record follows, and no read of anything else: a fan-out
// leaves no record, and a task nothing follows needs no digest.
func TestReadResumeRecords_WhatARunLeft(t *testing.T) {
	w := newRecordsWorld(t)
	build := w.commit("build", 0, `{"n":1}`, nil)
	w.commit("alone", 0, `{"n":2}`, nil)
	w.resolution("approve", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
	approve, err := identity.ResultSHA256(w.row("approve").GetValuesRef().GetSha256(), nil)
	if err != nil {
		t.Fatal(err)
	}
	w.composed(0, `{"digest":"d"}`, map[string]string{"approve": approve}, nil)
	w.resolution("review", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
	got := w.read()
	for id, want := range map[string]ResumeRecord{
		"build":   {Answer: KeptValid, ResultSHA256: build},
		"approve": {Answer: KeptValid, ResultSHA256: approve, Follows: []ResumeFollow{{"build", build}}},
		"ship":    {Answer: KeptValid, Follows: []ResumeFollow{{"approve", approve}}},
		"review":  {Answer: KeptValid, Follows: []ResumeFollow{{"build", build}}, Resolution: ResolutionAnswer{Completed: true}},
	} {
		g := got[id]
		if g.Answer != want.Answer || (want.ResultSHA256 != "" && g.ResultSHA256 != want.ResultSHA256) || !slices.Equal(g.Follows, want.Follows) ||
			g.Resolution != want.Resolution || g.Cause != nil {
			t.Errorf("%s = %+v, want %+v", id, g, want)
		}
	}
	if got["ship"].Values.GetSha256() != w.row("ship").GetValuesRef().GetSha256() || got["review"].Values.GetUri() == "" {
		t.Errorf("values: ship %v, review %v", got["ship"].Values, got["review"].Values)
	}
	if len(got) != 4 {
		t.Errorf("answers for %d tasks: %v", len(got), got)
	}
	for _, key := range w.reads {
		if strings.Contains(key, "/alone/") || strings.Contains(key, "/fan/") {
			t.Errorf("read %s", key)
		}
	}
}

// TestReadResumeRecords_AtTheRowsOwnScope: a record counts at the scope its
// row's result was made under, and no other.
func TestReadResumeRecords_AtTheRowsOwnScope(t *testing.T) {
	w := newRecordsWorld(t)
	old := w.commit("build", 0, `{"n":1}`, nil)
	now := w.commit("build", 42, `{"n":2}`, nil)
	w.row("build").ResultEpoch = 42
	w.row("approve").ResultEpoch = 42
	w.ask.Resumes = []artifacts.ResumeRecord{{Scope: 42, Tasks: []string{"build"}, ReopenedAwaits: []string{"approve"}}}
	w.resolution("approve", 0, `{"ok":true}`, map[string]string{"build": old}, nil)
	got := w.read()
	if got["build"].ResultSHA256 != now || got["approve"].Answer != KeptGone {
		t.Errorf("build %+v, approve %+v: want build's at 42, and no answer of approve's there", got["build"], got["approve"])
	}
	w.resolution("approve", 42, `{"ok":false}`, map[string]string{"build": now}, nil)
	if got := w.read()["approve"]; got.Answer != KeptValid || !slices.Equal(got.Follows, []ResumeFollow{{"build", now}}) {
		t.Errorf("approve = %+v, want its answer at 42", got)
	}
}

// TestReadResumeRecords_Answers: every answer of the store, for a kept
// answer, a kept composed task, a followed commit and an open await.
func TestReadResumeRecords_Answers(t *testing.T) {
	setup := func(w *recordsWorld) {
		build := w.commit("build", 0, `{"n":1}`, nil)
		follows := map[string]string{"build": build}
		w.resolution("approve", 0, `{"ok":true}`, follows, nil)
		w.resolution("review", 0, `{"ok":true}`, follows, nil)
		w.composed(0, `{"digest":"d"}`, nil, nil)
	}
	resolve := func(w *recordsWorld, task string, edit func(*orkstackv1.RecordedValues)) {
		w.resolution(task, 0, `{"ok":true}`, nil, edit)
	}
	for name, tc := range map[string]struct {
		make       func(*recordsWorld)
		task       string
		want       KeptAnswer
		resolution ResolutionAnswer
		why        string
		cause      bool
	}{
		"a kept answer":                {setup, "approve", KeptValid, ResolutionAnswer{}, "", false},
		"a kept answer with no record": {func(*recordsWorld) {}, "approve", KeptGone, ResolutionAnswer{}, "not on record", true},
		"a kept answer not answered":   {func(w *recordsWorld) { setup(w); w.faults["/approve/"] = unansweredStore() }, "approve", KeptUnavailable, ResolutionAnswer{}, "did not answer", true},
		"a kept answer of another admission": {func(w *recordsWorld) {
			resolve(w, "approve", func(r *orkstackv1.RecordedValues) { r.AdmissionSha256 = strings.Repeat("d", 64) })
		}, "approve", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"a kept answer that is a rejection": {func(w *recordsWorld) { w.resolution("approve", 0, "", nil, nil) }, "approve", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"a kept answer of other values":     {func(w *recordsWorld) { setup(w); w.row("approve").ValuesRef.Sha256 = strings.Repeat("e", 64) }, "approve", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"a kept answer stored late": {func(w *recordsWorld) {
			setup(w)
			w.row("approve").Await.Deadline = timestamppb.New(time.Now().Add(-time.Hour))
		}, "approve", KeptGone, ResolutionAnswer{}, "after", true},
		"a kept answer with no deadline": {func(w *recordsWorld) { setup(w); w.row("approve").Await.Deadline = nil }, "approve", KeptGone, ResolutionAnswer{}, "after", true},
		"a kept composed task":           {setup, "ship", KeptValid, ResolutionAnswer{}, "", false},
		"a kept composed task, artifacts it did not return": {func(w *recordsWorld) {
			setup(w)
			w.row("ship").Artifacts = []*orkstackv1.ArtifactRef{w.put(`{}`)}
			w.row("ship").Artifacts[0].Name = "planted"
		}, "ship", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"a kept composed task, another child":        {func(w *recordsWorld) { setup(w); w.row("ship").Child.RunId = "ork-other" }, "ship", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"a kept answer at another uri":               {func(w *recordsWorld) { setup(w); w.row("ship").ValuesRef.Uri += "x" }, "ship", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"an open await at a scope no resume gave it": {func(w *recordsWorld) { setup(w); w.row("review").ResultEpoch = 7 }, "review", 0, ResolutionAnswer{}, "scope", false},
		"a kept composed task, no record":            {func(*recordsWorld) {}, "ship", KeptGone, ResolutionAnswer{}, "not on record", true},
		"a kept composed task, unanswered":           {func(w *recordsWorld) { setup(w); w.faults["/ship/"] = unansweredStore() }, "ship", KeptUnavailable, ResolutionAnswer{}, "did not answer", true},
		"a followed commit, no record":               {func(*recordsWorld) {}, "build", KeptGone, ResolutionAnswer{}, "not on record", true},
		"a followed commit, unanswered":              {func(w *recordsWorld) { setup(w); w.faults["/build/"] = unansweredStore() }, "build", KeptUnavailable, ResolutionAnswer{}, "did not answer", true},
		"a followed commit of another admission": {func(w *recordsWorld) {
			w.commit("build", 0, `{}`, func(c *orkstackv1.ActivityCommit) { c.Recorded.AdmissionSha256 = strings.Repeat("d", 64) })
		}, "build", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"an open await, answered":   {setup, "review", KeptValid, ResolutionAnswer{Completed: true}, "", false},
		"an open await, rejected":   {func(w *recordsWorld) { w.resolution("review", 0, "", nil, nil) }, "review", KeptValid, ResolutionAnswer{Rejected: true}, "", false},
		"an open await, no record":  {func(*recordsWorld) {}, "review", 0, ResolutionAnswer{}, "", false},
		"an open await, unanswered": {func(w *recordsWorld) { setup(w); w.faults["/review/"] = unansweredStore() }, "review", KeptUnavailable, ResolutionAnswer{}, "did not answer", true},
		"an open await, not its own": {func(w *recordsWorld) {
			resolve(w, "review", func(r *orkstackv1.RecordedValues) { r.AdmissionSha256 = strings.Repeat("d", 64) })
		}, "review", KeptUnreadable, ResolutionAnswer{}, "cannot be read", true},
		"an open await, answered after its deadline": {func(w *recordsWorld) {
			setup(w)
			w.row("review").Await.Deadline = timestamppb.New(time.Now().Add(-time.Hour))
		}, "review", 0, ResolutionAnswer{}, "after", false},
		"an open await, answered after the run ended": {func(w *recordsWorld) { setup(w); w.ask.End = time.Now().Add(-time.Hour) }, "review", 0, ResolutionAnswer{}, "after", false},
		"an await that never opened":                  {func(w *recordsWorld) { setup(w); w.row("review").Await.Deadline = nil }, "review", 0, ResolutionAnswer{}, "", false},
		"a failed await, answered in time":            {func(w *recordsWorld) { setup(w); w.row("review").Status = sFail }, "review", KeptValid, ResolutionAnswer{Completed: true}, "", false},
		"a skipped await":                             {func(w *recordsWorld) { setup(w); w.row("review").Status = sSkip }, "review", 0, ResolutionAnswer{}, "", false},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRecordsWorld(t)
			tc.make(w)
			got := w.read()[tc.task]
			if got.Answer != tc.want || got.Resolution != tc.resolution || !strings.Contains(got.Why, tc.why) || (tc.why == "") != (got.Why == "") || (got.Cause != nil) != tc.cause {
				t.Errorf("%+v, want %v %+v, why %q", got, tc.want, tc.resolution, tc.why)
			}
			if got.Newer {
				t.Errorf("a record of this build's format is said to be newer: %+v", got)
			}
			if got.Answer != KeptValid && (got.ResultSHA256 != "" || got.Follows != nil || got.Values != nil) {
				t.Errorf("an answer %v names a result: %+v", got.Answer, got)
			}
			if strings.Contains(got.Why, "no answer") || strings.Contains(got.Why, recordsAdmission) {
				t.Errorf("why says the store's own words: %q", got.Why)
			}
		})
	}
}

// TestReadResumeRecords_AFormatThisBuildDoesNotRead: a record of a newer
// format is unreadable, never gone: what it says is not known.
func TestReadResumeRecords_AFormatThisBuildDoesNotRead(t *testing.T) {
	w := newRecordsWorld(t)
	file := w.resolution("approve", 0, `{"ok":true}`, nil, nil)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	newer := regexp.MustCompile(`"schemaVersion":\s*1`).ReplaceAllString(string(raw), `"schemaVersion":99`)
	if newer == string(raw) {
		t.Fatalf("no version in %s", raw)
	}
	if err := os.WriteFile(file, []byte(newer), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := w.read()["approve"]; got.Answer != KeptUnreadable || !got.Newer || !strings.Contains(got.Why, "newer") {
		t.Errorf("%+v, want unreadable", got)
	}
}

// TestReadResumeRecords_Refuses: an ask that names no run or admission, and a
// context that ended, answer nothing.
func TestReadResumeRecords_Refuses(t *testing.T) {
	for name, change := range map[string]func(*ResumeRecordsAsk){
		"no definition": func(a *ResumeRecordsAsk) { a.Definition = nil },
		"no run":        func(a *ResumeRecordsAsk) { a.Base = nil },
		"no admission":  func(a *ResumeRecordsAsk) { a.AdmissionSHA256 = "" },
		"no end":        func(a *ResumeRecordsAsk) { a.End = time.Time{} },
	} {
		w := newRecordsWorld(t)
		change(&w.ask)
		if got, err := ReadResumeRecords(context.Background(), w, w.ask); err == nil {
			t.Errorf("%s: %v", name, got)
		}
	}
	w := newRecordsWorld(t)
	if got, err := ReadResumeRecords(context.Background(), nil, w.ask); err == nil {
		t.Errorf("no store: %v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := ReadResumeRecords(ctx, w, w.ask); !errors.Is(err, context.Canceled) {
		t.Errorf("a canceled read = %v, %v", got, err)
	}
}

// silentStore answers no read until it is given up on.
type silentStore struct{}

func (silentStore) ReadAttestedCommit(ctx context.Context, _ artifacts.SlotClass, _ *artifacts.RecordSpacings, _ artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error) {
	<-ctx.Done()
	return artifacts.RecordRead[artifacts.CommitRecord]{}, ctx.Err()
}

func (silentStore) ReadRecordedValues(ctx context.Context, _ artifacts.SlotClass, _ *artifacts.RecordSpacings, _ string, _ artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error) {
	<-ctx.Done()
	return artifacts.RecordRead[*artifacts.RecordedFound]{}, ctx.Err()
}

// TestReadResumeRecords_ASilentStoreIsGivenUpOn: a store that never answers is
// unavailable once the read's own bound has passed, for a caller with none.
func TestReadResumeRecords_ASilentStoreIsGivenUpOn(t *testing.T) {
	w := newRecordsWorld(t)
	w.ask.ReadTimeout = 5 * time.Millisecond
	got, err := ReadResumeRecords(context.Background(), silentStore{}, w.ask)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"build", "approve", "ship", "review"} {
		if got[id].Answer != KeptUnavailable {
			t.Errorf("%s = %+v, want unavailable", id, got[id])
		}
	}
}

// TestReadResumeRecords_TheStateIsHeldToTheResumes: a result the state places
// at a scope a later resume replaced, or at one no resume opened, is not kept,
// though a real record of the task lies there.
func TestReadResumeRecords_TheStateIsHeldToTheResumes(t *testing.T) {
	for name, tc := range map[string]struct {
		epoch   int64
		resumes []artifacts.ResumeRecord
	}{
		"replaced by a resume that ran it again": {0, []artifacts.ResumeRecord{{Scope: 42, Tasks: []string{"build"}, ReopenedAwaits: []string{"approve"}}}},
		"replaced by the newer of two": {42, []artifacts.ResumeRecord{{Scope: 60, PreviousScope: 42, Tasks: []string{"build"}, ReopenedAwaits: []string{"approve"}},
			{Scope: 42, Tasks: []string{"build"}, ReopenedAwaits: []string{"approve"}}}},
		"a resume that does not name it": {42, []artifacts.ResumeRecord{{Scope: 42, Tasks: []string{"other"}}}},
		"a run never resumed":            {42, nil},
		"not a scope":                    {-1, nil},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRecordsWorld(t)
			w.ask.Resumes = tc.resumes
			at := uint64(max(tc.epoch, 0))
			w.commit("build", at, `{"n":1}`, nil)
			w.resolution("approve", at, `{"ok":true}`, nil, nil)
			w.row("build").ResultEpoch, w.row("approve").ResultEpoch = tc.epoch, tc.epoch
			got, held := w.read(), ResumeScopesHeld(w.ask)
			for _, id := range []string{"build", "approve"} {
				if got[id].Answer != KeptUnreadable || got[id].Newer || held[nodeUnit(id)] == nil {
					t.Errorf("%s = %+v, held %v; want unreadable", id, got[id], held[nodeUnit(id)])
				}
			}
		})
	}
}

// TestResumeScopesHeld: every result a resume could keep is held, a container
// nothing follows and a fan-out's items included, and nothing else is.
func TestResumeScopesHeld(t *testing.T) {
	item := func(i int32, status orkstackv1.Status, epoch int64) *orkstackv1.TaskRun {
		row := pitem(i, status)
		row.ResultEpoch = epoch
		return row
	}
	w := newRecordsWorld(t)
	w.ask.Resumes = []artifacts.ResumeRecord{{Scope: 60, PreviousScope: 42, Tasks: []string{"fan"}}, {Scope: 42, Tasks: []string{"fan", "alone"}, ReopenedAwaits: []string{"review"}}}
	w.row("fan").ResultEpoch = 60
	w.row("fan").Items = []*orkstackv1.TaskRun{item(0, sOK, 0), item(1, sOK, 42), item(2, sOK, 60), item(3, sOK, 50), item(4, sFail, 50)}
	w.row("alone").ResultEpoch, w.row("review").ResultEpoch = 42, 42
	if held := ResumeScopesHeld(w.ask); len(held) != 1 || held[ResumeUnit{Task: "fan", Item: 3}] == nil {
		t.Errorf("held = %v, want fan[3] alone", held)
	}
	w.row("alone").ResultEpoch, w.row("review").ResultEpoch, w.row("ship").ResultEpoch = 0, 0, 42
	held := ResumeScopesHeld(w.ask)
	for _, id := range []string{"alone", "review", "ship"} {
		if held[nodeUnit(id)] == nil {
			t.Errorf("%s is not held: %v", id, held)
		}
	}
	// What no result of is kept is not asked: one that failed, was skipped, or
	// never opened.
	w.row("alone").Status, w.row("ship").Status, w.row("review").Await.Deadline = sFail, sSkip, nil
	if held := ResumeScopesHeld(w.ask); len(held) != 1 {
		t.Errorf("held = %v, want fan[3] alone", held)
	}
}

// TestReadResumeRecords_ARecordWhoseValuesAreGone: a record that stands and
// whose values object is gone keeps nothing, and is no record that cannot be
// read.
func TestReadResumeRecords_ARecordWhoseValuesAreGone(t *testing.T) {
	w := newRecordsWorld(t)
	w.resolution("approve", 0, `{"ok":true}`, nil, nil)
	values, err := artifacts.NewLocalStore(w.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := values.Delete(context.Background(), w.row("approve").GetValuesRef().GetUri()); err != nil {
		t.Fatal(err)
	}
	if got := w.read()["approve"]; got.Answer != KeptGone || got.Newer {
		t.Errorf("%+v, want gone", got)
	}
}

// TestAdoptRecorded_ReadsAnAdmittedRunsSettledTaskFromItsRecord: a settled
// await or composed task of a run whose results the engine records is read
// back from its record at its scope, as ResumeRun verified it, and from
// nothing else.
func TestAdoptRecorded_ReadsAnAdmittedRunsSettledTaskFromItsRecord(t *testing.T) {
	task := func(w *recordsWorld, id string) *orkstackv1.Task {
		for _, d := range w.ask.Definition.GetTasks() {
			if d.GetId() == id {
				return d
			}
		}
		t.Fatalf("no task %s", id)
		return nil
	}
	read := func(w *recordsWorld, id string, scope uint64, admission string, values *orkstackv1.ArtifactRef) (*orkstackv1.ExecuteTaskResult, error) {
		store, err := artifacts.NewLocalStore(w.dir)
		if err != nil {
			t.Fatal(err)
		}
		acts := NewResumeActivities(nil, store).WithRecordReads(w)
		in := adoptInput(recordedEntryOf(w.ask.Definition, "ork-x", task(w, id), values, scope, admission))
		// A miss is confirmed after the reader's own two seconds, within the read's bound.
		in.GetTimeout = durationpb.New(5 * time.Second)
		out, err := acts.AdoptRecorded(context.Background(), in)
		if err != nil {
			return nil, err
		}
		return out.GetEntries()[0].GetResult(), nil
	}
	for name, tc := range map[string]struct {
		make   func(*recordsWorld)
		task   string
		scope  uint64
		edit   func(*recordsWorld) (admission string, values *orkstackv1.ArtifactRef)
		reason string
	}{
		"an answer":           {func(w *recordsWorld) { w.resolution("approve", 0, `{"ok":true}`, nil, nil) }, "approve", 0, nil, ""},
		"a composed task":     {func(w *recordsWorld) { w.composed(0, `{"d":1}`, nil, nil) }, "ship", 0, nil, ""},
		"at a resume's scope": {func(w *recordsWorld) { w.resolution("approve", 42, `{"ok":true}`, nil, nil) }, "approve", 42, nil, ""},
		"none":                {func(*recordsWorld) {}, "approve", 0, nil, ReasonValuesMissing},
		"its values gone": {func(w *recordsWorld) {
			w.resolution("approve", 0, `{"ok":true}`, nil, nil)
			store, _ := artifacts.NewLocalStore(w.dir)
			_ = store.Delete(context.Background(), w.row("approve").GetValuesRef().GetUri())
		}, "approve", 0, nil, ReasonValuesMissing},
		"another scope's": {func(w *recordsWorld) { w.resolution("approve", 0, `{"ok":true}`, nil, nil) }, "approve", 42, nil, ReasonValuesMissing},
		"another admission": {func(w *recordsWorld) { w.resolution("approve", 0, `{"ok":true}`, nil, nil) }, "approve", 0,
			func(w *recordsWorld) (string, *orkstackv1.ArtifactRef) {
				return strings.Repeat("d", 64), w.row("approve").GetValuesRef()
			}, ResolutionRecordInvalidCode},
		"other values than kept": {func(w *recordsWorld) { w.resolution("approve", 0, `{"ok":true}`, nil, nil) }, "approve", 0,
			func(w *recordsWorld) (string, *orkstackv1.ArtifactRef) {
				return recordsAdmission, w.put(`{"ok":false}`)
			}, ResolutionRecordInvalidCode},
		"a rejection": {func(w *recordsWorld) { w.resolution("approve", 0, "", nil, nil) }, "approve", 0,
			func(w *recordsWorld) (string, *orkstackv1.ArtifactRef) { return recordsAdmission, w.put(`{"ok":true}`) }, ResolutionRecordInvalidCode},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRecordsWorld(t)
			tc.make(w)
			admission, values := recordsAdmission, w.row(tc.task).GetValuesRef()
			if values == nil {
				values = w.put(`{"ok":true}`)
			}
			if tc.edit != nil {
				admission, values = tc.edit(w)
			}
			got, err := read(w, tc.task, tc.scope, admission, values)
			if tc.reason == "" {
				if err != nil || got.GetValuesRef().GetSha256() != values.GetSha256() || len(got.GetValuesJson()) == 0 {
					t.Fatalf("%v, %v", got, err)
				}
				return
			}
			if categoryOf(err).appType() != tc.reason || !isFinal(err) {
				t.Errorf("err = %v, want final %s", err, tc.reason)
			}
		})
	}
	w := newRecordsWorld(t)
	w.resolution("approve", 0, `{"ok":true}`, nil, nil)
	w.faults["/approve/"] = unansweredStore()
	if _, err := read(w, "approve", 0, recordsAdmission, w.row("approve").GetValuesRef()); categoryOf(err).appType() != ReasonCommitReadFailed || isFinal(err) {
		t.Errorf("a store that does not answer = %v, want a retryable COMMIT_READ_FAILED", err)
	}
	// A record of a newer format is a newer replica's to read: never final.
	w = newRecordsWorld(t)
	file := w.resolution("approve", 0, `{"ok":true}`, nil, nil)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, regexp.MustCompile(`"schemaVersion":\s*1`).ReplaceAll(raw, []byte(`"schemaVersion":99`)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := read(w, "approve", 0, recordsAdmission, w.row("approve").GetValuesRef()); categoryOf(err).appType() != ReasonCommitReadFailed || isFinal(err) {
		t.Errorf("a record of a newer format = %v, want a retryable COMMIT_READ_FAILED", err)
	}
}

// TestVerifyRecorded_AnAnswerTheRunNeverApplied: an await that did not
// succeed and is answered on record in time is settled from its record: the
// planner keeps it, and the block carries the row its record gives.
func TestVerifyRecorded_AnAnswerTheRunNeverApplied(t *testing.T) {
	w := newRecordsWorld(t)
	w.row("review").Status = sCanc
	w.row("review").ScheduledAt, w.row("review").StartedAt = timestamppb.Now(), timestamppb.Now()
	// The rows of what the run took, as a run that ended leaves them.
	for _, id := range []string{"approve", "ship"} {
		r := w.row(id)
		r.ScheduledAt, r.StartedAt, r.FinishedAt = timestamppb.Now(), timestamppb.Now(), timestamppb.Now()
	}
	w.row("approve").Await.ResolvedVia, w.row("approve").Await.ResolvedBy = orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, "alice"
	w.row("ship").Child = &orkstackv1.TaskChild{RunId: "ork-child"}
	build := w.commit("build", 0, `{"n":1}`, identified("build", 0))
	w.commit("alone", 0, `{"n":2}`, identified("alone", 0))
	w.resolution("approve", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
	approve, err := identity.ResultSHA256(w.row("approve").GetValuesRef().GetSha256(), nil)
	if err != nil {
		t.Fatal(err)
	}
	w.composed(0, `{"d":1}`, map[string]string{"approve": approve}, nil)
	w.resolution("review", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
	verified := w.recordedVerify(t)
	answered := verified.Evidence.Answered["review"]
	if answered == nil || answered.GetValuesRef().GetUri() == "" || answered.GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API ||
		answered.GetAwait().GetResolvedBy() != "alice" || !answered.GetAwait().GetResolvedByVerified() {
		t.Fatalf("answered = %v", answered)
	}
	in := ResumePlanInput{Definition: w.ask.Definition, Base: w.ask.Base, Evidence: verified.Evidence,
		Facts: ResumeBaseFacts{Dispatched: map[ResumeUnit]bool{}, ResetPoint: ResetPoint{CompletedEventID: 9, ScheduledEventID: 7, StartedEventID: 8}}}
	for _, row := range w.ask.Base.GetTaskRuns() {
		in.Facts.Dispatched[nodeUnit(row.GetTaskId())] = true
	}
	w.ask.Base.Status, w.ask.Base.Error = sFail, &orkstackv1.Error{Code: "RUN_FAILED"}
	w.row("alone").Status = sFail
	plan, err := PlanResume(in)
	if err != nil {
		t.Fatal(err)
	}
	if u := unitOfPlan(plan, "review"); u.Disposition != DispositionSettle {
		t.Fatalf("review = %+v, want settled from its record", u)
	}
	block, err := BuildResumeBlock(ResumeBlockInput{Input: in, Plan: plan, Verification: verified, SeqBase: 100, Scope: 100})
	if err != nil {
		t.Fatal(err)
	}
	units, err := DecodeResumeUnits(in.Definition, block, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range units {
		if u.Task == "review" && u.Row.GetValuesRef().GetSha256() != answered.GetValuesRef().GetSha256() {
			t.Errorf("the block settles review with %v", u.Row)
		}
	}
}

func unitOfPlan(p *ResumePlan, task string) ResumePlanUnit {
	for _, u := range p.Units {
		if u.Unit == nodeUnit(task) {
			return u
		}
	}
	return ResumePlanUnit{}
}

// TestVerifyRecorded_AKeptAnswersRowIsTheRecords: a kept approval whose row in
// the run's state names another values object, size and approver, with its
// digest left as the record's, is settled with the record's row: the state of
// a recorded run is no word on who answered, how, or with what.
func TestVerifyRecorded_AKeptAnswersRowIsTheRecords(t *testing.T) {
	w := newRecordsWorld(t)
	for _, id := range []string{"approve", "ship"} {
		r := w.row(id)
		r.ScheduledAt, r.StartedAt, r.FinishedAt = timestamppb.Now(), timestamppb.Now(), timestamppb.Now()
	}
	w.row("approve").Await.ResolvedVia, w.row("approve").Await.ResolvedBy = orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, "alice"
	w.row("ship").Child = &orkstackv1.TaskChild{RunId: "ork-child"}
	build := w.commit("build", 0, `{"n":1}`, identified("build", 0))
	w.commit("alone", 0, `{"n":2}`, identified("alone", 0))
	w.resolution("approve", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
	approve, err := identity.ResultSHA256(w.row("approve").GetValuesRef().GetSha256(), nil)
	if err != nil {
		t.Fatal(err)
	}
	w.composed(0, `{"d":1}`, map[string]string{"approve": approve}, nil)
	real := proto.Clone(w.row("approve").GetValuesRef()).(*orkstackv1.ArtifactRef)
	forged := w.row("approve")
	forged.ValuesRef.Uri, forged.ValuesRef.SizeBytes = real.GetUri()+".forged", real.GetSizeBytes()+7
	forged.Await.ResolvedBy, forged.Await.ResolvedVia = "mallory", orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK
	verified := w.recordedVerify(t)
	in := ResumePlanInput{Definition: w.ask.Definition, Base: w.ask.Base, Evidence: verified.Evidence,
		Facts: ResumeBaseFacts{Dispatched: map[ResumeUnit]bool{}, ResetPoint: ResetPoint{CompletedEventID: 9, ScheduledEventID: 7, StartedEventID: 8}}}
	for _, row := range w.ask.Base.GetTaskRuns() {
		in.Facts.Dispatched[nodeUnit(row.GetTaskId())] = true
	}
	w.ask.Base.Status, w.ask.Base.Error = sFail, &orkstackv1.Error{Code: "RUN_FAILED"}
	w.row("alone").Status = sFail
	plan, err := PlanResume(in)
	if err != nil {
		t.Fatal(err)
	}
	if u := unitOfPlan(plan, "approve"); u.Disposition != DispositionSettle {
		t.Fatalf("approve = %+v, want kept", u)
	}
	block, err := BuildResumeBlock(ResumeBlockInput{Input: in, Plan: plan, Verification: verified, SeqBase: 100, Scope: 100})
	if err != nil {
		t.Fatal(err)
	}
	units, err := DecodeResumeUnits(in.Definition, block, false)
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, u := range units {
		if u.Task != "approve" {
			continue
		}
		seen = true
		ref, aw := u.Row.GetValuesRef(), u.Row.GetAwait()
		if ref.GetUri() != real.GetUri() || ref.GetSizeBytes() != real.GetSizeBytes() || ref.GetSha256() != real.GetSha256() ||
			aw.GetResolvedBy() != "alice" || aw.GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API || !aw.GetResolvedByVerified() {
			t.Errorf("the block keeps approve as %v, want the record's row", u.Row)
		}
	}
	if !seen {
		t.Fatal("the block has no unit for approve")
	}
}

// outputs records child run child's outputs as an engine records them, under
// its admission; raw empty records them without their values object.
func (w *recordsWorld) outputs(child, raw string) *orkstackv1.ArtifactRef {
	w.t.Helper()
	ref := w.put(raw)
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, Namespace: "default",
		RunId: child, AdmissionSha256: childAdmission, InputBinding: make([]byte, 32), Values: ref,
		Artifacts: []*orkstackv1.ArtifactRef{{Name: "report", Uri: ref.GetUri(), Sha256: ref.GetSha256(), SizeBytes: ref.GetSizeBytes()}}}
	if _, err := w.store.WriteRecordedValues(context.Background(), rec); err != nil {
		w.t.Fatal(err)
	}
	return ref
}

const childAdmission = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

// TestReadResumeRecords_AComposedTaskSettledFromItsChild: a composed task that
// ended for want of its values' record, or that a force cancel left running,
// is settled from its own record when it has one, and otherwise from the
// outputs' record of the child the run's plan names once that child
// succeeded; its row is the records'. Any other ending reads nothing.
func TestReadResumeRecords_AComposedTaskSettledFromItsChild(t *testing.T) {
	unstored := func(w *recordsWorld) {
		r := w.row("ship")
		r.Status, r.Error = sFail, &orkstackv1.Error{Code: ChildValuesUnstoredCode}
		r.Child = &orkstackv1.TaskChild{RunId: "ork-forged"}
	}
	succeeded := map[string]ResumeChildAsk{"ship": {RunID: "ork-child", AdmissionSHA256: childAdmission, Succeeded: true}}
	for name, tc := range map[string]struct {
		make          func(*recordsWorld)
		children      map[string]ResumeChildAsk
		forceCanceled bool
		want          func(*testing.T, *recordsWorld, ResumeRecord, bool)
	}{
		"from its child's outputs": {func(w *recordsWorld) { unstored(w); w.outputs("ork-child", `{"d":1}`) }, succeeded, false,
			func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
				row := got.Answered
				if !read || got.Answer != KeptValid || !got.ChildOnRecord || !got.FromOutputs || got.ResultSHA256 == "" || row == nil ||
					row.GetStatus() != sOK || row.GetError() != nil || row.GetChild().GetRunId() != "ork-child" ||
					len(row.GetArtifacts()) != 1 || row.GetValuesRef().GetSha256() != got.Values.GetSha256() {
					t.Fatalf("%+v", got)
				}
			}},
		"from its own record": {func(w *recordsWorld) { unstored(w); w.composed(0, `{"d":1}`, nil, nil) }, succeeded, false,
			func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
				if !read || got.Answer != KeptValid || !got.ChildOnRecord || got.FromOutputs || got.Answered.GetChild().GetRunId() != "ork-child" {
					t.Fatalf("%+v", got)
				}
			}},
		"its child did not succeed": {func(w *recordsWorld) { unstored(w); w.outputs("ork-child", `{"d":1}`) },
			map[string]ResumeChildAsk{"ship": {RunID: "ork-child", AdmissionSHA256: childAdmission}}, false,
			func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
				if !read || got.Answer == KeptValid || got.ChildCause != ComposedChildNotSucceeded {
					t.Fatalf("%+v", got)
				}
			}},
		"no outputs on record": {unstored, succeeded, false, func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
			if !read || got.Answer == KeptValid || got.ChildCause != ComposedChildOutputsUnavailable {
				t.Fatalf("%+v", got)
			}
		}},
		"another admission's outputs": {func(w *recordsWorld) { unstored(w); w.outputs("ork-child", `{"d":1}`) },
			map[string]ResumeChildAsk{"ship": {RunID: "ork-child", AdmissionSHA256: strings.Repeat("e", 64), Succeeded: true}}, false,
			func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
				if !read || got.Answer != KeptUnreadable {
					t.Fatalf("%+v", got)
				}
			}},
		"the store does not answer": {func(w *recordsWorld) {
			unstored(w)
			w.outputs("ork-child", `{"d":1}`)
			w.faults["ork-child"] = unansweredStore()
		},
			succeeded, false, func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
				if !read || got.Answer != KeptUnavailable {
					t.Fatalf("%+v", got)
				}
			}},
		"a run resumed before": {func(w *recordsWorld) {
			unstored(w)
			w.outputs("ork-child", `{"d":1}`)
			w.ask.Resumes = []artifacts.ResumeRecord{{Scope: 42, Tasks: []string{"build"}}}
		}, succeeded, false, func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
			if !read || got.Answer == KeptValid || got.ChildCause != ComposedResumedBefore {
				t.Fatalf("%+v", got)
			}
		}},
		"no child in the plan": {unstored, nil, false, func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
			if !read || got.Answer != KeptUnreadable {
				t.Fatalf("%+v", got)
			}
		}},
		"another ending": {func(w *recordsWorld) {
			r := w.row("ship")
			r.Status, r.Error = sFail, &orkstackv1.Error{Code: "CHILD_FAILED"}
			w.outputs("ork-child", `{"d":1}`)
		}, succeeded, false, func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
			if read {
				t.Fatalf("%+v", got)
			}
		}},
		"running, not force-cancelled": {func(w *recordsWorld) { w.row("ship").Status = sRun; w.outputs("ork-child", `{"d":1}`) }, succeeded, false,
			func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
				if read {
					t.Fatalf("%+v", got)
				}
			}},
		"running at a force cancel": {func(w *recordsWorld) { w.row("ship").Status = sRun; w.outputs("ork-child", `{"d":1}`) }, succeeded, true,
			func(t *testing.T, w *recordsWorld, got ResumeRecord, read bool) {
				if !read || got.Answer != KeptValid || !got.FromOutputs {
					t.Fatalf("%+v", got)
				}
			}},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRecordsWorld(t)
			tc.make(w)
			w.ask.Children, w.ask.ForceCanceled = tc.children, tc.forceCanceled
			got, read := w.read()["ship"]
			tc.want(t, w, got, read)
		})
	}
}

// TestVerifyRecorded_AComposedTaskSettledFromItsChild: a run that failed
// because a composed task's values were not recorded is resumed with that task
// settled from its child's outputs, its row as the records give it; a need
// that runs again withdraws it, and a child that did not succeed refuses the
// resume with its own cause.
func TestVerifyRecorded_AComposedTaskSettledFromItsChild(t *testing.T) {
	setup := func(t *testing.T, children map[string]ResumeChildAsk, failApprove bool, edits ...func(*recordsWorld)) (*recordsWorld, ResumePlanInput, ResumeVerification) {
		w := newRecordsWorld(t)
		for _, id := range []string{"approve", "ship"} {
			r := w.row(id)
			r.ScheduledAt, r.StartedAt, r.FinishedAt = timestamppb.Now(), timestamppb.Now(), timestamppb.Now()
		}
		w.row("approve").Await.ResolvedVia, w.row("approve").Await.ResolvedBy = orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, "alice"
		build := w.commit("build", 0, `{"n":1}`, identified("build", 0))
		w.commit("alone", 0, `{"n":2}`, identified("alone", 0))
		w.resolution("approve", 0, `{"ok":true}`, map[string]string{"build": build}, nil)
		ship := w.row("ship")
		ship.Status, ship.Error = sFail, &orkstackv1.Error{Code: ChildValuesNotRecordedCode}
		ship.Child = &orkstackv1.TaskChild{RunId: "ork-forged"}
		w.outputs("ork-child", `{"d":1}`)
		if failApprove {
			w.row("build").Status = sFail
		}
		w.ask.Children = children
		w.ask.Base.Status, w.ask.Base.Error = sFail, &orkstackv1.Error{Code: "RUN_FAILED"}
		for _, edit := range edits {
			edit(w)
		}
		verified := w.recordedVerify(t)
		in := ResumePlanInput{Definition: w.ask.Definition, Base: w.ask.Base, Evidence: verified.Evidence,
			Facts: ResumeBaseFacts{Dispatched: map[ResumeUnit]bool{}, ResetPoint: ResetPoint{CompletedEventID: 9, ScheduledEventID: 7, StartedEventID: 8}}}
		for _, row := range w.ask.Base.GetTaskRuns() {
			in.Facts.Dispatched[nodeUnit(row.GetTaskId())] = true
		}
		return w, in, verified
	}
	succeeded := map[string]ResumeChildAsk{"ship": {RunID: "ork-child", AdmissionSHA256: childAdmission, Succeeded: true}}
	t.Run("settled", func(t *testing.T) {
		_, in, verified := setup(t, succeeded, false)
		plan, err := PlanResume(in)
		if err != nil {
			t.Fatal(err)
		}
		if u := unitOfPlan(plan, "ship"); u.Disposition != DispositionSettle {
			t.Fatalf("ship = %+v, want settled from its child", u)
		}
		block, err := BuildResumeBlock(ResumeBlockInput{Input: in, Plan: plan, Verification: verified, SeqBase: 100, Scope: 100})
		if err != nil {
			t.Fatal(err)
		}
		units, err := DecodeResumeUnits(in.Definition, block, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range units {
			if u.Task == "ship" && (u.Row.GetChild().GetRunId() != "ork-child" || u.Row.GetValuesRef().GetSha256() == "" || len(u.Row.GetArtifacts()) != 1) {
				t.Errorf("the block settles ship as %v", u.Row)
			}
		}
	})
	t.Run("a need runs again", func(t *testing.T) {
		_, in, _ := setup(t, succeeded, true)
		_, err := PlanResume(in)
		var r *ResumeRefusal
		if !errors.As(err, &r) || r.Code != RefuseComposedTask {
			t.Fatalf("err = %v, want COMPOSED_TASK", err)
		}
	})
	t.Run("its run was resumed before", func(t *testing.T) {
		_, in, _ := setup(t, succeeded, false, func(w *recordsWorld) {
			w.ask.Resumes = []artifacts.ResumeRecord{{Scope: 42, Tasks: []string{"review"}}}
		})
		_, err := PlanResume(in)
		var r *ResumeRefusal
		if !errors.As(err, &r) || r.Code != RefuseComposedTask || r.Cause != string(ComposedResumedBefore) {
			t.Fatalf("err = %v, want COMPOSED_TASK resumed_before", err)
		}
	})
	t.Run("its child did not succeed", func(t *testing.T) {
		_, in, _ := setup(t, map[string]ResumeChildAsk{"ship": {RunID: "ork-child", AdmissionSHA256: childAdmission}}, false)
		_, err := PlanResume(in)
		var r *ResumeRefusal
		if !errors.As(err, &r) || r.Code != RefuseComposedTask || r.Cause != string(ComposedChildNotSucceeded) {
			t.Fatalf("err = %v, want COMPOSED_TASK child_not_succeeded", err)
		}
	})
}

// TestHoldChildInputs: what a composed task's row says it passed its child is
// held to its producer's record; anything else makes its record unreadable,
// and an artifact received from a run above is left as it is.
func TestHoldChildInputs(t *testing.T) {
	out := &orkstackv1.ArtifactRef{Name: "out", Uri: "file:///o", Sha256: strings.Repeat("a", 64), SizeBytes: 3}
	received := []NamedArtifactPlan{{Name: "seed", RunID: "ork-root", TaskID: "gen", Artifact: ArtifactRefPlan{URI: "file:///s", SHA256: strings.Repeat("5", 64)}}}
	passed := func(edit func(*orkstackv1.NamedArtifact)) map[string]ResumeRecord {
		in := &orkstackv1.NamedArtifact{Name: "src", RunId: "ork-x", TaskId: "build",
			Artifact: &orkstackv1.ArtifactRef{Name: "src", Uri: out.GetUri(), Sha256: out.GetSha256(), SizeBytes: 3, Metadata: map[string]string{ArtifactSourceNameKey: "out"}}}
		if edit != nil {
			edit(in)
		}
		return map[string]ResumeRecord{
			"build": {Answer: KeptValid, Artifacts: []*orkstackv1.ArtifactRef{out}},
			"ship":  {Answer: KeptValid, Answered: &orkstackv1.TaskRun{TaskId: "ship", Child: &orkstackv1.TaskChild{InputArtifacts: []*orkstackv1.NamedArtifact{in}}}},
			"alone": {Answer: KeptGone},
		}
	}
	for name, tc := range map[string]struct {
		edit func(*orkstackv1.NamedArtifact)
		want KeptAnswer
	}{
		"as its producer recorded it":            {nil, KeptValid},
		"another object":                         {func(n *orkstackv1.NamedArtifact) { n.Artifact.Sha256 = strings.Repeat("f", 64) }, KeptUnreadable},
		"another place":                          {func(n *orkstackv1.NamedArtifact) { n.Artifact.Uri = "file:///elsewhere" }, KeptUnreadable},
		"another of its artifacts":               {func(n *orkstackv1.NamedArtifact) { n.Artifact.Metadata[ArtifactSourceNameKey] = "log" }, KeptUnreadable},
		"a producer whose record does not stand": {func(n *orkstackv1.NamedArtifact) { n.TaskId = "alone" }, KeptValid},
		"a producer that leaves no record":       {func(n *orkstackv1.NamedArtifact) { n.TaskId = "fan" }, KeptUnreadable},
		"as its run received it": {func(n *orkstackv1.NamedArtifact) {
			n.RunId, n.TaskId, n.Artifact.Uri, n.Artifact.Sha256 = "ork-root", "gen", "file:///s", strings.Repeat("5", 64)
		}, KeptValid},
		"not as its run received it": {func(n *orkstackv1.NamedArtifact) {
			n.RunId, n.TaskId, n.Artifact.Sha256 = "ork-root", "gen", strings.Repeat("f", 64)
		}, KeptUnreadable},
	} {
		t.Run(name, func(t *testing.T) {
			records := passed(tc.edit)
			HoldChildInputs(records, "ork-x", received)
			if got := records["ship"].Answer; got != tc.want {
				t.Errorf("ship = %v, want %v", records["ship"], tc.want)
			}
		})
	}
	// A chain: a composed task passes its child what another composed task's
	// child returned, and the first passed its own child something its
	// producer does not hold. Whatever order they are visited in, the second
	// is held to the first as it was read.
	for range 50 {
		bad := &orkstackv1.NamedArtifact{Name: "src", RunId: "ork-x", TaskId: "build",
			Artifact: &orkstackv1.ArtifactRef{Uri: "file:///forged", Sha256: strings.Repeat("f", 64), Metadata: map[string]string{ArtifactSourceNameKey: "out"}}}
		first := &orkstackv1.NamedArtifact{Name: "rep", RunId: "ork-x", TaskId: "a",
			Artifact: &orkstackv1.ArtifactRef{Uri: out.GetUri(), Sha256: out.GetSha256(), SizeBytes: 3, Metadata: map[string]string{ArtifactSourceNameKey: "out"}}}
		records := map[string]ResumeRecord{
			"build": {Answer: KeptValid, Artifacts: []*orkstackv1.ArtifactRef{out}},
			"a": {Answer: KeptValid, Artifacts: []*orkstackv1.ArtifactRef{out},
				Answered: &orkstackv1.TaskRun{TaskId: "a", Child: &orkstackv1.TaskChild{InputArtifacts: []*orkstackv1.NamedArtifact{bad}}}},
			"b": {Answer: KeptValid, Answered: &orkstackv1.TaskRun{TaskId: "b", Child: &orkstackv1.TaskChild{InputArtifacts: []*orkstackv1.NamedArtifact{first}}}},
		}
		HoldChildInputs(records, "ork-x", nil)
		if records["a"].Answer != KeptUnreadable || records["b"].Answer != KeptValid {
			t.Fatalf("a = %v, b = %v", records["a"].Answer, records["b"].Answer)
		}
	}
}

// TestReadResumeRecords_AComposedTaskOnALaterResume: on a run resumed before,
// a composed task the first resume settled is kept from its own record at the
// scope it ran under, and so is one whose record a confirm wrote and whose
// reset never ran; neither is to be recorded again.
func TestReadResumeRecords_AComposedTaskOnALaterResume(t *testing.T) {
	for name, failed := range map[string]bool{"settled by the first resume": false, "recorded by a confirm that never reset": true} {
		t.Run(name, func(t *testing.T) {
			w := newRecordsWorld(t)
			w.composed(0, `{"d":1}`, nil, nil)
			if failed {
				r := w.row("ship")
				r.Status, r.Error = sFail, &orkstackv1.Error{Code: ChildValuesUnstoredCode}
			}
			w.ask.Resumes = []artifacts.ResumeRecord{{Scope: 42, Tasks: []string{"review"}}}
			w.ask.Children = map[string]ResumeChildAsk{"ship": {RunID: "ork-child", AdmissionSHA256: childAdmission, Succeeded: true}}
			got := w.read()["ship"]
			if got.Answer != KeptValid || got.FromOutputs || got.ChildOnRecord != failed || got.ChildCause != "" {
				t.Fatalf("%+v", got)
			}
		})
	}
}
