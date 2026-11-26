package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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
	t      *testing.T
	dir    string
	store  *artifacts.RecordStore
	ask    ResumeRecordsAsk
	faults map[string]error // by a key's part
	mu     sync.Mutex
	reads  []string
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
		"a kept answer with no deadline":   {func(w *recordsWorld) { setup(w); w.row("approve").Await.Deadline = nil }, "approve", KeptGone, ResolutionAnswer{}, "after", true},
		"a kept composed task":             {setup, "ship", KeptValid, ResolutionAnswer{}, "", false},
		"a kept composed task, no record":  {func(*recordsWorld) {}, "ship", KeptGone, ResolutionAnswer{}, "not on record", true},
		"a kept composed task, unanswered": {func(w *recordsWorld) { setup(w); w.faults["/ship/"] = unansweredStore() }, "ship", KeptUnavailable, ResolutionAnswer{}, "did not answer", true},
		"a followed commit, no record":     {func(*recordsWorld) {}, "build", KeptGone, ResolutionAnswer{}, "not on record", true},
		"a followed commit, unanswered":    {func(w *recordsWorld) { setup(w); w.faults["/build/"] = unansweredStore() }, "build", KeptUnavailable, ResolutionAnswer{}, "did not answer", true},
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
