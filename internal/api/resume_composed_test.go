package api_test

import (
	"context"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// composedShape is a failed run with a composed task c.
type composedShape struct {
	tasks []*orkstackv1.Task
	rows  []*orkstackv1.TaskRun
	from  string
	// cause is the composed cause; reason is the refusal's when it is not
	// COMPOSED_TASK.
	cause, reason string
	// change is made last, to the whole fixture.
	change func(*testing.T, *resumeFixture)
}

func imageTask(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Uses: "image:busybox", Needs: needs}
}

func childTask(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Uses: "orkflow:release@1", Needs: needs,
		Orkflow: &orkstackv1.OrkflowRef{Name: "release", Version: "1"}}
}

func rowOf(id string, status orkstackv1.Status, code ...string) *orkstackv1.TaskRun {
	row := &orkstackv1.TaskRun{TaskId: id, Status: status}
	for _, c := range code {
		row.Error = &orkstackv1.Error{Code: c}
	}
	return row
}

// childRow is c's row once its child ended: putComposed stores its values.
func childRow(status orkstackv1.Status) *orkstackv1.TaskRun {
	at := func(m int) *timestamppb.Timestamp {
		return timestamppb.New(resumeEpoch.Add(time.Duration(m) * time.Minute))
	}
	return &orkstackv1.TaskRun{TaskId: "c", Status: status, ScheduledAt: at(1), StartedAt: at(2), FinishedAt: at(3),
		Child: &orkstackv1.TaskChild{OrkflowName: "release", OrkflowVersion: "1", RunId: "ork-child"}}
}

// putComposed stores the values of every succeeded composed or awaited row,
// and the commit of every succeeded container's.
func (f *resumeFixture) putComposed(t *testing.T) {
	t.Helper()
	for i, row := range f.run.GetTaskRuns() {
		if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			continue
		}
		if f.def.Tasks[i].GetUses() == "image:busybox" && f.def.Tasks[i].GetEach() == nil {
			f.commit(t, row.GetTaskId(), nil)
		}
		if row.GetChild() != nil {
			f.putValues(t, row)
		}
	}
}

func (f *resumeFixture) putValues(t *testing.T, row *orkstackv1.TaskRun) {
	t.Helper()
	ref, err := f.store.Put(context.Background(), "values.json", strings.NewReader(`{"digest":"d"}`), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	row.ValuesRef = &orkstackv1.ArtifactRef{Name: "values.json", Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}
}

// recordComposed records, for a run that holds an identity, what each composed
// row's values are, as the engine records them when its child ends.
func (f *resumeFixture) recordComposed(t *testing.T) {
	t.Helper()
	for _, row := range f.run.GetTaskRuns() {
		if row.GetChild() == nil || row.GetValuesRef().GetUri() == "" {
			continue
		}
		v := row.GetValuesRef()
		rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "default",
			RunId: "ork-r", TaskId: row.GetTaskId(), ResumeScope: uint64(max(row.GetResultEpoch(), 0)), AdmissionSha256: resumeAdmission,
			InputBinding: make([]byte, 32), Values: &orkstackv1.ArtifactRef{Uri: v.GetUri(), Sha256: v.GetSha256(), SizeBytes: v.GetSizeBytes()},
			Artifacts: row.GetArtifacts(), ChildRunId: "ork-child", ChildAdmissionSha256: resumeAdmission, ChildOutputsSha256: resumeAdmission}
		if _, err := f.recordStore.WriteRecordedValues(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
	}
}

func gone(ref *orkstackv1.ArtifactRef) *orkstackv1.ArtifactRef {
	return &orkstackv1.ArtifactRef{Name: ref.GetName(), Uri: strings.Replace(ref.GetUri(), ref.GetSha256()[:8], "00000000", 1), SizeBytes: 3}
}

const (
	stOK   = orkstackv1.Status_STATUS_SUCCEEDED
	stFail = orkstackv1.Status_STATUS_FAILED
	stCanc = orkstackv1.Status_STATUS_CANCELED
	stPend = orkstackv1.Status_STATUS_PENDING
	stSkip = orkstackv1.Status_STATUS_SKIPPED
)

func composedShapes() map[string]composedShape {
	await := func(id string, needs ...string) *orkstackv1.Task {
		return &orkstackv1.Task{Id: id, Needs: needs, Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}}
	}
	awaitRow := func(status orkstackv1.Status) *orkstackv1.TaskRun {
		row := rowOf("w", status)
		row.Await = &orkstackv1.TaskAwait{}
		return row
	}
	fan := imageTask("fan", "build")
	fan.Each = &orkstackv1.EachSpec{Over: "x", MaxItems: 5, Source: &orkstackv1.EachSource{TaskId: "build", Path: []string{"xs"}}}
	fanRow := rowOf("fan", stFail)
	item0, item1 := int32(0), int32(1)
	fanRow.Items = []*orkstackv1.TaskRun{{TaskId: "fan", Item: &item0, Status: stFail}, {TaskId: "fan", Item: &item1, Status: stFail}}
	handler := childTask("c", "x")
	handler.Join = orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED
	unread := func(id, code string, d orkstackv1.ResumeDisposition) *orkstackv1.TaskRun {
		row := rowOf(id, stFail, code)
		row.ResumeDisposition = d
		return row
	}
	unreadChild := childRow(stFail)
	unreadChild.Error = &orkstackv1.Error{Code: engine.ReasonValuesMissing}
	unreadChild.ResumeDisposition = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE
	row := func(f *resumeFixture, id string) *orkstackv1.TaskRun {
		for _, r := range f.run.GetTaskRuns() {
			if r.GetTaskId() == id {
				return r
			}
		}
		return nil
	}
	return map[string]composedShape{
		"it failed": {tasks: []*orkstackv1.Task{imageTask("x"), childTask("c", "x")},
			rows: []*orkstackv1.TaskRun{rowOf("x", stOK), rowOf("c", stFail, "CHILD_RUN_FAILED")}, cause: "failed"},
		"its child could not be started": {tasks: []*orkstackv1.Task{childTask("c")},
			rows: []*orkstackv1.TaskRun{rowOf("c", stFail, "CHILD_START_FAILED")}, cause: "failed"},
		"its child was canceled by the failure of another": {tasks: []*orkstackv1.Task{imageTask("x"), childTask("c")},
			rows: []*orkstackv1.TaskRun{rowOf("x", stFail), rowOf("c", stCanc, "CHILD_RUN_CANCELED")}, cause: "stopped"},
		"it never started": {tasks: []*orkstackv1.Task{imageTask("x"), childTask("c")},
			rows: []*orkstackv1.TaskRun{rowOf("x", stFail), rowOf("c", stPend)}, cause: "never_started"},
		"it was stopped by the run's timeout": {tasks: []*orkstackv1.Task{imageTask("x"), childTask("c")},
			rows:   []*orkstackv1.TaskRun{rowOf("x", stCanc), rowOf("c", stFail, engine.TaskStoppedByRunTimeoutCode)},
			change: func(_ *testing.T, f *resumeFixture) { f.run.Error.Code = engine.RunTimedOutCode }, cause: "failed"},
		"downstream of a failed task": {tasks: []*orkstackv1.Task{imageTask("x"), childTask("c", "x")},
			rows: []*orkstackv1.TaskRun{rowOf("x", stFail), rowOf("c", stCanc)}, cause: "downstream"},
		"downstream through others, and skipped": {tasks: []*orkstackv1.Task{imageTask("x"), imageTask("y", "x"), childTask("c", "y")},
			rows: []*orkstackv1.TaskRun{rowOf("x", stFail), rowOf("y", stCanc), rowOf("c", stSkip)}, cause: "downstream"},
		"downstream of a fan-out's failed items": {tasks: []*orkstackv1.Task{imageTask("build"), fan, childTask("c", "fan")},
			rows: []*orkstackv1.TaskRun{rowOf("build", stOK), fanRow, rowOf("c", stCanc)}, cause: "downstream"},
		"downstream of an await that waits again": {tasks: []*orkstackv1.Task{imageTask("x"), await("w"), childTask("c", "w")},
			rows: []*orkstackv1.TaskRun{rowOf("x", stFail), awaitRow(stCanc), rowOf("c", stCanc)}, cause: "downstream"},
		"downstream of a result that is gone": {tasks: []*orkstackv1.Task{imageTask("y"), imageTask("x"), childTask("c", "y")},
			rows:  []*orkstackv1.TaskRun{unread("y", engine.ReasonCommitMissing, orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT), rowOf("x", stFail), rowOf("c", stCanc)},
			cause: "downstream"},
		"a failure handler that had run": {tasks: []*orkstackv1.Task{imageTask("x"), handler},
			rows: []*orkstackv1.TaskRun{rowOf("x", stFail), childRow(stOK)}, cause: "downstream"},
		"Retry from here on it": {tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")}, from: "c",
			rows: []*orkstackv1.TaskRun{childRow(stOK), rowOf("x", stFail)}, cause: "selected"},
		"Retry from here above it": {tasks: []*orkstackv1.Task{imageTask("y"), childTask("c", "y"), imageTask("x")}, from: "y",
			rows: []*orkstackv1.TaskRun{rowOf("y", stOK), childRow(stOK), rowOf("x", stFail)}, cause: "downstream"},
		"its values are gone": {tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")},
			rows:   []*orkstackv1.TaskRun{childRow(stOK), rowOf("x", stFail)},
			change: func(_ *testing.T, f *resumeFixture) { row(f, "c").ValuesRef = gone(row(f, "c").GetValuesRef()) }, cause: "result_gone"},
		"an artifact it returned is gone": {tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")},
			rows: []*orkstackv1.TaskRun{childRow(stOK), rowOf("x", stFail)},
			change: func(_ *testing.T, f *resumeFixture) {
				row(f, "c").Artifacts = []*orkstackv1.ArtifactRef{gone(row(f, "c").GetValuesRef())}
			}, cause: "result_gone"},
		"its values were not read back, and are gone": {tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")},
			rows: []*orkstackv1.TaskRun{unreadChild, rowOf("x", stCanc)},
			change: func(t *testing.T, f *resumeFixture) {
				f.putValues(t, row(f, "c"))
				row(f, "c").ValuesRef = gone(row(f, "c").GetValuesRef())
			}, cause: "result_gone"},
		// Force-cancelled runs are not resumed yet (E85 PR5b); the planner's own
		// refusal of one is TestResumePlan_ComposedTasks'.
		"its child was open at a force cancel": {tasks: []*orkstackv1.Task{childTask("c")}, from: "c",
			rows: []*orkstackv1.TaskRun{rowOf("c", orkstackv1.Status_STATUS_RUNNING)},
			change: func(_ *testing.T, f *resumeFixture) {
				f.run.Status, f.run.Error.Code = stCanc, "RUN_FORCE_CANCELED"
			}, reason: "STATE_NOT_RESUMABLE"},
	}
}

// composedFixture is a fixture of shape s, whose plan holds an identity or
// not, and what is stored when it is made.
func composedFixture(t *testing.T, s composedShape, identity bool) (*resumeFixture, func() map[string]int64) {
	t.Helper()
	dir := t.TempDir()
	f := newResumeFixture(t, func(f *resumeFixture) {
		// One bucket holds the run's objects and its records, as a deployment's does.
		records, err := artifacts.NewRecordStore(context.Background(), artifacts.Config{Backend: "local", LocalDir: filepath.Join(dir, "objects")})
		if err != nil {
			t.Fatal(err)
		}
		f.records, f.recordStore = records, records
		if f.store, err = artifacts.NewLocalStore(filepath.Join(dir, "objects")); err != nil {
			t.Fatal(err)
		}
		if f.snaps, err = artifacts.NewLocalRunSnapshotStore(filepath.Join(dir, "snapshots")); err != nil {
			t.Fatal(err)
		}
		f.def.Tasks, f.run.TaskRuns = s.tasks, s.rows
		var ran []string
		for i, row := range s.rows {
			if s.tasks[i].GetUses() == "image:busybox" && row.GetStatus() != stPend && row.GetStatus() != stSkip {
				ran = append(ran, row.GetTaskId())
			}
		}
		f.cluster.histories = map[string][]*historypb.HistoryEvent{"exec-1": failedHistory(t, ran...)}
		if identity {
			f.plan.Identity = &engine.IdentityPlan{AdmissionSHA256: resumeAdmission,
				RecordBudget: &engine.RecordBudget{MissConfirmMS: 1, ReaskInitialMS: 1, ReaskMaxMS: 1}}
		}
		f.putComposed(t)
		var was map[string]*orkstackv1.ArtifactRef
		if identity {
			// What the child ended with is on record; a change that loses an
			// object of it loses the object, and the record stays.
			f.recordComposed(t)
			was = map[string]*orkstackv1.ArtifactRef{}
			for _, row := range f.run.GetTaskRuns() {
				if row.GetChild() != nil {
					was[row.GetTaskId()] = row.GetValuesRef()
				}
			}
		}
		if s.change != nil {
			s.change(t, f)
		}
		for _, row := range f.run.GetTaskRuns() {
			ref, ok := was[row.GetTaskId()]
			if !ok || ref == nil || proto.Equal(ref, row.GetValuesRef()) && len(row.GetArtifacts()) == 0 {
				continue
			}
			if err := f.store.Delete(context.Background(), ref.GetUri()); err != nil {
				t.Fatal(err)
			}
			row.ValuesRef, row.Artifacts = ref, nil
		}
	})
	return f, func() map[string]int64 {
		out := map[string]int64{}
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			info, err := d.Info()
			if err == nil {
				out[path] = info.Size()
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
}

// TestResumeRun_NeverRerunsAComposedTask: a composed task's child run has one
// run id, planned at submit, and starting it twice leaves a run no one can
// finish. So a resume whose plan would run one again is refused, whatever put
// it there, whether or not the run holds an identity, at a preview and at a
// confirm, and writes nothing.
func TestResumeRun_NeverRerunsAComposedTask(t *testing.T) {
	for name := range composedShapes() {
		for _, identity := range []bool{false, true} {
			for _, dry := range []bool{true, false} {
				t.Run(name+map[bool]string{true: ", an identity", false: ""}[identity]+map[bool]string{true: ", preview", false: ", confirm"}[dry], func(t *testing.T) {
					s := composedShapes()[name]
					f, stored := composedFixture(t, s, identity)
					f.resets(true)
					before := stored()
					_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: dry, ExpectedTerminalSequence: 9, FromTaskId: s.from, Reason: "again"})
					r := refusalOf(t, err)
					want := "COMPOSED_TASK"
					if s.reason != "" {
						want = s.reason
					}
					if r.GetReason() != want || r.GetCause() != s.cause || r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT {
						t.Errorf("refusal = %v, want %s %s", r, want, s.cause)
					}
					if len(f.cluster.resets) != 0 || len(f.claims.claims) != 0 || !maps.Equal(before, stored()) {
						t.Errorf("%d resets, %d claims, stored %v, want nothing written over %v", len(f.cluster.resets), len(f.claims.claims), stored(), before)
					}
				})
			}
		}
	}
}

// A confirm refuses on the plan it makes itself: a composed task the preview
// kept, whose values went since, is not run again for the preview's sake.
func TestResumeRun_AConfirmRefusesAComposedTaskItsPreviewKept(t *testing.T) {
	for _, digest := range []bool{true, false} {
		s := composedShape{tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")}, rows: []*orkstackv1.TaskRun{childRow(stOK), rowOf("x", stFail)}}
		f, stored := composedFixture(t, s, false)
		f.resets(true)
		p := f.preview(t)
		ref := f.run.GetTaskRuns()[0].GetValuesRef()
		if err := f.store.Delete(context.Background(), ref.GetUri()); err != nil {
			t.Fatal(err)
		}
		before := stored()
		_, err := f.confirm(t, func(r *orkstackv1.ResumeRunRequest) {
			if digest {
				r.ExpectedPlanDigest = p.GetPlanDigest()
			}
		})
		if r := refusalOf(t, err); r.GetReason() != "COMPOSED_TASK" || r.GetCause() != "result_gone" {
			t.Errorf("with the preview's digest %v: refusal = %v", digest, r)
		}
		if len(f.cluster.resets) != 0 || len(f.claims.claims) != 0 || !maps.Equal(before, stored()) {
			t.Errorf("%d resets, %d claims, or something was stored", len(f.cluster.resets), len(f.claims.claims))
		}
	}
}

// TestResumeRun_RecordNamesWhatItsPlanMarks: the record ResumeRun writes for
// identity names exactly the tasks the plan it resets to runs again, and the
// awaits it opens again, at the plan's own scope. Which rows a reset run marks
// for a plan is TestResumeRecordTasks_AreTheRowsTheResetRunMarks'.
func TestResumeRun_RecordNamesWhatItsPlanMarks(t *testing.T) {
	await := &orkstackv1.Task{Id: "w", Needs: []string{"x"}, Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}}
	nap := &orkstackv1.Task{Id: "nap", Needs: []string{"x"}, Sleep: &orkstackv1.SleepSpec{Seconds: 5}}
	fan := imageTask("fan", "build")
	fan.Each = &orkstackv1.EachSpec{Over: "x", MaxItems: 5, Source: &orkstackv1.EachSource{TaskId: "build", Path: []string{"xs"}}}
	fanRow := rowOf("fan", stFail)
	item0, item1 := int32(0), int32(1)
	fanRow.Items = []*orkstackv1.TaskRun{{TaskId: "fan", Item: &item0, Status: stCanc}, {TaskId: "fan", Item: &item1, Status: stFail}}
	s := composedShape{
		tasks: []*orkstackv1.Task{childTask("c"), imageTask("build"), fan, imageTask("x"), await, nap, imageTask("after", "w", "nap")},
		rows:  []*orkstackv1.TaskRun{childRow(stOK), rowOf("build", stOK), fanRow, rowOf("x", stFail), rowOf("w", stCanc), rowOf("nap", stCanc), rowOf("after", stCanc)},
	}
	f, _ := composedFixture(t, s, false)
	f.resets(true)
	if _, err := f.confirm(t, nil); err != nil {
		t.Fatal(err)
	}
	scope, link, err := f.records.NewestResume(context.Background(), "default", "ork-r")
	if err != nil || link.Answer != artifacts.RecordFound {
		t.Fatalf("the resume's record = %v, %v", link, err)
	}
	read, _, err := f.records.ReadResumePlan(context.Background(), "default", "ork-r", scope, link.Value.N)
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("the plan's record = %v, %v", read, err)
	}
	block := read.Value.GetBlock()
	units, err := engine.DecodeResumeUnits(f.def, block, false)
	if err != nil {
		t.Fatal(err)
	}
	var rerun, reopened []string
	for _, u := range units {
		switch {
		case u.Item >= 0:
		case u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN:
			rerun = append(rerun, u.Task)
		case u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT && u.Task == "w":
			reopened = append(reopened, u.Task)
		}
	}
	if !slices.Equal(link.Value.Tasks, rerun) || !slices.Equal(link.Value.ReopenedAwaits, reopened) || !slices.Equal(block.GetReopenedAwaits(), reopened) ||
		strings.Join(rerun, " ") != "fan x after" || strings.Join(reopened, " ") != "w" {
		t.Fatalf("record: %v again, %v reopened; its plan: %v again, %v reopened, block %v", link.Value.Tasks, link.Value.ReopenedAwaits, rerun, reopened, block.GetReopenedAwaits())
	}
	if link.Value.Scope != block.GetSeqBase() || block.GetScope() != block.GetSeqBase() {
		t.Fatalf("record scope %d, block seq_base %d and scope %d: want one", link.Value.Scope, block.GetSeqBase(), block.GetScope())
	}
}

// The control: a composed task that stays out of the rerun set is kept, and
// the same run is resumed; one that holds an identity is refused for that.
func TestResumeRun_KeepsAComposedTask(t *testing.T) {
	shape := func() composedShape {
		return composedShape{tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")}, rows: []*orkstackv1.TaskRun{childRow(stOK), rowOf("x", stFail)}}
	}
	f, _ := composedFixture(t, shape(), false)
	f.resets(true)
	p := f.preview(t)
	if c := p.GetTasks()[0]; c.GetTaskId() != "c" || c.GetCategory() != orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED || p.GetKeptCount() != 1 {
		t.Fatalf("preview = %v, want c settled", p)
	}
	if _, err := f.confirm(t, nil); err != nil || len(f.cluster.resets) != 1 {
		t.Fatalf("confirm = %v, %d resets", err, len(f.cluster.resets))
	}
	// One that holds an identity keeps it as the engine recorded it.
	f, _ = composedFixture(t, shape(), true)
	f.resets(true)
	p = f.preview(t)
	if c := p.GetTasks()[0]; c.GetTaskId() != "c" || c.GetCategory() != orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED || p.GetKeptCount() != 1 {
		t.Fatalf("an identity: preview = %v, want c settled", p)
	}
	if _, err := f.confirm(t, nil); err != nil || len(f.cluster.resets) != 1 {
		t.Fatalf("an identity: confirm = %v, %d resets", err, len(f.cluster.resets))
	}
}
