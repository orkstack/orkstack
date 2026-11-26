package api_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func (f *faultyRecords) ListRecords(ctx context.Context, class artifacts.SlotClass, ns, run, task, after string, max int) ([]artifacts.RecordName, string, bool, error) {
	if f.listErr != nil {
		return nil, "", false, f.listErr
	}
	return f.RecordStore.ListRecords(ctx, class, ns, run, task, after, max)
}

func (f *faultyRecords) ReadAttestedCommit(ctx context.Context, class artifacts.SlotClass, sp *artifacts.RecordSpacings, want artifacts.AttestedWant) (artifacts.RecordRead[artifacts.CommitRecord], error) {
	f.commits.Add(1)
	return f.RecordStore.ReadAttestedCommit(ctx, class, sp, want)
}

func (d *recordedDoor) list(edit func(*orkstackv1.ListRecordedResultsRequest)) (*orkstackv1.ListRecordedResultsResponse, error) {
	req := &orkstackv1.ListRecordedResultsRequest{RunId: recordedRun}
	if edit != nil {
		edit(req)
	}
	d.clock.advance(2 * time.Second) // past the caller's one a second
	resp, err := d.svc.ListRecordedResults(context.Background(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// states is each row as "task[item]@scope kind state".
func states(rows []*orkstackv1.RecordedResult) []string {
	var out []string
	for _, r := range rows {
		s := r.GetTaskId()
		if r.Item != nil {
			s += fmt.Sprintf("[%d]", r.GetItem())
		}
		if r.GetResumeScope() != 0 {
			s += fmt.Sprintf("@%d", r.GetResumeScope())
		}
		out = append(out, s+" "+strings.TrimPrefix(r.GetKind().String(), "RECORDED_RESULT_KIND_")+" "+
			strings.TrimPrefix(r.GetState().String(), "RECORDED_RESULT_STATE_"))
	}
	return out
}

func (d *recordedDoor) holds(got *orkstackv1.ListRecordedResultsResponse, err error, want ...string) {
	d.t.Helper()
	must.OK(d.t, err)
	if fmt.Sprint(states(got.GetResults())) != fmt.Sprint(want) {
		d.t.Fatalf("rows\n%s\nwant\n%s", strings.Join(states(got.GetResults()), "\n"), strings.Join(want, "\n"))
	}
}

// committed records a container task's result as the engine does.
func (d *recordedDoor) committed(task string, item artifacts.Item, scope uint64) {
	d.t.Helper()
	c := &orkstackv1.ActivityCommit{SchemaVersion: 1, Namespace: "default", RunId: recordedRun, TaskId: task,
		Values:   &orkstackv1.ArtifactRef{Uri: "file:///v", Sha256: strings.Repeat("c", 64)},
		Recorded: &orkstackv1.CommitRecording{AdmissionSha256: d.sha, Pool: "gpu", OrkerId: "o-1", Time: timestamppb.Now()}}
	raw, err := artifacts.MarshalCommit(c)
	must.OK(d.t, err)
	d.put(artifacts.RecordName{TaskID: task, Item: item, Scope: scope}, raw)
}

func (d *recordedDoor) put(n artifacts.RecordName, raw []byte) {
	d.t.Helper()
	key, err := artifacts.RecordKeyOf("default", recordedRun, n)
	must.OK(d.t, err)
	path := filepath.Join(d.dir, filepath.FromSlash(key))
	must.OK(d.t, os.MkdirAll(filepath.Dir(path), 0o755))
	must.OK(d.t, os.WriteFile(path, raw, 0o644))
}

// taskRun adds a task to the run the reader answers with.
func (d *recordedDoor) taskRun(tr *orkstackv1.TaskRun) {
	run := d.seeded
	run.TaskRuns = append(run.TaskRuns, tr)
	d.reader.setRun(run)
}

func (d *recordedDoor) gate() *orkstackv1.TaskRun { return taskRunFor(d.seeded, "gate") }

// TestListRecordedResults_States: each record the engine made, and each
// result that should have one, by what the run's state says of its task.
func TestListRecordedResults_States(t *testing.T) {
	t.Parallel()
	t.Run("recorded, and the run has not taken it", func(t *testing.T) {
		d := newRecordedDoor(t)
		_, err := d.approve(`{"approved":true}`, "alice")
		must.OK(t, err)
		got, err := d.list(nil)
		d.holds(got, err, "gate RESOLUTION NOT_APPLIED")
		row := got.GetResults()[0]
		if row.GetPrincipal() != "alice" || row.GetTaskStatus() != orkstackv1.Status_STATUS_AWAITING || row.GetStoredAt() == nil ||
			row.GetAskedAt() == nil || row.GetDeadline() == nil || row.GetValuesSha256() == "" || !got.GetReadComplete() {
			t.Fatalf("%v", got)
		}
	})
	t.Run("the run took it", func(t *testing.T) {
		d := newRecordedDoor(t)
		resp, err := d.approve(`{"approved":true}`, "alice")
		must.OK(t, err)
		d.gate().Status, d.gate().ValuesRef = orkstackv1.Status_STATUS_SUCCEEDED, &orkstackv1.ArtifactRef{Sha256: resp.GetValuesSha256()}
		d.reader.setRun(d.seeded)
		got, err := d.list(nil)
		d.holds(got, err, "gate RESOLUTION ON_RECORD")
	})
	t.Run("the run says another answer than the one on record", func(t *testing.T) {
		d := newRecordedDoor(t)
		_, err := d.approve(`{"approved":true}`, "alice")
		must.OK(t, err)
		d.gate().Status, d.gate().ValuesRef = orkstackv1.Status_STATUS_SUCCEEDED, &orkstackv1.ArtifactRef{Sha256: strings.Repeat("0", 64)}
		d.reader.setRun(d.seeded)
		got, err := d.list(nil)
		d.holds(got, err, "gate RESOLUTION NOT_APPLIED")
	})
	t.Run("stored after the deadline", func(t *testing.T) {
		d := newRecordedDoor(t)
		_, err := d.approve(`{"approved":true}`, "alice")
		must.OK(t, err)
		d.gate().Status = orkstackv1.Status_STATUS_FAILED
		d.gate().Error = &orkstackv1.Error{Code: "AWAIT_TIMED_OUT"}
		d.reader.setRun(d.seeded)
		d.storedAt(d.gate().GetAwait().GetDeadline().AsTime().Add(3 * time.Second))
		got, err := d.list(nil)
		d.holds(got, err, "gate RESOLUTION NOT_TAKEN")
	})
	t.Run("the run says approved, and there is no record", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.gate().Status = orkstackv1.Status_STATUS_SUCCEEDED
		d.reader.setRun(d.seeded)
		got, err := d.list(nil)
		d.holds(got, err, "gate RESOLUTION NOT_ON_RECORD")
		if !got.GetReadComplete() {
			t.Fatal("a miss is an answer")
		}
	})
	t.Run("a run that left the cluster", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.committed("build", artifacts.NoItem, 0)
		d.gate().Status = orkstackv1.Status_STATUS_SUCCEEDED
		d.taskRun(&orkstackv1.TaskRun{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED})
		d.reader.mu.Lock()
		delete(d.reader.starts, recordedRun)
		d.reader.startless = map[string]bool{recordedRun: true}
		d.reader.mu.Unlock()
		got, err := d.list(nil)
		d.holds(got, err, "build COMMIT ON_RECORD", "gate RESOLUTION RECLAIMED")
	})
	t.Run("a run whose records and admission were removed", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.gate().Status = orkstackv1.Status_STATUS_SUCCEEDED
		d.taskRun(&orkstackv1.TaskRun{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Commit: &orkstackv1.CommitRef{Uri: "s3://b/metadata/attested/default/ork-1/build/commit.json"}})
		must.OK(t, os.RemoveAll(filepath.Join(d.dir, "metadata")))
		d.reader.mu.Lock()
		delete(d.reader.starts, recordedRun)
		d.reader.startless = map[string]bool{recordedRun: true}
		d.reader.mu.Unlock()
		got, err := d.list(nil)
		d.holds(got, err, "build COMMIT RECLAIMED", "gate RESOLUTION RECLAIMED")
	})
	t.Run("the run holds another result than the one on record", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.committed("build", artifacts.NoItem, 0)
		d.taskRun(&orkstackv1.TaskRun{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: &orkstackv1.ArtifactRef{Sha256: strings.Repeat("0", 64)}})
		got, err := d.list(nil)
		d.holds(got, err, "build COMMIT NOT_APPLIED")
	})
	t.Run("a newer format, and one that cannot be used", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.put(artifacts.RecordName{Kind: artifacts.RecordResolution, TaskID: "gate"}, []byte(`{"schemaVersion": 9}`))
		d.put(artifacts.RecordName{TaskID: "build"}, []byte(`{`))
		d.taskRun(&orkstackv1.TaskRun{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED})
		got, err := d.list(nil)
		d.holds(got, err, "build COMMIT UNREAD", "gate RESOLUTION RECORDED_NEWER_FORMAT")
		if got.GetReadComplete() || got.GetIncompleteReason() != orkstackv1.RecordedReadStop_RECORDED_READ_STOP_ROWS_UNREAD {
			t.Fatalf("%v", got)
		}
	})
	t.Run("a resume's records, and the ones it replaced", func(t *testing.T) {
		d := newRecordedDoor(t)
		must.OK(t, d.records.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "default", RunID: recordedRun, Scope: 5, Tasks: []string{"build", "gate"}}))
		d.committed("build", artifacts.NoItem, 0)
		d.committed("build", artifacts.NoItem, 5)
		d.committed("test", artifacts.NoItem, 0)
		d.gate().Status = orkstackv1.Status_STATUS_SUCCEEDED
		d.taskRun(&orkstackv1.TaskRun{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED})
		d.taskRun(&orkstackv1.TaskRun{TaskId: "test", Status: orkstackv1.Status_STATUS_SUCCEEDED})
		got, err := d.list(nil)
		d.holds(got, err, "build COMMIT NOT_TAKEN", "build@5 COMMIT ON_RECORD", "gate@5 RESOLUTION NOT_ON_RECORD", "test COMMIT ON_RECORD")
	})
	t.Run("a child run's outputs, a composed task, a fan-out and a sleep", func(t *testing.T) {
		d := newRecordedDoor(t)
		d.seeded.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, RootRunId: "ork-0"}
		d.seeded.Status = orkstackv1.Status_STATUS_SUCCEEDED
		zero, one := int32(0), int32(1)
		d.committed("fan", artifacts.ItemAt(0), 0)
		d.taskRun(&orkstackv1.TaskRun{TaskId: "fan", Status: orkstackv1.Status_STATUS_SUCCEEDED, Each: &orkstackv1.TaskEach{},
			Items: []*orkstackv1.TaskRun{{Item: &zero, Status: orkstackv1.Status_STATUS_SUCCEEDED}, {Item: &one, Status: orkstackv1.Status_STATUS_SUCCEEDED}}})
		d.taskRun(&orkstackv1.TaskRun{TaskId: "sub", Status: orkstackv1.Status_STATUS_SUCCEEDED, Child: &orkstackv1.TaskChild{}})
		d.taskRun(&orkstackv1.TaskRun{TaskId: "wait", Status: orkstackv1.Status_STATUS_SUCCEEDED, Sleep: &orkstackv1.TaskSleep{}})
		got, err := d.list(nil)
		d.holds(got, err, "fan[0] COMMIT ON_RECORD", "fan[1] COMMIT NOT_ON_RECORD", " OUTPUTS NOT_ON_RECORD", "sub COMPOSED NOT_ON_RECORD")
		if !got.GetResults()[0].GetInChildRun() {
			t.Fatal("not said to be a child's")
		}
		got, err = d.list(func(r *orkstackv1.ListRecordedResultsRequest) { r.TaskId = "sub" })
		d.holds(got, err, "sub COMPOSED NOT_ON_RECORD")
	})
}

// TestListRecordedResults_AStoreThatDoesNotAnswer: every row could not be
// read, and none is said to be missing.
func TestListRecordedResults_AStoreThatDoesNotAnswer(t *testing.T) {
	t.Parallel()
	down := &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("down")}
	for name, cut := range map[string]func(*faultyRecords){
		"the listing": func(f *faultyRecords) { f.listErr = down },
		"the reads":   func(f *faultyRecords) { f.readErr = down },
	} {
		t.Run(name, func(t *testing.T) {
			d := newRecordedDoor(t)
			d.gate().Status = orkstackv1.Status_STATUS_SUCCEEDED
			d.reader.setRun(d.seeded)
			cut(d.records)
			got, err := d.list(nil)
			d.holds(got, err, "gate RESOLUTION UNREAD")
			if got.GetReadComplete() || got.GetIncompleteReason() != orkstackv1.RecordedReadStop_RECORDED_READ_STOP_ROWS_UNREAD {
				t.Fatalf("%v", got)
			}
		})
	}
	// With nothing expected, an empty page is not a complete one.
	empty := newRecordedDoor(t)
	empty.records.listErr = down
	got, err := empty.list(nil)
	empty.holds(got, err)
	if got.GetReadComplete() || got.GetIncompleteReason() != orkstackv1.RecordedReadStop_RECORDED_READ_STOP_ROWS_UNREAD {
		t.Fatalf("%v", got)
	}
	d := newRecordedDoor(t)
	d.reader.getErr = errors.New("frontend down")
	if _, err := d.list(nil); err == nil || connect.CodeOf(err) == connect.CodeNotFound {
		t.Fatalf("a run whose state cannot be read: %v", err)
	}
}

// TestListRecordedResults_Pages: every row once, in order, over pages of the
// size asked, whether it is there or missing.
func TestListRecordedResults_Pages(t *testing.T) {
	t.Parallel()
	d := newRecordedDoor(t)
	var want []string
	for i := range 23 {
		id := fmt.Sprintf("t%02d", i)
		d.taskRun(&orkstackv1.TaskRun{TaskId: id, Status: orkstackv1.Status_STATUS_SUCCEEDED})
		state := "NOT_ON_RECORD"
		if i%3 != 0 {
			d.committed(id, artifacts.NoItem, 0)
			state = "ON_RECORD"
		}
		want = append(want, id+" COMMIT "+state)
	}
	var got []string
	token, pages := "", 0
	for {
		page, err := d.list(func(r *orkstackv1.ListRecordedResultsRequest) { r.PageSize, r.PageToken = 5, token })
		must.OK(t, err)
		if len(page.GetResults()) > 5 {
			t.Fatalf("%d rows", len(page.GetResults()))
		}
		got = append(got, states(page.GetResults())...)
		if token = page.GetNextPageToken(); token == "" {
			break
		}
		if pages++; pages > 20 {
			t.Fatal("no end")
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rows\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// What a first call found costs no read again.
	before := d.records.commits.Load()
	if _, err := d.list(func(r *orkstackv1.ListRecordedResultsRequest) { r.PageSize, r.TaskId = 5, "t01" }); err != nil || d.records.commits.Load() != before {
		t.Fatalf("%v, %d reads", err, d.records.commits.Load()-before)
	}
	for _, edit := range []func(*orkstackv1.ListRecordedResultsRequest){
		func(r *orkstackv1.ListRecordedResultsRequest) { r.PageSize = 51 },
		func(r *orkstackv1.ListRecordedResultsRequest) { r.PageToken = "%" },
		func(r *orkstackv1.ListRecordedResultsRequest) { r.PageToken = strings.Repeat("YQ", 400) },
		func(r *orkstackv1.ListRecordedResultsRequest) { r.PageToken = "_w" },
		func(r *orkstackv1.ListRecordedResultsRequest) { r.RunId = "" },
	} {
		if _, err := d.list(edit); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%v", err)
		}
	}
}

// TestListRecordedResults_Bounds: a run's reads of a minute end a page where
// they end, and its token goes on from there; a caller has one call a second.
func TestListRecordedResults_Bounds(t *testing.T) {
	t.Parallel()
	d := newRecordedDoor(t)
	for i := range 20 {
		id := fmt.Sprintf("t%03d", i)
		d.committed(id, artifacts.NoItem, 0)
		d.seeded.TaskRuns = append(d.seeded.TaskRuns, &orkstackv1.TaskRun{TaskId: id, Status: orkstackv1.Status_STATUS_SUCCEEDED})
	}
	d.reader.setRun(d.seeded)
	d.clock.advance(time.Hour)
	if !d.svc.SpendRecordedReads("default/"+recordedRun, 400) {
		t.Fatal("a run never asked of has no reads left")
	}
	// Two seconds of the minute's reads come back before each call.
	seen, token, stops := 0, "", 0
	for range 10 {
		page, err := d.list(func(r *orkstackv1.ListRecordedResultsRequest) { r.PageToken = token })
		must.OK(t, err)
		seen += len(page.GetResults())
		if page.GetIncompleteReason() == orkstackv1.RecordedReadStop_RECORDED_READ_STOP_READ_BUDGET {
			if page.GetReadComplete() || page.GetNextPageToken() == "" {
				t.Fatalf("%v", page)
			}
			stops++
		}
		if token = page.GetNextPageToken(); token == "" {
			break
		}
	}
	if stops != 1 || seen != 20 || token != "" {
		t.Fatalf("stopped %d times, %d rows, token %q", stops, seen, token)
	}
	var last error
	for range 6 {
		_, last = d.svc.ListRecordedResults(context.Background(), connect.NewRequest(&orkstackv1.ListRecordedResultsRequest{RunId: recordedRun, TaskId: "t000"}))
	}
	if connect.CodeOf(last) != connect.CodeResourceExhausted {
		t.Fatalf("%v", last)
	}
}

// TestListRecordedResults_Unapplied: only the resolutions not applied, one
// read an await.
func TestListRecordedResults_Unapplied(t *testing.T) {
	t.Parallel()
	d := newRecordedDoor(t)
	d.committed("build", artifacts.NoItem, 0)
	unapplied := func(r *orkstackv1.ListRecordedResultsRequest) { r.UnappliedOnly = true }
	got, err := d.list(unapplied)
	d.holds(got, err)
	_, err = d.approve(`{"approved":true}`, "alice")
	must.OK(t, err)
	got, err = d.list(unapplied)
	d.holds(got, err, "gate RESOLUTION NOT_APPLIED")
	d.gate().Status = orkstackv1.Status_STATUS_SUCCEEDED
	d.reader.setRun(d.seeded)
	got, err = d.list(unapplied)
	d.holds(got, err)
}

// TestListRecordedResults_ARunWhoseResultsTheEngineDoesNotRecord has no rows.
func TestListRecordedResults_ARunWhoseResultsTheEngineDoesNotRecord(t *testing.T) {
	t.Parallel()
	d := newRecordedDoor(t)
	d.seedRun(t, "ork-2", "default", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_SUCCEEDED})
	got, err := d.list(func(r *orkstackv1.ListRecordedResultsRequest) { r.RunId = "ork-2" })
	must.OK(t, err)
	if len(got.GetResults()) != 0 || !got.GetReadComplete() {
		t.Fatalf("%v", got)
	}
}
