package engine

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resumedStats adds to index a record of run id started at started, which
// carries created when a resume made it.
func resumedStats(t *testing.T, index *statsIndex, id, execution string, started, closed, created time.Time, status enumspb.WorkflowExecutionStatus) {
	t.Helper()
	payload := func(v string) *commonpb.Payload {
		p, err := converter.GetDefaultDataConverter().ToPayload(v)
		must.OKf(t, err, "encode")
		return p
	}
	info := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: execution}, Type: &commonpb.WorkflowType{Name: RunWorkflowType},
		Status: status, StartTime: timestamppb.New(started),
		Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{MemoOrkflowName: payload("deploy"), MemoNamespace: payload("alpha")}},
	}
	if !closed.IsZero() {
		info.CloseTime = timestamppb.New(closed)
	}
	if !created.IsZero() {
		info.Memo.Fields[MemoCreatedAt] = payload(created.UTC().Format(time.RFC3339Nano))
	}
	index.records = append(index.records, indexedExecution{info: info, namespace: "alpha"})
}

// TestExecutions_AResumedRunIsOneRun: a resumed run is one run of the window
// that created it, as its newest execution left it: one duration, from its
// creation, whether it was resumed inside the window or after it, and none
// while it runs. Only runs a resume marked are looked up.
func TestExecutions_AResumedRunIsOneRun(t *testing.T) {
	const failed, completed, running = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	created := statsWindowStart.Add(time.Hour)
	base := func(t *testing.T, i *statsIndex) {
		resumedStats(t, i, "ork-r", "exec-0", created, created.Add(5*time.Minute), time.Time{}, failed)
	}
	for name, tc := range map[string]struct {
		build   func(t *testing.T, i *statsIndex)
		marked  []string
		err     error
		want    []time.Duration
		resumed []string
		looked  bool
	}{
		"resumed inside the window": {marked: []string{"ork-r"}, want: []time.Duration{time.Hour}, resumed: []string{"ork-r"}, looked: true, build: func(t *testing.T, i *statsIndex) {
			base(t, i)
			resumedStats(t, i, "ork-r", "exec-1", created.Add(30*time.Minute), created.Add(time.Hour), created, completed)
		}},
		"resumed after the window": {marked: []string{"ork-r"}, want: []time.Duration{6 * time.Hour}, resumed: []string{"ork-r"}, looked: true, build: func(t *testing.T, i *statsIndex) {
			base(t, i)
			resumedStats(t, i, "ork-r", "exec-1", created.Add(5*time.Hour), created.Add(6*time.Hour), created, completed)
		}},
		"resumed and running": {marked: []string{"ork-r"}, resumed: []string{"ork-r"}, looked: true, build: func(t *testing.T, i *statsIndex) {
			base(t, i)
			resumedStats(t, i, "ork-r", "exec-1", created.Add(5*time.Hour), time.Time{}, created, running)
		}},
		"created before the window, resumed inside it": {marked: []string{"ork-r"}, build: func(t *testing.T, i *statsIndex) {
			before := statsWindowStart.Add(-time.Hour)
			resumedStats(t, i, "ork-r", "exec-0", before, before.Add(time.Minute), time.Time{}, failed)
			resumedStats(t, i, "ork-r", "exec-1", created, created.Add(time.Minute), before, completed)
		}},
		"marked, its reset never ran": {marked: []string{"ork-r"}, want: []time.Duration{5 * time.Minute}, looked: true, build: base},
		"never marked":                {want: []time.Duration{5 * time.Minute}, build: base},
		"no answer":                   {err: errors.New("down"), build: base},
	} {
		t.Run(name, func(t *testing.T) {
			index := &statsIndex{}
			tc.build(t, index)
			reader := newRunReader(index, converter.GetDefaultDataConverter()).WithEndings(&fakeEndings{marked: tc.marked, err: tc.err})
			got, err := reader.Executions(context.Background(), StatsWindow{Namespace: "alpha", Since: statsWindowStart, Until: statsWindowEnd})
			if tc.err != nil {
				if err == nil {
					t.Fatal("answered without knowing which runs were resumed")
				}
				return
			}
			must.OKf(t, err, "Executions")
			if !slices.Equal(got.Durations, tc.want) {
				t.Fatalf("sampled %v, want %v", got.Durations, tc.want)
			}
			var resumed []string
			for id := range got.Resumed {
				resumed = append(resumed, id)
			}
			if !reflect.DeepEqual(resumed, tc.resumed) {
				t.Fatalf("resumed %v, want %v", resumed, tc.resumed)
			}
			looked := slices.ContainsFunc(index.queries, func(q string) bool { return strings.Contains(q, "WorkflowId") })
			if looked != tc.looked {
				t.Fatalf("looked up by id %v, want %v (%q)", looked, tc.looked, index.queries)
			}
		})
	}
}

// TestTallyRunIndex_AResumedRun: a resumed run's one row is counted as it
// ended, and named as resumed.
func TestTallyRunIndex_AResumedRun(t *testing.T) {
	tally := TallyRunIndex([]artifacts.RunIndexEntry{
		{RunID: "ork-a", Orkflow: "deploy", Status: "failed"},
		{RunID: "ork-r", Orkflow: "deploy", Status: "succeeded", Scope: 7},
	})
	if tally.Finished() != 2 || tally.Succeeded() != 1 || !reflect.DeepEqual(tally.ResumedIDs, map[string]struct{}{"ork-r": {}}) {
		t.Fatalf("tally = %+v", tally)
	}
}

// TestRunArchiver_ResumedRuns: the marked runs, for a stats read to ask about.
func TestRunArchiver_ResumedRuns(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(store)
	must.OKf(t, store.MarkResumed(ctx, "ork-r", 7, 0), "mark")
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_FAILED, time.Minute), 0, setAside(6)), "first")
	got, err := a.ResumedRuns(ctx)
	if err != nil || !reflect.DeepEqual(got, map[string]bool{"ork-r": true}) {
		t.Fatalf("= %v, %v", got, err)
	}
	if got, err = (*RunArchiver)(nil).ResumedRuns(ctx); err != nil || len(got) != 0 {
		t.Fatalf("no archive = %v, %v", got, err)
	}
}

// TestRunArchiver_OneEndingARun: an archive that keeps one ending a run cannot
// say which runs were resumed: an error, never none.
func TestRunArchiver_OneEndingARun(t *testing.T) {
	a := NewRunArchiver(oneEnding{})
	if got, err := a.ResumedRuns(context.Background()); err == nil {
		t.Fatalf("= %v, want an error", got)
	}
	if err := a.RetireEarlierRows(context.Background(), endingRun(orkstackv1.Status_STATUS_RUNNING, 0), 7); err == nil {
		t.Fatal("rows it cannot read were said to be retired")
	}
}

// oneEnding is an archive that keeps one ending a run.
type oneEnding struct{ artifacts.RunArchive }

// TestReleaseResumableRuns: the failed runs pinned to a release, each once: a
// failure a resume set aside is no run's, whatever the run became, and a run
// resumed and failed again is one.
func TestReleaseResumableRuns(t *testing.T) {
	const failed, completed, running = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	at := statsWindowStart
	index := &statsIndex{}
	add := func(id, execution, build string, hours int, status enumspb.WorkflowExecutionStatus) {
		started := at.Add(time.Duration(hours) * time.Hour)
		closed := started.Add(time.Minute)
		if status == running {
			closed = time.Time{}
		}
		resumedStats(t, index, id, execution, started, closed, time.Time{}, status)
		version, err := task.IndexedVersion("orkstack", build)
		must.OKf(t, err, "version")
		index.records[len(index.records)-1].version = version
	}
	add("ork-plain", "p0", "v1", 0, failed)
	add("ork-done", "d0", "v1", 0, completed)
	add("ork-elsewhere", "e0", "v2", 0, failed)
	add("ork-going-on", "g0", "v1", 0, failed)
	add("ork-going-on", "g1", "v1", 1, running)
	add("ork-again", "a0", "v1", 0, failed)
	add("ork-again", "a1", "v1", 1, failed)
	add("ork-stranded", "s0", "v1", 0, failed) // marked, never reset
	marked := []string{"ork-going-on", "ork-again", "ork-stranded", "ork-elsewhere"}
	reader := newRunReader(index, converter.GetDefaultDataConverter()).WithEndings(&fakeEndings{marked: marked})
	n, err := reader.ReleaseResumableRuns(context.Background(), "orkstack", "v1")
	if err != nil || n != 3 { // ork-plain, ork-again once, ork-stranded
		t.Fatalf("= %d, %v; want 3", n, err)
	}
	index.rejectIN = true
	if n, err = reader.ReleaseResumableRuns(context.Background(), "orkstack", "v1"); err != nil || n != 3 {
		t.Fatalf("where the cluster lists one id at a time: %d, %v; want 3", n, err)
	}
	if _, err = newRunReader(index, converter.GetDefaultDataConverter()).WithEndings(&fakeEndings{err: errors.New("down")}).
		ReleaseResumableRuns(context.Background(), "orkstack", "v1"); err == nil {
		t.Fatal("answered without knowing which runs were resumed")
	}
}
