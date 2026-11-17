package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// firstOf marks every indexed record of index as its chain's first
// execution, as the cluster reports a run that never switched.
func firstOf(index *statsIndex, ids ...string) {
	for _, rec := range index.records {
		for _, id := range ids {
			if rec.info.GetExecution().GetWorkflowId() == id {
				rec.info.FirstRunId = rec.info.GetExecution().GetRunId()
			}
		}
	}
}

func TestWalkLowWater_EarliestLiveCreationAndRecentlyClosed(t *testing.T) {
	old := visibilityNow.Add(-50 * time.Hour)
	index := indexOf(t,
		record{id: "ork-live", status: running, start: visibilityNow.Add(-2 * time.Hour)},
		record{id: "ork-old", status: running, start: old},
		record{id: "ork-closed", status: completed, start: visibilityNow.Add(-3 * time.Hour), closed: visibilityNow.Add(-time.Hour)},
	)
	firstOf(index, "ork-live", "ork-old", "ork-closed")
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-5*time.Hour))
	if err != nil || walk.Cause != "" {
		t.Fatalf("walk = %+v, %v", walk, err)
	}
	if walk.EarliestRunID != "ork-old" || !walk.Earliest.Equal(old) || walk.Chains != 2 {
		t.Fatalf("earliest = %s %v chains %d", walk.EarliestRunID, walk.Earliest, walk.Chains)
	}
	if len(walk.Closed) != 1 || walk.Closed[0].RunID != "ork-closed" {
		t.Fatalf("closed = %+v", walk.Closed)
	}
	for _, id := range []string{"ork-live", "ork-old", "ork-closed"} {
		if _, ok := walk.Seen[id]; !ok {
			t.Fatalf("%s not seen", id)
		}
	}
}

func TestWalkLowWater_ChainJudgedByEarliestCreation(t *testing.T) {
	created := visibilityNow.Add(-40 * time.Hour)
	// A continued chain: its live generation started a minute ago, but the
	// run was created 40 hours ago (memo).
	index := indexOf(t, switched("ork-chain", created, 5), successor("ork-chain", created, running))
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != "" {
		t.Fatalf("walk = %+v, %v", walk, err)
	}
	if !walk.Earliest.Equal(created) {
		t.Fatalf("earliest = %v, want the run's creation %v", walk.Earliest, created)
	}
}

func TestWalkLowWater_LaterExecutionWithoutCreationFailsClosed(t *testing.T) {
	// A record that is not its chain's first and carries no created_at.
	index := indexOf(t, record{id: "ork-resumed", status: running, start: visibilityNow.Add(-time.Minute)})
	index.records[0].info.FirstRunId = "ork-resumed@earlier"
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != archive.LowWaterNoCreation {
		t.Fatalf("walk = %+v, %v", walk, err)
	}
	// A cluster that does not say which execution is first (its list
	// answers never do) dates a record by its start, unless the record is a
	// closed generation, which carries created_at by construction.
	index.records[0].info.FirstRunId = ""
	if walk, _ := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour)); walk.Cause != "" {
		t.Fatalf("walk = %+v", walk)
	}
	gen := indexOf(t, record{id: "ork-gen", status: continued, start: visibilityNow.Add(-2 * time.Hour), closed: visibilityNow.Add(-time.Minute)})
	if walk, _ := readerOverIndex(gen).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour)); walk.Cause != archive.LowWaterNoCreation {
		t.Fatalf("closed generation without created_at: %+v", walk)
	}
}

func TestWalkLowWater_MidSwitchChainFailsClosed(t *testing.T) {
	created := visibilityNow.Add(-3 * time.Hour)
	index := indexOf(t, switched("ork-mid", created, 1))
	index.describeAs("ork-mid", running)
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != archive.LowWaterMidSwitch {
		t.Fatalf("walk = %+v, %v", walk, err)
	}
}

func TestWalkLowWater_ScanBoundIsAnError(t *testing.T) {
	index := indexOf(t, record{id: "ork-a", status: running, start: visibilityNow})
	index.endless = true
	if _, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow); err == nil {
		t.Fatal("a walk that never reached the end of the index answered")
	}
}

func TestWalkLowWater_DescribeErrorIsAnError(t *testing.T) {
	index := indexOf(t, switched("ork-mid", visibilityNow.Add(-time.Hour), 1))
	index.describeErr = errors.New("frontend unreachable")
	if _, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow); err == nil {
		t.Fatal("a failed describe answered a walk")
	}
}

func TestReclaimBody_ClassesAndFinish(t *testing.T) {
	finished := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	run := &orkstackv1.Run{Id: "ork-r", Namespace: "", Status: orkstackv1.Status_STATUS_SUCCEEDED, FinishedAt: timestamppb.New(finished),
		TaskRuns: []*orkstackv1.TaskRun{{
			Artifacts:       []*orkstackv1.ArtifactRef{{Uri: "s3://b/orkflows/f/ork-r/t/a/" + sha64}},
			SystemArtifacts: []*orkstackv1.ArtifactRef{{Uri: "s3://b/orkflows/f/ork-r/t/s/" + sha64}},
			ValuesRef:       &orkstackv1.ArtifactRef{Uri: "s3://b/orkflows/f/ork-r/t/v/" + sha64},
			Items:           []*orkstackv1.TaskRun{{Artifacts: []*orkstackv1.ArtifactRef{{Uri: "s3://b/orkflows/f/ork-r/t/i/" + sha64}}}},
		}}}
	got := reclaimObjects(run)
	want := []archive.BodyObject{
		{ID: "orkflows/f/ork-r/t/a/" + sha64, Class: archive.ClassArtifact},
		{ID: "orkflows/f/ork-r/t/s/" + sha64, Class: archive.ClassSystemArtifact},
		{ID: "orkflows/f/ork-r/t/v/" + sha64, Class: archive.ClassValues},
		{ID: "orkflows/f/ork-r/t/i/" + sha64, Class: archive.ClassArtifact},
	}
	if len(got) != len(want) {
		t.Fatalf("objects = %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("object %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	// The classes cover exactly what PutRun marks.
	ids := runObjectIDs(run)
	if len(ids) != len(got) {
		t.Fatalf("PutRun marks %d, the GC examines %d", len(ids), len(got))
	}
	for _, status := range []orkstackv1.Status{orkstackv1.Status_STATUS_RUNNING, orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_UNSPECIFIED} {
		if terminalRunStatus(status) {
			t.Fatalf("%v read as terminal", status)
		}
	}
}

const sha64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestWalkLowWater_ZeroCreationFailsClosed(t *testing.T) {
	index := indexOf(t, record{id: "ork-zero", status: running, start: visibilityNow, created: time.Unix(0, 0)})
	index.records[0].info.Memo.Fields[MemoCreatedAt] = index.records[0].info.Memo.Fields[MemoNamespace]
	index.records[0].info.FirstRunId = index.records[0].info.GetExecution().GetRunId()
	// A created_at that does not parse dates nothing, and neither does one
	// that parses to the zero instant.
	if walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow); err != nil || walk.Cause != archive.LowWaterNoCreation {
		t.Fatalf("unparsable memo: %+v, %v", walk, err)
	}
	r := readerOverIndex(index)
	payload, _ := converter.GetDefaultDataConverter().ToPayload(time.Time{}.Format(time.RFC3339Nano))
	index.records[0].info.Memo.Fields[MemoCreatedAt] = payload
	if walk, _ := r.WalkLowWater(context.Background(), visibilityNow); walk.Cause != archive.LowWaterNoCreation {
		t.Fatalf("zero memo: %+v", walk)
	}
}

func TestWalkLowWater_RunWithSeveralExecutionsAndNoCreationFailsClosed(t *testing.T) {
	// A reset (or a resume that did not carry created_at): the open record
	// is a later execution, and the cluster still holds the first.
	index := indexOf(t, record{id: "ork-reset", status: running, start: visibilityNow.Add(-time.Minute)})
	index.indexLater(t, record{id: "ork-reset", status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		start: visibilityNow.Add(-40 * time.Hour), closed: visibilityNow.Add(-2 * time.Minute)})
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != archive.LowWaterNoCreation {
		t.Fatalf("walk = %+v, %v; a later execution must never date its run", walk, err)
	}
	// The same, with the cluster unable to answer IN: asked id by id.
	index.rejectIN = true
	walk, err = readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != archive.LowWaterNoCreation {
		t.Fatalf("walk without IN = %+v, %v", walk, err)
	}
}

func TestWalkLowWater_CreationFromAnotherRecordOfTheRun(t *testing.T) {
	created := visibilityNow.Add(-30 * time.Hour)
	// The open record lacks created_at (its index entry lags), but an earlier
	// record of the same run carries it.
	index := indexOf(t, record{id: "ork-r", status: running, start: visibilityNow.Add(-time.Minute)})
	index.indexLater(t, record{id: "ork-r", status: continued, start: created, created: created, closed: visibilityNow.Add(-2 * time.Hour)})
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != "" || !walk.Earliest.Equal(created) {
		t.Fatalf("walk = %+v, %v; want dated by %v", walk, err, created)
	}
}

func TestWalkLowWater_ByIDListingFailureIsAnError(t *testing.T) {
	index := indexOf(t, record{id: "ork-a", status: running, start: visibilityNow})
	index.describeErr = nil
	index.endless = true
	if _, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow); err == nil {
		t.Fatal("an unreadable by-id listing dated a run")
	}
}

func TestWalkLowWater_ChainThatSwitchedDuringTheWalkStillDatesTheMark(t *testing.T) {
	created := visibilityNow.Add(-50 * time.Hour)
	// The live walk saw neither of the chain's records (they moved ahead of
	// its cursor); its new generation, started during the walk, carries the
	// run's creation, and the started-since listing reads it.
	index := indexOf(t, record{id: "ork-young", status: running, start: visibilityNow.Add(-2 * time.Hour)})
	firstOf(index, "ork-young")
	index.records = append(index.records, record{id: "ork-switched", status: running, start: visibilityNow.Add(-time.Second), created: created}.indexed(t))
	r := readerOverIndex(index)
	// Hide the switched chain from the live walk only.
	hidden := index.records[1]
	index.records = index.records[:1]
	index.startedOnly = []indexedExecution{hidden}
	walk, err := r.WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != "" || !walk.Earliest.Equal(created) || walk.EarliestRunID != "ork-switched" {
		t.Fatalf("walk = %+v, %v; the switched chain must date the mark at %v", walk, err, created)
	}
}

func TestWalkLowWater_StartStampDatesARunWithoutAskingTheCluster(t *testing.T) {
	stamp := visibilityNow.Add(-20 * time.Hour)
	index := indexOf(t, record{id: "ork-new", status: running, start: stamp.Add(time.Millisecond)})
	payload, err := converter.GetDefaultDataConverter().ToPayload(stamp.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	index.records[0].info.Memo.Fields[MemoStartStamp] = payload
	// A second execution the cluster still holds would fail an undated run
	// closed; a stamped one is dated by its stamp and never asked about.
	index.indexLater(t, record{id: "ork-new", status: completed, start: stamp, closed: stamp.Add(time.Minute)})
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != "" || !walk.Earliest.Equal(stamp) {
		t.Fatalf("walk = %+v, %v; want dated by its start stamp %v", walk, err, stamp)
	}
	for _, q := range index.queries {
		if strings.Contains(q, "WorkflowId") {
			t.Fatalf("a stamped run was asked about by id: %s", q)
		}
	}
}

func TestWalkLowWater_RecentlyStartedRunThatClosedDoesNotHoldTheMark(t *testing.T) {
	// A run started minutes ago and already finished is a closed run: the
	// closed listing reports it, and the GC asks whether it has an archive
	// body. The started-since listing, which is there for a chain that
	// carried on as new during the walk, must not count it live as well, or
	// it holds the mark for the whole switch window once it is archived.
	index := indexOf(t, record{id: "ork-done", status: completed,
		start: visibilityNow.Add(-10 * time.Minute), closed: visibilityNow.Add(-5 * time.Minute)})
	firstOf(index, "ork-done")
	walk, err := readerOverIndex(index).WalkLowWater(context.Background(), visibilityNow.Add(-time.Hour))
	if err != nil || walk.Cause != "" {
		t.Fatalf("walk = %+v, %v", walk, err)
	}
	if !walk.Earliest.IsZero() {
		t.Fatalf("a closed run dated the live mark: %s at %v", walk.EarliestRunID, walk.Earliest)
	}
	if len(walk.Closed) != 1 || walk.Closed[0].RunID != "ork-done" {
		t.Fatalf("closed = %+v, want ork-done", walk.Closed)
	}
}
