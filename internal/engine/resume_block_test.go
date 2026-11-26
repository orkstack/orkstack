package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// blockOf plans f and builds its block, with every kept container unit an
// entry of size bytes.
func blockOf(t *testing.T, f *planFixture, size int64) (*orkstackv1.ResumeBlock, error) {
	t.Helper()
	plan := f.plan(t)
	ver := ResumeVerification{Sizes: map[ResumeUnit]int64{}}
	for _, u := range plan.Units {
		if u.Disposition == DispositionAdopt {
			ver.Sizes[u.Unit] = size
		}
	}
	return BuildResumeBlock(ResumeBlockInput{Input: f.in, Plan: plan, Verification: ver, SeqBase: 12, Scope: 10, PreviousSeqBase: 5})
}

// TestBuildResumeBlock: the block says what each unit does, once, with what
// its result's read-back needs, and pins the read-back's bounds.
func TestBuildResumeBlock(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a"), ptask("b", "a"), ptask("c", "b"), ptask("s")},
		prow("a", sOK), prow("b", sFail), prow("c", sCanc), prow("s", sSkip))
	f.in.Base.TaskRuns[0].ResultEpoch = 7
	f.in.ResumesUsed = 3
	f.in.Action, f.in.FromTask = ResumeActionRetryFrom, "b"
	b, err := blockOf(t, f, 900)
	if err != nil {
		t.Fatal(err)
	}
	if b.GetSeqBase() != 12 || b.GetScope() != 10 || b.GetPreviousSeqBase() != 5 || b.GetResumesBefore() != 3 ||
		b.GetAction() != orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM || b.GetFromTaskId() != "b" {
		t.Fatalf("block = %v", b)
	}
	units, err := DecodeResumeUnits(f.in.Definition, b, false)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, u := range units {
		got = append(got, fmt.Sprintf("%s %s %d %d", u.unit(), strings.TrimPrefix(u.Disposition.String(), "RESUME_DISPOSITION_"), u.Epoch, u.EncodedSize))
	}
	if want := "a ADOPT 7 900, b RERUN 0 0, c RERUN 0 0, s KEEP_SKIPPED 0 0"; strings.Join(got, ", ") != want {
		t.Fatalf("units = %s, want %s", strings.Join(got, ", "), want)
	}
	tun, err := readbackTunablesOf(b.GetReadback())
	if err != nil || tun != defaultReadbackTunables() {
		t.Errorf("read-back bounds = %+v, %v; want the defaults", tun, err)
	}
}

// TestBuildResumeBlock_TooLarge: a result no read-back call could hold, a
// block over its bound, and a fan-out's dependent with too many scopes to
// track are refused, naming the task.
func TestBuildResumeBlock_TooLarge(t *testing.T) {
	plain := func() *planFixture {
		return newPlan([]*orkstackv1.Task{ptask("a"), ptask("b")}, prow("a", sOK), prow("b", sFail))
	}
	tooLarge := func(t *testing.T, err error, task string) {
		t.Helper()
		var r *ResumeRefusal
		if !errors.As(err, &r) || r.Code != RefuseTooLarge || r.Class != RefusalPermanent || (task != "" && (len(r.Tasks) != 1 || r.Tasks[0] != task)) {
			t.Fatalf("= %v, want RESUME_TOO_LARGE naming %q", err, task)
		}
	}
	if _, err := blockOf(t, plain(), MaxReadbackEntryBytes()); err != nil {
		t.Fatalf("the largest entry: %v", err)
	}
	_, err := blockOf(t, plain(), MaxReadbackEntryBytes()+1)
	tooLarge(t, err, "a")

	// Two fan-outs each within the bound, over it together for what needs both.
	alternate := func(id string) *orkstackv1.TaskRun {
		row := fanRow(id, sOK)
		for i := range int32(2 * (MaxItemEpochRuns - 300)) {
			it := pitem(i, sOK)
			if i%2 == 0 {
				it.ResultEpoch = 5
			}
			row.Items = append(row.Items, it)
		}
		return row
	}
	each := func(id string) *orkstackv1.Task { return eachTask(id, "src", "src") }
	f := newPlan([]*orkstackv1.Task{ptask("src"), each("f"), each("g"), ptask("both", "f", "g"), ptask("x", "both")},
		prow("src", sOK), alternate("f"), alternate("g"), prow("both", sFail), prow("x", sCanc))
	if err := CheckItemEpochs(f.in, f.plan(t), 12); err == nil {
		t.Fatal("a dependent with too many scopes to track was accepted")
	} else {
		tooLarge(t, err, "both")
	}
	one := newPlan([]*orkstackv1.Task{ptask("src"), each("f"), ptask("only", "f")}, prow("src", sOK), alternate("f"), prow("only", sFail))
	if err := CheckItemEpochs(one.in, one.plan(t), 12); err != nil {
		t.Fatalf("one fan-out within the bound: %v", err)
	}
}

// TestPreflightResume: the resume is admitted only while what it adds before
// the run goes on, with what the run's own failure would still append, lies
// below both the guard's limit and the hold line.
func TestPreflightResume(t *testing.T) {
	limit := &HistoryLimit{MaxEvents: 51_200, MaxBytes: 50 << 20, Reserve: DefaultHistoryReserve}
	budget := &ContinuationBudget{HoldPercent: 60}
	kept := func(n int, size int64) (ResumePlanInput, *ResumePlan, ResumeVerification) {
		var tasks []*orkstackv1.Task
		var rows []*orkstackv1.TaskRun
		plan, ver := &ResumePlan{}, ResumeVerification{Sizes: map[ResumeUnit]int64{}}
		for i := range n {
			id := fmt.Sprintf("t%04d", i)
			tasks, rows = append(tasks, ptask(id)), append(rows, prow(id, sOK))
			plan.Units = append(plan.Units, ResumePlanUnit{Unit: nodeUnit(id), Category: CategoryKept, Disposition: DispositionAdopt})
			ver.Sizes[nodeUnit(id)] = size
		}
		for i := range 10 {
			id := fmt.Sprintf("x%d", i)
			tasks, rows = append(tasks, ptask(id)), append(rows, prow(id, sFail))
			plan.Units = append(plan.Units, ResumePlanUnit{Unit: nodeUnit(id), Category: CategoryRerunFailed, Disposition: DispositionRerun, InRerunSet: true})
		}
		return newPlan(tasks, rows...).in, plan, ver
	}
	history := func(events, bytes int64) *ResumeHistory {
		return &ResumeHistory{ResetPoint: ResetPoint{CompletedEventID: events}, PrefixBytes: bytes, WorkflowTaskTimeout: 10 * time.Second}
	}
	for name, tc := range map[string]struct {
		kept   int
		size   int64
		prefix *ResumeHistory
		limit  *HistoryLimit
		ok     bool
	}{
		"kept-3990-small":        {3990, 600, history(15, 40<<10), limit, true},
		"kept-3990-large-values": {3990, 2900, history(15, 40<<10), limit, true},
		"near the hold line":     {3990, 600, history(30_500, 40<<10), limit, false},
		"near the byte line":     {3990, 600, history(15, 29<<20), limit, false},
		"a run with no limit":    {3990, 600, history(1_000_000, 1<<30), nil, true},
	} {
		t.Run(name, func(t *testing.T) {
			in, plan, ver := kept(tc.kept, tc.size)
			got, err := PreflightResume(in, RunPlan{History: tc.limit, Continuation: budget}, tc.prefix, plan, ver)
			var r *ResumeRefusal
			if tc.ok != (err == nil) || (!tc.ok && (!errors.As(err, &r) || r.Code != RefuseTooLarge)) {
				t.Fatalf("preflight = %+v, %v; want ok %v", got, err, tc.ok)
			}
			if tc.ok && (got.ReadbackCalls <= 0 || got.EstimatedReadback <= 0) {
				t.Fatalf("preflight = %+v, want its calls and how long they take", got)
			}
			if tc.ok && tc.limit != nil && (got.Headroom <= 0 || got.Headroom > 1) {
				t.Fatalf("headroom = %v", got.Headroom)
			}
		})
	}
	// What a history that carried on copies in again after its reset point is
	// charged too: its saved state, and a definition passed by reference.
	carried := history(15, 40<<10)
	carried.Continued, carried.Carry = true, &orkstackv1.RunContinuation{Checkpoint: &orkstackv1.ContinuationObject{SizeBytes: 6 << 20}}
	copies := &ContinuationBudget{HoldPercent: 60, ChunkBytes: 128 << 10, ChunksPerTask: 4, Restore: RestorePolicy{TimeoutSeconds: 300}}
	in, plan, ver := kept(100, 600)
	plain, err := PreflightResume(in, RunPlan{History: limit, Continuation: copies}, history(15, 40<<10), plan, ver)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := PreflightResume(in, RunPlan{History: limit, Continuation: copies, Definition: &DefinitionRef{SizeBytes: 1 << 20}}, carried, plan, ver)
	if err != nil || restored.Bytes-plain.Bytes < 7<<20 || restored.Events-plain.Events < 56 {
		t.Fatalf("a saved state of 6 MiB and a definition of 1 MiB add %d bytes and %d events, %v; want both, in 56 reads", restored.Bytes-plain.Bytes, restored.Events-plain.Events, err)
	}
	near := history(15, 28<<20)
	near.Continued, near.Carry = true, carried.Carry
	if _, err := PreflightResume(in, RunPlan{History: limit, Continuation: copies}, near, plan, ver); err == nil {
		t.Fatal("near-line-continued: a saved state that takes the run past its line was admitted")
	}
	// Only what reruns is charged a pending task's reserve.
	in, plan, ver = kept(100, 600)
	few, err := PreflightResume(in, RunPlan{History: limit, Continuation: budget}, history(15, 1), plan, ver)
	if err != nil {
		t.Fatal(err)
	}
	in, plan, ver = kept(101, 600)
	more, err := PreflightResume(in, RunPlan{History: limit, Continuation: budget}, history(15, 1), plan, ver)
	if err != nil || more.Events != few.Events {
		t.Fatalf("a kept task costs %d events, want none beyond its call", more.Events-few.Events)
	}
}

// TestBuildResumeBlock_TheLargestRunFits: a run of 4,000 tasks with the
// longest ids a task may have, every one kept but the last, fits every bound
// its plan has: the units' own, the stored record's and the payload the reset
// run reads it as.
func TestBuildResumeBlock_TheLargestRunFits(t *testing.T) {
	var tasks []*orkstackv1.Task
	var rows []*orkstackv1.TaskRun
	for n := range 4000 {
		id := fmt.Sprintf("t%s%04d", strings.Repeat("x", 195), n)
		status := sOK
		if n == 3999 {
			status = sFail
		}
		tasks, rows = append(tasks, ptask(id)), append(rows, prow(id, status))
	}
	f := newPlan(tasks, rows...)
	b, err := blockOf(t, f, 1200)
	if err != nil {
		t.Fatal(err)
	}
	rec := &orkstackv1.ResumePlanRecord{Format: artifacts.ResumePlanFormat, Namespace: "default", RunId: "ork-x", Scope: 10, Block: b}
	rec.PlanDigest = ResumePlanDigestOf(rec)
	stored, err := artifacts.MarshalResumePlan(rec)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := DataConverter().ToPayload(rec)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("4,000 tasks with 200-byte ids: units %d bytes of %d, block %d of %d, stored record %d of %d, payload %d of %d",
		len(b.GetUnits()), maxResumeUnitsBytes, proto.Size(b), 64<<10, len(stored), 256<<10, len(payload.GetData()), 256<<10)
	if len(b.GetUnits()) > maxResumeUnitsBytes || proto.Size(b) > 64<<10 || len(stored) > 256<<10 || len(payload.GetData()) > 256<<10 {
		t.Fatal("the largest run does not fit")
	}
}
