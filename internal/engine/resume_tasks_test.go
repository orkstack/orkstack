package engine

import (
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// recordable makes in's rows ones a run leaves, so a plan of them packs.
func recordable(in *ResumePlanInput) {
	in.Facts.Continued, in.Facts.Earlier = false, map[string]bool{}
	in.Evidence.Resolutions, in.Evidence.ChildOnRecord = map[string]ResolutionAnswer{}, nil
	for i, t := range in.Definition.GetTasks() {
		if t.GetEach() != nil {
			t.Each.MaxItems = 10
		}
		row := in.Base.TaskRuns[i]
		if row.GetStatus() == sSkip {
			row.Items = nil // it never expanded
		}
		for _, item := range row.GetItems() {
			if item.GetStatus() == sSkip {
				item.Status = sFail // no item is ever skipped
			}
		}
		var was *orkstackv1.TaskRun
		switch kind := (&planTask{def: t}).kind(); kind {
		case kindAwait:
			was = recordedAwait()
		case kindComposed:
			was = recordedChild()
		case kindSleep:
			was = recordedSleep()
		default:
			continue
		}
		if row.GetStatus() == sOK || unreadKept(row) {
			proto.Merge(row, was)
		}
	}
}

// resetRows are the rows of a reset run of in's definition once it has carried
// out step 1 of block: the code a reset run runs.
func resetRows(ctx workflow.Context, in ResumePlanInput, block *orkstackv1.ResumeBlock) (map[string]*orkstackv1.TaskRun, error) {
	units, err := DecodeResumeUnits(in.Definition, block, false)
	if err != nil {
		return nil, err
	}
	s := buildRunState("ork-r", in.Definition, nil, RunPlan{Namespace: "default"})
	return s.taskRuns, s.applyDispositions(ctx, block, units)
}

// What a resume's record names is exactly what its reset run marks: the tasks
// to run again, and the awaits it opens again, whose rows, and no others, take
// the resume's scope. Where the two differed, a task's results would be read
// at one scope and written at another, for ever. Planned, packed and carried
// out by the code ResumeRun and a reset run run, over generated runs.
func TestResumeRecordTasks_AreTheRowsTheResetRunMarks(t *testing.T) {
	rng := rand.New(rand.NewPCG(85, 12))
	seen := map[string]int{}
	check := func(ctx workflow.Context, in ResumePlanInput) {
		plan, err := PlanResume(in)
		if err != nil {
			seen["refused"]++
			return
		}
		sizes := map[ResumeUnit]int64{}
		for _, u := range plan.Units {
			sizes[u.Unit] = 100
		}
		const scope = 100
		block, err := BuildResumeBlock(ResumeBlockInput{Input: in, Plan: plan, Verification: ResumeVerification{Sizes: sizes}, SeqBase: scope, Scope: scope})
		if err != nil {
			t.Errorf("build the block: %v", err)
			return
		}
		rows, err := resetRows(ctx, in, block)
		if err != nil {
			t.Errorf("carry the block out: %v", err)
			return
		}
		rerun, reopened := ResumeRecordTasks(in.Definition, plan)
		for _, task := range in.Definition.GetTasks() {
			id := task.GetId()
			row := rows[id]
			marked := row.GetResumeDisposition() == dRerun
			opened := row.GetResumeDisposition() == dWait && task.GetAwait() != nil
			if marked != slices.Contains(rerun, id) || opened != slices.Contains(reopened, id) {
				t.Errorf("%s: the reset run marks it %v, its record names %v to run again and %v reopened", id, row.GetResumeDisposition(), rerun, reopened)
			}
			if scoped := row.GetResultEpoch() == scope; scoped != (marked || opened) {
				t.Errorf("%s: result_epoch %d, disposition %v: want the resume's scope for what its record names alone", id, row.GetResultEpoch(), row.GetResumeDisposition())
			}
			for _, item := range row.GetItems() {
				if again := item.GetResumeDisposition() == dRerun; again != (item.GetResultEpoch() == scope) || again && !marked {
					t.Errorf("%s: item %d is %v at %d", id, item.GetItem(), item.GetResumeDisposition(), item.GetResultEpoch())
				}
			}
			switch {
			case marked && task.GetEach() != nil && len(row.GetItems()) > 0:
				seen["a fan-out with kept items"]++
			case marked:
				seen["run again"]++
			case opened:
				seen["reopened"]++
			case row.GetResumeDisposition() == dWait:
				seen["a sleep again"]++
			case row.GetResumeDisposition() == dSet:
				seen["settled"]++
			}
		}
	}
	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		for range 3000 {
			data := make([]byte, 400)
			for i := range data {
				data[i] = byte(rng.Uint32())
			}
			in := fuzzPlanInput(data)
			recordable(&in)
			check(ctx, in)
			if t.Failed() {
				return nil
			}
		}
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"run again", "reopened", "a sleep again", "settled", "a fan-out with kept items"} {
		if seen[want] < 10 {
			t.Errorf("only %d plans with %q of %v: the generator no longer covers it", seen[want], want, seen)
		}
	}
	t.Log(seen)
}

// A task is named once, a fan-out by its own unit, and an await in the rerun
// set is one that opens again, not one that runs again.
func TestResumeRecordTasks(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("up"), awaitTask("approve", "up"), sleepTask("nap", "up"), ptask("deploy", "approve", "nap"),
		ptask("src"), eachTask("fan", "src", "src"), ptask("kept")},
		prow("up", sFail), prow("approve", sCanc), prow("nap", sCanc), prow("deploy", sCanc),
		prow("src", sOK), fanRow("fan", sFail, pitem(0, sOK), pitem(1, sFail)), prow("kept", sOK))
	rerun, reopened := ResumeRecordTasks(f.in.Definition, f.plan(t))
	if strings.Join(rerun, " ") != "up deploy fan" || strings.Join(reopened, " ") != "approve" {
		t.Fatalf("to run again %v, reopened %v", rerun, reopened)
	}
}
