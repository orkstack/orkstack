package engine_test

import (
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	settleIt = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE
	sleptIt  = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL
)

var settledAt = time.Unix(1700000000, 0).UTC()

func settledTimes() *orkstackv1.TaskRun {
	return &orkstackv1.TaskRun{ScheduledAt: timestamppb.New(settledAt), StartedAt: timestamppb.New(settledAt),
		FinishedAt: timestamppb.New(settledAt.Add(5 * time.Minute))}
}

func recordedUnit(task string, d orkstackv1.ResumeDisposition, row *orkstackv1.TaskRun) engine.ResumeUnitPlan {
	return engine.ResumeUnitPlan{Task: task, Item: -1, Disposition: d, EncodedSize: 512, Row: row}
}

func sleptUnit(task string) engine.ResumeUnitPlan {
	row := settledTimes()
	row.Sleep = &orkstackv1.TaskSleep{WakeAt: timestamppb.New(settledAt.Add(5 * time.Minute))}
	u := recordedUnit(task, sleptIt, row)
	u.EncodedSize = 0
	return u
}

// answered is an await's recorded row whose values are body; kept says the
// store has them.
func (h *resetHarness) answered(body string, kept bool) *orkstackv1.TaskRun {
	row := settledTimes()
	row.ValuesRef = h.values.put("mem://answer", []byte(body))
	if !kept {
		row.ValuesRef.Uri = "mem://gone"
	}
	row.Await = &orkstackv1.TaskAwait{Deadline: timestamppb.New(settledAt.Add(time.Hour)),
		ResolvedVia: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK, ResolvedBy: "ana@example.com"}
	return row
}

// An await answered before the run ended, and a sleep that woke, are done
// when the run goes on: their rows read as they did, no event of theirs is
// written, and what comes after them is handed the answer.
func TestResetRun_AnAnsweredAwaitAndASleepAreKept(t *testing.T) {
	h := newResetHarness(t)
	document := strings.Replace(resumeWaitDocument, "needs: [approve, settle]", "needs: [approve, settle]\n    condition: '{{ .tasks.approve.output.go }}'", 1)
	answer := h.answered(`{"go":true}`, true)
	h.resume(t, document, nil, unit("build", -1, keepIt), recordedUnit("approve", settleIt, answer), sleptUnit("settle"), unit("deploy", -1, rerunIt))
	must.OKf(t, h.runResetOf(t, document), "the resumed run")

	for _, id := range []string{"approve", "settle"} {
		if got := eventsFor(h.harness, "ork-reset-1", id); len(got) != 0 {
			t.Errorf("%s has events %v, want none", id, got)
		}
	}
	run := h.ended(t)
	row := taskRunOfRun(run, "approve")
	if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !row.GetAdopted() || row.GetReadingBack() ||
		row.GetAwait().GetResolvedBy() != "ana@example.com" || row.GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK ||
		!row.GetFinishedAt().AsTime().Equal(settledAt.Add(5*time.Minute)) || !row.GetStartedAt().AsTime().Equal(settledAt) ||
		row.GetOutput().GetData().GetFields()["go"].GetBoolValue() != true {
		t.Errorf("approve = %v, want its answer as it was", row)
	}
	nap := taskRunOfRun(run, "settle")
	if nap.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !nap.GetAdopted() || nap.GetSleep().GetSeconds() != 600 ||
		!nap.GetSleep().GetWakeAt().AsTime().Equal(settledAt.Add(5*time.Minute)) || !nap.GetFinishedAt().AsTime().Equal(settledAt.Add(5*time.Minute)) {
		t.Errorf("settle = %v, want woken when it was", nap)
	}
	wf := compile(t, document)
	for id, u := range map[string]engine.ResumeUnitPlan{"approve": recordedUnit("approve", settleIt, answer), "settle": sleptUnit("settle")} {
		if again := engine.CutRecordedRow(taskOf(wf, id), taskRunOfRun(run, id)); !proto.Equal(again, u.Row) {
			t.Errorf("%s recorded again = %v, want %v", id, again, u.Row)
		}
	}
	calls := h.taskCalls("deploy")
	if len(calls) != 1 || upstreamRefOf(calls[0], "approve").GetValues().GetUri() != "mem://answer" {
		t.Fatalf("deploy = %v, want one run handed the answer", calls)
	}
	for _, e := range eventsOf(h.harness, "ork-reset-1") {
		if k := e.GetResultsKept(); k != nil && (k.GetKeptCount() != 1 || k.GetSettledCount() != 1) {
			t.Errorf("RUN_RESULTS_KEPT = %v, want one kept, one settled", k)
		}
		if r := e.GetResumed(); r != nil && (r.GetKeptCount() != 2 || r.GetRerunCount() != 1) {
			t.Errorf("RUN_RESUMED = %v, want two kept, one again", r)
		}
	}
}

// An answer whose values are gone fails its await with its reason, and what
// the row said of it stays, so the archive and the next resume have it.
func TestResetRun_AnAnswerThatIsGoneFailsItsAwait(t *testing.T) {
	h := newResetHarness(t)
	answer := h.answered(`{"go":true}`, false)
	h.resume(t, resumeWaitDocument, nil, unit("build", -1, keepIt), recordedUnit("approve", settleIt, answer),
		sleptUnit("settle"), unit("deploy", -1, rerunIt))
	if err := h.runResetOf(t, resumeWaitDocument); err == nil {
		t.Fatal("the run succeeded without its answer")
	}
	row := taskRunOfRun(h.ended(t), "approve")
	if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != engine.ReasonValuesMissing || row.GetResumeDisposition() != settleIt ||
		!proto.Equal(engine.CutRecordedRow(taskOf(compile(t, resumeWaitDocument), "approve"), row), answer) {
		t.Fatalf("approve = %v, want FAILED VALUES_MISSING, saying who answered it and when", row)
	}
	if n := len(h.taskCalls("deploy")); n != 0 {
		t.Fatalf("deploy ran %d times", n)
	}
}

// A composed task whose child ended keeps its child, its values whole and
// the artifacts it passed and got back: no child run starts.
func TestResetRun_AComposedTaskIsKept(t *testing.T) {
	h := newResetHarness(t)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	h.children = plan.Children
	body := `{"digest":"sha256:d","url":"` + strings.Repeat("u", 5000) + `"}`
	row := settledTimes()
	row.ValuesRef = h.values.put("mem://child-values", []byte(body))
	row.Artifacts = []*orkstackv1.ArtifactRef{{Name: "report", Uri: "mem://report", SizeBytes: 3, Sha256: strings.Repeat("a", 64)}}
	row.Child = &orkstackv1.TaskChild{RunId: "ork-child-1", InputArtifacts: []*orkstackv1.NamedArtifact{
		{Name: "in", Artifact: &orkstackv1.ArtifactRef{Name: "in", Uri: "mem://in"}}}}
	h.resumeOf(t, parent, nil, unit("build", -1, keepIt), recordedUnit("release", settleIt, row), unit("announce", -1, rerunIt))
	must.OKf(t, h.runResetWf(t, parent, nil), "the resumed run")

	got := taskRunOfRun(h.ended(t), "release")
	if got.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || got.GetChild().GetRunId() != "ork-child-1" || got.GetChild().GetOrkflowName() != "release-block" ||
		len(got.GetChild().GetInputArtifacts()) != 1 || len(got.GetArtifacts()) != 1 || got.GetOutput().GetData().GetFields()["digest"].GetStringValue() != "sha256:d" {
		t.Fatalf("release = %v, want its child, its values and its artifacts", got)
	}
	calls := h.taskCalls("announce")
	if len(calls) != 1 || len(upstreamRefOf(calls[0], "release").GetArtifacts()) != 1 || upstreamRefOf(calls[0], "release").GetValues().GetUri() != "mem://child-values" {
		t.Fatalf("announce = %v, want one run handed the child's values and artifacts", calls)
	}
	if evs := eventsFor(h.harness, "ork-reset-1", "release"); len(evs) != 0 {
		t.Fatalf("release has events %v, want none", evs)
	}
}

// One that was not reached when a read-back failed keeps its record too: a
// composed task's artifacts are claims its run's archive lists.
func TestResetRun_AnUnreachedSettledTaskKeepsItsRecord(t *testing.T) {
	h := newResetHarness(t)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	h.children = plan.Children
	row := settledTimes()
	row.ValuesRef = h.values.put("mem://child-values", []byte(`{}`))
	row.Artifacts = []*orkstackv1.ArtifactRef{{Name: "report", Uri: "mem://report", SizeBytes: 3, Sha256: strings.Repeat("a", 64)}}
	row.Child = &orkstackv1.TaskChild{RunId: "ork-child-1", InputArtifacts: []*orkstackv1.NamedArtifact{
		{Name: "in", Artifact: &orkstackv1.ArtifactRef{Name: "in", Uri: "mem://in"}}}}
	h.resumeOf(t, parent, []string{"build"}, unit("build", -1, keepIt), recordedUnit("release", settleIt, row), unit("announce", -1, rerunIt))
	if err := h.runResetWf(t, parent, nil); err == nil {
		t.Fatal("the run succeeded without its kept result")
	}
	got := taskRunOfRun(h.ended(t), "release")
	if got.GetStatus() != orkstackv1.Status_STATUS_CANCELED || got.GetError().GetCode() != engine.ReasonResumeNotReadBack || !got.GetReadingBack() ||
		!proto.Equal(engine.CutRecordedRow(taskOf(parent, "release"), got), row) {
		t.Fatalf("release = %v, want not read back, with its record", got)
	}
}

func taskOf(wf *orkstackv1.Orkflow, id string) *orkstackv1.Task {
	for _, t := range wf.GetTasks() {
		if t.GetId() == id {
			return t
		}
	}
	return nil
}
