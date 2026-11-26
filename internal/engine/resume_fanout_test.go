package engine_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// build, then one convert per input element, then merge over them.
const resumeFanOutDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-fan
inputs:
  xs: {type: array}
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
  - id: convert
    uses: shell
    needs: [build]
    each: {over: "{{ .inputs.xs }}", as: x, max_items: 5%s}
    with: {op: exec, command: "echo {{ .each.x }}"}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec, command: "echo merge"}%s
`

// resumeLinesDocument fans out over the lines of what list wrote.
const resumeLinesDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-lines
tasks:
  - id: list
    uses: shell
    with: {op: exec, command: "list"}
  - id: convert
    uses: shell
    needs: [list]
    each: {over_artifact: list/files.jsonl, format: jsonl, max_items: 5}
    with: {op: exec, command: "convert"}
`

var fanInput = map[string]any{"xs": []any{"a", "b", "c"}}

func unit(task string, item int, d orkstackv1.ResumeDisposition) engine.ResumeUnitPlan {
	u := engine.ResumeUnitPlan{Task: task, Item: item, Disposition: d}
	if d == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT {
		u.EncodedSize = 512
	}
	return u
}

const (
	rerunIt = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN
	keepIt  = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT
	skipIt  = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED
)

// resume gives the harness a plan of document's units, and commits every kept
// result the plan reads back but those missing names.
func (h *resetHarness) resume(t *testing.T, document string, missing []string, units ...engine.ResumeUnitPlan) {
	t.Helper()
	h.resumeOf(t, compile(t, document), missing, units...)
}

func (h *resetHarness) resumeOf(t *testing.T, wf *orkstackv1.Orkflow, missing []string, units ...engine.ResumeUnitPlan) {
	t.Helper()
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, N: 0, PlanDigest: strings.Repeat("ab", 32)}.String()}
	h.plan = &orkstackv1.ResumePlanRecord{RunId: "ork-reset-1", Scope: 42, BaseTerminalSequence: 41, Principal: "ops@example.com", At: timestamppb.New(time.Unix(1700000000, 0)),
		Reason: "flaky network", SupersededReason: "RUN_FAILED",
		Block: &orkstackv1.ResumeBlock{SeqBase: 42, Scope: 42, Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME, Readback: testReadback()}}
	h.packOf(t, wf, units...)
	for _, u := range units {
		var def *orkstackv1.Task
		for _, d := range wf.GetTasks() {
			if d.GetId() == u.Task {
				def = d
			}
		}
		if u.Disposition != keepIt || def.GetEach() != nil && u.Item < 0 || slices.Contains(missing, unitName(u)) {
			continue
		}
		identity, err := engine.ExpectedCommitIdentity(wf, "ork-reset-1", def, int32(u.Item), u.Epoch)
		must.OK(t, err)
		commit := &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: identity.Namespace, RunId: identity.RunID, TaskId: u.Task,
			OrkflowDefinitionDigest: identity.DefinitionDigest, IdempotencyKey: identity.IdempotencyKey,
			Image: def.GetImage(), Attempt: 2, OrkerId: "orker-a",
		}
		if u.Task == "list" {
			commit.Artifacts = []*orkstackv1.ArtifactRef{linesSource}
		}
		_, err = h.commits.Create(context.Background(), "mem://commits/"+identity.Key, commit)
		must.OK(t, err)
	}
}

func unitName(u engine.ResumeUnitPlan) string {
	if u.Item < 0 {
		return u.Task
	}
	return fmt.Sprintf("%s[%d]", u.Task, u.Item)
}

// itemsRun are the items of task dispatched, with their keys.
func (h *resetHarness) itemsRun(task string) map[int32]string {
	out := map[int32]string{}
	for _, in := range h.taskCalls(task) {
		out[in.GetEach().GetIndex()] = in.GetIdempotencyKey()
	}
	return out
}

func (h *resetHarness) ended(t *testing.T) *orkstackv1.Run {
	t.Helper()
	run, ok := h.store.Get("ork-reset-1")
	if !ok {
		t.Fatal("no run was projected")
	}
	return run
}

func fan(document, each, merge string) string { return fmt.Sprintf(document, each, merge) }

// A fan-out keeps the items that succeeded and runs the others again, each
// under the resume's own key; what depends on it is told each item's scope.
func TestResetRun_AFanOutKeepsItsItemsAndRunsTheOthersAgain(t *testing.T) {
	h := newResetHarness(t)
	doc := fan(resumeFanOutDocument, "", "")
	kept0 := unit("convert", 0, keepIt)
	kept0.Epoch = 7
	h.resume(t, doc, nil, unit("build", -1, keepIt), unit("convert", -1, rerunIt), kept0, unit("convert", 1, rerunIt), unit("convert", 2, keepIt), unit("merge", -1, rerunIt))
	must.OKf(t, h.runResetWith(t, doc, fanInput), "the resumed run")

	if got := h.itemsRun("convert"); len(got) != 1 || !strings.Contains(got[1], "@r42/") {
		t.Fatalf("items run = %v, want item 1 alone, under scope 42", got)
	}
	run := h.ended(t)
	node := taskRunOfRun(run, "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(node.GetItems()) != 3 || node.GetEach().GetCount() != 3 {
		t.Fatalf("convert = %v", node)
	}
	for i, want := range []struct {
		adopted bool
		epoch   int64
	}{{true, 7}, {false, 42}, {true, 0}} {
		if item := node.GetItems()[i]; item.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || item.GetAdopted() != want.adopted ||
			item.GetResultEpoch() != want.epoch || item.GetReadingBack() {
			t.Errorf("item %d = %v, want SUCCEEDED adopted %v at epoch %d", i, item, want.adopted, want.epoch)
		}
	}
	merges := h.taskCalls("merge")
	if len(merges) != 1 {
		t.Fatalf("merge ran %d times", len(merges))
	}
	if e := upstreamRefOf(merges[0], "convert").GetEach(); e.GetCount() != 3 || len(e.GetEpochs()) != 2 ||
		e.GetEpochs()[0].GetFirst() != 0 || e.GetEpochs()[0].GetEpoch() != 7 || e.GetEpochs()[1].GetFirst() != 1 || e.GetEpochs()[1].GetEpoch() != 42 {
		t.Fatalf("merge was told %v, want item 0 at 7 and item 1 at 42", e)
	}
	// What the plan counts: items, not the fan-out beside them.
	for _, e := range h.bus.Events("ork-reset-1", 0) {
		if r := e.GetResumed(); r != nil && (r.GetKeptCount() != 3 || r.GetRerunCount() != 2) {
			t.Errorf("RUN_RESUMED = %v, want 3 kept (build, 2 items) and 2 again (1 item, merge)", r)
		}
		if k := e.GetResultsKept(); k != nil && k.GetKeptCount() != 3 {
			t.Errorf("RUN_RESULTS_KEPT = %v", k)
		}
	}
	if r := run.GetResumes(); len(r) != 1 || r[0].GetSequence() != 42 || r[0].GetScope() != 42 || r[0].GetResumedAfterSequence() != 41 ||
		r[0].GetPrincipal() != "ops@example.com" || r[0].GetSupersededReason() != "RUN_FAILED" ||
		r[0].GetFromState() != orkstackv1.ResumeFromState_RESUME_FROM_STATE_FAILED || r[0].GetEnded() != nil {
		t.Fatalf("resumes = %v, want this one", r)
	}
}

// What a resume runs again for another result reads no memo, and whatever
// else it runs does: a task, an item, and every item of a fan-out that runs
// again whole, ones its base did not have included.
func TestResetRun_AFreshRerunReadsNoMemo(t *testing.T) {
	doc := strings.Replace(fan(resumeFanOutDocument, "", "\n    cache: {enabled: true}"), "    each:", "    cache: {enabled: true}\n    each:", 1)
	fresh := func(u engine.ResumeUnitPlan) engine.ResumeUnitPlan {
		u.Fresh = true
		return u
	}
	for name, tc := range map[string]struct {
		node  engine.ResumeUnitPlan
		items []engine.ResumeUnitPlan
		want  map[int32]bool
	}{
		"one item of those that run again": {
			node:  unit("convert", -1, rerunIt),
			items: []engine.ResumeUnitPlan{unit("convert", 0, keepIt), fresh(unit("convert", 1, rerunIt)), unit("convert", 2, rerunIt)},
			want:  map[int32]bool{1: true, 2: false},
		},
		"items of a fan-out that runs again whole": {
			node:  unit("convert", -1, rerunIt),
			items: []engine.ResumeUnitPlan{fresh(unit("convert", 0, rerunIt)), unit("convert", 1, rerunIt), unit("convert", 2, rerunIt)},
			want:  map[int32]bool{0: true, 1: false, 2: false},
		},
		"a whole fan-out": {
			node:  fresh(unit("convert", -1, rerunIt)),
			items: []engine.ResumeUnitPlan{unit("convert", 0, rerunIt)},
			want:  map[int32]bool{0: true, 1: true, 2: true},
		},
	} {
		for _, merge := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s, merge fresh %v", name, merge), func(t *testing.T) {
				h := newResetHarness(t)
				last := unit("merge", -1, rerunIt)
				last.Fresh = merge
				h.resume(t, doc, nil, append(append([]engine.ResumeUnitPlan{unit("build", -1, keepIt), tc.node}, tc.items...), last)...)
				must.OKf(t, h.runResetWith(t, doc, fanInput), "the resumed run")
				got := map[int32]bool{}
				for _, in := range h.taskCalls("convert") {
					got[in.GetEach().GetIndex()] = in.GetCache().GetRefresh()
				}
				if !maps.Equal(got, tc.want) {
					t.Errorf("items run, and whether fresh = %v, want %v", got, tc.want)
				}
				if calls := h.taskCalls("merge"); len(calls) != 1 || calls[0].GetCache() == nil || calls[0].GetCache().GetRefresh() != merge {
					t.Errorf("merge = %v, want one run of a cached task, fresh %v", calls, merge)
				}
			})
		}
	}
}

// A fan-out kept whole runs nothing, and is SUCCEEDED when its items are in.
func TestResetRun_AFanOutKeptWholeRunsNothing(t *testing.T) {
	h := newResetHarness(t)
	doc := fan(resumeFanOutDocument, "", "")
	h.resume(t, doc, nil, unit("build", -1, keepIt), unit("convert", -1, keepIt), unit("convert", 0, keepIt), unit("convert", 1, keepIt),
		unit("convert", 2, keepIt), unit("merge", -1, rerunIt))
	must.OKf(t, h.runResetWith(t, doc, fanInput), "the resumed run")
	if n := len(h.taskCalls("convert")); n != 0 {
		t.Fatalf("%d items ran again", n)
	}
	node := taskRunOfRun(h.ended(t), "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !node.GetAdopted() || node.GetReadingBack() || !node.GetEach().GetExpanded() {
		t.Fatalf("convert = %v, want SUCCEEDED, adopted, expanded", node)
	}
	if e := upstreamRefOf(h.taskCalls("merge")[0], "convert").GetEach(); e.GetCount() != 3 || len(e.GetEpochs()) != 0 {
		t.Fatalf("merge was told %v", e)
	}
}

// A fan-out whose own unit runs again though every item is kept (its run
// ended between the last item and its own end) is done at its expansion.
func TestResetRun_AFanOutWithEveryItemKeptIsDoneAtItsExpansion(t *testing.T) {
	h := newResetHarness(t)
	doc := fan(resumeFanOutDocument, "", "")
	h.resume(t, doc, nil, unit("build", -1, keepIt), unit("convert", -1, rerunIt), unit("convert", 0, keepIt), unit("convert", 1, keepIt),
		unit("convert", 2, keepIt), unit("merge", -1, rerunIt))
	must.OKf(t, h.runResetWith(t, doc, fanInput), "the resumed run")
	if n := len(h.taskCalls("convert")); n != 0 || taskRunOfRun(h.ended(t), "convert").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("%d items ran, convert %v", n, taskRunOfRun(h.ended(t), "convert").GetStatus())
	}
}

// A fan-out over nothing, kept, and one skipped, are as their base left them.
func TestResetRun_AnEmptyOrSkippedFanOutIsKept(t *testing.T) {
	doc := fan(resumeFanOutDocument, "", "")
	t.Run("empty", func(t *testing.T) {
		h := newResetHarness(t)
		h.resume(t, doc, nil, unit("build", -1, keepIt), unit("convert", -1, keepIt), unit("merge", -1, rerunIt))
		must.OKf(t, h.runResetWith(t, doc, map[string]any{"xs": []any{}}), "the resumed run")
		if node := taskRunOfRun(h.ended(t), "convert"); node.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(node.GetItems()) != 0 {
			t.Fatalf("convert = %v", node)
		}
	})
	t.Run("skipped", func(t *testing.T) {
		h := newResetHarness(t)
		h.resume(t, doc, nil, unit("build", -1, rerunIt), unit("convert", -1, skipIt), unit("merge", -1, skipIt))
		must.OKf(t, h.runResetWith(t, doc, fanInput), "the resumed run")
		if node := taskRunOfRun(h.ended(t), "convert"); node.GetStatus() != orkstackv1.Status_STATUS_SKIPPED || len(h.taskCalls("convert")) != 0 {
			t.Fatalf("convert = %v", node)
		}
	})
}

// A fan-out none of whose items are kept is expanded anew, whatever its
// source counts now, each item under the resume's scope.
func TestResetRun_AFanOutThatRunsAgainWholeIsExpandedAnew(t *testing.T) {
	h := newResetHarness(t)
	doc := fan(resumeFanOutDocument, "", "")
	h.resume(t, doc, nil, unit("build", -1, rerunIt), unit("convert", -1, rerunIt), unit("convert", 0, rerunIt), unit("convert", 1, rerunIt), unit("merge", -1, rerunIt))
	must.OKf(t, h.runResetWith(t, doc, fanInput), "the resumed run")
	got := h.itemsRun("convert")
	if len(got) != 3 || !strings.Contains(got[0], "@r42/") || !strings.Contains(got[2], "@r42/") {
		t.Fatalf("items run = %v, want all three under scope 42", got)
	}
	for i, item := range taskRunOfRun(h.ended(t), "convert").GetItems() {
		if item.GetResultEpoch() != 42 || item.GetResumeDisposition() != rerunIt {
			t.Errorf("item %d = %v, want rerun at 42", i, item)
		}
	}
}

// A source that counts other than the kept items is refused, and no item runs.
func TestResetRun_AFanOutWhoseCountChangedKeepsNothing(t *testing.T) {
	h := newResetHarness(t)
	doc := fan(resumeFanOutDocument, "", "")
	h.resume(t, doc, nil, unit("build", -1, keepIt), unit("convert", -1, rerunIt), unit("convert", 0, keepIt), unit("convert", 1, rerunIt), unit("merge", -1, rerunIt))
	if err := h.runResetWith(t, doc, fanInput); err == nil {
		t.Fatal("the run succeeded over a fan-out that changed")
	}
	node := taskRunOfRun(h.ended(t), "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != engine.ResumeFanOutChangedCode || len(h.taskCalls("convert")) != 0 {
		t.Fatalf("convert = %v, %d items ran", node, len(h.taskCalls("convert")))
	}
	if got := []orkstackv1.Status{node.GetItems()[0].GetStatus(), node.GetItems()[1].GetStatus()}; !slices.Equal(got,
		[]orkstackv1.Status{orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.Status_STATUS_CANCELED}) {
		t.Fatalf("items = %v, want the kept one as it is and the other canceled", got)
	}
}

// A kept item whose result is gone fails its fan-out before anything runs
// again; the items not read back stay to be kept by the next resume.
func TestResetRun_AKeptItemThatIsGoneFailsItsFanOut(t *testing.T) {
	for name, node := range map[string]orkstackv1.ResumeDisposition{"kept whole": keepIt, "run again in part": rerunIt} {
		t.Run(name, func(t *testing.T) {
			h := newResetHarness(t)
			doc := fan(resumeFanOutDocument, "", "")
			last := unit("convert", 2, keepIt)
			if node == rerunIt {
				last = unit("convert", 2, rerunIt)
			}
			h.plan = nil
			h.resume(t, doc, []string{"convert[0]"}, unit("build", -1, keepIt), unit("convert", -1, node), unit("convert", 0, keepIt),
				unit("convert", 1, keepIt), last, unit("merge", -1, rerunIt))
			if err := h.runResetWith(t, doc, fanInput); err == nil {
				t.Fatal("the run succeeded without its kept item")
			}
			if n := len(h.taskCalls("convert")) + len(h.taskCalls("merge")); n != 0 {
				t.Fatalf("%d tasks ran", n)
			}
			row := taskRunOfRun(h.ended(t), "convert")
			statuses := []string{}
			for _, item := range row.GetItems() {
				statuses = append(statuses, strings.TrimPrefix(item.GetStatus().String(), "STATUS_")+" "+item.GetError().GetCode())
			}
			for _, m := range []string{row.GetError().GetMessage(), h.ended(t).GetError().GetMessage()} {
				if m == "" || strings.Contains(m, "activity") || strings.Contains(m, "scheduledEventID") {
					t.Errorf("message %q names how it was read, not what happened", m)
				}
			}
			// The task that is there is kept, not blamed for the one that is not.
			if b := taskRunOfRun(h.ended(t), "build"); b.GetStatus() != orkstackv1.Status_STATUS_CANCELED || b.GetError().GetCode() != "RESUME_NOT_READ_BACK" || !b.GetReadingBack() {
				t.Errorf("build = %v %v, want not read back and kept by the next resume", b.GetStatus(), b.GetError())
			}
			if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != "COMMIT_MISSING" ||
				!slices.Contains(statuses, "FAILED COMMIT_MISSING") || slices.ContainsFunc(statuses, func(s string) bool { return strings.HasPrefix(s, "PENDING") }) {
				t.Fatalf("convert = %v %v, items %v", row.GetStatus(), row.GetError(), statuses)
			}
			// What tells the next resume to check the item's result again.
			for i, item := range row.GetItems() {
				if item.GetError().GetCode() == "COMMIT_MISSING" && item.GetResumeDisposition() != keepIt {
					t.Errorf("item %d = %v, want still to be kept", i, item.GetResumeDisposition())
				}
			}
		})
	}
}

// The run failing before a resumed fan-out is expanded again leaves its kept
// items and cancels the others.
func TestResetRun_AFanOutNeverReachedKeepsWhatItHas(t *testing.T) {
	h := newResetHarness(t)
	h.behave = func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return nil, temporal.NewNonRetryableApplicationError("no", "ActivityFailed", nil)
	}
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-fan-late
inputs:
  xs: {type: array}
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
  - id: other
    uses: shell
    with: {op: exec, command: "echo other"}
  - id: gate
    uses: shell
    needs: [other]
    with: {op: exec, command: "echo gate"}
  - id: convert
    uses: shell
    needs: [build, gate]
    each: {over: "{{ .inputs.xs }}", as: x, max_items: 5}
    with: {op: exec, command: "echo {{ .each.x }}"}
`
	h.resume(t, doc, nil, unit("build", -1, keepIt), unit("other", -1, rerunIt), unit("gate", -1, rerunIt),
		unit("convert", -1, rerunIt), unit("convert", 0, keepIt), unit("convert", 1, rerunIt), unit("convert", 2, rerunIt))
	if err := h.runResetWith(t, doc, fanInput); err == nil {
		t.Fatal("the run succeeded")
	}
	node := taskRunOfRun(h.ended(t), "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_CANCELED || node.GetItems()[0].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
		node.GetItems()[1].GetStatus() != orkstackv1.Status_STATUS_CANCELED || node.GetItems()[2].GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("convert = %v", node)
	}
}

// A kept producer's lines are indexed again from the artifact its commit
// records, and the kept items stay; a producer that runs again has its own
// lines, and every item runs over them.
func TestResetRun_ALinesFanOutFollowsItsProducer(t *testing.T) {
	index := func(count int32) func(*orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		return func(*orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
			return &orkstackv1.IndexEachLinesResult{Count: count, TotalLines: int64(count), Index: linesIndex}, nil
		}
	}
	t.Run("kept", func(t *testing.T) {
		h := newResetHarness(t)
		h.index = index(3)
		h.resume(t, resumeLinesDocument, nil, unit("list", -1, keepIt), unit("convert", -1, rerunIt), unit("convert", 0, keepIt),
			unit("convert", 1, rerunIt), unit("convert", 2, keepIt))
		must.OKf(t, h.runResetOf(t, resumeLinesDocument), "the resumed run")
		if len(h.indexed) != 1 || h.indexed[0].GetSource().GetSha256() != linesSource.GetSha256() || len(h.taskCalls("list")) != 0 {
			t.Fatalf("indexed %v, want the kept artifact once", h.indexed)
		}
		calls := h.taskCalls("convert")
		if len(calls) != 1 || calls[0].GetEach().GetIndex() != 1 || calls[0].GetEach().GetLines().GetIndex().GetSha256() != linesIndex.GetSha256() ||
			calls[0].GetEach().GetLines().GetSource().GetSha256() != linesSource.GetSha256() {
			t.Fatalf("items run = %v, want item 1 over the kept lines", calls)
		}
	})
	t.Run("kept, and its lines count otherwise", func(t *testing.T) {
		h := newResetHarness(t)
		h.index = index(2)
		h.resume(t, resumeLinesDocument, nil, unit("list", -1, keepIt), unit("convert", -1, rerunIt), unit("convert", 0, keepIt),
			unit("convert", 1, rerunIt), unit("convert", 2, keepIt))
		if err := h.runResetOf(t, resumeLinesDocument); err == nil || taskRunOfRun(h.ended(t), "convert").GetError().GetCode() != engine.ResumeFanOutChangedCode {
			t.Fatalf("err = %v, convert %v", err, taskRunOfRun(h.ended(t), "convert").GetError())
		}
	})
	t.Run("run again", func(t *testing.T) {
		h := newResetHarness(t)
		h.index = index(2)
		h.behave = func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			r := succeeded(in.TaskId, map[string]any{"ok": true})
			if in.TaskId == "list" {
				r.Artifacts = []*orkstackv1.ArtifactRef{{Name: "files.jsonl", Uri: "mem://new", Sha256: strings.Repeat("99", 32), SizeBytes: 10}}
			}
			return r, nil
		}
		h.resume(t, resumeLinesDocument, nil, unit("list", -1, rerunIt), unit("convert", -1, rerunIt), unit("convert", 0, rerunIt),
			unit("convert", 1, rerunIt), unit("convert", 2, rerunIt))
		must.OKf(t, h.runResetOf(t, resumeLinesDocument), "the resumed run")
		if len(h.indexed) != 1 || h.indexed[0].GetSource().GetSha256() != strings.Repeat("99", 32) {
			t.Fatalf("indexed %v, want the new artifact", h.indexed)
		}
		if got := h.itemsRun("convert"); len(got) != 2 || !strings.Contains(got[0], "@r42/") || !strings.Contains(got[1], "@r42/") {
			t.Fatalf("items run = %v, want the two new lines under scope 42", got)
		}
	})
}

// A failure handler of a continue fan-out is told the scope of the items a
// resume ran again, and which failed.
func TestResetRun_AContinueFanOutsHandlerGetsItsItemsScopes(t *testing.T) {
	h := newResetHarness(t)
	h.behave = func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "convert" && in.GetEach().GetIndex() == 2 {
			return nil, temporal.NewNonRetryableApplicationError("no", "ActivityFailed", nil)
		}
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}
	doc := fan(resumeFanOutDocument, ", on_item_failure: continue", "\n    join: any_failed")
	h.resume(t, doc, nil, unit("build", -1, keepIt), unit("convert", -1, rerunIt), unit("convert", 0, keepIt), unit("convert", 1, rerunIt),
		unit("convert", 2, rerunIt), unit("merge", -1, rerunIt))
	if err := h.runResetWith(t, doc, fanInput); err == nil {
		t.Fatal("the run succeeded although an item failed")
	}
	merges := h.taskCalls("merge")
	if len(merges) != 1 {
		t.Fatalf("the handler ran %d times", len(merges))
	}
	e := upstreamRefOf(merges[0], "convert").GetEach()
	if !slices.Equal(e.GetFailed(), []int32{2}) || len(e.GetEpochs()) == 0 || e.GetEpochs()[0].GetFirst() != 1 || e.GetEpochs()[0].GetEpoch() != 42 {
		t.Fatalf("the handler was told %v, want item 2 failed and item 1 at 42", e)
	}
}

// A plan that keeps part of a fan-out it calls kept whole, or items of a
// skipped one, is not carried out.
func TestResetRun_AFanOutThatDoesNotGoTogetherIsNotRun(t *testing.T) {
	doc := fan(resumeFanOutDocument, "", "")
	for name, units := range map[string][]engine.ResumeUnitPlan{
		"kept, with an item skipped": {unit("build", -1, keepIt), unit("convert", -1, keepIt), unit("convert", 0, keepIt), unit("convert", 1, skipIt), unit("merge", -1, rerunIt)},
		"skipped, with an item kept": {unit("build", -1, keepIt), unit("convert", -1, skipIt), unit("convert", 0, keepIt), unit("merge", -1, rerunIt)},
	} {
		t.Run(name, func(t *testing.T) {
			h := newResetHarness(t)
			h.resume(t, doc, nil, units...)
			err := h.runResetWith(t, doc, fanInput)
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanInvalid {
				t.Fatalf("error = %v, want RESUME_PLAN_INVALID", err)
			}
			if n := len(h.taskCalls("convert")) + len(h.taskCalls("merge")); n != 0 {
				t.Fatalf("%d tasks ran", n)
			}
		})
	}
}

// An operator's cancel while kept results are read back ends the run
// canceled, blames no task, and leaves them to be kept by the next resume.
func TestResetRun_ACancelDuringTheReadBackBlamesNoTask(t *testing.T) {
	h := newResetHarness(t)
	doc := fan(resumeFanOutDocument, "", "")
	h.resume(t, doc, nil, unit("build", -1, keepIt), unit("convert", -1, rerunIt), unit("convert", 0, keepIt), unit("convert", 1, rerunIt),
		unit("convert", 2, keepIt), unit("merge", -1, rerunIt))
	h.adopting = func() error {
		h.env.CancelWorkflow()
		return temporal.NewApplicationError("the store did not answer", "COMMIT_READ_FAILED")
	}
	if err := h.runResetWith(t, doc, fanInput); err == nil {
		t.Fatal("the run succeeded")
	}
	run := h.ended(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run = %v %v, want CANCELED", run.GetStatus(), run.GetError())
	}
	notRead := func(row *orkstackv1.TaskRun) bool {
		return row.GetStatus() == orkstackv1.Status_STATUS_CANCELED && row.GetError().GetCode() == "RESUME_NOT_READ_BACK" && row.GetReadingBack()
	}
	convert := taskRunOfRun(run, "convert")
	if !notRead(taskRunOfRun(run, "build")) || !notRead(convert.GetItems()[0]) || !notRead(convert.GetItems()[2]) ||
		convert.GetStatus() != orkstackv1.Status_STATUS_CANCELED || convert.GetItems()[1].GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("rows = %v", run.GetTaskRuns())
	}
	if n := len(h.taskCalls("convert")) + len(h.taskCalls("merge")); n != 0 {
		t.Fatalf("%d tasks ran", n)
	}
}
