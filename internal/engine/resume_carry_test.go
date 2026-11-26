package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// A resumed run that carries on is the same resumed run: what it keeps stays
// kept and is never run, what runs again does so once, under the resume's
// scope and reading no memo where it was to, and its snapshots list the resume
// whichever history they are written in.
func TestResetRun_AResumedRunCarriesOn(t *testing.T) {
	// The run is held, and then carries on, as soon as its history is that
	// long: with one item run and two to run, and with the fan-out it keeps
	// items of not expanded yet.
	const held = 650
	raise := func(at string) func(*resetHarness) {
		return func(h *resetHarness) {
			h.behave = func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				if in.GetTaskId() == at {
					h.raiseHistory(held)
				}
				return succeeded(in.TaskId, map[string]any{"ok": true}), nil
			}
		}
	}
	t.Run("inside its fan-out", func(t *testing.T) { resumedCarriesOn(t, raise("convert"), "first", "convert[1]") })
	// One task in flight at most: the fan-out is held from the first pass.
	t.Run("before its fan-out expands", func(t *testing.T) {
		resumedCarriesOn(t, func(h *resetHarness) {
			raise("first")(h)
			h.inFlight = &engine.InFlightLimit{Max: 1, HardStop: 100}
		}, "first")
	})
}

// resumedCarriesOn runs a resumed run that hold makes carry on, having run
// first before it did.
func resumedCarriesOn(t *testing.T, hold func(*resetHarness), first ...string) {
	t.Helper()
	h := newResetHarness(t)
	hold(h)
	doc := strings.Replace(fan(resumeFanOutDocument, ", max_parallel: 1", "\n    cache: {enabled: true}"), "    each:", "    cache: {enabled: true}\n    each:", 1)
	// Beside them, a fan-out kept whole and one kept skipped.
	doc += "  - id: whole\n    uses: shell\n    needs: [build]\n    each: {over: \"{{ .inputs.xs }}\", as: x, max_items: 5}\n    with: {op: exec, command: \"echo whole\"}\n" +
		"  - id: skipped\n    uses: shell\n    needs: [build]\n    each: {over: \"{{ .inputs.xs }}\", as: x, max_items: 5}\n    with: {op: exec, command: \"echo skipped\"}\n"
	doc = strings.Replace(doc, "  - id: build\n", "  - id: first\n    uses: shell\n    with: {op: exec, command: \"echo first\"}\n  - id: build\n", 1)
	h.adjust = func(plan *engine.RunPlan) {
		continued := holdPlan()
		plan.History, plan.Continuation = continued.History, continued.Continuation
		if h.inFlight != nil {
			plan.InFlight = h.inFlight
		}
	}
	fresh := func(u engine.ResumeUnitPlan) engine.ResumeUnitPlan {
		u.Fresh = true
		return u
	}
	h.resume(t, doc, nil, unit("first", -1, rerunIt), unit("build", -1, keepIt), unit("convert", -1, rerunIt), unit("convert", 0, keepIt), unit("convert", 1, rerunIt),
		unit("convert", 2, keepIt), fresh(unit("convert", 3, rerunIt)), unit("convert", 4, rerunIt), fresh(unit("merge", -1, rerunIt)), unit("whole", -1, keepIt), unit("whole", 0, keepIt), unit("whole", 1, keepIt),
		unit("skipped", -1, skipIt))
	input := map[string]any{"xs": []any{"a", "b", "c", "d", "e"}}
	err := h.runResetWith(t, doc, input)

	calls := slices.Clone(h.calls)
	events := h.bus.Events("ork-reset-1", 0)
	var last *harness
	var carries []*orkstackv1.RunContinuation
	for generation := 1; ; generation++ {
		var can *workflow.ContinueAsNewError
		if !errors.As(err, &can) {
			break
		}
		if generation > 20 {
			t.Fatal("the run did not end")
		}
		var (
			id    string
			wf    *orkstackv1.Orkflow
			in    *orkstackv1.Payload
			plan  engine.RunPlan
			carry *orkstackv1.RunContinuation
		)
		must.OK(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &id, &wf, &in, &plan, &carry))
		carries = append(carries, carry)
		next := newHarnessShared(t, nil, h.snapshots, h.values, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return succeeded(in.TaskId, map[string]any{"ok": true}), nil
		})
		next.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-reset-1"})
		next.env.ExecuteWorkflow(engine.RunWorkflowType, id, wf, in, plan, carry)
		err = next.env.GetWorkflowError()
		calls = append(calls, next.calls...)
		events = append(events, next.bus.Events("ork-reset-1", 0)...)
		last = next
	}
	must.OKf(t, err, "the resumed run's last history")
	if len(carries) == 0 {
		t.Fatal("it never carried on")
	}
	var before []string
	for _, in := range h.calls {
		before = append(before, invocationName(in))
	}
	// Tasks that do not need each other run in no order.
	slices.Sort(before)
	slices.Sort(first)
	if !slices.Equal(before, first) {
		t.Fatalf("ran %v before it carried on, want %v", before, first)
	}
	for _, carry := range carries {
		if r := carry.GetResume(); r.GetSeqBase() != 42 || r.GetResumedAfter() != 41 || r.GetResumeCount() != 1 || r.GetResume().GetPrincipal() != "ops@example.com" {
			t.Fatalf("carried %v", r)
		}
	}

	got := map[string]string{}
	for _, in := range calls {
		name := invocationName(in)
		if _, twice := got[name]; twice {
			t.Errorf("%s ran twice", name)
		}
		got[name] = fmt.Sprintf("scoped %v, fresh %v", strings.Contains(in.GetIdempotencyKey(), "@r42"), in.GetCache().GetRefresh())
	}
	want := map[string]string{"first": "scoped true, fresh false", "convert[1]": "scoped true, fresh false", "convert[3]": "scoped true, fresh true",
		"convert[4]": "scoped true, fresh false", "merge": "scoped true, fresh true"}
	if len(got) != len(want) {
		t.Errorf("ran %v, want %v", got, want)
	}
	for name, how := range want {
		if got[name] != how {
			t.Errorf("%s: %s, want %s", name, got[name], how)
		}
	}

	run, ok := last.store.Get("ork-reset-1")
	if !ok || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(run.GetResumes()) != 1 || run.GetResumes()[0].GetScope() != 42 {
		t.Fatalf("run = %v, resumes %v", run.GetStatus(), run.GetResumes())
	}
	was, _ := h.store.Get("ork-reset-1")
	if !proto.Equal(run.GetResumes()[0], was.GetResumes()[0]) {
		t.Errorf("the resume reads %v after a switch, %v before it", run.GetResumes()[0], was.GetResumes()[0])
	}
	node := taskRunOfRun(run, "convert")
	for i, adopted := range []bool{true, false, true, false, false} {
		if item := node.GetItems()[i]; item.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || item.GetAdopted() != adopted {
			t.Errorf("item %d = %v", i, item)
		}
	}
	if row := taskRunOfRun(run, "whole"); row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !row.GetAdopted() || len(row.GetItems()) != 2 || !row.GetItems()[1].GetAdopted() {
		t.Errorf("whole = %v, want kept", row)
	}
	if row := taskRunOfRun(run, "skipped"); row.GetStatus() != orkstackv1.Status_STATUS_SKIPPED {
		t.Errorf("skipped = %v", row)
	}
	// RUN_CREATED and RUN_STARTED, then the resume's, with none left out.
	for i, e := range events {
		want := int64(i + 1)
		if i >= 2 {
			want = int64(42 + i - 2)
		}
		if e.GetSequence() != want {
			t.Fatalf("event %d is numbered %d, want %d", i, e.GetSequence(), want)
		}
	}
}

func invocationName(in *orkstackv1.ExecuteTaskInput) string {
	if in.GetEach() == nil {
		return in.GetTaskId()
	}
	return fmt.Sprintf("%s[%d]", in.GetTaskId(), in.GetEach().GetIndex())
}
