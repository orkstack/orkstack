package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// fuzzBytes hands out the fuzzer's bytes one decision at a time; past the
// end every decision is 0, so every input decodes to some plan input.
type fuzzBytes struct {
	b []byte
	i int
}

func (f *fuzzBytes) n(mod int) int {
	if f.i >= len(f.b) || mod <= 1 {
		f.i++
		return 0
	}
	v := int(f.b[f.i]) % mod
	f.i++
	return v
}

var fuzzStatuses = []orkstackv1.Status{sOK, sFail, sSkip, sCanc, sPend, sRun, sWait}

// fuzzPlanInput builds a consistent ResumePlanInput: a DAG whose needs point
// only at earlier tasks, every kind of task, pins, fan-outs over earlier
// tasks, every row status, and a verification answer for every unit.
func fuzzPlanInput(data []byte) ResumePlanInput {
	g := &fuzzBytes{b: data}
	n := 1 + g.n(10)
	in := ResumePlanInput{
		Definition: &orkstackv1.Orkflow{Name: "wf"},
		Base:       &orkstackv1.Run{Id: "ork-f", Status: sFail, Error: &orkstackv1.Error{Code: "RUN_FAILED"}},
		Facts: ResumeBaseFacts{
			ResetPoint: ResetPoint{CompletedEventID: 9},
			Earlier:    map[string]bool{},
			Dispatched: map[ResumeUnit]bool{},
			OpenPins:   map[string]bool{},
		},
		Evidence: ResumeEvidence{
			Kept:         map[ResumeUnit]KeptAnswer{},
			Follows:      map[string][]ResumeFollow{},
			ResultSHA256: map[string]string{},
			Resolutions:  map[string]ResolutionAnswer{},
			Recorded:     g.n(2) == 1,
		},
	}
	if g.n(3) == 0 {
		in.Class = ResumeBaseForceCanceled
		in.Base.Status = sCanc
		in.Base.Error.Code = forceCanceledReason
	}
	in.Facts.Continued = g.n(4) == 0
	answers := []KeptAnswer{KeptValid, KeptValid, KeptValid, KeptValid, KeptValid, KeptGone, KeptGone, KeptUnavailable, KeptUnreadable}
	for i := range n {
		id := fmt.Sprintf("t%d", i)
		t := &orkstackv1.Task{Id: id, Uses: "image:x"}
		for j := range i {
			if g.n(3) == 0 {
				t.Needs = append(t.Needs, fmt.Sprintf("t%d", j))
			}
		}
		switch g.n(7) {
		case 1:
			t.Uses, t.Await = "", &orkstackv1.AwaitSpec{}
		case 2:
			t.Uses, t.Sleep = "", &orkstackv1.SleepSpec{}
		case 3:
			t.Uses = "orkflow:child@1"
		case 4:
			if i > 0 {
				src := fmt.Sprintf("t%d", g.n(i))
				t.Each = &orkstackv1.EachSpec{Over: "x", Source: &orkstackv1.EachSource{TaskId: src, Path: []string{"xs"}}}
			}
		}
		if t.Each == nil && t.Uses == "image:x" && g.n(4) == 0 {
			t.Pin = fmt.Sprintf("p%d", g.n(2))
		}
		in.Definition.Tasks = append(in.Definition.Tasks, t)
		row := &orkstackv1.TaskRun{TaskId: id, Status: fuzzStatuses[g.n(len(fuzzStatuses))]}
		switch g.n(10) {
		case 0:
			row.Status = sCanc
			row.Error = &orkstackv1.Error{Code: taskStoppedByRunTimeoutCode}
		case 1:
			// Ended in an earlier generation: only a terminal success or skip
			// can be carried in without the run having failed there.
			if in.Facts.Continued && (row.Status == sOK || row.Status == sSkip) {
				in.Facts.Earlier[id] = true
			}
		}
		if g.n(8) == 0 {
			row.ReadingBack = true
			row.Status = sCanc
			row.Error = &orkstackv1.Error{Code: ReasonResumeNotReadBack}
		}
		if t.Each != nil {
			for k := range g.n(4) {
				k32 := int32(k)
				row.Items = append(row.Items, &orkstackv1.TaskRun{Item: &k32, Status: fuzzStatuses[g.n(len(fuzzStatuses))]})
				u := ResumeUnit{Task: id, Item: k}
				in.Evidence.Kept[u] = answers[g.n(len(answers))]
				in.Facts.Dispatched[u] = g.n(2) == 0
			}
		}
		in.Base.TaskRuns = append(in.Base.TaskRuns, row)
		u := nodeUnit(id)
		in.Evidence.Kept[u] = answers[g.n(len(answers))]
		in.Facts.Dispatched[u] = g.n(2) == 0
		in.Evidence.ResultSHA256[id] = fmt.Sprintf("d%d", g.n(2))
		if in.Evidence.Recorded && (t.Await != nil || strings.HasPrefix(t.Uses, "orkflow:")) {
			var follows []ResumeFollow
			for _, need := range t.Needs {
				if g.n(4) != 0 {
					follows = append(follows, ResumeFollow{Task: need, ResultSHA256: fmt.Sprintf("d%d", g.n(2))})
				}
			}
			in.Evidence.Follows[id] = follows
			if t.Await == nil && g.n(4) == 0 {
				in.Evidence.ChildOnRecord = map[string]bool{id: true}
			}
		}
		if t.Await != nil && g.n(3) == 0 {
			if g.n(2) == 0 {
				in.Evidence.Resolutions[id] = ResolutionAnswer{Completed: true}
			} else {
				in.Evidence.Resolutions[id] = ResolutionAnswer{Rejected: true}
			}
		}
	}
	if in.Class == ResumeBaseForceCanceled {
		for _, t := range in.Definition.Tasks {
			if t.Pin == "p0" && g.n(2) == 0 {
				in.Facts.OpenPins["p0"] = true
			}
		}
	}
	if in.Class == ResumeBaseForceCanceled || g.n(2) == 0 {
		in.Action = ResumeActionRetryFrom
		in.FromTask = fmt.Sprintf("t%d", g.n(n))
	}
	return in
}

// referenceRerunSet is an independent upper bound on S, from the definition
// and the base alone: every unit that could be a root (a lenient reading of
// the design's root rules), closed under the four closure rules and the
// downstream of every unit the plan waits again. A plan's S must lie within
// it: the planner may not put a unit in S that no rule reaches.
func referenceRerunSet(in ResumePlanInput, plan *ResumePlan) map[ResumeUnit]bool {
	defs := map[string]*orkstackv1.Task{}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, t := range in.Definition.Tasks {
		defs[t.Id] = t
	}
	for _, r := range in.Base.TaskRuns {
		rows[r.TaskId] = r
	}
	units := func(id string) []ResumeUnit {
		out := []ResumeUnit{{Task: id, Item: -1}}
		for _, it := range rows[id].Items {
			out = append(out, ResumeUnit{Task: id, Item: int(it.GetItem())})
		}
		return out
	}
	row := func(u ResumeUnit) *orkstackv1.TaskRun {
		if u.Item < 0 {
			return rows[u.Task]
		}
		for _, it := range rows[u.Task].Items {
			if int(it.GetItem()) == u.Item {
				return it
			}
		}
		return nil
	}
	ref := map[ResumeUnit]bool{}
	for _, t := range in.Definition.Tasks {
		if in.Facts.Earlier[t.Id] {
			continue
		}
		for _, u := range units(t.Id) {
			r := row(u)
			switch {
			case in.Evidence.Kept[u] == KeptGone && (r.Status == sOK || r.ReadingBack ||
				r.GetError().GetCode() == ReasonResumeNotReadBack || in.Class == ResumeBaseForceCanceled):
				ref[u] = true
			case r.Status != sOK && r.Status != sSkip:
				ref[u] = true
			case in.Class == ResumeBaseForceCanceled && in.Facts.OpenPins[t.Pin]:
				ref[u] = true
			}
		}
	}
	if in.FromTask != "" {
		for _, u := range units(in.FromTask) {
			ref[u] = true
		}
	}
	waits := map[string]bool{}
	for _, u := range plan.Units {
		if u.Disposition == DispositionWait && !u.InRerunSet && u.Unit.Item < 0 {
			waits[u.Unit.Task] = true
		}
	}
	for changed := true; changed; {
		changed = false
		addWhole := func(id string) {
			if in.Facts.Earlier[id] {
				return
			}
			for _, u := range units(id) {
				if !ref[u] {
					ref[u], changed = true, true
				}
			}
		}
		for _, t := range in.Definition.Tasks {
			node := ResumeUnit{Task: t.Id, Item: -1}
			for _, u := range units(t.Id)[1:] {
				if ref[u] && !ref[node] {
					ref[node], changed = true, true
				}
			}
			for _, need := range t.Needs {
				if ref[ResumeUnit{Task: need, Item: -1}] || waits[need] {
					addWhole(t.Id)
				}
			}
			if t.Pin != "" && ref[node] {
				for _, m := range in.Definition.Tasks {
					if m.Pin == t.Pin {
						addWhole(m.Id)
					}
				}
			}
			if src := t.GetEach().GetSource().GetTaskId(); src != "" && ref[ResumeUnit{Task: src, Item: -1}] {
				addWhole(t.Id)
			}
		}
	}
	return ref
}

// FuzzResumePlan checks the planner's invariants on generated runs: every
// plan it returns is closed under the closure rules, keeps every task
// outside S as in the base, keeps nothing that needs an S member, puts no
// composed task in S, adopts only verified results, and reruns every member
// of a pin open at a force cancel. A consistent input never yields an
// internal error, and planning twice gives the same plan.
func FuzzResumePlan(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{5, 1, 1, 0, 0, 1, 2, 3})
	f.Add([]byte{9, 0, 2, 0, 4, 1, 1, 1, 0, 3, 0, 2, 1, 0, 1, 6, 0, 2, 0, 1, 1})
	f.Add([]byte{3, 1, 0, 0, 3, 0, 2, 1, 1, 0, 4, 0, 2, 2, 1, 0, 0, 1, 0, 1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		in := fuzzPlanInput(data)
		plan, err := PlanResume(in)
		if err != nil {
			var r *ResumeRefusal
			if !errors.As(err, &r) {
				t.Fatalf("PlanResume on a consistent input: %v", err)
			}
			if r.Code == "" || r.Class == "" || r.Message == "" {
				t.Fatalf("refusal without code, class or message: %+v", r)
			}
			return
		}
		if err := checkResumePlan(in, plan); err != nil {
			t.Fatalf("invariant: %v", err)
		}
		ref := referenceRerunSet(in, plan)
		for _, u := range plan.RerunSet() {
			if !ref[u] {
				t.Fatalf("%s is in S, but no rule reaches it", u)
			}
		}
		again, err := PlanResume(in)
		if err != nil || fmt.Sprint(again.Units) != fmt.Sprint(plan.Units) {
			t.Fatalf("planning twice differs: %v", err)
		}
	})
}
