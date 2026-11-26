package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// carriedResume is the resume a carry holds of a run resumed at seq, from an
// ending whose last event was after.
func carriedResume(seq, after int64) *orkstackv1.RunResumeCarry {
	return &orkstackv1.RunResumeCarry{SeqBase: uint64(seq), ResumedAfter: after, ResumeCount: 1, Resume: &orkstackv1.RunResume{
		At: timestamppb.New(chainCreatedAt), Sequence: seq, Scope: uint64(seq), ResumedAfterSequence: after}}
}

// The stream of a resumed run that carried on is whole: what its reset set
// aside is read from that ending's body, whichever history the reader is in,
// and its resume is read from its carry.
func TestResumedRun_ThatCarriedOnIsTheWholeStream(t *testing.T) {
	base := setAside(6)
	for _, e := range base {
		e.RunId = chainRunID
	}
	chain := func(t *testing.T) (*chainFixture, *fakeEndings) {
		f := newChain(t, chainSpec{closed: []int{9, 2}, current: 1, finish: true, resumed: 7, resumedAfter: 6,
			skip: map[int64]bool{3: true, 4: true, 5: true, 6: true}})
		endings := &fakeEndings{events: base}
		f.reader = f.reader.WithEndings(endings)
		return f, endings
	}
	for from, asked := range map[int64]int{0: 1, 4: 1, 6: 0, 9: 0, 10: 0} {
		t.Run(fmt.Sprintf("live, from %d", from), func(t *testing.T) {
			f, endings := chain(t)
			got := liveReadsAgree(t, f.reader, chainRunID, from)
			requireContiguous(t, got, from+1, 13)
			if len(endings.asked) != 2*asked {
				t.Fatalf("asked for %v, want %d each", endings.asked, asked)
			}
		})
	}
	t.Run("archive", func(t *testing.T) {
		f, endings := chain(t)
		got, err := f.reader.EventsForArchive(context.Background(), chainRunID)
		if err != nil || got.Scope != 7 || len(got.Gaps) != 0 || !slices.Equal(endings.asked, []uint64{7}) {
			t.Fatalf("scope %d, gaps %v, asked %v, %v", got.Scope, got.Gaps, endings.asked, err)
		}
		requireContiguous(t, got.Events, 1, 13)
	})
	t.Run("archive, the body lost", func(t *testing.T) {
		f, endings := chain(t)
		endings.err = errEndingDamaged
		got, err := f.reader.EventsForArchive(context.Background(), chainRunID)
		if err != nil || got.Scope != 7 || len(got.Gaps) != 1 || got.Gaps[0].First != 3 || got.Gaps[0].Last != 6 {
			t.Fatalf("scope %d, gaps %v, %v; want 3 to 6 lost", got.Scope, got.Gaps, err)
		}
	})
	t.Run("the body says less than the resume set aside", func(t *testing.T) {
		f, endings := chain(t)
		endings.events = base[:4]
		got, err := f.reader.EventsForArchive(context.Background(), chainRunID)
		if err != nil || len(got.Gaps) != 1 || got.Gaps[0].First != 5 || got.Gaps[0].Last != 6 {
			t.Fatalf("gaps %v, %v; want 5 and 6 lost", got.Gaps, err)
		}
	})
	// A carry that names no object of the run's is refused, and the ending is
	// still the resume's.
	t.Run("archive, its carry refused", func(t *testing.T) {
		f, _ := chain(t)
		carry := proto.Clone(f.carry).(*orkstackv1.RunContinuation)
		carry.Segments[0].Object.Uri = strings.Replace(carry.Segments[0].Object.Uri, "/"+chainRunID+"/", "/ork-1700000000-8/", 1)
		f.client.generations[f.runID][f.current].events[0] = f.started(carry)
		got, err := f.reader.EventsForArchive(context.Background(), chainRunID)
		if err != nil || got.Scope != 7 || len(got.Gaps) == 0 {
			t.Fatalf("scope %d, gaps %v, %v; want scope 7 and what is lost", got.Scope, got.Gaps, err)
		}
	})
	// So it is where what its carry says of the resume does not go together.
	t.Run("archive, its carry's resume refused", func(t *testing.T) {
		f, _ := chain(t)
		carry := proto.Clone(f.carry).(*orkstackv1.RunContinuation)
		carry.Resume.ResumeCount = 0
		f.client.generations[f.runID][f.current].events[0] = f.started(carry)
		got, err := f.reader.EventsForArchive(context.Background(), chainRunID)
		if err != nil || got.Scope != 7 || len(got.Gaps) == 0 {
			t.Fatalf("scope %d, gaps %v, %v; want scope 7 and what is lost", got.Scope, got.Gaps, err)
		}
	})
	t.Run("a run read lists its resumes", func(t *testing.T) {
		f, endings := chain(t)
		endings.resumes = []*orkstackv1.RunResume{{Scope: 3}}
		run, err := f.reader.GetRun(context.Background(), chainRunID)
		if err != nil || len(run.GetResumes()) == 0 || run.GetResumes()[0].GetScope() != 3 {
			t.Fatalf("resumes %v, %v; want the earlier ones listed under scope 7", run.GetResumes(), err)
		}
	})
}

// A resumed run that carried on and published nothing since ended as its saved
// state left it, not as the ending its resume set aside.
func TestResumedRun_ThatCarriedOnAndEndedWithNoEvent(t *testing.T) {
	for name, closed := range map[string][]int{"once": {9}, "twice": {9, 2}} {
		t.Run(name, func(t *testing.T) {
			f := newChain(t, chainSpec{closed: closed, resumed: 7, resumedAfter: 6,
				skip:      map[int64]bool{3: true, 4: true, 5: true, 6: true},
				closeWith: closedEvent(9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, chainCreatedAt.Add(time.Hour))})
			earlier := &fakeEndings{events: setAside(6), run: &orkstackv1.Run{Id: chainRunID, Status: orkstackv1.Status_STATUS_FAILED,
				TaskRuns: []*orkstackv1.TaskRun{{TaskId: "another", Status: orkstackv1.Status_STATUS_FAILED}}}}
			f.reader = f.reader.WithEndings(earlier)
			want := len(chainState().GetTaskRuns())
			run, err := f.reader.GetRun(context.Background(), chainRunID)
			if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || len(run.GetTaskRuns()) != want {
				t.Fatalf("= %v, %v; want its saved state's %d tasks", run, err, want)
			}
			archived, got, err := f.reader.readForArchive(context.Background(), chainRunID, false)
			if err != nil || got.Scope != 7 || len(archived.GetTaskRuns()) != want {
				t.Fatalf("archived %v under %d, %v; want its saved state's %d tasks", archived, got.Scope, err, want)
			}
		})
	}
}

// A carry whose resume does not go together is refused before anything it
// names is read.
func TestValidateCarry_TheResumeGoesTogether(t *testing.T) {
	for name, change := range map[string]func(*orkstackv1.RunResumeCarry){
		"none of it":                   func(r *orkstackv1.RunResumeCarry) { r.Resume = nil },
		"no sequence":                  func(r *orkstackv1.RunResumeCarry) { r.SeqBase, r.Resume.Sequence, r.Resume.Scope = 0, 0, 0 },
		"past the run's events":        func(r *orkstackv1.RunResumeCarry) { r.SeqBase, r.Resume.Sequence, r.Resume.Scope = 10, 10, 10 },
		"past any sequence":            func(r *orkstackv1.RunResumeCarry) { r.SeqBase = 1 << 60 },
		"another sequence":             func(r *orkstackv1.RunResumeCarry) { r.Resume.Sequence = 8 },
		"another scope":                func(r *orkstackv1.RunResumeCarry) { r.Resume.Scope = 8 },
		"another ending":               func(r *orkstackv1.RunResumeCarry) { r.ResumedAfter = 5 },
		"an ending after it":           func(r *orkstackv1.RunResumeCarry) { r.ResumedAfter, r.Resume.ResumedAfterSequence = 7, 7 },
		"no ending":                    func(r *orkstackv1.RunResumeCarry) { r.ResumedAfter, r.Resume.ResumedAfterSequence = 0, 0 },
		"never resumed":                func(r *orkstackv1.RunResumeCarry) { r.ResumeCount = 0 },
		"resumed too often":            func(r *orkstackv1.RunResumeCarry) { r.ResumeCount = MaxResumesPerRun + 1 },
		"at no time":                   func(r *orkstackv1.RunResumeCarry) { r.Resume.At = nil },
		"one that ended while it runs": func(r *orkstackv1.RunResumeCarry) { r.Resume.Ended = &orkstackv1.RunResumeEnded{} },
	} {
		t.Run(name, func(t *testing.T) {
			f := newChain(t, chainSpec{closed: []int{9}, current: 1, resumed: 7, resumedAfter: 6})
			if err := validateCarry(chainRunID, chainNamespace, f.carry); err != nil {
				t.Fatalf("the carry itself: %v", err)
			}
			change(f.carry.Resume)
			if err := validateCarry(chainRunID, chainNamespace, f.carry); err == nil || !strings.Contains(err.Error(), "resume") {
				t.Fatalf("error = %v, want its resume refused", err)
			}
		})
	}
}

// freshState is a resumed run's state: build and fan and whole run again, with
// fan's item 0 kept and its item 1 settled; done ran; plain is not cached, and
// kept is kept.
func freshState() *runState {
	cached := func(task *orkstackv1.Task) *orkstackv1.Task {
		task.Cache = &orkstackv1.CacheSpec{}
		return task
	}
	fan, whole := cached(eachTask("fan", "build", "build")), cached(eachTask("whole", "build", "build"))
	fan.Each.MaxItems, whole.Each.MaxItems = 10, 10
	s := buildRunState("ork-r", &orkstackv1.Orkflow{Name: "wf", Tasks: []*orkstackv1.Task{
		cached(ptask("build")), fan, whole, cached(ptask("done")), ptask("plain"), cached(ptask("kept")),
	}}, nil, RunPlan{Namespace: "default"})
	for _, id := range []string{"build", "fan", "whole", "done", "plain"} {
		s.taskRuns[id].ResumeDisposition = dRerun
	}
	s.taskRuns["kept"].ResumeDisposition = dAdopt
	s.taskRuns["done"].Status = sOK
	for i, status := range []orkstackv1.Status{sOK, sOK, sPend, sPend, sPend, sPend} {
		item := pitem(int32(i), status)
		item.TaskId = "fan"
		item.ResumeDisposition = dRerun
		s.taskRuns["fan"].Items = append(s.taskRuns["fan"].Items, item)
	}
	s.taskRuns["fan"].Items[0].ResumeDisposition, s.taskRuns["fan"].Items[0].Adopted = dAdopt, true
	s.resumed = true
	return s
}

func ranges(firstCount ...int32) []*orkstackv1.ResumeItemRange {
	var out []*orkstackv1.ResumeItemRange
	for i := 0; i < len(firstCount); i += 2 {
		out = append(out, &orkstackv1.ResumeItemRange{First: firstCount[i], Count: firstCount[i+1]})
	}
	return out
}

func wholeFreshState() *orkstackv1.RunResumeState {
	return &orkstackv1.RunResumeState{Fresh: []*orkstackv1.ResumeFreshUnits{
		{Task: 0, All: true}, {Task: 1, Items: ranges(2, 1, 4, 2)}, {Task: 2, Items: ranges(0, 3, 9, 1)},
	}}
}

// What reads no memo crosses a switch, of what still runs: a task, some items
// of a fan-out, and ones it has no rows of yet.
func TestResumeState_RoundTrip(t *testing.T) {
	s := freshState()
	for _, u := range []ResumeUnit{nodeUnit("build"), nodeUnit("done"), {Task: "fan", Item: 1}, {Task: "fan", Item: 2}, {Task: "fan", Item: 4},
		{Task: "fan", Item: 5}, {Task: "whole", Item: 0}, {Task: "whole", Item: 1}, {Task: "whole", Item: 2}, {Task: "whole", Item: 9}} {
		s.readNoMemo(u)
	}
	state := s.resumeState()
	if !proto.Equal(state, wholeFreshState()) {
		t.Fatalf("state = %v, want %v", state, wholeFreshState())
	}
	next := freshState()
	if what := next.adoptResumeState(state); what != "" {
		t.Fatal(what)
	}
	if !proto.Equal(next.resumeState(), state) || !next.readsNoMemo("fan", 4) || next.readsNoMemo("fan", 3) || next.readsNoMemo("done", -1) {
		t.Fatalf("restored %v", next.fresh)
	}
	// Of a fan-out whose own unit is, every item.
	whole := freshState()
	whole.readNoMemo(nodeUnit("fan"))
	if state := whole.resumeState(); len(state.GetFresh()) != 1 || !state.GetFresh()[0].GetAll() || len(state.GetFresh()[0].GetItems()) != 0 {
		t.Fatalf("state = %v, want the whole fan-out", state)
	}
	if (&runState{}).resumeState() != nil {
		t.Fatal("a run never resumed carries a resume's state")
	}
}

// A saved state that names a unit the definition has not, one that ran, or one
// that is no rerun of a cached task is refused, never ignored.
func TestAdoptResumeState_RefusesWhatDoesNotRunAgain(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*runState)
		fresh  []*orkstackv1.ResumeFreshUnits
	}{
		"no unit":                     {fresh: nil},
		"a run never resumed":         {change: func(s *runState) { s.resumed = false }, fresh: wholeFreshState().Fresh},
		"a task there is not":         {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 6, All: true}}},
		"a task before the first":     {fresh: []*orkstackv1.ResumeFreshUnits{{Task: -1, All: true}}},
		"a task twice":                {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 0, All: true}, {Task: 0, All: true}}},
		"tasks out of order":          {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, All: true}, {Task: 0, All: true}}},
		"a task that ran":             {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 3, All: true}}},
		"a task that is not cached":   {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 4, All: true}}},
		"a kept task":                 {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 5, All: true}}},
		"neither a task nor its item": {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 0}}},
		"both":                        {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, All: true, Items: ranges(0, 1)}}},
		"items of no fan-out":         {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 0, Items: ranges(0, 1)}}},
		"a kept item":                 {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 1, Items: ranges(0, 1)}}},
		"an item that ran":            {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 1, Items: ranges(1, 2)}}},
		"an item there is not":        {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 1, Items: ranges(5, 2)}}},
		"an item past max_items":      {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(9, 2)}}},
		"a huge range":                {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(1, 1<<31-1)}}},
		"an empty range":              {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(1, 0)}}},
		"a negative item":             {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(-1, 2)}}},
		"ranges out of order":         {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(5, 1, 1, 1)}}},
		"ranges that overlap":         {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(1, 3, 2, 3)}}},
		"ranges that touch":           {fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(1, 2, 3, 2)}}},
	} {
		t.Run(name, func(t *testing.T) {
			s := freshState()
			if tc.change != nil {
				tc.change(s)
			}
			if what := s.adoptResumeState(&orkstackv1.RunResumeState{Fresh: tc.fresh}); what == "" {
				t.Fatalf("took %v", s.fresh)
			}
		})
	}
	if what := freshState().adoptResumeState(nil); what != "" {
		t.Fatalf("a saved state with none: %s", what)
	}
}

// FuzzAdoptResumeState: a saved state's resume is refused or is the one form a
// switch of the state it makes writes, whatever its bytes say, and nothing it
// names has run.
func FuzzAdoptResumeState(f *testing.F) {
	for _, state := range []*orkstackv1.RunResumeState{wholeFreshState(), {}, {Fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, All: true}}},
		{Fresh: []*orkstackv1.ResumeFreshUnits{{Task: 2, Items: ranges(0, 1<<31-1)}}}, {Fresh: []*orkstackv1.ResumeFreshUnits{{Task: 1, Items: ranges(0, 6)}}}} {
		raw, err := proto.Marshal(state)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
	}
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, raw []byte) {
		state := &orkstackv1.RunResumeState{}
		if proto.Unmarshal(raw, state) != nil || hasUnknownFields(state.ProtoReflect()) {
			return
		}
		s := freshState()
		if s.adoptResumeState(state) != "" {
			return
		}
		if again := s.resumeState(); !proto.Equal(again, state) {
			t.Fatalf("took %v, which a switch writes %v", state, again)
		}
		for u := range s.fresh {
			row := s.resumedRow(u)
			if u.Task != "build" && u.Task != "fan" && u.Task != "whole" || row != nil && (isTerminal(row.GetStatus()) || row.GetResumeDisposition() != dRerun) {
				t.Fatalf("%s reads no memo, and runs no more: %v", u, row)
			}
		}
	})
}

// A fan-out a resume kept items of is checked and taken up by its own rule:
// what runs again is not a prefix of its items.
func TestValidRestoredRow_AFanOutWithKeptItems(t *testing.T) {
	at := timestamppb.New(chainCreatedAt)
	row := func(expanded bool) (*runState, *orkstackv1.TaskRun) {
		s := freshState()
		r := s.taskRuns["fan"]
		r.Each = &orkstackv1.TaskEach{}
		if expanded {
			r.Status, r.Each = sRun, &orkstackv1.TaskEach{Expanded: true, Count: 6}
			r.Items[1].ScheduledAt = at
			return s, r
		}
		r.Items[1].Status = sPend
		return s, r
	}
	s, r := row(true)
	if what := s.validRestoredRow("fan", r); what != "" {
		t.Fatal(what)
	}
	e := s.eaches["fan"]
	e.count = 6
	restoreKeptFanOut(e, r)
	if e.next != 6 || e.settled != 2 || !slices.Equal(e.again, []int32{2, 3, 4, 5}) {
		t.Fatalf("next %d, settled %d, again %v; want all dispatched but 2 to 5, which run again", e.next, e.settled, e.again)
	}
	if s, r = row(false); true {
		r.Items = r.Items[1:]
		for i, item := range r.Items {
			item.Item = proto.Int32(int32(i))
		}
		if s.validRestoredRow("fan", r) == "" {
			t.Fatal("items of one not expanded, none of them kept, were taken")
		}
	}
	if s, r = row(false); s.validRestoredRow("fan", r) != "" || !s.keptItems("fan") {
		t.Fatalf("one not expanded yet: %q, kept items %v", s.validRestoredRow("fan", r), s.keptItems("fan"))
	}
	for name, change := range map[string]func(*runState, *orkstackv1.TaskRun){
		"a run never resumed":     func(s *runState, _ *orkstackv1.TaskRun) { s.resumed = false },
		"an item short":           func(_ *runState, r *orkstackv1.TaskRun) { r.Items = r.Items[:5] },
		"an item out of order":    func(_ *runState, r *orkstackv1.TaskRun) { r.Items[2].Item = proto.Int32(3) },
		"a running item":          func(_ *runState, r *orkstackv1.TaskRun) { r.Items[2].Status = sRun },
		"a kept item dispatched":  func(_ *runState, r *orkstackv1.TaskRun) { r.Items[0].ScheduledAt = at },
		"a kept item that failed": func(_ *runState, r *orkstackv1.TaskRun) { r.Items[0].Status = sFail },
		"one that ran undispatched": func(_ *runState, r *orkstackv1.TaskRun) {
			r.Items[1].ScheduledAt = nil
		},
		"one dispatched and pending": func(_ *runState, r *orkstackv1.TaskRun) { r.Items[2].ScheduledAt = at },
		"one of no disposition":      func(_ *runState, r *orkstackv1.TaskRun) { r.Items[2].ResumeDisposition = dNone },
	} {
		t.Run(name, func(t *testing.T) {
			s, r := row(true)
			change(s, r)
			if s.validRestoredRow("fan", r) == "" {
				t.Fatal("taken")
			}
		})
	}
	// An interruption put one back.
	s, r = row(true)
	r.Items[2].ScheduledAt, r.Items[2].Interruptions = at, &orkstackv1.TaskInterruptions{Last: &orkstackv1.TaskInterruption{}}
	if what := s.validRestoredRow("fan", r); what != "" {
		t.Fatalf("an item an interruption put back: %s", what)
	}
}

// A saved state holds the items of a fan-out a resume kept items of before it
// expands, and no part but the first holds a resume's state.
func TestCheckpointRun_AFanOutWithKeptItemsNotExpanded(t *testing.T) {
	s := freshState()
	s.taskRuns["fan"].Each = &orkstackv1.TaskEach{}
	run := &orkstackv1.Run{Id: "ork-r", TaskRuns: []*orkstackv1.TaskRun{s.taskRuns["build"], s.taskRuns["fan"]}}
	parts := checkpointParts(run, nil, wholeFreshState())
	got, err := CheckpointRun(parts...)
	if err != nil || len(got.GetTaskRuns()[1].GetItems()) != 6 || !proto.Equal(parts[0].GetResumeState(), wholeFreshState()) {
		t.Fatalf("= %v, %v", got, err)
	}
	if checkpointParts(run, nil, &orkstackv1.RunResumeState{})[0].ResumeState != nil {
		t.Fatal("a resume's state with nothing in it is written")
	}
	if _, err := CheckpointRun(parts[0], &orkstackv1.RunCheckpointPart{ResumeState: wholeFreshState()}); err == nil {
		t.Fatal("a later part's resume state was ignored")
	}
	run.TaskRuns[1].Items = run.TaskRuns[1].Items[1:]
	if _, err := CheckpointRun(checkpointParts(run, nil, nil)...); err == nil {
		t.Fatal("a fan-out short of its first item was taken")
	}
	run.TaskRuns[1].ResumeDisposition = dNone
	if _, err := CheckpointRun(checkpointParts(run, nil, nil)...); err == nil {
		t.Fatal("items of a fan-out no resume runs again were taken before it expanded")
	}
}
