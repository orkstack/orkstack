package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	dRerun = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN
	dAdopt = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT
	dSkip  = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED
	dWait  = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT
	dSet   = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE
	dSleep = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL
	dNone  = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED
)

// unitsOrkflow has one task of every kind, and a pin of two.
func unitsOrkflow() *orkstackv1.Orkflow {
	fan := eachTask("fan", "build", "build")
	fan.Each.MaxItems = 3
	fan.Cache = &orkstackv1.CacheSpec{}
	child := ptask("child", "build")
	child.Uses = "orkflow:other@1"
	p1, p2 := ptask("p1"), ptask("p2")
	p1.Pin, p2.Pin = "host", "host"
	return &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{
		ptask("build"), fan, awaitTask("approve"), sleepTask("nap"), child, p1, p2,
	}}
}

func unitsBlock() *orkstackv1.ResumeBlock {
	return &orkstackv1.ResumeBlock{SeqBase: 100, Scope: 100, Readback: &orkstackv1.ResumeReadback{Budget: 128 << 10}}
}

// unitsPlan treats every unit of unitsOrkflow, kept and rerun alike.
func unitsPlan() []ResumeUnitPlan {
	return []ResumeUnitPlan{
		{Task: "build", Item: -1, Disposition: dAdopt, Epoch: 40, EncodedSize: 700},
		{Task: "fan", Item: -1, Disposition: dRerun},
		{Task: "fan", Item: 0, Disposition: dAdopt, EncodedSize: 600},
		{Task: "fan", Item: 1, Disposition: dRerun, Fresh: true},
		{Task: "fan", Item: 2, Disposition: dAdopt, Epoch: 60, EncodedSize: 650},
		{Task: "approve", Item: -1, Disposition: dSet, Epoch: 60, EncodedSize: 300, Row: recordedAwait()},
		{Task: "nap", Item: -1, Disposition: dSleep, Row: recordedSleep()},
		{Task: "child", Item: -1, Disposition: dSet, EncodedSize: 310, Row: recordedChild()},
		{Task: "p1", Item: -1, Disposition: dRerun},
		{Task: "p2", Item: -1, Disposition: dRerun},
	}
}

func recordedTimes() *orkstackv1.TaskRun {
	at := timestamppb.New(time.Unix(1700000000, 0))
	return &orkstackv1.TaskRun{ScheduledAt: at, StartedAt: at, FinishedAt: timestamppb.New(time.Unix(1700000300, 0))}
}

func recordedAwait() *orkstackv1.TaskRun {
	r := recordedTimes()
	r.ValuesRef = &orkstackv1.ArtifactRef{Uri: "mem://v", Sha256: "ab", SizeBytes: 12}
	r.Await = &orkstackv1.TaskAwait{Deadline: r.StartedAt, ResolvedVia: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, ResolvedBy: "ana@example.com"}
	return r
}

func recordedSleep() *orkstackv1.TaskRun {
	r := recordedTimes()
	r.Sleep = &orkstackv1.TaskSleep{WakeAt: r.FinishedAt}
	return r
}

func recordedChild() *orkstackv1.TaskRun {
	r := recordedTimes()
	r.ValuesRef = &orkstackv1.ArtifactRef{Uri: "mem://c", Sha256: "cd", SizeBytes: 2}
	r.Artifacts = []*orkstackv1.ArtifactRef{{Name: "report", Uri: "mem://report"}}
	r.Child = &orkstackv1.TaskChild{RunId: "ork-child", InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "in"}}}
	return r
}

func sameUnits(a, b []ResumeUnitPlan) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.unit() != y.unit() || x.Disposition != y.Disposition || x.Epoch != y.Epoch || x.EncodedSize != y.EncodedSize || x.Fresh != y.Fresh ||
			!proto.Equal(x.Row, y.Row) {
			return false
		}
	}
	return true
}

func TestResumeUnits_RoundTrip(t *testing.T) {
	b := unitsBlock()
	if err := EncodeResumeUnits(unitsOrkflow(), b, unitsPlan(), false); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeResumeUnits(unitsOrkflow(), b, false)
	if err != nil {
		t.Fatal(err)
	}
	if !sameUnits(got, unitsPlan()) {
		t.Fatalf("decoded %+v, want %+v", got, unitsPlan())
	}
}

// What the planner gives no place in the plan never packs.
func TestEncodeResumeUnits_RefusesUnitsThatAreNotTheRuns(t *testing.T) {
	edit := func(f func([]ResumeUnitPlan) []ResumeUnitPlan) []ResumeUnitPlan { return f(unitsPlan()) }
	for name, units := range map[string][]ResumeUnitPlan{
		"a task left out":        edit(func(u []ResumeUnitPlan) []ResumeUnitPlan { return u[:len(u)-1] }),
		"the first task missing": edit(func(u []ResumeUnitPlan) []ResumeUnitPlan { return u[1:] }),
		"another task": edit(func(u []ResumeUnitPlan) []ResumeUnitPlan {
			return append(u, ResumeUnitPlan{Task: "zz", Item: -1, Disposition: dRerun})
		}),
		"an item missing":    edit(func(u []ResumeUnitPlan) []ResumeUnitPlan { return append(u[:3:3], u[4:]...) }),
		"items out of order": edit(func(u []ResumeUnitPlan) []ResumeUnitPlan { u[2].Item, u[3].Item = 1, 0; return u }),
		"an item of no fan-out": edit(func(u []ResumeUnitPlan) []ResumeUnitPlan {
			return append(u[:1:1], append([]ResumeUnitPlan{{Task: "build", Item: 0, Disposition: dRerun}}, u[1:]...)...)
		}),
		"more items than it has": edit(func(u []ResumeUnitPlan) []ResumeUnitPlan {
			return append(u[:5:5], append([]ResumeUnitPlan{{Task: "fan", Item: 3, Disposition: dRerun}}, u[5:]...)...)
		}),
		"no units": nil,
	} {
		t.Run(name, func(t *testing.T) {
			b := unitsBlock()
			if err := EncodeResumeUnits(unitsOrkflow(), b, units, false); err == nil || errors.Is(err, ErrResumeUnitsTooLarge) {
				t.Fatalf("error = %v, want a refusal of the units", err)
			}
			if b.Units != nil {
				t.Fatal("a refused plan left units in the block")
			}
		})
	}
}

// A disposition, scope, size or record its unit cannot have is refused, and
// so is a plan whose units do not go together.
func TestResumeUnits_RefusesWhatAUnitCannotHave(t *testing.T) {
	for name, tc := range map[string]struct {
		edit    func([]ResumeUnitPlan)
		block   func(*orkstackv1.ResumeBlock)
		want    string
		earlier bool
	}{
		"a container that waits":           {edit: func(u []ResumeUnitPlan) { u[0] = ResumeUnitPlan{Task: "build", Item: -1, Disposition: dWait} }, want: "cannot have"},
		"a container that settles":         {edit: func(u []ResumeUnitPlan) { u[0] = ResumeUnitPlan{Task: "build", Item: -1, Disposition: dSet} }, want: "cannot have"},
		"an await run again":               {edit: func(u []ResumeUnitPlan) { u[5] = ResumeUnitPlan{Task: "approve", Item: -1, Disposition: dRerun} }, want: "cannot have"},
		"an await adopted":                 {edit: func(u []ResumeUnitPlan) { u[5] = ResumeUnitPlan{Task: "approve", Item: -1, Disposition: dAdopt} }, want: "cannot have"},
		"a composed task run again":        {edit: func(u []ResumeUnitPlan) { u[7] = ResumeUnitPlan{Task: "child", Item: -1, Disposition: dRerun} }, want: "cannot have"},
		"a composed task that waits":       {edit: func(u []ResumeUnitPlan) { u[7].Disposition = dWait }, want: "cannot have"},
		"an earlier unit, never carried":   {edit: func(u []ResumeUnitPlan) { u[0] = ResumeUnitPlan{Task: "build", Item: -1, Disposition: dNone} }, want: "cannot have"},
		"a disposition of no kind":         {edit: func(u []ResumeUnitPlan) { u[0] = ResumeUnitPlan{Task: "build", Item: -1, Disposition: 99} }, want: "cannot have"},
		"a rerun with a scope":             {edit: func(u []ResumeUnitPlan) { u[1].Epoch = 40 }, want: "keeps no result"},
		"a scope not below the resume's":   {edit: func(u []ResumeUnitPlan) { u[0].Epoch = 100 }, want: "not below"},
		"no size":                          {edit: func(u []ResumeUnitPlan) { u[0].EncodedSize = 0 }, want: "0 bytes"},
		"a block with no read-back budget": {block: func(b *orkstackv1.ResumeBlock) { b.Readback = nil }, edit: func([]ResumeUnitPlan) {}, want: "over its call"},
		"a pin half kept, of a run that carried on": {edit: func(u []ResumeUnitPlan) {
			u[9] = ResumeUnitPlan{Task: "p2", Item: -1, Disposition: dAdopt, EncodedSize: 5}
		}, want: "pin", earlier: true},
		"a size over its call":        {edit: func(u []ResumeUnitPlan) { u[0].EncodedSize = 129 << 10 }, want: "over its call"},
		"a sleep with no wake-up":     {edit: func(u []ResumeUnitPlan) { u[6].Row.Sleep.WakeAt = nil }, want: "missing or no time"},
		"a sleep with no record":      {edit: func(u []ResumeUnitPlan) { u[6].Row = nil }, want: "no row"},
		"a settle with a wake-up":     {edit: func(u []ResumeUnitPlan) { u[7].Row.Sleep = &orkstackv1.TaskSleep{} }, want: "more than a record"},
		"a wake-up of no time":        {edit: func(u []ResumeUnitPlan) { u[6].Row.Sleep.WakeAt = &timestamppb.Timestamp{Nanos: -1} }, want: "missing or no time"},
		"an await nobody answered":    {edit: func(u []ResumeUnitPlan) { u[5].Row.Await.ResolvedVia = 0 }, want: "what resolved it"},
		"an await with a status":      {edit: func(u []ResumeUnitPlan) { u[5].Row.Status = sOK }, want: "more than a record"},
		"an await with an output":     {edit: func(u []ResumeUnitPlan) { u[5].Row.Output = &orkstackv1.Payload{} }, want: "more than a record"},
		"an await with a child":       {edit: func(u []ResumeUnitPlan) { u[5].Row.Child = &orkstackv1.TaskChild{RunId: "x"} }, want: "more than a record"},
		"an await with no values":     {edit: func(u []ResumeUnitPlan) { u[5].Row.ValuesRef, u[5].EncodedSize = nil, 0 }, want: "no values"},
		"an await with no end":        {edit: func(u []ResumeUnitPlan) { u[5].Row.FinishedAt = nil }, want: "missing or no time"},
		"a composed task of no run":   {edit: func(u []ResumeUnitPlan) { u[7].Row.Child.RunId = "" }, want: "no child run"},
		"a settled task of no size":   {edit: func(u []ResumeUnitPlan) { u[7].EncodedSize = 0 }, want: "0 bytes"},
		"a kept node of a rerun item": {edit: func(u []ResumeUnitPlan) { u[1] = ResumeUnitPlan{Task: "fan", Item: -1, Disposition: dAdopt} }, want: "runs again"},
		"a pin half kept": {edit: func(u []ResumeUnitPlan) {
			u[9] = ResumeUnitPlan{Task: "p2", Item: -1, Disposition: dAdopt, EncodedSize: 5}
		}, want: "pin"},
		"a pin half skipped":                        {edit: func(u []ResumeUnitPlan) { u[9] = ResumeUnitPlan{Task: "p2", Item: -1, Disposition: dSkip} }, want: "pin"},
		"no memo read of a task that is not cached": {edit: func(u []ResumeUnitPlan) { u[8].Fresh = true }, want: "read no memo"},
		"no memo read of a kept item":               {edit: func(u []ResumeUnitPlan) { u[2].Fresh = true }, want: "read no memo"},
		"nothing runs again": {want: "nothing again", edit: func(u []ResumeUnitPlan) {
			for i := range u {
				if u[i].Disposition == dRerun {
					u[i] = ResumeUnitPlan{Task: u[i].Task, Item: u[i].Item, Disposition: dSkip}
				}
			}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			units := unitsPlan()
			tc.edit(units)
			b := unitsBlock()
			if tc.block != nil {
				tc.block(b)
			}
			err := EncodeResumeUnits(unitsOrkflow(), b, units, tc.earlier)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
	units := unitsPlan()
	units[0] = ResumeUnitPlan{Task: "build", Item: -1, Disposition: dNone}
	b := unitsBlock()
	if err := EncodeResumeUnits(unitsOrkflow(), b, units, true); err != nil {
		t.Fatalf("a unit of an earlier part of a run that carried on = %v, want it taken", err)
	}
	if got, err := DecodeResumeUnits(unitsOrkflow(), b, true); err != nil || !sameUnits(got, units) {
		t.Fatalf("decoded %+v (%v), want %+v", got, err, units)
	}
	if _, err := DecodeResumeUnits(unitsOrkflow(), b, false); err == nil {
		t.Fatal("a run that never carried on took a unit of an earlier part")
	}
	// A pin carries on only unopened, so a member that had ended was skipped,
	// and the others run again without it.
	units = unitsPlan()
	units[8] = ResumeUnitPlan{Task: "p1", Item: -1, Disposition: dNone}
	if err := EncodeResumeUnits(unitsOrkflow(), unitsBlock(), units, true); err != nil {
		t.Fatalf("a pin whose skipped member stays as it was saved = %v, want it taken", err)
	}
}

// A kept fan-out's own unit has no scope, whatever its row says; its items
// keep theirs. Two fan-outs stand between plain tasks.
func TestResumeUnits_KeptFanOutsBetweenTasks(t *testing.T) {
	a, z := eachTask("a", "", ""), eachTask("z", "", "")
	a.Each.MaxItems, z.Each.MaxItems = 2, 5
	a.Each.Source, z.Each.Source = &orkstackv1.EachSource{Input: "xs"}, &orkstackv1.EachSource{Input: "xs"}
	wf := &orkstackv1.Orkflow{Tasks: []*orkstackv1.Task{ptask("first"), a, ptask("mid"), z, ptask("last")}}
	units := []ResumeUnitPlan{
		{Task: "first", Item: -1, Disposition: dAdopt, EncodedSize: 1},
		{Task: "a", Item: -1, Disposition: dAdopt, Epoch: 40},
		{Task: "a", Item: 0, Disposition: dAdopt, Epoch: 40, EncodedSize: 2},
		{Task: "a", Item: 1, Disposition: dAdopt, EncodedSize: 3},
		{Task: "mid", Item: -1, Disposition: dSkip},
		{Task: "z", Item: -1, Disposition: dRerun}, // not expanded
		{Task: "last", Item: -1, Disposition: dRerun},
	}
	b := unitsBlock()
	if err := EncodeResumeUnits(wf, b, units, false); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeResumeUnits(wf, b, false)
	want := append([]ResumeUnitPlan(nil), units...)
	want[1].Epoch = 0
	if err != nil || !sameUnits(got, want) {
		t.Fatalf("decoded %+v (%v), want %+v", got, err, want)
	}
}

// Bytes another writer could leave are refused, each malformed or second
// form of a plan included.
func TestDecodeResumeUnits_RefusesMalformedColumns(t *testing.T) {
	wf := unitsOrkflow()
	packed := func() *orkstackv1.ResumeUnits {
		u, err := packResumeUnits(wf, unitsPlan())
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	for name, edit := range map[string]func(*orkstackv1.ResumeUnits){
		"a column short":                           func(u *orkstackv1.ResumeUnits) { u.RunEpochs = u.RunEpochs[1:] },
		"dispositions long":                        func(u *orkstackv1.ResumeUnits) { u.RunDispositions = append(u.RunDispositions, dRerun) },
		"an empty run":                             func(u *orkstackv1.ResumeUnits) { u.RunLengths[0] = 0 },
		"a negative run":                           func(u *orkstackv1.ResumeUnits) { u.RunLengths[0] = -1 },
		"too many units":                           func(u *orkstackv1.ResumeUnits) { u.RunLengths[len(u.RunLengths)-1]++ },
		"a huge run":                               func(u *orkstackv1.ResumeUnits) { u.RunLengths[len(u.RunLengths)-1] = 1<<31 - 1 },
		"too few units":                            func(u *orkstackv1.ResumeUnits) { u.RunLengths[len(u.RunLengths)-1]-- },
		"a run split in two":                       func(u *orkstackv1.ResumeUnits) { splitLastRun(u) },
		"an epoch out of range":                    func(u *orkstackv1.ResumeUnits) { u.RunEpochs[0] = 9 },
		"a negative epoch":                         func(u *orkstackv1.ResumeUnits) { u.RunEpochs[0] = -1 },
		"epochs not ascending":                     func(u *orkstackv1.ResumeUnits) { u.Epochs[0], u.Epochs[1] = u.Epochs[1], u.Epochs[0] },
		"an epoch of 0":                            func(u *orkstackv1.ResumeUnits) { u.Epochs[0] = 0 },
		"an epoch twice":                           func(u *orkstackv1.ResumeUnits) { u.Epochs[0] = u.Epochs[1] },
		"an epoch no unit has":                     func(u *orkstackv1.ResumeUnits) { u.Epochs = append(u.Epochs, 70) },
		"a size too many":                          func(u *orkstackv1.ResumeUnits) { u.EncodedSizes = append(u.EncodedSizes, 1) },
		"a size too few":                           func(u *orkstackv1.ResumeUnits) { u.EncodedSizes = u.EncodedSizes[1:] },
		"a negative size":                          func(u *orkstackv1.ResumeUnits) { u.EncodedSizes[0] = -1 },
		"a record too many":                        func(u *orkstackv1.ResumeUnits) { u.Recorded = append(u.Recorded, &orkstackv1.ResumeRecordedUnit{}) },
		"a record too few":                         func(u *orkstackv1.ResumeUnits) { u.Recorded = u.Recorded[1:] },
		"a fan-out twice":                          func(u *orkstackv1.ResumeUnits) { u.FanOuts = append(u.FanOuts, u.FanOuts[0]) },
		"a fan-out of no task":                     func(u *orkstackv1.ResumeUnits) { u.FanOuts[0].Task = 99 },
		"a fan-out before the first":               func(u *orkstackv1.ResumeUnits) { u.FanOuts[0].Task = -1 },
		"items of a container":                     func(u *orkstackv1.ResumeUnits) { u.FanOuts[0].Task = 0 },
		"no items":                                 func(u *orkstackv1.ResumeUnits) { u.FanOuts[0].ItemCount = 0 },
		"items over max_items":                     func(u *orkstackv1.ResumeUnits) { u.FanOuts[0].ItemCount = 4 },
		"a disposition this release does not know": func(u *orkstackv1.ResumeUnits) { u.RunDispositions[0] = 99 },
		"a negative disposition":                   func(u *orkstackv1.ResumeUnits) { u.RunDispositions[0] = -1 },
		"lengths that wrap to the right sum":       func(u *orkstackv1.ResumeUnits) { wrapLengths(u) },
		"a fresh run too few":                      func(u *orkstackv1.ResumeUnits) { u.RunFresh = u.RunFresh[1:] },
		"a fresh run too many":                     func(u *orkstackv1.ResumeUnits) { u.RunFresh = append(u.RunFresh, false) },
		"no run fresh, spelled out":                func(u *orkstackv1.ResumeUnits) { clear(u.RunFresh) },
		"a fresh kept task":                        func(u *orkstackv1.ResumeUnits) { u.RunFresh[0] = true },
		"a fresh rerun of no cached task":          func(u *orkstackv1.ResumeUnits) { u.RunFresh[len(u.RunFresh)-1] = true },
		"an unknown field":                         func(u *orkstackv1.ResumeUnits) { u.ProtoReflect().SetUnknown([]byte{0x7a, 0x00}) },
		"an unknown field below":                   func(u *orkstackv1.ResumeUnits) { u.Recorded[0].ProtoReflect().SetUnknown([]byte{0x7a, 0x00}) },
	} {
		t.Run(name, func(t *testing.T) {
			u := packed()
			edit(u)
			raw, err := proto.Marshal(u)
			if err != nil {
				t.Fatal(err)
			}
			b := unitsBlock()
			b.Units = raw
			if got, err := DecodeResumeUnits(wf, b, false); err == nil {
				t.Fatalf("decoded %+v, want a refusal", got)
			}
		})
	}
	for name, raw := range map[string][]byte{"no bytes": nil, "not a message": {0xff, 0xff}, "over its bound": make([]byte, maxResumeUnitsBytes+1)} {
		b := unitsBlock()
		b.Units = raw
		if _, err := DecodeResumeUnits(wf, b, false); err == nil {
			t.Errorf("%s: decoded, want a refusal", name)
		}
	}
}

// wrapLengths makes the lengths' sum right only if 2^32 is nothing.
func wrapLengths(u *orkstackv1.ResumeUnits) {
	u.RunLengths[0] = 1<<31 - 1
	u.RunLengths[1] = 1<<31 - 1
	u.RunLengths[2] += 3 - u.RunLengths[1] + u.RunLengths[0]
}

// splitLastRun writes the last run, of two units, as two runs of one.
func splitLastRun(u *orkstackv1.ResumeUnits) {
	n := len(u.RunLengths) - 1
	u.RunLengths[n]--
	u.RunLengths = append(u.RunLengths, 1)
	u.RunDispositions = append(u.RunDispositions, u.RunDispositions[n])
	u.RunEpochs = append(u.RunEpochs, u.RunEpochs[n])
	u.RunFresh = append(u.RunFresh, u.RunFresh[n])
}

// The largest plans fit: the block its bound, and the record and the
// LoadResumePlan answer, which holds the whole record, theirs. Task ids of
// 200 bytes play no part.
func TestResumeUnits_TheLargestPlansFit(t *testing.T) {
	const tasks, items = 4000, 10000
	id := func(i int) string { return fmt.Sprintf("t%04d%s", i, strings.Repeat("x", 195)) }
	for name, tc := range map[string]struct {
		items int
		unit  func(i int) ResumeUnitPlan
	}{
		"4,000 kept tasks": {unit: func(int) ResumeUnitPlan {
			return ResumeUnitPlan{Disposition: dAdopt, EncodedSize: 20000}
		}},
		"4,000 tasks, kept and run again in turn": {unit: func(i int) ResumeUnitPlan {
			if i%2 == 0 {
				return ResumeUnitPlan{Disposition: dRerun}
			}
			return ResumeUnitPlan{Disposition: dAdopt, EncodedSize: 20000}
		}},
		"4,000 tasks and 10,000 items, run again with a memo and without in turn": {items: items, unit: func(i int) ResumeUnitPlan {
			return ResumeUnitPlan{Disposition: dRerun, Fresh: i%2 == 0}
		}},
		"4,000 tasks and 10,000 items, each unlike the next, 20 resumes before": {items: items, unit: func(i int) ResumeUnitPlan {
			return ResumeUnitPlan{Disposition: dAdopt, EncodedSize: 20000, Epoch: 1<<52 + uint64(i%20)}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			wf := &orkstackv1.Orkflow{}
			var units []ResumeUnitPlan
			for i := range tasks {
				u := tc.unit(len(units))
				u.Task, u.Item = id(i), -1
				def := ptask(id(i))
				def.Cache = &orkstackv1.CacheSpec{}
				if i == tasks-1 {
					u = ResumeUnitPlan{Task: id(i), Item: -1, Disposition: dRerun, Fresh: true}
					if tc.items > 0 {
						def.Each = &orkstackv1.EachSpec{MaxItems: items, Source: &orkstackv1.EachSource{Input: "xs"}}
					}
				}
				wf.Tasks = append(wf.Tasks, def)
				units = append(units, u)
			}
			for i := range tc.items {
				u := tc.unit(len(units))
				u.Task, u.Item = id(tasks-1), i
				units = append(units, u)
			}
			b := unitsBlock()
			b.SeqBase, b.Scope = maxResumeScope, maxResumeScope
			b.Readback.Deadline = durationpb.New(time.Minute)
			if err := EncodeResumeUnits(wf, b, units, false); err != nil {
				t.Fatal(err)
			}
			record := &orkstackv1.ResumePlanRecord{Format: artifacts.ResumePlanFormat, Namespace: "team-a", RunId: "ork-01jx",
				Scope: maxResumeScope, Block: b, Principal: strings.Repeat("p", 256), Reason: strings.Repeat("r", 1024)}
			stored, err := artifacts.MarshalResumePlan(record)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := task.PayloadSize(record)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("units %d bytes, block %d, stored record %d, payload %d", len(b.Units), proto.Size(b), len(stored), payload)
			if len(stored) > 256<<10 || payload > task.MaxActivityInputBytes {
				t.Fatalf("record %d bytes, payload %d, over 256 KiB", len(stored), payload)
			}
		})
	}
}

func TestEncodeResumeUnits_OverItsBound(t *testing.T) {
	wf, units := &orkstackv1.Orkflow{}, []ResumeUnitPlan{}
	for i := range 1500 {
		id := fmt.Sprintf("a%d", i)
		wf.Tasks = append(wf.Tasks, awaitTask(id))
		units = append(units, ResumeUnitPlan{Task: id, Item: -1, Disposition: dSet,
			EncodedSize: 500, Row: recordedAwait()})
	}
	units[0] = ResumeUnitPlan{Task: "a0", Item: -1, Disposition: dWait}
	if err := EncodeResumeUnits(wf, unitsBlock(), units, false); !errors.Is(err, ErrResumeUnitsTooLarge) {
		t.Fatalf("error = %v, want ErrResumeUnitsTooLarge", err)
	}
}

// What decodes is one form: packed again, it is the same plan and reads back
// as it.
func FuzzDecodeResumeUnits(f *testing.F) {
	wf := unitsOrkflow()
	seed := func(u *orkstackv1.ResumeUnits) {
		if raw, err := proto.Marshal(u); err == nil {
			f.Add(raw)
		}
	}
	good, err := packResumeUnits(wf, unitsPlan())
	if err != nil {
		f.Fatal(err)
	}
	seed(good)
	split := proto.Clone(good).(*orkstackv1.ResumeUnits)
	splitLastRun(split)
	seed(split)
	seed(&orkstackv1.ResumeUnits{RunLengths: []int32{1<<31 - 1}, RunDispositions: []orkstackv1.ResumeDisposition{dRerun}, RunEpochs: []int32{0}})
	for _, edit := range []func(*orkstackv1.ResumeUnits){
		func(u *orkstackv1.ResumeUnits) { u.RunLengths[0] = 1<<31 - 1 },
		func(u *orkstackv1.ResumeUnits) { u.RunLengths[0] = -1 << 31 },
		func(u *orkstackv1.ResumeUnits) { u.RunDispositions[0] = 99 },
		func(u *orkstackv1.ResumeUnits) { u.FanOuts[0].ItemCount = 1<<31 - 1 },
		func(u *orkstackv1.ResumeUnits) { u.RunEpochs[0] = 2 },
		func(u *orkstackv1.ResumeUnits) { u.Epochs[0] = 1 },
		func(u *orkstackv1.ResumeUnits) { u.EncodedSizes[0] = 1 << 40 },
		func(u *orkstackv1.ResumeUnits) { u.Recorded = u.Recorded[1:] },
		func(u *orkstackv1.ResumeUnits) { u.RunFresh = nil },
		func(u *orkstackv1.ResumeUnits) { u.RunFresh[0] = true },
		func(u *orkstackv1.ResumeUnits) { u.RunFresh = u.RunFresh[:1] },
		func(u *orkstackv1.ResumeUnits) { clear(u.RunFresh) },
	} {
		hostile := proto.Clone(good).(*orkstackv1.ResumeUnits)
		edit(hostile)
		seed(hostile)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff})
	f.Fuzz(func(t *testing.T, raw []byte) {
		b := unitsBlock()
		b.Units = raw
		units, err := DecodeResumeUnits(wf, b, false)
		if err != nil {
			return
		}
		if most := len(wf.GetTasks()) + 3; len(units) > most {
			t.Fatalf("decoded %d units of a run of at most %d", len(units), most)
		}
		for _, u := range units {
			if _, known := orkstackv1.ResumeDisposition_name[int32(u.Disposition)]; !known || u.Disposition == dNone {
				t.Fatalf("decoded disposition %v of %s", u.Disposition, u.unit())
			}
			if u.Fresh && (u.Task != "fan" || u.Disposition != dRerun) {
				t.Fatalf("decoded %s, %v, as reading no memo", u.unit(), u.Disposition)
			}
		}
		again := unitsBlock()
		if err := EncodeResumeUnits(wf, again, units, false); err != nil {
			t.Fatalf("a decoded plan does not pack: %v", err)
		}
		back, err := DecodeResumeUnits(wf, again, false)
		if err != nil || !sameUnits(units, back) {
			t.Fatalf("packed again it reads %+v (%v), want %+v", back, err, units)
		}
	})
}
