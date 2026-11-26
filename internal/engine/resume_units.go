package engine

import (
	"errors"
	"fmt"
	"slices"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// A resume's units travel packed (ResumeBlock.units, orkstack.v1.ResumeUnits):
// a plan names every task and item of the run, and by name a run of a few
// thousand tasks outgrows its own record and the reset run's history.
// EncodeResumeUnits is the one writer, DecodeResumeUnits the one reader, and
// a plan the reader does not take whole is never carried out.

// maxResumeUnitsBytes bounds the packed units' binary encoding, before JSON
// makes base64 (a third more) of it in the stored record and the payload:
// 4,000 tasks and 10,000 items,
// each treated unlike its neighbour, with 20 resumes behind them, take 84 KB
// (TestResumeUnits_TheLargestPlansFit).
const maxResumeUnitsBytes = 96 << 10

// ErrResumeUnitsTooLarge: a plan's units, packed, are over their bound.
// ResumeRun refuses such a plan RESUME_TOO_LARGE.
var ErrResumeUnitsTooLarge = errors.New("the resume's units are too large")

// ResumeUnitPlan is what a resume does with one unit.
type ResumeUnitPlan struct {
	// Task is the unit's task, and Item its item's index, or -1 for the task
	// itself.
	Task string
	Item int
	// Disposition is UNSPECIFIED for a unit of an earlier part of the run,
	// which stays as it was checkpointed.
	Disposition orkstackv1.ResumeDisposition
	// Epoch is the scope a kept result lies under; 0 is the first run's.
	Epoch uint64
	// EncodedSize is a read-back entry's encoded size.
	EncodedSize int64
	// Fresh is set for a cached task's unit that runs again and reads no memo.
	Fresh bool
	// Row is a SETTLE or SLEEP_UNTIL unit's row as the base ended, as
	// cutRecordedRow cuts it.
	Row *orkstackv1.TaskRun
}

func (u ResumeUnitPlan) unit() ResumeUnit { return ResumeUnit{Task: u.Task, Item: u.Item} }

// readBackUnit says whether u's result is read back: a kept container's, an
// item's included, and a settled task's values. A fan-out's node has no result
// of its own.
func readBackUnit(t *orkstackv1.Task, u ResumeUnitPlan) bool {
	switch u.Disposition {
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT:
		return u.Item >= 0 || t.GetEach() == nil
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE:
		return u.Row.GetValuesRef().GetUri() != ""
	}
	return false
}

// keepsRecord says whether d keeps its unit from a record of its own.
func keepsRecord(d orkstackv1.ResumeDisposition) bool {
	return d == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE ||
		d == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL
}

// EncodeResumeUnits packs units into b, which already holds its seq_base and
// its read-back bounds. units are every unit of wf, in unit order: each task
// in definition order, then its items from 0. What it packs is read back
// before it is kept, so it never writes what the reset run would refuse.
// earlier says the base carried on, so units of an earlier part may be left
// as checkpointed.
func EncodeResumeUnits(wf *orkstackv1.Orkflow, b *orkstackv1.ResumeBlock, units []ResumeUnitPlan, earlier bool) error {
	packed, err := packResumeUnits(wf, units)
	if err != nil {
		return err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(packed)
	if err != nil {
		return fmt.Errorf("engine: encode the resume's units: %w", err)
	}
	if len(raw) > maxResumeUnitsBytes {
		return fmt.Errorf("%w: %d bytes, over %d", ErrResumeUnitsTooLarge, len(raw), maxResumeUnitsBytes)
	}
	probe := proto.Clone(b).(*orkstackv1.ResumeBlock)
	probe.Units = raw
	if _, err := DecodeResumeUnits(wf, probe, earlier); err != nil {
		return fmt.Errorf("engine: the resume's units do not read back: %w", err)
	}
	b.Units = raw
	return nil
}

func packResumeUnits(wf *orkstackv1.Orkflow, units []ResumeUnitPlan) (*orkstackv1.ResumeUnits, error) {
	out := &orkstackv1.ResumeUnits{}
	units = slices.Clone(units)
	fans := map[string]bool{}
	for _, t := range wf.GetTasks() {
		fans[t.GetId()] = t.GetEach() != nil
	}
	for i, u := range units {
		if u.Item < 0 && fans[u.Task] && u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT {
			// A kept fan-out's own unit has no result: its items have theirs.
			u.Epoch = 0
			units[i].Epoch = 0
		}
		if u.Epoch > 0 {
			out.Epochs = append(out.Epochs, u.Epoch)
		}
	}
	slices.Sort(out.Epochs)
	out.Epochs = slices.Compact(out.Epochs)
	at := 0
	for i, t := range wf.GetTasks() {
		if at >= len(units) || units[at].Task != t.GetId() || units[at].Item != -1 {
			return nil, fmt.Errorf("engine: encode the resume's units: unit %d is not task %q", at, t.GetId())
		}
		items := 0
		for first := true; first || at < len(units) && units[at].Task == t.GetId(); at++ {
			u := units[at]
			if !first {
				if u.Item != items {
					return nil, fmt.Errorf("engine: encode the resume's units: fan-out %q has item %d where %d belongs", u.Task, u.Item, items)
				}
				items++
			}
			first = false
			addResumeUnit(out, t, u)
		}
		if items > 0 {
			out.FanOuts = append(out.FanOuts, &orkstackv1.ResumeFanOutItems{Task: int32(i), ItemCount: int32(items)})
		}
	}
	if at != len(units) {
		return nil, fmt.Errorf("engine: encode the resume's units: unit %d is %s, which the definition has no place for", at, units[at].unit())
	}
	if !slices.Contains(out.RunFresh, true) {
		out.RunFresh = nil
	}
	return out, nil
}

// addResumeUnit appends u to out's columns, widening the last run when u is
// treated alike.
func addResumeUnit(out *orkstackv1.ResumeUnits, t *orkstackv1.Task, u ResumeUnitPlan) {
	epoch := int32(0)
	if u.Epoch > 0 {
		at, _ := slices.BinarySearch(out.Epochs, u.Epoch)
		epoch = int32(at + 1)
	}
	if n := len(out.RunLengths); n > 0 && out.RunDispositions[n-1] == u.Disposition && out.RunEpochs[n-1] == epoch && out.RunFresh[n-1] == u.Fresh {
		out.RunLengths[n-1]++
	} else {
		out.RunLengths = append(out.RunLengths, 1)
		out.RunDispositions = append(out.RunDispositions, u.Disposition)
		out.RunEpochs = append(out.RunEpochs, epoch)
		out.RunFresh = append(out.RunFresh, u.Fresh)
	}
	if readBackUnit(t, u) {
		out.EncodedSizes = append(out.EncodedSizes, u.EncodedSize)
	}
	if keepsRecord(u.Disposition) {
		out.Recorded = append(out.Recorded, &orkstackv1.ResumeRecordedUnit{Row: u.Row})
	}
}

// DecodeResumeUnits reads b's packed units as every unit of wf, in unit
// order, and refuses anything but the one form EncodeResumeUnits writes: a
// unit missing, named twice or of another definition, a disposition its kind
// of unit cannot have, and a bound it breaks. Its error says what is wrong;
// the caller ends the resume RESUME_PLAN_INVALID with it.
func DecodeResumeUnits(wf *orkstackv1.Orkflow, b *orkstackv1.ResumeBlock, earlier bool) ([]ResumeUnitPlan, error) {
	raw := b.GetUnits()
	if len(raw) == 0 {
		return nil, errors.New("its block holds no units")
	}
	if len(raw) > maxResumeUnitsBytes {
		return nil, fmt.Errorf("its block's units are %d bytes, over %d", len(raw), maxResumeUnitsBytes)
	}
	packed := &orkstackv1.ResumeUnits{}
	if err := proto.Unmarshal(raw, packed); err != nil {
		return nil, fmt.Errorf("its block's units do not decode: %w", err)
	}
	if hasUnknownFields(packed.ProtoReflect()) {
		return nil, errors.New("its block's units hold a field this release does not know")
	}
	d := &resumeUnitsDecoder{wf: wf, b: b, in: packed, earlier: earlier}
	return d.decode()
}

type resumeUnitsDecoder struct {
	wf      *orkstackv1.Orkflow
	b       *orkstackv1.ResumeBlock
	in      *orkstackv1.ResumeUnits
	earlier bool

	run, left       int // the run being read, and how many of its units are left
	sizes, recorded int // how many of each have been taken
	usedEpochs      map[int32]bool
}

func (d *resumeUnitsDecoder) decode() ([]ResumeUnitPlan, error) {
	items, err := d.fanOuts()
	if err != nil {
		return nil, err
	}
	if err := d.epochs(); err != nil {
		return nil, err
	}
	if err := d.runs(); err != nil {
		return nil, err
	}
	total := len(d.wf.GetTasks())
	for _, n := range items {
		total += n
	}
	// Before anything is made of a run's length, all of them add up to the
	// units the definition has: a length no run can have allocates nothing.
	sum := int64(0)
	for _, n := range d.in.GetRunLengths() {
		sum += int64(n)
	}
	if sum != int64(total) {
		return nil, fmt.Errorf("its block names %d units, and the run has %d", sum, total)
	}
	out := make([]ResumeUnitPlan, 0, total)
	for i, t := range d.wf.GetTasks() {
		for item := -1; item < items[i]; item++ {
			u, err := d.next(t, item)
			if err != nil {
				return nil, err
			}
			out = append(out, u)
		}
	}
	switch {
	case d.sizes != len(d.in.GetEncodedSizes()):
		return nil, fmt.Errorf("its block gives %d sizes for %d results read back", len(d.in.GetEncodedSizes()), d.sizes)
	case d.recorded != len(d.in.GetRecorded()):
		return nil, fmt.Errorf("its block gives %d records for %d units kept from one", len(d.in.GetRecorded()), d.recorded)
	case len(d.usedEpochs) != len(d.in.GetEpochs()):
		return nil, errors.New("its block lists a scope no unit lies under")
	}
	return out, d.together(out)
}

// fanOuts are the item counts, by task index.
func (d *resumeUnitsDecoder) fanOuts() (map[int]int, error) {
	items := map[int]int{}
	last := -1
	for _, f := range d.in.GetFanOuts() {
		i := int(f.GetTask())
		if i <= last || i >= len(d.wf.GetTasks()) {
			return nil, fmt.Errorf("its block names task %d as a fan-out out of order, or of no task", i)
		}
		last = i
		t := d.wf.GetTasks()[i]
		if t.GetEach() == nil {
			return nil, fmt.Errorf("its block gives items to %q, which is no fan-out", t.GetId())
		}
		if n := f.GetItemCount(); n < 1 || n > t.GetEach().GetMaxItems() {
			return nil, fmt.Errorf("its block gives %q %d items, and it has at most %d", t.GetId(), n, t.GetEach().GetMaxItems())
		}
		items[i] = int(f.GetItemCount())
	}
	return items, nil
}

func (d *resumeUnitsDecoder) epochs() error {
	d.usedEpochs = map[int32]bool{}
	last := uint64(0)
	for _, e := range d.in.GetEpochs() {
		if e <= last || e >= d.b.GetSeqBase() {
			return fmt.Errorf("its block lists scope %d out of order, or not below this resume's %d", e, d.b.GetSeqBase())
		}
		last = e
	}
	return nil
}

func (d *resumeUnitsDecoder) runs() error {
	n := len(d.in.GetRunLengths())
	if len(d.in.GetRunDispositions()) != n || len(d.in.GetRunEpochs()) != n {
		return errors.New("its block's units are not one run each")
	}
	if fresh := d.in.GetRunFresh(); len(fresh) > 0 && (len(fresh) != n || !slices.Contains(fresh, true)) {
		return errors.New("its block's fresh runs are not its runs, or none is")
	}
	for i := range n {
		epoch := d.in.GetRunEpochs()[i]
		switch {
		case d.in.GetRunLengths()[i] < 1:
			return fmt.Errorf("its block's run %d has no unit", i)
		case epoch < 0 || int(epoch) > len(d.in.GetEpochs()):
			return fmt.Errorf("its block's run %d lies under scope %d, which it does not list", i, epoch)
		case i > 0 && d.in.GetRunDispositions()[i-1] == d.in.GetRunDispositions()[i] && d.in.GetRunEpochs()[i-1] == epoch &&
			d.fresh(i-1) == d.fresh(i):
			return fmt.Errorf("its block's runs %d and %d are one", i-1, i)
		}
	}
	return nil
}

// fresh is run i's.
func (d *resumeUnitsDecoder) fresh(i int) bool {
	return i < len(d.in.GetRunFresh()) && d.in.GetRunFresh()[i]
}

// next reads the next unit, which is item of t.
func (d *resumeUnitsDecoder) next(t *orkstackv1.Task, item int) (ResumeUnitPlan, error) {
	u := ResumeUnitPlan{Task: t.GetId(), Item: item}
	if d.left == 0 {
		d.left = int(d.in.GetRunLengths()[d.run])
		d.run++
	}
	d.left--
	u.Disposition = d.in.GetRunDispositions()[d.run-1]
	if epoch := d.in.GetRunEpochs()[d.run-1]; epoch > 0 {
		d.usedEpochs[epoch] = true
		u.Epoch = d.in.GetEpochs()[epoch-1]
	}
	if !d.allowed(t, u) {
		return u, fmt.Errorf("its block gives %s the disposition %v, which it cannot have", u.unit(), u.Disposition)
	}
	if u.Fresh = d.fresh(d.run - 1); u.Fresh && (t.GetCache() == nil || u.Disposition != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN) {
		return u, fmt.Errorf("its block has %s read no memo, and it is not a cached task's that runs again", u.unit())
	}
	if keepsRecord(u.Disposition) {
		if d.recorded >= len(d.in.GetRecorded()) {
			return u, fmt.Errorf("its block holds no record of %s", u.unit())
		}
		u.Row = d.in.GetRecorded()[d.recorded].GetRow()
		d.recorded++
		if why := recordedRowFault((&planTask{def: t}).kind(), u.Row); why != "" {
			return u, fmt.Errorf("its block's record of %s is not one of its disposition: %s", u.unit(), why)
		}
	}
	reads := readBackUnit(t, u)
	if u.Epoch > 0 && !reads && u.Disposition != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE {
		return u, fmt.Errorf("its block gives %s a scope, and it keeps no result", u.unit())
	}
	if reads {
		if d.sizes >= len(d.in.GetEncodedSizes()) {
			return u, fmt.Errorf("its block gives no size of %s", u.unit())
		}
		u.EncodedSize = d.in.GetEncodedSizes()[d.sizes]
		d.sizes++
		if budget := d.b.GetReadback().GetBudget(); u.EncodedSize < 1 || u.EncodedSize > budget {
			return u, fmt.Errorf("its block reads %s back in %d bytes, over its call's %d", u.unit(), u.EncodedSize, budget)
		}
	}
	return u, nil
}

// allowed is whether a unit of t's kind can have u's disposition.
func (d *resumeUnitsDecoder) allowed(t *orkstackv1.Task, u ResumeUnitPlan) bool {
	switch u.Disposition {
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED:
		return d.earlier
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED:
		return true
	}
	kind := (&planTask{def: t}).unitKind(u.unit())
	switch u.Disposition {
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN, orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT:
		return kind == kindContainer || kind == kindFanOut
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE:
		return kind == kindAwait || kind == kindComposed
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT:
		return kind == kindAwait || kind == kindSleep
	case orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL:
		return kind == kindSleep
	}
	return false
}

// together checks what no unit says alone: a fan-out with an item that runs
// again runs again itself, a pin runs again whole or not at all, and
// something runs or waits again.
func (d *resumeUnitsDecoder) together(units []ResumeUnitPlan) error {
	again := func(u ResumeUnitPlan) bool {
		return u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN
	}
	pins := map[string]string{}
	for _, t := range d.wf.GetTasks() {
		pins[t.GetId()] = t.GetPin()
	}
	nodeAgain, pinAgain, pinKept, moves := false, map[string]bool{}, map[string]string{}, false
	for _, u := range units {
		moves = moves || again(u) || u.Disposition == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT
		if u.Item < 0 {
			nodeAgain = again(u)
		} else if again(u) && !nodeAgain {
			return fmt.Errorf("its block keeps fan-out %q, whose item %d runs again", u.Task, u.Item)
		}
		if pin := pins[u.Task]; pin != "" {
			switch {
			case again(u):
				pinAgain[pin] = true
			case u.Disposition != orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED:
				// One left as it was saved opened nothing (checkRows).
				pinKept[pin] = u.Task
			}
		}
	}
	// A pin with any member run again runs again whole (§4.3): a kept member
	// beside a rerun one would leave the rerun an empty /orkstack/shared.
	for _, t := range d.wf.GetTasks() {
		if pin := t.GetPin(); pinAgain[pin] && pinKept[pin] != "" {
			return fmt.Errorf("its block keeps %q of pin %q, whose other members run again", pinKept[pin], pin)
		}
	}
	if !moves {
		return errors.New("its block runs nothing again")
	}
	return nil
}

// hasUnknownFields is whether m, or any message it holds, has a field this
// build does not know.
func hasUnknownFields(m protoreflect.Message) bool {
	if len(m.GetUnknown()) > 0 {
		return true
	}
	found := false
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.Kind() != protoreflect.MessageKind || f.IsMap() {
			return true
		}
		if f.IsList() {
			for i := range v.List().Len() {
				found = found || hasUnknownFields(v.List().Get(i).Message())
			}
		} else {
			found = hasUnknownFields(v.Message())
		}
		return !found
	})
	return found
}
