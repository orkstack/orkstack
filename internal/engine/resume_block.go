package engine

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// ResumeBlockInput is what a resume's block is built from.
type ResumeBlockInput struct {
	Input        ResumePlanInput
	Plan         *ResumePlan
	Verification ResumeVerification
	// SeqBase is RUN_RESUMED's sequence, Scope the first seq_base of the
	// resume's scope and PreviousSeqBase the seq_base before it (0: none).
	SeqBase, Scope, PreviousSeqBase uint64
}

// BuildResumeBlock is the plan as the reset run carries it out. It returns a
// *ResumeRefusal, RESUME_TOO_LARGE, for a plan no reset run could read back
// or hold.
func BuildResumeBlock(in ResumeBlockInput) (*orkstackv1.ResumeBlock, error) {
	b := &orkstackv1.ResumeBlock{
		SeqBase: in.SeqBase, Scope: in.Scope, PreviousSeqBase: in.PreviousSeqBase,
		FromTaskId: in.Input.FromTask, ResumesBefore: int32(in.Input.ResumesUsed),
		Action:   orkstackv1.ResumeAction_RESUME_ACTION_RESUME,
		Readback: readbackOf(defaultReadbackTunables()),
	}
	if in.Input.Action == ResumeActionRetryFrom {
		b.Action = orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM
	}
	p, err := newPlanner(withoutEvidence(in.Input))
	if err != nil {
		return nil, err
	}
	units := make([]ResumeUnitPlan, 0, len(in.Plan.Units))
	for _, u := range in.Plan.Units {
		t := p.tasks[u.Unit.Task]
		if t == nil {
			return nil, fmt.Errorf("engine: resume block: the plan names task %q, which the definition does not declare", u.Unit.Task)
		}
		row := t.unitRow(u.Unit)
		out := ResumeUnitPlan{Task: u.Unit.Task, Item: u.Unit.Item, Disposition: u.Disposition.Proto(), Fresh: u.Fresh}
		switch u.Disposition {
		case DispositionAdopt:
			out.Epoch = uint64(row.GetResultEpoch())
			if t.unitKind(u.Unit) != kindContainer {
				break // a fan-out's node, whose items are read back
			}
			size, measured := in.Verification.Sizes[u.Unit]
			if !measured {
				return nil, fmt.Errorf("engine: resume block: %s is kept and was not measured", u.Unit)
			}
			if size > MaxReadbackEntryBytes() {
				return nil, resultTooLarge(u.Unit)
			}
			out.EncodedSize = size
		case DispositionSettle:
			if answered := in.Input.Evidence.Answered[u.Unit.Task]; answered != nil && (in.Input.Evidence.Recorded || !keptCandidate(row)) {
				// The answer's row as its record gives it: for a recorded run
				// always, since its state is no word on it, and an answer the
				// run never applied otherwise (design §28.1, §28.9).
				row = answered
			}
			out.Row, out.Epoch = cutRecordedRow(t.kind(), row), uint64(row.GetResultEpoch())
			if why := recordedRowFault(t.kind(), out.Row); why != "" {
				// An answer or a child's end the base's row does not hold: the
				// evidence of one is not read by this build (E85 PR5b).
				return nil, fmt.Errorf("engine: resume block: %s is settled and its row does not say how: %s", u.Unit, why)
			}
			size, measured := in.Verification.Sizes[u.Unit]
			if !measured {
				return nil, fmt.Errorf("engine: resume block: %s is settled and was not measured", u.Unit)
			}
			if size > MaxReadbackEntryBytes() {
				return nil, resultTooLarge(u.Unit)
			}
			out.EncodedSize = size
		case DispositionSleepUntil:
			out.Row = cutRecordedRow(t.kind(), row)
		case DispositionWait:
			if t.unitKind(u.Unit) == kindAwait {
				b.ReopenedAwaits = append(b.ReopenedAwaits, u.Unit.Task)
			}
		}
		units = append(units, out)
	}
	err = EncodeResumeUnits(in.Input.Definition, b, units, in.Input.Facts.Continued)
	if errors.Is(err, ErrResumeUnitsTooLarge) {
		return nil, refuse(RefuseTooLarge, RefusalPermanent,
			"too many tasks and items (%d) would have to be tracked; retry from a later task, or Run again", len(units))
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ResumeRecordTasks is what a resume's record names (artifacts.ResumeRecord):
// the tasks whose rows the reset run marks to run again, and the awaits it
// opens again, each in definition order. Identity reads a task's results at
// the newest resume whose record names it, and the run writes them under its
// row's, so the two are the same tasks: exactly the rows the reset run gives
// its own scope (applyDispositions).
func ResumeRecordTasks(wf *orkstackv1.Orkflow, plan *ResumePlan) (rerun, reopened []string) {
	awaits := map[string]bool{}
	for _, t := range wf.GetTasks() {
		awaits[t.GetId()] = t.GetAwait() != nil
	}
	rerun, reopened = []string{}, []string{}
	for _, u := range plan.Units {
		switch {
		case u.Unit.Item >= 0: // its fan-out's own unit runs again with it
		case u.Disposition == DispositionRerun:
			rerun = append(rerun, u.Unit.Task)
		case u.Disposition == DispositionWait && awaits[u.Unit.Task]:
			reopened = append(reopened, u.Unit.Task)
		}
	}
	return rerun, reopened
}

// resultTooLarge refuses to keep u, whose saved result no call can read back.
func resultTooLarge(u ResumeUnit) *ResumeRefusal {
	r := refuse(RefuseTooLarge, RefusalPermanent,
		"task %s's saved result is too large to read back; retry from a later task, or Run again", u)
	r.Tasks = []string{u.Task}
	return r
}

var wireDispositions = map[ResumeDisposition]orkstackv1.ResumeDisposition{
	DispositionRerun:       orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN,
	DispositionAdopt:       orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT,
	DispositionSettle:      orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SETTLE,
	DispositionKeepSkipped: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_KEEP_SKIPPED,
	DispositionWait:        orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT,
	DispositionSleepUntil:  orkstackv1.ResumeDisposition_RESUME_DISPOSITION_SLEEP_UNTIL,
}

func withoutEvidence(in ResumePlanInput) ResumePlanInput {
	in.Evidence = ResumeEvidence{}
	return in
}

// readbackOf pins t in a block.
func readbackOf(t readbackTunables) *orkstackv1.ResumeReadback {
	return &orkstackv1.ResumeReadback{
		Deadline: durationpb.New(t.Deadline), Budget: t.Budget, MaxEntries: int32(t.MaxEntries), Concurrency: int32(t.Concurrency),
		GetTimeout: durationpb.New(t.GetTimeout), GetTries: int32(t.GetTries), CallTimeout: durationpb.New(t.CallTimeout),
		RetryInitial: durationpb.New(t.RetryInitial), RetryCoefficient: t.RetryCoefficient, CallsPerTask: int32(t.CallsPerTask),
	}
}

// CheckItemEpochs refuses a plan that leaves a task to run again with more
// ranges of item scopes to be told, summed over every fan-out it needs, than
// its input may carry (MaxItemEpochRuns).
func CheckItemEpochs(in ResumePlanInput, plan *ResumePlan, seqBase uint64) error {
	p, err := newPlanner(withoutEvidence(in))
	if err != nil {
		return err
	}
	inS := map[ResumeUnit]bool{}
	for _, u := range plan.Units {
		inS[u.Unit] = u.InRerunSet
	}
	ranges := map[string]int{}
	for _, id := range p.order {
		t := p.tasks[id]
		if len(t.items) == 0 {
			continue
		}
		items := make([]*orkstackv1.TaskRun, t.items[len(t.items)-1]+1)
		for _, i := range t.items {
			u := ResumeUnit{Task: id, Item: i}
			epoch := t.unitRow(u).GetResultEpoch()
			if inS[u] {
				epoch = int64(seqBase)
			}
			items[i] = &orkstackv1.TaskRun{ResultEpoch: epoch}
		}
		ranges[id] = len(itemEpochRuns(items))
	}
	for _, id := range p.order {
		if !inS[nodeUnit(id)] {
			continue
		}
		total := 0
		for _, need := range p.tasks[id].def.GetNeeds() {
			total += ranges[need]
		}
		if total > MaxItemEpochRuns {
			r := refuse(RefuseTooLarge, RefusalPermanent,
				"too many recorded results (%d ranges of items) would have to be tracked for task %s; retry from a later task, or Run again", total, id)
			r.Tasks = []string{id}
			return r
		}
	}
	return nil
}

const (
	// resumeEventCost bounds one engine event's publish in history, with its
	// workflow task: scheduled, started and completed of each.
	resumeEventCost = 6
	// readbackCallEvents bounds one read-back call: its marker, and a
	// workflow task of its own.
	readbackCallEvents = 4
	// slotRecordEvents is one write of a slot's memo and attributes, and
	// reassertEvents one tick of a wait in line (E88 §5.2.4): its timer, its
	// workflow task and the acquire's marker.
	slotRecordEvents = 2
	reassertEvents   = 6
	// heartbeatEvents is one workflow task's heartbeat (spike T13).
	heartbeatEvents = 3
	// defaultWorkflowTaskTimeout is assumed of a history that records none.
	defaultWorkflowTaskTimeout = 10 * time.Second
	// readbackReadTime is what one read of a kept result takes, for the
	// estimate a preview shows; it decides nothing.
	readbackReadTime = 100 * time.Millisecond
)

// ResumePreflight is what carrying a plan out costs its history.
type ResumePreflight struct {
	ReadbackCalls     int
	EstimatedReadback time.Duration
	// Events and Bytes are the history once the run goes on, with what its
	// own failure would still append.
	Events, Bytes int64
	// Headroom is the smaller share of the line left, in events or bytes;
	// 1 for a run with no limit.
	Headroom float64
}

// PreflightResume refuses a resume whose run could not go on, or could not
// fail explicitly, within its history (§3.3): it evaluates the engine's own
// failure reserve on the planned state. Kept results are read back before the
// run goes on and add nothing to it. in's limit is its plan's; a run with none
// is never refused.
func PreflightResume(in ResumePlanInput, run RunPlan, h *ResumeHistory, plan *ResumePlan, verified ResumeVerification) (ResumePreflight, error) {
	tun := defaultReadbackTunables()
	var sizes []int64
	var total int64
	for _, u := range plan.Units {
		if size := verified.Sizes[u.Unit]; (u.Disposition == DispositionAdopt || u.Disposition == DispositionSettle) && size > 0 {
			sizes = append(sizes, size)
			total += size
		}
	}
	calls, err := packReadback(sizes, tun)
	if err != nil {
		return ResumePreflight{}, fmt.Errorf("engine: preflight a resume: %w", err)
	}
	out := ResumePreflight{ReadbackCalls: len(calls), Headroom: 1}
	if len(sizes) > 0 {
		out.EstimatedReadback = max(time.Second, time.Duration((len(sizes)+tun.Concurrency-1)/tun.Concurrency)*readbackReadTime)
	}
	limit := run.History
	if limit == nil || limit.MaxEvents <= 0 || limit.MaxBytes <= 0 {
		return out, nil
	}
	wft := h.WorkflowTaskTimeout
	if wft <= 0 {
		wft = defaultWorkflowTaskTimeout
	}
	// A call blocks its workflow task, which heartbeats at 80% of its timeout.
	beats := int64((tun.Deadline+wft*4/5-1)/(wft*4/5)) * heartbeatEvents
	out.Events = h.ResetPoint.CompletedEventID + 2*resumeEventCost + int64(len(calls))*readbackCallEvents + beats
	copied, reads := restoreCost(run, h.Carry, wft)
	out.Events += reads
	out.Bytes = h.PrefixBytes + total + copied + (out.Events-h.ResetPoint.CompletedEventID)*limit.Reserve.BytesPerEvent
	slotEvents, slotBytes := resumeSlotCost(run.Concurrency)
	out.Events += slotEvents
	out.Bytes += slotEvents*limit.Reserve.BytesPerEvent + slotBytes
	reserveEvents, reserveBytes, err := plannedReserve(in, run, plan)
	if err != nil {
		return ResumePreflight{}, err
	}
	out.Events += reserveEvents
	out.Bytes += reserveEvents*limit.Reserve.BytesPerEvent + reserveBytes
	percent := int64(100)
	if c := run.Continuation; c != nil && c.HoldPercent > 0 {
		percent = min(percent, c.HoldPercent)
	}
	maxEvents, maxBytes := limit.MaxEvents*percent/100, limit.MaxBytes*percent/100
	out.Headroom = min(1-float64(out.Events)/float64(maxEvents), 1-float64(out.Bytes)/float64(maxBytes))
	if out.Events >= maxEvents || out.Bytes >= maxBytes {
		return out, refuse(RefuseTooLarge, RefusalPermanent,
			"reading back %d results wouldn't fit this run's history; retry from a later task, or Run again", len(sizes))
	}
	return out, nil
}

// restoreCost is what a resume of a run that carried on copies into its history
// before it goes on, after its reset point: its saved state, and a definition
// passed by reference, in bytes, and in events a marker a read, a yield and a
// workflow task a batch, and the heartbeats of the time it may take.
func restoreCost(run RunPlan, carry *orkstackv1.RunContinuation, wft time.Duration) (bytes, events int64) {
	c := run.Continuation
	if carry == nil || c == nil || c.ChunkBytes <= 0 || c.ChunksPerTask <= 0 {
		return 0, 0
	}
	reads := int64(0)
	for _, size := range []int64{carry.GetCheckpoint().GetSizeBytes(), run.definitionBytes()} {
		bytes += size
		reads += (size + c.ChunkBytes - 1) / c.ChunkBytes
	}
	beats := int64((bootstrapTimeout(*c)+wft*4/5-1)/(wft*4/5)) * heartbeatEvents
	// A yield is a timer's start and its firing, beside its workflow task.
	return bytes, reads + (reads/int64(c.ChunksPerTask)+1)*(heartbeatEvents+2) + beats
}

// resumeSlotCost bounds what a resumed run's concurrency slot appends (E88
// §18 item 10): the commands that set its earlier incarnations aside, and the
// longest wait in line, at the re-assert interval that grows to its maximum.
// A run that waits in line never carries on into a later history.
func resumeSlotCost(p *ConcurrencyPlan) (events, bytes int64) {
	slot := p.runSlot()
	if slot == nil {
		return 0, 0
	}
	// One signal and its answer a gate, the records, and the workflow task
	// that commits them; then the acquire, RUN_QUEUED, the two records of the
	// wait and of the grant, and the deadline.
	events = int64(2*len(p.Slots)) + 5*slotRecordEvents + heartbeatEvents + resumeEventCost
	bytes = int64(len(p.Slots)+2) * p.PerSlotBytes
	if slot.Strategy != gateStrategyQueue {
		return events, bytes // refused at once, never in line
	}
	wait, interval := time.Duration(slot.QueueTimeoutSeconds)*time.Second, p.RequeueInterval
	if interval <= 0 {
		return events, bytes
	}
	at, ticks := interval, int64(0)
	for ; at < wait && interval < p.RequeueIntervalMax && p.RequeueBackoffFactor > 1; at += interval {
		ticks++
		interval = min(time.Duration(p.RequeueBackoffFactor)*interval, p.RequeueIntervalMax)
	}
	if at < wait {
		ticks += int64((wait-at)/interval) + 1
	}
	return events + ticks*reassertEvents, bytes
}

// plannedReserve is failReserve of the run as it goes on: its kept results
// read back, what reruns or waits pending, and nothing in flight.
func plannedReserve(in ResumePlanInput, run RunPlan, plan *ResumePlan) (events, bytes int64, err error) {
	p, err := newPlanner(withoutEvidence(in))
	if err != nil {
		return 0, 0, err
	}
	s := &runState{
		plan: run, wf: in.Definition, order: p.order,
		tasks: map[string]*orkstackv1.Task{}, taskRuns: map[string]*orkstackv1.TaskRun{},
	}
	kept := map[string]bool{}
	for _, u := range plan.Units {
		if u.Unit.Item >= 0 {
			continue
		}
		switch u.Disposition {
		case DispositionRerun:
		case DispositionWait:
			// Its wait, and then its cancel.
			events += run.History.Reserve.PerAwait
		default:
			kept[u.Unit.Task] = true
		}
	}
	for _, id := range p.order {
		t := p.tasks[id]
		s.tasks[id] = t.def
		status := orkstackv1.Status_STATUS_PENDING
		if kept[id] {
			status = orkstackv1.Status_STATUS_SUCCEEDED
		}
		s.taskRuns[id] = &orkstackv1.TaskRun{TaskId: id, Status: status}
	}
	e, by := s.failReserve(nil, run.History.Reserve)
	return events + e, by, nil
}
