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
		out := ResumeUnitPlan{Task: u.Unit.Task, Item: u.Unit.Item, Disposition: u.Disposition.Proto()}
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
				r := refuse(RefuseTooLarge, RefusalPermanent,
					"task %s's saved result is too large to read back; retry from a later task, or Run again", u.Unit)
				r.Tasks = []string{u.Unit.Task}
				return nil, r
			}
			out.EncodedSize = size
		case DispositionSettle:
			out.Values, out.Epoch = row.GetValuesRef(), uint64(row.GetResultEpoch())
		case DispositionSleepUntil:
			out.WakeAt = row.GetSleep().GetWakeAt()
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
		if size := verified.Sizes[u.Unit]; u.Disposition == DispositionAdopt && size > 0 {
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
	out.Bytes = h.PrefixBytes + total + (out.Events-h.ResetPoint.CompletedEventID)*limit.Reserve.BytesPerEvent
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
		case DispositionWait, DispositionSleepUntil:
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
