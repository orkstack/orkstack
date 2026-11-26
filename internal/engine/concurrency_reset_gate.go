package engine

import (
	"errors"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// The reset's half of the gate workflow (E88 §5.1.11): the freeze a reset
// reads the state under, the start that takes it over, and the records the
// new gate writes of it.

// GateResetChange marks the first thing a gate does for a reset: the freeze it
// answers, or the start that begins it. Neither is in a history recorded
// before resets, so no history holds its DefaultVersion branch.
const GateResetChange = "concurrency-gate-reset"

// gateResetsMemoKey is the gate execution's memo of its key's resets
// (gateResetMemo).
const gateResetsMemoKey = "orkflow_gate_resets"

// GateErrResetRefused and GateErrResetKnown are a freeze's refusals: a request
// the gate does not take (its retryable form is GateErrBusy), and one it has
// applied.
const (
	GateErrResetRefused = "CONCURRENCY_RESET_REFUSED"
	GateErrResetKnown   = "CONCURRENCY_RESET_KNOWN"
)

// GateResetStart is what a reset starts a gate with: its request, the state it
// read, and until when the gate it read it from was frozen, zero when it was
// not (no gate, or one that does not answer).
type GateResetStart struct {
	Request     gateReset `json:"request"`
	Seeds       gateSeeds `json:"seeds"`
	FrozenUntil time.Time `json:"frozen_until,omitzero"`
	// StartBy is by when a start made without a freeze, of a key that had no
	// gate or one that was ended, had to reach the cluster: a later one is a
	// stray copy, which other resets may have overtaken.
	StartBy time.Time `json:"start_by,omitzero"`
	// Forced: the gate it replaced did not answer.
	Forced bool `json:"forced,omitempty"`
}

// GateFreeze asks a gate to freeze for Request.
type GateFreeze struct {
	Request gateReset `json:"request"`
}

// GateFrozen is the state of a frozen gate, and how long it stays so.
type GateFrozen struct {
	Until time.Time `json:"until"`
	State gateState `json:"state"`
}

// freeze is the Update handler: it answers the state it froze, in the same
// workflow task, so nothing is granted between them.
func (w *gateRun) freeze(ctx workflow.Context, f GateFreeze) (GateFrozen, error) {
	workflow.GetVersion(ctx, GateResetChange, workflow.DefaultVersion, 1)
	if err := gateResetError(w.g.freeze(f.Request, workflow.Now(ctx))); err != nil {
		return GateFrozen{}, err
	}
	// A reset that finds this gate not answering reads here that another
	// holds it, and does not end it under that one.
	if err := w.rememberResets(ctx); err != nil {
		return GateFrozen{}, err
	}
	w.poke()
	return GateFrozen{Until: w.g.FrozenUntil, State: *w.g}, nil
}

// gateResetError is a freeze's refusal as the error the reset tells apart.
func gateResetError(err error) error {
	var known *resetKnownError
	var refused *resetRefusal
	switch {
	case errors.As(err, &known):
		return temporal.NewNonRetryableApplicationError(known.Error(), GateErrResetKnown, nil)
	case errors.As(err, &refused) && refused.Retryable:
		return temporal.NewApplicationError(refused.Reason, GateErrBusy)
	case errors.As(err, &refused):
		return temporal.NewNonRetryableApplicationError(refused.Reason, GateErrResetRefused, nil)
	}
	return err
}

// beginReset takes over the state a reset read and records that it did. It
// reports whether the cluster started this gate late, once the freeze that
// state was read under had ended, or past the time a start without one had:
// another reset may then have been applied in between, so what is remembered
// is not known to be all, and no reset is taken until any such one is past its
// window. The state itself is kept: the records the rebuild reads add to it,
// and a pass checks each entry.
func (w *gateRun) beginReset(ctx workflow.Context, r GateResetStart) (bool, error) {
	workflow.GetVersion(ctx, GateResetChange, workflow.DefaultVersion, 1)
	now, started := workflow.Now(ctx), workflow.GetInfo(ctx).WorkflowStartTime
	w.g.seed(r.Seeds, r.Request, now)
	w.g.ResetAt = started
	if !r.FrozenUntil.IsZero() || !r.StartBy.IsZero() {
		// Another replica that asked the same may repeat the start for as
		// long: under the freeze of the gate this one replaced, which it
		// made before that gate ended, or until its own StartBy.
		w.g.ResetHeldUntil = started.Add(w.g.Tunables.FreezeTimeout)
	}
	if err := w.g.checkOrdinals(); err != nil {
		return false, temporal.NewNonRetryableApplicationError(err.Error(), "CONCURRENCY_GATE_CARRY_INVALID", nil)
	}
	lapsed := !r.FrozenUntil.IsZero() && started.After(r.FrozenUntil) || !r.StartBy.IsZero() && started.After(r.StartBy)
	if lapsed {
		w.g.refuseResets(now)
		workflow.GetLogger(ctx).Warn("a concurrency gate was reset later than the state it was started from could be relied on; it takes no further reset until its window has passed",
			"gate", w.in.GateID, "reset", r.Request.ID, "frozen_until", r.FrozenUntil, "start_by", r.StartBy, "started", started)
	}
	w.recordReset(ctx, ResetRecordApplied)
	return lapsed, nil
}

// finishReset records the end of the reset this gate began with, once its
// record is rebuilt.
func (w *gateRun) finishReset(ctx workflow.Context) error {
	if !w.g.resetDone() {
		return nil
	}
	w.recordReset(ctx, ResetRecordCompleted)
	return w.rememberResets(ctx)
}

// rememberResets writes what the gate remembers into its memo, where the start
// that follows it reads it (CR-1).
func (w *gateRun) rememberResets(ctx workflow.Context) error {
	return workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{gateResetsMemoKey: w.g.resetMemo()}))
}

// recordReset writes one record of the reset this gate began with, if it did:
// a report, which decides nothing. One that cannot be written within
// rebuild_timeout is logged and left.
func (w *gateRun) recordReset(ctx workflow.Context, kind string) {
	if w.g.ResetID == "" {
		return
	}
	rec := ConcurrencyResetRecord{Kind: kind, Namespace: w.in.Namespace, GateID: w.in.GateID, Key: w.in.Key, RequestID: w.g.ResetID,
		StartedAt: w.g.ResetAt, Epoch: w.g.Epoch, Holders: len(w.g.Holders), Queued: len(w.g.Queue), Forced: w.in.Reset != nil && w.in.Reset.Forced}
	// One at a time, in the order they were made, so a reset is never
	// recorded completed before it is recorded applied.
	w.records = append(w.records, rec)
	if len(w.records) > 1 {
		return
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		defer w.poke()
		for len(w.records) > 0 {
			next := w.records[0]
			lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
				StartToCloseTimeout:    w.g.Tunables.RecordTimeout,
				ScheduleToCloseTimeout: w.g.Tunables.RebuildTimeout,
				RetryPolicy:            &temporal.RetryPolicy{InitialInterval: w.g.Tunables.RebuildBackoffMin, MaximumInterval: w.g.Tunables.RebuildBackoffMax},
			})
			if err := workflow.ExecuteLocalActivity(lctx, ActivityRecordConcurrencyReset, next).Get(lctx, nil); err != nil {
				workflow.GetLogger(ctx).Error("a concurrency reset's record could not be written; the reset itself is unaffected",
					"gate", w.in.GateID, "reset", next.RequestID, "record", next.Kind, "error", err)
			}
			w.records = w.records[1:]
		}
	})
}

// resetMemoOf is what an execution's memo says of its key's resets: nil for
// nothing, Unreadable for what this release cannot read.
func resetMemoOf(memo *commonpb.Memo) *gateResetMemo {
	payload, ok := memo.GetFields()[gateResetsMemoKey]
	if !ok {
		return nil
	}
	var m gateResetMemo
	if len(payload.GetData()) == 0 || DataConverter().FromPayload(payload, &m) != nil || !m.valid() {
		return &gateResetMemo{Unreadable: true}
	}
	if m.empty() {
		return nil
	}
	return &m
}

// valid reports that every request m remembers is one a gate could have
// applied.
func (m *gateResetMemo) valid() bool {
	for _, r := range m.Resets {
		if r.ID == "" || r.Body == "" || r.IssuedAt.IsZero() {
			return false
		}
	}
	return true
}

// inherited is m as the next gate takes it: the requests a gate of window
// still remembers at now by any clock, and nothing of which one it began with.
func (m *gateResetMemo) inherited(window time.Duration, now time.Time) *gateResetMemo {
	if m == nil {
		return nil
	}
	next := *m
	next.Current, next.FrozenUntil = "", time.Time{}
	next.Resets = slices.DeleteFunc(slices.Clone(m.Resets), func(r gateReset) bool {
		return r.IssuedAt.Add(window + resetClockAhead).Before(now)
	})
	if next.UnknownUntil.Add(resetClockAhead).Before(now) {
		next.UnknownUntil = time.Time{}
	}
	if next.empty() {
		return nil
	}
	return &next
}
