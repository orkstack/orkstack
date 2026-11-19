package engine

import (
	"fmt"
	"math"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// Interruptions (E92 §4.7–4.8, AGENTS.md §4): an attempt ended by an
// infrastructure fact of the run's pinned interruption class runs again
// without spending the task's own attempts, within budgets the run's plan
// pinned at submit. This file is the class's whole arithmetic, pure: it
// reads a failure the workflow already holds (categoryOf) and the unit's
// recorded state, and it issues no command and reads no clock — the caller
// passes the workflow's own time. The interpreter owns the commands (the
// re-dispatch, the Timer of a wait), behind the interruption-retry gate.

const (
	// interruptionTypePrefix prefixes the error type of a typed
	// interruption, as the Orker emits it (internal/orker/emission.go):
	// OrkInterruption:ORKER_STOPPED.
	interruptionTypePrefix = "OrkInterruption:"
	// heartbeatTimeoutType listed asks the cluster's heartbeat timeout to end
	// the execution, so a lost attempt is the engine's to classify.
	heartbeatTimeoutType = "TemporalTimeout:Heartbeat"

	orkerStoppedReason              = "ORKER_STOPPED"
	containerStoppedByRuntimeReason = "CONTAINER_STOPPED_BY_RUNTIME"
	attemptStoreUnavailableReason   = "ATTEMPT_STORE_UNAVAILABLE"
	runtimeStateUnknownReason       = "RUNTIME_STATE_UNKNOWN"
	profileDriftedReason            = "PROFILE_DRIFTED"
	// The contract faults (E91 contracts): the Orker found the profile no
	// longer matches the contract the run pinned, or serves none of the
	// contracts the run may run under. Listed only for a unit naming a
	// profile under a run that pinned contracts (interruptionTypes).
	profileChangedReason           = "PROFILE_CHANGED"
	profileContractNotServedReason = "PROFILE_CONTRACT_NOT_SERVED"
	orkerHeartbeatLostReason       = "ORKER_HEARTBEAT_LOST"

	// reportedBeatPhase is the phase of the beat an Orker sends once it has
	// reported an attempt (internal/orker PhaseReported), frozen here with
	// the decoder that reads it.
	reportedBeatPhase = "reported"

	// What closed a class: TaskInterruption.closed_by.
	closedByLost         = "lost"
	closedByLostRunCap   = "lost_run_cap"
	closedByHandedBack   = "handed_back"
	closedByNeverStarted = "never_started"

	// maxInterruptionBackoff and maxInterruptionBackoffExponent bound the
	// never-started back-off. AGENTS.md §4 fixes the 60 s ceiling, as the
	// 256 KiB payload bound is fixed, and the plan pins only the initial
	// wait; the exponent never decides a wait, since configuration refuses a
	// first wait below 1 s and 1 s × 2^6 is already past the ceiling.
	maxInterruptionBackoff         = 60 * time.Second
	maxInterruptionBackoffExponent = 6
)

// neverStartedFaultReasons are the terminating faults before a start that
// every unit lists (§4.7); the contract faults are listed only for a
// profiled unit of a run that pinned contracts (interruptionTypes), so they
// are not among these. Each spends the consecutive fault budget, then own attempts, then
// fails with its own reason. PROFILE_DRIFTED is E91's: its Orker cordons
// itself, so the re-dispatch goes to another Orker of the run's release.
func neverStartedFaultReasons() [3]string {
	return [3]string{attemptStoreUnavailableReason, runtimeStateUnknownReason, profileDriftedReason}
}

// isNeverStartedFault reports a never-started fault. The contract faults are
// among them; they are consulted only for a type the execution listed
// (interruptionOf), which only a profiled unit of a run with contracts lists.
func isNeverStartedFault(reason string) bool {
	switch reason {
	case attemptStoreUnavailableReason, runtimeStateUnknownReason, profileDriftedReason,
		profileChangedReason, profileContractNotServedReason:
		return true
	}
	return false
}

// interruptionKind is §4.7's class of an ended execution.
type interruptionKind int

const (
	notInterruption interruptionKind = iota
	// neverStartedStop: a stopping Orker handed the attempt back before its
	// container started. It never fails and never spends own attempts.
	neverStartedStop
	// neverStartedFault: a store, runtime or profile fault before the start.
	neverStartedFault
	// handedBack: a stopping Orker handed back an attempt that had started.
	handedBack
	// lost: the cluster's heartbeat timeout ended the attempt.
	lost
)

func (k interruptionKind) String() string {
	switch k {
	case neverStartedStop:
		return "never started (stop)"
	case neverStartedFault:
		return "never started (fault)"
	case handedBack:
		return "handed back"
	case lost:
		return "lost"
	}
	return "not an interruption"
}

// classifiedInterruption is one ended execution read as an interruption.
type classifiedInterruption struct {
	kind interruptionKind
	// event is what the unit records as its last interruption.
	event *orkstackv1.TaskInterruption
	// attempt is the in-execution attempt it ended at: 1 when not known.
	attempt int32
	// started: the attempt's container is known to have started, which
	// resets the never-started counters. Unknown reads as not started.
	started bool
	// stopping: the attempt's last beat said its Orker was stopping, so a
	// loss is budgeted as a hand-back (E92-N2), whether or not the instant
	// it named parsed.
	stopping bool
	// undecoded: the failure's details were missing or did not decode as
	// the Orker sends them, so it was budgeted conservatively (a typed one as
	// a hand-back at attempt 1); the interpreter logs it. A loss with no beat
	// at all (lost before its first) is not undecoded.
	undecoded bool
}

// interruptionBeat is the frozen heartbeat-detail decoder (AGENTS.md §7.2's
// command paths): the fields of an attempt's last beat (task.HeartbeatProgress,
// JSON) a loss is classified and budgeted from. It is a copy and never that
// type, so a field the Orker adds later changes no decision a recorded
// history made; a field it never sent reads as zero, the conservative side.
type interruptionBeat struct {
	Phase         string `json:"phase"`
	OrkerID       string `json:"orker_id"`
	Ordinal       int32  `json:"ordinal"`
	Attempt       int32  `json:"attempt"`
	StoppingSince string `json:"stopping_since"`
	Started       bool   `json:"started"`
}

// classifyInterruption reads r as §4.7's signal table: a typed interruption
// by its reason and never_started mark; the cluster's heartbeat timeout only
// while lostListed (the unit's policy listed it); anything else is not one.
// now is the workflow's time, recorded as the interruption's instant.
func classifyInterruption(r failureReading, lostListed bool, now time.Time) classifiedInterruption {
	if reason, ok := strings.CutPrefix(r.appType(), interruptionTypePrefix); ok {
		return classifyTyped(r, reason, now)
	}
	if lostListed && r.timeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT {
		return classifyLoss(r, now)
	}
	return classifiedInterruption{kind: notInterruption}
}

func classifyTyped(r failureReading, reason string, now time.Time) classifiedInterruption {
	// A detail that is missing or does not decode leaves detail nil: every
	// read below is a getter, so it reads as zero — not known never started,
	// so never the class that never fails — and the event says so.
	detail, ok := r.activityDetail()
	c := classifiedInterruption{
		attempt:   max(detail.GetAttempt(), 1),
		started:   detail.GetStarted(),
		undecoded: !ok || detail == nil,
		event: &orkstackv1.TaskInterruption{
			Reason:       reason,
			OrkerId:      detail.GetOrkerId(),
			At:           timestamppb.New(now),
			NeverStarted: detail.GetNeverStarted(),
			StoreError:   detail.GetStoreError(),
		},
	}
	if detail != nil && detail.Ordinal != nil {
		ordinal := detail.GetOrdinal()
		c.event.Ordinal = &ordinal
	}
	switch {
	case isNeverStartedFault(reason):
		// Every such fault is found before a container starts (§4.2): one
		// created and never started is removed first.
		c.kind, c.started = neverStartedFault, false
		c.event.NeverStarted = true
	case reason == orkerStoppedReason && detail.GetNeverStarted() && !detail.GetStarted():
		c.kind = neverStartedStop
	case reason == orkerStoppedReason || reason == containerStoppedByRuntimeReason:
		c.kind = handedBack
		c.event.NeverStarted = false
	default:
		// A type the engine never lists: not one of the class.
		return classifiedInterruption{kind: notInterruption}
	}
	switch {
	case c.event.NeverStarted:
		c.event.Phase = orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_BEFORE_START
	case c.started:
		c.event.Phase = orkstackv1.InterruptionPhase_INTERRUPTION_PHASE_RUNNING
	}
	return c
}

// classifyLoss reads a heartbeat timeout's last beat. A beat that does not
// decode is a loss of an unknown attempt: attempt 1, not known started. A
// loss after a beat that said the attempt reported is the next attempt's,
// lost before its first beat, so it counts one more. A loss whose beat says
// the Orker was stopping is recorded ORKER_STOPPED (E92-N2): a known
// shutdown, budgeted as a hand-back.
func classifyLoss(r failureReading, now time.Time) classifiedInterruption {
	var beat interruptionBeat
	undecoded := false
	if r.timeout != nil && r.timeout.HasLastHeartbeatDetails() && !r.heartbeatDetails(&beat) {
		beat, undecoded = interruptionBeat{}, true
	}
	c := classifiedInterruption{
		undecoded: undecoded,
		kind:      lost,
		attempt:   max(beat.Attempt, 1),
		started:   beat.Started,
		event: &orkstackv1.TaskInterruption{
			Reason:  orkerHeartbeatLostReason,
			OrkerId: beat.OrkerID,
			Phase:   task.InterruptionPhaseOf(beat.Phase),
			At:      timestamppb.New(now),
		},
	}
	if beat.Phase == reportedBeatPhase {
		// The beat is attempt k's after its report; the loss is attempt
		// k+1's before its first beat, and k's start says nothing of it.
		c.started = false
		if beat.Attempt > 0 && beat.Attempt < math.MaxInt32 {
			c.attempt = beat.Attempt + 1
		}
	}
	if beat.Ordinal > 0 {
		c.event.Ordinal = &beat.Ordinal
	}
	if beat.StoppingSince != "" {
		// The Orker said it was stopping: that decides the budget, even
		// when the instant it named does not parse (recorded unset, marked).
		c.stopping = true
		c.event.Reason = orkerStoppedReason
		if since, err := time.Parse(time.RFC3339Nano, beat.StoppingSince); err == nil {
			c.event.OrkerStoppingSince = timestamppb.New(since)
		} else {
			c.undecoded = true
		}
	}
	return c
}

// unitBudgets are one task's (or fan-out item's) budgets: the run's plan,
// capped by its retry.max_interruptions.
type unitBudgets struct {
	lost, lostRunCap, handedBack, neverStarted, faults int32
	// backoff is the never-started back-off's initial wait, faultBackoff the
	// fault back-off's (§4.8 4a: floored at the store-outage detection time).
	backoff, faultBackoff time.Duration
}

// budgetsForUnit is a unit's budgets: nil — nothing listed, every failure
// today's — for a run with no budgets (an older plan, a mixed fleet) or a
// task that opted out (max_interruptions: 0). A positive max_interruptions
// caps lost and handed back and never raises them; the never-started class
// keeps the plan's. faultFloor is the store-outage detection time the run
// pinned, the fault back-off's floor.
func budgetsForUnit(plan *InterruptionBudgets, maxInterruptions *int32, faultFloor time.Duration) *unitBudgets {
	if plan == nil || (maxInterruptions != nil && *maxInterruptions <= 0) {
		return nil
	}
	// A plan is validated at boot; a negative number read from a corrupt one
	// is 0, and the initial wait is clamped before it is multiplied.
	b := &unitBudgets{
		lost: max(plan.Lost, 0), lostRunCap: max(plan.LostRunCap, 0), handedBack: max(plan.HandedBack, 0),
		neverStarted: max(plan.NeverStarted, 0), faults: max(plan.NeverStartedFaults, 0),
		backoff: time.Duration(min(max(plan.NeverStartedBackoffMillis, 0), maxInterruptionBackoff.Milliseconds())) * time.Millisecond,
	}
	// Budgets all 0 (a mixed fleet's plan) list nothing, as no budgets do.
	if b.lost == 0 && b.handedBack == 0 && b.neverStarted == 0 && b.faults == 0 {
		return nil
	}
	if maxInterruptions != nil {
		b.lost = min(b.lost, *maxInterruptions)
		b.handedBack = min(b.handedBack, *maxInterruptions)
	}
	b.faultBackoff = max(b.backoff, faultFloor)
	return b
}

// interruptionTypes is the NonRetryableErrorTypes a unit's next execution
// lists, in one fixed order: the never-started class always (it never
// closes; ORKER_STOPPED is also the hand-back's type, so an exhausted
// hand-back still arrives typed and is closed when it does);
// CONTAINER_STOPPED_BY_RUNTIME while hand-backs have budget; the heartbeat
// timeout while losses have budget and the run's loss events are under the
// cap. Nil budgets list nothing: the policy is today's, byte for byte.
//
// profileFaults lists the contract faults too, after the three existing
// faults: a unit that names a profile under a run that pinned contracts.
// Every other unit lists exactly what it listed before contracts.
func interruptionTypes(b *unitBudgets, st *orkstackv1.TaskInterruptions, runLoss *orkstackv1.RunLossEvents, profileFaults bool) []string {
	if b == nil {
		return nil
	}
	types := make([]string, 0, 8)
	types = append(types, interruptionTypePrefix+orkerStoppedReason)
	for _, f := range neverStartedFaultReasons() {
		types = append(types, interruptionTypePrefix+f)
	}
	if profileFaults {
		types = append(types, interruptionTypePrefix+profileChangedReason, interruptionTypePrefix+profileContractNotServedReason)
	}
	if st.GetHandedBack() < b.handedBack {
		types = append(types, interruptionTypePrefix+containerStoppedByRuntimeReason)
	}
	if st.GetLost() < b.lost && runLoss.GetCount() < b.lostRunCap {
		types = append(types, heartbeatTimeoutType)
	}
	return types
}

// remainingAttempts is the next execution's MaximumAttempts: what the
// task's own attempts (M = max(1, retry.maximum_attempts)) leave after the
// ones interruptions spent, never fewer than one.
func remainingAttempts(m int32, st *orkstackv1.TaskInterruptions) int32 {
	return max(1, max(m, 1)-st.GetOwnAttemptsUsed())
}

// interruptionDecision is what the interpreter does with an interruption.
type interruptionDecision struct {
	// redispatch: run the unit again, after wait when it is above zero.
	redispatch bool
	wait       time.Duration
	// fail: the unit fails with the interruption's reason and message.
	fail bool
	// closedBy names the class that closed, when one did.
	closedBy string
	// message is the failure's account of the interruptions, set with fail.
	message string
}

// applyInterruption is §4.8's arithmetic for one interruption of a unit
// whose own attempts are m (retry.maximum_attempts): it updates st and
// runLoss in place and says what to do. lossWait is the unit's cancellation
// grace plus its heartbeat timeout, the fence after a loss and the window a
// loss event spans. A cancel is the caller's and wins before this is asked.
func applyInterruption(st *orkstackv1.TaskInterruptions, b unitBudgets, m int32, lossWait time.Duration,
	runLoss *orkstackv1.RunLossEvents, ev classifiedInterruption, now time.Time) interruptionDecision {
	m = max(m, 1)
	// Own attempts the execution spent before the one interrupted.
	k := min(max(ev.attempt, 1), remainingAttempts(m, st))
	st.OwnAttemptsUsed = saturatingAdd(st.OwnAttemptsUsed, k-1)
	// The reset rule: an attempt whose container started ends the run of
	// never-started bounces and faults; nothing resets lost or handed_back.
	if ev.started {
		st.NeverStarted, st.StoreFaults = 0, 0
	}
	st.Last = ev.event
	st.NotBefore = nil

	var d interruptionDecision
	switch ev.kind {
	case neverStartedStop:
		st.NeverStarted = saturatingAdd(st.NeverStarted, 1)
		d = interruptionDecision{redispatch: true}
		if st.NeverStarted > b.neverStarted {
			d.wait = backoffWait(b.backoff, st.NeverStarted-b.neverStarted-1)
		}
	case neverStartedFault:
		st.StoreFaults = saturatingAdd(st.StoreFaults, 1)
		if st.StoreFaults <= b.faults {
			d = interruptionDecision{redispatch: true}
			if st.StoreFaults > 1 {
				d.wait = backoffWait(b.faultBackoff, st.StoreFaults-2)
			}
		} else {
			st.StoreFaults = 0
			d = closeClass(st, closedByNeverStarted, m)
		}
	case handedBack:
		if st.HandedBack < b.handedBack {
			st.HandedBack++
			d = interruptionDecision{redispatch: true}
		} else {
			d = closeClass(st, closedByHandedBack, m)
		}
	case lost:
		d = applyLoss(st, b, m, lossWait, runLoss, ev, now)
	default:
		return interruptionDecision{}
	}
	if d.redispatch && d.wait > 0 {
		st.NotBefore = timestamppb.New(now.Add(d.wait))
	}
	if d.fail {
		d.message = interruptionAccount(st, b, runLoss, d.closedBy, ev.kind == lost && ev.stopping)
	}
	return d
}

// applyLoss is a loss's step: a loss while its Orker was stopping keeps the
// fence but spends handed_back and opens no loss event (E92-N2); any other
// joins or opens a loss event, then spends lost while the unit and the run
// have budget.
func applyLoss(st *orkstackv1.TaskInterruptions, b unitBudgets, m int32, lossWait time.Duration,
	runLoss *orkstackv1.RunLossEvents, ev classifiedInterruption, now time.Time) interruptionDecision {
	if ev.stopping {
		st.LostWhileStopping = saturatingAdd(st.LostWhileStopping, 1)
		if st.HandedBack < b.handedBack {
			st.HandedBack++
			return interruptionDecision{redispatch: true, wait: lossWait}
		}
		return closeClass(st, closedByHandedBack, m)
	}
	countLossEvent(runLoss, ev.event.GetOrkerId(), lossWait, now)
	if st.Lost >= b.lost {
		return closeClass(st, closedByLost, m)
	}
	if runLoss.GetCount() > b.lostRunCap {
		return closeClass(st, closedByLostRunCap, m)
	}
	st.Lost++
	return interruptionDecision{redispatch: true, wait: lossWait}
}

// countLossEvent joins a loss to an open event of its Orker within window,
// or opens one (counted). Events past their window drop out of open. A loss
// naming no Orker is always its own event and is never kept open. runLoss
// is the run's own and never nil: the interpreter holds one for every run.
func countLossEvent(runLoss *orkstackv1.RunLossEvents, orker string, window time.Duration, now time.Time) {
	open := make([]*orkstackv1.OpenLossEvent, 0, len(runLoss.GetOpen())+1)
	joined := false
	for _, e := range runLoss.GetOpen() {
		if now.Sub(e.GetFirstAt().AsTime()) >= window {
			continue
		}
		if orker != "" && e.GetOrkerId() == orker {
			joined = true
		}
		open = append(open, e)
	}
	if !joined {
		runLoss.Count = saturatingAdd(runLoss.Count, 1)
		if orker != "" {
			open = append(open, &orkstackv1.OpenLossEvent{OrkerId: orker, FirstAt: timestamppb.New(now)})
		}
	}
	runLoss.Open = open
}

// closeClass is §4.8 step 7: a class that is out of budget spends one own
// attempt; the unit runs again while one remains, and fails otherwise.
func closeClass(st *orkstackv1.TaskInterruptions, closedBy string, m int32) interruptionDecision {
	st.OwnAttemptsUsed = saturatingAdd(st.OwnAttemptsUsed, 1)
	if st.Last != nil {
		st.Last.ClosedBy = closedBy
	}
	if m-st.OwnAttemptsUsed >= 1 {
		return interruptionDecision{redispatch: true, closedBy: closedBy}
	}
	return interruptionDecision{fail: true, closedBy: closedBy}
}

// interruptionAccount is a failed unit's account of its interruptions, the
// message beside its real reason.
//
// lostWhileStopping: the interruption that closed the class was a loss
// while its Orker was stopping (R11-2), which the account says.
func interruptionAccount(st *orkstackv1.TaskInterruptions, b unitBudgets, runLoss *orkstackv1.RunLossEvents, closedBy string, lostWhileStopping bool) string {
	last := st.GetLast()
	if closedBy == closedByNeverStarted {
		account := fmt.Sprintf("%s before it started, past its budget of %d in a row", last.GetReason(), b.faults)
		if e := last.GetStoreError(); e != "" {
			account += ": object storage answered: " + e
		}
		return account + "; see orkctl run attempts"
	}
	total := saturatingAdd(saturatingAdd(st.GetLost(), st.GetHandedBack()), 1)
	account := fmt.Sprintf("interrupted %d times (lost %d/%d, handed back %d/%d, lost Orkers %d/%d: %s)",
		total, st.GetLost(), b.lost, st.GetHandedBack(), b.handedBack, runLoss.GetCount(), b.lostRunCap, closedBy)
	if lostWhileStopping {
		account += "; lost while its Orker was stopping"
	}
	return account + "; see orkctl run attempts"
}

// backoffWait is initial × 2^n, the exponent clamped to [0, 6] and the wait
// to (0, 60 s]: never negative, never overflowing, never zero.
func backoffWait(initial time.Duration, n int32) time.Duration {
	n = min(max(n, 0), maxInterruptionBackoffExponent)
	initial = min(max(initial, time.Millisecond), maxInterruptionBackoff)
	return min(initial<<n, maxInterruptionBackoff)
}

// saturatingAdd is a + d for counters, held at math.MaxInt32.
func saturatingAdd(a, d int32) int32 {
	if d > 0 && a > math.MaxInt32-d {
		return math.MaxInt32
	}
	return a + d
}
