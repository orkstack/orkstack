package orker

import (
	"context"
	"strconv"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// AttemptRecordObserver is told when this Orker meets an attempt record an
// Orker of an earlier release wrote, numbering attempts as Temporal does, for
// the Orker's metrics (internal/telemetry, AttemptRecordMetrics). It returns
// nothing, so telemetry can never change what an attempt does.
type AttemptRecordObserver interface {
	// LegacyRecordsAtClaim is n records with no claim in the scope a claim
	// was taken in.
	LegacyRecordsAtClaim(n int)
	// LegacyRecordKept is a record already under the ordinal this Orker
	// claimed, which it kept.
	LegacyRecordKept()
}

// claimBackoff is how long a failed claim waits before each of its three
// further tries: about ten seconds in all (resume/design.md §4.2), well inside
// the heartbeat timeout the loop keeps answering meanwhile.
var claimBackoff = []time.Duration{time.Second, 3 * time.Second, 6 * time.Second}

// claimTimeout bounds one try of a claim.
const claimTimeout = 10 * time.Second

// maxPendingLogLines bounds what an attempt logs before it has an ordinal to
// log under. Its own [ork] lines are a handful; a bound is still a bound.
const maxPendingLogLines = 1024

// attemptOrdinal is one attempt's ordinal, from the claim that takes it to the
// marker that releases it, and the log that waits for it.
//
// The ordinal numbers the attempts of one scope (a task, or an item of a
// fan-out) the way the attempt store sees them: a record, a log prefix and a
// container label all carry it. Until the claim, the attempt's [ork] lines
// wait here; the claim opens the log under the ordinal and writes them first.
// An attempt that never claims (cancelled, its heartbeat lost, its Orker
// stopping, or the store refusing the claim) logs nothing and records nothing.
type attemptOrdinal struct {
	d        *OCIDispatcher
	in       *orkstackv1.ExecuteTaskInput
	ctx      context.Context
	temporal int32
	beat     *Heartbeater

	mu      sync.Mutex
	n       int32
	sink    *logChunkSink
	pending []string
	dropped int
	closed  bool
	// live is a container this attempt created and has not seen removed:
	// while it stands, the ordinal is not released.
	live     bool
	released bool
}

func (d *OCIDispatcher) newAttemptOrdinal(ctx context.Context, in *orkstackv1.ExecuteTaskInput, temporal int32) *attemptOrdinal {
	o := &attemptOrdinal{d: d, in: in, ctx: ctx, temporal: temporal}
	if d.Claims == nil {
		// Nothing to claim in: orkctl task run, orkctl activity test, an Orker
		// with no attempt store. The attempt is numbered as Temporal numbers it.
		o.bind(temporal)
	}
	return o
}

// Ordinal is the claimed ordinal, 0 before the claim and for an attempt that
// never claimed one.
func (o *attemptOrdinal) Ordinal() int32 {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.n
}

func (o *attemptOrdinal) scope() artifacts.AttemptRef {
	ref := artifacts.AttemptRef{RunID: o.in.GetRunId(), TaskID: o.in.GetTaskId(), Item: artifacts.NoItem}
	if each := o.in.GetEach(); each != nil {
		ref.Item = artifacts.ItemAt(each.GetIndex())
	}
	return ref
}

// Claim takes the ordinal before the attempt's container: one try and three
// more over about ten seconds. A claim the store never grants is the
// never-started fault ATTEMPT_STORE_UNAVAILABLE. A claim already taken is
// kept.
func (o *attemptOrdinal) Claim(ctx context.Context) *Failure {
	if o.Ordinal() != 0 {
		return nil
	}
	var last error
	for try := 0; ; try++ {
		n, err := o.try(ctx)
		if err == nil {
			o.bind(n)
			return nil
		}
		last = err
		if ctx.Err() != nil {
			// The attempt is being stopped; how is Execute's to say.
			return FailCause(ReasonAttemptStoreUnavailable, err,
				"the attempt was stopped while it claimed its ordinal: %v", err)
		}
		if try >= len(claimBackoff) {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(claimBackoff[try]):
		}
	}
	return FailCause(ReasonAttemptStoreUnavailable, last,
		"orker %s could not claim an attempt ordinal for task %q in %d tries over about 10 s: %v. "+
			"The attempt did nothing: no container was created and no record was written. It runs "+
			"again if the task's retry policy allows",
		o.d.OrkerID, o.in.GetTaskId(), len(claimBackoff)+1, last)
}

// ClaimBestEffort gives an attempt that reached an outcome of its own without
// a container (an adopted commit, a memo hit, a refusal) an ordinal to record
// that outcome under: one try, and nothing changes when it fails, because the
// outcome does not depend on it.
func (o *attemptOrdinal) ClaimBestEffort(ctx context.Context) {
	if o.Ordinal() != 0 {
		return
	}
	n, err := o.try(context.WithoutCancel(ctx))
	if err != nil {
		o.d.logger().Warn("no attempt ordinal for an attempt that ended without a container; it is not recorded",
			"run_id", o.in.GetRunId(), "task_id", o.in.GetTaskId(), "error", err)
		return
	}
	o.bind(n)
}

func (o *attemptOrdinal) try(ctx context.Context) (int32, error) {
	call, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	claimed, err := o.d.Claims.ClaimAttempt(call, o.scope(), o.d.OrkerID)
	if err != nil {
		return 0, err
	}
	if len(claimed.Legacy) > 0 {
		// A record with no claim was written by an Orker that numbers
		// attempts as Temporal does: the pool runs two releases, and that
		// Orker can overwrite this one's records (docs/DEPLOY.md, "Upgrading
		// a pool's Orkers").
		o.d.logger().Warn("an attempt record of this task was written by an Orker of an earlier release; "+
			"roll every Orker of the pool to one release",
			"run_id", o.in.GetRunId(), "task_id", o.in.GetTaskId(), "item", o.scope().Item.String(),
			"legacy_attempts", claimed.Legacy, "ordinal", claimed.Ordinal)
		if o.d.AttemptRecords != nil {
			o.d.AttemptRecords.LegacyRecordsAtClaim(len(claimed.Legacy))
		}
	}
	return claimed.Ordinal, nil
}

// bind fixes the ordinal, opens the attempt's log under it and writes what
// waited, and tells the heartbeat loop: the next beat names it.
func (o *attemptOrdinal) bind(n int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.n != 0 {
		return
	}
	o.n = n
	if o.beat != nil {
		o.beat.setOrdinal(n)
	}
	if o.closed {
		return
	}
	o.sink = o.d.openLogSink(o.ctx, o.in, n)
	for _, line := range o.pending {
		o.sink.Write(line)
	}
	if o.dropped > 0 {
		o.sink.Write("[ork] " + strconv.Itoa(o.dropped) +
			" earlier log lines of this attempt were dropped before it had an ordinal")
	}
	o.pending, o.dropped = nil, 0
}

// setBeat attaches the heartbeat loop, which names the ordinal once bound.
func (o *attemptOrdinal) setBeat(beat *Heartbeater) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.beat = beat
	if o.n != 0 && beat != nil {
		beat.setOrdinal(o.n)
	}
}

// Write logs one line under the ordinal, or holds it until there is one.
func (o *attemptOrdinal) Write(line string) {
	o.mu.Lock()
	if o.n == 0 {
		if !o.closed {
			if len(o.pending) < maxPendingLogLines {
				o.pending = append(o.pending, line)
			} else {
				o.dropped++
			}
		}
		o.mu.Unlock()
		return
	}
	sink := o.sink
	o.mu.Unlock()
	sink.Write(line)
}

// Close closes the attempt's log, the durability barrier logChunkSink.Close
// documents. Lines still waiting for an ordinal are dropped: an attempt with
// none has no log.
func (o *attemptOrdinal) Close() {
	o.mu.Lock()
	o.closed = true
	o.pending = nil
	sink := o.sink
	o.mu.Unlock()
	sink.Close()
}

// ContainerCreated and ContainerRemoved bracket the attempt's container: the
// ordinal is released only once nothing of the attempt runs.
func (o *attemptOrdinal) ContainerCreated() {
	o.mu.Lock()
	o.live = true
	o.mu.Unlock()
}

// ContainerRemoved is the container gone, or still there (err).
func (o *attemptOrdinal) ContainerRemoved(err error) {
	o.mu.Lock()
	if err == nil {
		o.live = false
	}
	o.mu.Unlock()
}

// Release writes {n}.released once the attempt holds nothing: its container
// removed, or none created. Best-effort and once: a marker that cannot be
// written is logged, and the boot reap writes it for a container it removes
// later (resume/design.md §26.7).
func (o *attemptOrdinal) Release(ctx context.Context) {
	o.mu.Lock()
	n, live, done := o.n, o.live, o.released
	if n == 0 || live || done || o.d.Claims == nil {
		o.mu.Unlock()
		return
	}
	o.released = true
	o.mu.Unlock()
	ref := o.scope()
	ref.Attempt = n
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTimeout)
	defer cancel()
	if err := o.d.Claims.ReleaseAttempt(call, ref, o.d.OrkerID); err != nil {
		o.d.logger().Warn("attempt release marker not written",
			"run_id", o.in.GetRunId(), "task_id", o.in.GetTaskId(), "ordinal", n, "error", err)
	}
}

// abortedBeforeClaim reports an attempt ended by something other than its own
// work (a cancel, a lost heartbeat, its Orker stopping, the store refusing the
// claim): one that takes no ordinal at its end if it had none.
func abortedBeforeClaim(outcome AttemptOutcome) bool {
	switch outcome.Reason {
	case ReasonOrkerStopped, ReasonHeartbeatLost, ReasonAttemptStoreUnavailable:
		return true
	}
	return outcome.Canceled
}

// beforeClaimStopped is the failure an attempt returns when it finds itself
// stopped at the latch or before its container: Execute replaces it with
// what stopped it (a cancel, the server's decision, a lost heartbeat), so the
// reason here is only the fallback for a context done with no such cause.
func beforeClaimStopped(err error) *Failure {
	return FailCause(ReasonHeartbeatLost, err,
		"the attempt was stopped before it created a container: %v", err)
}
