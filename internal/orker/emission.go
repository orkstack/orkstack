package orker

import (
	"context"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The error types an engine lists in an attempt's NonRetryableErrorTypes to
// ask the Orker for typed interruptions (E92 §4.7). An engine that lists none
// gets today's category-typed errors; this Orker emits a typed interruption
// only for a type the attempt's own retry policy names.
const (
	// interruptionTypePrefix prefixes an interruption's reason:
	// OrkInterruption:ORKER_STOPPED.
	interruptionTypePrefix = "OrkInterruption:"
	// HeartbeatTimeoutType listed means a lost attempt is the engine's to
	// classify from the cluster's own heartbeat timeout, so the Orker leaves
	// ORKER_HEARTBEAT_LOST unreported.
	HeartbeatTimeoutType = "TemporalTimeout:Heartbeat"
)

// InterruptionType is the error type of reason as a typed interruption.
func InterruptionType(reason string) string { return interruptionTypePrefix + reason }

// mayEmit is the Orker's compiled set of interruptions, each with its own
// condition: ORKER_STOPPED always; ATTEMPT_STORE_UNAVAILABLE only for an
// attempt that started no container; CONTAINER_STOPPED_BY_RUNTIME only while
// this Orker is stopping, when its own runtime going away is the stop's doing.
func (f *Failure) mayEmit(stopping bool) bool {
	switch f.Reason {
	case ReasonOrkerStopped:
		return true
	case ReasonAttemptStoreUnavailable:
		return f.NeverStarted
	case ReasonContainerStoppedByRuntime:
		return stopping
	}
	return false
}

// EmittedError is the error an attempt reports for f, given the types its
// retry policy lists. A listed interruption the Orker may emit is typed
// OrkInterruption:<REASON> and non-retryable, so it ends the execution and
// the engine re-dispatches it; a listed one it may not emit is declined and
// stays today's error. observe, when set, hears each decision: true emitted,
// false declined. A reason nobody listed is neither, and nothing changes.
func (f *Failure) EmittedError(listed []string, stopping bool, observe func(emitted bool)) error {
	if !slices.Contains(listed, InterruptionType(f.Reason)) {
		return f.TemporalError()
	}
	if !f.mayEmit(stopping) {
		if observe != nil {
			observe(false)
		}
		return f.TemporalError()
	}
	if observe != nil {
		observe(true)
	}
	detail := f.Detail
	if detail == nil {
		detail = &orkstackv1.ActivityFailureDetail{ReasonCode: f.Reason, Category: CategoryEnum(f.Category())}
	}
	return temporal.NewNonRetryableApplicationError(f.Error(), InterruptionType(f.Reason), nil, detail)
}

// leaveHeartbeatLossUnreported: the engine listed the cluster's heartbeat
// timeout, so an attempt that lost its heartbeat reports nothing and is
// classified from the timeout itself. Only an attempt the cluster will time
// out (heartbeatTimeout above zero) may stay silent; any other reports its
// loss, or it would wait on no clock at all.
func leaveHeartbeatLossUnreported(listed []string, heartbeatTimeout time.Duration) bool {
	return heartbeatTimeout > 0 && slices.Contains(listed, HeartbeatTimeoutType)
}

// heartbeatTimeoutOf is the attempt's own heartbeat timeout, zero outside an
// activity.
func heartbeatTimeoutOf(ctx context.Context) time.Duration {
	if !activity.IsActivity(ctx) {
		return 0
	}
	return activity.GetInfo(ctx).HeartbeatTimeout
}

// emit is the error an attempt reports for failure: a typed interruption when
// its retry policy lists one this Orker may emit (EmittedError), else today's
// error. A declined one is logged, so an engine that asked for a type and got
// another can be told why.
func (d *OCIDispatcher) emit(ctx context.Context, in *orkstackv1.ExecuteTaskInput, failure *Failure) error {
	// Whether the Orker was stopping when the failure was observed, not now.
	stopping := failure.DuringStop
	return failure.EmittedError(d.listedTypes(ctx), stopping, func(emitted bool) {
		if d.Interruptions != nil {
			d.Interruptions.InterruptionEmission(emitted)
		}
		if !emitted {
			d.logger().Warn("the engine asked for a typed interruption this orker may not emit; reporting the failure as it is",
				"run", in.GetRunId(), "task", in.GetTaskId(), "reason", failure.Reason,
				"never_started", failure.NeverStarted, "stopping", stopping)
		}
	})
}

// listedTypes is the attempt's NonRetryableErrorTypes, through d.listed
// when a test set it.
func (d *OCIDispatcher) listedTypes(ctx context.Context) []string {
	if d.listed != nil {
		return d.listed(ctx)
	}
	return listedTypes(ctx)
}

// listedTypes is the attempt's NonRetryableErrorTypes, read from its own
// retry policy: none outside an activity, or for one with no policy.
func listedTypes(ctx context.Context) []string {
	if !activity.IsActivity(ctx) {
		return nil
	}
	if policy := activity.GetInfo(ctx).RetryPolicy; policy != nil {
		return policy.NonRetryableErrorTypes
	}
	return nil
}

// neverStarted marks f as an attempt that started no container: its claim
// comes before any container exists, so a claim's fault did no work.
func neverStarted(f *Failure) *Failure {
	if f != nil {
		f.NeverStarted = true
	}
	return f
}

// StampAttempt records on f's detail whether the attempt's start was asked
// for (conservatively its container's start: a start cut off may still have
// started it), whether it is a never-started interruption, its ordinal
// (0: none claimed) and, for a refused claim, the store's error. started and
// never_started are never both sent: a container that ran means the attempt
// did work, whatever else marked it.
func (f *Failure) StampAttempt(started bool, ordinal int32) *Failure {
	if f == nil {
		return nil
	}
	if f.Detail == nil {
		f.Detail = &orkstackv1.ActivityFailureDetail{ReasonCode: f.Reason, Category: CategoryEnum(f.Category())}
	}
	// A container that ran means the attempt did work, whatever marked it:
	// the mark itself is cleared, so what is emitted and what is sent agree.
	if started {
		f.NeverStarted = false
	}
	f.Detail.Started = started
	f.Detail.NeverStarted = f.NeverStarted
	if ordinal > 0 {
		f.Detail.Ordinal = &ordinal
	}
	if f.Reason == ReasonAttemptStoreUnavailable && f.Detail.NeverStarted && f.Cause != nil {
		f.Detail.StoreError = f.Cause.Error()
	}
	return f
}

// InterruptionObserver hears each typed-interruption decision: emitted, or a
// listed one declined (ork.orker.interruption_emission, internal/telemetry).
type InterruptionObserver interface {
	InterruptionEmission(emitted bool)
}

// stampFor is StampAttempt for an attempt whose retry policy lists listed: the
// detail's new fields (never_started, started, ordinal, store_error) go only
// to an engine that asked for interruptions, because an engine that predates
// them decodes the detail with protojson, which refuses an unknown field and
// would lose the whole detail. The never-started mark itself is still
// cleared for an attempt that started, so what is emitted stays right.
func (f *Failure) stampFor(listed []string, started bool, ordinal int32) *Failure {
	if f == nil {
		return nil
	}
	if !asksForInterruptions(listed) {
		if started {
			f.NeverStarted = false
		}
		return f
	}
	return f.StampAttempt(started, ordinal)
}

// asksForInterruptions: the attempt's retry policy lists a typed interruption
// or the cluster's heartbeat timeout, which only an engine that knows the
// detail's new fields lists.
func asksForInterruptions(listed []string) bool {
	for _, t := range listed {
		if t == HeartbeatTimeoutType || strings.HasPrefix(t, interruptionTypePrefix) {
			return true
		}
	}
	return false
}
