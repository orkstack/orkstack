package engine

import (
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// failureReading is what the engine reads off an error one of its Temporal
// calls returned: an activity's, a local activity's, a child run's, a timer's
// or the run's own. categoryOf is its one reader (E92 §4.7): no other file of
// the engine names an SDK error type (TestCategoryOf_NoOtherReader), so a new
// kind of failure is classified here, once, and every reader sees it alike.
//
// It is a pure read of the error the workflow already holds: it issues no
// command and reads no clock, so workflow code may call it anywhere.
type failureReading struct {
	// app is the first ApplicationError in the chain, nil when there is none.
	app *temporal.ApplicationError
	// timeout is the first TimeoutError in the chain, nil when there is none.
	timeout *temporal.TimeoutError
	// canceled is temporal.IsCanceledError: the call was cancelled.
	canceled bool
	// terminated: the chain holds a TerminatedError (a child run terminated
	// from outside).
	terminated bool
	// unknownRun: the run a signal was sent to does not exist, or has
	// already closed (UnknownExternalWorkflowExecutionError).
	unknownRun bool
	// viaActivity and viaChild: the chain holds an activity's, or a child
	// run's, failure wrapper, so the error is that task's and not the run's
	// own.
	viaActivity bool
	viaChild    bool
	// retryState is why Temporal stopped retrying an activity, UNSPECIFIED for
	// any other error.
	retryState orkstackv1.RetryState
	// childInner is the text below a child run's failure wrapper: "" with no
	// wrapper, and "the child run failed" when the wrapper holds no cause.
	childInner string
}

// categoryOf reads err once. A nil err reads as nothing at all.
func categoryOf(err error) failureReading {
	var r failureReading
	if err == nil {
		return r
	}
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		r.app = app
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) {
		r.timeout = timeout
	}
	r.canceled = temporal.IsCanceledError(err)
	var terminated *temporal.TerminatedError
	r.terminated = errors.As(err, &terminated)
	var unknown *temporal.UnknownExternalWorkflowExecutionError
	r.unknownRun = errors.As(err, &unknown)
	var activityErr *temporal.ActivityError
	if errors.As(err, &activityErr) {
		r.viaActivity = true
		r.retryState = retryStateEnumFor(activityErr.RetryState())
	}
	var childErr *temporal.ChildWorkflowExecutionError
	if errors.As(err, &childErr) {
		r.viaChild = true
		r.childInner = "the child run failed"
		if inner := errors.Unwrap(childErr); inner != nil {
			r.childInner = inner.Error()
		}
	}
	return r
}

// isApplication: the chain holds an ApplicationError.
func (r failureReading) isApplication() bool { return r.app != nil }

// appType is the ApplicationError's type, "" without one.
func (r failureReading) appType() string {
	if r.app == nil {
		return ""
	}
	return r.app.Type()
}

// appMessage is the ApplicationError's own message, "" without one.
func (r failureReading) appMessage() string {
	if r.app == nil {
		return ""
	}
	return r.app.Message()
}

// nonRetryable is the ApplicationError's NonRetryable, false without one.
func (r failureReading) nonRetryable() bool { return r.app != nil && r.app.NonRetryable() }

// appDetails decodes the ApplicationError's details into v: false without an
// ApplicationError, without details, or when they do not decode as v.
func (r failureReading) appDetails(v any) bool {
	return r.app != nil && r.app.HasDetails() && r.app.Details(v) == nil
}

// activityDetail decodes the ActivityFailureDetail the ApplicationError
// carries: ok says its details decoded at all, which may still leave detail
// nil. It decodes on each call and only when asked, never in categoryOf: an
// in-process ApplicationError whose details are some other Go value (the
// history guard's string) cannot be decoded as a detail, so only a reader that
// expects one asks for it.
func (r failureReading) activityDetail() (detail *orkstackv1.ActivityFailureDetail, ok bool) {
	if r.app == nil || r.app.Details(&detail) != nil {
		return nil, false
	}
	return detail, true
}

// reasonCode is the ActivityFailureDetail's reason code, "" without one.
func (r failureReading) reasonCode() string {
	detail, _ := r.activityDetail()
	return detail.GetReasonCode()
}

// timedOut: the chain holds a TimeoutError.
func (r failureReading) timedOut() bool { return r.timeout != nil }

// timeoutType is the TimeoutError's type, UNSPECIFIED without one.
func (r failureReading) timeoutType() enumspb.TimeoutType {
	if r.timeout == nil {
		return enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}
	return r.timeout.TimeoutType()
}

// heartbeatDetails decodes the TimeoutError's last heartbeat details into v:
// false without a TimeoutError, without details, or when they do not decode.
func (r failureReading) heartbeatDetails(v any) bool {
	return r.timeout != nil && r.timeout.HasLastHeartbeatDetails() && r.timeout.LastHeartbeatDetails(v) == nil
}
