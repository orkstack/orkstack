package orker

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// TaskKeys gives an Orker the key of a run tree it holds an attempt in, for
// the context a sealed task input names (payload-encryption.md §6). The
// engine's GetPayloadKey is its source (P20b); an Orker with none runs no
// sealed task.
type TaskKeys interface {
	KeyFor(ctx context.Context, c payloadcrypt.Context) (*payloadcrypt.Key, error)
}

// WithTaskKeys sets the Orker's source of run keys.
func (a *Activities) WithTaskKeys(keys TaskKeys) *Activities {
	a.taskKeys = keys
	return a
}

// ExecuteTaskRaw is ExecuteTask as cmd/orker registers it: it takes its
// input as the SDK's raw value, opens a sealed one with its run tree's key,
// and seals what the attempt returns under the same key. A task that is not
// sealed runs exactly as through ExecuteTask, and returns what it returns.
func (a *Activities) ExecuteTaskRaw(ctx context.Context, raw converter.RawValue) (any, error) {
	in, scope, err := openTaskInput(ctx, a.taskKeys, raw)
	if err != nil {
		return nil, err
	}
	// An Orker with no source of run keys runs no sealed task: its errors
	// stay themselves, for any failure converter. One that has a source is a
	// process that seals, and marks an unsealed run's error as such.
	if a.taskKeys != nil {
		ctx = withTaskScope(ctx, scope)
	}
	// What the attempt holds of this Orker, where only now it is known.
	if err := admissionFrom(ctx).Resolve(executeTaskRequest(a.dispatcher.Policy, in)); err != nil {
		return sealTaskResult(ctx, nil, admissionError(err))
	}
	res, err := a.ExecuteTask(ctx, in)
	return sealTaskResult(ctx, res, err)
}

// openTaskInput reads a task input: plaintext as it is, sealed under the key
// of the run tree it names. Each refusal is an explicit failure with its
// reason: no source of keys, PAYLOAD_ENCRYPTION_NOT_CONFIGURED (never
// retried); a key the source could not give, PAYLOAD_KEY_UNAVAILABLE
// (retried); one that does not open it, or an input that names another run
// than it was sealed for, PAYLOAD_UNREADABLE.
func openTaskInput(ctx context.Context, keys TaskKeys, raw converter.RawValue) (*orkstackv1.ExecuteTaskInput, *payloadcrypt.Scope, error) {
	p := raw.Payload()
	scope := payloadcrypt.Unsealed()
	if payloadcrypt.IsSealed(p) {
		c, err := payloadcrypt.ParseContext(string(p.GetMetadata()[payloadcrypt.MetaContext]))
		if err != nil || c.Kind != payloadcrypt.KindRun {
			return nil, nil, payloadFailure(keys, ReasonPayloadUnreadable, "the task input is sealed for no run")
		}
		if keys == nil {
			return nil, nil, payloadFailure(keys, ReasonPayloadEncryptionNotConfigured,
				"this task's run is encrypted, and this Orker has no way to get its key")
		}
		key, err := keys.KeyFor(ctx, c)
		switch {
		case errors.Is(err, payloadcrypt.ErrKeyless):
			// The run can never be opened: no attempt will change that.
			return nil, nil, payloadFailure(keys, ReasonPayloadUnreadable, "this task's run has no key that opens it")
		case err != nil:
			// The source's own words stay out of the failure, which no key
			// seals here: they may quote what it read.
			return nil, nil, payloadFailure(keys, ReasonPayloadKeyUnavailable, "the key of this task's run could not be had")
		}
		// A sealed input is bound to its run: an attempt scheduled by any other
		// workflow is refused before anything of it is opened.
		if activity.IsActivity(ctx) && activity.GetInfo(ctx).WorkflowExecution.ID != c.ID {
			return nil, nil, payloadFailure(keys, ReasonPayloadUnreadable, "the task input is sealed for another run than the one that scheduled it")
		}
		scope = payloadcrypt.NewScope(key, c)
		if p, err = scope.Open(p); err != nil {
			return nil, nil, payloadFailure(keys, ReasonPayloadUnreadable, "the task input does not open under its run's key")
		}
	}
	in, err := decodeTaskInput(p)
	if err != nil {
		return nil, nil, payloadFailure(keys, ReasonPayloadUnreadable, "the task input does not decode")
	}
	if scope.Sealed() && in.GetRunId() != scope.Context().ID {
		return nil, nil, payloadFailure(keys, ReasonPayloadUnreadable, "the task input names another run than it is sealed for")
	}
	return in, scope, nil
}

// decodeTaskInput reads a task input in plaintext: the one way the worker
// interceptor, which sizes the attempt's reservation from it, and the attempt
// itself do.
func decodeTaskInput(p *commonpb.Payload) (*orkstackv1.ExecuteTaskInput, error) {
	var in *orkstackv1.ExecuteTaskInput
	if err := converter.GetDefaultDataConverter().FromPayload(p, &in); err != nil {
		return nil, err
	}
	return in, nil
}

// payloadFailure is one of openTaskInput's refusals: fixed words and a
// reason code, since nothing of the run was read. An Orker that seals (it has
// keys) marks it, so it is written as it is; any other returns it as it is.
func payloadFailure(keys TaskKeys, reason, message string) error {
	err := (&Failure{Reason: reason, Message: message}).TemporalError()
	if keys == nil {
		return err
	}
	return payloadcrypt.Keyless(err)
}

// sealTaskResult is what an attempt returns, sealed under its run's key: the
// result, and the failure whole, with its reason code as its plaintext. For
// a run that is not sealed each is itself (markUnsealed).
func sealTaskResult(ctx context.Context, res *orkstackv1.ExecuteTaskResult, err error) (any, error) {
	// The SDK compares ErrResultPending by identity: an attempt whose outcome
	// the Orker reports itself returns it as it is.
	if errors.Is(err, activity.ErrResultPending) {
		return nil, err
	}
	scope := attemptScope(ctx)
	if !scope.Sealed() {
		return res, markUnsealed(ctx, err)
	}
	var result any
	if res != nil {
		result = payloadcrypt.Seal(scope, res)
	}
	return result, payloadcrypt.WithKey(err, scope, attemptReason(err))
}

// sealReport is an outcome the Orker reports on an attempt's behalf
// (OCIDispatcher.report): sealed under the attempt's run key like any
// result, so a stopping Orker's report of a sealed run is never plaintext.
func sealReport(ctx context.Context, result any, err error) (any, error) {
	scope := attemptScope(ctx)
	if !scope.Sealed() {
		return result, markUnsealed(ctx, err)
	}
	if res, ok := result.(*orkstackv1.ExecuteTaskResult); ok || result == nil {
		return sealTaskResult(ctx, res, err)
	}
	return payloadcrypt.Seal(scope, result), payloadcrypt.WithKey(err, scope, attemptReason(err))
}

// markUnsealed is an unsealed run's error: the error itself, so any failure
// converter writes it as it always did, except on an Orker that seals (its
// attempts carry a scope), whose converter withholds an unmarked error.
func markUnsealed(ctx context.Context, err error) error {
	if _, seals := ctx.Value(taskScopeKey{}).(*payloadcrypt.Scope); !seals {
		return err
	}
	return payloadcrypt.WithKey(err, payloadcrypt.Unsealed(), attemptReason(err))
}

// reasonOfTemporal is the reason code an attempt's error carries in its
// detail: what becomes the plaintext message of its sealed failure.
func reasonOfTemporal(err error) string {
	var app *temporal.ApplicationError
	if !errors.As(err, &app) {
		return ""
	}
	var detail *orkstackv1.ActivityFailureDetail
	if app.Details(&detail) != nil {
		return ""
	}
	return detail.GetReasonCode()
}

// taskScopeKey carries an attempt's run key to what the attempt sends the
// cluster besides its result: its heartbeat details.
type taskScopeKey struct{}

func withTaskScope(ctx context.Context, s *payloadcrypt.Scope) context.Context {
	return context.WithValue(ctx, taskScopeKey{}, s)
}

// attemptScope is the run key ExecuteTaskRaw opened the attempt's input
// with; Unsealed() for an attempt that did not come through it, whose input
// was not sealed (ExecuteTask, orkctl task run).
func attemptScope(ctx context.Context) *payloadcrypt.Scope {
	if s, ok := ctx.Value(taskScopeKey{}).(*payloadcrypt.Scope); ok {
		return s
	}
	return payloadcrypt.Unsealed()
}

// heartbeatDetail is progress as an attempt sends it: sealed under its run's
// key for a sealed run (payload-encryption.md §2, heartbeat details), the
// progress itself otherwise.
func heartbeatDetail(ctx context.Context, progress HeartbeatProgress) any {
	return payloadcrypt.Seal(attemptScope(ctx), progress)
}

// attemptReason is the reason code an attempt's error carries, the plaintext
// message of its sealed failure.
func attemptReason(err error) string {
	var f *Failure
	if errors.As(err, &f) {
		return f.Reason
	}
	if err != nil {
		return reasonOfTemporal(err)
	}
	return ""
}
