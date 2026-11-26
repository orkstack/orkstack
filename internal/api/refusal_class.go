package api

import (
	"context"
	"errors"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/consent"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// refusalClass is what a refusal to start a run means for an event source's
// message (docs/EVENT_SOURCES.md). A person's submit reads none of it.
type refusalClass int

const (
	// refusalUncertain: not classed, or a fault that would repeat. Tried
	// again a bounded number of times, then the source pauses.
	refusalUncertain refusalClass = iota
	// refusalReject: this message can never be accepted.
	refusalReject
	// refusalWait: nothing is wrong with the message or the source.
	refusalWait
	// refusalPause: every message of the source would be refused alike.
	refusalPause
)

// What a wait waits for.
const (
	waitAdmission = "admission"
	waitPool      = "pool"
	waitRollout   = "rollout"
	waitOutage    = "outage"
	waitTimeout   = "timeout"
)

// The codes of what is not classed further.
const (
	codeStoreRefused   = "STORE_REFUSED"
	codeClusterRefused = "CLUSTER_REFUSED"
	codeUnclassified   = "UNCLASSIFIED"
)

// verdict is a refusal as the host service judges it: its class, its code
// for a reject, a pause and an uncertain one, and a wait's cause. Both are
// from closed sets: none carries a word of the error.
type verdict struct {
	class refusalClass
	code  string
	cause string
}

// refusalMark marks an error with its verdict. It stands inside the Connect
// error, as busyRefusal does, so what rebuilds the outer one keeps it, and it
// changes no word and no code.
type refusalMark struct {
	verdict
	err error
}

func (r *refusalMark) Error() string { return r.err.Error() }
func (r *refusalMark) Unwrap() error { return r.err }

// refused marks err as a refusal of class with its code: a pause's or an
// uncertain one's refusal code, a reject's reason, a wait's cause.
func refused(class refusalClass, code string, err error) error {
	v := verdict{class: class, code: code}
	if class == refusalWait {
		v = verdict{class: class, cause: code}
	}
	return &refusalMark{verdict: v, err: err}
}

// unlessTyped marks err as refused does where neither its type nor a mark
// says what it is.
func unlessTyped(class refusalClass, code string, err error) error {
	if v := classifyRefusal(err); v != uncertainOf(codeUnclassified) {
		return err
	}
	return refused(class, code, err)
}

// outage marks a read or a write of the store, the cluster, a registry or
// the fleet that failed: a wait, or, where it would fail again however often
// it is tried, uncertain.
func outage(err error) error { return &refusalMark{verdict: outageVerdict(err), err: err} }

func outageVerdict(err error) verdict {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return waits(waitTimeout)
	case errors.Is(err, errNoAdmissionReader):
		return uncertainOf(codeUnclassified)
	case artifacts.StoreRefuses(err):
		return uncertainOf(codeStoreRefused)
	case classifyStart(err) == startRefused:
		return uncertainOf(codeClusterRefused)
	}
	return waits(waitOutage)
}

// refusalRule judges one type of refusal. name is the type's, as its package
// spells it.
type refusalRule struct {
	name  string
	judge func(error) (verdict, bool)
}

func always[E error](name string, v verdict) refusalRule {
	return refusalRule{name: name, judge: func(err error) (verdict, bool) {
		_, ok := errors.AsType[E](err)
		return v, ok
	}}
}

func waits(cause string) verdict      { return verdict{class: refusalWait, cause: cause} }
func pauses(code string) verdict      { return verdict{class: refusalPause, code: code} }
func uncertainOf(code string) verdict { return verdict{class: refusalUncertain, code: code} }

// refusalTypes are the typed refusals of a submit's path.
var refusalTypes = []refusalRule{
	always[*engine.AdmissionRefusedError]("engine.AdmissionRefusedError", waits(waitAdmission)),
	always[*busyRefusal]("api.busyRefusal", waits(waitAdmission)),
	always[*limitReached]("api.limitReached", waits(waitAdmission)),
	{name: "api.checkUnavailable", judge: func(err error) (verdict, bool) {
		_, ok := errors.AsType[*checkUnavailable](err)
		return outageVerdict(err), ok
	}},

	always[*engine.PoolEmptyError]("engine.PoolEmptyError", waits(waitPool)),
	always[*engine.PoolNoPinHostError]("engine.PoolNoPinHostError", waits(waitPool)),
	{name: "engine.PoolUnreachableError", judge: func(err error) (verdict, bool) {
		_, ok := errors.AsType[*engine.PoolUnreachableError](err)
		return outageVerdict(err), ok
	}},
	always[*engine.PoolNoEachSupportError]("engine.PoolNoEachSupportError", waits(waitRollout)),
	always[*engine.PoolNoCacheSupportError]("engine.PoolNoCacheSupportError", waits(waitRollout)),
	always[*engine.PoolNoJoinSupportError]("engine.PoolNoJoinSupportError", waits(waitRollout)),
	always[*engine.PoolNoSecretMaskingError]("engine.PoolNoSecretMaskingError", waits(waitRollout)),
	always[*engine.PoolNoAttemptOrdinalsError]("engine.PoolNoAttemptOrdinalsError", waits(waitRollout)),
	always[*engine.PoolNoIdentitySupportError]("engine.PoolNoIdentitySupportError", waits(waitRollout)),
	always[*engine.PoolNoProviderError]("engine.PoolNoProviderError", waits(waitRollout)),
	{name: "engine.ProfileUnavailableError", judge: func(err error) (verdict, bool) {
		e, ok := errors.AsType[*engine.ProfileUnavailableError](err)
		if ok && e.Nowhere {
			// A roll that has not reached any Orker, or a name nobody will
			// ever define: not told apart.
			return uncertainOf(engine.ReasonProfileUnavailable), true
		}
		return waits(waitRollout), ok
	}},
	always[*engine.ProfileMismatchError]("engine.ProfileMismatchError", waits(waitRollout)),
	always[*engine.ProfileKindUnsupportedError]("engine.ProfileKindUnsupportedError", waits(waitRollout)),

	always[*engine.PoolNotAllowedError]("engine.PoolNotAllowedError", pauses("POOL_NOT_ALLOWED")),
	always[*engine.PinPoolMismatchError]("engine.PinPoolMismatchError", pauses("PIN_POOL_MISMATCH")),
	always[*engine.SecretNotAllowedError]("engine.SecretNotAllowedError", pauses("SECRET_NOT_ALLOWED")),
	// Judged from the Orkers that answered, which a roll changes.
	always[*engine.PoolResourcesUnsatisfiableError]("engine.PoolResourcesUnsatisfiableError", uncertainOf("RESOURCES_UNSATISFIABLE")),
	always[*engine.DevicesNotAProfileError]("engine.DevicesNotAProfileError", uncertainOf("DEVICES_NOT_A_PROFILE")),
	always[*engine.GPUWithoutGPUProfileError]("engine.GPUWithoutGPUProfileError", uncertainOf("GPU_WITHOUT_PROFILE")),
	always[*engine.ForeignSecretsUnboundedError]("engine.ForeignSecretsUnboundedError", pauses("FOREIGN_SECRETS_UNBOUNDED")),
	always[*authz.NotSharedError]("authz.NotSharedError", pauses("ORKFLOW_NOT_SHARED")),
	{name: "task.ErrInvalidPool", judge: func(err error) (verdict, bool) {
		return pauses("ORKFLOW_INVALID"), errors.Is(err, task.ErrInvalidPool)
	}},

	always[*identity.AudienceNotPermittedError]("identity.AudienceNotPermittedError", pauses("IDENTITY_NOT_PERMITTED")),
	always[*identity.SubjectTooLongError]("identity.SubjectTooLongError", pauses("IDENTITY_SUBJECT_TOO_LONG")),
	{name: "identity.GrantRefusedError", judge: func(err error) (verdict, bool) {
		e, ok := errors.AsType[*identity.GrantRefusedError](err)
		if !ok {
			return verdict{}, false
		}
		return pauses(e.Reason), true
	}},
	{name: "task.ImagePolicyRefusal", judge: func(err error) (verdict, bool) {
		e, ok := errors.AsType[*task.ImagePolicyRefusal](err)
		if !ok {
			return verdict{}, false
		}
		return pauses(e.Code), true
	}},
	{name: "eventsources.Error", judge: func(err error) (verdict, bool) {
		e, ok := errors.AsType[*eventsources.Error](err)
		if !ok {
			return verdict{}, false
		}
		return pauses(e.Reason), true
	}},
	{name: "consent.ErrChildMissing", judge: func(err error) (verdict, bool) {
		return pauses("COMPOSED_ORKFLOW_UNUSABLE"), errors.Is(err, consent.ErrChildMissing) || errors.Is(err, consent.ErrTooDeep)
	}},
}

// classifyRefusal judges why a run was not started: the mark it carries, its
// type, a deadline, and otherwise uncertain. It never reads a Connect code or
// a sentence.
func classifyRefusal(err error) verdict {
	if err == nil {
		return verdict{}
	}
	if r, ok := errors.AsType[*refusalMark](err); ok {
		return r.verdict
	}
	for _, rule := range refusalTypes {
		if v, ok := rule.judge(err); ok {
			return v
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return waits(waitTimeout)
	}
	return uncertainOf(codeUnclassified)
}
