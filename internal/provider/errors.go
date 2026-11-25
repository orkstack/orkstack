package provider

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/orkstack/orkstack/internal/task"
)

// Class is how a provider's answer is classified. A provider picks a class
// from its own table (providers.md §11); it never picks a failure reason:
// Classify does, from the class and where the call was made.
type Class int

const (
	// Unavailable is nobody answering, a 5xx, throttling, an unknown code:
	// retried, and loud. Anything a provider cannot place is Unavailable,
	// so an unknown answer is never a silent permanent failure.
	Unavailable Class = iota
	// Refused is the provider saying no to this principal.
	Refused
	// NotFound is a secret or a path that is not there.
	NotFound
	// Invalid is a request the provider says is malformed.
	Invalid
)

func (c Class) String() string {
	switch c {
	case Unavailable:
		return "unavailable"
	case Refused:
		return "refused"
	case NotFound:
		return "not_found"
	case Invalid:
		return "invalid"
	default:
		return "unavailable"
	}
}

// Error is the only error a provider returns. Its text is built from Op and
// Code only: AWS and Vault put account ids, role names and paths in their
// messages, and a failure message is stored in history. Err is kept for
// errors.Is and errors.As and never printed.
type Error struct {
	Class Class
	// Op is the operation, from the provider's own fixed list
	// ("login", "read", "issue", "renew", "close").
	Op string
	// Code is the provider's own error code ("AccessDenied", "403"), from a
	// fixed list. It never holds a value, a token or a response body.
	Code string
	Err  error
}

// codeShape bounds what a Code may look like, so that a provider cannot pass
// a message through it: a short token of letters, digits and "._-".
var codeShape = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func (e *Error) Error() string {
	op := e.Op
	if !codeShape.MatchString(op) {
		op = "call"
	}
	if e.Code == "" {
		return fmt.Sprintf("provider %s: %s", op, e.Class)
	}
	code := e.Code
	if !codeShape.MatchString(code) {
		code = "unprintable-code"
	}
	return fmt.Sprintf("provider %s: %s (%s)", op, e.Class, code)
}

func (e *Error) Unwrap() error { return e.Err }

// ClassOf is the class of err. An error that is not an *Error, a context
// deadline among them, is Unavailable: a provider that failed to classify
// is retried, never treated as a refusal.
func ClassOf(err error) Class {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Class
	}
	return Unavailable
}

// Where is the step a provider call was made in.
type Where int

const (
	// AtSetup is before the container starts (step 6b of the attempt).
	AtSetup Where = iota
	// AtRefresh is while the container runs.
	AtRefresh
)

// The reasons a provider call can end an attempt with (providers.md §11).
// Each is a value of ApplicationErrorDetail.reason_code.
const (
	ReasonProviderUnavailable       = "PROVIDER_UNAVAILABLE"
	ReasonCredentialRefreshFailed   = "CREDENTIAL_REFRESH_FAILED"
	ReasonCredentialRefused         = "CREDENTIAL_REFUSED"
	ReasonSecretAccessDenied        = "SECRET_ACCESS_DENIED"
	ReasonSecretNotFound            = "SECRET_NOT_FOUND"
	ReasonProviderRequestInvalid    = "PROVIDER_REQUEST_INVALID"
	ReasonSecretTooLarge            = "SECRET_TOO_LARGE"
	ReasonMaskCapacityExceeded      = "MASK_CAPACITY_EXCEEDED"
	ReasonProviderNotConfigured     = "PROVIDER_NOT_CONFIGURED"
	ReasonProviderContractViolation = "PROVIDER_CONTRACT_VIOLATION"
)

// The refusals at validate, create and submit, before a run exists. They
// carry a Connect code and no category.
const (
	ReasonProviderUnknown   = "PROVIDER_UNKNOWN"
	ReasonGrantNotDefined   = "GRANT_NOT_DEFINED"
	ReasonGrantNotPermitted = "GRANT_NOT_PERMITTED"
	ReasonPoolNoProvider    = "POOL_NO_PROVIDER"
)

// Outcome is the reason and category an attempt ends with, and whether
// Temporal may retry it.
type Outcome struct {
	Reason    string
	Category  string
	Retryable bool
}

// MaxValueBytes bounds one provider value: a secret, or one file of a
// bundle. Over it the attempt fails SECRET_TOO_LARGE.
const MaxValueBytes = 1 << 20

// Call is which provider call an answer came from: the table of §11 tells a
// login from a read of a secret, since a refused login is the grant's and a
// refused read the secret's.
type Call int

const (
	// CallLogin is Provider.Login.
	CallLogin Call = iota
	// CallSecret is Session.Secret.
	CallSecret
	// CallCredential is Session.Credential or Session.Renew.
	CallCredential
)

// Classify is the one table from a provider's class, the step and the call
// to the reason an attempt fails with (providers.md §11). A provider never
// picks a reason.
//
// A cancellation is not classified here: the caller ends the attempt
// Cancelled before it asks, so a context error that reaches this table is a
// timeout of the call and is Unavailable like any other.
func Classify(class Class, where Where, call Call) Outcome {
	infra := func(reason string) Outcome {
		return Outcome{Reason: reason, Category: task.CategoryInfrastructureError, Retryable: true}
	}
	contract := func(reason string) Outcome {
		return Outcome{Reason: reason, Category: task.CategoryContractViolation}
	}
	switch class {
	case Refused:
		if call == CallSecret {
			return contract(ReasonSecretAccessDenied)
		}
		return contract(ReasonCredentialRefused)
	case NotFound:
		return contract(ReasonSecretNotFound)
	case Invalid:
		return contract(ReasonProviderRequestInvalid)
	default:
		if where == AtRefresh {
			return infra(ReasonCredentialRefreshFailed)
		}
		return infra(ReasonProviderUnavailable)
	}
}

// Fixed outcomes that do not come from a provider's answer.
var (
	// OutcomeTooLarge is a provider value over MaxValueBytes.
	OutcomeTooLarge = Outcome{Reason: ReasonSecretTooLarge, Category: task.CategoryContractViolation}
	// OutcomeMaskCapacity is the task's values, at their steady state, over
	// the masker's bound at setup: the task declares too much, and it is told
	// before it runs.
	OutcomeMaskCapacity = Outcome{Reason: ReasonMaskCapacityExceeded, Category: task.CategoryContractViolation}
	// OutcomeNotConfigured is a provider this Orker does not declare. It is
	// not retried, as IDENTITY_NOT_CONFIGURED is not: the same Orker would
	// refuse again, and the pool's Orkers are meant to agree (AGENTS.md §5).
	OutcomeNotConfigured = Outcome{Reason: ReasonProviderNotConfigured, Category: task.CategoryInfrastructureError}
	// OutcomeContractViolation is a provider image that broke its contract,
	// at setup. At a refresh a violation never ends a task by itself.
	OutcomeContractViolation = Outcome{Reason: ReasonProviderContractViolation, Category: task.CategoryInfrastructureError}
	// OutcomeRefreshFailed is a credential that expired because its refresh
	// failed: a write fault, a masker that would not take its values, a
	// provider that did not answer until expiry, or an unusable answer.
	OutcomeRefreshFailed = Outcome{Reason: ReasonCredentialRefreshFailed, Category: task.CategoryInfrastructureError, Retryable: true}
)
