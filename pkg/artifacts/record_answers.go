package artifacts

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// RecordAnswer is what a read of an identity record answers when the store
// answered (docs/TASK_IDENTITY.md). A store that did not answer is an
// error, *RecordStoreUnanswered, never an answer.
type RecordAnswer int

const (
	// RecordFound: the record decodes, names exactly what was asked, and
	// every object it names is present and hashes to its digest.
	RecordFound RecordAnswer = iota + 1
	// RecordNone: the store said "no such key" for the record, as many
	// times as the read believes it.
	RecordNone
	// RecordInvalid: an object is there and cannot be trusted; Object and
	// Reason say which and why. Never read as none.
	RecordInvalid
	// RecordUnsupported: an object decodes and holds a format version this
	// build does not know; Object and Version say which.
	RecordUnsupported
)

func (a RecordAnswer) String() string {
	switch a {
	case RecordFound:
		return "found"
	case RecordNone:
		return "none"
	case RecordInvalid:
		return "invalid"
	case RecordUnsupported:
		return "unsupported"
	}
	return fmt.Sprintf("answer(%d)", int(a))
}

// RecordRead is one read's answer and what it found.
type RecordRead[T any] struct {
	Answer RecordAnswer
	Value  T
	// Object names the object an invalid or unsupported answer is about
	// (record, detail, values, link); Reason says why it is invalid.
	Object string
	Reason string
	// Version is the unknown format an unsupported answer read.
	Version int64
}

func invalidRead[T any](object, format string, args ...any) RecordRead[T] {
	return RecordRead[T]{Answer: RecordInvalid, Object: object, Reason: fmt.Sprintf(format, args...)}
}

// The classes of a store that did not answer a record's read.
const (
	UnansweredUnavailable = "unavailable"
	UnansweredDenied      = "denied"
	UnansweredTimeout     = "timeout"
	UnansweredBusy        = "busy"
)

// RecordStoreUnanswered is a read or write of an identity record the store
// did not answer (RECORD_STORE_UNANSWERED), with its class: denied (the
// store refused the credential, or has no such bucket), unavailable
// (anything else that is not an answer), timeout (nothing answered within
// the attempt) or busy (the replica had no slot for the request). It is
// retryable.
type RecordStoreUnanswered struct {
	Class string
	Err   error
}

func (e *RecordStoreUnanswered) Error() string {
	return fmt.Sprintf("RECORD_STORE_UNANSWERED (%s): %v", e.Class, e.Err)
}

func (e *RecordStoreUnanswered) Unwrap() error { return e.Err }

// unanswered classes err, which is not an answer. A cancellation is the
// caller's, returned as it is by the reads that hold its context; one the
// store or a transport made is unavailable.
func unanswered(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &RecordStoreUnanswered{Class: UnansweredTimeout, Err: err}
	}
	return &RecordStoreUnanswered{Class: storeErrorClass(err), Err: err}
}

// unansweredIn is unanswered for a request made under ctx: the caller's
// own cancellation is returned as context.Canceled.
func unansweredIn(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	return unanswered(err)
}

// UnansweredClass is the class of err, a step that was not answered: the one
// the records' reader gave it, or the one it has of itself.
func UnansweredClass(err error) string {
	var known *RecordStoreUnanswered
	if errors.As(err, &known) {
		return known.Class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return UnansweredTimeout
	}
	return storeErrorClass(err)
}

// storeErrorClass says denied for an answer that refuses the credential
// or names no bucket, and unavailable otherwise.
func storeErrorClass(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket", "AllAccessDisabled", "InvalidToken", "ExpiredToken":
			return UnansweredDenied
		}
	}
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		if s := response.HTTPStatusCode(); s == http.StatusUnauthorized || s == http.StatusForbidden {
			return UnansweredDenied
		}
	}
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalid) {
		return UnansweredDenied
	}
	return UnansweredUnavailable
}
