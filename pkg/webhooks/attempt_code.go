package webhooks

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
)

// An attempt that failed before the receiver answered is recorded as one of
// these codes and the sentence beside it, never as the transport's own error:
// net/http's error quotes the URL, query and all, and a webhook URL often
// carries a credential. An engine's attempt record travels through the
// execution cluster (its result, its heartbeat and its failure), so it holds
// a code and OrkStack's words only; Attempt.Cause keeps the transport's error,
// without the URL, for the process that made the request to log.
const (
	// AttemptCodeDestinationBlocked: the destination policy refused the
	// address (ReasonDestinationBlocked). Never retried.
	AttemptCodeDestinationBlocked = "destination_blocked"
	// AttemptCodeDNS: the receiver's host name did not resolve.
	AttemptCodeDNS = "dns"
	// AttemptCodeConnect: no connection to the receiver could be made.
	AttemptCodeConnect = "connect"
	// AttemptCodeTLS: the TLS handshake with the receiver failed.
	AttemptCodeTLS = "tls"
	// AttemptCodeTimeout: the request did not finish in time.
	AttemptCodeTimeout = "timeout"
	// AttemptCodeRequest: the request could not be built from the URL.
	AttemptCodeRequest = "request"
	// AttemptCodeTransport: the request failed before a response arrived,
	// for any other reason.
	AttemptCodeTransport = "transport"

	// The codes below are of attempts that made no request; the engine
	// records them (internal/engine) with its own sentence.

	// AttemptCodeBodyLost: the delivery's stored body is gone or altered.
	AttemptCodeBodyLost = "body_lost"
	// AttemptCodeStoreUnavailable: the stored body could not be read.
	AttemptCodeStoreUnavailable = "store_unavailable"
	// AttemptCodeSubscriptionUnavailable: the subscription could not be read.
	AttemptCodeSubscriptionUnavailable = "subscription_unavailable"
	// AttemptCodeSubscriptionDeleted: the subscription was deleted.
	AttemptCodeSubscriptionDeleted = "subscription_deleted"
	// AttemptCodeUnreported: the attempt ended without reporting its outcome.
	AttemptCodeUnreported = "unreported"
)

// attemptSentences is the one sentence each code is recorded with.
var attemptSentences = map[string]string{
	AttemptCodeDestinationBlocked: ReasonDestinationBlocked + ": the destination is not a public address and the operator's allow-list does not admit it",
	AttemptCodeDNS:                "the receiver's host name could not be resolved",
	AttemptCodeConnect:            "no connection to the receiver could be made",
	AttemptCodeTLS:                "the TLS handshake with the receiver failed",
	AttemptCodeTimeout:            "the request did not finish in time",
	AttemptCodeRequest:            "no request could be built from the subscription's URL",
	AttemptCodeTransport:          "the request failed before the receiver answered",
}

// AttemptSentence is the sentence an attempt with code is recorded with.
func AttemptSentence(code string) string {
	if sentence, ok := attemptSentences[code]; ok {
		return sentence
	}
	return attemptSentences[AttemptCodeTransport]
}

// classifyTransport is the code of a request that failed before a response.
func classifyTransport(err error) string {
	var (
		dnsErr      *net.DNSError
		netErr      net.Error
		opErr       *net.OpError
		unknownCA   x509.UnknownAuthorityError
		hostname    x509.HostnameError
		invalid     x509.CertificateInvalidError
		verify      *tls.CertificateVerificationError
		recordError tls.RecordHeaderError
		alert       tls.AlertError
	)
	var built *requestError
	switch {
	case errors.As(err, &built):
		return AttemptCodeRequest
	case errors.Is(err, ErrDestinationBlocked):
		return AttemptCodeDestinationBlocked
	case errors.As(err, &dnsErr):
		return AttemptCodeDNS
	case errors.As(err, &verify), errors.As(err, &unknownCA), errors.As(err, &hostname),
		errors.As(err, &invalid), errors.As(err, &recordError), errors.As(err, &alert):
		return AttemptCodeTLS
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return AttemptCodeTimeout
	case errors.As(err, &opErr) && opErr.Op == "dial":
		return AttemptCodeConnect
	}
	return AttemptCodeTransport
}

// withoutURL is err with net/http's URL wrapper taken off, so what is left
// names the operation and the address, never the URL.
func withoutURL(err error) error {
	var wrapped *url.Error
	if errors.As(err, &wrapped) && wrapped.Err != nil {
		return wrapped.Err
	}
	return err
}

// requestError is a request that could not be built from the URL.
type requestError struct{ err error }

func (e *requestError) Error() string { return e.err.Error() }
func (e *requestError) Unwrap() error { return e.err }
