package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Signature headers and the algorithm tag. The scheme is the one GitHub uses:
// the signature covers the raw request body and nothing else, so a receiver
// verifies it by hashing the exact bytes it read, with no canonicalization
// step to get wrong. Headers vary between attempts (X-Ork-Attempt) and are
// deliberately outside the signed material, which keeps one delivery's
// signature identical across every retry.
const (
	// SignatureHeader carries the HMAC of the request body.
	SignatureHeader = "X-Ork-Signature"
	// SignaturePrefix tags the digest algorithm inside SignatureHeader.
	SignaturePrefix = "sha256="

	// EventHeader names the transition that produced the delivery.
	EventHeader = "X-Ork-Event"
	// DeliveryHeader is the unique id of this delivery attempt series.
	DeliveryHeader = "X-Ork-Delivery"
	// WebhookHeader is the id of the subscription being delivered to.
	WebhookHeader = "X-Ork-Webhook-Id"
	// RunHeader is the id of the run that transitioned.
	RunHeader = "X-Ork-Run-Id"
	// TaskHeader names the task of a task outcome delivery. Absent on a run
	// transition. Not signed, like every other header.
	TaskHeader = "X-Ork-Task-Id"
	// TimestampHeader is when the transition happened, RFC3339 in UTC.
	TimestampHeader = "X-Ork-Timestamp"
	// AttemptHeader counts delivery attempts from 1. It is not signed.
	AttemptHeader = "X-Ork-Attempt"

	// UserAgent identifies the sender to receivers and proxies.
	UserAgent = "orkstack-webhooks/1"
)

// Sign returns the HMAC-SHA256 of body under secret, as "sha256=" + lowercase
// hex. An empty secret yields an empty signature: the header is then omitted
// rather than sent with a forgeable value.
func Sign(secret string, body []byte) string {
	if secret == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return SignaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify reports whether signature is the signature of body under secret. It
// is what a receiver calls, so it compares in constant time and accepts the
// header with or without its algorithm prefix. An empty secret or an empty
// signature never verifies.
func Verify(secret string, body []byte, signature string) bool {
	if secret == "" || signature == "" {
		return false
	}
	want := Sign(secret, body)
	got := signature
	if !strings.HasPrefix(got, SignaturePrefix) {
		got = SignaturePrefix + got
	}
	return hmac.Equal([]byte(want), []byte(got))
}
