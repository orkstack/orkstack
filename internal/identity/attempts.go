package identity

import (
	"fmt"
	"time"
)

// The attempt cap's and the token life's defaults: identity.attempts.* and
// identity.token.* (docs/DEPLOY.md §1).
const (
	DefaultAttemptsExtra    = 0
	DefaultAttemptsMaxExtra = 4
	DefaultAttemptsMax      = 32
	DefaultTokenTTL         = time.Hour
	DefaultTokenMinTTL      = 10 * time.Minute
	DefaultTokenMaxTTL      = 12 * time.Hour
	// MaxTokenTTL bounds identity.token.max_ttl.
	MaxTokenTTL = 24 * time.Hour
	// DefaultCommitWait is identity.results.commit_wait: how long an Orker
	// waits for an engine at a commit, longer than a rolling restart of the
	// engines; MinCommitWait and MaxCommitWait bound it.
	DefaultCommitWait = 15 * time.Minute
	MinCommitWait     = 30 * time.Second
	MaxCommitWait     = time.Hour
)

// identity.issuance.records: whether a granted mint must record what it
// issued before it answers.
const (
	IssuanceRequired = "required"
	IssuanceOff      = "off"
)

// identity.unscoped_orkers: whether a live Orker whose storage credential is
// unscoped or unchecked refuses new runs that use task identity, or is
// reported only.
const (
	UnscopedRefuse = "refuse"
	UnscopedReport = "report"
)

// AttemptsOverLimitError is IDENTITY_ATTEMPTS_OVER_LIMIT: a task whose
// slots, max(1, retry.maximum_attempts) plus extra, exceed
// identity.attempts.max. The caller names the task.
type AttemptsOverLimitError struct {
	Slots, Limit int
}

func (e *AttemptsOverLimitError) Error() string {
	return fmt.Sprintf("IDENTITY_ATTEMPTS_OVER_LIMIT: %d attempts may hold an identity, over identity.attempts.max (%d)", e.Slots, e.Limit)
}

// Slots is a task's identity_slots: max(1, retry.maximum_attempts) plus
// extra, so never below the attempts the retry policy allows. Above limit
// (identity.attempts.max) it answers the number with an
// *AttemptsOverLimitError.
func Slots(maxAttempts, extra, limit int) (int, error) {
	n := max(1, maxAttempts) + max(0, extra)
	if n > limit {
		return n, &AttemptsOverLimitError{Slots: n, Limit: limit}
	}
	return n, nil
}

// SlotExpired says whether a slot taken at takenAt has outlived its
// attempt at now, by the engine's clock: more than the attempt's deadline
// plus auth.clock_skew has passed (IDENTITY_ATTEMPT_EXPIRED). An honest
// attempt never meets it, since the cluster ends it at that deadline.
func SlotExpired(takenAt time.Time, deadline, skew time.Duration, now time.Time) bool {
	return now.Sub(takenAt) > deadline+skew
}

// TTLBounds are identity.token.ttl, min_ttl and max_ttl.
type TTLBounds struct {
	Default, Min, Max time.Duration
}

// Clamp is the life a token is minted with for an identity that asked
// for asked: the deployment's ttl when it asked for none, held between
// min_ttl and max_ttl.
func (b TTLBounds) Clamp(asked time.Duration) time.Duration {
	if asked == 0 {
		asked = b.Default
	}
	return min(max(asked, b.Min), b.Max)
}
