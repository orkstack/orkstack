package provider

import (
	"context"
	"errors"
	"sync"
	"time"
)

// The pacing defaults (providers.md §5; workload-identity.md §7). The Orker's
// configuration overrides them; these are what the settings default to.
const (
	// DefaultRetryMin is orker.identity.retry_min: the base of the one retry
	// rule.
	DefaultRetryMin = 5 * time.Second
	// RetryMax caps the retry rule's bound.
	RetryMax = 60 * time.Second
	// DefaultCallTimeout is orker.providers.call_timeout: one provider call.
	DefaultCallTimeout = 15 * time.Second
	// DefaultMaxConcurrent is orker.providers.max_concurrent: calls in
	// flight from one Orker to one provider.
	DefaultMaxConcurrent = 8
	// CloseTimeout bounds closing one session at the end of an attempt, on a
	// context detached from the attempt's cancellation.
	CloseTimeout = 10 * time.Second
	// RefusalWindow is how long a refusal at login or issue must stand,
	// over RefusalAnswers identical answers each with a fresh token, before
	// it is reported: it absorbs the edge of a clock difference and most of
	// the time a changed trust policy takes to apply (providers.md §11).
	RefusalWindow = 30 * time.Second
	// RefusalAnswers is how many identical refusals are needed.
	RefusalAnswers = 3
)

// RetryDelay is the one retry rule, here and for identity tokens
// (workload-identity.md §7): the n-th delay after a failure, n from 1, is
// drawn uniformly between half of b and b, where b is retryMin doubled n
// times and capped at RetryMax. With the 5 s default: 5 to 10 s, then 10 to
// 20, 20 to 40, 30 to 60, and 30 to 60 from then on. It is never below
// retryMin. unit is a uniform draw in [0, 1); it is injected so a test can
// pin it.
func RetryDelay(n int, retryMin time.Duration, unit float64) time.Duration {
	if retryMin <= 0 {
		retryMin = DefaultRetryMin
	}
	if n < 1 {
		n = 1
	}
	bound := retryMin
	for i := 0; i < n && bound < RetryMax; i++ {
		bound *= 2
	}
	if bound > RetryMax {
		bound = RetryMax
	}
	unit = clampUnit(unit)
	half := bound / 2
	d := half + time.Duration(float64(bound-half)*unit)
	if d < retryMin {
		d = retryMin
	}
	return d
}

// RefreshAfter is when a refresh of something with this lifetime is due,
// counted from the moment it was received: at a uniform draw between one
// third of the lifetime and a quarter later than that (so between 4/12 and
// 5/12 of it), so the items of a fan-out do not refresh together. Never
// earlier than a third: the masker's bound counts on it
// (secrets-and-masking.md §3.3). A zero lifetime does not expire and is
// never refreshed: ok is false.
func RefreshAfter(lifetime time.Duration, unit float64) (after time.Duration, ok bool) {
	if lifetime <= 0 {
		return 0, false
	}
	third := lifetime / 3
	return third + time.Duration(float64(third/4)*clampUnit(unit)), true
}

func clampUnit(unit float64) float64 {
	if unit != unit || unit < 0 { // NaN or negative
		return 0
	}
	if unit >= 1 {
		return 0.999999
	}
	return unit
}

// Limiter bounds the calls in flight from one Orker to one provider. Waiting
// counts against the caller's budget: Acquire returns the context's error
// once it is done.
type Limiter struct {
	slots chan struct{}
}

// NewLimiter admits at most n calls at once; n below one admits one.
func NewLimiter(n int) *Limiter {
	if n < 1 {
		n = 1
	}
	return &Limiter{slots: make(chan struct{}, n)}
}

// Acquire takes a slot, or returns ctx's error. The returned release gives
// it back and is safe to call more than once.
func (l *Limiter) Acquire(ctx context.Context) (release func(), err error) {
	if err := ctx.Err(); err != nil {
		return func() {}, err
	}
	select {
	case l.slots <- struct{}{}:
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
	var once sync.Once
	return func() { once.Do(func() { <-l.slots }) }, nil
}

// RefusalConfirm decides when a refusal at login or issue is reported. A
// first refusal is not believed: it may be the edge of a clock difference or
// a trust policy that has not applied yet. It is reported once RefusalAnswers
// identical answers, each with a fresh token, span at least RefusalWindow.
// Any other answer starts again. It is one attempt's, for one grant, and not
// safe for concurrent use.
type RefusalConfirm struct {
	code  string
	count int
	first time.Time
}

// Observe records one answer at now. It reports true when the refusal is
// confirmed: then the caller stops asking. A nil error, or an error that is
// not Refused, resets it and reports false.
func (r *RefusalConfirm) Observe(err error, now time.Time) bool {
	var pe *Error
	if err == nil || ClassOf(err) != Refused {
		*r = RefusalConfirm{}
		return false
	}
	code := ""
	if errors.As(err, &pe) {
		code = pe.Code
	}
	if r.count == 0 || code != r.code {
		*r = RefusalConfirm{code: code, count: 1, first: now}
		return false
	}
	r.count++
	return r.count >= RefusalAnswers && now.Sub(r.first) >= RefusalWindow
}

// NextTry is how long to wait before asking again after a refusal that is not
// yet confirmed: the window is spread over the answers it needs.
func (r *RefusalConfirm) NextTry() time.Duration {
	return RefusalWindow / (RefusalAnswers - 1)
}
