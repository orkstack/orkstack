package sourcehost

import (
	"errors"
	"regexp"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// ConfirmEvery is how often a running instance is confirmed.
	ConfirmEvery = 5 * time.Minute
	// judgmentWaitAfter is how long after the last confirmation that accepted
	// an instance takes nothing more.
	judgmentWaitAfter = 10 * time.Minute
	// unansweredMax is how long an engine that answers nothing keeps an
	// instance.
	unansweredMax = 3 * ConfirmEvery
	// offInARow confirmations every replica refused for a guard, over at least
	// offSpan, end an instance.
	offInARow = 3
	offSpan   = 10 * time.Minute
)

// Judgment is how long an attempt of an instance goes on without the engine's
// word that it may: one clock, from the last confirmation that accepted, kept by
// the Orker for the attempt. One goroutine uses it.
type Judgment struct {
	// Started is when the attempt was taken.
	Started time.Time
	// WaitMax bounds the wait for a first confirmation, and UnavailableMax a
	// running instance's since its last.
	WaitMax        time.Duration
	UnavailableMax time.Duration
	// HeldSince is when the activity's earlier attempts began to wait for a
	// judgment; none if they did not.
	HeldSince time.Time

	accepted, answered time.Time
	held               string
	unanswered, denied bool
	stop               string
	off                int
	offSince           time.Time
}

// JudgmentEnd is how the judgment ends an attempt.
type JudgmentEnd struct {
	Reason string
	// Stopped: the engine's word, and the activity completes.
	Stopped bool
	// Unread is spec or record, the source's own object nobody could read, and
	// Held what the engine last waited on.
	Unread string
	Held   string
}

// Observe takes one confirmation's answer, or its error.
func (j *Judgment) Observe(now time.Time, c Confirmed, err error) {
	j.denied = errors.Is(err, ErrHostNotPermitted)
	j.unanswered = err != nil && !j.denied
	if refused, ok := errors.AsType[*RefusedError](err); ok && isGuard(refused.Reason) {
		if j.off++; j.off == 1 {
			j.offSince = now
		}
		// Every replica reached has answered, if not for the instance: what
		// bounds that is the rule of three.
		j.answered = now
		return
	}
	j.off = 0
	if err != nil {
		return
	}
	j.answered = now
	switch c.Kind {
	case ConfirmAccepted:
		j.accepted, j.held, j.HeldSince = now, "", time.Time{}
	case ConfirmWait:
		j.held = heldWord(c.Reason)
		if j.HeldSince.IsZero() || j.HeldSince.After(now) {
			j.HeldSince = now
		}
	case ConfirmStop:
		j.stop = stopReason(c.Reason)
	}
}

// Accepted reports that a confirmation has accepted the attempt.
func (j *Judgment) Accepted() bool { return !j.accepted.IsZero() }

// AcceptedAt is the last that did.
func (j *Judgment) AcceptedAt() time.Time { return j.accepted }

// Waiting reports that the instance takes, acknowledges and starts nothing more
// until a confirmation accepts it.
func (j *Judgment) Waiting(now time.Time) bool {
	since := j.Started
	if j.Accepted() {
		since = j.accepted
	}
	return now.Sub(since) >= judgmentWaitAfter
}

// Held is what the judgment waits on and since when, "" when on nothing.
func (j *Judgment) Held() (string, time.Time) {
	if j.held == "" {
		return "", time.Time{}
	}
	return j.held, j.HeldSince
}

// End is how the attempt ends now, where it does.
func (j *Judgment) End(now time.Time) (JudgmentEnd, bool) {
	switch {
	case j.denied:
		return JudgmentEnd{Reason: eventsources.ReasonHostNotPermitted}, true
	case j.stop != "":
		return JudgmentEnd{Reason: j.stop, Stopped: true}, true
	case j.off >= offInARow && now.Sub(j.offSince) >= offSpan:
		return JudgmentEnd{Reason: eventsources.ReasonDisabled, Stopped: true}, true
	}
	since, most := j.Started, j.WaitMax
	if j.Accepted() {
		since, most = j.accepted, j.UnavailableMax
		last := j.accepted
		if j.answered.After(last) {
			last = j.answered
		}
		if now.Sub(last) >= unansweredMax {
			return JudgmentEnd{Reason: eventsources.ReasonEngineUnavailable}, true
		}
	}
	if now.Sub(since) < most {
		return JudgmentEnd{}, false
	}
	if j.unanswered || j.answered.IsZero() {
		return JudgmentEnd{Reason: eventsources.ReasonEngineUnavailable}, true
	}
	return JudgmentEnd{Reason: eventsources.ReasonIdentityUnavailable, Unread: unreadOwn(j.held), Held: j.held}, true
}

// stopReason is what the engine stopped an instance for, where that is one of the
// reasons it has.
func stopReason(reason string) string {
	if e, ok := eventsources.LookupInstanceEnd(reason); ok && e.Completes == orkstackv1.InstanceEnd_INSTANCE_END_STOPPED {
		return reason
	}
	return eventsources.ReasonNotHeld
}

func isGuard(reason string) bool {
	return reason == eventsources.ReasonDisabled || reason == eventsources.ReasonRequireAuthentication || reason == eventsources.ReasonRequireVersioning
}

var heldShape = regexp.MustCompile(`\A(held=[a-z]+(-[a-z]+)*|unread=(spec|record|policy|access))\z`)

// heldWord is what a wait says it waits on, where that is one of the words it
// has.
func heldWord(reason string) string {
	if len(reason) > 64 || !heldShape.MatchString(reason) {
		return ""
	}
	return reason
}

// unreadOwn is the source's own object a wait says was not read, "" for one that
// is the deployment's.
func unreadOwn(held string) string {
	if what, ok := strings.CutPrefix(held, "unread="); ok && (what == "spec" || what == "record") {
		return what
	}
	return ""
}
