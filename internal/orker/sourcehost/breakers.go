package sourcehost

import "time"

// The breaker codes an Orker counts (01 §7); it asks the engine to pause
// the source with one.
const (
	BreakerRefused          = "EVENT_SOURCE_REFUSED"
	BreakerAllRejected      = "EVENT_SOURCE_ALL_REJECTED"
	BreakerSettleFailing    = "EVENT_SOURCE_SETTLE_FAILING"
	BreakerSettleUnanswered = "EVENT_SOURCE_SETTLE_UNANSWERED"
)

// Breaker thresholds (01 §7, §5).
const (
	settleRefusedInARow  = 5
	uncertainRounds      = 5
	uncertainSpan        = 2 * time.Minute
	unansweredWaits      = 5
	unansweredSpan       = 10 * time.Minute
	DefaultRejectBreaker = 20
)

// Breakers are one instance's own counts toward a pause. They are not safe
// for concurrent use: the instance's loop owns them.
type Breakers struct {
	rejectLimit int
	trial       string

	rejected      map[string]bool
	settleRefused int
	uncertain     []time.Time
	unanswered    []time.Time
}

// NewBreakers counts toward a reject breaker of rejectLimit distinct
// messages (0 disables it). trial is the breaker that paused the source,
// when this is the first attempt of an activity a resume scheduled: that
// breaker then trips at its first event.
func NewBreakers(rejectLimit int, trial string) *Breakers {
	return &Breakers{rejectLimit: rejectLimit, trial: trial, rejected: map[string]bool{}}
}

// OnTrial reports the breaker the instance is on trial for, "" when none.
func (b *Breakers) OnTrial() string { return b.trial }

// Accepted is a message accepted (or found a duplicate): it ends a run of
// rejections and the trial.
func (b *Breakers) Accepted() {
	clear(b.rejected)
	b.trial = ""
}

// Rejected counts a rejected message, once whichever delivery is first
// rejected, and returns the breaker that trips, "" for none.
func (b *Breakers) Rejected(messageID string) string {
	if b.rejectLimit == 0 {
		return ""
	}
	if b.trial == BreakerAllRejected {
		return BreakerAllRejected
	}
	b.rejected[messageID] = true
	if len(b.rejected) >= b.rejectLimit {
		return BreakerAllRejected
	}
	return ""
}

// Settled counts one settlement: refused by the broker, or not.
func (b *Breakers) Settled(refused bool) string {
	if !refused {
		b.settleRefused = 0
		if b.trial == BreakerSettleFailing || b.trial == BreakerSettleUnanswered {
			b.trial = ""
		}
		return ""
	}
	if b.trial == BreakerSettleFailing || b.trial == BreakerSettleUnanswered {
		return b.trial
	}
	b.settleRefused++
	if b.settleRefused >= settleRefusedInARow {
		return BreakerSettleFailing
	}
	return ""
}

// Uncertain counts one failed round of a refusal nobody classified: five
// in a row over at least two minutes ask for a pause.
func (b *Breakers) Uncertain(at time.Time) string {
	b.uncertain = append(b.uncertain, at)
	if len(b.uncertain) > uncertainRounds {
		b.uncertain = b.uncertain[1:]
	}
	if len(b.uncertain) == uncertainRounds && at.Sub(b.uncertain[0]) >= uncertainSpan {
		return BreakerRefused
	}
	return ""
}

// Certain ends a run of uncertain rounds.
func (b *Breakers) Certain() { b.uncertain = b.uncertain[:0] }

// SettleUnanswered counts one broker wait caused by unanswered settlements
// and ended by a passing check: five in a row spanning ten minutes ask for
// a pause.
func (b *Breakers) SettleUnanswered(at time.Time) string {
	if b.trial == BreakerSettleUnanswered {
		return BreakerSettleUnanswered
	}
	b.unanswered = append(b.unanswered, at)
	if len(b.unanswered) > unansweredWaits {
		b.unanswered = b.unanswered[1:]
	}
	if len(b.unanswered) == unansweredWaits && at.Sub(b.unanswered[0]) >= unansweredSpan {
		return BreakerSettleUnanswered
	}
	return ""
}

// SettleAnswered ends a run of unanswered settlements.
func (b *Breakers) SettleAnswered() { b.unanswered = b.unanswered[:0] }
