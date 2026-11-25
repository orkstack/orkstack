package sourcehost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// lookEvery is how often a lane looks for the container's step when no
// notification arrives (14 §4.3): at most once in 50 ms.
const lookEvery = 50 * time.Millisecond

// Steps reads the container's files: the answer mount's handle.
type Steps interface {
	ReadStep(name string, bound int64) ([]byte, error)
}

// Asks writes the Orker's files under ask/.
type Asks interface {
	WriteJSON(name string, v any) error
}

// Recorder writes one delivery's receipt before its verdict. A required
// receipt that cannot be written holds the verdict (within the bounds the
// instance keeps); any other is best-effort.
type Recorder interface {
	Record(ctx context.Context, msg HandedMessage, seq int64, r Receipt) error
}

// LaneLimits are the deadlines a lane keeps, from the service spec.
type LaneLimits struct {
	PollWait     time.Duration
	Answer       time.Duration
	Settle       time.Duration
	Hold         time.Duration
	LeaseSeconds int
	MaxBodyBytes int64
}

// LaneDriver runs one lane's exchanges (06 §4) over the files of one
// service. One goroutine owns one driver.
type LaneDriver struct {
	Index    int
	Lane     servicehost.Lane
	Steps    Steps
	Asks     Asks
	Judge    *Judge
	Recorder Recorder
	Limits   LaneLimits
	Now      func() time.Time
	// Failed is told of every failure the driver does not return: a
	// receipt that is not required and could not be written. It counts and
	// logs them; nil drops them.
	Failed func(kind string, err error)

	// polled is when the current exchange's poll was written.
	polled time.Time
}

func (d *LaneDriver) failed(kind string, err error) {
	if d.Failed != nil {
		d.Failed(kind, err)
	}
}

// ExchangeKind is how an exchange ended.
type ExchangeKind int

const (
	// ExchangeQuiet: empty.json without an error.
	ExchangeQuiet ExchangeKind = iota + 1
	// ExchangeReceiveFailed: empty.json with an error.
	ExchangeReceiveFailed
	// ExchangeSettled: a verdict was given and settled.json came back.
	ExchangeSettled
	// ExchangeHeld: the message's disposition is a wait; it stays in hand,
	// leased, until the instance gives it a verdict (Release).
	ExchangeHeld
	// ExchangeHoldExpired: the container released the message itself at its
	// hold bound, so whatever verdict was decided for it was not applied.
	ExchangeHoldExpired
)

// Exchange is one exchange's end.
type Exchange struct {
	Kind     ExchangeKind
	Message  HandedMessage
	Body     []byte
	Decision Decision
	Error    *StepError
	Settled  Settled
}

// ErrUnresponsive is a lane whose three exchanges in a row had a late step,
// or whose wait for a step outlived the attempt's patience.
var ErrUnresponsive = errors.New(eventsources.ReasonServiceUnresponsive)

// Poll runs one exchange from its poll: the poll, the container's answer,
// and, for a message, its judgement, receipt, verdict and settlement. A
// held message (a wait) returns ExchangeHeld with the exchange still open;
// the instance later calls Release or Verdict on it. An error ends the
// attempt: a contract violation, ErrUnresponsive, or ctx's end.
func (d *LaneDriver) Poll(ctx context.Context, waitSeconds int) (Exchange, error) {
	seq, ok := d.Lane.Begin()
	if !ok {
		return Exchange{}, fmt.Errorf("lane %d: an exchange is open", d.Index)
	}
	d.polled = d.Now()
	if err := d.Asks.WriteJSON(LanePath(d.Index, StepPoll), Poll{SchemaVersion: 1, Seq: seq, WaitSeconds: waitSeconds,
		LeaseSeconds: d.Limits.LeaseSeconds}); err != nil {
		d.Lane.Close()
		return Exchange{}, fmt.Errorf("%w: %v", ErrAskUnwritable, err)
	}
	d.Lane.Await(d.Now(), time.Duration(waitSeconds)*time.Second+d.Limits.Answer-d.Limits.PollWait)
	for {
		if raw, err := d.read(StepEmpty, MaxSmallStepBytes); err != nil {
			return Exchange{}, err
		} else if raw != nil {
			e, err := ReadEmpty(raw)
			if err != nil {
				return Exchange{}, err
			}
			done, err := d.arrived(e.Seq)
			if err != nil {
				return Exchange{}, err
			}
			if done {
				d.Lane.Close()
				if e.Error != nil {
					return Exchange{Kind: ExchangeReceiveFailed, Error: e.Error}, nil
				}
				return Exchange{Kind: ExchangeQuiet}, nil
			}
		}
		if raw, err := d.read(StepMessage, MaxMessageStepBytes); err != nil {
			return Exchange{}, err
		} else if raw != nil {
			msg, err := ReadMessage(raw)
			if err != nil {
				return Exchange{}, err
			}
			done, err := d.arrived(msg.Seq)
			if err != nil {
				return Exchange{}, err
			}
			if done {
				return d.judge(ctx, msg)
			}
		}
		if err := d.tick(ctx); err != nil {
			return Exchange{}, err
		}
	}
}

// arrived takes a step of seq: true for the current exchange's, false for a
// leftover, an error for one nobody asked for.
func (d *LaneDriver) arrived(seq int64) (bool, error) {
	switch d.Lane.Arrived(seq) {
	case servicehost.ArrivalCurrent:
		return true, nil
	case servicehost.ArrivalLeftover:
		return false, nil
	}
	return false, &servicehost.ViolationError{Name: LanePath(d.Index, "*"), Reason: "a step nobody asked for"}
}

// judge reads a message's body, decides it, and gives its verdict unless
// it is held.
func (d *LaneDriver) judge(ctx context.Context, msg HandedMessage) (Exchange, error) {
	var body []byte
	if !msg.Oversize {
		b, err := d.Steps.ReadStep(LanePath(d.Index, StepBody), d.Limits.MaxBodyBytes)
		if err != nil {
			if errors.Is(err, servicehost.ErrNotYet) {
				return Exchange{}, &servicehost.ViolationError{Name: LanePath(d.Index, StepBody), Reason: "message.json without its body"}
			}
			return Exchange{}, err
		}
		body = b
	}
	decision, err := d.Judge.Decide(ctx, msg, body)
	if errors.Is(err, ErrBodyMismatch) {
		return Exchange{}, &servicehost.ViolationError{Name: LanePath(d.Index, StepBody), Reason: err.Error()}
	}
	if err != nil {
		return Exchange{}, err
	}
	if decision.Err != nil {
		d.failed("wait:"+decision.Cause, decision.Err)
	}
	if decision.Disposition == Wait {
		return Exchange{Kind: ExchangeHeld, Message: msg, Body: body, Decision: decision}, nil
	}
	ex, err := d.Verdict(ctx, msg, decision)
	if ex.Kind == ExchangeHeld {
		// A verdict that could not be given yet (its confirmation aged, or
		// its required receipt was refused): the message is held with its
		// body, to be judged again.
		ex.Body = body
	}
	return ex, err
}

// Verdict writes the receipt, then the verdict, and waits for settled.json.
// A required receipt that cannot be written is returned as an error and no
// verdict is given: the instance holds the message within its bound.
func (d *LaneDriver) Verdict(ctx context.Context, msg HandedMessage, decision Decision) (Exchange, error) {
	if decision.Receipt.Outcome == artifacts.ReceiptFiltered && !d.Judge.StillConfirmed(ctx, decision) {
		// The confirmation aged while the decision waited: a FILTERED
		// acknowledgement, which cannot be taken back, is never written on
		// it. The message is held and judged again.
		return Exchange{Kind: ExchangeHeld, Message: msg, Decision: Decision{Disposition: Wait, Cause: "confirm"}}, nil
	}
	if closed, err := d.closed(); err != nil {
		return Exchange{}, err
	} else if closed {
		// The container's own close at the hold bound came first: no verdict
		// is written, and the receipt says the message was given back.
		d.recordClose(ctx, msg, decision.Receipt, false)
		d.Lane.Close()
		return Exchange{Kind: ExchangeHoldExpired, Message: msg, Decision: decision}, nil
	}
	if decision.Receipt.Outcome != "" {
		if err := d.Recorder.Record(ctx, msg, d.Lane.Seq(), decision.Receipt); err != nil {
			if decision.Receipt.Required {
				return Exchange{Kind: ExchangeHeld, Message: msg, Decision: decision}, fmt.Errorf("%w: %v", ErrReceiptRequired, err)
			}
			d.failed("receipt", err)
		}
	}
	id := newVerdictID(d.Lane.Seq())
	if err := d.Asks.WriteJSON(LanePath(d.Index, StepVerdict), Verdict{SchemaVersion: 1, Seq: d.Lane.Seq(), VerdictID: id,
		Action: decision.Action, DelaySeconds: decision.Delay}); err != nil {
		return Exchange{}, fmt.Errorf("%w: %v", ErrAskUnwritable, err)
	}
	d.Lane.Await(d.Now(), d.Limits.Settle)
	for {
		s, err := d.readSettled()
		if err != nil {
			return Exchange{}, err
		}
		if s != nil {
			closed, err := d.settles(*s, id)
			if err != nil {
				return Exchange{}, err
			}
			d.Lane.Arrived(s.Seq)
			d.Lane.Close()
			if closed {
				// The container closed at its hold bound as the verdict was
				// written, and both sides ignore that verdict.
				d.recordClose(ctx, msg, decision.Receipt, true)
				return Exchange{Kind: ExchangeHoldExpired, Message: msg, Decision: decision}, nil
			}
			return Exchange{Kind: ExchangeSettled, Message: msg, Decision: decision, Settled: *s}, nil
		}
		if err := d.tick(ctx); err != nil {
			return Exchange{}, err
		}
	}
}

// newVerdictID names a verdict: its lane's exchange and 128 random bits, so
// a settle carrying it back answers exactly this one, and no image can make
// one up without reading the verdict.
func newVerdictID(seq int64) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%d-%s", seq, hex.EncodeToString(b[:]))
}

// readSettled reads this exchange's settle: nil while there is none (no
// file, or a leftover of an earlier exchange), a violation for one of an
// exchange nobody began.
func (d *LaneDriver) readSettled() (*Settled, error) {
	raw, err := d.read(StepSettled, MaxSmallStepBytes)
	if err != nil || raw == nil {
		return nil, err
	}
	s, err := ReadSettled(raw)
	if err != nil {
		return nil, err
	}
	switch {
	case s.Seq < d.Lane.Seq():
		return nil, nil
	case s.Seq > d.Lane.Seq():
		return nil, d.settleViolation("a step nobody asked for")
	}
	return &s, nil
}

// settles judges this exchange's settle against the verdict given, "" while
// none was: it names that verdict, or it is the container's own close at its
// hold bound (closed), which names none. Anything else is a settle nobody
// asked for, whenever it lands, and nothing is recorded as its outcome.
//
// A close answers no verdict, so its time is what is checked: one before the
// hold bound is a settle without a verdict under another name.
func (d *LaneDriver) settles(s Settled, id string) (closed bool, err error) {
	expired := s.Error != nil && s.Error.Kind == KindHoldExpired
	switch {
	case expired && s.VerdictID != "":
		return false, d.settleViolation("closed at the hold bound (" + KindHoldExpired + "), yet it names a verdict")
	case expired && d.age() < d.Limits.Hold-holdSlack:
		return false, d.settleViolation("closed (" + KindHoldExpired + ") before its hold bound")
	case expired:
		return true, nil
	case id == "":
		return false, d.settleViolation("settled without a verdict")
	case s.VerdictID != id:
		return false, d.settleViolation("settled, but not answering this lane's verdict (verdictId)")
	}
	return false, nil
}

func (d *LaneDriver) settleViolation(reason string) error {
	return &servicehost.ViolationError{Name: LanePath(d.Index, StepSettled), Reason: reason}
}

// holdSlack is how early a container's clock may put its hold bound: one
// that counts in whole seconds (date +%s) reaches it up to a second early.
const holdSlack = 2 * time.Second

// age is how long ago this exchange's poll was written, by whichever clock
// says longer: a container may count on the wall clock or on the monotonic
// one, and a suspended host moves only the first. The container counts from
// its hand-over, which comes after the poll, so an exchange younger than the
// hold bound is one no container could have held that long.
func (d *LaneDriver) age() time.Duration {
	now := d.Now()
	return max(now.Sub(d.polled), now.Round(0).Sub(d.polled.Round(0)))
}

// closed looks for a settle of this exchange while no verdict of it has been
// written: true for the container's own close at its hold bound, a
// violation for any other, an image that acknowledged on its own.
func (d *LaneDriver) closed() (bool, error) {
	s, err := d.readSettled()
	if err != nil || s == nil {
		return false, err
	}
	return d.settles(*s, "")
}

// recordClose writes the receipt of a message its container gave back at the
// hold bound. An accepted message (or its duplicate) keeps its own: its run
// exists, whatever became of the delivery. written says the decided receipt
// is already there. The exchange is over, so a receipt that cannot be
// written is told and holds nothing.
func (d *LaneDriver) recordClose(ctx context.Context, msg HandedMessage, decided Receipt, written bool) {
	r := Receipt{Outcome: artifacts.ReceiptReleased, Reason: eventsources.ReasonMessageReleasedHoldExpired}
	if d.Judge.AtMostOnce {
		// Given back, at most once, is forgotten.
		r.Outcome = artifacts.ReceiptDropped
	}
	if decided.Outcome == artifacts.ReceiptAccepted || decided.Outcome == artifacts.ReceiptDuplicate {
		r = decided
	}
	if written && r.Outcome == decided.Outcome {
		return
	}
	if err := d.Recorder.Record(ctx, msg, d.Lane.Seq(), r); err != nil {
		d.failed("receipt", err)
	}
}

// ErrAskUnwritable is a step the Orker could not write under ask/: its own
// filesystem failed, and the attempt ends.
var ErrAskUnwritable = errors.New("a step could not be written under ask/")

// ErrReceiptRequired is a required receipt that could not be written: the
// verdict waits.
var ErrReceiptRequired = errors.New("a required receipt could not be written")

// HoldExpired reports whether the container closed the held exchange
// itself at its hold bound, and writes the release's receipt.
func (d *LaneDriver) HoldExpired(ctx context.Context, msg HandedMessage) (bool, error) {
	closed, err := d.closed()
	if err != nil || !closed {
		return false, err
	}
	d.recordClose(ctx, msg, Receipt{}, false)
	d.Lane.Close()
	return true, nil
}

// read reads a step of this lane: nil when it is not there yet.
func (d *LaneDriver) read(step string, bound int64) ([]byte, error) {
	raw, err := d.Steps.ReadStep(LanePath(d.Index, step), bound)
	if errors.Is(err, servicehost.ErrNotYet) {
		return nil, nil
	}
	return raw, err
}

// tick waits one look and counts a missed deadline. A lane wedged for
// longer than the profile's longest hold ends the attempt by itself; the
// instance's watchdog judges the lanes together (servicehost.Unresponsive).
func (d *LaneDriver) tick(ctx context.Context) error {
	if d.Lane.Tick(d.Now()) {
		return fmt.Errorf("%w: lane %d had three late exchanges in a row", ErrUnresponsive, d.Index)
	}
	if wedged, since := d.Lane.Wedged(); wedged && d.Limits.Hold > 0 && d.Now().Sub(since) > d.Limits.Hold {
		return fmt.Errorf("%w: lane %d waited past the longest hold", ErrUnresponsive, d.Index)
	}
	t := time.NewTimer(lookEvery)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
