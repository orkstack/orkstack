package sourcehost

import (
	"context"
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
	// hold bound.
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
	if err := d.settledEarly(); err != nil {
		return Exchange{}, err
	}
	if decision.Receipt.Outcome != "" {
		if err := d.Recorder.Record(ctx, msg, d.Lane.Seq(), decision.Receipt); err != nil {
			if decision.Receipt.Required {
				return Exchange{Kind: ExchangeHeld, Message: msg, Decision: decision}, fmt.Errorf("%w: %v", ErrReceiptRequired, err)
			}
			d.failed("receipt", err)
		}
	}
	if err := d.Asks.WriteJSON(LanePath(d.Index, StepVerdict), Verdict{SchemaVersion: 1, Seq: d.Lane.Seq(), Action: decision.Action,
		DelaySeconds: decision.Delay}); err != nil {
		return Exchange{}, fmt.Errorf("%w: %v", ErrAskUnwritable, err)
	}
	d.Lane.Await(d.Now(), d.Limits.Settle)
	for {
		if raw, err := d.read(StepSettled, MaxSmallStepBytes); err != nil {
			return Exchange{}, err
		} else if raw != nil {
			s, err := ReadSettled(raw)
			if err != nil {
				return Exchange{}, err
			}
			done, err := d.arrived(s.Seq)
			if err != nil {
				return Exchange{}, err
			}
			if done {
				d.Lane.Close()
				return Exchange{Kind: ExchangeSettled, Message: msg, Decision: decision, Settled: s}, nil
			}
		}
		if err := d.tick(ctx); err != nil {
			return Exchange{}, err
		}
	}
}

// settledEarly looks, before any receipt or verdict is written, for a
// settle of this exchange the container wrote before it was given one: an
// image that acknowledged on its own. That is a violation, and nothing is
// recorded for it. A close at the hold bound is not one: HoldExpired reads
// it. The Orker sees only what is there when it looks, so a settle written
// after this look and before the verdict is read as the verdict's.
func (d *LaneDriver) settledEarly() error {
	raw, err := d.read(StepSettled, MaxSmallStepBytes)
	if err != nil || raw == nil {
		return err
	}
	s, err := ReadSettled(raw)
	if err != nil {
		return err
	}
	if s.Seq == d.Lane.Seq() && (s.Error == nil || s.Error.Kind != KindHoldExpired) {
		return &servicehost.ViolationError{Name: LanePath(d.Index, StepSettled), Reason: "settled without a verdict"}
	}
	return nil
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
	raw, err := d.read(StepSettled, MaxSmallStepBytes)
	if err != nil || raw == nil {
		return false, err
	}
	s, err := ReadSettled(raw)
	if err != nil {
		return false, err
	}
	switch {
	case s.Seq < d.Lane.Seq():
		return false, nil
	case s.Seq > d.Lane.Seq():
		return false, &servicehost.ViolationError{Name: LanePath(d.Index, StepSettled), Reason: "a step nobody asked for"}
	case s.Error == nil || s.Error.Kind != KindHoldExpired:
		return false, &servicehost.ViolationError{Name: LanePath(d.Index, StepSettled), Reason: "settled without a verdict"}
	}
	if err := d.Recorder.Record(ctx, msg, s.Seq, Receipt{Outcome: artifacts.ReceiptReleased, Reason: eventsources.ReasonMessageReleasedHoldExpired}); err != nil {
		d.failed("receipt", err)
	}
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
