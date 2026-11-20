package sourcehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// Bounds of a message's judgement (01 §4.3, §5).
const (
	// confirmFresh is how recent a confirmation must be for a FILTERED
	// verdict, which cannot be taken back.
	confirmFresh = 5 * time.Second
	// maxRefresh is how often one message is judged again on REFRESH; the
	// next is a wait.
	maxRefresh = 3
	// maxUploadAgain is how often one start is answered UPLOAD_AGAIN; the
	// next is a wait.
	maxUploadAgain = 3
	// releaseDelaySeconds is the delay of a release on a pause or a wait.
	releaseDelaySeconds = 30
	// maxRejectDelaySeconds bounds a rejection's delay.
	maxRejectDelaySeconds = 30
)

// Disposition is how one message a lane holds ends (01 §5).
type Disposition int

const (
	// Settle: ACCEPTED, DUPLICATE or FILTERED; receipt, then ack.
	Settle Disposition = iota + 1
	// Reject: the message can never be accepted; receipt, then reject.
	Reject
	// Wait: nothing is wrong with the message or the source; the instance
	// stops polling and parks.
	Wait
	// Pause: the source is paused; release, and the instance ends.
	Pause
)

// Receipt is what the Orker records about one delivery before the verdict
// (08 §3).
type Receipt struct {
	Outcome  artifacts.ReceiptOutcome
	Reason   string
	Cause    string
	RunID    string
	Required bool
	// Sampled: a FILTERED receipt, written at most one per source per
	// instance per ten minutes.
	Sampled bool
}

// Decision is one message's disposition: its verdict (none for a wait,
// whose messages the instance parks) and its receipt.
type Decision struct {
	Disposition Disposition
	Action      string
	Delay       int
	Receipt     Receipt
	// Cause is a wait's cause.
	Cause string
	// Uncertain: a wait the classifier could not place; counted toward
	// asking for a pause.
	Uncertain bool
	// Err is a wait's underlying failure, for the instance to log and
	// count with its cause.
	Err error
	// ConfirmedAt is, for a FILTERED settle, when the judging revision it
	// was judged by was last confirmed: the verdict is held to it again
	// when it is written.
	ConfirmedAt time.Time
}

// Stager stages one part of a message (its body or its metadata document)
// and says what it staged.
type Stager interface {
	Stage(ctx context.Context, msg HandedMessage, part Part, bytes []byte) (StagedPart, error)
}

// Judge is one instance's judgement of its messages: the judging spec it
// holds, its door and its stager. Every lane of the instance shares it; the
// spec, its revision and its last confirmation are held under mu, and a
// confirmation is the source's, so one lane's confirms the revision for all.
type Judge struct {
	Door            Door
	Stager          Stager
	Now             func() time.Time
	MaxMessageBytes int64
	AtMostOnce      bool
	// Ref names the source in its messages' metadata documents.
	Ref SourceRef

	mu          sync.Mutex
	source      *orkstackv1.EventSource
	revision    int64
	confirmedAt time.Time
}

// NewJudge holds the judging spec src at revision.
func NewJudge(src *orkstackv1.EventSource, revision int64, door Door, stager Stager, now func() time.Time) *Judge {
	return &Judge{Door: door, Stager: stager, Now: now, source: src, revision: revision}
}

// Revision is the judging revision the instance holds.
func (j *Judge) Revision() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.revision
}

// Swap takes a new judging spec without a restart.
func (j *Judge) Swap(src *orkstackv1.EventSource, revision int64, confirmedAt time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.source, j.revision, j.confirmedAt = src, revision, confirmedAt
}

// held is the spec, revision and last confirmation the instance holds now.
func (j *Judge) held() (*orkstackv1.EventSource, int64, time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.source, j.revision, j.confirmedAt
}

// StillConfirmed reports whether a FILTERED decision may be written now:
// its revision confirmed within the last five seconds, asking the door
// again when it is older. false means the message is to be judged again,
// or held.
func (j *Judge) StillConfirmed(ctx context.Context, d Decision) bool {
	if j.Now().Sub(d.ConfirmedAt) <= confirmFresh {
		return true
	}
	fresh, _, ok := j.confirmed(ctx)
	return ok && fresh
}

// ErrBodyMismatch is a body whose copy is not the size message.json said:
// a contract violation.
var ErrBodyMismatch = errors.New("the body is not the size message.json says")

// Decide judges one message and gives its disposition. The body is the
// Orker's own copy. An error is a contract violation that ends the
// attempt.
func (j *Judge) Decide(ctx context.Context, msg HandedMessage, body []byte) (Decision, error) {
	if !msg.Oversize && !msg.BodyMatches(body) {
		return Decision{}, ErrBodyMismatch
	}
	for refresh := 0; ; refresh++ {
		if refresh > maxRefresh {
			return j.wait("refresh"), nil
		}
		src, revision, _ := j.held()
		pre := j.prejudge(src, msg, body)
		if pre.Filtered {
			fresh, d, ok := j.confirmed(ctx)
			if !ok {
				return d, nil
			}
			if !fresh {
				continue
			}
			_, _, at := j.held()
			return Decision{Disposition: Settle, Action: ActionAck, ConfirmedAt: at,
				Receipt: Receipt{Outcome: artifacts.ReceiptFiltered, Reason: eventsources.ReasonMessageNotMatched, Sampled: true}}, nil
		}
		sum := sha256.Sum256(body)
		offer := Offer{Message: msg, BodyDigest: hex.EncodeToString(sum[:]), BodySize: int64(len(body)), Values: pre.Values, Revision: revision}
		if msg.Oversize {
			offer.BodySize = msg.OversizeBytes
		}
		if pre.Rejection != nil {
			offer.PreRejection = pre.Rejection.Reason
		}
		answer, err := j.Door.Offer(ctx, offer)
		if err != nil {
			if ctx.Err() != nil {
				return Decision{}, ctx.Err()
			}
			return j.waitErr("engine_unreachable", err), nil
		}
		if answer.Kind == AnswerRefresh {
			if answer.Source == nil || answer.Source.GetJudgingRevision() < revision {
				// A refresh that carries no spec, or an older one, confirms
				// nothing and is never taken.
				return j.wait("refresh"), nil
			}
			j.Swap(answer.Source, answer.Source.GetJudgingRevision(), j.Now())
			continue
		}
		if answer.Kind != AnswerProceed {
			return j.finish(msg, answer), nil
		}
		return j.start(ctx, msg, body, offer, answer.Parts)
	}
}

// prejudge runs bounds, parse, match and extraction with the spec held.
func (j *Judge) prejudge(src *orkstackv1.EventSource, msg HandedMessage, body []byte) eventsources.Prejudgement {
	if msg.Oversize {
		return eventsources.Prejudgement{Rejection: &eventsources.MessageError{Reason: eventsources.ReasonMessageTooLarge, Offset: -1,
			Detail: fmt.Sprintf("the message is %d bytes", msg.OversizeBytes)}}
	}
	return eventsources.Prejudge(src, msg.Message(body), j.MaxMessageBytes)
}

// confirmed makes sure the spec held is the document's within the last five
// seconds before a FILTERED verdict. fresh false means a new spec was
// taken: judge again. ok false is the decision to return instead.
func (j *Judge) confirmed(ctx context.Context) (fresh bool, d Decision, ok bool) {
	_, revision, at := j.held()
	if !at.IsZero() && j.Now().Sub(at) <= confirmFresh {
		return true, Decision{}, true
	}
	c, err := j.Door.Confirm(ctx, revision)
	if err != nil {
		return false, j.waitErr("engine_unreachable", err), false
	}
	if c.Stop {
		return false, j.pause(), false
	}
	if c.Revision < revision {
		// A lagging replica: never go back to an older match: or map:.
		return false, j.wait("rollout"), false
	}
	if c.Revision != revision {
		if c.Source == nil {
			return false, j.wait("rollout"), false
		}
		j.Swap(c.Source, c.Revision, j.Now())
		return false, Decision{}, true
	}
	j.mu.Lock()
	if j.revision == revision {
		j.confirmedAt = j.Now()
	}
	j.mu.Unlock()
	return true, Decision{}, true
}

// start stages the parts the offer named and starts the run. An error is
// the Orker's own fault (a metadata document it cannot encode) or ctx's
// end; every failure that might pass is a wait.
func (j *Judge) start(ctx context.Context, msg HandedMessage, body []byte, offer Offer, parts []Part) (Decision, error) {
	retry := false
	for again := 0; ; again++ {
		if again > maxUploadAgain {
			return j.wait("upload_again"), nil
		}
		staged := make([]StagedPart, 0, len(parts))
		for _, p := range parts {
			if p.Kind != PartBody && p.Kind != PartMetadata {
				return j.wait("unknown_part"), nil
			}
			bytes := body
			if p.Kind == PartMetadata {
				doc, err := MetadataDocument(msg, offer, j.Ref)
				if err != nil {
					return Decision{}, fmt.Errorf("sourcehost: encode the metadata document: %w", err)
				}
				bytes = doc
			}
			s, err := j.Stager.Stage(ctx, msg, p, bytes)
			if err != nil {
				if ctx.Err() != nil {
					return Decision{}, ctx.Err()
				}
				return j.waitErr("upload", err), nil
			}
			staged = append(staged, s)
		}
		answer, err := j.Door.Start(ctx, Start{Offer: offer, Staged: staged, Retry: retry})
		if err != nil {
			if ctx.Err() != nil {
				return Decision{}, ctx.Err()
			}
			// No answer: a wait. The message's next judgement offers it
			// again, and the engine finds the run by the message's claim if
			// this start reached it.
			return j.waitErr("engine_unreachable", err), nil
		}
		if answer.Kind == AnswerUploadAgain {
			retry = true
			continue
		}
		if answer.Kind == AnswerRefresh || answer.Kind == AnswerProceed {
			return j.wait("start_answer_unexpected"), nil
		}
		return j.finish(msg, answer), nil
	}
}

// finish turns an answer other than PROCEED, REFRESH or UPLOAD_AGAIN into a
// disposition.
func (j *Judge) finish(msg HandedMessage, a Answer) Decision {
	switch a.Kind {
	case AnswerAccepted:
		return Decision{Disposition: Settle, Action: ActionAck, Receipt: Receipt{Outcome: artifacts.ReceiptAccepted, RunID: a.RunID}}
	case AnswerDuplicate:
		return Decision{Disposition: Settle, Action: ActionAck, Receipt: Receipt{Outcome: artifacts.ReceiptDuplicate, RunID: a.RunID}}
	case AnswerReject:
		d := Decision{Disposition: Reject, Action: ActionReject, Delay: RejectDelay(msg.DeliveryCount),
			Receipt: Receipt{Outcome: artifacts.ReceiptRejected, Reason: a.Reason, Required: true}}
		if j.AtMostOnce {
			d.Receipt.Outcome = artifacts.ReceiptDropped
		}
		return d
	case AnswerStop:
		return j.pause()
	case AnswerUncertain:
		d := j.wait("uncertain")
		d.Uncertain, d.Receipt.Reason = true, a.Reason
		return d
	case AnswerWait:
		return j.wait(a.Cause)
	}
	return j.wait("unknown_answer")
}

func (j *Judge) wait(cause string) Decision {
	return Decision{Disposition: Wait, Cause: cause}
}

func (j *Judge) waitErr(cause string, err error) Decision {
	return Decision{Disposition: Wait, Cause: cause, Err: err}
}

func (j *Judge) pause() Decision {
	d := Decision{Disposition: Pause, Action: ActionRelease, Delay: releaseDelaySeconds,
		Receipt: Receipt{Outcome: artifacts.ReceiptReleased, Reason: eventsources.ReasonPaused}}
	if j.AtMostOnce {
		d.Receipt.Outcome, d.Receipt.Required = artifacts.ReceiptDropped, true
	}
	return d
}

// RejectDelay is a rejection's delay: 1 s doubling with the delivery count
// to 30 s, so the broker's own maximum dead-letters the message.
func RejectDelay(deliveryCount int) int {
	if deliveryCount <= 1 {
		return 1
	}
	if deliveryCount > 6 {
		return maxRejectDelaySeconds
	}
	return min(1<<(deliveryCount-1), maxRejectDelaySeconds)
}

// DeadLettered is the receipt a message gets instead when it is not
// accepted and is about to be given back at the broker's delivery limit
// (01 §5): required, MESSAGE_DELIVERIES_EXHAUSTED, with the reason it was
// refused or waiting for as its cause. limit is the smaller of the last two
// limits the container reported; 0 when none is known.
func DeadLettered(d Decision, deliveryCount, limit int, atMostOnce bool) (Receipt, bool) {
	if d.Disposition == Settle || limit <= 0 || deliveryCount < limit {
		return d.Receipt, false
	}
	cause := d.Receipt.Reason
	if cause == "" {
		cause = d.Cause
	}
	outcome := artifacts.ReceiptDeadLettered
	if atMostOnce {
		outcome = artifacts.ReceiptDropped
	}
	return Receipt{Outcome: outcome, Reason: eventsources.ReasonMessageDeliveriesExhausted, Cause: cause, Required: true}, true
}
