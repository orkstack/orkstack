package sourcehost

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

const (
	// receiptTimeout bounds one write of a receipt, and stageTimeout one of a
	// message's part.
	receiptTimeout = 10 * time.Second
	stageTimeout   = 60 * time.Second
	// filteredEvery is how often an instance records a filtered message.
	filteredEvery = 10 * time.Minute
	// maxRejectedKept bounds the rejected messages an instance remembers; past
	// it each rejection is recorded.
	maxRejectedKept = 10_000
)

// RecordStore writes what an instance records, create-only: the object already
// there is the same one (artifacts.EventSourceRecordStore).
type RecordStore interface {
	PutReceipt(ctx context.Context, key string, body []byte) error
	PutStaged(ctx context.Context, key string, body []byte) error
}

// Records is what one attempt of an instance writes to object storage for its
// messages: receipts and staged parts.
type Records struct {
	Store    RecordStore
	Instance *orkstackv1.InstanceRef
	OrkerID  string
	// Provider is the image's broker kind as its status reported it, and Limit
	// the broker's delivery limit and when it was read.
	Provider func() string
	Limit    func() (int, time.Time)
	Now      func() time.Time

	mu       sync.Mutex
	filtered time.Time
	rejected map[[sha256.Size]byte]bool
}

var _ Stager = (*Records)(nil)

// Lane is the recorder of one lane.
func (r *Records) Lane(lane int) Recorder { return laneRecords{r, lane} }

type laneRecords struct {
	*Records
	lane int
}

// Stage is Stager's.
func (r *Records) Stage(ctx context.Context, msg HandedMessage, part Part, body []byte) (StagedPart, error) {
	digest := bodyDigest(body)
	key, err := artifacts.StagingKey(r.Instance.GetNamespace(), r.Instance.GetEventSourceId(), msg.ID, digest)
	if err != nil {
		return StagedPart{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()
	if err := r.Store.PutStaged(ctx, key, body); err != nil {
		return StagedPart{}, err
	}
	return StagedPart{Input: part.Input, Digest: digest, Size: int64(len(body))}, nil
}

// Record is Recorder's.
func (l laneRecords) Record(ctx context.Context, msg HandedMessage, seq int64, rec Receipt) (err error) {
	write, undo := l.due(msg, rec)
	if !write {
		return nil
	}
	defer func() {
		if err != nil {
			undo()
		}
	}()
	id := receiptIdentity(msg.ID)
	key, err := artifacts.ReceiptKey(artifacts.ReceiptKeyParts{Namespace: l.Instance.GetNamespace(), SourceID: l.Instance.GetEventSourceId(),
		Taken: msg.Taken, Delivery: msg.DeliveryCount, Epoch: l.Instance.GetEpoch(), Lane: l.lane, Seq: seq, Outcome: rec.Outcome, Identity: id})
	if err != nil {
		return err
	}
	body, err := l.marshal(msg, id, rec)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, receiptTimeout)
	defer cancel()
	return l.Store.PutReceipt(ctx, key, body)
}

// due reports whether the receipt is written, counting it as written, and how
// to take that back: a filtered message's is sampled, and a rejected one's is
// written the first time in the instance's run of rejections.
func (r *Records) due(msg HandedMessage, rec Receipt) (bool, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case rec.Sampled:
		now, last := r.Now(), r.filtered
		if !last.IsZero() && now.Sub(last) < filteredEvery {
			return false, nil
		}
		r.filtered = now
		return true, func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.filtered.Equal(now) {
				r.filtered = last
			}
		}
	case rec.Outcome == artifacts.ReceiptAccepted, rec.Outcome == artifacts.ReceiptDuplicate:
		r.rejected = nil
	case rec.Outcome == artifacts.ReceiptRejected:
		key := r.rejectedKey(msg.ID)
		if r.rejected[key] {
			return false, nil
		}
		if r.rejected == nil {
			r.rejected = map[[sha256.Size]byte]bool{}
		}
		if len(r.rejected) < maxRejectedKept {
			r.rejected[key] = true
		}
		return true, func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			delete(r.rejected, key)
		}
	}
	return true, func() {}
}

// rejectedKey is what is remembered of a rejected message: an image names one at
// any length.
func (*Records) rejectedKey(id string) [sha256.Size]byte { return sha256.Sum256([]byte(id)) }

var receiptOutcomes = map[artifacts.ReceiptOutcome]orkstackv1.ReceiptOutcome{
	artifacts.ReceiptAccepted:     orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_ACCEPTED,
	artifacts.ReceiptDuplicate:    orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_DUPLICATE,
	artifacts.ReceiptFiltered:     orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_FILTERED,
	artifacts.ReceiptRejected:     orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_REJECTED,
	artifacts.ReceiptReleased:     orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_RELEASED,
	artifacts.ReceiptDeadLettered: orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_DEAD_LETTERED,
	artifacts.ReceiptDropped:      orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_DROPPED,
}

// marshal is the receipt's stored bytes. A broker's facts that do not fit are
// left out: what was decided is never lost for them.
func (r *Records) marshal(msg HandedMessage, id string, rec Receipt) ([]byte, error) {
	limit, checked := r.Limit()
	out := &orkstackv1.Receipt{
		Id: artifacts.ReceiptID(msg.Taken), Namespace: r.Instance.GetNamespace(), EventSourceId: r.Instance.GetEventSourceId(),
		EventSourceName: r.Instance.GetName(), Provider: r.Provider(), MessageId: id,
		DeliveryCount: int32(min(msg.DeliveryCount, math.MaxInt32)), MaxDeliveries: int32(limit),
		BodySha256: msg.BodySHA256, BodyBytes: msg.BodyBytes, Outcome: receiptOutcomes[rec.Outcome], Reason: rec.Reason,
		Message: eventsources.MessageSentence(rec.Reason), RunId: rec.RunID, ReceivedAt: timestamppb.New(msg.Taken),
		DecidedAt: timestamppb.New(r.Now()), Broker: msg.Broker, Cause: rec.Cause, Instance: r.Instance.GetIndex(), OrkerId: r.OrkerID,
	}
	if msg.Oversize {
		out.BodySha256, out.BodyBytes = "", msg.OversizeBytes
	}
	if !checked.IsZero() {
		out.LimitCheckedAt = timestamppb.New(checked)
	}
	body, err := artifacts.MarshalReceipt(out)
	if err != nil && len(out.GetBroker()) > 0 {
		out.Broker = nil
		body, err = artifacts.MarshalReceipt(out)
	}
	if err != nil {
		return nil, fmt.Errorf("sourcehost: receipt: %w", err)
	}
	return body, nil
}

// receiptIdentity is a message's identity as its receipt names it: one that is
// over the bound or not text is what the message was rejected for.
func receiptIdentity(id string) string {
	id = strings.ToValidUTF8(id, "\ufffd")
	if len(id) <= eventsources.MaxIdentityBytes {
		return id
	}
	id = id[:eventsources.MaxIdentityBytes]
	for !utf8.ValidString(id) {
		id = id[:len(id)-1]
	}
	return id
}
