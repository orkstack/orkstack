package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// What an Orker that hosts an event source writes to object storage
// (docs/STORAGE.md, "Event sources"): a receipt for each settled message
// delivery, a message's bound parts on their way to a run, and an
// instance's logs and attempt records. Each key is built here and nowhere
// else, and every one names the source by its id.

const (
	// ReceiptPrefix holds one object per settled message delivery, written
	// create-only by the Orker before the verdict. A record, never a read
	// path for a decision; expired by a bucket lifecycle rule.
	ReceiptPrefix = "metadata/receipts"
	// EventSourceStagingPrefix holds a message's bound parts between the
	// offer and the start, keyed by the message and then the content.
	EventSourceStagingPrefix = "metadata/staging/event-sources"
	// MaxReceiptBytes bounds one receipt (08 §3).
	MaxReceiptBytes = 16 << 10
	// sourceSegment is the segment under an instance's logs and attempt
	// records that no task id can be (a task id is a slug).
	sourceSegment = "_source"
	// receiptStampLayout is the receipt's time in its key: UTC, fixed
	// width, so the keys of one day sort by it.
	receiptStampLayout = "20060102T150405.000000000Z"
)

// ReceiptOutcome is the word a receipt's key carries for what was decided.
type ReceiptOutcome string

// The outcomes a receipt records (08 §3).
const (
	ReceiptAccepted     ReceiptOutcome = "accepted"
	ReceiptDuplicate    ReceiptOutcome = "duplicate"
	ReceiptRejected     ReceiptOutcome = "rejected"
	ReceiptDeadLettered ReceiptOutcome = "dead_lettered"
	ReceiptDropped      ReceiptOutcome = "dropped"
	ReceiptReleased     ReceiptOutcome = "released"
	ReceiptFiltered     ReceiptOutcome = "filtered"
)

var receiptOutcomes = map[ReceiptOutcome]bool{
	ReceiptAccepted: true, ReceiptDuplicate: true, ReceiptRejected: true, ReceiptDeadLettered: true,
	ReceiptDropped: true, ReceiptReleased: true, ReceiptFiltered: true,
}

var stagingDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ReceiptKeyParts name one settled delivery: the source, the instant the
// Orker took the message from the container (fixed for the exchange, never
// the time of the write), the delivery count, the exchange (the instance's
// epoch, the lane and the step's seq), the outcome and the message's
// identity.
type ReceiptKeyParts struct {
	Namespace string
	SourceID  string
	Taken     time.Time
	Delivery  int
	Epoch     int64
	Lane      int
	Seq       int64
	Outcome   ReceiptOutcome
	Identity  string
}

// ReceiptKey is the key of one delivery's receipt:
// metadata/receipts/{ns}/{id}/{yyyy}/{mm}/{dd}/{stamp}-{delivery:04}-{epoch}-{lane}-{seq}-{outcome}-{sha256(identity)[:16]}.json.
// The source comes first, so one source's receipts are one prefix; a write
// retried for the same exchange lands on the same object, and two
// deliveries of one message are two.
func ReceiptKey(p ReceiptKeyParts) (string, error) {
	switch {
	case !eventSourceSlug.MatchString(p.Namespace):
		return "", fmt.Errorf("artifacts: receipt: namespace %q is not a slug", p.Namespace)
	case !eventsources.ValidID(p.SourceID):
		return "", fmt.Errorf("artifacts: receipt: %q is not an event source id", p.SourceID)
	case p.Taken.IsZero():
		return "", errors.New("artifacts: receipt: the instant the message was taken is required")
	case p.Delivery < 0 || p.Epoch < 0 || p.Lane < 0 || p.Seq < 0:
		return "", errors.New("artifacts: receipt: the delivery count, epoch, lane and seq are not negative")
	case !receiptOutcomes[p.Outcome]:
		return "", fmt.Errorf("artifacts: receipt: %q is not an outcome", p.Outcome)
	case p.Identity == "" || len(p.Identity) > eventsources.MaxIdentityBytes:
		return "", fmt.Errorf("artifacts: receipt: a message identity is 1 to %d bytes", eventsources.MaxIdentityBytes)
	}
	t := p.Taken.UTC()
	sum := sha256.Sum256([]byte(p.Identity))
	return fmt.Sprintf("%s/%s/%s/%04d/%02d/%02d/%s-%04d-%d-%d-%d-%s-%s.json",
		ReceiptPrefix, p.Namespace, p.SourceID, t.Year(), int(t.Month()), t.Day(), t.Format(receiptStampLayout),
		p.Delivery, p.Epoch, p.Lane, p.Seq, p.Outcome, hex.EncodeToString(sum[:])[:16]), nil
}

// ReceiptSourcePrefix is the prefix every receipt of one source is under.
func ReceiptSourcePrefix(namespace, sourceID string) (string, error) {
	if !eventSourceSlug.MatchString(namespace) || !eventsources.ValidID(sourceID) {
		return "", fmt.Errorf("artifacts: receipts of %q/%q: a namespace is a slug and an id evs- and 16 hex digits", namespace, sourceID)
	}
	return ReceiptPrefix + "/" + namespace + "/" + sourceID + "/", nil
}

// StagingKey is the key of one bound part of a message on its way to a run:
// metadata/staging/event-sources/{ns}/{id}/{sha256(identity)}/{sha256 of the part}.
// It names no run: the engine copies it to the run's own key at the start.
func StagingKey(namespace, sourceID, identity, contentDigest string) (string, error) {
	switch {
	case !eventSourceSlug.MatchString(namespace):
		return "", fmt.Errorf("artifacts: staging: namespace %q is not a slug", namespace)
	case !eventsources.ValidID(sourceID):
		return "", fmt.Errorf("artifacts: staging: %q is not an event source id", sourceID)
	case identity == "" || len(identity) > eventsources.MaxIdentityBytes:
		return "", fmt.Errorf("artifacts: staging: a message identity is 1 to %d bytes", eventsources.MaxIdentityBytes)
	case !stagingDigest.MatchString(contentDigest):
		return "", fmt.Errorf("artifacts: staging: %q is not a SHA-256", contentDigest)
	}
	sum := sha256.Sum256([]byte(identity))
	return EventSourceStagingPrefix + "/" + namespace + "/" + sourceID + "/" + hex.EncodeToString(sum[:]) + "/" + contentDigest, nil
}

// InstanceAttempt names one attempt of one instance of a source: its index,
// the epoch the source's workflow scheduled it under, and the attempt.
type InstanceAttempt struct {
	SourceID string
	Index    int
	Epoch    int64
	Attempt  int
}

func (a InstanceAttempt) valid() error {
	if !eventsources.ValidID(a.SourceID) {
		return fmt.Errorf("artifacts: instance: %q is not an event source id", a.SourceID)
	}
	if a.Index < 0 || a.Index > 99999 || a.Epoch < 0 || a.Epoch > 999999999999 || a.Attempt < 0 || a.Attempt > 9999 {
		return errors.New("artifacts: instance: index, epoch and attempt are within their key widths")
	}
	return nil
}

func (a InstanceAttempt) path() string {
	return fmt.Sprintf("%s/%s/%05d/%012d-%04d", a.SourceID, sourceSegment, a.Index, a.Epoch, a.Attempt)
}

// SourceLogKey is the key of one chunk of an instance attempt's rolling
// stdout and stderr: metadata/logs/{id}/_source/{index:05}/{epoch:012}-{attempt:04}/{seq}.
func SourceLogKey(a InstanceAttempt, seq int) (string, error) {
	if err := a.valid(); err != nil {
		return "", err
	}
	if seq < 0 {
		return "", errors.New("artifacts: instance log: seq is not negative")
	}
	return fmt.Sprintf("%s/%s/%d", LogPrefix, a.path(), seq), nil
}

// SourceAttemptKey is the key of one instance attempt's record:
// metadata/attempts/{id}/_source/{index:05}/{epoch:012}-{attempt:04}.json.
func SourceAttemptKey(a InstanceAttempt) (string, error) {
	if err := a.valid(); err != nil {
		return "", err
	}
	return AttemptPrefix + "/" + a.path() + ".json", nil
}

// EventSourceRecordStore writes what an Orker hosting an event source
// records: receipts and staged parts, create-only. It needs put on the two
// prefixes an Orker's credential is allowed beyond a task's
// (OrkerAllowedPrefixes) and nothing else.
type EventSourceRecordStore struct {
	blobBase
}

// NewEventSourceRecordStore opens the record store on the deployment's
// artifact backend.
func NewEventSourceRecordStore(ctx context.Context, cfg Config) (*EventSourceRecordStore, error) {
	b, err := openBlobs(ctx, cfg, "event source record store", ReceiptPrefix, EventSourceStagingPrefix)
	if err != nil {
		return nil, err
	}
	return &EventSourceRecordStore{blobBase: blobBase{b}}, nil
}

// NewLocalEventSourceRecordStore opens (creating if needed) the record
// store under dir.
func NewLocalEventSourceRecordStore(dir string) (*EventSourceRecordStore, error) {
	b, err := openLocalBlobs("event source record store", dir, ReceiptPrefix, EventSourceStagingPrefix)
	if err != nil {
		return nil, err
	}
	return &EventSourceRecordStore{blobBase: blobBase{b}}, nil
}

// PutReceipt writes one receipt at key create-only. A receipt already
// there is success: a write retried for the same exchange is the same
// receipt.
func (s *EventSourceRecordStore) PutReceipt(ctx context.Context, key string, body []byte) error {
	if !isUnder(key, ReceiptPrefix) {
		return fmt.Errorf("artifacts: receipt key %q is not under %s", key, ReceiptPrefix)
	}
	if len(body) == 0 || len(body) > MaxReceiptBytes {
		return fmt.Errorf("artifacts: a receipt is 1 to %d bytes, got %d", MaxReceiptBytes, len(body))
	}
	if err := s.b.create(ctx, key, "application/json", body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: write receipt %s: %w", s.b.uri(key), err)
	}
	return nil
}

// PutStaged writes one bound part at key create-only. The key ends in the
// part's own digest, which the bytes are checked against, so a part
// already there is the same part and is success.
func (s *EventSourceRecordStore) PutStaged(ctx context.Context, key string, body []byte) error {
	if !isUnder(key, EventSourceStagingPrefix) || len(key) < 64 {
		return fmt.Errorf("artifacts: staging key %q is not under %s", key, EventSourceStagingPrefix)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != key[len(key)-64:] {
		return errors.New("artifacts: staged part does not match the digest its key names")
	}
	if err := s.b.create(ctx, key, "application/octet-stream", body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: write staged part %s: %w", s.b.uri(key), err)
	}
	return nil
}

// isUnder reports a key strictly under prefix with no empty, "." or ".."
// segment, so no key resolves outside the prefix on any backend.
func isUnder(key, prefix string) bool {
	if len(key) <= len(prefix)+1 || key[:len(prefix)+1] != prefix+"/" || path.Clean(key) != key {
		return false
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// MarshalReceipt is a receipt's stored bytes: protojson, at most
// MaxReceiptBytes, never a body, an attribute value or variable text in its
// sentence (the caller sets the reason's fixed sentence).
func MarshalReceipt(r *orkstackv1.Receipt) ([]byte, error) {
	if err := checkReceipt(r); err != nil {
		return nil, err
	}
	raw, err := protojson.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode receipt: %w", err)
	}
	if len(raw) > MaxReceiptBytes {
		return nil, fmt.Errorf("artifacts: a receipt is %d bytes; at most %d", len(raw), MaxReceiptBytes)
	}
	return raw, nil
}

// UnmarshalReceiptRecord reads a stored receipt strictly: a field this
// release does not know, an unknown outcome, or bytes over the bound refuse
// it.
func UnmarshalReceiptRecord(raw []byte) (*orkstackv1.Receipt, error) {
	if len(raw) > MaxReceiptBytes {
		return nil, fmt.Errorf("artifacts: a receipt is %d bytes; at most %d", len(raw), MaxReceiptBytes)
	}
	r := &orkstackv1.Receipt{}
	if err := (protojson.UnmarshalOptions{}).Unmarshal(raw, r); err != nil {
		return nil, fmt.Errorf("artifacts: receipt does not parse: %w", err)
	}
	if err := checkReceipt(r); err != nil {
		return nil, err
	}
	return r, nil
}

func checkReceipt(r *orkstackv1.Receipt) error {
	switch {
	case r == nil:
		return errors.New("artifacts: no receipt")
	case r.GetOutcome() == orkstackv1.ReceiptOutcome_RECEIPT_OUTCOME_UNSPECIFIED || orkstackv1.ReceiptOutcome_name[int32(r.GetOutcome())] == "":
		return errors.New("artifacts: a receipt has an outcome")
	case !eventsources.ValidID(r.GetEventSourceId()) || !eventSourceSlug.MatchString(r.GetNamespace()):
		return errors.New("artifacts: a receipt names its source by namespace and id")
	case r.GetMessageId() == "" || len(r.GetMessageId()) > eventsources.MaxIdentityBytes:
		return fmt.Errorf("artifacts: a receipt's message identity is 1 to %d bytes", eventsources.MaxIdentityBytes)
	case len(r.GetBroker()) > 16:
		return errors.New("artifacts: a receipt carries at most 16 broker facts")
	case r.GetDeliveryCount() < 0 || r.GetMaxDeliveries() < 0 || r.GetBodyBytes() < 0 || r.GetInstance() < 0:
		return errors.New("artifacts: a receipt's counts are not negative")
	}
	return nil
}
