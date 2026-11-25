package artifacts

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// What engines, and only engines, record about an event source's instances
// (docs/STORAGE.md, "Event sources"): that one attempt confirmed at the door,
// with what it was scheduled to run, and that a source was deleted.
const (
	// EventSourceConfirmPrefix holds one object an attempt that confirmed.
	EventSourceConfirmPrefix = "metadata/event-source-confirms"
	// EventSourceTombstonePrefix holds one object a deleted source.
	EventSourceTombstonePrefix = "metadata/event-source-tombstones"

	confirmSchema   = 1
	maxConfirmBytes = 4 << 10
	maxOrkerID      = 512
	maxAdmission    = 256
)

// ConfirmRecord is what one attempt's first accepted confirmation wrote: who
// holds it, when, and the service spec and admission save the source's
// workflow scheduled its epoch with. It never changes, because an attempt
// runs one spec, and every later call of the attempt is held to it.
type ConfirmRecord struct {
	OrkerID       string    `json:"orkerId"`
	ConfirmedAt   time.Time `json:"confirmedAt"`
	ServiceDigest string    `json:"serviceDigest"`
	AdmissionSave string    `json:"admissionSave,omitzero"`
}

type confirmDocument struct {
	SchemaVersion int `json:"schemaVersion"`
	ConfirmRecord `json:",inline"`
}

// EventSourceInstanceStore keeps the confirmations and the tombstones. An
// Orker's credential reaches neither.
type EventSourceInstanceStore struct {
	blobBase
}

// NewEventSourceInstanceStore opens the store on the deployment's artifact
// backend.
func NewEventSourceInstanceStore(ctx context.Context, cfg Config) (*EventSourceInstanceStore, error) {
	b, err := openBlobs(ctx, cfg, "event source instance store", EventSourceConfirmPrefix, EventSourceTombstonePrefix)
	if err != nil {
		return nil, err
	}
	return &EventSourceInstanceStore{blobBase: blobBase{b}}, nil
}

// NewLocalEventSourceInstanceStore opens (creating if needed) the store under
// dir.
func NewLocalEventSourceInstanceStore(dir string) (*EventSourceInstanceStore, error) {
	b, err := openLocalBlobs("event source instance store", dir, EventSourceConfirmPrefix, EventSourceTombstonePrefix)
	if err != nil {
		return nil, err
	}
	return &EventSourceInstanceStore{blobBase: blobBase{b}}, nil
}

// confirmKey is metadata/event-source-confirms/{id}/{index:05}-{epoch:012}-{attempt:04}.
func confirmKey(a InstanceAttempt) (string, error) {
	if err := a.valid(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/%05d-%012d-%04d", EventSourceConfirmPrefix, a.SourceID, a.Index, a.Epoch, a.Attempt), nil
}

func tombstoneKey(sourceID string) (string, error) {
	if !eventsources.ValidID(sourceID) {
		return "", fmt.Errorf("artifacts: tombstone: %q is not an event source id", sourceID)
	}
	return EventSourceTombstonePrefix + "/" + sourceID + ".json", nil
}

// Confirmed reads the attempt's record; found is false only when the store
// said it is not there. One that cannot be read is an error: no answer.
func (s *EventSourceInstanceStore) Confirmed(ctx context.Context, a InstanceAttempt) (r ConfirmRecord, found bool, err error) {
	key, err := confirmKey(a)
	if err != nil {
		return ConfirmRecord{}, false, err
	}
	raw, _, err := s.b.get(ctx, key, maxConfirmBytes+1)
	if errors.Is(err, errBlobNotFound) {
		return ConfirmRecord{}, false, nil
	}
	if err != nil {
		return ConfirmRecord{}, false, fmt.Errorf("artifacts: read confirm record %s: %w", s.b.uri(key), err)
	}
	if r, err = decodeConfirm(raw); err != nil {
		return ConfirmRecord{}, false, fmt.Errorf("artifacts: confirm record %s: %w", s.b.uri(key), err)
	}
	return r, true, nil
}

// Confirm records the attempt's first accepted confirmation, and returns the
// record that holds: this one, or the one another engine wrote first.
func (s *EventSourceInstanceStore) Confirm(ctx context.Context, a InstanceAttempt, r ConfirmRecord) (ConfirmRecord, error) {
	key, err := confirmKey(a)
	if err != nil {
		return ConfirmRecord{}, err
	}
	r.ConfirmedAt = r.ConfirmedAt.UTC()
	if err := r.check(); err != nil {
		return ConfirmRecord{}, err
	}
	body, err := stablejson.Marshal(confirmDocument{SchemaVersion: confirmSchema, ConfirmRecord: r})
	if err != nil {
		return ConfirmRecord{}, fmt.Errorf("artifacts: encode confirm record: %w", err)
	}
	err = s.b.create(ctx, key, "application/json", body)
	if err == nil {
		return r, nil
	}
	if !errors.Is(err, errBlobExists) {
		return ConfirmRecord{}, fmt.Errorf("artifacts: write confirm record %s: %w", s.b.uri(key), err)
	}
	holder, found, err := s.Confirmed(ctx, a)
	if err != nil {
		return ConfirmRecord{}, err
	}
	if !found {
		return ConfirmRecord{}, fmt.Errorf("artifacts: write confirm record %s: something held the key and nothing is read there", s.b.uri(key))
	}
	return holder, nil
}

func (r ConfirmRecord) check() error {
	_, instant := storedInstant(r.ConfirmedAt)
	switch {
	case r.OrkerID == "" || len(r.OrkerID) > maxOrkerID:
		return errors.New("artifacts: confirm record: an Orker id is 1 to 512 bytes")
	case !instant:
		return errors.New("artifacts: confirm record: the instant it was confirmed is required")
	case !eventSourceDigest.MatchString(r.ServiceDigest):
		return fmt.Errorf("artifacts: confirm record: %q is not a service digest", r.ServiceDigest)
	case len(r.AdmissionSave) > maxAdmission:
		return errors.New("artifacts: confirm record: an admission save is at most 256 bytes")
	}
	return nil
}

func decodeConfirm(raw []byte) (ConfirmRecord, error) {
	if len(raw) > maxConfirmBytes {
		return ConfirmRecord{}, fmt.Errorf("over %d bytes", maxConfirmBytes)
	}
	var doc confirmDocument
	if err := json.Unmarshal(raw, &doc, json.RejectUnknownMembers(true)); err != nil {
		return ConfirmRecord{}, fmt.Errorf("does not decode: %w", err)
	}
	if doc.SchemaVersion != confirmSchema {
		return ConfirmRecord{}, fmt.Errorf("is of version %d", doc.SchemaVersion)
	}
	doc.ConfirmedAt = doc.ConfirmedAt.UTC()
	return doc.ConfirmRecord, doc.check()
}

type tombstoneDocument struct {
	SchemaVersion int       `json:"schemaVersion"`
	ID            string    `json:"id"`
	Namespace     string    `json:"namespace"`
	Name          string    `json:"name"`
	DeletedAt     time.Time `json:"deletedAt"`
}

// Tombstone records that the source was deleted. One already there is
// success; nothing in OrkStack removes one.
func (s *EventSourceInstanceStore) Tombstone(ctx context.Context, sourceID, namespace, name string, at time.Time) error {
	key, err := tombstoneKey(sourceID)
	if err != nil {
		return err
	}
	body, err := stablejson.Marshal(tombstoneDocument{SchemaVersion: 1, ID: sourceID, Namespace: namespace, Name: name, DeletedAt: at.UTC()})
	if err != nil {
		return fmt.Errorf("artifacts: encode tombstone: %w", err)
	}
	if err := s.b.create(ctx, key, "application/json", body); err != nil && !errors.Is(err, errBlobExists) {
		return fmt.Errorf("artifacts: write tombstone %s: %w", s.b.uri(key), err)
	}
	return nil
}

// Tombstoned reports whether the source was deleted. The key's being there
// is what is read, whatever the object holds, and only the store's own "not
// found" is none.
func (s *EventSourceInstanceStore) Tombstoned(ctx context.Context, sourceID string) (bool, error) {
	key, err := tombstoneKey(sourceID)
	if err != nil {
		return false, err
	}
	// A read and not a head: a head's "not found" has no body to tell a
	// missing object from a missing bucket.
	_, _, err = s.b.get(ctx, key, 1)
	if errors.Is(err, errBlobNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("artifacts: read tombstone %s: %w", s.b.uri(key), err)
	}
	return true, nil
}
