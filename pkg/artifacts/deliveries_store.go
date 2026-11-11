package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// BlobDeliveryStore keeps delivery records in the shared object store every
// engine replica already uses, which is why any replica answers for a
// delivery another replica made (AGENTS.md §7). A local directory holds them
// at exactly the bucket's key layout.
type BlobDeliveryStore struct {
	blobBase
	// bodiesRead counts the record bodies this store has asked the backend
	// for (BodiesRead).
	bodiesRead atomic.Int64
}

// LocalDeliveryStore and S3DeliveryStore are the delivery store over a local
// directory and over a bucket: one implementation, two backends.
type (
	LocalDeliveryStore = BlobDeliveryStore
	S3DeliveryStore    = BlobDeliveryStore
)

var _ DeliveryStore = (*BlobDeliveryStore)(nil)

// NewLocalDeliveryStore opens (creating if needed) the delivery tree under dir.
func NewLocalDeliveryStore(dir string) (*LocalDeliveryStore, error) {
	b, err := openLocalBlobs("delivery store", dir, DeliveryPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobDeliveryStore{blobBase: blobBase{b}}, nil
}

// NewS3DeliveryStore builds the delivery store from the same ORK_S3_* config
// the artifact, attempt, trigger and audit stores use.
func NewS3DeliveryStore(ctx context.Context, cfg Config) (*S3DeliveryStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobDeliveryStore{blobBase: blobBase{b}}, nil
}

// BodiesRead is how many delivery record bodies this store has requested:
// one per read fetchDelivery issues, whatever the answer, NotFound included,
// because a request is the cost. Only tests read it, always as the delta
// around one call: it is how a test proves a filtered listing reads a body
// only for a record whose key matched.
func (s *BlobDeliveryStore) BodiesRead() int64 { return s.bodiesRead.Load() }

// PutDelivery writes one record create-only (S3 If-None-Match: *, local
// link(2)): an existing key is ErrDeliveryExists and is never overwritten.
func (s *BlobDeliveryStore) PutDelivery(ctx context.Context, rec *orkstackv1.Delivery) error {
	key, document, err := encodeDeliveryForStore(rec)
	if err != nil {
		return err
	}
	if err := s.b.create(ctx, key, DeliveryContentType, document); err != nil {
		if errors.Is(err, errBlobExists) {
			return fmt.Errorf("artifacts: put delivery record %q: %w", s.b.uri(key), ErrDeliveryExists)
		}
		return fmt.Errorf("artifacts: put delivery record %q: %w", s.b.uri(key), err)
	}
	return nil
}

// GetDelivery is one keys-only list of the id's prefix, then one get.
func (s *BlobDeliveryStore) GetDelivery(ctx context.Context, id string) (*orkstackv1.Delivery, bool, error) {
	return getDelivery(ctx, s, id)
}

// ListDeliveries walks the day prefixes newest first.
func (s *BlobDeliveryStore) ListDeliveries(ctx context.Context, q DeliveryQuery) (DeliveryPage, error) {
	return walkDeliveries(ctx, s, q)
}

func (s *BlobDeliveryStore) listDeliveryRange(ctx context.Context, dayPrefix, from, to string, max int) ([]string, error) {
	// StartAfter skips every key older than the window without listing it;
	// the walk stops at the first key past its end. Listing is ascending, so
	// both bounds are one comparison each.
	var keys []string
	err := listObjects(ctx, s.b, blobListInput{Prefix: dayPrefix, StartAfter: from}, func(object blobObject) (bool, error) {
		if object.Key >= to {
			return true, nil
		}
		if object.Key <= from {
			return false, nil
		}
		keys = append(keys, object.Key)
		return len(keys) > max, nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: list delivery day %q: %w", s.b.uri(dayPrefix), err)
	}
	return keys, nil
}

func (s *BlobDeliveryStore) listDeliveryID(ctx context.Context, idPrefix string) ([]string, error) {
	// One request: a delivery has exactly one record, so two keys are
	// already the anomaly getDelivery reports, and a third adds nothing.
	page, err := s.b.list(ctx, blobListInput{Prefix: idPrefix, MaxKeys: 2})
	if err != nil {
		return nil, fmt.Errorf("artifacts: list delivery %q: %w", s.b.uri(idPrefix), err)
	}
	return objectKeys(page), nil
}

func (s *BlobDeliveryStore) fetchDelivery(ctx context.Context, key string) ([]byte, bool, error) {
	s.bodiesRead.Add(1)
	document, _, err := s.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			// Expired under a lifecycle rule between the listing and the read.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("artifacts: get delivery record %q: %w", s.b.uri(key), err)
	}
	return document, true, nil
}

// objectKeys is the keys of one listing page, in order.
func objectKeys(page blobListPage) []string {
	keys := make([]string, 0, len(page.Objects))
	for _, object := range page.Objects {
		keys = append(keys, object.Key)
	}
	return keys
}
