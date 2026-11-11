package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// DeliveryBodyPrefix holds the signed bodies of webhook deliveries still being
// sent: metadata/delivery-bodies/{delivery-id}/{sha256}.json. A body is too
// large to travel in the delivery's own workflow input (it carries the whole
// run), so the engine stores it here when it queues the delivery, every
// attempt reads the same bytes back by digest, and the delivery's last step
// deletes it. It is a transient object, never a queue: nothing lists this
// prefix to decide what to send.
const DeliveryBodyPrefix = "metadata/delivery-bodies"

// DeliveryBodyContentType is what a stored body is: the JSON a receiver gets.
const DeliveryBodyContentType = "application/json"

// ErrDeliveryBodyAltered is a stored body whose bytes no longer hash to the
// digest its delivery recorded.
var ErrDeliveryBodyAltered = errors.New("delivery body does not match its digest")

// DeliveryBodyStore keeps the bodies of deliveries in flight.
type DeliveryBodyStore interface {
	// PutDeliveryBody stores body create-only under its delivery and its
	// SHA-256, which it returns. The same bytes already there are success.
	PutDeliveryBody(ctx context.Context, deliveryID string, body []byte) (sha256hex string, err error)
	// GetDeliveryBody reads a body back and checks it against its digest. A
	// missing body matches ErrNotFound; altered bytes ErrDeliveryBodyAltered.
	GetDeliveryBody(ctx context.Context, deliveryID, sha256hex string) ([]byte, error)
	// DeleteDeliveryBody removes a body. A missing body is not an error.
	DeleteDeliveryBody(ctx context.Context, deliveryID, sha256hex string) error
}

// DeliveryBodyKey renders a body's key. Both coordinates are validated, so
// nothing but a delivery id and a digest becomes a key segment.
func DeliveryBodyKey(deliveryID, sha256hex string) (string, error) {
	if !webhooks.ValidDeliveryID(deliveryID) {
		return "", fmt.Errorf("artifacts: delivery body: invalid delivery id %q", deliveryID)
	}
	if !isSHA256Hex(sha256hex) {
		return "", fmt.Errorf("artifacts: delivery body of %s: invalid sha256 %q", deliveryID, sha256hex)
	}
	return DeliveryBodyPrefix + "/" + deliveryID + "/" + sha256hex + ".json", nil
}

// BlobDeliveryBodyStore keeps delivery bodies in the object store every
// engine replica shares, so the replica that runs a retry reads the body the
// replica that queued it wrote.
type BlobDeliveryBodyStore struct{ blobBase }

var _ DeliveryBodyStore = (*BlobDeliveryBodyStore)(nil)

// NewDeliveryBodyStore selects the same shared backend as every other record
// store.
func NewDeliveryBodyStore(ctx context.Context, cfg Config) (*BlobDeliveryBodyStore, error) {
	b, err := openBlobs(ctx, cfg, "delivery body store", DeliveryBodyPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobDeliveryBodyStore{blobBase{b}}, nil
}

// NewLocalDeliveryBodyStore opens (creating if needed) the body tree under
// dir.
func NewLocalDeliveryBodyStore(dir string) (*BlobDeliveryBodyStore, error) {
	b, err := openLocalBlobs("delivery body store", dir, DeliveryBodyPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobDeliveryBodyStore{blobBase{b}}, nil
}

// PutDeliveryBody stores body create-only at its content address.
func (s *BlobDeliveryBodyStore) PutDeliveryBody(ctx context.Context, deliveryID string, body []byte) (string, error) {
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	key, err := DeliveryBodyKey(deliveryID, digest)
	if err != nil {
		return "", err
	}
	err = s.b.create(ctx, key, DeliveryBodyContentType, body)
	if err == nil {
		return digest, nil
	}
	if !errors.Is(err, errBlobExists) {
		return "", fmt.Errorf("artifacts: put delivery body %q: %w", s.b.uri(key), err)
	}
	// A retried queue of the same delivery: the bytes already there must be
	// these bytes, which the key's digest lets a read prove.
	if _, err := s.GetDeliveryBody(ctx, deliveryID, digest); err != nil {
		return "", fmt.Errorf("artifacts: put delivery body %q: an object is already at the key: %w", s.b.uri(key), err)
	}
	return digest, nil
}

// GetDeliveryBody reads a body back, verified against its digest.
func (s *BlobDeliveryBodyStore) GetDeliveryBody(ctx context.Context, deliveryID, sha256hex string) ([]byte, error) {
	key, err := DeliveryBodyKey(deliveryID, sha256hex)
	if err != nil {
		return nil, err
	}
	body, _, err := s.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return nil, fmt.Errorf("artifacts: get delivery body %q: %w", s.b.uri(key), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get delivery body %q: %w", s.b.uri(key), err)
	}
	if sum := sha256.Sum256(body); hex.EncodeToString(sum[:]) != sha256hex {
		return nil, fmt.Errorf("artifacts: get delivery body %q: %w", s.b.uri(key), ErrDeliveryBodyAltered)
	}
	return body, nil
}

// DeleteDeliveryBody removes a body; a missing one is already gone.
func (s *BlobDeliveryBodyStore) DeleteDeliveryBody(ctx context.Context, deliveryID, sha256hex string) error {
	key, err := DeliveryBodyKey(deliveryID, sha256hex)
	if err != nil {
		return err
	}
	if err := s.b.remove(ctx, key); err != nil {
		return fmt.Errorf("artifacts: delete delivery body %q: %w", s.b.uri(key), err)
	}
	return nil
}
