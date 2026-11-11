package artifacts

import (
	"context"
	"errors"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// BlobAuditStore keeps audit records in the shared object store every engine
// replica already uses, which is why any replica answers for a write another
// replica recorded (AGENTS.md §7). A local directory holds them at exactly
// the bucket's key layout.
type BlobAuditStore struct{ blobBase }

// LocalAuditStore and S3AuditStore are the audit store over a local directory
// and over a bucket: one implementation, two backends.
type (
	LocalAuditStore = BlobAuditStore
	S3AuditStore    = BlobAuditStore
)

var _ AuditStore = (*BlobAuditStore)(nil)

// NewLocalAuditStore opens (creating if needed) the audit tree under dir.
func NewLocalAuditStore(dir string) (*LocalAuditStore, error) {
	b, err := openLocalBlobs("audit store", dir, AuditPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobAuditStore{blobBase{b}}, nil
}

// NewS3AuditStore builds the audit store from the same ORK_S3_* config the
// artifact, attempt and trigger stores use.
func NewS3AuditStore(ctx context.Context, cfg Config) (*S3AuditStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobAuditStore{blobBase{b}}, nil
}

// PutAuditRecord writes one record create-only (S3 If-None-Match: *, local
// link(2)): an existing key is ErrAuditRecordExists and is never overwritten.
func (s *BlobAuditStore) PutAuditRecord(ctx context.Context, rec *orkstackv1.AuditRecord) error {
	key, document, err := encodeAuditForStore(rec)
	if err != nil {
		return err
	}
	if err := s.b.create(ctx, key, AuditContentType, document); err != nil {
		if errors.Is(err, errBlobExists) {
			return fmt.Errorf("artifacts: put audit record %q: %w", s.b.uri(key), ErrAuditRecordExists)
		}
		return fmt.Errorf("artifacts: put audit record %q: %w", s.b.uri(key), err)
	}
	return nil
}

// ListAuditRecords walks the day prefixes newest first.
func (s *BlobAuditStore) ListAuditRecords(ctx context.Context, q AuditQuery) (AuditPage, error) {
	return walkAudit(ctx, s, q)
}

func (s *BlobAuditStore) listAuditDay(ctx context.Context, dayPrefix string) ([]string, error) {
	keys, err := listKeys(ctx, s.b, dayPrefix)
	if err != nil {
		return nil, fmt.Errorf("artifacts: list audit day %q: %w", s.b.uri(dayPrefix), err)
	}
	return keys, nil
}

func (s *BlobAuditStore) fetchAudit(ctx context.Context, key string) ([]byte, bool, error) {
	document, _, err := s.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			// Expired under a lifecycle rule between the listing and the read.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("artifacts: get audit record %q: %w", s.b.uri(key), err)
	}
	return document, true, nil
}
