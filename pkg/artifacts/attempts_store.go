package artifacts

import (
	"context"
	"errors"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// BlobAttemptStore keeps task attempt records in the object store both the
// Orker writing them and every engine replica reading them already share.
//
// It is the reason any replica can answer for any run's retry history
// (AGENTS.md §7): the records are in the store, not in the Orker process that
// produced them, so a task whose Orker has long exited reads exactly the same
// as one retrying right now. A local directory holds them at exactly the
// bucket's key layout.
type BlobAttemptStore struct{ blobBase }

// LocalAttemptStore and S3AttemptStore are the attempt store over a local
// directory and over a bucket: one implementation, two backends.
type (
	LocalAttemptStore = BlobAttemptStore
	S3AttemptStore    = BlobAttemptStore
)

// compile-time contract checks.
var (
	_ AttemptStore          = (*BlobAttemptStore)(nil)
	_ AttemptDocumentReader = (*BlobAttemptStore)(nil)
	_ RunAttemptReader      = (*BlobAttemptStore)(nil)
)

// NewLocalAttemptStore opens (creating if needed) the attempt tree under dir.
func NewLocalAttemptStore(dir string) (*LocalAttemptStore, error) {
	b, err := openLocalBlobs("attempt store", dir, AttemptPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobAttemptStore{blobBase{b}}, nil
}

// NewS3AttemptStore builds the attempt store from the same ORK_S3_* config the
// artifact, log, orkflow and namespace stores use.
func NewS3AttemptStore(ctx context.Context, cfg Config) (*S3AttemptStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobAttemptStore{blobBase{b}}, nil
}

// Location is the URI of the prefix this store owns.
func (s *BlobAttemptStore) Location() string { return s.b.locate(AttemptPrefix + "/") }

// PutAttempt writes one record atomically, so a reader listing the prefix
// either sees the whole record or does not see it at all: a partially
// readable record is worse than a missing one. The write is unconditional:
// Temporal never runs one attempt number twice, so there is no second writer
// to arbitrate against.
func (s *BlobAttemptStore) PutAttempt(ctx context.Context, attempt *orkstackv1.TaskAttempt) error {
	key, document, err := encodeAttemptForStore(attempt)
	if err != nil {
		return err
	}
	if err := s.b.put(ctx, key, AttemptContentType, document); err != nil {
		return fmt.Errorf("artifacts: put task attempt %q: %w", s.b.uri(key), err)
	}
	return nil
}

// ListAttempts returns one task's records, ascending by attempt.
func (s *BlobAttemptStore) ListAttempts(
	ctx context.Context, runID, taskID string, item Item,
) ([]*orkstackv1.TaskAttempt, error) {
	documents, err := s.ReadAttemptDocuments(ctx, runID, taskID, item)
	if err != nil {
		return nil, err
	}
	return attemptRecords(documents), nil
}

// ReadAttemptDocuments lists the task's prefix and fetches every record in
// key order, with its bytes, ascending by attempt; ListAttempts is its
// projection.
//
// The bodies are fetched, not merely listed: an attempt record IS its content,
// unlike the log prefix where the key alone answers "which attempts exist".
// There is one small object per attempt and a retry policy's maximum_attempts
// bounds how many, so the listing is not paged by a byte budget the way
// ReadLogChunks is.
func (s *BlobAttemptStore) ReadAttemptDocuments(
	ctx context.Context, runID, taskID string, item Item,
) ([]AttemptDocument, error) {
	prefix, err := AttemptScopePrefix(runID, taskID, item)
	if err != nil {
		return nil, err
	}
	refs := make([]AttemptRef, 0, 8)
	keys := make([]string, 0, 8)
	err = listObjects(ctx, s.b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
		// A fan-out item's records lie below a task's own scope: the filter
		// keeps NoItem from ever reading a fan-out's items.
		ref, ok := ParseAttemptKey(object.Key)
		if ok && ref.RunID == runID && ref.TaskID == taskID && ref.Item == item {
			keys = append(keys, object.Key)
			refs = append(refs, ref)
		}
		return false, nil
	})
	if err != nil {
		// A task with no records reads empty, which a listing already
		// answers: that is the ordinary state of a task that has not run yet,
		// and of every run finished before this key family existed.
		return nil, fmt.Errorf("artifacts: list task attempts %q: %w", s.b.uri(prefix), err)
	}
	found := make([]AttemptDocument, 0, len(keys))
	for i, key := range keys {
		document, _, err := s.b.get(ctx, key, 0)
		if err != nil {
			if errors.Is(err, errBlobNotFound) {
				// Expired under a lifecycle rule between the listing and the
				// read. A missing record reads as a missing record.
				continue
			}
			return nil, fmt.Errorf("artifacts: get task attempt %q: %w", s.b.uri(key), err)
		}
		record, err := UnmarshalAttempt(document)
		if err != nil {
			return nil, fmt.Errorf("artifacts: get task attempt %q: %w", s.b.uri(key), err)
		}
		found = append(found, AttemptDocument{Ref: refs[i], Key: key, Raw: document, Record: record})
	}
	sortAttemptDocuments(found)
	return found, nil
}

// ListRunAttemptRefs pages one listing over the run's prefix, as ListAttempts
// pages one task's, and opens no record.
func (s *BlobAttemptStore) ListRunAttemptRefs(ctx context.Context, runID string) ([]AttemptRef, error) {
	prefix, err := RunAttemptPrefix(runID)
	if err != nil {
		return nil, err
	}
	keys, err := listKeys(ctx, s.b, prefix)
	if err != nil {
		return nil, fmt.Errorf("artifacts: list run attempts %q: %w", s.b.uri(prefix), err)
	}
	return runAttemptRefs(runID, keys), nil
}

// GetAttempt reads one record; a missing one matches ErrNotFound.
func (s *BlobAttemptStore) GetAttempt(ctx context.Context, ref AttemptRef) (*orkstackv1.TaskAttempt, error) {
	key, err := AttemptKey(ref)
	if err != nil {
		return nil, err
	}
	document, _, err := s.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return nil, fmt.Errorf("artifacts: get task attempt %q: %w", s.b.uri(key), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get task attempt %q: %w", s.b.uri(key), err)
	}
	record, err := UnmarshalAttempt(document)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get task attempt %q: %w", s.b.uri(key), err)
	}
	return record, nil
}
