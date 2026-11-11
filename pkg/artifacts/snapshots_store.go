package artifacts

import (
	"context"
	"errors"
	"fmt"
)

// BlobRunSnapshotStore keeps run snapshots in the object store every engine
// replica already shares: the snapshot a workflow on one replica externalized
// is read back by whichever replica serves the run. A local directory holds
// them at exactly the bucket's key layout.
type BlobRunSnapshotStore struct{ blobBase }

// LocalRunSnapshotStore and S3RunSnapshotStore are the snapshot store over a
// local directory and over a bucket: one implementation, two backends.
type (
	LocalRunSnapshotStore = BlobRunSnapshotStore
	S3RunSnapshotStore    = BlobRunSnapshotStore
)

var _ RunSnapshotStore = (*BlobRunSnapshotStore)(nil)

// NewLocalRunSnapshotStore opens (creating if needed) the snapshot tree under
// dir.
func NewLocalRunSnapshotStore(dir string) (*LocalRunSnapshotStore, error) {
	b, err := openLocalBlobs("snapshot store", dir, SnapshotPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobRunSnapshotStore{blobBase{b}}, nil
}

// NewS3RunSnapshotStore builds the snapshot store from the same ORK_S3_*
// config the artifact, log and archive stores use.
func NewS3RunSnapshotStore(ctx context.Context, cfg Config) (*S3RunSnapshotStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobRunSnapshotStore{blobBase{b}}, nil
}

// Location is the URI of the prefix this store owns.
func (s *BlobRunSnapshotStore) Location() string { return s.b.locate(SnapshotPrefix + "/") }

// WriteSnapshot writes one snapshot. The key is deterministic, so a retried
// write overwrites the same object with the same bytes and returns the same
// URI.
func (s *BlobRunSnapshotStore) WriteSnapshot(
	ctx context.Context,
	namespace, runID string,
	sequence int64,
	data []byte,
) (string, error) {
	key, err := RunSnapshotKey(namespace, runID, sequence)
	if err != nil {
		return "", err
	}
	return s.putSnapshot(ctx, key, data)
}

// WriteSnapshotOverlay writes one overlay beside the run's full snapshots.
func (s *BlobRunSnapshotStore) WriteSnapshotOverlay(
	ctx context.Context,
	namespace, runID string,
	sequence int64,
	data []byte,
) (string, error) {
	key, err := RunSnapshotOverlayKey(namespace, runID, sequence)
	if err != nil {
		return "", err
	}
	return s.putSnapshot(ctx, key, data)
}

func (s *BlobRunSnapshotStore) putSnapshot(ctx context.Context, key string, data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("artifacts: snapshot %q: empty snapshot", s.b.uri(key))
	}
	if err := s.b.put(ctx, key, SnapshotContentType, data); err != nil {
		return "", fmt.Errorf("artifacts: put snapshot %q: %w", s.b.uri(key), err)
	}
	return s.b.locate(key), nil
}

// ReadSnapshot returns the bytes at a URI WriteSnapshot, WriteSnapshotOverlay
// or WriteContinuation returned.
func (s *BlobRunSnapshotStore) ReadSnapshot(ctx context.Context, uri string) ([]byte, error) {
	key, err := s.parseURI(uri)
	if err != nil {
		return nil, err
	}
	data, _, err := s.b.get(ctx, key, 0)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return nil, fmt.Errorf("artifacts: snapshot %q: %w", uri, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: snapshot %q: %w", uri, err)
	}
	return data, nil
}

// DeleteSnapshot removes one snapshot. Removing a snapshot that is already
// gone returns nil.
func (s *BlobRunSnapshotStore) DeleteSnapshot(ctx context.Context, uri string) error {
	key, err := s.parseURI(uri)
	if err != nil {
		return err
	}
	if err := s.b.remove(ctx, key); err != nil {
		return fmt.Errorf("artifacts: snapshot %q: delete: %w", uri, err)
	}
	return nil
}

// parseURI accepts only a URI of this store inside its own snapshot prefix,
// so a foreign or workload URI can never be read as a snapshot.
func (s *BlobRunSnapshotStore) parseURI(uri string) (string, error) {
	key, err := s.b.resolve(uri)
	if err != nil {
		return "", fmt.Errorf("artifacts: snapshot %w", err)
	}
	if !isSnapshotObjectKey(key) {
		return "", fmt.Errorf("artifacts: snapshot %q is not a snapshot key: %w", uri, ErrInvalidURI)
	}
	return key, nil
}

// ContinuationURI is the URI this store gives the continuation object at
// RunContinuationKey(namespace, runID, generation, kind, sha256hex).
func (s *BlobRunSnapshotStore) ContinuationURI(namespace, runID string, generation int64, kind, sha256hex string) (string, error) {
	return continuationURI(s.b.locate, namespace, runID, generation, kind, sha256hex)
}

// WriteContinuation writes one continuation object create-if-absent at its
// content-addressed key (S3 If-None-Match: *, local link(2)). An object
// already at the key is success only when its bytes hash to the same
// SHA-256.
func (s *BlobRunSnapshotStore) WriteContinuation(
	ctx context.Context,
	namespace, runID string,
	generation int64,
	kind string,
	data []byte,
) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("artifacts: continuation: empty object")
	}
	sum := ContinuationSHA256(data)
	key, err := RunContinuationKey(namespace, runID, generation, kind, sum)
	if err != nil {
		return "", err
	}
	uri := s.b.locate(key)
	err = s.b.create(ctx, key, ContinuationContentType, data)
	if err == nil {
		return uri, nil
	}
	if !errors.Is(err, errBlobExists) {
		return "", fmt.Errorf("artifacts: put continuation %q: %w", uri, err)
	}
	existing, _, readErr := s.b.get(ctx, key, 0)
	if readErr != nil {
		return "", fmt.Errorf("artifacts: put continuation %q: read existing object: %w", uri, readErr)
	}
	if ContinuationSHA256(existing) != sum {
		return "", fmt.Errorf("artifacts: put continuation %q: an object with other bytes is already at the key", uri)
	}
	return uri, nil
}

// ReadContinuationRange reads length bytes at offset of the continuation
// object at uri, after checking that the stored object is wantSize bytes.
func (s *BlobRunSnapshotStore) ReadContinuationRange(
	ctx context.Context,
	uri string,
	wantSize, offset, length int64,
) ([]byte, error) {
	key, err := s.parseURI(uri)
	if err != nil {
		return nil, err
	}
	if !isContinuationKey(key) {
		return nil, fmt.Errorf("artifacts: continuation %q is not a continuation key: %w", uri, ErrInvalidURI)
	}
	if err := checkContinuationRange(uri, wantSize, offset, length); err != nil {
		return nil, err
	}
	data, total, err := s.b.getRange(ctx, key, offset, length)
	switch {
	case errors.Is(err, errBlobNotFound):
		return nil, fmt.Errorf("artifacts: continuation %q: %w", uri, ErrNotFound)
	case errors.Is(err, errBlobRange):
		return nil, fmt.Errorf("artifacts: continuation %q is shorter than the %d bytes its reference records: %w",
			uri, wantSize, ErrContinuationSize)
	case err != nil:
		return nil, fmt.Errorf("artifacts: continuation %q: %w", uri, err)
	}
	if total != wantSize {
		return nil, fmt.Errorf("artifacts: continuation %q is %d bytes, its reference records %d: %w",
			uri, total, wantSize, ErrContinuationSize)
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("artifacts: continuation %q: range [%d, %d) returned %d bytes",
			uri, offset, offset+length, len(data))
	}
	return data, nil
}
