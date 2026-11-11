package artifacts

import (
	"context"
	"errors"
	"fmt"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// BlobMemoStore keeps memos in the object store every Orker and engine
// replica already shares for commits, so a memo one Orker wrote is one any
// Orker can read. A local directory holds them at exactly the bucket's key
// layout.
type BlobMemoStore struct{ blobBase }

// LocalMemoStore and S3MemoStore are the memo store over a local directory
// and over a bucket: one implementation, two backends.
type (
	LocalMemoStore = BlobMemoStore
	S3MemoStore    = BlobMemoStore
)

var _ MemoStore = (*BlobMemoStore)(nil)

// NewLocalMemoStore opens (creating if needed) the memo tree under dir.
func NewLocalMemoStore(dir string) (*LocalMemoStore, error) {
	b, err := openLocalBlobs("memo store", dir, MemoPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobMemoStore{blobBase{b}}, nil
}

// NewS3MemoStore builds the memo store from the same ORK_S3_* config the
// commit and attempt stores use.
func NewS3MemoStore(ctx context.Context, cfg Config) (*S3MemoStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobMemoStore{blobBase{b}}, nil
}

// GetMemo reads one memo, reading at most MemoMaxBytes+1 bytes of it.
func (s *BlobMemoStore) GetMemo(ctx context.Context, ref MemoRef) (*orkstackv1.CacheMemo, error) {
	key, err := MemoKey(ref)
	if err != nil {
		return nil, err
	}
	document, _, err := s.b.get(ctx, key, MemoMaxBytes+1)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return nil, fmt.Errorf("artifacts: get memo %q: %w", s.b.uri(key), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get memo %q: %w", s.b.uri(key), err)
	}
	return decodeStoredMemo(s.b.uri(key), document)
}

// PutMemo writes one memo with a plain write: last writer wins, and a reader
// never sees a half-written memo.
func (s *BlobMemoStore) PutMemo(ctx context.Context, memo *orkstackv1.CacheMemo) error {
	key, document, err := encodeMemoForStore(memo)
	if err != nil {
		return err
	}
	if err := s.b.put(ctx, key, MemoContentType, document); err != nil {
		return fmt.Errorf("artifacts: put memo %q: %w", s.b.uri(key), err)
	}
	return nil
}

// ListMemos lists the scope's prefix from `after`, in key order, until limit
// memos are collected. LastModified is the listing's: when the memo was last
// written. No memo body is read.
func (s *BlobMemoStore) ListMemos(ctx context.Context, scope MemoScopeRef, after string, limit int) ([]MemoEntry, string, error) {
	prefix, err := MemoScopePrefix(scope)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 1000
	}
	entries := make([]MemoEntry, 0, min(limit, 1000))
	cursor := ""
	err = listPages(ctx, s.b, blobListInput{Prefix: prefix, StartAfter: after}, func(page blobListPage) (bool, error) {
		for i, object := range page.Objects {
			ref, ok := ParseMemoKey(object.Key)
			if !ok {
				continue
			}
			entries = append(entries, MemoEntry{Ref: ref, LastModified: object.Modified, Size: object.Size})
			if len(entries) == limit {
				if i < len(page.Objects)-1 || page.Truncated {
					cursor = object.Key
				}
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, "", fmt.Errorf("artifacts: list memos %q: %w", s.b.uri(prefix), err)
	}
	return entries, cursor, nil
}

// DeleteMemo removes one memo; an absent memo is not an error.
func (s *BlobMemoStore) DeleteMemo(ctx context.Context, ref MemoRef) error {
	key, err := MemoKey(ref)
	if err != nil {
		return err
	}
	if err := s.b.remove(ctx, key); err != nil {
		return fmt.Errorf("artifacts: delete memo %q: %w", s.b.uri(key), err)
	}
	return nil
}
