package artifacts

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// S3MemoStore keeps memos in the bucket every Orker and engine replica already
// shares for commits, so a memo one Orker wrote is one any Orker can read.
type S3MemoStore struct {
	client *s3.Client
	bucket string
}

var _ MemoStore = (*S3MemoStore)(nil)

// NewS3MemoStore builds the memo store from the same ORK_S3_* config the
// commit and attempt stores use.
func NewS3MemoStore(ctx context.Context, cfg Config) (*S3MemoStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3MemoStore{client: client, bucket: bucket}, nil
}

// Bucket returns the bucket holding the memos.
func (s *S3MemoStore) Bucket() string { return s.bucket }

func (s *S3MemoStore) uri(key string) string { return "s3://" + s.bucket + "/" + key }

// GetMemo reads one memo, reading at most MemoMaxBytes+1 bytes of it.
func (s *S3MemoStore) GetMemo(ctx context.Context, ref MemoRef) (*orkstackv1.CacheMemo, error) {
	key, err := MemoKey(ref)
	if err != nil {
		return nil, err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: get memo %q: %w", s.uri(key), ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: get memo %q: %w", s.uri(key), err)
	}
	document, readErr := io.ReadAll(io.LimitReader(object.Body, MemoMaxBytes+1))
	closeErr := object.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("artifacts: get memo %q: read body: %w", s.uri(key), readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("artifacts: get memo %q: close body: %w", s.uri(key), closeErr)
	}
	return decodeStoredMemo(s.uri(key), document)
}

// PutMemo writes one memo with a plain PUT: last writer wins.
func (s *S3MemoStore) PutMemo(ctx context.Context, memo *orkstackv1.CacheMemo) error {
	key, document, err := encodeMemoForStore(memo)
	if err != nil {
		return err
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(MemoContentType),
	}); err != nil {
		return fmt.Errorf("artifacts: put memo %q: %w", s.uri(key), err)
	}
	return nil
}

// ListMemos pages ListObjectsV2 from `after`, in the bucket's key order, until
// limit memos are collected. LastModified comes from the listing; no memo body
// is read.
func (s *S3MemoStore) ListMemos(ctx context.Context, scope MemoScopeRef, after string, limit int) ([]MemoEntry, string, error) {
	prefix, err := MemoScopePrefix(scope)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 1000
	}
	input := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)}
	if after != "" {
		input.StartAfter = aws.String(after)
	}
	entries := make([]MemoEntry, 0, min(limit, 1000))
	pager := s3.NewListObjectsV2Paginator(s.client, input)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, "", fmt.Errorf("artifacts: list memos %q: %w", s.uri(prefix), err)
		}
		for i, object := range page.Contents {
			key := aws.ToString(object.Key)
			ref, ok := ParseMemoKey(key)
			if !ok {
				continue
			}
			var modified time.Time
			if object.LastModified != nil {
				modified = object.LastModified.UTC()
			}
			entries = append(entries, MemoEntry{Ref: ref, LastModified: modified, Size: aws.ToInt64(object.Size)})
			if len(entries) == limit {
				if i == len(page.Contents)-1 && !pager.HasMorePages() {
					return entries, "", nil
				}
				return entries, key, nil
			}
		}
	}
	return entries, "", nil
}

// DeleteMemo removes one memo. S3 DELETE of an absent key succeeds, so it is
// idempotent by construction.
func (s *S3MemoStore) DeleteMemo(ctx context.Context, ref MemoRef) error {
	key, err := MemoKey(ref)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("artifacts: delete memo %q: %w", s.uri(key), err)
	}
	return nil
}
