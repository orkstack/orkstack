package artifacts

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// S3AttemptStore keeps task attempt records in an S3-compatible bucket — the
// store both the Orker writing them and every engine replica reading them
// already share.
//
// It is the reason any replica can answer for any run's retry history
// (AGENTS.md §7): the records are in the bucket, not in the Orker process that
// produced them, so a task whose Orker has long exited reads exactly the same
// as one retrying right now.
type S3AttemptStore struct {
	client *s3.Client
	bucket string
}

// compile-time contract check.
var _ AttemptStore = (*S3AttemptStore)(nil)

// NewS3AttemptStore builds the attempt store from the same ORK_S3_* config the
// artifact, log, orkflow and namespace stores use.
func NewS3AttemptStore(ctx context.Context, cfg Config) (*S3AttemptStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3AttemptStore{client: client, bucket: bucket}, nil
}

// Bucket returns the bucket holding the attempt records.
func (s *S3AttemptStore) Bucket() string { return s.bucket }

// Location is the s3:// URI of the prefix this store owns.
func (s *S3AttemptStore) Location() string { return s.uri(AttemptPrefix + "/") }

func (s *S3AttemptStore) uri(key string) string { return "s3://" + s.bucket + "/" + key }

// PutAttempt writes one record object. An S3 PUT is atomic, so a reader listing
// the prefix either sees the whole record or does not see it at all. The put is
// unconditional: Temporal never runs one attempt number twice, so there is no
// second writer to arbitrate against.
func (s *S3AttemptStore) PutAttempt(ctx context.Context, attempt *orkstackv1.TaskAttempt) error {
	key, document, err := encodeAttemptForStore(attempt)
	if err != nil {
		return err
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(AttemptContentType),
	}); err != nil {
		return fmt.Errorf("artifacts: put task attempt %q: %w", s.uri(key), err)
	}
	return nil
}

// ListAttempts lists the task's prefix and fetches every record in key order.
//
// The bodies are fetched, not merely listed: an attempt record IS its content,
// unlike the log prefix where the key alone answers "which attempts exist".
// There is one small object per attempt and a retry policy's maximum_attempts
// bounds how many, so the listing is not paged by a byte budget the way
// ReadLogChunks is.
func (s *S3AttemptStore) ListAttempts(
	ctx context.Context, runID, taskID string,
) ([]*orkstackv1.TaskAttempt, error) {
	prefix, err := AttemptTaskPrefix(runID, taskID)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, 8)
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list task attempts %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			ref, ok := ParseAttemptKey(key)
			if !ok || ref.RunID != runID || ref.TaskID != taskID {
				continue
			}
			keys = append(keys, key)
		}
	}
	found := make([]*orkstackv1.TaskAttempt, 0, len(keys))
	for _, key := range keys {
		object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if isNotFound(err) {
				// Expired under a lifecycle rule between the listing and the
				// read. A missing record reads as a missing record.
				continue
			}
			return nil, fmt.Errorf("artifacts: get task attempt %q: %w", s.uri(key), err)
		}
		document, readErr := io.ReadAll(object.Body)
		closeErr := object.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("artifacts: get task attempt %q: read body: %w", s.uri(key), readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("artifacts: get task attempt %q: close body: %w", s.uri(key), closeErr)
		}
		record, err := UnmarshalAttempt(document)
		if err != nil {
			return nil, fmt.Errorf("artifacts: get task attempt %q: %w", s.uri(key), err)
		}
		found = append(found, record)
	}
	sortAttempts(found)
	return found, nil
}
