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

// S3AuditStore keeps audit records in the shared S3-compatible bucket every
// engine replica already uses, which is why any replica answers for a write
// another replica recorded (AGENTS.md §7).
type S3AuditStore struct {
	client *s3.Client
	bucket string
}

var _ AuditStore = (*S3AuditStore)(nil)

// NewS3AuditStore builds the audit store from the same ORK_S3_* config the
// artifact, attempt and trigger stores use.
func NewS3AuditStore(ctx context.Context, cfg Config) (*S3AuditStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3AuditStore{client: client, bucket: bucket}, nil
}

func (s *S3AuditStore) uri(key string) string { return "s3://" + s.bucket + "/" + key }

// PutAuditRecord writes one record with a conditional create (If-None-Match:
// *), exactly as S3TriggerStore.Create does: an existing key is
// ErrAuditRecordExists and is never overwritten.
func (s *S3AuditStore) PutAuditRecord(ctx context.Context, rec *orkstackv1.AuditRecord) error {
	key, document, err := encodeAuditForStore(rec)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(AuditContentType),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("artifacts: put audit record %q: %w", s.uri(key), ErrAuditRecordExists)
		}
		return fmt.Errorf("artifacts: put audit record %q: %w", s.uri(key), err)
	}
	return nil
}

// ListAuditRecords walks the day prefixes newest first.
func (s *S3AuditStore) ListAuditRecords(ctx context.Context, q AuditQuery) (AuditPage, error) {
	return walkAudit(ctx, s, q)
}

func (s *S3AuditStore) listAuditDay(ctx context.Context, dayPrefix string) ([]string, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(dayPrefix),
	})
	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list audit day %q: %w", s.uri(dayPrefix), err)
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}
	return keys, nil
}

func (s *S3AuditStore) fetchAudit(ctx context.Context, key string) ([]byte, bool, error) {
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			// Expired under a lifecycle rule between the listing and the read.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("artifacts: get audit record %q: %w", s.uri(key), err)
	}
	document, readErr := io.ReadAll(object.Body)
	closeErr := object.Body.Close()
	if readErr != nil {
		return nil, false, fmt.Errorf("artifacts: get audit record %q: read body: %w", s.uri(key), readErr)
	}
	if closeErr != nil {
		return nil, false, fmt.Errorf("artifacts: get audit record %q: close body: %w", s.uri(key), closeErr)
	}
	return document, true, nil
}
