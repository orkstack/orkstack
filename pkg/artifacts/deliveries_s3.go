package artifacts

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// S3DeliveryStore keeps delivery records in the shared S3-compatible bucket
// every engine replica already uses, which is why any replica answers for a
// delivery another replica made (AGENTS.md §7).
type S3DeliveryStore struct {
	client *s3.Client
	bucket string
	// bodiesRead counts the record bodies this store has asked the backend
	// for (BodiesRead).
	bodiesRead atomic.Int64
}

var _ DeliveryStore = (*S3DeliveryStore)(nil)

// NewS3DeliveryStore builds the delivery store from the same ORK_S3_* config
// the artifact, attempt, trigger and audit stores use.
func NewS3DeliveryStore(ctx context.Context, cfg Config) (*S3DeliveryStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3DeliveryStore{client: client, bucket: bucket}, nil
}

// BodiesRead is how many delivery record bodies this store has requested:
// one per GetObject fetchDelivery issues, whatever the answer, NotFound
// included, because a request is the cost. Only tests read it, always as the
// delta around one call: it is how a test proves a filtered listing reads a
// body only for a record whose key matched.
func (s *S3DeliveryStore) BodiesRead() int64 { return s.bodiesRead.Load() }

func (s *S3DeliveryStore) uri(key string) string { return "s3://" + s.bucket + "/" + key }

// PutDelivery writes one record with a conditional create (If-None-Match: *),
// exactly as S3TriggerStore.Create does: an existing key is ErrDeliveryExists
// and is never overwritten.
func (s *S3DeliveryStore) PutDelivery(ctx context.Context, rec *orkstackv1.Delivery) error {
	key, document, err := encodeDeliveryForStore(rec)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(document),
		ContentLength: aws.Int64(int64(len(document))),
		ContentType:   aws.String(DeliveryContentType),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("artifacts: put delivery record %q: %w", s.uri(key), ErrDeliveryExists)
		}
		return fmt.Errorf("artifacts: put delivery record %q: %w", s.uri(key), err)
	}
	return nil
}

// GetDelivery is one keys-only list of the id's prefix, then one get.
func (s *S3DeliveryStore) GetDelivery(ctx context.Context, id string) (*orkstackv1.Delivery, bool, error) {
	return getDelivery(ctx, s, id)
}

// ListDeliveries walks the day prefixes newest first.
func (s *S3DeliveryStore) ListDeliveries(ctx context.Context, q DeliveryQuery) (DeliveryPage, error) {
	return walkDeliveries(ctx, s, q)
}

func (s *S3DeliveryStore) listDeliveryRange(ctx context.Context, dayPrefix, from, to string, max int) ([]string, error) {
	// StartAfter skips every key older than the window without listing it;
	// the walk stops at the first key past its end. Listing is ascending, so
	// both bounds are one comparison each.
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:     aws.String(s.bucket),
		Prefix:     aws.String(dayPrefix),
		StartAfter: aws.String(from),
	})
	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list delivery day %q: %w", s.uri(dayPrefix), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if key >= to {
				return keys, nil
			}
			if key <= from {
				continue
			}
			keys = append(keys, key)
			if len(keys) > max {
				return keys, nil
			}
		}
	}
	return keys, nil
}

func (s *S3DeliveryStore) listDeliveryID(ctx context.Context, idPrefix string) ([]string, error) {
	// One request: a delivery has exactly one record, so two keys are
	// already the anomaly getDelivery reports, and a third adds nothing.
	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(idPrefix),
		MaxKeys: aws.Int32(2),
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: list delivery %q: %w", s.uri(idPrefix), err)
	}
	keys := make([]string, 0, len(out.Contents))
	for _, object := range out.Contents {
		keys = append(keys, aws.ToString(object.Key))
	}
	return keys, nil
}

func (s *S3DeliveryStore) fetchDelivery(ctx context.Context, key string) ([]byte, bool, error) {
	s.bodiesRead.Add(1)
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			// Expired under a lifecycle rule between the listing and the read.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("artifacts: get delivery record %q: %w", s.uri(key), err)
	}
	document, readErr := io.ReadAll(object.Body)
	closeErr := object.Body.Close()
	if readErr != nil {
		return nil, false, fmt.Errorf("artifacts: get delivery record %q: read body: %w", s.uri(key), readErr)
	}
	if closeErr != nil {
		return nil, false, fmt.Errorf("artifacts: get delivery record %q: close body: %w", s.uri(key), closeErr)
	}
	return document, true, nil
}
