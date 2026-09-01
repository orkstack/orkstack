package artifacts

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3RunSnapshotStore keeps run snapshots in an S3-compatible bucket — the
// store every engine replica already shares. The snapshot a workflow on one
// replica externalized is read back by whichever replica serves the run.
type S3RunSnapshotStore struct {
	client *s3.Client
	bucket string
}

// compile-time contract check.
var _ RunSnapshotStore = (*S3RunSnapshotStore)(nil)

// NewS3RunSnapshotStore builds the snapshot store from the same ORK_S3_*
// config the artifact, log, orkflow and namespace stores use.
func NewS3RunSnapshotStore(ctx context.Context, cfg Config) (*S3RunSnapshotStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3RunSnapshotStore{client: client, bucket: bucket}, nil
}

// Bucket returns the bucket holding the snapshots.
func (s *S3RunSnapshotStore) Bucket() string { return s.bucket }

// Location is the s3:// URI of the prefix this store owns.
func (s *S3RunSnapshotStore) Location() string { return s.uri(SnapshotPrefix + "/") }

// WriteSnapshot writes one snapshot object. An S3 PUT is atomic and the key
// is derived from the coordinates alone, so a retried write lands on the same
// key and returns the same URI.
func (s *S3RunSnapshotStore) WriteSnapshot(
	ctx context.Context,
	namespace, runID string,
	sequence int64,
	data []byte,
) (string, error) {
	key, err := RunSnapshotKey(namespace, runID, sequence)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("artifacts: s3 snapshot %q: empty snapshot", key)
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String(SnapshotContentType),
	}); err != nil {
		return "", fmt.Errorf("artifacts: put snapshot %q: %w", s.uri(key), err)
	}
	return s.uri(key), nil
}

// ReadSnapshot returns the bytes at a URI WriteSnapshot returned.
func (s *S3RunSnapshotStore) ReadSnapshot(ctx context.Context, uri string) ([]byte, error) {
	key, err := s.parseURI(uri)
	if err != nil {
		return nil, err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: s3 snapshot %q: %w", uri, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: s3 snapshot %q: %w", uri, err)
	}
	data, readErr := io.ReadAll(object.Body)
	closeErr := object.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("artifacts: s3 snapshot %q: read body: %w", uri, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("artifacts: s3 snapshot %q: close body: %w", uri, closeErr)
	}
	return data, nil
}

// DeleteSnapshot removes one snapshot. An S3 delete of an absent key already
// succeeds, so the idempotency contract holds without a preceding read.
func (s *S3RunSnapshotStore) DeleteSnapshot(ctx context.Context, uri string) error {
	key, err := s.parseURI(uri)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("artifacts: s3 snapshot %q: delete: %w", uri, err)
	}
	return nil
}

// uri renders the canonical s3:// URI for a snapshot key.
func (s *S3RunSnapshotStore) uri(key string) string {
	return (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + key}).String()
}

// parseURI reverses uri: it accepts only an s3 URI in this store's own bucket
// and snapshot prefix, so a foreign or workload URI can never be read as a
// snapshot.
func (s *S3RunSnapshotStore) parseURI(uri string) (string, error) {
	u, perr := url.Parse(uri)
	if perr != nil {
		return "", fmt.Errorf("artifacts: s3 snapshot %q: %w: %v", uri, ErrInvalidURI, perr)
	}
	if u.Scheme != "s3" {
		return "", fmt.Errorf("artifacts: s3 snapshot %q is not an s3 uri: %w", uri, ErrInvalidURI)
	}
	if u.Host != s.bucket {
		return "", fmt.Errorf("artifacts: s3 snapshot %q names bucket %q, this store owns %q: %w",
			uri, u.Host, s.bucket, ErrInvalidURI)
	}
	key := strings.TrimPrefix(u.Path, "/")
	if !strings.HasPrefix(key, SnapshotPrefix+"/") || !strings.HasSuffix(key, snapshotSuffix) {
		return "", fmt.Errorf("artifacts: s3 snapshot %q is not a snapshot key: %w", uri, ErrInvalidURI)
	}
	return key, nil
}
