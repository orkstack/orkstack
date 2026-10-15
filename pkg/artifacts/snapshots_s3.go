package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
	return s.putSnapshot(ctx, key, data)
}

// WriteSnapshotOverlay writes one snapshot overlay object at
// RunSnapshotOverlayKey, exactly as WriteSnapshot writes a full snapshot.
func (s *S3RunSnapshotStore) WriteSnapshotOverlay(
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

// putSnapshot writes one snapshot or overlay object at key.
func (s *S3RunSnapshotStore) putSnapshot(ctx context.Context, key string, data []byte) (string, error) {
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
	if !isSnapshotObjectKey(key) {
		return "", fmt.Errorf("artifacts: s3 snapshot %q is not a snapshot key: %w", uri, ErrInvalidURI)
	}
	return key, nil
}

// ContinuationURI is the URI this store gives the continuation object at
// RunContinuationKey(namespace, runID, generation, kind, sha256hex).
func (s *S3RunSnapshotStore) ContinuationURI(namespace, runID string, generation int64, kind, sha256hex string) (string, error) {
	return continuationURI(s.uri, namespace, runID, generation, kind, sha256hex)
}

// WriteContinuation writes one continuation object create-if-absent
// (If-None-Match: *) at its content-addressed key. When the backend refuses
// because an object is already there, the existing bytes are read back and
// the write is success only when they hash to the same SHA-256.
func (s *S3RunSnapshotStore) WriteContinuation(
	ctx context.Context,
	namespace, runID string,
	generation int64,
	kind string,
	data []byte,
) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("artifacts: s3 continuation: empty object")
	}
	sum := ContinuationSHA256(data)
	key, err := RunContinuationKey(namespace, runID, generation, kind, sum)
	if err != nil {
		return "", err
	}
	uri := s.uri(key)
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String(ContinuationContentType),
		IfNoneMatch:   aws.String("*"),
	})
	if err == nil {
		return uri, nil
	}
	if !isPreconditionFailed(err) {
		return "", fmt.Errorf("artifacts: put continuation %q: %w", uri, err)
	}
	existing, readErr := s.ReadSnapshot(ctx, uri)
	if readErr != nil {
		return "", fmt.Errorf("artifacts: put continuation %q: read existing object: %w", uri, readErr)
	}
	if ContinuationSHA256(existing) != sum {
		return "", fmt.Errorf("artifacts: put continuation %q: an object with other bytes is already at the key", uri)
	}
	return uri, nil
}

// ReadContinuationRange reads length bytes at offset of the continuation
// object at uri with one ranged GET; the object's total size comes from the
// response's Content-Range and must be wantSize.
func (s *S3RunSnapshotStore) ReadContinuationRange(
	ctx context.Context,
	uri string,
	wantSize, offset, length int64,
) ([]byte, error) {
	key, err := s.parseURI(uri)
	if err != nil {
		return nil, err
	}
	if !isContinuationKey(key) {
		return nil, fmt.Errorf("artifacts: s3 continuation %q is not a continuation key: %w", uri, ErrInvalidURI)
	}
	if err := checkContinuationRange(uri, wantSize, offset, length); err != nil {
		return nil, err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: s3 continuation %q: %w", uri, ErrNotFound)
		}
		if isInvalidRange(err) {
			// The range is inside wantSize (checked above), so the stored
			// object is shorter than its reference says.
			return nil, fmt.Errorf("artifacts: s3 continuation %q is shorter than the %d bytes its reference records: %w",
				uri, wantSize, ErrContinuationSize)
		}
		return nil, fmt.Errorf("artifacts: s3 continuation %q: %w", uri, err)
	}
	defer object.Body.Close()
	total, ok := contentRangeTotal(aws.ToString(object.ContentRange))
	if !ok {
		return nil, fmt.Errorf("artifacts: s3 continuation %q: unreadable Content-Range %q",
			uri, aws.ToString(object.ContentRange))
	}
	if total != wantSize {
		return nil, fmt.Errorf("artifacts: s3 continuation %q is %d bytes, its reference records %d: %w",
			uri, total, wantSize, ErrContinuationSize)
	}
	data, err := io.ReadAll(io.LimitReader(object.Body, length+1))
	if err != nil {
		return nil, fmt.Errorf("artifacts: s3 continuation %q: read body: %w", uri, err)
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("artifacts: s3 continuation %q: range [%d, %d) returned %d bytes",
			uri, offset, offset+length, len(data))
	}
	return data, nil
}

// contentRangeTotal parses the complete length of "bytes a-b/total".
func contentRangeTotal(header string) (int64, bool) {
	slash := strings.LastIndexByte(header, '/')
	if !strings.HasPrefix(header, "bytes ") || slash < 0 {
		return 0, false
	}
	total, err := strconv.ParseInt(header[slash+1:], 10, 64)
	if err != nil || total < 0 {
		return 0, false
	}
	return total, true
}

// isInvalidRange reports the backend's 416 for a range past the object's end.
func isInvalidRange(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
		return true
	}
	var respErr *smithyhttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable
}
