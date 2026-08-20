package artifacts

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3RunArchive is the S3-compatible RunArchive. It writes
// s3://{bucket}/metadata/runs/{run-id}.jsonl and the index keys beside it, and
// answers queries with ListObjectsV2 prefix scans that transfer keys only.
type S3RunArchive struct {
	client *s3.Client
	bucket string
}

// compile-time contract check.
var _ RunArchive = (*S3RunArchive)(nil)

// NewS3RunArchive builds an S3-backed run archive from cfg. It shares the
// artifact store's client construction, so one set of ORK_S3_* variables
// configures both.
func NewS3RunArchive(ctx context.Context, cfg Config) (*S3RunArchive, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: s3 run archive: %w", err)
	}
	return &S3RunArchive{client: client, bucket: bucket}, nil
}

// Bucket returns the bucket this archive reads and writes.
func (a *S3RunArchive) Bucket() string { return a.bucket }

// URI renders the canonical s3:// URI of an archived run body.
func (a *S3RunArchive) URI(runID string) (string, error) {
	key, err := RunArchiveKey(runID)
	if err != nil {
		return "", err
	}
	return "s3://" + a.bucket + "/" + key, nil
}

// PutRun writes the body, then the index key. Body first is what makes a crash
// between the two writes safe: the worst case is an archived run the index has
// not advertised yet, never an advertised run whose body is missing.
//
// The body is immutable. A run that already has one keeps it — this is what
// makes an activity retry, a workflow replay and a second engine writing the
// same terminal event converge instead of racing.
func (a *S3RunArchive) PutRun(ctx context.Context, rec RunRecord) error {
	bodyKey, err := RunArchiveKey(rec.RunID)
	if err != nil {
		return err
	}
	indexKey, err := RunIndexKey(rec)
	if err != nil {
		return err
	}

	exists, err := a.exists(ctx, bodyKey)
	if err != nil {
		return fmt.Errorf("artifacts: s3 archive run %q: %w", rec.RunID, err)
	}
	if !exists {
		if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(a.bucket),
			Key:           aws.String(bodyKey),
			Body:          bytes.NewReader(rec.JSONL),
			ContentLength: aws.Int64(int64(len(rec.JSONL))),
			ContentType:   aws.String(RunArchiveContentType),
		}); err != nil {
			return fmt.Errorf("artifacts: s3 archive run %q: write body %q: %w", rec.RunID, bodyKey, err)
		}
	}

	// The index carries everything in its key, so its body is empty. Rewriting
	// it is harmless: the key is derived from the same terminal record.
	if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(a.bucket),
		Key:           aws.String(indexKey),
		Body:          bytes.NewReader(nil),
		ContentLength: aws.Int64(0),
	}); err != nil {
		return fmt.Errorf("artifacts: s3 archive run %q: write index %q: %w", rec.RunID, indexKey, err)
	}
	return nil
}

// GetRun returns the archived JSONL body for runID.
func (a *S3RunArchive) GetRun(ctx context.Context, runID string) ([]byte, error) {
	key, err := RunArchiveKey(runID)
	if err != nil {
		return nil, err
	}
	out, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: s3 archive get %q: %w", runID, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: s3 archive get %q: %w", runID, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("artifacts: s3 archive get %q: read body: %w", runID, err)
	}
	return body, nil
}

// ListRuns answers a query with one prefix scan. Nothing but keys crosses the
// wire: every filtered field lives in the key itself.
func (a *S3RunArchive) ListRuns(ctx context.Context, q RunQuery) ([]RunIndexEntry, error) {
	prefix, startAfter, err := q.ScanPrefix()
	if err != nil {
		return nil, err
	}
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}

	// With orkflow and status pinned, the remaining keys sort by timestamp, so
	// an upper bound ends the scan instead of filtering the rest of the subtree.
	stopAfterUntil := q.Namespace != "" && q.Orkflow != "" && q.Status != "" && !q.Until.IsZero()
	stopKey := prefix + RunStamp(q.Until)

	var entries []RunIndexEntry
	pager := s3.NewListObjectsV2Paginator(a.client, input)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: s3 archive list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if stopAfterUntil && key >= stopKey {
				return q.finish(entries), nil
			}
			entry, ok := ParseRunIndexKey(key)
			if !ok || !q.Matches(entry) {
				continue
			}
			entries = append(entries, entry)
		}
	}
	return q.finish(entries), nil
}

func (a *S3RunArchive) exists(ctx context.Context, key string) (bool, error) {
	_, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("head %q: %w", key, err)
}

// TrimBucketURI reports the object key inside uri when it addresses this
// archive's bucket. It exists so proof tooling can turn a URI back into a key.
func (a *S3RunArchive) TrimBucketURI(uri string) (string, bool) {
	prefix := "s3://" + a.bucket + "/"
	if !strings.HasPrefix(uri, prefix) {
		return "", false
	}
	return strings.TrimPrefix(uri, prefix), true
}
