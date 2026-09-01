package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3RunArchive is the S3-compatible RunArchive. It writes
// s3://{bucket}/metadata/runs/{run-id}.jsonl and the index keys beside it, and
// answers queries with ListObjectsV2 prefix scans that transfer keys only.
type S3RunArchive struct {
	client *s3.Client
	bucket string
	// indexKeysScanned counts index keys enumerated by ListRuns. See
	// IndexKeysScanned.
	indexKeysScanned atomic.Int64
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

// PutRun writes the body and object references before publishing the index key.
// An interrupted write can be retried without advertising a run whose body or
// reference accounting is incomplete.
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
	objectIDs := dedupeObjectIDs(rec.ObjectIDs)
	listKey, err := RunObjectListKey(rec.RunID)
	if err != nil {
		return err
	}
	listBody, err := json.Marshal(objectIDs)
	if err != nil {
		return fmt.Errorf("artifacts: s3 archive run %q: encode object list: %w", rec.RunID, err)
	}
	refKeys := make([]string, 0, len(objectIDs))
	for _, objectID := range objectIDs {
		refKey, err := RunObjectRefKey(objectID, rec.RunID)
		if err != nil {
			return err
		}
		refKeys = append(refKeys, refKey)
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

	// The list keeps exact object ids out of marker-key components and makes
	// deletion proportional to this run's references rather than the archive.
	if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(a.bucket), Key: aws.String(listKey), Body: bytes.NewReader(listBody),
		ContentLength: aws.Int64(int64(len(listBody))), ContentType: aws.String(RunCleanupContentType),
	}); err != nil {
		return fmt.Errorf("artifacts: s3 archive run %q: write object list %q: %w", rec.RunID, listKey, err)
	}
	if err := a.putObjectRefs(ctx, refKeys); err != nil {
		return fmt.Errorf("artifacts: s3 archive run %q: %w", rec.RunID, err)
	}
	// The index carries everything in its key, so its body is empty. Publishing
	// it last makes all prior durable records visible to cleanup first.
	if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(a.bucket), Key: aws.String(indexKey), Body: bytes.NewReader(nil), ContentLength: aws.Int64(0),
	}); err != nil {
		return fmt.Errorf("artifacts: s3 archive run %q: write index %q: %w", rec.RunID, indexKey, err)
	}
	return nil
}

// putObjectRefs writes every reference marker with bounded concurrency.
//
// One PUT per referenced object made archiving linear in a run's artifact
// count, and archiving happens inside a projection activity with a 10-second
// budget whose failure is only logged — so a wide fan-out run could quietly
// end up with no archive body at all, which is the only record of it once
// Temporal retention passes. Overlapping the round trips keeps that cost flat
// enough to stay inside the budget; it does not remove the linearity.
//
// Every marker is still written before PutRun's caller publishes the index
// key, and any single failure fails the whole call. A dropped marker would
// under-count references and let DeleteUnreferencedObjects delete an object
// another run still needs, so there is no partial-success path here.
func (a *S3RunArchive) putObjectRefs(ctx context.Context, refKeys []string) error {
	if len(refKeys) == 0 {
		return nil
	}
	errs := make([]error, len(refKeys))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, archiveRefWriteConcurrency)
	)
	for i, refKey := range refKeys {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, refKey string) {
			defer wg.Done()
			defer func() { <-slots }()
			if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(a.bucket), Key: aws.String(refKey),
				Body: bytes.NewReader(nil), ContentLength: aws.Int64(0),
			}); err != nil {
				errs[i] = fmt.Errorf("write object reference %q: %w", refKey, err)
			}
		}(i, refKey)
	}
	wg.Wait()
	return errors.Join(errs...)
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

// CleanupState returns a run's immutable cleanup sidecar.
func (a *S3RunArchive) CleanupState(ctx context.Context, runID string) (RunCleanupState, error) {
	key, err := RunCleanupKey(runID)
	if err != nil {
		return RunCleanupState{}, err
	}
	out, err := a.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(key)})
	if isNotFound(err) {
		return RunCleanupState{}, fmt.Errorf("artifacts: s3 cleanup state %q: %w", runID, ErrNotFound)
	}
	if err != nil {
		return RunCleanupState{}, fmt.Errorf("artifacts: s3 cleanup state %q: %w", runID, err)
	}
	defer out.Body.Close()
	var state RunCleanupState
	if err := json.NewDecoder(out.Body).Decode(&state); err != nil {
		return RunCleanupState{}, fmt.Errorf("artifacts: s3 cleanup state %q: decode: %w", runID, err)
	}
	return state, nil
}

// MarkArtifactsCleaned writes cleanup state once using a conditional put.
func (a *S3RunArchive) MarkArtifactsCleaned(ctx context.Context, runID string, deletedAt time.Time) error {
	key, err := RunCleanupKey(runID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(RunCleanupState{RunID: runID, ArtifactsCleaned: true, TemporalDeletedAt: deletedAt.UTC()})
	if err != nil {
		return fmt.Errorf("artifacts: s3 mark cleanup %q: %w", runID, err)
	}
	_, err = a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(a.bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String(RunCleanupContentType),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return nil
		}
		return fmt.Errorf("artifacts: s3 mark cleanup %q: %w", runID, err)
	}
	return nil
}

// DeleteRunArtifacts removes run-owned logs, commits, and reference markers.
// Removing a marker only drops this run's claim: shared workload objects stay
// untouched until DeleteUnreferencedObjects observes that no claims remain.
func (a *S3RunArchive) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (CleanupResult, error) {
	logPrefix, err := RunLogPrefix(runID)
	if err != nil {
		return CleanupResult{}, err
	}
	commitPrefix, err := RunCommitPrefix(namespace, runID)
	if err != nil {
		return CleanupResult{}, err
	}
	var result CleanupResult
	for _, prefix := range []string{logPrefix, commitPrefix} {
		pager := s3.NewListObjectsV2Paginator(a.client, &s3.ListObjectsV2Input{Bucket: aws.String(a.bucket), Prefix: aws.String(prefix)})
		for pager.HasMorePages() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return result, fmt.Errorf("artifacts: s3 cleanup run %q list %q: %w", runID, prefix, err)
			}
			for _, object := range page.Contents {
				if _, err := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(a.bucket), Key: object.Key}); err != nil {
					return result, fmt.Errorf("artifacts: s3 cleanup run %q delete %q: %w", runID, aws.ToString(object.Key), err)
				}
				result.ObjectsDeleted++
				result.BytesReclaimed += aws.ToInt64(object.Size)
			}
		}
	}
	listKey, err := RunObjectListKey(runID)
	if err != nil {
		return result, err
	}
	listObject, err := a.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(listKey)})
	if isNotFound(err) {
		// Archives written before reference accounting have no list. Falling
		// back to a namespace-wide scan would make every cleanup unbounded.
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("artifacts: s3 cleanup run %q read object list: %w", runID, err)
	}
	var objectIDs []string
	decodeErr := json.NewDecoder(listObject.Body).Decode(&objectIDs)
	closeErr := listObject.Body.Close()
	if decodeErr != nil {
		return result, fmt.Errorf("artifacts: s3 cleanup run %q decode object list: %w", runID, decodeErr)
	}
	if closeErr != nil {
		return result, fmt.Errorf("artifacts: s3 cleanup run %q close object list: %w", runID, closeErr)
	}
	for _, objectID := range objectIDs {
		key, err := RunObjectRefKey(objectID, runID)
		if err != nil {
			return result, err
		}
		if _, err := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(key)}); err != nil {
			return result, fmt.Errorf("artifacts: s3 cleanup run %q delete object reference %q: %w", runID, key, err)
		}
	}
	if _, err := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(listKey)}); err != nil {
		return result, fmt.Errorf("artifacts: s3 cleanup run %q delete object list %q: %w", runID, listKey, err)
	}

	return result, nil
}

// DeleteUnreferencedObjects removes S3 content-addressed objects only after
// their reference prefix is empty. Missing objects make retries a no-op.
func (a *S3RunArchive) DeleteUnreferencedObjects(ctx context.Context, objectIDs []string) (CleanupResult, error) {
	var result CleanupResult
	var errs []error
	seen := make(map[string]struct{}, len(objectIDs))
	for _, objectID := range objectIDs {
		if _, duplicate := seen[objectID]; duplicate {
			continue
		}
		seen[objectID] = struct{}{}
		_, _, localOK := splitKey(objectID)
		s3Segments, s3OK := splitS3ObjectKey(objectID)
		if !localOK && !s3OK {
			errs = append(errs, fmt.Errorf("object %q: not a canonical object id", objectID))
			continue
		}
		prefix := runObjectRefPrefix(objectID)
		page, err := a.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(a.bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1),
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("object %q: list references: %w", objectID, err))
			continue
		}
		if len(page.Contents) != 0 {
			continue
		}
		head, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(objectID)})
		if isNotFound(err) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("object %q: stat: %w", objectID, err))
			continue
		}
		if _, err := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(objectID)}); err != nil {
			errs = append(errs, fmt.Errorf("object %q: delete: %w", objectID, err))
			continue
		}
		result.ObjectsDeleted++
		result.BytesReclaimed += aws.ToInt64(head.ContentLength)
		if s3OK && !isCleanS3Component(s3Segments[len(s3Segments)-2]) {
			manifestKey := s3ManifestKey(objectID)
			if _, err := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(manifestKey)}); err != nil {
				errs = append(errs, fmt.Errorf("object %q: delete name manifest: %w", objectID, err))
				continue
			}
		}
	}
	return result, errors.Join(errs...)
}

// ListRuns answers a query with one prefix scan. Nothing but keys crosses the
// wire: every filtered field lives in the key itself.
//
// A KeyOrder query resumes at StartAfterKey and returns as soon as the page is
// full, so paging a whole traversal costs one enumeration rather than one per
// page. S3 already lists in key order, so the cursor is just StartAfter.
func (a *S3RunArchive) ListRuns(ctx context.Context, q RunQuery) ([]RunIndexEntry, error) {
	prefix, startAfter, err := q.ScanPrefix()
	if err != nil {
		return nil, err
	}
	if q.KeyOrder && q.StartAfterKey != "" {
		startAfter = q.StartAfterKey
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

	entries := make([]RunIndexEntry, 0, q.Limit)
	pager := s3.NewListObjectsV2Paginator(a.client, input)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: s3 archive list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if stopAfterUntil && key >= stopKey {
				return a.finishList(q, entries), nil
			}
			a.indexKeysScanned.Add(1)
			entry, ok := ParseRunIndexKey(key)
			if !ok || !q.Matches(entry) {
				continue
			}
			entries = append(entries, entry)
			if q.KeyOrder && q.Limit > 0 && len(entries) >= q.Limit {
				return entries, nil
			}
		}
	}
	return a.finishList(q, entries), nil
}

// finishList applies the newest-first ordering unless the caller asked for a
// key-order page, which is already in the order it must be resumed from.
func (a *S3RunArchive) finishList(q RunQuery, entries []RunIndexEntry) []RunIndexEntry {
	if q.KeyOrder {
		return entries
	}
	return q.finish(entries)
}

// IndexKeysScanned reports how many index keys this archive has enumerated
// across every ListRuns call. It is the instrument behind the cleanup worker's
// scan-cost contract: a bounded page is only bounded if the backend stops at
// the page boundary. Tests assert on it; nothing in the running system reads it.
func (a *S3RunArchive) IndexKeysScanned() int64 { return a.indexKeysScanned.Load() }

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
