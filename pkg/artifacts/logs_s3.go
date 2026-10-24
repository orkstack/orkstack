package artifacts

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3LogStore keeps task log chunks in an S3-compatible bucket — the store both
// the Orker writing them and every engine replica reading them already share.
//
// It is the reason any replica serves any run's logs (AGENTS.md §7): the
// chunks are in the bucket, not in the process that produced them, so a run
// whose Orker has exited and whose engine replica is gone reads exactly the
// same as one executing right now.
type S3LogStore struct {
	client *s3.Client
	bucket string
}

// compile-time contract check.
var (
	_ LogStore            = (*S3LogStore)(nil)
	_ RunLogAttemptLister = (*S3LogStore)(nil)
)

// NewS3LogStore builds the log store from the same ORK_S3_* config the
// artifact, orkflow and namespace stores use.
func NewS3LogStore(ctx context.Context, cfg Config) (*S3LogStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3LogStore{client: client, bucket: bucket}, nil
}

// Bucket returns the bucket holding the logs.
func (s *S3LogStore) Bucket() string { return s.bucket }

// Location is the s3:// URI of the prefix this store owns.
func (s *S3LogStore) Location() string { return s.uri(LogPrefix + "/") }

func (s *S3LogStore) uri(key string) string { return "s3://" + s.bucket + "/" + key }

// PutLogChunk writes one chunk object. An S3 PUT is atomic, so a reader
// listing the prefix either sees the whole chunk or does not see it at all.
func (s *S3LogStore) PutLogChunk(ctx context.Context, chunk LogChunk) error {
	key, err := LogChunkKey(chunk.LogRef, chunk.Seq, chunk.Truncated)
	if err != nil {
		return err
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(chunk.Data),
		ContentLength: aws.Int64(int64(len(chunk.Data))),
		ContentType:   aws.String(LogChunkContentType),
	}); err != nil {
		return fmt.Errorf("artifacts: put log chunk %q: %w", s.uri(key), err)
	}
	return nil
}

// ReadLogChunks lists the attempt's prefix from afterSeq and fetches the
// bodies in key order, bounded by both limit and maxBytes.
//
// StartAfter carries the resume, so a follower re-listing every few hundred
// milliseconds pays for the keys it has not seen rather than for the whole
// log again — a 16 MiB attempt is ~256 chunks, and re-listing all of them per
// poll is what would make a long run expensive. The bound is the truncation
// spelling of afterSeq, which is the highest key that sequence can occupy, so
// neither an ordinary chunk nor a notice is ever delivered twice.
//
// maxBytes stops the GETs, not the listing: how big a chunk is, is known only
// once it has been downloaded — object sizes on a page are a listing's word,
// and this store fetches what it lists. So the budget is checked after each
// body, which bounds what one call holds to the budget plus the last chunk.
func (s *S3LogStore) ReadLogChunks(
	ctx context.Context,
	ref LogRef,
	afterSeq int64,
	limit int,
	maxBytes int64,
) ([]LogChunk, error) {
	prefix, err := LogAttemptPrefix(ref)
	if err != nil {
		return nil, err
	}
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	}
	if afterSeq > 0 {
		ceiling, err := logChunkCeilingKey(ref, afterSeq)
		if err != nil {
			return nil, err
		}
		input.StartAfter = aws.String(ceiling)
	}
	if limit > 0 {
		input.MaxKeys = aws.Int32(int32(limit))
	}
	found := make([]LogChunk, 0, 16)
	pager := s3.NewListObjectsV2Paginator(s.client, input)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list log chunks %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			chunkRef, seq, truncated, ok := ParseLogChunkKey(key)
			if !ok || chunkRef != ref || seq <= afterSeq {
				continue
			}
			found = append(found, LogChunk{LogRef: ref, Seq: seq, Truncated: truncated})
		}
		if limit > 0 && len(found) >= limit {
			break
		}
	}
	// S3 lists in lexicographic key order, which the padded key makes numeric
	// order — but the sort is kept so the contract holds for any backend
	// whose paginator ever reorders a page.
	sort.Slice(found, func(i, j int) bool { return found[i].Seq < found[j].Seq })
	if limit > 0 && limit < len(found) {
		found = found[:limit]
	}
	read := int64(0)
	for i := range found {
		key, err := LogChunkKey(found[i].LogRef, found[i].Seq, found[i].Truncated)
		if err != nil {
			return nil, err
		}
		object, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if isNotFound(err) {
				// Expired under a lifecycle rule between the listing and the
				// read. Stop at the gap rather than skipping it, so a caller
				// resumes from the last sequence it actually received.
				return found[:i], nil
			}
			return nil, fmt.Errorf("artifacts: get log chunk %q: %w", s.uri(key), err)
		}
		data, err := io.ReadAll(object.Body)
		_ = object.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("artifacts: get log chunk %q: read body: %w", s.uri(key), err)
		}
		found[i].Data = data
		// After the chunk, never before it: the first is always returned
		// whole, so no chunk is too big to be delivered.
		read += int64(len(data))
		if maxBytes > 0 && read >= maxBytes {
			return found[:i+1], nil
		}
	}
	return found, nil
}

// LatestLogAttempt returns the highest attempt of a task that wrote a chunk.
//
// The listing is DELIMITED at the attempt level: S3 answers the scope's
// attempt prefixes (and its items' prefixes, which are told apart by their
// width and skipped) rather than every chunk key under them, so a task whose
// attempt wrote twelve thousand chunks costs one short listing, not twelve
// pages. A prefix exists only while some object is under it, which is the
// "still has something to read" the other backends check.
func (s *S3LogStore) LatestLogAttempt(ctx context.Context, runID, taskID string, item Item) (int32, error) {
	prefix, err := LogScopePrefix(runID, taskID, item)
	if err != nil {
		return 0, err
	}
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	latest := int32(0)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("artifacts: list log attempts %q: %w", s.uri(prefix), err)
		}
		for _, common := range page.CommonPrefixes {
			segment := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(common.Prefix), prefix), "/")
			attempt, ok := parseFixedWidth(segment, logAttemptWidth)
			if !ok || attempt < 1 || attempt > (1<<31-1) {
				continue
			}
			if int32(attempt) > latest {
				latest = int32(attempt)
			}
		}
	}
	if latest == 0 {
		return 0, fmt.Errorf("artifacts: task %q of run %q has no logs: %w", taskID, runID, ErrNotFound)
	}
	return latest, nil
}

// LatestLogAttempts answers every scope of a run from ONE listing of the
// run's prefix, reading keys only.
//
// A long attempt is not listed to its end: when a page stops inside an
// attempt, the next request starts just past that attempt's prefix (a fresh
// StartAfter rather than the continuation token), because the attempt's
// remaining chunks can only repeat what the page already said. So the listing
// costs about one request per page of DISTINCT attempts, never one per page of
// chunks.
func (s *S3LogStore) LatestLogAttempts(ctx context.Context, runID string) (map[LogScope]int32, error) {
	prefix, err := LogRunPrefix(runID)
	if err != nil {
		return nil, err
	}
	latest := map[LogScope]int32{}
	startAfter := ""
	for {
		input := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(prefix)}
		if startAfter != "" {
			input.StartAfter = aws.String(startAfter)
		}
		page, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("artifacts: list run log attempts %q: %w", s.uri(prefix), err)
		}
		for _, object := range page.Contents {
			ref, _, _, ok := ParseLogChunkKey(aws.ToString(object.Key))
			if !ok || ref.RunID != runID {
				continue
			}
			scope := LogScope{TaskID: ref.TaskID, Item: ref.Item}
			if ref.Attempt > latest[scope] {
				latest[scope] = ref.Attempt
			}
		}
		if !aws.ToBool(page.IsTruncated) || len(page.Contents) == 0 {
			return latest, nil
		}
		last := aws.ToString(page.Contents[len(page.Contents)-1].Key)
		next := last
		if ref, _, _, ok := ParseLogChunkKey(last); ok {
			if attemptPrefix, err := LogAttemptPrefix(ref); err == nil {
				next = logAttemptPrefixEnd(attemptPrefix)
			}
		}
		if next <= startAfter {
			// Never re-ask from where this request started: a listing that
			// did not move would loop for ever.
			next = last
		}
		startAfter = next
	}
}
