package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// BlobLogStore keeps task log chunks in the object store both the Orker
// writing them and every engine replica reading them already share.
//
// It is the reason any replica serves any run's logs (AGENTS.md §7): the
// chunks are in the store, not in the process that produced them, so a run
// whose Orker has exited and whose engine replica is gone reads exactly the
// same as one executing right now. A local directory holds them at exactly
// the bucket's key layout.
type BlobLogStore struct{ blobBase }

// LocalLogStore and S3LogStore are the log store over a local directory and
// over a bucket: one implementation, two backends.
type (
	LocalLogStore = BlobLogStore
	S3LogStore    = BlobLogStore
)

// compile-time contract check.
var (
	_ LogStore            = (*BlobLogStore)(nil)
	_ RunLogAttemptLister = (*BlobLogStore)(nil)
)

// NewLocalLogStore opens (creating if needed) the log tree under dir.
func NewLocalLogStore(dir string) (*LocalLogStore, error) {
	b, err := openLocalBlobs("log store", dir, LogPrefix)
	if err != nil {
		return nil, err
	}
	return &BlobLogStore{blobBase{b}}, nil
}

// NewS3LogStore builds the log store from the same ORK_S3_* config the
// artifact, orkflow and namespace stores use.
func NewS3LogStore(ctx context.Context, cfg Config) (*S3LogStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &BlobLogStore{blobBase{b}}, nil
}

// Location is the URI of the prefix this store owns.
func (s *BlobLogStore) Location() string { return s.b.locate(LogPrefix + "/") }

// PutLogChunk writes one chunk atomically, so a reader listing the prefix
// either sees the whole chunk or does not see it at all.
func (s *BlobLogStore) PutLogChunk(ctx context.Context, chunk LogChunk) error {
	key, err := LogChunkKey(chunk.LogRef, chunk.Seq, chunk.Truncated)
	if err != nil {
		return err
	}
	if err := s.b.put(ctx, key, LogChunkContentType, chunk.Data); err != nil {
		return fmt.Errorf("artifacts: put log chunk %q: %w", s.b.uri(key), err)
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
// maxBytes stops the reads, not the listing: how big a chunk is, is known
// only once it has been downloaded — object sizes on a page are a listing's
// word, and this store fetches what it lists. So the budget is checked after
// each body, which bounds what one call holds to the budget plus the last
// chunk.
func (s *BlobLogStore) ReadLogChunks(
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
	in := blobListInput{Prefix: prefix}
	if afterSeq > 0 {
		ceiling, err := logChunkCeilingKey(ref, afterSeq)
		if err != nil {
			return nil, err
		}
		in.StartAfter = ceiling
	}
	if limit > 0 {
		in.MaxKeys = int32(limit)
	}
	found := make([]LogChunk, 0, 16)
	err = listPages(ctx, s.b, in, func(page blobListPage) (bool, error) {
		for _, object := range page.Objects {
			chunkRef, seq, truncated, ok := ParseLogChunkKey(object.Key)
			if !ok || chunkRef != ref || seq <= afterSeq {
				continue
			}
			found = append(found, LogChunk{LogRef: ref, Seq: seq, Truncated: truncated})
		}
		return limit > 0 && len(found) >= limit, nil
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: list log chunks %q: %w", s.b.uri(prefix), err)
	}
	// The listing is in lexicographic key order, which the padded key makes
	// numeric order — but the sort is kept so the contract holds for any
	// backend whose listing ever reorders a page.
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
		data, _, err := s.b.get(ctx, key, 0)
		if err != nil {
			if errors.Is(err, errBlobNotFound) {
				// Expired under a lifecycle rule between the listing and the
				// read. Stop at the gap rather than skipping it, so a caller
				// resumes from the last sequence it actually received.
				return found[:i], nil
			}
			return nil, fmt.Errorf("artifacts: get log chunk %q: %w", s.b.uri(key), err)
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
// The listing is DELIMITED at the attempt level: the store answers the
// scope's attempt prefixes (and its items' prefixes, which are told apart by
// their width and skipped) rather than every chunk key under them, so a task
// whose attempt wrote twelve thousand chunks costs one short listing, not
// twelve pages. A prefix exists only while some object is under it, which is
// "still has something to read".
func (s *BlobLogStore) LatestLogAttempt(ctx context.Context, runID, taskID string, item Item) (int32, error) {
	prefix, err := LogScopePrefix(runID, taskID, item)
	if err != nil {
		return 0, err
	}
	latest := int32(0)
	err = listPages(ctx, s.b, blobListInput{Prefix: prefix, Delimiter: "/"}, func(page blobListPage) (bool, error) {
		for _, common := range page.Prefixes {
			segment := strings.TrimSuffix(strings.TrimPrefix(common, prefix), "/")
			attempt, ok := parseFixedWidth(segment, logAttemptWidth)
			if !ok || attempt < 1 || attempt > (1<<31-1) {
				continue
			}
			if int32(attempt) > latest {
				latest = int32(attempt)
			}
		}
		return false, nil
	})
	if err != nil {
		return 0, fmt.Errorf("artifacts: list log attempts %q: %w", s.b.uri(prefix), err)
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
func (s *BlobLogStore) LatestLogAttempts(ctx context.Context, runID string) (map[LogScope]int32, error) {
	prefix, err := LogRunPrefix(runID)
	if err != nil {
		return nil, err
	}
	latest := map[LogScope]int32{}
	startAfter := ""
	for {
		page, err := s.b.list(ctx, blobListInput{Prefix: prefix, StartAfter: startAfter})
		if err != nil {
			return nil, fmt.Errorf("artifacts: list run log attempts %q: %w", s.b.uri(prefix), err)
		}
		for _, object := range page.Objects {
			ref, _, _, ok := ParseLogChunkKey(object.Key)
			if !ok || ref.RunID != runID {
				continue
			}
			scope := LogScope{TaskID: ref.TaskID, Item: ref.Item}
			if ref.Attempt > latest[scope] {
				latest[scope] = ref.Attempt
			}
		}
		if !page.Truncated || len(page.Objects) == 0 {
			return latest, nil
		}
		last := page.Objects[len(page.Objects)-1].Key
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
