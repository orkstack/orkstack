package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path"
	"regexp"
	"strings"
)

// A fan-out over an artifact's lines (E89) reads its items through a line
// index an Orker builds once per run, fan-out task and source: an immutable
// object beside the fan-out task's commit record, so the run's prefix
// cleanup reclaims it with the run's commits.

// EachLinesIndexDir is the directory under a task's commit directory that
// holds its line indexes.
const EachLinesIndexDir = "each-lines"

// EachLinesIndexContentType is the media type a line index is written with.
const EachLinesIndexContentType = "application/octet-stream"

// EachLinesIndexRef names one line index: the run and fan-out task it is for,
// and the source it indexes by digest and format. The producer is not in it:
// an index is of a source's bytes, whoever wrote them.
type EachLinesIndexRef struct {
	Namespace    string
	RunID        string
	Task         string // the fan-out task
	SourceSHA256 string // the source artifact's committed digest, lowercase hex
	Format       string // "jsonl" or "lines"
}

var eachLinesIndexName = regexp.MustCompile(`^(jsonl|lines)-([0-9a-f]{64})\.idx$`)

// EachLinesIndexKey renders the object key of one line index:
// metadata/commits/{namespace}/{encoded-run-id}/{task-id}/each-lines/{format}-{sha256}.idx,
// beside the fan-out task's commit.json, under RunCommitPrefix. It is the
// single renderer; EachIndexStore reads and writes no other key.
func EachLinesIndexKey(ref EachLinesIndexRef) (string, error) {
	name := ref.Format + "-" + ref.SourceSHA256 + ".idx"
	if !eachLinesIndexName.MatchString(name) {
		return "", fmt.Errorf("artifacts: line index key: format %q and source digest %q: %w",
			ref.Format, ref.SourceSHA256, ErrInvalidCommitKey)
	}
	commit, err := CommitKey(ref.Namespace, ref.RunID, ref.Task)
	if err != nil {
		return "", fmt.Errorf("artifacts: line index key: %w", err)
	}
	return path.Join(path.Dir(commit), EachLinesIndexDir, name), nil
}

// ParseEachLinesIndexKey is the ref a key EachLinesIndexKey rendered names;
// false for any other key.
func ParseEachLinesIndexKey(key string) (EachLinesIndexRef, bool) {
	rest, ok := strings.CutPrefix(key, CommitPrefix+"/")
	if !ok {
		return EachLinesIndexRef{}, false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 5 || parts[3] != EachLinesIndexDir {
		return EachLinesIndexRef{}, false
	}
	m := eachLinesIndexName.FindStringSubmatch(parts[4])
	if m == nil {
		return EachLinesIndexRef{}, false
	}
	run, ok := decodeArchiveSegment(parts[1])
	if !ok {
		return EachLinesIndexRef{}, false
	}
	ref := EachLinesIndexRef{Namespace: parts[0], RunID: run, Task: parts[2], Format: m[1], SourceSHA256: m[2]}
	// Only a key the renderer itself produces: anything it would spell
	// differently is not an index key.
	if again, err := EachLinesIndexKey(ref); err != nil || again != key {
		return EachLinesIndexRef{}, false
	}
	return ref, true
}

// EachIndexStore keeps line indexes in the object store the commit records
// live in, at the bucket's key layout in a local directory: one
// implementation, two backends.
type EachIndexStore struct{ blobBase }

// NewLocalEachIndexStore opens (creating if needed) the line-index tree under
// dir, the directory the local commit store uses.
func NewLocalEachIndexStore(dir string) (*EachIndexStore, error) {
	b, err := openLocalBlobs("line index store", dir, CommitPrefix)
	if err != nil {
		return nil, err
	}
	return &EachIndexStore{blobBase{b}}, nil
}

// NewS3EachIndexStore builds the store from the ORK_S3_* configuration the
// commit store uses.
func NewS3EachIndexStore(ctx context.Context, cfg Config) (*EachIndexStore, error) {
	b, err := openS3Blobs(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &EachIndexStore{blobBase{b}}, nil
}

// URI is the absolute URI the index ref names in this store.
func (s *EachIndexStore) URI(ref EachLinesIndexRef) (string, error) {
	key, err := EachLinesIndexKey(ref)
	if err != nil {
		return "", err
	}
	return s.b.locate(key), nil
}

// CreateIfAbsent writes body as the index ref names, only when no index is
// there (S3 If-None-Match: *, local link(2)); maxBytes bounds both the body
// written and an existing index read back. created false means an index was
// already there: the reference returned is that one's, read back, so the
// caller compares its digest with its own and never takes it on trust. A
// write whose answer was lost and found equal bytes already there reports
// created true, so two writers of the same deterministic index may both see
// it; either way the index at the key is the body's bytes.
func (s *EachIndexStore) CreateIfAbsent(ctx context.Context, ref EachLinesIndexRef, body []byte, maxBytes int64) (ArtifactRef, bool, error) {
	key, err := EachLinesIndexKey(ref)
	if err != nil {
		return ArtifactRef{}, false, err
	}
	if err := checkIndexBound(maxBytes); err != nil {
		return ArtifactRef{}, false, err
	}
	if int64(len(body)) > maxBytes {
		return ArtifactRef{}, false, fmt.Errorf("artifacts: line index %q: %d bytes, larger than %d",
			s.b.uri(key), len(body), maxBytes)
	}
	err = s.b.create(ctx, key, EachLinesIndexContentType, body)
	switch {
	case err == nil:
		return s.ref(key, body), true, nil
	case errors.Is(err, errBlobExists):
		existing, _, err := s.b.get(ctx, key, maxBytes+1)
		if err != nil {
			if errors.Is(err, errBlobNotFound) {
				err = ErrNotFound
			}
			return ArtifactRef{}, false, fmt.Errorf("artifacts: line index %q: read the existing index: %w", s.b.uri(key), err)
		}
		if int64(len(existing)) > maxBytes {
			return ArtifactRef{}, false, fmt.Errorf("artifacts: line index %q: the existing index is larger than %d bytes",
				s.b.uri(key), maxBytes)
		}
		return s.ref(key, existing), false, nil
	default:
		return ArtifactRef{}, false, fmt.Errorf("artifacts: line index %q: create: %w", s.b.uri(key), err)
	}
}

// Get reads the index at uri, at most maxBytes of it: a larger object is
// refused rather than read. A URI that is not an index this store renders is
// ErrInvalidURI; a missing index is ErrNotFound.
func (s *EachIndexStore) Get(ctx context.Context, uri string, maxBytes int64) ([]byte, ArtifactRef, error) {
	if err := checkIndexBound(maxBytes); err != nil {
		return nil, ArtifactRef{}, err
	}
	key, err := s.b.resolve(uri)
	if err != nil {
		return nil, ArtifactRef{}, err
	}
	if _, ok := ParseEachLinesIndexKey(key); !ok {
		return nil, ArtifactRef{}, fmt.Errorf("artifacts: line index %q: not a line index: %w", uri, ErrInvalidURI)
	}
	body, _, err := s.b.get(ctx, key, maxBytes+1)
	if err != nil {
		if errors.Is(err, errBlobNotFound) {
			return nil, ArtifactRef{}, fmt.Errorf("artifacts: line index %q: %w", uri, ErrNotFound)
		}
		return nil, ArtifactRef{}, fmt.Errorf("artifacts: line index %q: %w", uri, err)
	}
	if int64(len(body)) > maxBytes {
		return nil, ArtifactRef{}, fmt.Errorf("artifacts: line index %q: larger than %d bytes", uri, maxBytes)
	}
	return body, s.ref(key, body), nil
}

// checkIndexBound refuses a byte bound that cannot be read as one: below one
// byte, or so large that reading one byte past it overflows.
func checkIndexBound(maxBytes int64) error {
	if maxBytes < 1 || maxBytes == math.MaxInt64 {
		return fmt.Errorf("artifacts: line index: byte bound %d is not a bound", maxBytes)
	}
	return nil
}

func (s *EachIndexStore) ref(key string, body []byte) ArtifactRef {
	sum := sha256.Sum256(body)
	return ArtifactRef{
		Name:        path.Base(key),
		URI:         s.b.locate(key),
		ContentType: EachLinesIndexContentType,
		SizeBytes:   int64(len(body)),
		SHA256:      hex.EncodeToString(sum[:]),
	}
}
