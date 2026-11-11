package artifacts

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// blobs is the object store every record store is written over: the shared
// S3-compatible bucket (s3Blobs) or a local directory laid out exactly as the
// bucket's keys (localBlobs). A record store is written once, against this
// interface, so the two backends cannot drift apart: the same keys, the same
// conditional writes and the same reads, whichever answers.
//
// Every method takes an object key, never a URI, and every key a store passes
// is one its own key functions built and validated. Failures that a store
// branches on are the sentinels below; every other failure is the backend's
// own error, which the store wraps with what it was doing.
type blobs interface {
	// uri renders key as this backend names it in errors: s3://bucket/key on
	// S3, the key itself in a local directory.
	uri(key string) string
	// locate renders key as the absolute URI a caller may hold and hand back
	// (s3://bucket/key, or file:///dir/key), and resolve reverses it: a URI
	// of another scheme, bucket or directory is ErrInvalidURI.
	locate(key string) string
	resolve(uri string) (key string, err error)

	// get reads the object at key, at most max bytes when max is positive (a
	// caller that bounds a document reads max+1 to see that it is too big). A
	// missing object is errBlobNotFound. version is the token replace names:
	// the S3 ETag, or the SHA-256 of the local file's bytes.
	get(ctx context.Context, key string, max int64) (body []byte, version string, err error)
	// getRange reads length bytes at offset and reports the object's total
	// size. A missing object is errBlobNotFound; a range past its end is
	// errBlobRange.
	getRange(ctx context.Context, key string, offset, length int64) (data []byte, total int64, err error)
	// head reports an object's size without reading it. A missing object is
	// errBlobNotFound.
	head(ctx context.Context, key string) (size int64, err error)

	// put writes body at key, replacing whatever is there; a reader sees the
	// old object or the new one, never a partial write.
	put(ctx context.Context, key, contentType string, body []byte) error
	// create writes body at key only when nothing is there (S3 If-None-Match:
	// *, local link(2)). An existing object is errBlobExists and is never
	// replaced.
	create(ctx context.Context, key, contentType string, body []byte) error
	// replace writes body at key only while the object still carries version
	// (S3 If-Match, local compare-and-rename). A changed or vanished object is
	// errBlobChanged, so a replace can never recreate a deleted object.
	replace(ctx context.Context, key, contentType string, body []byte, version string) error

	// list returns one page of the keys under in.Prefix, in key (byte) order:
	// exactly one ListObjectsV2 request on S3.
	list(ctx context.Context, in blobListInput) (blobListPage, error)
	// remove deletes the object at key. A missing object is not an error.
	remove(ctx context.Context, key string) error
	// removeKeys deletes up to blobRemoveBatch keys in one request. A failure
	// of the request as a whole is err; a key the store refused is in failed,
	// keyed by key. A missing key is not a refusal.
	removeKeys(ctx context.Context, keys []string) (failed map[string]error, err error)
}

// blobRemoveBatch is the most keys one removeKeys call may name: the most one
// S3 DeleteObjects request may name, and the most one listing page returns, so
// one listing page is one request.
const blobRemoveBatch = 1000

// blobListInput is one listing request. StartAfter skips every key at or
// before it; Token resumes a listing a previous page cut short. Delimiter "/"
// groups the keys below the next "/" after Prefix into Prefixes. MaxKeys zero
// is the backend's page size (1000).
type blobListInput struct {
	Prefix     string
	StartAfter string
	Delimiter  string
	MaxKeys    int32
	Token      string
}

// blobListPage is one page of a listing. Truncated says more keys follow,
// and Next is the Token that lists them.
type blobListPage struct {
	Objects   []blobObject
	Prefixes  []string
	Truncated bool
	Next      string
}

// blobObject is one listed key, its size and when it was last written.
type blobObject struct {
	Key      string
	Size     int64
	Modified time.Time
}

var (
	// errBlobNotFound is a missing object.
	errBlobNotFound = errors.New("object not found")
	// errBlobExists is a create that found an object already at its key.
	errBlobExists = errors.New("object already exists")
	// errBlobChanged is a replace whose object changed or vanished since it
	// was read.
	errBlobChanged = errors.New("object changed since it was read")
	// errBlobRange is a range read past the object's end.
	errBlobRange = errors.New("range is past the end of the object")
)

// listPages pages a listing to its end, handing visit every page in order.
// visit returning stop ends the listing, so a caller that has what it needs
// issues no further request.
func listPages(ctx context.Context, b blobs, in blobListInput, visit func(blobListPage) (stop bool, err error)) error {
	for {
		page, err := b.list(ctx, in)
		if err != nil {
			return err
		}
		stop, err := visit(page)
		if err != nil || stop {
			return err
		}
		if !page.Truncated || page.Next == "" {
			return nil
		}
		in.Token = page.Next
	}
}

// listObjects pages a listing to its end, calling visit for every object in
// key order. visit returning stop ends the listing early.
func listObjects(ctx context.Context, b blobs, in blobListInput, visit func(blobObject) (stop bool, err error)) error {
	return listPages(ctx, b, in, func(page blobListPage) (bool, error) {
		for _, object := range page.Objects {
			stop, err := visit(object)
			if err != nil || stop {
				return true, err
			}
		}
		return false, nil
	})
}

// listKeys is every key under prefix, in key order.
func listKeys(ctx context.Context, b blobs, prefix string) ([]string, error) {
	var keys []string
	err := listObjects(ctx, b, blobListInput{Prefix: prefix}, func(object blobObject) (bool, error) {
		keys = append(keys, object.Key)
		return false, nil
	})
	return keys, err
}

// openBlobs opens the backend cfg selects — the one ORK_ARTIFACT_BACKEND every
// store shares, with no setting of its own. what names the store in errors;
// subdirs are the prefixes a local directory creates up front.
func openBlobs(ctx context.Context, cfg Config, what string, subdirs ...string) (blobs, error) {
	switch cfg.Backend {
	case "", BackendLocal:
		return openLocalBlobs(what, localDir(cfg), subdirs...)
	case BackendS3:
		return openS3Blobs(ctx, cfg)
	default:
		return nil, fmt.Errorf("artifacts: %s backend %q: %w (want %q or %q)", what, cfg.Backend, ErrUnknownBackend, BackendLocal, BackendS3)
	}
}

// blobBase is what every record store embeds: its backend, and the accessors
// operators and tests read off a store.
type blobBase struct {
	b blobs
}

// Dir returns the absolute root directory of a local store, and "" for a
// bucket.
func (s *blobBase) Dir() string {
	if local, ok := s.b.(*localBlobs); ok {
		return local.dir
	}
	return ""
}

// Bucket returns the bucket a store keeps its records in, and "" for a local
// directory.
func (s *blobBase) Bucket() string {
	if remote, ok := s.b.(*s3Blobs); ok {
		return remote.bucket
	}
	return ""
}

// Close releases a local store's directory handle; a bucket holds none.
func (s *blobBase) Close() error {
	if local, ok := s.b.(*localBlobs); ok {
		return local.close()
	}
	return nil
}
