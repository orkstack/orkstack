package artifacts

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// SigninsPrefix holds one record per sign-in to the web UI,
// {sha256(cookie)}.json, encrypted whole under the sign-in key
// (authn-authz.md §8). A record is created only if absent, updated under
// its etag, and deleted at sign-out, at revocation, or when it is read
// expired; a bucket lifecycle rule on this prefix removes the rest. No run,
// document or decision depends on one: it is not execution state.
const SigninsPrefix = IdentityPrefix + "/signins/"

// MaxSigninsListed bounds one ListSignins page.
const MaxSigninsListed = 1000

// signinIDPattern is a record's id: the lowercase hex SHA-256 of its
// cookie.
var signinIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// signinBlobs is what the sign-in store needs of a bucket.
type signinBlobs interface {
	blobCAS
	remove(ctx context.Context, key string) error
}

// SigninStore keeps the sign-in records. It holds sealed bytes only: it
// never sees a cookie, a refresh token or a key.
type SigninStore struct {
	blobs signinBlobs
}

// NewS3SigninStore opens the sign-in store in the deployment's bucket.
func NewS3SigninStore(ctx context.Context, cfg Config) (*SigninStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &SigninStore{blobs: &s3CAS{client: client, bucket: bucket}}, nil
}

// NewMemorySigninStore is an in-memory store for tests, with the S3
// store's rules; cas, when given, is the memory it uses.
func NewMemorySigninStore(cas *MemoryCAS) *SigninStore {
	if cas == nil {
		cas = NewMemoryCAS()
	}
	return &SigninStore{blobs: cas}
}

// Location names where the records are kept, for the boot's log.
func (s *SigninStore) Location() string { return s.blobs.location(SigninsPrefix) }

// SigninKey is the key of the record id names.
func SigninKey(id string) string { return SigninsPrefix + id + ".json" }

func checkSigninID(id string) error {
	if !signinIDPattern.MatchString(id) {
		return fmt.Errorf("artifacts: %q is not a sign-in record's id", id)
	}
	return nil
}

// Create writes a new record create-only. A record that already exists is
// ErrPreconditionFailed: an id is 256 random bits hashed, so that is never
// a sign-in's own.
func (s *SigninStore) Create(ctx context.Context, id string, sealed []byte) (etag string, err error) {
	if err := checkSigninID(id); err != nil {
		return "", err
	}
	key := SigninKey(id)
	etag, err = s.blobs.put(ctx, key, sealed, PutConditions{IfAbsent: true})
	if err != nil {
		return "", fmt.Errorf("artifacts: create sign-in %q: %w", s.blobs.location(key), err)
	}
	return etag, nil
}

// Get reads a record. A missing one is ErrNotFound.
func (s *SigninStore) Get(ctx context.Context, id string) (sealed []byte, etag string, err error) {
	if err := checkSigninID(id); err != nil {
		return nil, "", err
	}
	key := SigninKey(id)
	body, etag, _, err := s.blobs.get(ctx, key, "")
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("artifacts: read sign-in %q: %w", s.blobs.location(key), err)
	}
	return body, etag, nil
}

// Replace writes a record over the one read under etag. Another write
// since is ErrPreconditionFailed; a record deleted since is too.
func (s *SigninStore) Replace(ctx context.Context, id string, sealed []byte, etag string) (string, error) {
	if err := checkSigninID(id); err != nil {
		return "", err
	}
	if etag == "" {
		return "", errors.New("artifacts: replace sign-in: an etag is required")
	}
	key := SigninKey(id)
	next, err := s.blobs.put(ctx, key, sealed, PutConditions{IfMatch: etag})
	if err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			return "", ErrPreconditionFailed
		}
		return "", fmt.Errorf("artifacts: replace sign-in %q: %w", s.blobs.location(key), err)
	}
	return next, nil
}

// Delete removes a record; one already gone is not an error.
func (s *SigninStore) Delete(ctx context.Context, id string) error {
	if err := checkSigninID(id); err != nil {
		return err
	}
	key := SigninKey(id)
	if err := s.blobs.remove(ctx, key); err != nil {
		return fmt.Errorf("artifacts: delete sign-in %q: %w", s.blobs.location(key), err)
	}
	return nil
}

// List names up to MaxSigninsListed records after startAfter, in id order;
// a key that is not a record's is skipped. more says another page follows.
func (s *SigninStore) List(ctx context.Context, startAfter string) (ids []string, more bool, err error) {
	after := ""
	if startAfter != "" {
		if err := checkSigninID(startAfter); err != nil {
			return nil, false, err
		}
		after = SigninKey(startAfter)
	}
	keys, err := s.blobs.list(ctx, SigninsPrefix, after, MaxSigninsListed+1)
	if err != nil {
		return nil, false, fmt.Errorf("artifacts: list sign-ins %q: %w", s.blobs.location(SigninsPrefix), err)
	}
	if len(keys) > MaxSigninsListed {
		keys, more = keys[:MaxSigninsListed], true
	}
	for _, key := range keys {
		id, ok := strings.CutSuffix(strings.TrimPrefix(key, SigninsPrefix), ".json")
		if ok && signinIDPattern.MatchString(id) {
			ids = append(ids, id)
		}
	}
	return ids, more, nil
}

func (c *s3CAS) remove(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

func (m *MemoryCAS) remove(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	delete(m.objects, key)
	delete(m.etags, key)
	return nil
}
