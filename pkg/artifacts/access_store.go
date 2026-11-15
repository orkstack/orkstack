package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// The identity prefix holds what decides access and identity. Only an
// engine's credential reaches it (docs/STORAGE.md).
const (
	IdentityPrefix = "metadata/identity"
	// AccessDocumentKey is the access document's head: roles, bindings,
	// service accounts and disabled principals, written only by
	// compare-and-swap on the etag a read returned.
	AccessDocumentKey = IdentityPrefix + "/access.json"
	// AccessRevisionsPrefix holds one immutable object per revision,
	// {revision:012}-{sha256}.json, written create-only before the head
	// moves. A revision is a record: nothing reads one to decide anything.
	AccessRevisionsPrefix = IdentityPrefix + "/access-revisions/"
)

// AccessEnvelopeVersion is the only head and revision format this build
// reads or writes.
const AccessEnvelopeVersion = 1

// MaxAccessObjectBytes bounds one head or revision object: the document's
// own 4 MiB bound and its envelope.
const MaxAccessObjectBytes = authz.MaxAccessDocumentBytes + 64<<10

// ErrAccessInvalid is an access document this build refuses to write: none,
// or one over its bound.
var ErrAccessInvalid = errors.New("invalid access document")

// ErrAccessUnreadable is a head or revision that does not decode, or names
// a format this build does not know. It is never read as "no document".
var ErrAccessUnreadable = errors.New("access document unreadable")

// AccessRevisionMeta is what a revision says of itself beside its document.
type AccessRevisionMeta struct {
	Revision          int64
	SHA256            string // of the object's bytes; empty until stored
	PreviousSHA256    string // of the revision before it; empty for the first
	WrittenAt         time.Time
	WrittenBy         string
	WrittenByVerified bool
	Change            string
}

// AccessHead is one read of the access document.
type AccessHead struct {
	// Document is the stored document, its Etag set to ETag and its
	// Revision to Meta.Revision.
	Document *orkstackv1.AccessDocument
	Meta     AccessRevisionMeta
	// ETag is the store's compare-and-swap token for the head.
	ETag string
}

// AccessRevisionKey is the key of one revision.
func AccessRevisionKey(revision int64, sha string) string {
	return fmt.Sprintf("%s%012d-%s.json", AccessRevisionsPrefix, revision, sha)
}

// accessEnvelope is the stored bytes of a head and of its revision: the
// same bytes, so the head's SHA-256 names the revision it is.
type accessEnvelope struct {
	FormatVersion     int            `json:"format_version"`
	Revision          int64          `json:"revision"`
	PreviousSHA256    string         `json:"previous_sha256,omitzero"`
	WrittenAt         string         `json:"written_at"`
	WrittenBy         string         `json:"written_by,omitzero"`
	WrittenByVerified bool           `json:"written_by_verified,omitzero"`
	Change            string         `json:"change,omitzero"`
	Document          jsontext.Value `json:"document"`
}

var shaPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// storedDocument is the document as stored: the fields a client sets, with
// every output-only field cleared.
func storedDocument(doc *orkstackv1.AccessDocument, revision int64) *orkstackv1.AccessDocument {
	out := proto.Clone(doc).(*orkstackv1.AccessDocument)
	out.Revision = revision
	out.Etag = ""
	out.BuiltInRoles = nil
	out.ConfiguredBindings = nil
	out.WrittenAroundApi = false
	for _, b := range out.GetBindings() {
		b.Pin = orkstackv1.BindingPin_BINDING_PIN_UNSPECIFIED
		b.FromConfiguration = false
	}
	for _, r := range out.GetRoles() {
		r.BuiltIn = false
	}
	return out
}

// EncodeAccessObject renders a head's bytes. Exported for the store's fuzz
// target and the tests that write a head around the API.
func EncodeAccessObject(doc *orkstackv1.AccessDocument, meta AccessRevisionMeta) ([]byte, error) {
	if doc == nil {
		return nil, fmt.Errorf("artifacts: encode access document: %w: nil document", ErrAccessInvalid)
	}
	if meta.PreviousSHA256 != "" && !shaPattern.MatchString(meta.PreviousSHA256) {
		return nil, fmt.Errorf("artifacts: encode access document: previous_sha256 %q is not a SHA-256", meta.PreviousSHA256)
	}
	raw, err := protojson.Marshal(storedDocument(doc, meta.Revision))
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode access document: %w", err)
	}
	env := accessEnvelope{
		FormatVersion: AccessEnvelopeVersion, Revision: meta.Revision, PreviousSHA256: meta.PreviousSHA256,
		WrittenAt: meta.WrittenAt.UTC().Format(time.RFC3339Nano), WrittenBy: meta.WrittenBy,
		WrittenByVerified: meta.WrittenByVerified, Change: meta.Change, Document: jsontext.Value(raw),
	}
	body, err := stablejson.MarshalIndent(env, "  ")
	if err != nil {
		return nil, fmt.Errorf("artifacts: encode access document: %w", err)
	}
	body = append(body, '\n')
	if len(body) > MaxAccessObjectBytes {
		return nil, fmt.Errorf("artifacts: encode access document: %w: %d bytes, more than %d", ErrAccessInvalid, len(body), MaxAccessObjectBytes)
	}
	return body, nil
}

// DecodeAccessObject reads a head's or a revision's bytes. An unknown format,
// a duplicate key, an unknown field of the document or a document whose
// revision is not its envelope's is ErrAccessUnreadable: closed, never
// guessed.
func DecodeAccessObject(body []byte) (*orkstackv1.AccessDocument, AccessRevisionMeta, error) {
	if len(body) > MaxAccessObjectBytes {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: %d bytes, more than %d", ErrAccessUnreadable, len(body), MaxAccessObjectBytes)
	}
	var env accessEnvelope
	if err := json.Unmarshal(body, &env, json.RejectUnknownMembers(true)); err != nil {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: %w", ErrAccessUnreadable, err)
	}
	if env.FormatVersion != AccessEnvelopeVersion {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: format_version %d is not one this release reads (%d)", ErrAccessUnreadable, env.FormatVersion, AccessEnvelopeVersion)
	}
	if env.Revision < 1 {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: revision %d is not positive", ErrAccessUnreadable, env.Revision)
	}
	if env.PreviousSHA256 != "" && !shaPattern.MatchString(env.PreviousSHA256) {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: previous_sha256 is not a SHA-256", ErrAccessUnreadable)
	}
	at, err := time.Parse(time.RFC3339Nano, env.WrittenAt)
	if err != nil {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: written_at: %w", ErrAccessUnreadable, err)
	}
	doc := &orkstackv1.AccessDocument{}
	if err := (protojson.UnmarshalOptions{}).Unmarshal(env.Document, doc); err != nil {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: document: %w", ErrAccessUnreadable, err)
	}
	if doc.GetRevision() != env.Revision {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: the document says revision %d and its envelope %d", ErrAccessUnreadable, doc.GetRevision(), env.Revision)
	}
	if doc.GetEtag() != "" || len(doc.GetBuiltInRoles()) > 0 || len(doc.GetConfiguredBindings()) > 0 || doc.GetWrittenAroundApi() {
		return nil, AccessRevisionMeta{}, fmt.Errorf("%w: the document holds a field that is never stored", ErrAccessUnreadable)
	}
	sum := sha256.Sum256(body)
	return doc, AccessRevisionMeta{
		Revision: env.Revision, SHA256: hex.EncodeToString(sum[:]), PreviousSHA256: env.PreviousSHA256,
		WrittenAt: at, WrittenBy: env.WrittenBy, WrittenByVerified: env.WrittenByVerified, Change: env.Change,
	}, nil
}

// blobCAS is the little of an object store the identity documents need:
// a read that may be conditional, a conditional write and an existence
// check. The S3 and in-memory stores share every rule above it.
type blobCAS interface {
	// get reads key. With ifNoneMatch set and equal to the stored etag it
	// answers notModified and no body. A missing key is ErrNotFound.
	get(ctx context.Context, key, ifNoneMatch string) (body []byte, etag string, notModified bool, err error)
	// put writes key under cond; a refused condition is ErrPreconditionFailed.
	put(ctx context.Context, key string, body []byte, cond PutConditions) (etag string, err error)
	// exists reports whether key is stored.
	exists(ctx context.Context, key string) (bool, error)
	// list answers up to limit keys under prefix that sort after
	// startAfter, in key order.
	list(ctx context.Context, prefix, startAfter string, limit int) ([]string, error)
	location(key string) string
}

// AccessStore reads and writes the access document. It holds no cache:
// the engine's policy cache revalidates through Revalidate.
type AccessStore struct {
	blobs blobCAS
}

// NewS3AccessStore opens the access store in the deployment's bucket.
func NewS3AccessStore(ctx context.Context, cfg Config) (*AccessStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &AccessStore{blobs: &s3CAS{client: client, bucket: bucket}}, nil
}

// NewMemoryAccessStore is an in-memory store for tests, with the S3 store's
// rules.
func NewMemoryAccessStore() *AccessStore { return &AccessStore{blobs: NewMemoryCAS()} }

// NewAccessStoreOn is an access store over a MemoryCAS a test shares with
// another store or writes around.
func NewAccessStoreOn(m *MemoryCAS) *AccessStore { return &AccessStore{blobs: m} }

// Location is the s3:// URI (or memory: name) of the head.
func (s *AccessStore) Location() string { return s.blobs.location(AccessDocumentKey) }

// Get reads the head. A deployment that never wrote one is ErrNotFound.
func (s *AccessStore) Get(ctx context.Context) (*AccessHead, error) {
	head, _, err := s.Revalidate(ctx, "")
	return head, err
}

// Revalidate reads the head unless it still carries etag: then it answers
// changed=false and no head, for the cost of one conditional get.
func (s *AccessStore) Revalidate(ctx context.Context, etag string) (head *AccessHead, changed bool, err error) {
	body, gotETag, notModified, err := s.blobs.get(ctx, AccessDocumentKey, etag)
	if err != nil {
		return nil, false, fmt.Errorf("artifacts: get access document %q: %w", s.Location(), err)
	}
	if notModified {
		return nil, false, nil
	}
	doc, meta, err := DecodeAccessObject(body)
	if err != nil {
		return nil, false, fmt.Errorf("artifacts: get access document %q: %w", s.Location(), err)
	}
	doc.Etag = gotETag
	return &AccessHead{Document: doc, Meta: meta, ETag: gotETag}, true, nil
}

// HasRevision reports whether the revision a head names exists: a head
// without one was written around the API.
func (s *AccessStore) HasRevision(ctx context.Context, meta AccessRevisionMeta) (bool, error) {
	return s.blobs.exists(ctx, AccessRevisionKey(meta.Revision, meta.SHA256))
}

// GetRevision reads one revision by its number and digest.
func (s *AccessStore) GetRevision(ctx context.Context, revision int64, sha string) (*AccessHead, error) {
	if revision < 1 || !shaPattern.MatchString(sha) {
		return nil, fmt.Errorf("artifacts: get access revision: %w: revision %d, digest %q", ErrNotFound, revision, sha)
	}
	key := AccessRevisionKey(revision, sha)
	body, _, _, err := s.blobs.get(ctx, key, "")
	if err != nil {
		return nil, fmt.Errorf("artifacts: get access revision %q: %w", s.blobs.location(key), err)
	}
	doc, meta, err := DecodeAccessObject(body)
	if err != nil {
		return nil, fmt.Errorf("artifacts: get access revision %q: %w", s.blobs.location(key), err)
	}
	if meta.Revision != revision || meta.SHA256 != sha {
		return nil, fmt.Errorf("artifacts: get access revision %q: %w: it holds revision %d with digest %s", s.blobs.location(key), ErrAccessUnreadable, meta.Revision, meta.SHA256)
	}
	return &AccessHead{Document: doc, Meta: meta}, nil
}

// AccessWrite is who makes a write and what it changes.
type AccessWrite struct {
	At         time.Time
	By         string
	ByVerified bool
	Change     string
}

// Write stores doc as the revision after prev (nil for the first): the
// revision create-only, then the head under prev's etag. A revision that
// cannot be written aborts the change. A head whose etag moved is
// ErrPreconditionFailed: the caller reads again, changes again and retries;
// the revision it wrote is then an orphan nothing follows.
func (s *AccessStore) Write(ctx context.Context, doc *orkstackv1.AccessDocument, prev *AccessHead, w AccessWrite) (*AccessHead, error) {
	meta := AccessRevisionMeta{Revision: 1, WrittenAt: w.At, WrittenBy: w.By, WrittenByVerified: w.ByVerified, Change: w.Change}
	cond := PutConditions{IfAbsent: true}
	if prev != nil && prev.ETag == "" {
		return nil, fmt.Errorf("artifacts: write access document: %w: the head it changes was read with no etag (a revision is not a head)", ErrPreconditionFailed)
	}
	if prev != nil {
		meta.Revision = prev.Meta.Revision + 1
		meta.PreviousSHA256 = prev.Meta.SHA256
		cond = PutConditions{IfMatch: prev.ETag}
	}
	body, err := EncodeAccessObject(doc, meta)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	meta.SHA256 = hex.EncodeToString(sum[:])
	key := AccessRevisionKey(meta.Revision, meta.SHA256)
	if _, err := s.blobs.put(ctx, key, body, PutConditions{IfAbsent: true}); err != nil && !errors.Is(err, ErrPreconditionFailed) {
		return nil, fmt.Errorf("artifacts: write access revision %q: %w", s.blobs.location(key), err)
	}
	etag, err := s.blobs.put(ctx, AccessDocumentKey, body, cond)
	if err != nil {
		return nil, fmt.Errorf("artifacts: swap access document %q: %w", s.Location(), err)
	}
	stored := storedDocument(doc, meta.Revision)
	stored.Etag = etag
	return &AccessHead{Document: stored, Meta: meta, ETag: etag}, nil
}

// s3CAS is blobCAS over a bucket.
type s3CAS struct {
	client *s3.Client
	bucket string
}

func (c *s3CAS) location(key string) string { return "s3://" + c.bucket + "/" + key }

func (c *s3CAS) get(ctx context.Context, key, ifNoneMatch string) ([]byte, string, bool, error) {
	in := &s3.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)}
	if ifNoneMatch != "" {
		in.IfNoneMatch = aws.String(ifNoneMatch)
	}
	out, err := c.client.GetObject(ctx, in)
	if err != nil {
		if isNotModified(err) {
			return nil, ifNoneMatch, true, nil
		}
		if isNotFound(err) {
			return nil, "", false, ErrNotFound
		}
		return nil, "", false, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, MaxAccessObjectBytes+1))
	if err != nil {
		return nil, "", false, fmt.Errorf("read body: %w", err)
	}
	if len(body) > MaxAccessObjectBytes {
		return nil, "", false, fmt.Errorf("%w: more than %d bytes", ErrAccessUnreadable, MaxAccessObjectBytes)
	}
	return body, aws.ToString(out.ETag), false, nil
}

func (c *s3CAS) put(ctx context.Context, key string, body []byte, cond PutConditions) (string, error) {
	in := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String("application/json"),
	}
	if cond.IfAbsent {
		in.IfNoneMatch = aws.String("*")
	}
	if cond.IfMatch != "" {
		in.IfMatch = aws.String(cond.IfMatch)
	}
	out, err := putConfirmed(ctx, c.client, in, body)
	if err != nil {
		if cond.conditional() && refusedCondition(err, in) {
			return "", ErrPreconditionFailed
		}
		return "", err
	}
	return out.ETag, nil
}

func (c *s3CAS) exists(ctx context.Context, key string) (bool, error) {
	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key)})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

func (c *s3CAS) list(ctx context.Context, prefix, startAfter string, limit int) ([]string, error) {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(c.bucket), Prefix: aws.String(prefix)}
	if startAfter != "" {
		in.StartAfter = aws.String(startAfter)
	}
	var keys []string
	for len(keys) < limit {
		in.MaxKeys = aws.Int32(int32(min(limit-len(keys), 1000)))
		out, err := c.client.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, object := range out.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		if aws.ToString(out.NextContinuationToken) == "" {
			return nil, fmt.Errorf("a truncated listing of %q gave no continuation token", prefix)
		}
		// A continuation carries the position: StartAfter is for the first
		// page only, and a store that does not ignore it on later pages
		// would skip keys.
		in.StartAfter = nil
		in.ContinuationToken = out.NextContinuationToken
	}
	return keys, nil
}

// isNotModified reports a conditional get whose object still carries the
// etag asked about.
func isNotModified(err error) bool {
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotModified {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotModified"
}

// MemoryCAS is blobCAS in memory, for tests. Its etags change on every
// write, as a store's do. Fail, when set, answers every call with its error,
// as an unreachable store would.
type MemoryCAS struct {
	mu      sync.Mutex
	objects map[string][]byte
	etags   map[string]string
	seq     int
	fail    error
	listErr error
	hang    chan struct{}
	arrived chan<- struct{}
}

// SetHang makes every read wait until hang is closed or the caller's
// context ends, as a store that accepts connections and never answers
// would; nil answers again. arrived, when set, is told of each read that
// starts to wait, without blocking.
func (m *MemoryCAS) SetHang(hang chan struct{}, arrived chan<- struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hang, m.arrived = hang, arrived
}

// NewMemoryCAS returns an empty in-memory object store.
func NewMemoryCAS() *MemoryCAS {
	return &MemoryCAS{objects: map[string][]byte{}, etags: map[string]string{}}
}

// SetFailure makes every call fail with err until it is set to nil.
func (m *MemoryCAS) SetFailure(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = err
}

// SetListFailure makes every listing fail with err until it is set to nil,
// while reads and writes still answer.
func (m *MemoryCAS) SetListFailure(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listErr = err
}

// PutRaw writes key as a writer around the API would: unconditionally.
func (m *MemoryCAS) PutRaw(key string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.objects[key] = bytes.Clone(body)
	m.etags[key] = `"` + strconv.Itoa(m.seq) + `"`
}

// Delete removes key, as a writer around the API would.
func (m *MemoryCAS) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	delete(m.etags, key)
}

// Raw reads key's bytes.
func (m *MemoryCAS) Raw(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	return bytes.Clone(b), ok
}

// Keys lists every stored key.
func (m *MemoryCAS) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.objects))
	for k := range m.objects {
		out = append(out, k)
	}
	return out
}

func (m *MemoryCAS) location(key string) string { return "memory:" + key }

func (m *MemoryCAS) get(ctx context.Context, key, ifNoneMatch string) ([]byte, string, bool, error) {
	m.mu.Lock()
	hang, arrived := m.hang, m.arrived
	m.mu.Unlock()
	if hang != nil {
		if arrived != nil {
			select {
			case arrived <- struct{}{}:
			default:
			}
		}
		select {
		case <-hang:
		case <-ctx.Done():
			return nil, "", false, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return nil, "", false, m.fail
	}
	b, ok := m.objects[key]
	if !ok {
		return nil, "", false, ErrNotFound
	}
	if ifNoneMatch != "" && ifNoneMatch == m.etags[key] {
		return nil, ifNoneMatch, true, nil
	}
	return bytes.Clone(b), m.etags[key], false, nil
}

func (m *MemoryCAS) put(_ context.Context, key string, body []byte, cond PutConditions) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return "", m.fail
	}
	_, present := m.objects[key]
	if cond.IfAbsent && present {
		return "", ErrPreconditionFailed
	}
	if cond.IfMatch != "" && (!present || m.etags[key] != cond.IfMatch) {
		return "", ErrPreconditionFailed
	}
	m.seq++
	m.objects[key] = bytes.Clone(body)
	m.etags[key] = `"` + strconv.Itoa(m.seq) + `"`
	return m.etags[key], nil
}

func (m *MemoryCAS) exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return false, m.fail
	}
	_, ok := m.objects[key]
	return ok, nil
}

func (m *MemoryCAS) list(_ context.Context, prefix, startAfter string, limit int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return nil, m.fail
	}
	if m.listErr != nil {
		return nil, m.listErr
	}
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}
