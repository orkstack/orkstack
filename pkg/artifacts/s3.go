package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Store is the S3-compatible Store. It targets AWS S3 and any compatible
// server (MinIO in this repo's dev stack) through a custom endpoint plus
// path-style addressing.
//
// Object keys are a human-readable hierarchy that names the run an artifact
// came from:
//
//	s3://{bucket}/orkflows/{orkflow-name}/{run-id}/{task-id}/{artifact-name}
//
// The Orker supplies those coordinates as Provenance (see PutOptions.Metadata),
// so a URI is traceable on sight and an S3 lifecycle rule can expire one
// orkflow, run or task by prefix. A versioned binary companion under
// `manifests/` preserves the exact logical name — including arbitrary bytes and
// names too long for a key — without S3 key-length growth.
type S3Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	// multipartThreshold and partSize override the multipart defaults in
	// tests; zero means the default.
	multipartThreshold int64
	partSize           int64
}

// Provenance is the run coordinates an artifact came from. The S3 store lays
// objects out by provenance, so the object key reads as the run that produced
// it.
//
// Provenance travels in PutOptions.Metadata (see Metadata), which leaves the
// artifact contract — Store, PutOptions and ArtifactRef — unchanged, and makes
// every object state its own origin in S3 user metadata.
type Provenance struct {
	// Orkflow is the orkflow name, e.g. "hello-ork".
	Orkflow string
	// TaskID is the task that produced the artifact, e.g. "fetch".
	TaskID string
	// RunID is the run that produced it, e.g. "ork-1755302400123456789-1".
	RunID string
}

// Metadata keys carrying Provenance through PutOptions.Metadata.
const (
	MetaOrkflow = "orkflow"
	MetaTaskID  = "task_id"
	MetaRunID   = "run_id"
)

// metaContentSHA256 records the content digest on the object itself, so Stat
// and List report the digest of the bytes that are actually there. It is
// reserved: the store writes it and strips it from every ArtifactRef.
const metaContentSHA256 = "ork-content-sha256"

// Metadata copies extra and writes the provenance keys over it. Provenance
// addresses the object, so caller-supplied metadata can never move an artifact.
func (p Provenance) Metadata(extra map[string]string) map[string]string {
	meta := make(map[string]string, len(extra)+3)
	for k, v := range extra {
		meta[k] = v
	}
	for key, value := range map[string]string{
		MetaOrkflow: p.Orkflow,
		MetaTaskID:  p.TaskID,
		MetaRunID:   p.RunID,
	} {
		if value == "" {
			delete(meta, key)
			continue
		}
		meta[key] = value
	}
	return meta
}

// ProvenanceFromMetadata reads back what Metadata wrote. Missing coordinates
// stay empty and the store files the artifact under its reserved unattributed
// prefix.
func ProvenanceFromMetadata(meta map[string]string) Provenance {
	return Provenance{
		Orkflow: metadataValue(meta, MetaOrkflow),
		TaskID:  metadataValue(meta, MetaTaskID),
		RunID:   metadataValue(meta, MetaRunID),
	}
}

// compile-time contract check.
var _ Store = (*S3Store)(nil)
var _ RefPredictor = (*S3Store)(nil)

// NewS3Store builds an S3-backed store from cfg.
func NewS3Store(ctx context.Context, cfg Config) (*S3Store, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3Store{client: client, presign: s3.NewPresignClient(client), bucket: bucket}, nil
}

// newS3Client builds the client and resolves the bucket that both S3-backed
// stores share: run artifacts (S3Store) and orkflow definitions
// (S3OrkflowStore). Static credentials are used when both key fields are set;
// otherwise the standard AWS credential chain applies, so the same code serves
// MinIO and real AWS.
func newS3Client(ctx context.Context, cfg Config) (*s3.Client, string, error) {
	bucket := cfg.S3Bucket
	if bucket == "" {
		bucket = DefaultS3Bucket
	}
	region := cfg.S3Region
	if region == "" {
		region = DefaultS3Region
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		// S3-compatible servers reject the SDK's default trailing checksums on
		// some uploads; only calculate a checksum when the operation requires it.
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	}
	if cfg.S3AccessKey != "" && cfg.S3SecretKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, "", fmt.Errorf("artifacts: s3 store: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
	return client, bucket, nil
}

// Bucket returns the bucket this store reads and writes.
func (s *S3Store) Bucket() string { return s.bucket }

// Multipart defaults (F-orker-storage-perf-7). Above the threshold a seekable
// file is uploaded in parts read straight from it, so a failure re-sends one
// part rather than the whole object.
const (
	defaultMultipartThreshold = 64 << 20
	defaultPartSize           = 16 << 20
	multipartConcurrency      = 4
)

// Put uploads r under the provenance key derived from opts.Metadata. How it
// reads r depends on what r is:
//
//   - a seekable reader whose digest the caller already knows
//     (opts.ExpectedSHA256) is uploaded straight from itself: the first 512
//     bytes are sniffed for the content type, then the whole is sent once, with
//     the digest as an x-amz-checksum-sha256 the server verifies. Bytes that do
//     not match are refused and nothing is stored under the digest-named key.
//     Above the multipart threshold, a reader that is also an io.ReaderAt (a
//     file) is sent in parts instead;
//   - a seekable reader with no expectation (a log capture) is hashed in one
//     pass, rewound, and uploaded the same way;
//   - anything else is spooled to a file in opts.SpoolDir — the only shape
//     whose digest and length cannot be known any other way — and uploaded
//     from the spool.
func (s *S3Store) Put(ctx context.Context, name string, r io.Reader, opts PutOptions) (ArtifactRef, error) {
	if r == nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: nil reader", name)
	}
	if err := ctx.Err(); err != nil {
		return ArtifactRef{}, err
	}
	if opts.ExpectedSHA256 != "" {
		if !digestPattern.MatchString(opts.ExpectedSHA256) || opts.ExpectedSize < 0 {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: expected digest %q / size %d is not a "+
				"lowercase hex sha256 and a length", name, opts.ExpectedSHA256, opts.ExpectedSize)
		}
	}
	if seeker, ok := r.(io.ReadSeeker); ok {
		return s.putSeekable(ctx, name, seeker, opts)
	}
	return s.putSpooled(ctx, name, r, opts)
}

// putSeekable uploads from r itself, never from a copy.
func (s *S3Store) putSeekable(ctx context.Context, name string, r io.ReadSeeker, opts PutOptions) (ArtifactRef, error) {
	start, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: locate reader: %w", name, err)
	}
	var (
		sum  = opts.ExpectedSHA256
		size = opts.ExpectedSize
		head []byte
	)
	if sum != "" {
		if opts.ContentType == "" {
			head = make([]byte, sniffLen)
			n, err := io.ReadFull(&ctxReader{ctx: ctx, r: r}, head)
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: sniff: %w", name, err)
			}
			head = head[:n]
		}
	} else {
		hasher := sha256.New()
		sniff := &sniffWriter{}
		n, err := io.Copy(io.MultiWriter(hasher, sniff), &ctxReader{ctx: ctx, r: r})
		if err != nil {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: hash: %w", name, err)
		}
		sum, size, head = hex.EncodeToString(hasher.Sum(nil)), n, sniff.head
	}
	if _, err := r.Seek(start, io.SeekStart); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: rewind: %w", name, err)
	}
	// A file is also an io.ReaderAt: give the upload a section of exactly the
	// declared bytes, which multipart can read in parallel parts.
	var body io.ReadSeeker = r
	if at, ok := r.(io.ReaderAt); ok {
		body = io.NewSectionReader(at, start, size)
	}
	return s.upload(ctx, name, body, sum, size, resolveContentType(opts.ContentType, head), opts)
}

// putSpooled copies a non-seekable reader to a spool file in opts.SpoolDir,
// hashing it on the way, and uploads from the spool.
func (s *S3Store) putSpooled(ctx context.Context, name string, r io.Reader, opts PutOptions) (ArtifactRef, error) {
	if opts.SpoolDir != "" {
		if err := os.MkdirAll(opts.SpoolDir, 0o700); err != nil {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: create spool dir: %w", name, err)
		}
	}
	tmp, err := os.CreateTemp(opts.SpoolDir, "ork-artifact-*")
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: create spool file: %w", name, err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	hasher := sha256.New()
	sniff := &sniffWriter{}
	size, err := io.Copy(io.MultiWriter(tmp, hasher, sniff), &ctxReader{ctx: ctx, r: r})
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: spool: %w", name, err)
	}
	sum := hex.EncodeToString(hasher.Sum(nil))
	if opts.ExpectedSHA256 != "" && (sum != opts.ExpectedSHA256 || size != opts.ExpectedSize) {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: the bytes hash to %s (%d bytes), not the "+
			"expected %s (%d bytes)", name, sum, size, opts.ExpectedSHA256, opts.ExpectedSize)
	}
	return s.upload(ctx, name, io.NewSectionReader(tmp, 0, size), sum, size,
		resolveContentType(opts.ContentType, sniff.head), opts)
}

// upload sends body — exactly size bytes whose sha256 is sum — under the
// provenance key, then writes the name manifest when the key cannot hold the
// name.
func (s *S3Store) upload(
	ctx context.Context, name string, body io.ReadSeeker, sum string, size int64, contentType string, opts PutOptions,
) (ArtifactRef, error) {
	key := s3ObjectKey(ProvenanceFromMetadata(opts.Metadata), name, sum)
	metadata := copyMetadata(opts.Metadata)

	// The digest rides on the object, so Stat and List always report the digest
	// of the bytes actually stored under this key, even after an overwrite.
	stored := make(map[string]string, len(metadata)+1)
	for k, v := range metadata {
		stored[k] = v
	}
	stored[metaContentSHA256] = sum

	_, isFile := body.(io.ReaderAt)
	if isFile && size > s.threshold() {
		if err := s.uploadMultipart(ctx, key, body, contentType, stored); err != nil {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: %w", s.uri(key), err)
		}
	} else {
		raw, err := hex.DecodeString(sum)
		if err != nil {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: digest %q: %w", name, sum, err)
		}
		if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			Body:          body,
			ContentLength: aws.Int64(size),
			ContentType:   aws.String(contentType),
			Metadata:      stored,
			// The server recomputes this over the bytes it receives and refuses
			// a mismatch, so the object under a digest-named key is that digest.
			ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(raw)),
		}, withKnownPayloadHash(sum)); err != nil {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: %w", s.uri(key), err)
		}
	}

	// A clean artifact segment already IS the logical name, so only a name the
	// key cannot hold verbatim needs a companion manifest. The manifest follows
	// the object: an object without its manifest is invisible to Stat and List,
	// never a half-written artifact.
	if !isCleanS3Component(name) {
		if err := s.writeNameManifest(ctx, key, name); err != nil {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: %w", name, err)
		}
	}

	return ArtifactRef{
		Name:        name,
		URI:         s.uri(key),
		ContentType: contentType,
		SizeBytes:   size,
		SHA256:      sum,
		Metadata:    metadata,
	}, nil
}

// uploadMultipart sends a large file in parts, multipartConcurrency at a time,
// each read straight from the file. A part that fails is retried alone by the
// SDK; an upload that fails outright is aborted (LeavePartsOnError=false), and
// docs/DEPLOY.md's AbortIncompleteMultipartUpload lifecycle rule collects the
// parts of a process killed mid-upload.
//
// The whole-object digest cannot ride a multipart upload as one checksum, so
// it is recorded in the object's metadata as for every Put; each part is
// covered by the request's own payload signature (or TLS).
//
// The upload manager is marked deprecated in favour of the SDK's
// transfermanager. That module is still v0, needs a newer SDK core and S3
// client than this module pins, and copies each part into a pooled buffer
// where the manager reads it straight from the file, so moving to it is a
// change of its own, proven against object storage; until then this is the
// SDK's maintained multipart path.
func (s *S3Store) uploadMultipart(
	ctx context.Context, key string, body io.ReadSeeker, contentType string, stored map[string]string,
) error {
	//lint:ignore SA1019 manager is the multipart uploader the pinned SDK maintains; its v0 successor is a change of its own (see uploadMultipart)
	uploader := manager.NewUploader(s.client, func(u *manager.Uploader) {
		u.PartSize = s.part()
		u.Concurrency = multipartConcurrency
		u.LeavePartsOnError = false
		// The same rule the client is built with (newS3Client): no checksum an
		// S3-compatible server may refuse unless one is required.
		u.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	//lint:ignore SA1019 manager is the multipart uploader the pinned SDK maintains; its v0 successor is a change of its own (see uploadMultipart)
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String(contentType),
		Metadata:    stored,
	})
	return err
}

func (s *S3Store) threshold() int64 {
	if s.multipartThreshold > 0 {
		return s.multipartThreshold
	}
	return defaultMultipartThreshold
}

func (s *S3Store) part() int64 {
	if s.partSize > 0 {
		return s.partSize
	}
	return defaultPartSize
}

// withKnownPayloadHash signs a PutObject with the digest the caller already
// knows, rather than letting the SDK read the whole body once more to compute
// it over plain HTTP. Over TLS it replaces UNSIGNED-PAYLOAD with the real
// digest, which the server then verifies too.
func withKnownPayloadHash(sum string) func(*s3.Options) {
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("OrkKnownPayloadHash",
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
					middleware.InitializeOutput, middleware.Metadata, error) {
					return next.HandleInitialize(v4.SetPayloadHash(ctx, sum), in)
				}), middleware.After)
		})
	}
}

// Get opens the object body at uri. The caller closes it.
func (s *S3Store) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	key, _, err := s.parseURI(uri)
	if err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("artifacts: s3 get %q: %w", uri, ErrNotFound)
		}
		return nil, fmt.Errorf("artifacts: s3 get %q: %w", uri, err)
	}
	return out.Body, nil
}

// Stat heads the object and restores its exact logical name — from the key
// itself, or from the binary companion manifest when the key could not hold it.
func (s *S3Store) Stat(ctx context.Context, uri string) (ArtifactRef, error) {
	key, segments, err := s.parseURI(uri)
	if err != nil {
		return ArtifactRef{}, err
	}
	name, err := s.resolveName(ctx, key, segments)
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 stat %q: %w", uri, err)
	}
	return s.statKey(ctx, key, name)
}

// List scans the object hierarchy, restores each exact logical name, filters by
// the caller's logical prefix, and orders results by URI. Each object is also
// headed to match local-provider fields.
func (s *S3Store) List(ctx context.Context, prefix string) ([]ArtifactRef, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s3OrkflowNamespace + "/"),
	})

	var refs []ArtifactRef
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: s3 list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			segments, ok := splitS3ObjectKey(key)
			if !ok {
				continue
			}
			name, err := s.resolveName(ctx, key, segments)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue // deleted between list and manifest read
				}
				return nil, fmt.Errorf("artifacts: s3 list %q: %w", prefix, err)
			}
			if !strings.HasPrefix(name, prefix) {
				continue
			}
			ref, err := s.statKey(ctx, key, name)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue // deleted between list and head
				}
				return nil, fmt.Errorf("artifacts: s3 list %q: %w", prefix, err)
			}
			refs = append(refs, ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].URI < refs[j].URI })
	return refs, nil
}

// Presign returns a signed GET URL valid for ttl.
func (s *S3Store) Presign(ctx context.Context, uri string, ttl time.Duration) (string, error) {
	key, _, err := s.parseURI(uri)
	if err != nil {
		return "", err
	}
	if ttl <= 0 {
		return "", fmt.Errorf("artifacts: s3 presign %q: ttl must be positive", uri)
	}
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("artifacts: s3 presign %q: %w", uri, err)
	}
	return req.URL, nil
}

// Delete removes the object and any binary name manifest beside it. Like S3
// itself, it is idempotent.
func (s *S3Store) Delete(ctx context.Context, uri string) error {
	key, segments, err := s.parseURI(uri)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil && !isNotFound(err) {
		return fmt.Errorf("artifacts: s3 delete %q: %w", uri, err)
	}
	if _, exact := s3NameFromKey(segments); exact {
		return nil // the key held the name; there is no manifest to remove
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3ManifestKey(key)),
	}); err != nil && !isNotFound(err) {
		return fmt.Errorf("artifacts: s3 delete %q: remove name manifest: %w", uri, err)
	}
	return nil
}

// resolveName returns the exact logical name of an object. A key whose artifact
// segment is clean carries the name itself, which costs no request at all; only
// a sanitized segment needs its companion manifest.
func (s *S3Store) resolveName(ctx context.Context, key string, segments []string) (string, error) {
	if name, exact := s3NameFromKey(segments); exact {
		return name, nil
	}
	return s.readNameManifest(ctx, key)
}

func (s *S3Store) writeNameManifest(ctx context.Context, objectKey, name string) error {
	manifest := marshalS3NameManifest(objectKey, name)
	key := s3ManifestKey(objectKey)
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(manifest),
		ContentLength: aws.Int64(int64(len(manifest))),
		ContentType:   aws.String("application/octet-stream"),
	}); err != nil {
		return fmt.Errorf("write binary name manifest %q: %w", key, err)
	}
	return nil
}

func (s *S3Store) readNameManifest(ctx context.Context, objectKey string) (string, error) {
	key := s3ManifestKey(objectKey)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return "", fmt.Errorf("read binary name manifest %q: %w", key, ErrNotFound)
		}
		return "", fmt.Errorf("read binary name manifest %q: %w", key, err)
	}
	defer out.Body.Close()
	manifest, err := io.ReadAll(out.Body)
	if err != nil {
		return "", fmt.Errorf("read binary name manifest %q body: %w", key, err)
	}
	name, err := unmarshalS3NameManifest(manifest, objectKey)
	if err != nil {
		return "", fmt.Errorf("decode binary name manifest %q: %w", key, err)
	}
	return name, nil
}

func (s *S3Store) statKey(ctx context.Context, key, name string) (ArtifactRef, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return ArtifactRef{}, fmt.Errorf("artifacts: s3 stat %q: %w", s.uri(key), ErrNotFound)
		}
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 stat %q: %w", s.uri(key), err)
	}
	metadata := copyMetadata(out.Metadata)
	// The digest comes from the object's own metadata, never from ETag: ETag is
	// opaque and is not a content hash for multipart or encrypted objects.
	sum := metadataValue(metadata, metaContentSHA256)
	deleteMetadataKey(metadata, metaContentSHA256)
	if len(metadata) == 0 {
		metadata = nil
	}
	return ArtifactRef{
		Name:        name,
		URI:         s.uri(key),
		ContentType: aws.ToString(out.ContentType),
		SizeBytes:   aws.ToInt64(out.ContentLength),
		SHA256:      sum,
		Metadata:    metadata,
	}, nil
}

// uri renders the canonical s3:// URI for a generated physical object key.
func (s *S3Store) uri(key string) string {
	return (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + key}).String()
}

// parseURI validates an s3:// URI against this store's bucket and returns the
// physical object key with its segments.
func (s *S3Store) parseURI(uri string) (string, []string, error) {
	u, perr := url.Parse(uri)
	if perr != nil {
		return "", nil, fmt.Errorf("artifacts: s3 %q: %w: %v", uri, ErrInvalidURI, perr)
	}
	if u.Scheme != "s3" {
		return "", nil, fmt.Errorf("artifacts: s3 %q: %w: scheme must be s3://", uri, ErrInvalidURI)
	}
	if u.Host != s.bucket {
		return "", nil, fmt.Errorf("artifacts: s3 %q: %w: bucket must be %q", uri, ErrInvalidURI, s.bucket)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", nil, fmt.Errorf("artifacts: s3 %q: %w: query and fragment are not allowed", uri, ErrInvalidURI)
	}
	key := strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", nil, fmt.Errorf("artifacts: s3 %q: %w: empty key", uri, ErrInvalidURI)
	}
	segments, ok := splitS3ObjectKey(key)
	if !ok {
		return "", nil, fmt.Errorf("artifacts: s3 %q: %w: key must be %s/{orkflow-name}/{run-id}/{task-id}/{artifact-name}/{sha256}",
			uri, ErrInvalidURI, s3OrkflowNamespace)
	}
	return key, segments, nil
}

// metadataValue reads a metadata key regardless of case: S3 servers normalize
// user-metadata header names.
func metadataValue(meta map[string]string, key string) string {
	if value, ok := meta[key]; ok {
		return value
	}
	for k, v := range meta {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

func deleteMetadataKey(meta map[string]string, key string) {
	for k := range meta {
		if strings.EqualFold(k, key) {
			delete(meta, k)
		}
	}
}

// isNotFound reports whether err means "the object is not there". A 403 is
// never treated as not-found: it can mean the caller merely lacks permission to
// tell the difference.
func isNotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	// HeadObject has no body, so the SDK surfaces a bare HTTP response error.
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}
