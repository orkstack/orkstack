package artifacts

import (
	"context"
	"crypto/sha256"
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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Store is the S3-compatible Store. It targets AWS S3 and any compatible
// server (MinIO in this repo's dev stack) through a custom endpoint plus
// path-style addressing.
//
// Objects use the same content-addressed key as the local store,
// "<name>/<sha256>", and the canonical URI is s3://<bucket>/<key>.
type S3Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
}

// compile-time contract check.
var _ Store = (*S3Store)(nil)

// NewS3Store builds an S3-backed store from cfg. Static credentials are used
// when both key fields are set; otherwise the standard AWS credential chain
// applies, so the same code serves MinIO and real AWS.
func NewS3Store(ctx context.Context, cfg Config) (*S3Store, error) {
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
		return nil, fmt.Errorf("artifacts: s3 store: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
	return &S3Store{client: client, presign: s3.NewPresignClient(client), bucket: bucket}, nil
}

// Bucket returns the bucket this store reads and writes.
func (s *S3Store) Bucket() string { return s.bucket }

// Put spools the reader to a local temp file to compute its digest and length,
// then uploads it under the content-addressed key. Spooling keeps the upload
// body seekable, so an unknown-length, non-seekable reader is supported without
// buffering it all in memory.
func (s *S3Store) Put(ctx context.Context, name string, r io.Reader, opts PutOptions) (ArtifactRef, error) {
	cleaned, err := cleanName(name)
	if err != nil {
		return ArtifactRef{}, err
	}
	if r == nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: nil reader", cleaned)
	}
	if err := ctx.Err(); err != nil {
		return ArtifactRef{}, err
	}

	tmp, err := os.CreateTemp("", "ork-artifact-*")
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: create spool file: %w", cleaned, err)
	}
	defer func() {
		tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	hasher := sha256.New()
	sniff := &sniffWriter{}
	size, err := io.Copy(io.MultiWriter(tmp, hasher, sniff), &ctxReader{ctx: ctx, r: r})
	if err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: spool: %w", cleaned, err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: rewind spool: %w", cleaned, err)
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	key := objectKey(cleaned, sum)
	contentType := resolveContentType(opts.ContentType, sniff.head)
	metadata := copyMetadata(opts.Metadata)

	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          tmp,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
		Metadata:      metadata,
	}); err != nil {
		return ArtifactRef{}, fmt.Errorf("artifacts: s3 put %q: %w", s.uri(key), err)
	}

	return ArtifactRef{
		Name:        cleaned,
		URI:         s.uri(key),
		ContentType: contentType,
		SizeBytes:   size,
		SHA256:      sum,
		Metadata:    metadata,
	}, nil
}

// Get opens the object body at uri. The caller closes it.
func (s *S3Store) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	key, _, _, err := s.parseURI(uri)
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

// Stat heads the object and returns its reference.
func (s *S3Store) Stat(ctx context.Context, uri string) (ArtifactRef, error) {
	key, name, sum, err := s.parseURI(uri)
	if err != nil {
		return ArtifactRef{}, err
	}
	return s.statKey(ctx, key, name, sum)
}

// List returns every object whose logical name starts with prefix, ordered by
// URI. Each entry is completed with a HeadObject so listings carry the same
// fields the local store returns.
func (s *S3Store) List(ctx context.Context, prefix string) ([]ArtifactRef, error) {
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	var refs []ArtifactRef
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifacts: s3 list %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			name, sum, ok := splitKey(key)
			if !ok || !strings.HasPrefix(name, prefix) {
				continue
			}
			ref, err := s.statKey(ctx, key, name, sum)
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
	key, _, _, err := s.parseURI(uri)
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

// Delete removes the object. Like S3 itself, it is idempotent.
func (s *S3Store) Delete(ctx context.Context, uri string) error {
	key, _, _, err := s.parseURI(uri)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("artifacts: s3 delete %q: %w", uri, err)
	}
	return nil
}

func (s *S3Store) statKey(ctx context.Context, key, name, sum string) (ArtifactRef, error) {
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
	return ArtifactRef{
		Name:        name,
		URI:         s.uri(key),
		ContentType: aws.ToString(out.ContentType),
		SizeBytes:   aws.ToInt64(out.ContentLength),
		// The digest comes from the key, never from ETag: ETag is opaque and is
		// not a content hash for multipart or encrypted objects.
		SHA256:   sum,
		Metadata: copyMetadata(out.Metadata),
	}, nil
}

// uri renders the canonical s3:// URI for an object key, percent-encoding
// characters such as '#', '?', and '%' while preserving key separators.
func (s *S3Store) uri(key string) string {
	return (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + key}).String()
}

// parseURI validates an s3:// URI against this store's bucket.
func (s *S3Store) parseURI(uri string) (key, name, sum string, err error) {
	u, perr := url.Parse(uri)
	if perr != nil {
		return "", "", "", fmt.Errorf("artifacts: s3 %q: %w: %v", uri, ErrInvalidURI, perr)
	}
	if u.Scheme != "s3" {
		return "", "", "", fmt.Errorf("artifacts: s3 %q: %w: scheme must be s3://", uri, ErrInvalidURI)
	}
	if u.Host != s.bucket {
		return "", "", "", fmt.Errorf("artifacts: s3 %q: %w: bucket must be %q", uri, ErrInvalidURI, s.bucket)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", "", "", fmt.Errorf("artifacts: s3 %q: %w: query and fragment are not allowed", uri, ErrInvalidURI)
	}
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", "", fmt.Errorf("artifacts: s3 %q: %w: empty key", uri, ErrInvalidURI)
	}
	name, sum, ok := splitKey(key)
	if !ok {
		return "", "", "", fmt.Errorf("artifacts: s3 %q: %w: key must end with /<sha256>", uri, ErrInvalidURI)
	}
	return key, name, sum, nil
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
