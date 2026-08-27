package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

var (
	_ CommitStore  = (*LocalCommitStore)(nil)
	_ CommitStore  = (*S3CommitStore)(nil)
	_ RefPredictor = (*LocalStore)(nil)
	_ RefPredictor = (*S3Store)(nil)
)

// LocalCommitStore is the name-addressed completion-record store beside a
// LocalStore. It is separate because Store.Get returns bytes while
// CommitStore.Get returns a decoded immutable record.
type LocalCommitStore struct{ dir string }

func NewLocalCommitStore(dir string) (*LocalCommitStore, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("artifacts: local commit store: resolve %q: %w", dir, err)
	}
	if err := os.MkdirAll(filepath.Join(abs, localTempDir), localDirPerm); err != nil {
		return nil, err
	}
	return &LocalCommitStore{dir: filepath.Clean(abs)}, nil
}

// S3CommitStore shares S3 configuration with S3Store while exposing the
// commit-specific Create/Get contract.
type S3CommitStore struct {
	client *s3.Client
	bucket string
}

func NewS3CommitStore(ctx context.Context, cfg Config) (*S3CommitStore, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &S3CommitStore{client: client, bucket: bucket}, nil
}
func (s *S3CommitStore) uri(key string) string {
	return (&url.URL{Scheme: "s3", Host: s.bucket, Path: "/" + key}).String()
}

// Create publishes a local completion record with the POSIX primitive that is
// both atomic and create-if-absent: link(2). The canonical path is never opened
// for writing and is never the destination of rename(2).
func (s *LocalCommitStore) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (CommitRecord, error) {
	if err := ctx.Err(); err != nil {
		return CommitRecord{}, err
	}
	document, err := MarshalCommit(c)
	if err != nil {
		return CommitRecord{}, err
	}
	if len(document) > CommitMaxBytes {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit create %q: %d bytes exceeds %d: %w", uri, len(document), CommitMaxBytes, ErrInvalidCommitKey)
	}
	dst, err := s.commitPath(uri)
	if err != nil {
		return CommitRecord{}, err
	}
	if err := os.MkdirAll(filepath.Dir(dst), localDirPerm); err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit create %q: mkdir: %w", uri, err)
	}

	tmp, err := os.CreateTemp(filepath.Join(s.dir, localTempDir), "commit-*")
	if err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit create %q: temp: %w", uri, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err = tmp.Write(document); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit create %q: durable temp: %w", uri, err)
	}

	// Retry only an interrupted syscall. EEXIST is the immutable winner and is
	// never retried or replaced.
	err = os.Link(tmpName, dst)
	if errors.Is(err, syscall.EINTR) {
		err = os.Link(tmpName, dst)
	}
	if errors.Is(err, fs.ErrExist) {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit create %q: %w", uri, ErrPreconditionFailed)
	}
	if err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit create %q: link: %w", uri, err)
	}
	if err := syncDirectory(filepath.Dir(dst)); err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit create %q: sync parent: %w", uri, err)
	}
	return recordFor(document, c)
}

func (s *LocalCommitStore) Get(ctx context.Context, uri string) (CommitRecord, error) {
	if err := ctx.Err(); err != nil {
		return CommitRecord{}, err
	}
	filename, err := s.commitPath(uri)
	if err != nil {
		return CommitRecord{}, err
	}
	document, err := os.ReadFile(filename)
	if errors.Is(err, fs.ErrNotExist) {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit get %q: %w", uri, ErrNotFound)
	}
	if err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: local commit get %q: %w", uri, err)
	}
	c, err := UnmarshalCommit(document)
	if err != nil {
		return CommitRecord{Bytes: int64(len(document))}, fmt.Errorf("artifacts: local commit get %q: %w", uri, err)
	}
	return recordFor(document, c)
}

func (s *LocalCommitStore) commitPath(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "file" || (u.Host != "" && u.Host != "localhost") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("artifacts: local commit uri %q: %w", raw, ErrInvalidURI)
	}
	filename := filepath.Clean(filepath.FromSlash(u.Path))
	rel, err := filepath.Rel(s.dir, filename)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("artifacts: local commit uri %q is outside %s: %w", raw, s.dir, ErrInvalidURI)
	}
	if !strings.HasPrefix(filepath.ToSlash(rel), CommitPrefix+"/") || filepath.Base(filename) != CommitObjectName {
		return "", fmt.Errorf("artifacts: local commit uri %q is not a commit key: %w", raw, ErrInvalidURI)
	}
	return filename, nil
}

func syncDirectory(name string) error {
	d, err := os.Open(name)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *LocalCommitStore) commitProbeURI(token string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(s.dir, CommitPrefix, "default", "probe-"+token, "probe", CommitObjectName))}).String()
}

func (s *S3CommitStore) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (CommitRecord, error) {
	key, err := s.commitKey(uri)
	if err != nil {
		return CommitRecord{}, err
	}
	document, err := MarshalCommit(c)
	if err != nil {
		return CommitRecord{}, err
	}
	if len(document) > CommitMaxBytes {
		return CommitRecord{}, fmt.Errorf("artifacts: s3 commit create %q: %d bytes exceeds %d: %w", uri, len(document), CommitMaxBytes, ErrInvalidCommitKey)
	}
	var out *s3.PutObjectOutput
	for attempt := 0; attempt < 2; attempt++ {
		out, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(document), ContentLength: aws.Int64(int64(len(document))), ContentType: aws.String(CommitContentType), IfNoneMatch: aws.String("*")})
		if err == nil {
			break
		}
		if isPreconditionFailed(err) {
			return CommitRecord{}, fmt.Errorf("artifacts: s3 commit create %q: %w", uri, ErrPreconditionFailed)
		}
		if !isConditionalConflict(err) || attempt == 1 {
			return CommitRecord{}, fmt.Errorf("artifacts: s3 commit create %q: %w", uri, err)
		}
	}
	rec, _ := recordFor(document, c)
	rec.ETag = aws.ToString(out.ETag)
	return rec, nil
}

func (s *S3CommitStore) Get(ctx context.Context, uri string) (CommitRecord, error) {
	key, err := s.commitKey(uri)
	if err != nil {
		return CommitRecord{}, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if isNotFound(err) {
		return CommitRecord{}, fmt.Errorf("artifacts: s3 commit get %q: %w", uri, ErrNotFound)
	}
	if err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: s3 commit get %q: %w", uri, err)
	}
	defer out.Body.Close()
	document, err := io.ReadAll(io.LimitReader(out.Body, CommitMaxBytes+1))
	if err != nil {
		return CommitRecord{}, fmt.Errorf("artifacts: s3 commit get %q: %w", uri, err)
	}
	if len(document) > CommitMaxBytes {
		return CommitRecord{Bytes: int64(len(document))}, fmt.Errorf("artifacts: s3 commit get %q: oversized document: %w", uri, ErrCommitUndecodable)
	}
	c, err := UnmarshalCommit(document)
	if err != nil {
		return CommitRecord{ETag: aws.ToString(out.ETag), Bytes: int64(len(document))}, fmt.Errorf("artifacts: s3 commit get %q: %w", uri, err)
	}
	rec, _ := recordFor(document, c)
	rec.ETag = aws.ToString(out.ETag)
	return rec, nil
}

func (s *S3CommitStore) commitKey(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host != s.bucket || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("artifacts: s3 commit uri %q: %w", raw, ErrInvalidURI)
	}
	key := strings.TrimPrefix(u.Path, "/")
	if !strings.HasPrefix(key, CommitPrefix+"/") || !strings.HasSuffix(key, "/"+CommitObjectName) {
		return "", fmt.Errorf("artifacts: s3 commit uri %q is not a commit key: %w", raw, ErrInvalidURI)
	}
	return key, nil
}
func (s *S3CommitStore) commitProbeURI(token string) string {
	return s.uri(CommitPrefix + "/default/probe-" + token + "/probe/" + CommitObjectName)
}

func isConditionalConflict(err error) bool {
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "ConditionalRequestConflict" {
		return true
	}
	var response *smithyhttp.ResponseError
	return errors.As(err, &response) && response.HTTPStatusCode() == http.StatusConflict
}
