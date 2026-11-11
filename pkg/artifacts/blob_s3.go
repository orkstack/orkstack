package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// s3Blobs is the shared S3-compatible bucket. Each method is exactly one S3
// request, so a store's request pattern is its own calls, one for one.
type s3Blobs struct {
	client *s3.Client
	bucket string
}

var _ blobs = (*s3Blobs)(nil)

// openS3Blobs builds the bucket client from the same ORK_S3_* config every
// store uses.
func openS3Blobs(ctx context.Context, cfg Config) (*s3Blobs, error) {
	client, bucket, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &s3Blobs{client: client, bucket: bucket}, nil
}

func (b *s3Blobs) uri(key string) string { return "s3://" + b.bucket + "/" + key }

func (b *s3Blobs) locate(key string) string {
	return (&url.URL{Scheme: "s3", Host: b.bucket, Path: "/" + key}).String()
}

func (b *s3Blobs) resolve(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("%q: %w: %v", uri, ErrInvalidURI, err)
	}
	if u.Scheme != "s3" {
		return "", fmt.Errorf("%q is not an s3 uri: %w", uri, ErrInvalidURI)
	}
	if u.Host != b.bucket {
		return "", fmt.Errorf("%q names bucket %q, this store owns %q: %w", uri, u.Host, b.bucket, ErrInvalidURI)
	}
	return strings.TrimPrefix(u.Path, "/"), nil
}

func (b *s3Blobs) get(ctx context.Context, key string, max int64) ([]byte, string, error) {
	object, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return nil, "", errBlobNotFound
		}
		return nil, "", err
	}
	var reader io.Reader = object.Body
	if max > 0 {
		reader = io.LimitReader(object.Body, max)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := object.Body.Close()
	if readErr != nil {
		return nil, "", fmt.Errorf("read body: %w", readErr)
	}
	if closeErr != nil {
		return nil, "", fmt.Errorf("close body: %w", closeErr)
	}
	return body, aws.ToString(object.ETag), nil
}

func (b *s3Blobs) getRange(ctx context.Context, key string, offset, length int64) ([]byte, int64, error) {
	object, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, 0, errBlobNotFound
		}
		if isInvalidRange(err) {
			return nil, 0, errBlobRange
		}
		return nil, 0, err
	}
	defer object.Body.Close()
	total, ok := contentRangeTotal(aws.ToString(object.ContentRange))
	if !ok {
		return nil, 0, fmt.Errorf("unreadable Content-Range %q", aws.ToString(object.ContentRange))
	}
	data, err := io.ReadAll(io.LimitReader(object.Body, length+1))
	if err != nil {
		return nil, total, fmt.Errorf("read body: %w", err)
	}
	return data, total, nil
}

func (b *s3Blobs) head(ctx context.Context, key string) (int64, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return 0, errBlobNotFound
		}
		return 0, err
	}
	return aws.ToInt64(out.ContentLength), nil
}

func (b *s3Blobs) putInput(key, contentType string, body []byte) *s3.PutObjectInput {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	return in
}

func (b *s3Blobs) put(ctx context.Context, key, contentType string, body []byte) error {
	_, err := b.client.PutObject(ctx, b.putInput(key, contentType, body))
	return err
}

func (b *s3Blobs) create(ctx context.Context, key, contentType string, body []byte) error {
	in := b.putInput(key, contentType, body)
	in.IfNoneMatch = aws.String("*")
	// Equal bytes after a lost answer (putConfirmed) are this create, or are
	// harmless: an audit, trigger or webhook record carries a fresh id; a
	// delivery record is one settled delivery, whose caller takes one already
	// there as recorded; a continuation object and a delivery body are
	// content-addressed; a cleanup record's caller ignores one already there.
	if _, err := putConfirmed(ctx, b.client, in, body); err != nil {
		if isPreconditionFailed(err) {
			return errBlobExists
		}
		return err
	}
	return nil
}

func (b *s3Blobs) replace(ctx context.Context, key, contentType string, body []byte, version string) error {
	in := b.putInput(key, contentType, body)
	in.IfMatch = aws.String(version)
	// Equal bytes after a lost answer (putConfirmed) leave exactly the record
	// this trigger or webhook update meant to write, whoever wrote them.
	if _, err := putConfirmed(ctx, b.client, in, body); err != nil {
		// A conditional write on an object deleted since the read answers
		// NotFound rather than PreconditionFailed: either way the object is
		// not the one that was read.
		if isPreconditionFailed(err) || isNotFound(err) {
			return errBlobChanged
		}
		return err
	}
	return nil
}

func (b *s3Blobs) list(ctx context.Context, in blobListInput) (blobListPage, error) {
	input := &s3.ListObjectsV2Input{Bucket: aws.String(b.bucket), Prefix: aws.String(in.Prefix)}
	if in.StartAfter != "" {
		input.StartAfter = aws.String(in.StartAfter)
	}
	if in.Delimiter != "" {
		input.Delimiter = aws.String(in.Delimiter)
	}
	if in.MaxKeys > 0 {
		input.MaxKeys = aws.Int32(in.MaxKeys)
	}
	if in.Token != "" {
		input.ContinuationToken = aws.String(in.Token)
	}
	out, err := b.client.ListObjectsV2(ctx, input)
	if err != nil {
		return blobListPage{}, err
	}
	page := blobListPage{
		Objects:   make([]blobObject, 0, len(out.Contents)),
		Truncated: aws.ToBool(out.IsTruncated),
		Next:      aws.ToString(out.NextContinuationToken),
	}
	for _, object := range out.Contents {
		listed := blobObject{Key: aws.ToString(object.Key), Size: aws.ToInt64(object.Size)}
		if object.LastModified != nil {
			listed.Modified = object.LastModified.UTC()
		}
		page.Objects = append(page.Objects, listed)
	}
	for _, common := range out.CommonPrefixes {
		page.Prefixes = append(page.Prefixes, aws.ToString(common.Prefix))
	}
	return page, nil
}

func (b *s3Blobs) remove(ctx context.Context, key string) error {
	if _, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)}); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// removeKeys is one DeleteObjects request in quiet mode, so the response names
// only the keys that were NOT deleted. S3 reports deleting an absent key as
// success.
func (b *s3Blobs) removeKeys(ctx context.Context, keys []string) (map[string]error, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	objects := make([]types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
	}
	out, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(b.bucket),
		Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return nil, err
	}
	var failed map[string]error
	for _, refused := range out.Errors {
		if failed == nil {
			failed = map[string]error{}
		}
		failed[aws.ToString(refused.Key)] = fmt.Errorf("delete %q: %s: %s",
			aws.ToString(refused.Key), aws.ToString(refused.Code), aws.ToString(refused.Message))
	}
	return failed, nil
}

// contentRangeTotal parses the total size out of "bytes a-b/total".
func contentRangeTotal(header string) (int64, bool) {
	slash := strings.LastIndexByte(header, '/')
	if !strings.HasPrefix(header, "bytes ") || slash < 0 {
		return 0, false
	}
	total, err := strconv.ParseInt(header[slash+1:], 10, 64)
	if err != nil || total < 0 {
		return 0, false
	}
	return total, true
}

// isInvalidRange reports a range read past an object's end (416).
func isInvalidRange(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
		return true
	}
	var respErr *smithyhttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable
}
