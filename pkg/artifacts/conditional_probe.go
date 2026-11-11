package artifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ConditionalAnswersProbePrefix holds the two objects the conditional-answers
// probe writes and removes at every boot that runs it.
const ConditionalAnswersProbePrefix = "metadata/probes/conditional-answers"

// probeAbsentETag is an etag no object probed has: the MD5 or SHA-256 of a
// body is never all zeros.
const probeAbsentETag = `"00000000000000000000000000000000"`

// probeConditionalAnswers asks the store the two conditional answers the
// orkflow store relies on besides a create (docs/STORAGE.md, "What the store
// must honour"), at every Orker's boot and every engine's. A PutObject with
// If-Match on a key that does not exist must be refused, 404 or 412, so a
// promotion whose pointer a Delete took never writes it again from nothing.
// A DeleteObject with If-Match must be held to its condition, refused 412 on
// another etag with the object left in place, so a rollback never removes an
// object another apply created since. The probe writes two small objects of
// its own, named afresh for this call so that processes booting at once never
// meet each other's, and removes them before it returns; a process killed
// mid-probe leaves at most those two.
func probeConditionalAnswers(ctx context.Context, client *s3.Client, bucket string) error {
	prefix := ConditionalAnswersProbePrefix + "/" + strings.ToLower(rand.Text()) + "/"
	missing, present := prefix+"missing", prefix+"present"
	defer func() {
		for _, key := range []string{missing, present} {
			_, _ = client.DeleteObject(context.WithoutCancel(ctx),
				&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		}
	}()
	body := []byte("conditional answers probe\n")
	put := func(key string, condition func(*s3.PutObjectInput)) (*s3.PutObjectOutput, error) {
		input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body)))}
		condition(input)
		return client.PutObject(ctx, input)
	}
	exists := func(key string) (bool, error) {
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if isNotFound(err) {
			return false, nil
		}
		return err == nil, err
	}

	_, err := put(missing, func(in *s3.PutObjectInput) { in.IfMatch = aws.String(probeAbsentETag) })
	switch {
	case err == nil:
		return fmt.Errorf("conditional write probe %s: the store stored a PutObject with If-Match on a key "+
			"that does not exist; it must refuse it (404 or 412)", probeURI(bucket, missing))
	case !isNotFound(err) && !isPreconditionFailed(err):
		return conditionalProbeError(probeURI(bucket, missing), "PutObject with If-Match on a key that does not exist", err)
	}

	out, err := put(present, func(in *s3.PutObjectInput) { in.IfNoneMatch = aws.String("*") })
	if err != nil {
		return conditionalProbeError(probeURI(bucket, present), "create", err)
	}
	etag := aws.ToString(out.ETag)
	if etag == "" {
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(present)})
		if err != nil {
			return conditionalProbeError(probeURI(bucket, present), "read the created object's etag", err)
		}
		etag = aws.ToString(head.ETag)
	}
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(present),
		IfMatch: aws.String(probeAbsentETag)})
	survived, readErr := exists(present)
	switch {
	case readErr != nil:
		return conditionalProbeError(probeURI(bucket, present), "read after a DeleteObject with If-Match naming another etag", readErr)
	case !survived:
		return fmt.Errorf("conditional write probe %s: the store deleted an object under a DeleteObject with "+
			"If-Match naming another etag; it must refuse it (412) and keep the object", probeURI(bucket, present))
	case err == nil:
		return fmt.Errorf("conditional write probe %s: the store answered a DeleteObject with If-Match naming "+
			"another etag as done; it must refuse it (412)", probeURI(bucket, present))
	case !isPreconditionFailed(err):
		return conditionalProbeError(probeURI(bucket, present), "DeleteObject with If-Match naming another etag", err)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket),
		Key: aws.String(present), IfMatch: aws.String(etag)}); err != nil {
		return conditionalProbeError(probeURI(bucket, present), "DeleteObject with If-Match naming the object's etag", err)
	}
	switch kept, err := exists(present); {
	case err != nil:
		return conditionalProbeError(probeURI(bucket, present), "read after a DeleteObject with If-Match naming the object's etag", err)
	case kept:
		return fmt.Errorf("conditional write probe %s: the store kept an object a DeleteObject with If-Match "+
			"naming its etag deleted", probeURI(bucket, present))
	}
	return nil
}

// ProbeConditionalAnswers asks the store the conditional answers this store's
// rollback and promotion rely on (probeConditionalAnswers). The engine asks
// them at boot, before it accepts an apply, as every Orker does: an error
// matching ErrCommitStoreUnavailable means the store could not be asked, and
// any other means it answered one of them wrongly.
func (s *S3OrkflowStore) ProbeConditionalAnswers(ctx context.Context) error {
	return probeConditionalAnswers(ctx, s.client, s.bucket)
}

// probeURI is the s3:// URI of one probe object, for a message.
func probeURI(bucket, key string) string {
	return (&url.URL{Scheme: "s3", Host: bucket, Path: "/" + key}).String()
}

// conditionalProbeError is a probe request's failure: the store could not be
// used at all (ErrCommitStoreUnavailable), or it answered and failed the probe.
func conditionalProbeError(uri, what string, err error) error {
	if isStoreUnavailable(err) || errors.Is(err, errOutcomeUnknown) {
		return fmt.Errorf("%w: conditional write probe %s: %s: %w", ErrCommitStoreUnavailable, uri, what, err)
	}
	return fmt.Errorf("conditional write probe %s: %s: %w", uri, what, err)
}
