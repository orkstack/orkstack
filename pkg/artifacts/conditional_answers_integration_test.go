//go:build integration

package artifacts_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_S3_ConditionalWriteAnswers pins the two answers of the real
// store that the conditional writes rely on (docs/STORAGE.md): a PutObject with
// If-Match on a key that does not exist (a Delete took it away) is refused,
// 404 or 412 — SeaweedFS answers 404 — and a DeleteObject with If-Match is held
// to its condition: a wrong etag is refused 412 and the object stays, which a
// rollback's removal depends on. A store bump that ignores the delete
// condition fails here.
func TestIntegration_S3_ConditionalWriteAnswers(t *testing.T) {
	_, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	prefix := "probes/conditional-answers/" + uniqueToken(t) + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })
	put := func(key string, body []byte, ifMatch string) (string, error) {
		input := &s3.PutObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
			Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body)))}
		if ifMatch != "" {
			input.IfMatch = aws.String(ifMatch)
		}
		out, err := raw.PutObject(ctx, input)
		if err != nil {
			return "", err
		}
		return aws.ToString(out.ETag), nil
	}
	exists := func(key string) bool {
		_, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
		return err == nil
	}

	// If-Match on a key that is not there.
	_, err := put(prefix+"missing", []byte("x"), `"00000000000000000000000000000000"`)
	missing := statusOf(err)
	if missing != http.StatusNotFound && missing != http.StatusPreconditionFailed {
		t.Fatalf("PutObject If-Match on a missing key = %v (status %d), want 404 or 412", err, missing)
	}

	// DeleteObject If-Match with an etag that is not the object's.
	key := prefix + "present"
	etag, err := put(key, []byte("present"), "")
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	_, err = raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
		IfMatch: aws.String(`"00000000000000000000000000000000"`)})
	wrongStatus, survived := statusOf(err), exists(key)
	honoured := wrongStatus == http.StatusPreconditionFailed && survived
	if !honoured {
		t.Fatalf("DeleteObject If-Match with a wrong etag = %v (status %d), object survived %v: the store "+
			"ignores the delete condition, which a rollback's removal relies on", err, wrongStatus, survived)
	}
	_, rightErr := raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
		IfMatch: aws.String(etag)})
	if rightErr != nil || exists(key) {
		t.Fatalf("DeleteObject If-Match with the object's etag = %v, object gone %v; want it deleted", rightErr, !exists(key))
	}
	line := fmt.Sprintf("PutObject If-Match on a missing key: status %d. DeleteObject If-Match with a wrong etag: "+
		"status %d (error %v), object survived %v, so the condition is honoured: %v; with the object's etag: %v, "+
		"object gone %v.\n", missing, wrongStatus, err, survived, honoured, rightErr, !exists(key))
	t.Log(line)
	proof.Write(t, "artifacts", "conditional-answers.txt", []byte(line))
}

// statusOf is the HTTP status an S3 call's error carries, 0 for none.
func statusOf(err error) int {
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		return response.HTTPStatusCode()
	}
	return 0
}
