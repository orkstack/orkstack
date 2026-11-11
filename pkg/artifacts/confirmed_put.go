package artifacts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// errOutcomeUnknown reports a conditional write whose answer was lost and
// whose object could not be read back: whether it landed is not known.
var errOutcomeUnknown = errors.New("the write's answer was lost and its object could not be read back, so whether it landed is unknown")

// putOutcome is what putConfirmed learned of a write that landed: the stored
// object's etag, and whether only a read showed it landed (readBack). Equal
// bytes read back after an unknown outcome are this write or another write of
// exactly the same bytes, and the two cannot be told apart, so a caller for
// whom "who wrote it" matters treats a readBack as an outcome it does not know.
// Each caller's comment says why equal bytes there are this write, or why it
// does not matter who wrote them.
type putOutcome struct {
	ETag     string
	ReadBack bool
}

// putConfirmed sends one PutObject of body and answers whether its write
// landed: its outcome and nil when it did, the PUT's own error when it did
// not.
//
// The SDK resends a PUT whose answer it lost — a 5xx, a dropped connection —
// and a resend of a conditional PUT meets its own first send when that send
// was stored: If-None-Match finds the object it created, and If-Match an etag
// it moved. The answer is then a 412, or the SDK gives up, for a write that
// landed. So when the outcome is unknown — the SDK resent the write, or its
// last answer was a 5xx or no answer at all — the key is read, and a key
// holding exactly body is this write, landed. Each document written this way
// carries an instant to the nanosecond or a fresh id, or is the same bytes
// whoever writes it (content addressing, a claim naming the version every
// claimant is answered with), so equal bytes after an unknown outcome are
// this write or one that leaves exactly what it would have; the outcome says
// it was read back, for the callers to whom that difference matters.
//
// An answer the store gave to the write's only send is final: a 412 then is
// another writer's, even over bytes equal to body — another attempt's
// identical commit, the boot probe's fixed candidate, a document stamped by a
// still clock. A refused write is never reported as landed. A read that finds
// other bytes or none leaves the PUT's error the answer; a read that fails
// leaves the outcome unknown (errOutcomeUnknown), never a refusal.
func putConfirmed(ctx context.Context, client *s3.Client, input *s3.PutObjectInput, body []byte) (putOutcome, error) {
	var resent atomic.Bool
	out, err := client.PutObject(ctx, input, func(o *s3.Options) {
		o.Retryer = noteResends(o.Retryer, &resent)
	})
	if err == nil {
		return putOutcome{ETag: aws.ToString(out.ETag)}, nil
	}
	if !resent.Load() && answeredByStore(err) {
		return putOutcome{}, err
	}
	switch etag, found := readBack(ctx, client, aws.ToString(input.Bucket), aws.ToString(input.Key), body); found {
	case readSame:
		return putOutcome{ETag: etag, ReadBack: true}, nil
	case readUnreadable:
		// Nothing can tell whether the write landed: neither its answer nor
		// the object. Its error must not read as a refusal — a 412 met after a
		// resend may be the write's own first send — so it is an outcome not
		// known.
		return putOutcome{}, &unknownOutcomeError{cause: err}
	default:
		// The key holds other bytes, or none: this write is not there.
		return putOutcome{}, err
	}
}

// unknownOutcomeError is a write's error once neither its answer nor a read
// of its object can say whether it landed. It is errOutcomeUnknown, and the
// cause too unless the cause was the store's 4xx answer, which after a lost
// answer may be to the write's own first send and so is no refusal of it.
type unknownOutcomeError struct{ cause error }

func (e *unknownOutcomeError) Error() string {
	return errOutcomeUnknown.Error() + ": " + e.cause.Error()
}

func (e *unknownOutcomeError) Unwrap() []error {
	if answeredByStore(e.cause) {
		return []error{errOutcomeUnknown}
	}
	return []error{errOutcomeUnknown, e.cause}
}

// refusedCondition reports whether err is the store refusing a conditional
// PUT's condition: a 412 or 409, or — for If-Match — a 404, which a store
// answers when the key is gone (a Delete took it), and which says the same:
// the object is not the one the write was conditioned on.
func refusedCondition(err error, input *s3.PutObjectInput) bool {
	return isPreconditionFailed(err) || (input.IfMatch != nil && isNotFound(err))
}

// answeredByStore reports whether err is the store's own refusal of a request
// it did not carry out: a 4xx. A 5xx, no response at all, or a response that
// was not a refusal (a 2xx or 3xx the client could not read) says nothing
// about whether the write was stored.
func answeredByStore(err error) bool {
	var response *smithyhttp.ResponseError
	if !errors.As(err, &response) {
		return false
	}
	status := response.HTTPStatusCode()
	return status >= 400 && status < 500
}

// readFound is what a read of a written key found.
type readFound int

const (
	readSame       readFound = iota // exactly the bytes written
	readOther                       // other bytes, or no object at all
	readUnreadable                  // the read failed: nothing is known
)

// readBack reads key and compares it with body, with the etag the read
// returned when it holds exactly body.
func readBack(ctx context.Context, client *s3.Client, bucket, key string, body []byte) (string, readFound) {
	object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return "", readOther
		}
		return "", readUnreadable
	}
	defer object.Body.Close()
	stored, err := io.ReadAll(io.LimitReader(object.Body, int64(len(body))+1))
	switch {
	case err != nil:
		return "", readUnreadable
	case !bytes.Equal(stored, body):
		return "", readOther
	}
	return aws.ToString(object.ETag), readSame
}

// noteResends is retryer, unchanged in every decision, with each resend it
// grants recorded in resent: the retry middleware takes a retry token before
// every resend and only then.
func noteResends(retryer aws.Retryer, resent *atomic.Bool) aws.Retryer {
	v2, ok := retryer.(aws.RetryerV2)
	if !ok {
		v2 = initialTokenRetryer{retryer}
	}
	return resendNotingRetryer{RetryerV2: v2, resent: resent}
}

type resendNotingRetryer struct {
	aws.RetryerV2
	resent *atomic.Bool
}

func (r resendNotingRetryer) GetRetryToken(ctx context.Context, opErr error) (func(error) error, error) {
	release, err := r.RetryerV2.GetRetryToken(ctx, opErr)
	if err == nil {
		r.resent.Store(true)
	}
	return release, err
}

// initialTokenRetryer is a retryer written before aws.RetryerV2, answering
// GetAttemptToken as the SDK does for one: with its initial token.
type initialTokenRetryer struct{ aws.Retryer }

func (r initialTokenRetryer) GetAttemptToken(context.Context) (func(error) error, error) {
	return r.GetInitialToken(), nil
}
