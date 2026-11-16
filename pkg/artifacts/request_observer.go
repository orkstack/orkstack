package artifacts

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
)

// RequestObservation is one call a store made to the S3-compatible bucket:
// what kind, which family of keys, how it ended, how many bytes it declared
// and how long it took, retries included. It never carries the key, which
// holds run ids and names.
type RequestObservation struct {
	// Operation is get, put, head, list, delete, delete_batch,
	// multipart or other.
	Operation string
	// Family is the kind of record the key belongs to (requestFamily).
	Family string
	// Outcome is RequestOK, RequestNotFound, RequestConflict, RequestCanceled
	// or RequestError.
	Outcome string
	// Bytes is a put's or an upload part's declared length, or a get's
	// returned Content-Length; 0 for every other call.
	Bytes    int64
	Duration time.Duration
}

// The outcomes of a request.
const (
	RequestOK       = "ok"
	RequestNotFound = "not_found"
	// RequestConflict is a conditional write that lost (If-Match,
	// If-None-Match): a decision the store branches on, not a fault.
	RequestConflict = "conflict"
	RequestCanceled = "canceled"
	RequestError    = "error"
)

// FamilyOther is a key outside every family requestFamily knows.
const FamilyOther = "other"

// RequestObserver receives every observation. It runs inline with the
// request, so it must not block.
type RequestObserver func(context.Context, RequestObservation)

var requestObserver atomic.Pointer[RequestObserver]

// SetRequestObserver makes o the process's observer of S3 requests; nil
// stops observing. It is process-wide because every store of a process opens
// its own client, and the process has one telemetry pipeline.
func SetRequestObserver(o RequestObserver) {
	if o == nil {
		requestObserver.Store(nil)
		return
	}
	requestObserver.Store(&o)
}

// observeRequests adds the observer to a client's stack, after the SDK has
// named the operation and before its retries, so a call is observed once.
func observeRequests(stack *middleware.Stack) error {
	return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("OrkStackRequestObserver",
		func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
			middleware.InitializeOutput, middleware.Metadata, error,
		) {
			observe := requestObserver.Load()
			if observe == nil || ctx.Value(notARequestKey{}) != nil {
				return next.HandleInitialize(ctx, in)
			}
			start := time.Now()
			out, metadata, err := next.HandleInitialize(ctx, in)
			key, sent := requestKey(in.Parameters)
			report(ctx, *observe, RequestObservation{
				Operation: requestOperation(awsmiddleware.GetOperationName(ctx)),
				Family:    requestFamily(key),
				Outcome:   requestOutcome(ctx, err),
				Bytes:     sent + receivedBytes(out.Result, err),
				Duration:  time.Since(start),
			})
			return out, metadata, err
		}), middleware.After)
}

// report hands o one observation. An observer that panics loses that
// observation and never the request it describes: observing can fail
// nothing. Each panic is counted (ObserverPanics), and the first logged.
func report(ctx context.Context, o RequestObserver, r RequestObservation) {
	defer func() {
		if p := recover(); p != nil {
			if observerPanics.Add(1) == 1 {
				slog.Default().Warn("store request observer panicked; store request metrics are lost until it is fixed", "panic", p)
			}
		}
	}()
	o(ctx, r)
}

var observerPanics atomic.Int64

// ObserverPanics is how many observations the request observer lost to a
// panic in this process.
func ObserverPanics() int64 { return observerPanics.Load() }

// notARequestKey marks a context whose SDK call sends nothing — presigning a
// URL runs the stack but never reaches the store — so it is not observed.
type notARequestKey struct{}

func notARequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, notARequestKey{}, true)
}

func requestOperation(name string) string {
	switch name {
	case "GetObject":
		return "get"
	case "PutObject":
		return "put"
	case "HeadObject":
		return "head"
	case "ListObjectsV2":
		return "list"
	case "DeleteObject":
		return "delete"
	case "DeleteObjects":
		return "delete_batch"
	case "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload", "AbortMultipartUpload":
		return "multipart"
	}
	return "other"
}

// requestKey is the key (or listing prefix) a call names, and the bytes it
// declares sending.
func requestKey(params any) (key string, sent int64) {
	switch in := params.(type) {
	case *s3.GetObjectInput:
		return deref(in.Key), 0
	case *s3.PutObjectInput:
		return deref(in.Key), derefInt(in.ContentLength)
	case *s3.HeadObjectInput:
		return deref(in.Key), 0
	case *s3.ListObjectsV2Input:
		return deref(in.Prefix), 0
	case *s3.DeleteObjectInput:
		return deref(in.Key), 0
	case *s3.DeleteObjectsInput:
		if in.Delete != nil && len(in.Delete.Objects) > 0 {
			return deref(in.Delete.Objects[0].Key), 0
		}
	case *s3.UploadPartInput:
		return deref(in.Key), derefInt(in.ContentLength)
	case *s3.CreateMultipartUploadInput:
		return deref(in.Key), 0
	case *s3.CompleteMultipartUploadInput:
		return deref(in.Key), 0
	case *s3.AbortMultipartUploadInput:
		return deref(in.Key), 0
	}
	return "", 0
}

func receivedBytes(result any, err error) int64 {
	if err != nil {
		return 0
	}
	if out, ok := result.(*s3.GetObjectOutput); ok {
		return derefInt(out.ContentLength)
	}
	return 0
}

func requestOutcome(ctx context.Context, err error) string {
	switch {
	case err == nil:
		return RequestOK
	case isNotFound(err):
		return RequestNotFound
	case isPreconditionFailed(err):
		return RequestConflict
	case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		return RequestCanceled
	}
	return RequestError
}

// requestFamilies are the record families under metadata/, and the
// top-level prefixes, that a store writes. A key in none of them is
// FamilyOther; a metadata/ key in none is "metadata". The set is fixed in
// code, so a family label never carries a name a caller chose.
var (
	metadataFamilies = map[string]bool{
		"attempts": true, "attested": true, "audit": true, "cleanup": true, "commits": true,
		"deliveries": true, "delivery-bodies": true, "identity": true, "index": true, "logs": true,
		"memo": true, "memos": true, "namespaces": true, "orkflows": true, "probes": true,
		"ranged": true, "receipts": true, "refs": true, "runs": true, "schedules": true,
		"settings": true, "snapshots": true, "staging": true, "triggers": true, "webhooks": true,
	}
	topFamilies = map[string]bool{
		objectNamespace: true, "orkflows": true, "manifests": true, "records": true, "outputs": true,
	}
)

// requestFamily is the family of key (requestFamilies).
func requestFamily(key string) string {
	first, rest, _ := strings.Cut(key, "/")
	if first == "metadata" {
		second, _, _ := strings.Cut(rest, "/")
		if metadataFamilies[second] {
			return second
		}
		return "metadata"
	}
	if topFamilies[first] {
		return first
	}
	return FamilyOther
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(n *int64) int64 {
	if n == nil {
		return 0
	}
	return *n
}
