package artifacts

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeS3 answers a PUT with 200, a GET of a key holding "missing" with
// NoSuchKey, any other GET with its body, and a HEAD with 200.
func fakeS3(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "missing"):
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>nope</Message></Error>`))
		case r.Method == http.MethodGet:
			w.Header().Set("Content-Length", "11")
			_, _ = w.Write([]byte("hello world"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

type observed struct {
	mu   sync.Mutex
	seen []RequestObservation
}

func (o *observed) observe(_ context.Context, r RequestObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen = append(o.seen, r)
}

// TestRequestObserver_ReportsEachS3Call: every S3 call a store makes is
// observed once, with its operation, the key's family, its outcome and its
// bytes, and never its key.
func TestRequestObserver_ReportsEachS3Call(t *testing.T) {
	var got observed
	SetRequestObserver(got.observe)
	t.Cleanup(func() { SetRequestObserver(nil) })

	srv := fakeS3(t)
	client, bucket, err := newS3Client(context.Background(), Config{
		Backend: BackendS3, S3Endpoint: srv.URL, S3Region: "us-east-1", S3Bucket: "b",
		S3AccessKey: "k", S3SecretKey: "s", S3PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket),
		Key: aws.String("metadata/audit/run-1/x.json"), Body: bytes.NewReader([]byte("12345")), ContentLength: aws.Int64(5)}); err != nil {
		t.Fatal(err)
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("objects/aa/run-1/sha")})
	if err != nil {
		t.Fatal(err)
	}
	_ = out.Body.Close()
	if _, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("metadata/runs/missing")}); err == nil {
		t.Fatal("GET of a missing key succeeded")
	}
	if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("ork-acme-tenant/secret-run")}); err != nil {
		t.Fatal(err)
	}

	want := []RequestObservation{
		{Operation: "put", Family: "audit", Outcome: RequestOK, Bytes: 5},
		{Operation: "get", Family: "objects", Outcome: RequestOK, Bytes: 11},
		{Operation: "get", Family: "runs", Outcome: RequestNotFound},
		{Operation: "head", Family: FamilyOther, Outcome: RequestOK},
	}
	got.mu.Lock()
	defer got.mu.Unlock()
	if len(got.seen) != len(want) {
		t.Fatalf("observed %d calls; want %d: %+v", len(got.seen), len(want), got.seen)
	}
	for i, w := range want {
		g := got.seen[i]
		if g.Operation != w.Operation || g.Family != w.Family || g.Outcome != w.Outcome || g.Bytes != w.Bytes {
			t.Errorf("call %d = %+v; want %+v", i, g, w)
		}
		if g.Duration <= 0 {
			t.Errorf("call %d has no duration", i)
		}
	}
}

// TestRequestFamily_IsBounded: a key's family is one of a fixed set, never a
// part of the key a caller chose.
func TestRequestFamily_IsBounded(t *testing.T) {
	for key, want := range map[string]string{
		"metadata/attempts/r/t/1.json": "attempts",
		"metadata/cleanup/gc-clock":    "cleanup",
		"metadata/payments-team/x":     "metadata",
		"metadata":                     "metadata",
		"orkflows/hello/1":             "orkflows",
		"objects/xx/yy":                "objects",
		"tenant-a/run-1/log":           FamilyOther,
		"":                             FamilyOther,
	} {
		if got := requestFamily(key); got != want {
			t.Errorf("requestFamily(%q) = %q; want %q", key, got, want)
		}
	}
}

// TestRequestObserver_PresigningIsNotARequest: minting a download URL sends
// nothing to the store, so it is not observed.
func TestRequestObserver_PresigningIsNotARequest(t *testing.T) {
	var got observed
	SetRequestObserver(got.observe)
	t.Cleanup(func() { SetRequestObserver(nil) })
	store, err := NewS3Store(context.Background(), Config{
		Backend: BackendS3, S3Endpoint: fakeS3(t).URL, S3Region: "us-east-1", S3Bucket: "b",
		S3AccessKey: "k", S3SecretKey: "s", S3PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Presign(context.Background(), "s3://b/orkflows/hello/run-1/build/out.txt/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", time.Minute); err != nil {
		t.Fatal(err)
	}
	if len(got.seen) != 0 {
		t.Fatalf("presigning was observed: %+v", got.seen)
	}
}

// TestRequestObserver_APanickingObserverFailsNoRequest: observing can fail
// nothing; an observer that panics loses its observation, never the call.
func TestRequestObserver_APanickingObserverFailsNoRequest(t *testing.T) {
	SetRequestObserver(func(context.Context, RequestObservation) { panic("boom") })
	t.Cleanup(func() { SetRequestObserver(nil) })
	client, bucket, err := newS3Client(context.Background(), Config{
		Backend: BackendS3, S3Endpoint: fakeS3(t).URL, S3Region: "us-east-1", S3Bucket: "b",
		S3AccessKey: "k", S3SecretKey: "s", S3PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := ObserverPanics()
	if _, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("objects/x")}); err != nil {
		t.Fatalf("a panicking observer failed the request: %v", err)
	}
	if got := ObserverPanics() - before; got != 1 {
		t.Fatalf("observer panics counted = %d; want 1, never a silent loss", got)
	}
}
