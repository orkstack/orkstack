package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// copyBackend is one backend's staging writer and artifact store over one
// place.
type copyBackend struct {
	staging *EventSourceRecordStore
	store   interface {
		Store
		StagedCopier
		RefPredictor
	}
	// remove takes a staged object away, as a lifecycle rule would.
	remove func(t *testing.T, key string)
	double *s3Double
}

func copyBackends(t *testing.T) map[string]copyBackend {
	t.Helper()
	dir := t.TempDir()
	localStaging, err := NewLocalEventSourceRecordStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	local, err := NewLocalStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	d := newS3Double(t)
	remoteStaging, err := NewEventSourceRecordStore(context.Background(), d.config())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := NewS3Store(context.Background(), d.config())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]copyBackend{
		"local": {staging: localStaging, store: local, remove: func(t *testing.T, key string) {
			t.Helper()
			if err := os.Remove(filepath.Join(dir, filepath.FromSlash(key))); err != nil {
				t.Fatal(err)
			}
		}},
		"s3": {staging: remoteStaging, store: remote, double: d, remove: func(_ *testing.T, key string) {
			d.mu.Lock()
			defer d.mu.Unlock()
			delete(d.objects, key)
		}},
	}
}

func stagedPart(body []byte) StagedPart {
	sum := sha256.Sum256(body)
	return StagedPart{Namespace: "payments", SourceID: recID, Identity: claimMessage, Digest: hex.EncodeToString(sum[:]), Size: int64(len(body))}
}

func stage(t *testing.T, b copyBackend, body []byte) (StagedPart, string) {
	t.Helper()
	p := stagedPart(body)
	key, err := StagingKey(p.Namespace, p.SourceID, p.Identity, p.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.staging.PutStaged(context.Background(), key, body); err != nil {
		t.Fatal(err)
	}
	return p, key
}

var copyProv = Provenance{Orkflow: "settle", RunID: claimRun, TaskID: "_inputs"}

// The destination is the key the predictor gives, the bytes are equal and the
// provenance is the run's; a copy repeated is one object.
func TestCopy_FromStagingToARunsKey(t *testing.T) {
	for name, b := range copyBackends(t) {
		for _, artifact := range []string{"order", "order body.json"} {
			t.Run(name+"/"+artifact, func(t *testing.T) {
				ctx := context.Background()
				body := []byte(`{"id":"o-1"}`)
				part, _ := stage(t, b, body)
				want := b.store.RefFor(copyProv, artifact, part.Digest, part.Size, "application/json")
				for range 2 {
					got, err := b.store.CopyStaged(ctx, part, copyProv, artifact, "application/json")
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("copied to %+v, %v; want %+v", got, err, want)
					}
				}
				r, err := b.store.Get(ctx, want.URI)
				if err != nil {
					t.Fatal(err)
				}
				read, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil || !bytes.Equal(read, body) {
					t.Fatalf("the copy reads %q, %v", read, err)
				}
				st, err := b.store.Stat(ctx, want.URI)
				if err != nil || st.SHA256 != part.Digest || st.SizeBytes != part.Size || st.Name != artifact || st.ContentType != "application/json" {
					t.Fatalf("stat %+v, %v", st, err)
				}
				if got := ProvenanceFromMetadata(st.Metadata); got != copyProv {
					t.Fatalf("provenance %+v; want %+v", got, copyProv)
				}
				refs, err := b.store.List(ctx, artifact)
				if err != nil || len(refs) != 1 {
					t.Fatalf("%d objects named %s (%v); want one", len(refs), artifact, err)
				}
			})
		}
	}
}

// On S3 the store copies: the caller sends no byte of the part, and reads
// only the copy, to check it.
func TestCopy_OnS3IsInsideTheStore(t *testing.T) {
	b := copyBackends(t)["s3"]
	body := bytes.Repeat([]byte("x"), 1<<20)
	part, key := stage(t, b, body)
	dest := strings.TrimPrefix(b.store.RefFor(copyProv, "order", part.Digest, part.Size, "x").URI, "s3://double/")
	var sent int64
	var gets []string
	b.double.onRequest = func(r *http.Request) {
		sent += max(r.ContentLength, 0)
		if r.Method == http.MethodGet {
			gets = append(gets, r.URL.Path)
		}
	}
	if _, err := b.store.CopyStaged(context.Background(), part, copyProv, "order", "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if sent != 0 || slices.ContainsFunc(gets, func(p string) bool { return p != "/double/"+dest }) {
		t.Fatalf("%d bytes were sent and %v read for a part of %d bytes", sent, gets, len(body))
	}
	b.double.mu.Lock()
	h := b.double.headers[dest]
	b.double.mu.Unlock()
	if h.Get("X-Amz-Copy-Source") != "double/"+key || h.Get("X-Amz-Metadata-Directive") != "REPLACE" || h.Get("X-Amz-Copy-Source-If-Match") == "" {
		t.Fatalf("the copy's headers: %v", h)
	}
}

// Whatever the store answers, a copy is referenced only once its own bytes
// are the digest's: a store that ignores the copy's condition (one without
// it, or behind a proxy that strips the header), that stores other bytes, or
// that stores none. Only a part that is itself another's is a mismatch: a
// store that copied a good part badly may not the next time.
func TestCopy_WhatWasCopiedIsVerified(t *testing.T) {
	body, other := []byte("these bytes"), []byte("other bytes")
	swap := func(d *s3Double, key string) {
		d.ignoreCopyCondition = true
		d.onRequest = func(r *http.Request) {
			if r.Header.Get("X-Amz-Copy-Source") != "" {
				d.mu.Lock()
				d.objects[key] = other
				d.mu.Unlock()
			}
		}
	}
	for name, tt := range map[string]struct {
		set  func(d *s3Double, key string)
		want error
	}{
		"nothing goes wrong":                               {func(*s3Double, string) {}, nil},
		"the condition is ignored, nothing changes":        {func(d *s3Double, _ string) { d.ignoreCopyCondition = true }, nil},
		"the condition is ignored and the part changes":    {swap, ErrStagedMismatch},
		"the staged part was never the digest's":           {func(d *s3Double, key string) { d.objects[key] = other }, ErrStagedMismatch},
		"other bytes of that length are stored":            {func(d *s3Double, _ string) { d.copyAs = other }, errCopyWrong},
		"fewer bytes are stored":                           {func(d *s3Double, _ string) { d.copyAs = body[:3] }, errCopyWrong},
		"more bytes are stored, the part's at their start": {func(d *s3Double, _ string) { d.copyAs = append(slices.Clone(body), '!') }, errCopyWrong},
		"no bytes are stored":                              {func(d *s3Double, _ string) { d.copyNothing = true }, errCopyNotStored},
	} {
		t.Run(name, func(t *testing.T) {
			b := copyBackends(t)["s3"]
			part, key := stage(t, b, body)
			tt.set(b.double, key)
			got, err := b.store.CopyStaged(context.Background(), part, copyProv, "order", "text/plain")
			if !errors.Is(err, tt.want) || (tt.want == nil) != (err == nil) {
				t.Fatalf("%v; want %v", err, tt.want)
			}
			if errors.Is(tt.want, errCopyWrong) && errors.Is(err, ErrStagedMismatch) {
				t.Fatalf("a good part was called a mismatch: %v", err)
			}
			if err != nil && got.URI != "" {
				t.Fatalf("a copy that failed was referenced: %+v", got)
			}
		})
	}
}

// A copy that is there and is the digest's is not made again, so a staged
// part that changes after a run references its copy never replaces it.
func TestCopy_AGoodCopyIsNeverReplaced(t *testing.T) {
	b := copyBackends(t)["s3"]
	ctx := context.Background()
	body := []byte("these bytes")
	part, key := stage(t, b, body)
	ref, err := b.store.CopyStaged(ctx, part, copyProv, "order", "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	b.double.mu.Lock()
	b.double.ignoreCopyCondition = true
	b.double.objects[key] = []byte("other bytes")
	b.double.mu.Unlock()
	again, err := b.store.CopyStaged(ctx, part, copyProv, "order", "text/plain")
	if err != nil || !reflect.DeepEqual(again, ref) || b.double.copies != 1 {
		t.Fatalf("%+v, %v, %d copies; want the one that is there", again, err, b.double.copies)
	}
	stored, _ := b.double.object(strings.TrimPrefix(ref.URI, "s3://double/"))
	if !bytes.Equal(stored, body) {
		t.Fatalf("the copy now holds %q", stored)
	}
}

func TestCopy_StagedMissingOrWrong(t *testing.T) {
	for name, b := range copyBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			part, key := stage(t, b, []byte("here"))
			wrong := part
			wrong.Size++
			if _, err := b.store.CopyStaged(ctx, wrong, copyProv, "order", "text/plain"); !errors.Is(err, ErrStagedMismatch) {
				t.Errorf("another size than the Orker gave: %v", err)
			}
			b.remove(t, key)
			if _, err := b.store.CopyStaged(ctx, part, copyProv, "order", "text/plain"); !errors.Is(err, ErrStagedMissing) {
				t.Errorf("a staged part that is not there: %v", err)
			}
			if _, err := b.store.Stat(ctx, b.store.RefFor(copyProv, "order", part.Digest, part.Size, "text/plain").URI); !errors.Is(err, ErrNotFound) {
				t.Errorf("a refused copy left an object: %v", err)
			}
			for what, edit := range map[string]func(*StagedPart){
				"namespace": func(p *StagedPart) { p.Namespace = "../x" },
				"source":    func(p *StagedPart) { p.SourceID = "evs-1" },
				"identity":  func(p *StagedPart) { p.Identity = "" },
				"digest":    func(p *StagedPart) { p.Digest = "nope" },
				"size":      func(p *StagedPart) { p.Size = -1 },
				"length":    func(p *StagedPart) { p.Size = maxStagedPart + 1 },
			} {
				bad := part
				edit(&bad)
				if _, err := b.store.CopyStaged(ctx, bad, copyProv, "order", "text/plain"); err == nil || errors.Is(err, ErrStagedMissing) || errors.Is(err, ErrStagedMismatch) {
					t.Errorf("a bad %s: %v; want it refused as an argument", what, err)
				}
			}
			if _, err := b.store.CopyStaged(ctx, part, Provenance{Orkflow: "settle", TaskID: "_inputs"}, "order", "text/plain"); err == nil {
				t.Error("a copy to no run was made")
			}
		})
	}
}

// What a refused copy means: only a part that is not there is staged again,
// and only its own size and digest make it a mismatch. A conflict, a part
// staged again as it was copied, and a failure the store reports after it
// began answering are failures another try may not meet.
func TestCopy_ARefusedCopyIsClassified(t *testing.T) {
	for _, tt := range []struct {
		name   string
		answer doubleAnswer
		want   error // nil: none of the two typed errors
	}{
		{"a conflicting operation", doubleAnswer{http.StatusConflict, "OperationAborted"}, nil},
		{"a conditional conflict", doubleAnswer{http.StatusConflict, "ConditionalRequestConflict"}, nil},
		{"staged again as it was copied", doubleAnswer{http.StatusPreconditionFailed, "PreconditionFailed"}, nil},
		{"refused", doubleAnswer{http.StatusForbidden, "AccessDenied"}, nil},
		{"no bucket", doubleAnswer{http.StatusNotFound, "NoSuchBucket"}, nil},
		{"an error in an answer that began", doubleAnswer{http.StatusOK, "AccessDenied"}, nil},
		{"gone", doubleAnswer{http.StatusNotFound, "NoSuchKey"}, ErrStagedMissing},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := copyBackends(t)["s3"]
			part, _ := stage(t, b, []byte("here"))
			b.double.copyAnswers = slices.Repeat([]doubleAnswer{tt.answer}, 3)
			_, err := b.store.CopyStaged(context.Background(), part, copyProv, "order", "text/plain")
			if err == nil || errors.Is(err, ErrStagedMissing) != (tt.want == ErrStagedMissing) || errors.Is(err, ErrStagedMismatch) {
				t.Fatalf("%v; want %v", err, tt.want)
			}
			b.double.copyAnswers = nil
			if _, err := b.store.CopyStaged(context.Background(), part, copyProv, "order", "text/plain"); err != nil {
				t.Fatalf("the next try: %v", err)
			}
		})
	}
}

// A store whose bucket is not there has not lost a staged part: staging it
// again would not help.
func TestCopy_NoBucketIsNotAMissingPart(t *testing.T) {
	d := newS3Double(t)
	cfg := d.config()
	cfg.S3Bucket = "nowhere"
	st, err := NewS3Store(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CopyStaged(context.Background(), stagedPart([]byte("x")), copyProv, "order", "text/plain"); err == nil || errors.Is(err, ErrStagedMissing) {
		t.Fatalf("no bucket: %v", err)
	}
}

// A staged object that goes, or changes, between the check and the copy.
func TestCopy_StagedChangesUnderTheCopy(t *testing.T) {
	for name, tt := range map[string]struct {
		change func(d *s3Double, key string)
		want   error
	}{
		"goes": {func(d *s3Double, key string) { delete(d.objects, key) }, ErrStagedMissing},
		// The store refuses the copy's condition: the next try checks again.
		"changes": {func(d *s3Double, key string) { d.objects[key] = []byte("else") }, nil},
	} {
		t.Run(name, func(t *testing.T) {
			b := copyBackends(t)["s3"]
			part, key := stage(t, b, []byte("here"))
			b.double.onRequest = func(r *http.Request) {
				if r.Header.Get("X-Amz-Copy-Source") != "" {
					b.double.mu.Lock()
					tt.change(b.double, key)
					b.double.mu.Unlock()
				}
			}
			_, err := b.store.CopyStaged(context.Background(), part, copyProv, "order", "text/plain")
			if err == nil || errors.Is(err, ErrStagedMissing) != (tt.want == ErrStagedMissing) || errors.Is(err, ErrStagedMismatch) {
				t.Fatalf("%v; want %v", err, tt.want)
			}
		})
	}
}

// The engine takes away every staged part of one message, and nothing of
// another's.
func TestRemoveStaged_OneMessageOnly(t *testing.T) {
	for name, b := range copyBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			part, _ := stage(t, b, []byte("body"))
			stage(t, b, []byte("metadata"))
			other := stagedPart([]byte("body"))
			other.Identity = claimMessage + "0"
			otherKey, err := StagingKey(other.Namespace, other.SourceID, other.Identity, other.Digest)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.staging.PutStaged(ctx, otherKey, []byte("body")); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := b.staging.RemoveStaged(ctx, part.Namespace, part.SourceID, part.Identity); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := b.store.CopyStaged(ctx, part, copyProv, "order", "text/plain"); !errors.Is(err, ErrStagedMissing) {
				t.Errorf("the message's part is still there: %v", err)
			}
			if _, err := b.store.CopyStaged(ctx, other, copyProv, "order", "text/plain"); err != nil {
				t.Errorf("another message's part went with it: %v", err)
			}
			if err := b.staging.RemoveStaged(ctx, part.Namespace, part.SourceID, ""); err == nil {
				t.Error("a removal of no message was made")
			}
		})
	}
}

// TestStoreRefuses: what a store would answer again is told from what may
// pass.
func TestStoreRefuses(t *testing.T) {
	status := func(code int) error {
		return fmt.Errorf("get: %w", &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}}, Err: errors.New("said")})
	}
	for code, want := range map[int]bool{400: true, 401: true, 403: true, 404: true, 405: true, 501: true,
		408: false, 409: false, 412: false, 429: false, 500: false, 503: false, 504: false} {
		if got := StoreRefuses(status(code)); got != want {
			t.Errorf("%d: %v; want %v", code, got, want)
		}
	}
	for _, err := range []error{nil, errors.New("connection refused"), context.DeadlineExceeded} {
		if StoreRefuses(err) {
			t.Errorf("%v is refused", err)
		}
	}
	if !StoreRefuses(fmt.Errorf("open: %w", os.ErrPermission)) {
		t.Error("a local store that may not be read")
	}
}
