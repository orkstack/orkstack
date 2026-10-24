//go:build integration

// What an apply leaves behind when it LOSES, and what an apply is checked
// against when the head pointer is missing.
//
// Both are properties of the bucket, so both are measured against a real
// MinIO (`make infra-up`):
//
//	go test -tags=integration ./pkg/artifacts/... -run S3OrkflowStore -count=1
package artifacts_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestS3OrkflowStore_AnApplyRefusedInsideTheWriteLeavesNothingStored is the
// window TestS3OrkflowStore_AnApplyWhoseExpectedHeadMovedIsRefused cannot
// reach.
//
// That test hands Put a head that has ALREADY moved, so the refusal happens
// before the version object is written and there was never anything to leave
// behind. The window that matters is the other one: the head is still the one
// the edit named when Put checks it, and moves while Put is writing. An apply
// writes two objects — the version, then the head pointer that commits it —
// and a competing apply landing between them makes the compare-and-swap fail
// with the version object already in the bucket.
//
// Before the repair that residue stayed: the refused version was addressable,
// listed and promotable though the apply that wrote it returned an error, and
// the user's retargeted retry of the SAME edit found its own bytes stored,
// took Put's identical-document no-op and never moved the head. The edit could
// not be published under that version again, ever.
//
// The interleaving is not a hope about timing: the losing store talks to MinIO
// through a proxy that holds its head-pointer PUT until this test has landed
// the competing version.
func TestS3OrkflowStore_AnApplyRefusedInsideTheWriteLeavesNothingStored(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	applied := time.Now().UTC().Truncate(time.Millisecond)
	for i, version := range []string{"1.0.0", "2.0.0"} {
		if _, _, err := store.Put(ctx, testOrkflow(t, name, version,
			applied.Add(time.Duration(i)*time.Second)), artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s): %v", version, err)
		}
	}

	// The editor's replica: same bucket, reached through the barrier.
	barrier := newHeadWriteBarrier(t, cfg)
	editor, err := artifacts.NewS3OrkflowStore(ctx, barrier.config)
	if err != nil {
		t.Fatalf("NewS3OrkflowStore (behind the barrier): %v", err)
	}

	fork := testOrkflow(t, name, "2.1.0", applied.Add(time.Minute))
	fork.Description = "an edit of 2.0.0, refused because the head moved during its write"
	type outcome struct {
		created bool
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		_, created, err := editor.Put(ctx, fork, artifacts.ExpectHead("2.0.0"))
		done <- outcome{created: created, err: err}
	}()

	// The version object is written by now; the commit is held.
	barrier.awaitHeld(t)
	if _, _, err := store.Put(ctx, testOrkflow(t, name, "3.0.0", applied.Add(2*time.Second)),
		artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("the competing apply of 3.0.0: %v", err)
	}
	barrier.release()

	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("the held apply never returned")
	}
	if !errors.Is(got.err, artifacts.ErrHeadMoved) {
		t.Fatalf("the apply that lost the swap = created %v, %v; want artifacts.ErrHeadMoved",
			got.created, got.err)
	}
	var moved *artifacts.HeadMovedError
	if !errors.As(got.err, &moved) || moved.Expected != "2.0.0" || moved.Head != "3.0.0" {
		t.Fatalf("the refusal = %v, want one naming expected 2.0.0 and head 3.0.0", got.err)
	}

	// ---- the refusal stored nothing ----------------------------------------
	if _, err := store.Get(ctx, artifacts.DefaultNamespace, name, "2.1.0"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("the refused version is still addressable: Get(2.1.0) = %v, want ErrNotFound", err)
	}
	versions, err := store.Versions(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if want := "3.0.0,2.0.0,1.0.0"; versionNumbers(versions.All) != want {
		t.Errorf("the catalog lists %s, want %s — a refused apply is not a stored version",
			versionNumbers(versions.All), want)
	}
	if versions.Head != "3.0.0" {
		t.Errorf("head = %q, want 3.0.0 — the apply that committed", versions.Head)
	}

	// ---- and the same edit, retargeted, lands ------------------------------
	// This is what the residue made impossible: the retry's bytes were already
	// in the bucket, so Put answered "already stored" and moved nothing.
	stored, created, err := store.Put(ctx, fork, artifacts.ExpectHead("3.0.0"))
	if err != nil || !created {
		t.Fatalf("the retargeted apply = created %v, %v; want created and no error", created, err)
	}
	if stored.GetVersion() != "2.1.0" {
		t.Fatalf("the retargeted apply stored %q, want 2.1.0", stored.GetVersion())
	}
	versions, err = store.Versions(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("Versions after the retry: %v", err)
	}
	if versions.Head != "2.1.0" {
		t.Errorf("head after the retry = %q, want 2.1.0", versions.Head)
	}
	if want := "2.1.0,3.0.0,2.0.0,1.0.0"; versionNumbers(versions.All) != want {
		t.Errorf("the catalog lists %s, want %s", versionNumbers(versions.All), want)
	}
}

// TestS3OrkflowStore_AStoredCatalogWithNoHeadPointerIsStillEditable is the
// state a Delete that failed after removing the pointers leaves a catalog in:
// the version documents are there and the pointer is not.
//
// Listings answer such a catalog by DERIVING the head from the documents
// (orkflowScan.headOf), so an editor is shown "head 2.0.0" and sends 2.0.0
// back as the head it expects. Put must check against that same derived head:
// reading the absent pointer as a head of "" would refuse the apply and leave
// the catalog readable and not editable, on the version-scoped path and on the
// plain one alike, because both send a head they were shown.
//
// The bucket is put in that state directly: apply the versions, then remove
// the pointer object.
func TestS3OrkflowStore_AStoredCatalogWithNoHeadPointerIsStillEditable(t *testing.T) {
	store, cfg := newS3OrkflowStore(t)
	ctx := context.Background()
	raw := newRawS3Client(t, cfg)
	name := testOrkflowName(t)
	prefix := artifacts.OrkflowPrefix + "/" + artifacts.DefaultNamespace + "/" + name + "/"
	t.Cleanup(func() { deleteByPrefix(t, context.Background(), raw, cfg.S3Bucket, prefix) })

	applied := time.Now().UTC().Truncate(time.Millisecond)
	for i, version := range []string{"1.0.0", "2.0.0"} {
		if _, _, err := store.Put(ctx, testOrkflow(t, name, version,
			applied.Add(time.Duration(i)*time.Second)), artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("Put(%s): %v", version, err)
		}
	}
	headKey, err := artifacts.OrkflowHeadKey(artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("OrkflowHeadKey: %v", err)
	}
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(headKey),
	}); err != nil {
		t.Fatalf("remove the head pointer: %v", err)
	}

	// What the editor is shown.
	versions, err := store.Versions(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("Versions: %v", err)
	}
	if versions.Head != "2.0.0" {
		t.Fatalf("head of a pointer-less catalog = %q, want the derived 2.0.0", versions.Head)
	}

	// A head that is not the derived one is still refused, naming it: the
	// derived head is a real head, not a way of accepting anything.
	stale := testOrkflow(t, name, "9.9.9", applied.Add(time.Minute))
	if _, _, err := store.Put(ctx, stale, artifacts.ExpectHead("1.0.0")); !errors.Is(err, artifacts.ErrHeadMoved) {
		t.Fatalf("Put naming 1.0.0 as the head = %v, want artifacts.ErrHeadMoved", err)
	} else {
		var moved *artifacts.HeadMovedError
		if !errors.As(err, &moved) || moved.Head != "2.0.0" {
			t.Fatalf("the refusal = %v, want one naming the derived head 2.0.0", err)
		}
	}
	if _, err := store.Get(ctx, artifacts.DefaultNamespace, name, "9.9.9"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("the refused apply stored its version anyway: %v", err)
	}

	// And the edit an editor of this catalog actually makes — a fork of the
	// version it was shown, expecting the head it was shown — lands. It is the
	// version-scoped path and the plain one at once: both send a head they
	// were shown, and the derived head is the head they were shown.
	fork := testOrkflow(t, name, "2.1.0", applied.Add(2*time.Minute))
	fork.Description = "an edit of a catalog whose head pointer is missing"
	stored, created, err := store.Put(ctx, fork, artifacts.ExpectHead("2.0.0"))
	if err != nil || !created {
		t.Fatalf("the edit of a pointer-less catalog = created %v, %v; want created", created, err)
	}
	if stamp := stored.GetCreatedAt().AsTime(); !stamp.After(applied.Add(time.Second)) {
		t.Errorf("the fork is stamped %s, which is not after the derived head it extended (%s)",
			stamp, applied.Add(time.Second))
	}

	versions, err = store.Versions(ctx, artifacts.DefaultNamespace, name)
	if err != nil {
		t.Fatalf("Versions after the edit: %v", err)
	}
	if versions.Head != "2.1.0" {
		t.Errorf("head after the edit = %q, want 2.1.0", versions.Head)
	}
	if want := "2.1.0,2.0.0,1.0.0"; versionNumbers(versions.All) != want {
		t.Errorf("the catalog lists %s, want %s", versionNumbers(versions.All), want)
	}
	// The apply that landed committed a pointer, so the catalog is no longer
	// pointer-less: everything after this is the ordinary compare-and-swap.
	if _, err := raw.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(headKey),
	}); err != nil {
		t.Errorf("the first apply on a pointer-less catalog wrote no head pointer: %v", err)
	}
}

// ---- the barrier ---------------------------------------------------------------

// headWriteBarrier is a reverse proxy in front of MinIO that holds the FIRST
// PUT of a head pointer until the test releases it, and passes everything else
// straight through.
//
// It sits at the HTTP layer rather than inside the store because the store has
// no seam to inject one into, and it must not grow one for a test: a store
// with a "pause here" hook is a store whose production path has a hook. The
// proxy leaves the inbound Host header alone, which is what keeps the AWS
// SigV4 signature the store computed valid at MinIO.
type headWriteBarrier struct {
	config  artifacts.Config
	held    chan struct{}
	freed   chan struct{}
	once    sync.Once
	release func()
}

func newHeadWriteBarrier(t *testing.T, cfg artifacts.Config) *headWriteBarrier {
	t.Helper()
	target, err := url.Parse(cfg.S3Endpoint)
	if err != nil {
		t.Fatalf("parse %q: %v", cfg.S3Endpoint, err)
	}
	barrier := &headWriteBarrier{
		held:  make(chan struct{}),
		freed: make(chan struct{}),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/@head.json") {
			barrier.once.Do(func() {
				close(barrier.held)
				<-barrier.freed
			})
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	barrier.config = cfg
	barrier.config.S3Endpoint = server.URL
	barrier.release = sync.OnceFunc(func() { close(barrier.freed) })
	// A test that fails before releasing must not leave the held goroutine
	// blocked on a closed server.
	t.Cleanup(func() { barrier.release() })
	return barrier
}

// awaitHeld blocks until the store under test has reached the commit point:
// its version object is written and its head pointer is not.

func (b *headWriteBarrier) awaitHeld(t *testing.T) {
	t.Helper()
	select {
	case <-b.held:
	case <-time.After(2 * time.Minute):
		t.Fatal("the apply never reached its head-pointer write")
	}
}
