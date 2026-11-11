package artifacts

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rollbackKeys are the three keys a refused apply of deploy@2.0.0 and a
// promotion of it touch.
func rollbackKeys() (versionKey, headKey, pointerKey string) {
	versionKey, _ = OrkflowKey(DefaultNamespace, "deploy", "2.0.0")
	headKey, _ = OrkflowHeadKey(DefaultNamespace, "deploy")
	pointerKey, _ = OrkflowCurrentKey(DefaultNamespace, "deploy")
	return versionKey, headKey, pointerKey
}

// promotionDuringRemoval starts a promotion of 2.0.0 as the refused apply's
// rollback is about to remove it, and holds the promotion's read-back of its
// target until release returns: the promotion has read the version (still
// there) and moved the current pointer onto it by then. It returns the
// promotion's result, ready once done is closed, and readingBack, closed once
// the read-back is held.
func promotionDuringRemoval(t *testing.T, d *s3Double, release func()) (done, readingBack <-chan struct{}, result *error) {
	t.Helper()
	ctx := context.Background()
	versionKey, headKey, _ := rollbackKeys()
	promoter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var (
		removing    atomic.Bool
		versionGets atomic.Int32
		promoteErr  error
	)
	finished, atReadBack := make(chan struct{}), make(chan struct{})
	refusedApplyHooks(d, headKey, versionKey, func() {
		removing.Store(true)
		go func() {
			defer close(finished)
			_, promoteErr = promoter.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "2.0.0", NoCurrentExpectation())
		}()
		<-atReadBack
	})
	hooks := d.onRequest
	d.onRequest = func(r *http.Request) {
		// The promotion's second read of the version is its read-back: the
		// first is its validation.
		if removing.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, versionKey) &&
			versionGets.Add(1) == 2 {
			close(atReadBack)
			release()
			return
		}
		hooks(r)
	}
	return finished, atReadBack, &promoteErr
}

// HIGH-A: a promotion's read-back that falls after the rollback's removal and
// before its restore, once the rollback's re-read saw the pointer: the
// promotion finds its target gone while the pointer is still its own, puts
// the version back and succeeds, and the rollback's restore finds it there.
// Before, the promotion moved its pointer back while the rollback restored the
// version: a version stored and listed that no pointer named, both callers
// refused, and the refused apply's account ("a promotion made it current")
// false.
func TestSetCurrentVersion_AReadBackBetweenARollbacksRemovalAndRestorePutsTheVersionBack(t *testing.T) {
	ctx := context.Background()
	versionKey, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	restoring := make(chan struct{})
	var restoreSeen atomic.Bool
	done, readingBack, promoteErr := promotionDuringRemoval(t, d, func() { <-restoring })
	hooks := d.onRequest
	d.onRequest = func(r *http.Request) {
		held := false
		select {
		case <-readingBack:
			held = true
		default:
		}
		if held && r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, versionKey) &&
			restoreSeen.CompareAndSwap(false, true) {
			// The rollback's restore: its re-read saw the pointer. The
			// promotion reads back first.
			close(restoring)
			<-done
			return
		}
		hooks(r)
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	<-done
	if !restoreSeen.Load() {
		t.Fatal("the rollback never restored: the race this test places did not happen")
	}
	if *promoteErr != nil {
		t.Fatalf("the promotion = %v, want it landed: its pointer names a version it put back", *promoteErr)
	}
	if !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "a promotion made it current") {
		t.Fatalf("Put = %v, want the head moved with the version kept for the promotion", err)
	}
	if got := storedPointer(t, d, pointerKey); got != "2.0.0" {
		t.Fatalf("current = %q, want 2.0.0", got)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the current pointer names 2.0.0, which is not stored")
	}
}

// HIGH-A, the other order: the rollback's restore lands before the
// promotion's read-back, which finds the version there.
func TestSetCurrentVersion_AReadBackAfterARollbacksRestoreFindsTheVersion(t *testing.T) {
	ctx := context.Background()
	versionKey, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	returned := make(chan struct{})
	done, _, promoteErr := promotionDuringRemoval(t, d, func() { <-returned })
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	close(returned)
	<-done
	if *promoteErr != nil || !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "a promotion made it current") {
		t.Fatalf("Put = %v, promotion = %v; want the head moved with the version kept, and the promotion landed", err, *promoteErr)
	}
	if got := storedPointer(t, d, pointerKey); got != "2.0.0" {
		t.Fatalf("current = %q, want 2.0.0", got)
	}
	if _, ok := d.object(versionKey); !ok {
		t.Fatal("the current pointer names 2.0.0, which is not stored")
	}
}

// HIGH-B: a Delete of the orkflow and another apply's create of the refused
// apply's version with other content land between the rollback's read of the
// pointers and its read of the version; that apply's head swap lands only
// after the rollback returned. The rollback removes a version only when it
// holds exactly the document this apply wrote, so the other apply's version
// stays, and its head names a stored version.
func TestConfirmedPut_ARollbackRemovesOnlyTheDocumentItWrote(t *testing.T) {
	ctx := context.Background()
	versionKey, headKey, _ := rollbackKeys()
	d, store := orkflowFixture(t)
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	theirs := applyOf("2.0.0", 9)
	theirs.Description = "created afresh after a Delete"
	var (
		moved, raced, swapping atomic.Bool
		deleteErr, theirErr    error
	)
	atSwap, rolledBack, theirsDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	d.onRequest = func(r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && moved.CompareAndSwap(false, true):
			// Another apply moved the head: this apply is refused, and rolls back.
			document, _ := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_005, 0)})
			d.mu.Lock()
			d.objects[headKey] = document
			d.mu.Unlock()
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, versionKey) && moved.Load() &&
			raced.CompareAndSwap(false, true):
			// The rollback reads its version: a Delete and the other apply's
			// create land first.
			deleteErr = other.Delete(ctx, DefaultNamespace, "deploy")
			go func() {
				defer close(theirsDone)
				_, _, theirErr = other.Put(ctx, theirs, NoHeadExpectation())
			}()
			<-atSwap
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && raced.Load() &&
			swapping.CompareAndSwap(false, true):
			// The other apply's head swap waits for the rollback to return.
			close(atSwap)
			<-rolledBack
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), ExpectHead("1.0.0"))
	close(rolledBack)
	<-theirsDone
	if deleteErr != nil || theirErr != nil {
		t.Fatalf("the Delete = %v, the other apply = %v; want both landed", deleteErr, theirErr)
	}
	if !errors.Is(err, ErrHeadMoved) || !strings.Contains(err.Error(), "another document is stored under this version") {
		t.Fatalf("Put = %v, want the head moved and the other apply's version left alone", err)
	}
	if got := headVersion(t, store); got != "2.0.0" {
		t.Fatalf("head = %q, want the other apply's 2.0.0", got)
	}
	stored, err := store.read(ctx, versionKey)
	if err != nil {
		t.Fatalf("the head names 2.0.0, which is not stored: %v", err)
	}
	if stored.GetDescription() != theirs.GetDescription() {
		t.Fatalf("2.0.0 holds %q, want the other apply's document", stored.GetDescription())
	}
}

// MEDIUM-E: a Delete took the current pointer, and has not yet reached the
// versions, while a promotion's swap was in flight: its If-Match meets a
// missing key (404). The promotion is refused as the orkflow deleted; it never
// writes the pointer again from nothing.
func TestSetCurrentVersion_ASwapOnADeletedPointerIsRefused(t *testing.T) {
	ctx := context.Background()
	_, headKey, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	if _, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation()); err != nil {
		t.Fatalf("Put(2.0.0) = %v", err)
	}
	if _, err := store.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "1.0.0", NoCurrentExpectation()); err != nil {
		t.Fatalf("promote 1.0.0 = %v", err)
	}
	var deleted atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, pointerKey) && deleted.CompareAndSwap(false, true) {
			d.mu.Lock()
			delete(d.objects, pointerKey)
			delete(d.objects, headKey)
			d.mu.Unlock()
		}
	}
	_, err := store.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "2.0.0", NoCurrentExpectation())
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "deleted") {
		t.Fatalf("SetCurrentVersion = %v, want refused: the orkflow was deleted", err)
	}
	if _, ok := d.object(pointerKey); ok {
		t.Fatal("the promotion wrote the current pointer again after a Delete took it")
	}
}
