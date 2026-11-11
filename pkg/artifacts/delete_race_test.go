package artifacts

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// nothingLeft fails the test when any object of the orkflow deploy is still
// stored: a Delete that returned nil leaves no pointer and no version.
func nothingLeft(t *testing.T, d *s3Double) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	for key := range d.objects {
		if strings.HasPrefix(key, OrkflowPrefix+"/"+DefaultNamespace+"/deploy/") {
			t.Errorf("%s outlived a Delete that returned nil", key)
		}
	}
}

// deleteHeldAtTheVersions runs a Delete of deploy and holds it once it has
// taken both pointers, before its first version: during runs then, and the
// Delete goes on when it returns. It returns the Delete's error.
func deleteHeldAtTheVersions(t *testing.T, d *s3Double, store *S3OrkflowStore, during func()) error {
	t.Helper()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	var held atomic.Bool
	hooks := d.onRequest
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, v1Key) && held.CompareAndSwap(false, true) {
			during()
		}
		if hooks != nil {
			hooks(r)
		}
	}
	err := store.Delete(context.Background(), DefaultNamespace, "deploy")
	if !held.Load() {
		t.Fatal("the Delete never reached its versions: the race this test places did not happen")
	}
	return err
}

// HIGH-1 (c): a promotion reads its target and swaps the current pointer
// while a Delete is between its pointers and its versions, and reads its
// target back only after the Delete returned. The promotion loses to the
// Delete: it is refused as the orkflow deleted, puts nothing back, and no
// pointer and no version remain. Before, it found its own pointer and its
// target gone, put the version back and landed: the orkflow lived again after
// a Delete that reported success.
func TestSetCurrentVersion_APromotionReadingBackAfterADeleteLosesToIt(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	d, store := orkflowFixture(t)
	promoter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var (
		promoteErr error
		v1Gets     atomic.Int32
		promoting  atomic.Bool
	)
	readingBack, deleted, promoted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	err := deleteHeldAtTheVersions(t, d, store, func() {
		promoting.Store(true)
		hooks := d.onRequest
		d.onRequest = func(r *http.Request) {
			// The promotion's second read of 1.0.0 is its read-back.
			if promoting.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, v1Key) && v1Gets.Add(1) == 2 {
				close(readingBack)
				<-deleted
				return
			}
			hooks(r)
		}
		go func() {
			defer close(promoted)
			_, promoteErr = promoter.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "1.0.0", NoCurrentExpectation())
		}()
		<-readingBack
	})
	close(deleted)
	<-promoted
	if err != nil {
		t.Fatalf("Delete = %v", err)
	}
	if !errors.Is(promoteErr, ErrNotFound) || !strings.Contains(promoteErr.Error(), "deleted") {
		t.Fatalf("the promotion = %v, want refused: the orkflow was deleted", promoteErr)
	}
	nothingLeft(t, d)
}

// HIGH-1 (a): the promotion validates its target inside the Delete and swaps
// the current pointer only after the Delete returned, onto no pointer at all.
// Its read-back finds the target gone and no head, which no rollback leaves:
// it removes its own pointer and is refused as the orkflow deleted.
func TestSetCurrentVersion_APromotionSwappingAfterADeleteLosesToIt(t *testing.T) {
	ctx := context.Background()
	_, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	promoter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var (
		promoteErr error
		promoting  atomic.Bool
	)
	swapping, deleted, promoted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	err := deleteHeldAtTheVersions(t, d, store, func() {
		promoting.Store(true)
		hooks := d.onRequest
		d.onRequest = func(r *http.Request) {
			if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, pointerKey) &&
				promoting.CompareAndSwap(true, false) {
				close(swapping)
				<-deleted
				return
			}
			hooks(r)
		}
		go func() {
			defer close(promoted)
			_, promoteErr = promoter.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "1.0.0", NoCurrentExpectation())
		}()
		<-swapping
	})
	close(deleted)
	<-promoted
	if err != nil {
		t.Fatalf("Delete = %v", err)
	}
	// LOW-C: a Delete that stopped part way reads the same, and is said so.
	if !errors.Is(promoteErr, ErrNotFound) || !strings.Contains(promoteErr.Error(), "deleted") ||
		!strings.Contains(promoteErr.Error(), "stopped part way") {
		t.Fatalf("the promotion = %v, want refused: the orkflow was deleted, or a Delete stopped part way", promoteErr)
	}
	nothingLeft(t, d)
}

// HIGH-1 (b): a promotion that runs wholly inside a Delete, between its
// pointers and its versions, finds its target there and lands; the Delete
// then takes the version and, last, both pointers again, so no pointer
// written during its version phase outlives it.
func TestDelete_APointerWrittenDuringItsVersionsDoesNotOutliveIt(t *testing.T) {
	ctx := context.Background()
	d, store := orkflowFixture(t)
	promoter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var promoteErr error
	err := deleteHeldAtTheVersions(t, d, store, func() {
		_, promoteErr = promoter.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "1.0.0", NoCurrentExpectation())
	})
	if err != nil || promoteErr != nil {
		t.Fatalf("Delete = %v, the promotion inside it = %v; want both landed", err, promoteErr)
	}
	nothingLeft(t, d)
}

// MEDIUM-2: a Delete runs whole while an unstated apply's head swap is in
// flight. The swap meets a missing head (404), the apply goes round again,
// finds nothing stored, and swaps a head in from nothing onto its own version
// — which the Delete took. The apply reads its version back after a swap onto
// no head, finds it gone, removes its own head and is refused as the orkflow
// deleted; its rollback has nothing left to remove. Before, it reported
// success and left a head naming a version that is not stored.
func TestConfirmedPut_AnApplyWhoseHeadADeleteTookLosesToIt(t *testing.T) {
	ctx := context.Background()
	_, headKey, _ := rollbackKeys()
	d, store := orkflowFixture(t)
	deleter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var (
		swapped   atomic.Bool
		deleteErr error
	)
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && r.Header.Get("If-Match") != "" &&
			swapped.CompareAndSwap(false, true) {
			deleteErr = deleter.Delete(ctx, DefaultNamespace, "deploy")
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation())
	if !swapped.Load() || deleteErr != nil {
		t.Fatalf("the Delete inside the swap ran %v, = %v", swapped.Load(), deleteErr)
	}
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "deleted") {
		t.Fatalf("Put = %v, want refused: the orkflow was deleted as it was applied", err)
	}
	nothingLeft(t, d)
}

// MEDIUM-3: a promotion's read-back finds another document under its target
// — a refused apply's version removed, and the version created again with
// other content — and says so. The pointer it moved names a document its
// interface check never compared, which the API promises never to leave: while
// the pointer is still this promotion's own write, it is moved back to what it
// named before (cleared when it named nothing), under that write's etag. A
// pointer another promotion has moved since is left alone.
func TestConfirmPromoted_MovesThePointerBackOffAnotherDocument(t *testing.T) {
	ctx := context.Background()
	versionKey, _, pointerKey := rollbackKeys()
	target := applyOf("2.0.0", 2)
	raw, _ := marshalOrkflow(target)
	theirs := applyOf("2.0.0", 9)
	theirs.Description = "created afresh"
	other, _ := marshalOrkflow(theirs)
	mine := currentPointer{Version: "2.0.0", UpdatedAt: target.GetCreatedAt().AsTime()}
	minePointer, _ := marshalCurrentPointer(mine)
	moved := pointerDocument(t, "3.0.0", target.GetCreatedAt().AsTime().Add(time.Second))
	for _, tc := range []struct {
		name     string
		previous string
		pointer  []byte // what the current pointer holds at the read-back
		want     string // the version the pointer names afterwards ("" for none)
		says     string
	}{
		{"moved back to the version before", "1.0.0", minePointer, "1.0.0", "the current pointer was moved back to \"1.0.0\""},
		{"cleared when it named nothing before", "", minePointer, "", "the current pointer was cleared"},
		{"left alone once another promotion moved it", "1.0.0", moved, "3.0.0", "has moved since"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, store := orkflowFixture(t)
			d.objects[versionKey] = other
			d.objects[pointerKey] = tc.pointer
			previous := currentPointer{Version: tc.previous}
			var previousRaw []byte
			if tc.previous != "" {
				previousRaw, _ = marshalCurrentPointer(previous)
			}
			err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", versionKey, previous, previousRaw,
				mine, target, raw)
			if !errors.Is(err, ErrVersionExists) || !strings.Contains(err.Error(), "another document is stored under this version now") ||
				!strings.Contains(err.Error(), tc.says) {
				t.Fatalf("confirmPromoted = %v, want refused: another document is stored, and %s", err, tc.says)
			}
			got, err := store.CurrentVersion(ctx, DefaultNamespace, "deploy")
			if err != nil || got != tc.want {
				t.Fatalf("current = %q, %v; want %q", got, err, tc.want)
			}
			if stored, _ := d.object(versionKey); string(stored) != string(other) {
				t.Fatal("the other document was changed")
			}
		})
	}
}

// MEDIUM-A: an apply re-creates the orkflow, with a version the Delete never
// scanned, and a promotion makes it current, both while the Delete is taking
// its versions. The Delete's last pass over the pointers removes only a
// pointer naming a version that is not stored: these name one that is, and
// they stay. A Delete that erased them would leave a stored version no
// pointer names, resolved through the derived head instead of the promotion.
func TestDelete_LeavesThePointersOfAVersionItNeverScanned(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	v3Key, _ := OrkflowKey(DefaultNamespace, "deploy", "3.0.0")
	d, store := orkflowFixture(t)
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var applyErr, promoteErr error
	err := deleteHeldAtTheVersions(t, d, store, func() {
		_, _, applyErr = other.Put(ctx, applyOf("3.0.0", 3), NoHeadExpectation())
		_, promoteErr = other.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "3.0.0", NoCurrentExpectation())
	})
	if err != nil || applyErr != nil || promoteErr != nil {
		t.Fatalf("Delete = %v, the apply inside it = %v, the promotion = %v; want all landed", err, applyErr, promoteErr)
	}
	if got := headVersion(t, store); got != "3.0.0" {
		t.Fatalf("head = %q, want 3.0.0: the Delete erased the head of a version it never scanned", got)
	}
	if got, err := store.CurrentVersion(ctx, DefaultNamespace, "deploy"); err != nil || got != "3.0.0" {
		t.Fatalf("current = %q, %v; want 3.0.0: the Delete erased the pointer of a version it never scanned", got, err)
	}
	if _, ok := d.object(v3Key); !ok {
		t.Fatal("3.0.0 is not stored")
	}
	if _, ok := d.object(v1Key); ok {
		t.Fatal("the Delete left 1.0.0, which it scanned")
	}
}

// LOW-B: a promotion that moves the pointer back writes the previous
// pointer's exact bytes, so the promotion that wrote them still knows its own
// write: when a rollback then removes that promotion's target, its read-back
// finds its own pointer and puts the target back, rather than leaving the
// pointer on a missing version.
func TestConfirmPromoted_AMoveBackKeepsThePreviousPromotionsOwnPointer(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	v2Key, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	earlier := currentPointer{Version: "1.0.0", UpdatedAt: time.Unix(1_700_000_010, 0).UTC()}
	earlierRaw, _ := marshalCurrentPointer(earlier)
	mine := currentPointer{Version: "2.0.0", UpdatedAt: time.Unix(1_700_000_011, 0).UTC()}
	mineRaw, _ := marshalCurrentPointer(mine)
	d.objects[pointerKey] = mineRaw
	target := applyOf("2.0.0", 2)
	raw, _ := marshalOrkflow(target)
	theirs := applyOf("2.0.0", 9)
	theirs.Description = "created afresh"
	d.objects[v2Key], _ = marshalOrkflow(theirs)
	if err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", v2Key, earlier, earlierRaw, mine, target, raw); !errors.Is(err, ErrVersionExists) {
		t.Fatalf("confirmPromoted = %v, want refused: another document under 2.0.0", err)
	}
	if got, _ := d.object(pointerKey); string(got) != string(earlierRaw) {
		t.Fatalf("the pointer was moved back to %s, not to the earlier promotion's own write %s", got, earlierRaw)
	}
	// The earlier promotion reads its target back after a rollback removed it.
	v1Raw, _ := d.object(v1Key)
	v1, err := unmarshalOrkflow(v1Raw)
	if err != nil {
		t.Fatalf("decode 1.0.0: %v", err)
	}
	d.mu.Lock()
	delete(d.objects, v1Key)
	d.mu.Unlock()
	if err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", v1Key, currentPointer{}, nil, earlier, v1, v1Raw); err != nil {
		t.Fatalf("the earlier promotion's read-back = %v, want its target put back under its own pointer", err)
	}
	if _, ok := d.object(v1Key); !ok {
		t.Fatal("the current pointer names 1.0.0, which is not stored")
	}
}

// LOW-C: a resend of the move-back's delete that meets the pointer already
// gone — its first send landed — is a cleared pointer, and says so.
func TestConfirmPromoted_AClearWhoseResendMeetsNothingSaysCleared(t *testing.T) {
	ctx := context.Background()
	v2Key, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	d.missingIfMatchDelete404 = true
	mine := currentPointer{Version: "2.0.0", UpdatedAt: time.Unix(1_700_000_011, 0).UTC()}
	d.objects[pointerKey], _ = marshalCurrentPointer(mine)
	target := applyOf("2.0.0", 2)
	raw, _ := marshalOrkflow(target)
	theirs := applyOf("2.0.0", 9)
	theirs.Description = "created afresh"
	d.objects[v2Key], _ = marshalOrkflow(theirs)
	var landed atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, pointerKey) && landed.CompareAndSwap(false, true) {
			d.mu.Lock()
			delete(d.objects, pointerKey) // the first send landed; this one meets nothing
			d.mu.Unlock()
		}
	}
	err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", v2Key, currentPointer{}, nil, mine, target, raw)
	if !errors.Is(err, ErrVersionExists) || !strings.Contains(err.Error(), "cleared") {
		t.Fatalf("confirmPromoted = %v, want refused, the current pointer cleared", err)
	}
}

// LOW-D: a promotion inside a Delete whose own pointer cannot be removed
// leaves that pointer for a re-created orkflow to inherit, and says what
// clears it.
func TestConfirmPromoted_APointerItCannotRemoveSaysWhatClearsIt(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	_, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	v1Raw, _ := d.object(v1Key)
	v1, _ := unmarshalOrkflow(v1Raw)
	mine := currentPointer{Version: "1.0.0", UpdatedAt: time.Unix(1_700_000_011, 0).UTC()}
	d.mu.Lock()
	d.objects[pointerKey], _ = marshalCurrentPointer(mine)
	delete(d.objects, v1Key) // a Delete took the version and the head
	delete(d.objects, headKey)
	d.deleteConditionAnswer = http.StatusInternalServerError
	d.mu.Unlock()
	err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", v1Key, currentPointer{}, nil, mine, v1, v1Raw)
	if err == nil || !strings.Contains(err.Error(), "promote again or delete the orkflow") {
		t.Fatalf("confirmPromoted = %v, want an error saying what clears the pointer it left", err)
	}
}

// LOW-C: an apply the Delete beat says only that: its version is gone, so
// its rollback has nothing to say, and never that the version "stays
// stored" — even when the pointers cannot be read afterwards.
func TestConfirmedPut_AnApplyADeleteBeatSaysOnlyThat(t *testing.T) {
	ctx := context.Background()
	_, headKey, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	deleter := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var swapped, deleted atomic.Bool
	d.onRequest = func(r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && r.Header.Get("If-Match") != "" &&
			swapped.CompareAndSwap(false, true):
			if err := deleter.Delete(ctx, DefaultNamespace, "deploy"); err != nil {
				t.Errorf("Delete = %v", err)
			}
			deleted.Store(true)
		case deleted.Load() && r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, headKey):
			// The apply removes its own head: the pointers are unreadable after.
			d.mu.Lock()
			d.failGets[pointerKey] = 3
			d.failGets[headKey] = 3
			d.mu.Unlock()
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation())
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "deleted") || strings.Contains(err.Error(), "stays stored") {
		t.Fatalf("Put = %v, want only that the orkflow was deleted as it was applied", err)
	}
}

// An unstated apply whose head swap meets a head a Delete took (404) lost to
// the Delete: it is refused as deleted and rolls back its own version. Before,
// it went round again, derived the head from its own uncommitted version, and
// reported success with no head written — for a version the Delete then
// removed.
func TestConfirmedPut_AnUnstatedApplyWhoseHeadADeleteTookIsRefused(t *testing.T) {
	ctx := context.Background()
	v2Key, headKey, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	var took atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && r.Header.Get("If-Match") != "" &&
			took.CompareAndSwap(false, true) {
			// A Delete's first pass: both pointers, and not yet the versions.
			d.mu.Lock()
			delete(d.objects, headKey)
			delete(d.objects, pointerKey)
			d.mu.Unlock()
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation())
	if !took.Load() {
		t.Fatal("the apply never swapped the head: the race this test places did not happen")
	}
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "deleted") {
		t.Fatalf("Put = %v, want refused: the orkflow was deleted as it was applied", err)
	}
	if _, ok := d.object(v2Key); ok {
		t.Fatal("the refused apply left 2.0.0 stored")
	}
	if _, ok := d.object(headKey); ok {
		t.Fatal("the refused apply wrote a head")
	}
}

// LOW-1: a move back onto a version that is no longer stored would leave the
// pointer on nothing; the pointer is cleared instead, and the refusal says so.
func TestConfirmPromoted_AMoveBackOntoAGoneVersionClears(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	v2Key, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	earlier := currentPointer{Version: "1.0.0", UpdatedAt: time.Unix(1_700_000_010, 0).UTC()}
	earlierRaw, _ := marshalCurrentPointer(earlier)
	mine := currentPointer{Version: "2.0.0", UpdatedAt: time.Unix(1_700_000_011, 0).UTC()}
	d.objects[pointerKey], _ = marshalCurrentPointer(mine)
	delete(d.objects, v1Key) // the version it would move back to is gone
	target := applyOf("2.0.0", 2)
	raw, _ := marshalOrkflow(target)
	theirs := applyOf("2.0.0", 9)
	theirs.Description = "created afresh"
	d.objects[v2Key], _ = marshalOrkflow(theirs)
	err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", v2Key, earlier, earlierRaw, mine, target, raw)
	if !errors.Is(err, ErrVersionExists) || !strings.Contains(err.Error(), "cleared") || !strings.Contains(err.Error(), `"1.0.0"`) {
		t.Fatalf("confirmPromoted = %v, want refused, the pointer cleared because 1.0.0 is gone", err)
	}
	if _, ok := d.object(pointerKey); ok {
		t.Fatal("the pointer was moved back onto a version that is not stored")
	}
}

// LOW-3: a refused clear whose pointer cannot be read again says so, never
// that the pointer moved on.
func TestConfirmPromoted_AClearItCannotReadAgainSaysSo(t *testing.T) {
	ctx := context.Background()
	v2Key, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	d.missingIfMatchDelete404 = true
	mine := currentPointer{Version: "2.0.0", UpdatedAt: time.Unix(1_700_000_011, 0).UTC()}
	d.objects[pointerKey], _ = marshalCurrentPointer(mine)
	target := applyOf("2.0.0", 2)
	raw, _ := marshalOrkflow(target)
	theirs := applyOf("2.0.0", 9)
	theirs.Description = "created afresh"
	d.objects[v2Key], _ = marshalOrkflow(theirs)
	var landed atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, pointerKey) && landed.CompareAndSwap(false, true) {
			d.mu.Lock()
			delete(d.objects, pointerKey)
			d.failGets[pointerKey] = 3
			d.mu.Unlock()
		}
	}
	err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", v2Key, currentPointer{}, nil, mine, target, raw)
	if err == nil || !strings.Contains(err.Error(), "could not be read again") || strings.Contains(err.Error(), "moved since") {
		t.Fatalf("confirmPromoted = %v, want the unread pointer said, not a pointer that moved", err)
	}
}

// LOW-2: a promotion whose pointer a Delete removed, while another apply has
// re-created the orkflow, is told that — its version is gone, and deleting
// the orkflow again would delete the re-created one.
func TestSetCurrentVersion_APromotionAfterARecreatedOrkflowSaysSo(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	d, store := orkflowFixture(t)
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var (
		promoteErr, applyErr error
		v1Gets               atomic.Int32
		promoting            atomic.Bool
	)
	readingBack, deleted, promoted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	err := deleteHeldAtTheVersions(t, d, store, func() {
		_, _, applyErr = other.Put(ctx, applyOf("3.0.0", 3), NoHeadExpectation())
		promoting.Store(true)
		hooks := d.onRequest
		d.onRequest = func(r *http.Request) {
			if promoting.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, v1Key) && v1Gets.Add(1) == 2 {
				close(readingBack)
				<-deleted
				return
			}
			hooks(r)
		}
		go func() {
			defer close(promoted)
			_, promoteErr = other.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "1.0.0", NoCurrentExpectation())
		}()
		<-readingBack
	})
	close(deleted)
	<-promoted
	if err != nil || applyErr != nil {
		t.Fatalf("Delete = %v, the apply that re-created the orkflow = %v", err, applyErr)
	}
	// LOW-2: a pointer that vanished between a Delete's two pointer deletes
	// reads the same, and the refusal says so.
	if !errors.Is(promoteErr, ErrNotFound) || !strings.Contains(promoteErr.Error(), "re-created") ||
		!strings.Contains(promoteErr.Error(), "or a Delete is still removing it") ||
		strings.Contains(promoteErr.Error(), "deleting it again") {
		t.Fatalf("the promotion = %v, want refused: the orkflow was re-created, or a Delete is still removing it", promoteErr)
	}
	if _, ok := d.object(v1Key); ok {
		t.Fatal("1.0.0, which the Delete removed, is stored again")
	}
}

// LOW-2: an apply whose head swapped in from nothing finds its version gone
// and another apply's head in its place: the orkflow was re-created, and it
// is told so, without advice to delete it again.
func TestConfirmHeadFromNothing_AReCreatedOrkflowSaysSo(t *testing.T) {
	ctx := context.Background()
	v2Key, headKey, _ := rollbackKeys()
	d, store := orkflowFixture(t)
	theirs, _ := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_009, 0)})
	d.objects[headKey] = theirs
	err := store.confirmHeadFromNothing(ctx, DefaultNamespace, "deploy", v2Key, applyOf("2.0.0", 2), time.Unix(1_700_000_002, 0), true)
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "re-created") || strings.Contains(err.Error(), "deleting it again") {
		t.Fatalf("confirmHeadFromNothing = %v, want refused: another apply re-created the orkflow", err)
	}
	if got, _ := d.object(headKey); string(got) != string(theirs) {
		t.Fatal("the other apply's head was changed")
	}
}

// MEDIUM-1: a Delete is taking the versions; another apply re-creates the
// orkflow with 3.0.0, its head swapped in from nothing; a promotion of 1.0.0 —
// a version of the orkflow being deleted — reads it, swaps the current
// pointer, and reads it back only after the Delete removed it and before the
// Delete's last pass. Its own pointer and a head are both there, but 1.0.0 was
// created before the head's incarnation began (since): it belongs to the
// deleted orkflow. The promotion removes its own pointer and is refused, and
// 1.0.0 stays deleted. Before, it put 1.0.0 back and stood.
func TestSetCurrentVersion_APromotionOfAVersionFromBeforeTheIncarnationIsRefused(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	_, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var (
		applyErr, promoteErr error
		v1Gets               atomic.Int32
		promoting, removed   atomic.Bool
		heldPass             atomic.Bool
	)
	readingBack, v1Gone, promoted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	err := deleteHeldAtTheVersions(t, d, store, func() {
		_, _, applyErr = other.Put(ctx, applyOf("3.0.0", 3), NoHeadExpectation())
		promoting.Store(true)
		hooks := d.onRequest
		d.onRequest = func(r *http.Request) {
			switch {
			case promoting.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, v1Key) && v1Gets.Add(1) == 2:
				// The promotion's read-back waits until the Delete removed 1.0.0.
				close(readingBack)
				<-v1Gone
				return
			case removed.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, pointerKey) &&
				heldPass.CompareAndSwap(false, true):
				// The Delete's last pass: 1.0.0 is gone; the promotion reads
				// back now, and the pass goes on once it is done.
				close(v1Gone)
				<-promoted
				return
			}
			hooks(r)
		}
		go func() {
			defer close(promoted)
			_, promoteErr = other.SetCurrentVersion(ctx, DefaultNamespace, "deploy", "1.0.0", NoCurrentExpectation())
		}()
		<-readingBack
		// The Delete's held removal of 1.0.0 goes on as this returns; the
		// promotion is held, so the Delete's next read of the current pointer
		// is its last pass, after the removal.
		removed.Store(true)
	})
	<-promoted
	if err != nil || applyErr != nil {
		t.Fatalf("Delete = %v, the re-creating apply = %v", err, applyErr)
	}
	if !heldPass.Load() {
		t.Fatal("the promotion never read back between the removal and the last pass: the race did not happen")
	}
	if !errors.Is(promoteErr, ErrNotFound) {
		t.Fatalf("the promotion = %v, want refused: 1.0.0 belongs to the deleted orkflow", promoteErr)
	}
	if _, ok := d.object(v1Key); ok {
		t.Fatal("1.0.0, which the Delete removed, is stored again")
	}
	if got := headVersion(t, store); got != "3.0.0" {
		t.Fatalf("head = %q, want the re-created orkflow's 3.0.0", got)
	}
	if got, _ := store.CurrentVersion(ctx, DefaultNamespace, "deploy"); got != "" {
		t.Fatalf("current = %q, want none: the promotion lost to the Delete", got)
	}
}

// The head carries the instant its incarnation began: set when a head is
// swapped in from nothing, copied by every swap after.
func TestHeadPointer_SinceIsSetFromNothingAndCopied(t *testing.T) {
	ctx := context.Background()
	d, store := orkflowFixture(t)
	_, headKey, _ := rollbackKeys()
	first, _, err := store.readHead(ctx, DefaultNamespace, "deploy")
	if err != nil || first.Since.IsZero() || !first.Since.Equal(first.AppliedAt) {
		t.Fatalf("head after the first apply = %+v, %v; want since = its applied_at", first, err)
	}
	if _, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation()); err != nil {
		t.Fatalf("Put(2.0.0) = %v", err)
	}
	second, _, _ := store.readHead(ctx, DefaultNamespace, "deploy")
	if second.Version != "2.0.0" || !second.Since.Equal(first.Since) {
		t.Fatalf("head after the second apply = %+v, want since copied from %v", second, first.Since)
	}
	// An older engine rewrites the head without the field: a swap after it
	// copies nothing, never inventing an incarnation it did not see begin.
	d.mu.Lock()
	d.objects[headKey] = []byte(`{"version":"2.0.0","applied_at":"` + second.AppliedAt.Format(time.RFC3339Nano) + `"}` + "\n")
	d.mu.Unlock()
	if _, _, err := store.Put(ctx, applyOf("4.0.0", 4), NoHeadExpectation()); err != nil {
		t.Fatalf("Put(4.0.0) = %v", err)
	}
	if third, _, _ := store.readHead(ctx, DefaultNamespace, "deploy"); third.Version != "4.0.0" || !third.Since.IsZero() {
		t.Fatalf("head after a swap over an older engine's head = %+v, want no since", third)
	}
}

// A head without since — written before the field, or rewritten by an older
// engine — falls back to the restore: a promotion whose target a refused
// apply's rollback removed puts it back, however old the target.
func TestConfirmPromoted_AHeadWithoutSinceFallsBackToTheRestore(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	headKey, _ := OrkflowHeadKey(DefaultNamespace, "deploy")
	_, _, pointerKey := rollbackKeys()
	d, store := orkflowFixture(t)
	v1Raw, _ := d.object(v1Key)
	v1, _ := unmarshalOrkflow(v1Raw)
	mine := currentPointer{Version: "1.0.0", UpdatedAt: time.Unix(1_700_000_011, 0).UTC()}
	d.mu.Lock()
	d.objects[pointerKey], _ = marshalCurrentPointer(mine)
	d.objects[headKey] = []byte(`{"version":"3.0.0","applied_at":"2023-11-14T22:13:29Z"}` + "\n")
	delete(d.objects, v1Key)
	d.mu.Unlock()
	if err := store.confirmPromoted(ctx, DefaultNamespace, "deploy", v1Key, currentPointer{}, nil, mine, v1, v1Raw); err != nil {
		t.Fatalf("confirmPromoted = %v, want 1.0.0 put back under a head that names no incarnation", err)
	}
	if _, ok := d.object(v1Key); !ok {
		t.Fatal("the current pointer names 1.0.0, which is not stored")
	}
}

// recreateDuringSwap makes the next head swap of deploy meet an orkflow deleted
// and re-created since the apply read its head: a Delete takes both pointers
// and every version, then another apply creates 3.0.0, stamped at, its head
// swapped in from nothing — or, with oldEngine, written by an engine that
// records no since.
func recreateDuringSwap(t *testing.T, d *s3Double, at time.Time, oldEngine bool) *atomic.Bool {
	t.Helper()
	ctx := context.Background()
	_, headKey, _ := rollbackKeys()
	other := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	var hit atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, headKey) || !hit.CompareAndSwap(false, true) {
			return
		}
		d.mu.Lock()
		for key := range d.objects {
			if strings.HasPrefix(key, OrkflowPrefix+"/"+DefaultNamespace+"/deploy/") {
				delete(d.objects, key)
			}
		}
		d.mu.Unlock()
		recreated := applyOf("3.0.0", 0)
		recreated.CreatedAt = timestamppb.New(at)
		if _, _, err := other.Put(ctx, recreated, NoHeadExpectation()); err != nil {
			t.Errorf("the re-creating apply = %v", err)
		}
		if oldEngine {
			head, _, _ := other.readHead(ctx, DefaultNamespace, "deploy")
			d.mu.Lock()
			d.objects[headKey] = []byte(`{"version":"` + head.Version + `","applied_at":"` +
				head.AppliedAt.Format(time.RFC3339Nano) + `"}` + "\n")
			d.mu.Unlock()
		}
	}
	return &hit
}

// MEDIUM-1 (pre-existing): an unstated apply reads its head and creates
// 2.0.0; a Delete removes the pointers and 2.0.0, and another apply re-creates
// the orkflow with 3.0.0. The apply's swap is refused (412) and it goes round
// again. It is refused — never success for a version that is gone, nor a head
// moved onto it — and the re-created orkflow keeps its head: when the head's
// since cannot tell (an older engine's head, a first read with no head
// pointer), the apply reads its version back before it swaps, and finds it
// gone without touching the head.
func TestConfirmedPut_AnApplyWhoseOrkflowWasReCreatedIsRefused(t *testing.T) {
	ctx := context.Background()
	v2Key, headKey, _ := rollbackKeys()
	before := time.Unix(1_700_000_001, 500_000_000) // after 1.0.0, before 2.0.0
	after := time.Unix(1_700_000_009, 0)
	for _, tc := range []struct {
		name      string
		at        time.Time // the re-creating apply's stamp
		oldEngine bool
		noPointer bool // the first read finds no head pointer: a derived head
	}{
		{name: "since tells, the re-created head stamped after this apply", at: after},
		{name: "since tells, this apply stamped after the re-created head", at: before},
		{name: "an older engine's head, stamped before this apply", at: before, oldEngine: true},
		{name: "an older engine's head, stamped after this apply", at: after, oldEngine: true},
		{name: "a first read with no head pointer, this apply stamped after", at: before, noPointer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, store := orkflowFixture(t)
			if tc.noPointer {
				d.mu.Lock()
				delete(d.objects, headKey)
				d.mu.Unlock()
			}
			hit := recreateDuringSwap(t, d, tc.at, tc.oldEngine)
			_, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation())
			if !hit.Load() {
				t.Fatal("the apply never swapped its head: the race did not happen")
			}
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("Put = %v, want refused: the orkflow it read was deleted and re-created", err)
			}
			if _, ok := d.object(v2Key); ok {
				t.Fatal("2.0.0 is stored after its apply was refused")
			}
			if head := headVersion(t, store); head != "3.0.0" {
				t.Fatalf("head = %q, want the re-created orkflow's 3.0.0 untouched", head)
			}
		})
	}
}

// LOW-1: a catalog with no head pointer; an apply reads the head it derives
// from it, and a concurrent first apply swaps a head in first, so the apply's
// swap is refused — which proves a head existed. A Delete then takes that head
// and the other apply's version, before it reached this apply's. The apply's
// next round finds no head: that is a Delete, whatever its first read showed,
// and the apply is refused and rolls back, never deriving the head from its
// own uncommitted version.
func TestConfirmedPut_AnApplyThatFindsTheHeadGoneAfterARefusedSwapIsRefused(t *testing.T) {
	ctx := context.Background()
	v1Key, _ := OrkflowKey(DefaultNamespace, "deploy", "1.0.0")
	v2Key, headKey, _ := rollbackKeys()
	d, store := orkflowFixture(t)
	d.mu.Lock()
	delete(d.objects, headKey) // a catalog with no head pointer
	d.mu.Unlock()
	var raced, deleted atomic.Bool
	d.onRequest = func(r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && raced.CompareAndSwap(false, true):
			// A concurrent first apply's head lands first: this swap is refused.
			document, _ := marshalHeadPointer(headPointer{Version: "1.0.0", AppliedAt: time.Unix(1_700_000_001, 0),
				Since: time.Unix(1_700_000_001, 0)})
			d.mu.Lock()
			d.objects[headKey] = document
			d.mu.Unlock()
		case raced.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, headKey) &&
			deleted.CompareAndSwap(false, true):
			// The next round reads the head: a Delete took it, and 1.0.0, first.
			d.mu.Lock()
			delete(d.objects, headKey)
			delete(d.objects, v1Key)
			d.mu.Unlock()
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation())
	if !deleted.Load() {
		t.Fatal("the apply never went round again: the race did not happen")
	}
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "deleted") {
		t.Fatalf("Put = %v, want refused: the head it swapped against is gone", err)
	}
	if _, ok := d.object(v2Key); ok {
		t.Fatal("2.0.0 is stored after its apply was refused")
	}
}

// LOW-2: an identical apply finds 2.0.0 stored ahead of the head by a refused
// apply, and goes to complete its commit; its swap is refused, and the refused
// apply's rollback removes 2.0.0 meanwhile. The apply did not create it, so it
// cannot tell a rollback from a Delete, and says neither as fact.
func TestConfirmedPut_AnIdenticalApplyWhoseVersionWasRemovedSaysSoNeutrally(t *testing.T) {
	ctx := context.Background()
	v2Key, headKey, _ := rollbackKeys()
	d, store := orkflowFixture(t)
	theirs, _ := marshalOrkflow(namespacedOrkflow(applyOf("2.0.0", 2))) // the refused apply's version, ahead of the head
	d.mu.Lock()
	d.objects[v2Key] = theirs
	d.mu.Unlock()
	var moved atomic.Bool
	d.onRequest = func(r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, headKey) && moved.CompareAndSwap(false, true) {
			d.mu.Lock()
			before, _ := unmarshalHeadPointer(d.objects[headKey])
			document, _ := marshalHeadPointer(headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_009, 0),
				Since: before.Since})
			d.objects[headKey] = document // another apply moves the head
			delete(d.objects, v2Key)      // and the refused apply's rollback removes 2.0.0
			d.mu.Unlock()
		}
	}
	_, _, err := store.Put(ctx, applyOf("2.0.0", 5), NoHeadExpectation())
	if !moved.Load() {
		t.Fatal("the apply never swapped: the race did not happen")
	}
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "a refused apply's rollback, or a Delete") ||
		strings.Contains(err.Error(), "re-created") {
		t.Fatalf("Put = %v, want refused neutrally: the version it found was removed since", err)
	}
	if got := headVersion(t, store); got != "3.0.0" {
		t.Fatalf("head = %q, want 3.0.0 untouched", got)
	}
}

// An apply that does not know it created its version, and finds it gone
// after a refused swap, goes on only when the head it now reads is of the
// incarnation it read a pointer head of before, and the version was created
// within it — the refused apply's rollback, which its swap undoes. Otherwise
// (another incarnation, a head that records none, a first read with no head
// pointer) it cannot tell a rollback from a Delete, and is refused saying so;
// an apply that created its version knows only a Delete removes it.
func TestVersionStillStored_TellsARollbackOnlyWithinAnIncarnation(t *testing.T) {
	ctx := context.Background()
	v2Key, _, _ := rollbackKeys()
	_, store := orkflowFixture(t)
	version := applyOf("2.0.0", 5)
	within := headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_009, 0), Since: time.Unix(1_700_000_001, 0)}
	after := headPointer{Version: "3.0.0", AppliedAt: time.Unix(1_700_000_009, 0), Since: time.Unix(1_700_000_008, 0)}
	for _, tc := range []struct {
		name       string
		known      bool
		head       headPointer
		same       bool // the incarnation this apply read a pointer head of before
		rolledBack bool
		says       string
	}{
		{"not known created, within the incarnation it read: a rollback", false, within, true, true, ""},
		{"not known created, the version before the incarnation", false, after, true, false, "a refused apply's rollback, or a Delete"},
		{"not known created, not the incarnation it read", false, within, false, false, "a refused apply's rollback, or a Delete"},
		{"created by this apply", true, within, true, false, "was deleted as version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rolledBack, err := store.versionStillStored(ctx, DefaultNamespace, "deploy", v2Key, version, tc.known, tc.head, tc.same)
			if rolledBack != tc.rolledBack || (tc.says == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.says)) {
				t.Fatalf("versionStillStored = %v, %v; want rolled back %v, saying %q", rolledBack, err, tc.rolledBack, tc.says)
			}
		})
	}
}

// LOW-1: an apply's first read finds no head pointer (a derived head), and
// its create's answer is lost, so it does not know it created 2.0.0. During
// its swap a Delete takes everything and another apply re-creates the
// orkflow, its since not after 2.0.0's stamp. The apply goes round again and
// finds 2.0.0 gone; with no pointer head read before its refused swap, it
// cannot tell a rollback within one incarnation from a Delete, so it is
// refused and puts nothing back — the re-created orkflow keeps its head and
// the deleted version stays deleted.
func TestConfirmedPut_AnApplyWithADerivedFirstHeadNeverRestoresIntoANewIncarnation(t *testing.T) {
	ctx := context.Background()
	v2Key, headKey, _ := rollbackKeys()
	d, store := orkflowFixture(t)
	d.mu.Lock()
	delete(d.objects, headKey) // a catalog with no head pointer
	d.mu.Unlock()
	d.loseAnswer(v2Key, 0) // the create lands, its answer is lost, and a read shows it
	hit := recreateDuringSwap(t, d, time.Unix(1_700_000_001, 500_000_000), false)
	_, _, err := store.Put(ctx, applyOf("2.0.0", 2), NoHeadExpectation())
	if !hit.Load() {
		t.Fatal("the apply never swapped its head: the race did not happen")
	}
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "a refused apply's rollback, or a Delete") {
		t.Fatalf("Put = %v, want refused: it cannot tell a rollback from a Delete", err)
	}
	if _, ok := d.object(v2Key); ok {
		t.Fatal("2.0.0, which the Delete removed, is stored again")
	}
	if head := headVersion(t, store); head != "3.0.0" {
		t.Fatalf("head = %q, want the re-created orkflow's 3.0.0 untouched", head)
	}
}
