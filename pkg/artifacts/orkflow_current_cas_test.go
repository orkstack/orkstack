package artifacts

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// casFixture is an S3 double holding versions 1.0.0 and 2.0.0 of one orkflow
// and a current pointer naming 1.0.0, with a store against it.
func casFixture(t *testing.T) (*s3Double, *S3OrkflowStore, string) {
	t.Helper()
	d := newS3Double(t)
	for _, version := range []string{"1.0.0", "2.0.0"} {
		key, err := OrkflowKey(DefaultNamespace, "deploy", version)
		if err != nil {
			t.Fatalf("OrkflowKey(%s): %v", version, err)
		}
		document, err := marshalOrkflow(&orkstackv1.Orkflow{Name: "deploy", Version: version})
		if err != nil {
			t.Fatalf("marshalOrkflow(%s): %v", version, err)
		}
		d.objects[key] = document
	}
	pointerKey, err := OrkflowCurrentKey(DefaultNamespace, "deploy")
	if err != nil {
		t.Fatalf("OrkflowCurrentKey: %v", err)
	}
	d.objects[pointerKey] = pointerDocument(t, "1.0.0", time.Unix(1_700_000_000, 0))
	return d, &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}, pointerKey
}

func pointerDocument(t *testing.T, version string, at time.Time) []byte {
	t.Helper()
	document, err := marshalCurrentPointer(currentPointer{Version: version, UpdatedAt: at.UTC()})
	if err != nil {
		t.Fatalf("marshalCurrentPointer: %v", err)
	}
	return document
}

// storedPointer is the version the double's pointer names now.
func storedPointer(t *testing.T, d *s3Double, key string) string {
	t.Helper()
	document, ok := d.object(key)
	if !ok {
		t.Fatal("the double holds no current pointer")
	}
	pointer, err := unmarshalCurrentPointer(document)
	if err != nil {
		t.Fatalf("decode the stored pointer: %v", err)
	}
	return pointer.Version
}

// A promotion whose swap was stored but whose answer was lost is resent by
// the SDK, and the resend's If-Match then meets the promotion's own write: a
// 412 for a promotion that landed. The store must report what happened — it
// landed, over 1.0.0 — not a promotion somebody else beat (expected), nor the
// version its own write left (unconditional). The same holds when every
// resend fails outright and the SDK gives up: the write it first sent is
// there.
func TestSetCurrentVersion_ASwapWhoseAnswerWasLostIsReportedLanded(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected CurrentExpectation
		// resends is how many of the SDK's resends fail before checking
		// anything; 0 lets the first resend meet the stored write (412).
		resends int
	}{
		{"expected 1.0.0, the resend meets its own write", ExpectCurrent("1.0.0"), 0},
		{"unconditional, the resend meets its own write", NoCurrentExpectation(), 0},
		{"expected 1.0.0, every resend fails", ExpectCurrent("1.0.0"), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, store, pointerKey := casFixture(t)
			d.loseAnswer(pointerKey, tc.resends)

			previous, err := store.SetCurrentVersion(context.Background(), DefaultNamespace, "deploy", "2.0.0", tc.expected)
			if err != nil {
				t.Fatalf("SetCurrentVersion = %v; the swap landed (the pointer names %s)", err, storedPointer(t, d, pointerKey))
			}
			if previous != "1.0.0" {
				t.Fatalf("SetCurrentVersion replaced %q, want 1.0.0: the version its own write left is not what it replaced", previous)
			}
			if got := storedPointer(t, d, pointerKey); got != "2.0.0" {
				t.Fatalf("the pointer names %s, want 2.0.0", got)
			}
			if puts, _, _ := d.counts(); puts < 2 {
				t.Fatalf("%d PUTs: the SDK never resent the swap, so nothing here was tested", puts)
			}
		})
	}
}

// Writers re-promoting the version the pointer already names move its etag
// and not its version. A promotion that loses to such a rewrite on every one
// of its attempts never lands, and its expectation never stopped holding: it
// is refused for contention (ErrPreconditionFailed, which the API answers
// FailedPrecondition, "promoted by somebody else while this promotion was
// being written"), not reported as a pointer that moved (ErrCurrentMoved).
// That is exactly "another writer moved the pointer first (attempt 2)".
func TestSetCurrentVersion_ContentionThatOutlastsTheRetryIsARefusal(t *testing.T) {
	d, store, pointerKey := casFixture(t)
	rewrites := 0
	d.onRequest = func(r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, pointerKey) {
			return
		}
		// Another writer's re-promotion of 1.0.0 lands just before each swap.
		d.mu.Lock()
		defer d.mu.Unlock()
		rewrites++
		d.objects[pointerKey] = pointerDocument(t, "1.0.0", time.Unix(1_700_000_000+int64(rewrites), 0))
	}

	_, err := store.SetCurrentVersion(context.Background(), DefaultNamespace, "deploy", "2.0.0", ExpectCurrent("1.0.0"))
	if !errors.Is(err, ErrPreconditionFailed) || errors.Is(err, ErrCurrentMoved) {
		t.Fatalf("SetCurrentVersion = %v, want a refusal for contention (ErrPreconditionFailed, not ErrCurrentMoved)", err)
	}
	if !strings.Contains(err.Error(), "another writer moved the pointer first (attempt 2)") {
		t.Fatalf("SetCurrentVersion = %v, want the refusal after its last attempt", err)
	}
	d.mu.Lock()
	last := rewrites
	d.mu.Unlock()
	stored, _ := d.object(pointerKey)
	if want := pointerDocument(t, "1.0.0", time.Unix(1_700_000_000+int64(last), 0)); !bytes.Equal(stored, want) {
		t.Fatalf("the pointer holds %s, want the other writer's last rewrite: a refused promotion landed", stored)
	}
	if puts, _, _ := d.counts(); puts != setCurrentAttempts {
		t.Fatalf("%d PUTs, want one per attempt (%d)", puts, setCurrentAttempts)
	}
}
