package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// MEDIUM-D: the boot probe asks the store the two conditional answers a
// rollback and a promotion rely on besides the commit's create
// (docs/STORAGE.md): a PutObject with If-Match on a key that does not exist is
// refused, and a DeleteObject with If-Match is held to its condition. A store
// that gets either wrong refuses to boot, saying which, as a store that
// answered rather than one that could not; one that gets both right boots,
// and the probe leaves nothing behind but its durable commit winner.
func TestProbeCommitStore_AsksTheConditionalAnswersARollbackNeeds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		breaks func(*s3Double)
		want   string
		// keeps: the store keeps what the probe deletes, so its objects stay.
		keeps bool
	}{
		{name: "a store that answers both"},
		{name: "a store that stores an If-Match PutObject on a missing key",
			breaks: func(d *s3Double) { d.ignoreMissingIfMatch = true }, want: "If-Match on a key that does not exist"},
		{name: "a store that ignores a DeleteObject's If-Match",
			breaks: func(d *s3Double) { d.ignoreDeleteCondition = true }, want: "DeleteObject with If-Match"},
		// LOW-7: a store that rejects a conditional delete outright answered,
		// and the probe says what it answered.
		{name: "a store that rejects a conditional delete as a bad request",
			breaks: func(d *s3Double) { d.deleteConditionAnswer = 400 }, want: "DeleteObject with If-Match naming another etag"},
		{name: "a store that does not implement a conditional delete",
			breaks: func(d *s3Double) { d.deleteConditionAnswer = 501 }, want: "NotImplemented"},
		{name: "a store that keeps an object a delete under its etag removed",
			breaks: func(d *s3Double) { d.keepOnDelete = true }, want: "kept an object", keeps: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newS3Double(t)
			if tc.breaks != nil {
				tc.breaks(d)
			}
			store := &S3CommitStore{client: d.fastClient(t), bucket: d.bucket}
			err := ProbeCommitStore(context.Background(), store)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("ProbeCommitStore = %v, want the store accepted", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) || errors.Is(err, ErrCommitStoreUnavailable) {
				t.Fatalf("ProbeCommitStore = %v, want the store refused, as answered: %s", err, tc.want)
			}
			if tc.keeps {
				return
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			for key := range d.objects {
				if key != CommitProbeKey {
					t.Errorf("the probe left %s behind", key)
				}
			}
		})
	}
}

// LOW-5: the engine asks the same answers of the store it keeps orkflows in,
// because the rollback and the promotion run there; a store that ignores a
// conditional delete refuses to boot it, as answered.
func TestS3OrkflowStore_ProbeConditionalAnswers(t *testing.T) {
	d := newS3Double(t)
	store := &S3OrkflowStore{client: d.fastClient(t), bucket: d.bucket}
	if err := store.ProbeConditionalAnswers(context.Background()); err != nil {
		t.Fatalf("ProbeConditionalAnswers = %v, want a store that answers both accepted", err)
	}
	if len(d.objects) != 0 {
		t.Fatalf("the probe left %d objects behind", len(d.objects))
	}
	d.ignoreDeleteCondition = true
	err := store.ProbeConditionalAnswers(context.Background())
	if err == nil || !strings.Contains(err.Error(), "DeleteObject with If-Match") || errors.Is(err, ErrCommitStoreUnavailable) {
		t.Fatalf("ProbeConditionalAnswers = %v, want the store refused, as answered", err)
	}
}
