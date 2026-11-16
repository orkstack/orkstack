package artifacts

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"
)

// downBlobs fails every call of the store underneath while down is set.
type downBlobs struct {
	blobs
	down bool
	err  error
}

func (d *downBlobs) fail() error {
	if d.err != nil {
		return d.err
	}
	return errors.New("connection refused")
}
func (d *downBlobs) get(ctx context.Context, key string, max int64) ([]byte, string, error) {
	if d.down {
		return nil, "", d.fail()
	}
	return d.blobs.get(ctx, key, max)
}
func (d *downBlobs) getStored(ctx context.Context, key string, max int64) ([]byte, time.Time, error) {
	if d.down {
		return nil, time.Time{}, d.fail()
	}
	return d.blobs.getStored(ctx, key, max)
}
func (d *downBlobs) create(ctx context.Context, key, ct string, body []byte) error {
	if d.down {
		return d.fail()
	}
	return d.blobs.create(ctx, key, ct, body)
}
func (d *downBlobs) list(ctx context.Context, in blobListInput) (blobListPage, error) {
	if d.down {
		return blobListPage{}, d.fail()
	}
	return d.blobs.list(ctx, in)
}

func newTestRecordStore(t *testing.T) (*RecordStore, *downBlobs) {
	t.Helper()
	b, err := openLocalBlobs("test", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := &downBlobs{blobs: b}
	return newRecordStore(d), d
}

func resumeAt(scope, previous uint64, n int, tasks ...string) ResumeRecord {
	return ResumeRecord{Namespace: "ns", RunID: "ork-1", Scope: scope, N: n, PreviousScope: previous,
		Tasks: tasks, ReopenedAwaits: []string{}}
}

// TestResumeStore_TheNewestScopeIsTheOneNoLinkLeavesFrom: a run never
// resumed has no newest record; each resume's record and link make its
// scope the newest, and a scope's record is the one of its highest plan.
func TestResumeStore_TheNewestScopeIsTheOneNoLinkLeavesFrom(t *testing.T) {
	s, _ := newTestRecordStore(t)
	ctx := context.Background()
	if scope, got, err := s.NewestResume(ctx, "ns", "ork-1"); err != nil || scope != 0 || got.Answer != RecordNone {
		t.Fatalf("never resumed = %d, %+v, %v", scope, got, err)
	}
	if err := s.WriteResume(ctx, resumeAt(1, 0, 0, "build")); err != nil {
		t.Fatal(err)
	}
	// The second resume's first plan was abandoned before its record.
	if err := s.WriteResume(ctx, resumeAt(2, 1, 1, "test")); err != nil {
		t.Fatal(err)
	}
	scope, got, err := s.NewestResume(ctx, "ns", "ork-1")
	if err != nil || scope != 2 || got.Answer != RecordFound || got.Value.N != 1 || got.Value.Tasks[0] != "test" {
		t.Fatalf("newest = %d, %+v, %v", scope, got, err)
	}
	if got, err := s.ResumeOf(ctx, "ns", "ork-1", 1); err != nil || got.Answer != RecordFound || got.Value.Tasks[0] != "build" {
		t.Fatalf("scope 1 = %+v, %v", got, err)
	}
}

// TestResumeStore_WriterRefusals: a record written again with the same
// bytes, or its link again naming the same scope, succeeds; a link that
// names another scope, a scope a later resume already left, and other bytes
// at a record's key are refused, and nothing further is written.
func TestResumeStore_WriterRefusals(t *testing.T) {
	s, b := newTestRecordStore(t)
	ctx := context.Background()
	// An unlinked record of another resume at this plan's key.
	if err := b.create(ctx, mustKey(ResumeRecordKey("ns", "ork-1", 3, 0)), "application/json", mustEncode(resumeAt(3, 0, 0, "lost"))); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteResume(ctx, resumeAt(3, 0, 0, "other")); !errors.Is(err, ErrResumeRecordDiffers) {
		t.Fatalf("other bytes at a record's key: %v", err)
	}
	first := resumeAt(1, 0, 0, "build")
	if err := s.WriteResume(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteResume(ctx, first); err != nil {
		t.Fatalf("the same resume again: %v", err)
	}
	if err := s.WriteResume(ctx, resumeAt(1, 0, 0, "other")); !errors.Is(err, ErrResumeLinkElsewhere) {
		t.Fatalf("another record for a linked scope: %v", err)
	}
	if err := s.WriteResume(ctx, resumeAt(2, 0, 0, "x")); !errors.Is(err, ErrResumeLinkElsewhere) {
		t.Fatalf("a second link from scope 0: %v", err)
	}
	if err := s.WriteResume(ctx, resumeAt(2, 1, 0, "x")); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteResume(ctx, resumeAt(1, 0, 1, "late")); !errors.Is(err, ErrResumeScopeHasSuccessor) {
		t.Fatalf("a record for a scope a later resume left: %v", err)
	}
}

// TestResumeStore_UntrustedAndUnanswered: a link to a scope with no record,
// a record whose previous scope is not the one that links to it, and a link
// that does not lead upward are invalid; a store that does not answer is
// RECORD_STORE_UNANSWERED with its class, and a cancellation is a
// cancellation.
func TestResumeStore_UntrustedAndUnanswered(t *testing.T) {
	ctx := context.Background()
	s, b := newTestRecordStore(t)
	link, _ := ResumeLinkKey("ns", "ork-1", 0)
	if err := b.create(ctx, link, "application/json", encodeResumeLink(resumeLink{Scope: 3, RecordSHA256: strings.Repeat("a", 64)})); err != nil {
		t.Fatal(err)
	}
	if _, got, err := s.NewestResume(ctx, "ns", "ork-1"); err != nil || got.Answer != RecordInvalid {
		t.Fatalf("a link to a scope with no record = %+v, %v", got, err)
	}

	s, b = newTestRecordStore(t)
	if err := s.WriteResume(ctx, resumeAt(2, 1, 0, "x")); err != nil {
		t.Fatal(err)
	}
	if err := b.remove(ctx, mustKey(ResumeLinkKey("ns", "ork-1", 1))); err != nil {
		t.Fatal(err)
	}
	if err := b.create(ctx, link, "application/json", encodeResumeLink(resumeLink{Scope: 2, RecordSHA256: hexDigest(mustEncode(resumeAt(2, 1, 0, "x")))})); err != nil {
		t.Fatal(err)
	}
	if _, got, err := s.NewestResume(ctx, "ns", "ork-1"); err != nil || got.Answer != RecordInvalid {
		t.Fatalf("a record whose previous scope did not link to it = %+v, %v", got, err)
	}

	s, b = newTestRecordStore(t)
	b.down = true
	var un *RecordStoreUnanswered
	if _, _, err := s.NewestResume(ctx, "ns", "ork-1"); !errors.As(err, &un) || un.Class != UnansweredUnavailable {
		t.Fatalf("a store that does not answer = %v", err)
	}
	if err := s.WriteResume(ctx, resumeAt(1, 0, 0)); !errors.As(err, &un) {
		t.Fatalf("a write the store does not answer = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	b.err = context.Canceled
	if _, _, err := s.NewestResume(canceled, "ns", "ork-1"); !errors.Is(err, context.Canceled) || errors.As(err, &un) {
		t.Fatalf("a cancellation = %v", err)
	}
}

func mustKey(k string, err error) string {
	if err != nil {
		panic(err)
	}
	return k
}

// TestResumeStore_AScopeIsOnePage: a scope's record is found in one page
// of a listing, and a scope that holds more than a page, which no resume
// writes, is invalid rather than read on.
func TestResumeStore_AScopeIsOnePage(t *testing.T) {
	s, b := newTestRecordStore(t)
	ctx := context.Background()
	if err := s.WriteResume(ctx, resumeAt(1, 0, maxResumePlan, "x")); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ResumeOf(ctx, "ns", "ork-1", 1); err != nil || got.Answer != RecordFound || got.Value.N != maxResumePlan {
		t.Fatalf("the highest plan = %+v, %v", got, err)
	}
	dir, _ := ResumeRecordKey("ns", "ork-1", 1, 0)
	for i := range resumeScopePage {
		if err := b.create(ctx, path.Dir(dir)+fmt.Sprintf("/extra-%04d", i), "application/json", []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := s.ResumeOf(ctx, "ns", "ork-1", 1); err != nil || got.Answer != RecordInvalid {
		t.Fatalf("a scope past one page = %+v, %v", got, err)
	}
}

// TestResumeStore_TheLinkNamesTheRecordItCommitted: a second writer of a
// scope already linked, under another plan, is refused before it writes,
// and the newest scope's record is the one its link names, never a later
// plan's.
func TestResumeStore_TheLinkNamesTheRecordItCommitted(t *testing.T) {
	s, b := newTestRecordStore(t)
	ctx := context.Background()
	if err := s.WriteResume(ctx, resumeAt(1, 0, 0, "first")); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteResume(ctx, resumeAt(1, 0, 1, "second")); !errors.Is(err, ErrResumeLinkElsewhere) {
		t.Fatalf("a second plan of a linked scope = %v", err)
	}
	// A racing writer's orphan, a later plan, written straight to the store.
	raw, _ := EncodeResumeRecord(resumeAt(1, 0, 2, "orphan"))
	if err := b.create(ctx, mustKey(ResumeRecordKey("ns", "ork-1", 1, 2)), "application/json", raw); err != nil {
		t.Fatal(err)
	}
	scope, got, err := s.NewestResume(ctx, "ns", "ork-1")
	if err != nil || scope != 1 || got.Answer != RecordFound || got.Value.N != 0 || got.Value.Tasks[0] != "first" {
		t.Fatalf("newest = %d, %+v, %v", scope, got, err)
	}
}

func mustEncode(r ResumeRecord) []byte {
	raw, err := EncodeResumeRecord(r)
	if err != nil {
		panic(err)
	}
	return raw
}
