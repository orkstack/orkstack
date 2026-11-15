package api

import (
	"context"
	"errors"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// aroundFixture is one deployment's store, a replica that has read its
// first revision, and a head then put straight into the bucket.
type aroundFixture struct {
	cas    *artifacts.MemoryCAS
	store  *artifacts.AccessStore
	cache  *PolicyCache
	doc    *orkstackv1.AccessDocument
	first  *artifacts.AccessHead
	around artifacts.AccessRevisionMeta
}

func newAroundFixture(t *testing.T) *aroundFixture {
	t.Helper()
	ctx := context.Background()
	f := &aroundFixture{cas: artifacts.NewMemoryCAS()}
	f.store = artifacts.NewAccessStoreOn(f.cas)
	f.doc = &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{
		{Name: "b", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:a"}}}}
	var err error
	if f.first, err = f.store.Write(ctx, f.doc, nil, artifacts.AccessWrite{At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	f.cache = NewPolicyCache(f.store, PolicyCacheOptions{})
	f.cache.Refresh(ctx)
	body, _ := artifacts.EncodeAccessObject(f.doc, artifacts.AccessRevisionMeta{Revision: 9, WrittenAt: time.Now()})
	f.cas.PutRaw(artifacts.AccessDocumentKey, body)
	_, meta, err := artifacts.DecodeAccessObject(body)
	if err != nil {
		t.Fatal(err)
	}
	f.around = meta
	f.cache.Refresh(ctx)
	if !f.cache.State().WrittenAroundAPI {
		t.Fatal("fixture: the head put into the bucket raised no banner")
	}
	return f
}

// TestPolicyCache_AnotherReplicasAcknowledgementClearsTheBanner: the record
// is what every replica reads, so an acknowledgement made through one
// clears the banner on all of them at their next read.
func TestPolicyCache_AnotherReplicasAcknowledgementClearsTheBanner(t *testing.T) {
	ctx := context.Background()
	f := newAroundFixture(t)
	f.cache.Refresh(ctx)
	if !f.cache.State().WrittenAroundAPI {
		t.Fatal("the banner cleared with no acknowledgement")
	}
	if _, err := f.store.Acknowledge(ctx, artifacts.AccessAck{Revision: 9, SHA256: f.around.SHA256, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	f.cache.Refresh(ctx)
	st := f.cache.State()
	if st.WrittenAroundAPI || st.AcknowledgedRevision != 9 {
		t.Fatalf("after another replica acknowledged it: %+v; want no banner, acknowledged 9", st)
	}
}

// TestPolicyCache_AnAcknowledgedHeadRaisesNoBanner: a replica that finds a
// head written around the API checks for its record first.
func TestPolicyCache_AnAcknowledgedHeadRaisesNoBanner(t *testing.T) {
	ctx := context.Background()
	f := newAroundFixture(t)
	if _, err := f.store.Acknowledge(ctx, artifacts.AccessAck{Revision: 9, SHA256: f.around.SHA256, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	other := NewPolicyCache(f.store, PolicyCacheOptions{})
	// It had read revision 1 before the head was put into the bucket.
	other.Wrote(f.first)
	other.Refresh(ctx)
	if st := other.State(); st.WrittenAroundAPI || st.AcknowledgedRevision != 9 {
		t.Fatalf("a head already acknowledged = %+v; want no banner", st)
	}
}

// TestPolicyCache_AnotherRevisionsAcknowledgementLeavesTheBanner: only the
// revision found, or the head the deployment has now, clears it: a restore
// to an older revision after a later one was acknowledged still shows.
func TestPolicyCache_AnotherRevisionsAcknowledgementLeavesTheBanner(t *testing.T) {
	ctx := context.Background()
	f := newAroundFixture(t)
	for _, ack := range []artifacts.AccessAck{
		{Revision: 9, SHA256: f.first.Meta.SHA256}, // the right number, another digest
		{Revision: 10, SHA256: f.around.SHA256},    // a later number, the head's digest
		{Revision: 1, SHA256: f.first.Meta.SHA256}, // an earlier revision
	} {
		ack.At = time.Now()
		if _, err := f.store.Acknowledge(ctx, ack); err != nil {
			t.Fatal(err)
		}
	}
	f.cache.Refresh(ctx)
	st := f.cache.State()
	if !st.WrittenAroundAPI {
		t.Fatal("an acknowledgement of another revision cleared the banner")
	}
	if st.AcknowledgedRevision != 10 {
		t.Fatalf("acknowledged revision = %d; want the highest present, 10", st.AcknowledgedRevision)
	}
}

// TestPolicyCache_AcknowledgingTheCurrentHeadClearsAnEarlierBanner: a write
// through the API after one around it leaves the banner up; acknowledging
// the head the deployment has now clears it.
func TestPolicyCache_AcknowledgingTheCurrentHeadClearsAnEarlierBanner(t *testing.T) {
	ctx := context.Background()
	f := newAroundFixture(t)
	head, err := f.store.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next, err := f.store.Write(ctx, f.doc, head, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	f.cache.Refresh(ctx)
	if !f.cache.State().WrittenAroundAPI {
		t.Fatal("a write through the API cleared a banner no one acknowledged")
	}
	if _, err := f.store.Acknowledge(ctx, artifacts.AccessAck{Revision: next.Meta.Revision, SHA256: next.Meta.SHA256, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	f.cache.Refresh(ctx)
	if f.cache.State().WrittenAroundAPI {
		t.Fatal("acknowledging the current head left the banner")
	}
}

// TestPolicyCache_AStoreDownNeverClearsTheBanner: an acknowledgement that
// could not be looked for is not taken for one.
func TestPolicyCache_AStoreDownNeverClearsTheBanner(t *testing.T) {
	ctx := context.Background()
	f := newAroundFixture(t)
	f.cas.SetFailure(errors.New("store down"))
	f.cache.Refresh(ctx)
	if !f.cache.State().WrittenAroundAPI {
		t.Fatal("a store that did not answer cleared the banner")
	}
}

// TestPolicyCache_AcknowledgeLocally: this replica's own acknowledgement
// clears its banner at once, and only for the revision and digest found.
func TestPolicyCache_AcknowledgeLocally(t *testing.T) {
	f := newAroundFixture(t)
	f.cache.Acknowledge(9, f.first.Meta.SHA256)
	if !f.cache.State().WrittenAroundAPI {
		t.Fatal("an acknowledgement of another digest cleared the banner")
	}
	f.cache.Acknowledge(9, f.around.SHA256)
	if st := f.cache.State(); st.WrittenAroundAPI || st.AcknowledgedRevision != 9 {
		t.Fatalf("after acknowledging it here: %+v", st)
	}
}

// TestPolicyCache_AFailedListingIsListedAgain: a listing that failed when
// the head moved is listed again at the next read, though the head has not
// moved since and no banner is up (Go review MEDIUM 1).
func TestPolicyCache_AFailedListingIsListedAgain(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	doc := &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{
		{Name: "b", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:a"}}}}
	head, err := store.Write(ctx, doc, nil, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acknowledge(ctx, artifacts.AccessAck{Revision: 1, SHA256: head.Meta.SHA256, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	c := NewPolicyCache(store, PolicyCacheOptions{})
	cas.SetListFailure(errors.New("listing down"))
	c.Refresh(ctx)
	if got := c.State().AcknowledgedRevision; got != 0 {
		t.Fatalf("acknowledged revision with the listing down = %d; want 0", got)
	}
	cas.SetListFailure(nil)
	c.Refresh(ctx)
	if got := c.State().AcknowledgedRevision; got != 1 {
		t.Fatalf("acknowledged revision once the listing answers = %d; want 1", got)
	}
}

// TestPolicyCache_AGoneDocumentsBannerClearsAtTheNextHead: a document
// removed around the API names no revision to acknowledge; its banner
// clears once a document is written again and that head is acknowledged.
func TestPolicyCache_AGoneDocumentsBannerClearsAtTheNextHead(t *testing.T) {
	ctx := context.Background()
	f := newAroundFixture(t)
	f.cas.Delete(artifacts.AccessDocumentKey)
	f.cache.Refresh(ctx)
	if !f.cache.State().WrittenAroundAPI {
		t.Fatal("a document gone raised no banner")
	}
	next, err := f.store.Write(ctx, f.doc, nil, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	f.cache.Wrote(next)
	if !f.cache.State().WrittenAroundAPI {
		t.Fatal("writing a document again cleared a banner no one acknowledged")
	}
	if _, err := f.store.Acknowledge(ctx, artifacts.AccessAck{Revision: next.Meta.Revision, SHA256: next.Meta.SHA256, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	f.cache.Refresh(ctx)
	if f.cache.State().WrittenAroundAPI {
		t.Fatal("acknowledging the head written after the document was gone left the banner")
	}
}
