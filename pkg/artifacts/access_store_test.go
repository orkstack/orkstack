package artifacts_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func accessDoc(bindings ...*orkstackv1.Binding) *orkstackv1.AccessDocument {
	return &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: bindings}
}

func adminBinding(subject string) *orkstackv1.Binding {
	return &orkstackv1.Binding{Name: "admins", Role: "admin", Scope: "deployment", Subjects: []string{subject}}
}

func TestAccessStore_WriteReadAndChain(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	if _, err := store.Get(ctx); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get of a deployment that never wrote one = %v; want ErrNotFound", err)
	}
	at := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	first, err := store.Write(ctx, accessDoc(adminBinding("group:corp:sre")), nil, artifacts.AccessWrite{At: at, By: "user:corp:a", ByVerified: true, Change: "create binding admins"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Meta.Revision != 1 || first.Meta.PreviousSHA256 != "" || first.ETag == "" {
		t.Fatalf("first write = %+v", first.Meta)
	}
	got, err := store.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Meta.SHA256 != first.Meta.SHA256 || got.ETag != first.ETag || got.Document.GetRevision() != 1 || got.Document.GetEtag() != first.ETag {
		t.Fatalf("read back %+v, etag %q; wrote %+v", got.Meta, got.ETag, first.Meta)
	}
	if ok, err := store.HasRevision(ctx, got.Meta); err != nil || !ok {
		t.Fatalf("the head's revision exists = %v, %v", ok, err)
	}
	second, err := store.Write(ctx, accessDoc(adminBinding("group:corp:ops")), got, artifacts.AccessWrite{At: at.Add(time.Minute), By: "user:corp:a"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Meta.Revision != 2 || second.Meta.PreviousSHA256 != first.Meta.SHA256 {
		t.Fatalf("second write = %+v; want revision 2 linked to %s", second.Meta, first.Meta.SHA256)
	}
	rev1, err := store.GetRevision(ctx, 1, first.Meta.SHA256)
	if err != nil || rev1.Document.GetBindings()[0].GetSubjects()[0] != "group:corp:sre" || rev1.Meta.WrittenBy != "user:corp:a" || !rev1.Meta.WrittenByVerified {
		t.Fatalf("revision 1 = %+v, %v", rev1, err)
	}
}

func TestAccessStore_LostSwapLeavesTheHeadAndAnOrphanRevision(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	at := time.Now()
	base, err := store.Write(ctx, accessDoc(adminBinding("group:corp:sre")), nil, artifacts.AccessWrite{At: at})
	if err != nil {
		t.Fatal(err)
	}
	winner, err := store.Write(ctx, accessDoc(adminBinding("group:corp:ops")), base, artifacts.AccessWrite{At: at})
	if err != nil {
		t.Fatal(err)
	}
	// A second writer that read base loses the swap.
	_, err = store.Write(ctx, accessDoc(adminBinding("group:corp:lost")), base, artifacts.AccessWrite{At: at})
	if !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a write on a stale etag = %v; want ErrPreconditionFailed", err)
	}
	head, err := store.Get(ctx)
	if err != nil || head.Meta.SHA256 != winner.Meta.SHA256 {
		t.Fatalf("the head after a lost swap = %+v, %v; want the winner's", head, err)
	}
	// Its revision was written before the swap, and is an orphan: two
	// revision-2 objects exist, and the head links to base.
	revisions := 0
	for _, k := range cas.Keys() {
		if strings.HasPrefix(k, artifacts.AccessRevisionsPrefix+"000000000002-") {
			revisions++
		}
	}
	if revisions != 2 || head.Meta.PreviousSHA256 != base.Meta.SHA256 {
		t.Fatalf("revision-2 objects = %d (want the winner's and the orphan), head links to %s", revisions, head.Meta.PreviousSHA256)
	}
	// The retry from a fresh read succeeds.
	if _, err := store.Write(ctx, accessDoc(adminBinding("group:corp:lost")), head, artifacts.AccessWrite{At: at}); err != nil {
		t.Fatalf("the retry from a fresh read: %v", err)
	}
}

func TestAccessStore_RevisionFailureAbortsTheChange(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	base, err := store.Write(ctx, accessDoc(adminBinding("group:corp:sre")), nil, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	cas.SetFailure(errors.New("store down"))
	if _, err := store.Write(ctx, accessDoc(adminBinding("group:corp:ops")), base, artifacts.AccessWrite{At: time.Now()}); err == nil {
		t.Fatal("a write with the store down succeeded")
	}
	cas.SetFailure(nil)
	head, err := store.Get(ctx)
	if err != nil || head.Meta.SHA256 != base.Meta.SHA256 {
		t.Fatalf("the head moved without its revision: %+v, %v", head, err)
	}
}

func TestAccessStore_Revalidate(t *testing.T) {
	ctx := context.Background()
	store := artifacts.NewMemoryAccessStore()
	head, err := store.Write(ctx, accessDoc(adminBinding("group:corp:sre")), nil, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	got, changed, err := store.Revalidate(ctx, head.ETag)
	if err != nil || changed || got != nil {
		t.Fatalf("Revalidate on the current etag = %v, %v, %v; want unchanged", got, changed, err)
	}
	next, err := store.Write(ctx, accessDoc(adminBinding("group:corp:ops")), head, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	got, changed, err = store.Revalidate(ctx, head.ETag)
	if err != nil || !changed || got.Meta.SHA256 != next.Meta.SHA256 {
		t.Fatalf("Revalidate after a write = %+v, %v, %v; want the new head", got, changed, err)
	}
}

func TestAccessStore_WrittenAroundTheAPIHasNoRevision(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	body, err := artifacts.EncodeAccessObject(accessDoc(adminBinding("group:corp:x")), artifacts.AccessRevisionMeta{Revision: 5, WrittenAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	cas.PutRaw(artifacts.AccessDocumentKey, body)
	head, err := store.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := store.HasRevision(ctx, head.Meta); err != nil || ok {
		t.Fatalf("a head written around the API has a revision: %v, %v", ok, err)
	}
}

func TestDecodeAccessObject_Refuses(t *testing.T) {
	good, err := artifacts.EncodeAccessObject(accessDoc(adminBinding("group:corp:x")), artifacts.AccessRevisionMeta{Revision: 3, WrittenAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := artifacts.DecodeAccessObject(good); err != nil {
		t.Fatalf("a good object: %v", err)
	}
	cases := map[string]string{
		"an unknown format":     strings.Replace(string(good), `"format_version": 1`, `"format_version": 2`, 1),
		"a duplicate key":       strings.Replace(string(good), `"format_version": 1,`, `"format_version": 1, "format_version": 1,`, 1),
		"an unknown member":     strings.Replace(string(good), `"format_version": 1,`, `"format_version": 1, "extra": 1,`, 1),
		"a revision mismatch":   strings.Replace(string(good), `"revision": 3,`, `"revision": 4,`, 1),
		"an unknown doc field":  strings.Replace(string(good), `"schemaVersion"`, `"denyAll": true, "schemaVersion"`, 1),
		"not JSON":              "nope",
		"a stored etag":         strings.Replace(string(good), `"schemaVersion"`, `"etag": "x", "schemaVersion"`, 1),
		"a bad previous digest": strings.Replace(string(good), `"revision": 3,`, `"previous_sha256": "zz", "revision": 3,`, 1),
	}
	for name, body := range cases {
		if body == string(good) {
			t.Fatalf("%s: the case did not change the object", name)
		}
		if _, _, err := artifacts.DecodeAccessObject([]byte(body)); !errors.Is(err, artifacts.ErrAccessUnreadable) {
			t.Errorf("%s: Decode = %v; want ErrAccessUnreadable", name, err)
		}
	}
}

// FuzzAccessDocument: a stored object that decodes and compiles re-encodes
// to an object that decodes to the same document and metadata, and every
// reference in it resolves (Compile). An unknown format is refused, never
// guessed.
func FuzzAccessDocument(f *testing.F) {
	seed, _ := artifacts.EncodeAccessObject(accessDoc(adminBinding("group:corp:x"),
		&orkstackv1.Binding{Name: "devs", Role: "developer", Scope: "namespace:payments", Subjects: []string{"claims:github:repository=acme/x"}}),
		artifacts.AccessRevisionMeta{Revision: 2, PreviousSHA256: strings.Repeat("a", 64), WrittenAt: time.Unix(1, 0), WrittenBy: "user:corp:a", Change: "x"})
	f.Add(seed)
	f.Add([]byte(`{"format_version":2}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		doc, meta, err := artifacts.DecodeAccessObject(body)
		if err != nil {
			return
		}
		if _, err := authz.Compile(doc, nil, time.Time{}); err != nil {
			return
		}
		again, err := artifacts.EncodeAccessObject(doc, meta)
		if err != nil {
			t.Fatalf("a decoded document does not encode: %v", err)
		}
		doc2, meta2, err := artifacts.DecodeAccessObject(again)
		if err != nil {
			t.Fatalf("a re-encoded document does not decode: %v", err)
		}
		if !proto.Equal(doc, doc2) || meta.Revision != meta2.Revision || meta.PreviousSHA256 != meta2.PreviousSHA256 ||
			!meta.WrittenAt.Equal(meta2.WrittenAt) || meta.WrittenBy != meta2.WrittenBy || meta.Change != meta2.Change {
			t.Fatalf("the document changed in a round trip:\n%v\n%v", doc, doc2)
		}
	})
}

func TestModeStore(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewModeStoreOn(cas)
	if _, _, err := store.Get(ctx); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("no record = %v; want ErrNotFound", err)
	}
	at := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	etag, err := store.Put(ctx, artifacts.ModeRecord{Mode: artifacts.AuthModeObserve, RaisedAt: at, UpdatedAt: at}, artifacts.PutConditions{IfAbsent: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, artifacts.ModeRecord{Mode: artifacts.AuthModeEnforce, UpdatedAt: at}, artifacts.PutConditions{IfAbsent: true}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a second first write = %v; want ErrPreconditionFailed", err)
	}
	rec, got, err := store.Get(ctx)
	if err != nil || got != etag || rec.Mode != artifacts.AuthModeObserve || !rec.RaisedAt.Equal(at) {
		t.Fatalf("Get = %+v, %q, %v", rec, got, err)
	}
	cas.PutRaw(artifacts.ModeRecordKey, []byte(`{"format_version":1,"mode":"paranoid","raised_at":"2026-11-13T00:00:00Z","updated_at":"2026-11-13T00:00:00Z"}`))
	if _, etag, err := store.Get(ctx); !errors.Is(err, artifacts.ErrModeRecordUnreadable) || etag == "" {
		t.Fatalf("an unknown mode = %v (etag %q); want ErrModeRecordUnreadable with the etag read", err, etag)
	}
}

// FuzzModeRecord: an unknown format or mode is refused, never guessed; an
// accepted record round-trips.
func FuzzModeRecord(f *testing.F) {
	seed, _ := artifacts.EncodeModeRecord(artifacts.ModeRecord{Mode: artifacts.AuthModeEnforce, RaisedAt: time.Unix(1, 0), UpdatedAt: time.Unix(2, 0)})
	f.Add(seed)
	f.Add([]byte(`{"format_version":2,"mode":"enforce"}`))
	f.Fuzz(func(t *testing.T, body []byte) {
		r, err := artifacts.DecodeModeRecord(body)
		if err != nil {
			return
		}
		if r.FormatVersion != artifacts.ModeRecordFormat || artifacts.AuthModeRank(r.Mode) == 0 {
			t.Fatalf("accepted %+v", r)
		}
		again, err := artifacts.EncodeModeRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		r2, err := artifacts.DecodeModeRecord(again)
		if err != nil || r2.Mode != r.Mode || !r2.RaisedAt.Equal(r.RaisedAt) || !r2.LeftEnforceAt.Equal(r.LeftEnforceAt) {
			t.Fatalf("round trip: %+v, %v", r2, err)
		}
	})
}

// TestAccessStore_WriteRefusesAPreviousHeadWithNoETag: a write after a head
// read with no etag (a revision, say) would be an unconditional put.
func TestAccessStore_WriteRefusesAPreviousHeadWithNoETag(t *testing.T) {
	ctx := context.Background()
	store := artifacts.NewMemoryAccessStore()
	head, err := store.Write(ctx, accessDoc(adminBinding("group:corp:sre")), nil, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.GetRevision(ctx, head.Meta.Revision, head.Meta.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(ctx, accessDoc(adminBinding("group:corp:x")), rev, artifacts.AccessWrite{At: time.Now()}); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a write after a head with no etag = %v; want ErrPreconditionFailed", err)
	}
}
