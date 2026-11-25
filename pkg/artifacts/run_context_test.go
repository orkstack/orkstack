package artifacts

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
)

func int32p(v int32) *int32 { return &v }

func sampleRunContextObject() RunContextObject {
	return RunContextObject{
		Root: RunContextRoot{
			Orkflow: RunContextOrkflow{Name: "load", Version: "7", DefinitionDigest: "sha256:" + strings.Repeat("a", 64)},
			Origin: RunContextOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Reference: "https://ci/4411",
				Description: "nightly", Principal: "ci-bot"},
			OriginShared: &RunContextOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Description: "nightly"},
		},
		Entries: []RunContextEntry{
			{},
			{Parent: int32p(0), ParentTaskID: "publish", Orkflow: &RunContextOrkflow{Name: "pub", Version: "2"}, Shared: true},
		},
	}
}

func TestRunContextObject_EncodeDecodeRoundTrip(t *testing.T) {
	raw, sha, err := EncodeRunContextObject(sampleRunContextObject())
	if err != nil {
		t.Fatal(err)
	}
	if sha != hexDigest(raw) {
		t.Fatalf("sha %s does not name the bytes", sha)
	}
	got, err := DecodeRunContextObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, sha2, err := EncodeRunContextObject(got)
	if err != nil || sha2 != sha || !bytes.Equal(again, raw) {
		t.Fatalf("round trip changed the object: %v\n%s\n%s", err, raw, again)
	}
}

func TestRunContextObject_RefusesWhatIsNoObject(t *testing.T) {
	good, _, err := EncodeRunContextObject(sampleRunContextObject())
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty":            nil,
		"not json":         []byte("{"),
		"later format":     bytes.Replace(good, []byte(`"format":1`), []byte(`"format":2`), 1),
		"unknown member":   bytes.Replace(good, []byte(`{"format":1`), []byte(`{"extra":1,"format":1`), 1),
		"not canonical":    append(append([]byte{}, good[:len(good)-1]...), ' ', '\n'),
		"root entry holds": bytes.Replace(good, []byte(`"entries":[{}`), []byte(`"entries":[{"shared":true}`), 1),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRunContextObject(raw); err == nil {
				t.Fatalf("decoded %q", raw)
			}
		})
	}
}

func TestRunContextObject_EncodeRefusesAnInconsistentTree(t *testing.T) {
	cases := map[string]func(*RunContextObject){
		"no entries":               func(o *RunContextObject) { o.Entries = nil },
		"parent not before":        func(o *RunContextObject) { o.Entries[1].Parent = int32p(1) },
		"child without a parent":   func(o *RunContextObject) { o.Entries[1].Parent = nil },
		"child without a task":     func(o *RunContextObject) { o.Entries[1].ParentTaskID = "" },
		"child without an orkflow": func(o *RunContextObject) { o.Entries[1].Orkflow = nil },
		"shared child, no shared origin": func(o *RunContextObject) {
			o.Root.OriginShared = nil
		},
		"shared origin, no shared child": func(o *RunContextObject) { o.Entries[1].Shared = false },
		"shared origin names the principal": func(o *RunContextObject) {
			o.Root.OriginShared.Principal = "ci-bot"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := sampleRunContextObject()
			mutate(&o)
			if _, _, err := EncodeRunContextObject(o); err == nil {
				t.Fatal("encoded an inconsistent tree")
			}
		})
	}
}

func TestRunContextKey_IsUnderAPrefixOfItsOwn(t *testing.T) {
	sha := strings.Repeat("b", 64)
	key, err := RunContextKey("", "ork-root", sha)
	if err != nil {
		t.Fatal(err)
	}
	if want := "metadata/run-context/default/ork-root/" + sha + ".json"; key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	// Never beside a run's saved state and its admission, which an Orker's
	// credential must not reach.
	if strings.HasPrefix(key, SnapshotPrefix+"/") {
		t.Fatalf("key %q is under the snapshots", key)
	}
	if _, err := RunContextKey("default", "ork-root", "BAD"); err == nil {
		t.Fatal("a key for a non-SHA name")
	}
}

// TestRunContextStore_UnderTheDocumentedOrkerPolicy: an Orker's credential
// reads the object its task names and nothing more: it lists nothing and
// writes nothing, and a missing object, which the store then refuses as it
// refuses a forbidden one, is never read as missing.
func TestRunContextStore_UnderTheDocumentedOrkerPolicy(t *testing.T) {
	ctx := context.Background()
	f := newScopeFixture(t, s3scope.OrkerPolicy())
	engine := &RunContextStore{blobBase{&s3Blobs{client: f.double.fastClient(t), bucket: f.double.bucket}}}
	orker := &RunContextStore{blobBase{&s3Blobs{client: f.client, bucket: f.double.bucket}}}
	raw, sha, err := EncodeRunContextObject(sampleRunContextObject())
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.WriteRunContext(ctx, "default", "ork-root", raw); err != nil {
		t.Fatal(err)
	}
	if _, err := orker.ReadRunContext(ctx, "default", "ork-root", sha); err != nil {
		t.Fatalf("the Orker's own task's run context: %v", err)
	}
	_, err = orker.ReadRunContext(ctx, "default", "ork-root", strings.Repeat("1", 64))
	if !errors.Is(err, ErrRunContextRefused) || errors.Is(err, ErrRunContextMissing) {
		t.Fatalf("a missing object behind a get-only credential: %v, want ErrRunContextRefused", err)
	}
	if err := orker.WriteRunContext(ctx, "default", "ork-other", raw); err == nil {
		t.Fatal("the Orker's credential wrote a run context")
	}
	for _, r := range f.proxy.Requests() {
		if r.Verb == s3scope.List {
			t.Fatalf("reading a run context listed %q", r.Key)
		}
	}
}

// TestRetireRunArtifacts_ReclaimsTheRunContext: a retired root's run-context
// object goes with it, and another root's stays.
func TestRetireRunArtifacts_ReclaimsTheRunContext(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewLocalRunContextStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := NewLocalRunArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	raw, sha, err := EncodeRunContextObject(sampleRunContextObject())
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"ork-retired", "ork-kept"} {
		if err := store.WriteRunContext(ctx, "default", root, raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := archive.DeleteRunArtifacts(ctx, "default", "ork-retired"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadRunContext(ctx, "default", "ork-retired", sha); !errors.Is(err, ErrRunContextMissing) {
		t.Fatalf("the retired root's run context: %v, want ErrRunContextMissing", err)
	}
	if _, err := store.ReadRunContext(ctx, "default", "ork-kept", sha); err != nil {
		t.Fatalf("another root's run context: %v", err)
	}
}

func TestRunContextStore_WriteIsCreateOnlyAndReadVerifies(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalRunContextStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	raw, sha, err := EncodeRunContextObject(sampleRunContextObject())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadRunContext(ctx, "default", "ork-root", sha); !errors.Is(err, ErrRunContextMissing) {
		t.Fatalf("read before write: %v, want ErrRunContextMissing", err)
	}
	if err := store.WriteRunContext(ctx, "default", "ork-root", []byte(`{"format":1}`)); err == nil {
		t.Fatal("wrote bytes that are no run-context object")
	}
	if _, err := store.ReadRunContext(ctx, "default", "ork-root", "not-a-sha"); !errors.Is(err, ErrRunContextInvalid) {
		t.Fatalf("read of a name that is no SHA-256: %v, want ErrRunContextInvalid", err)
	}
	for range 2 { // a retried or reissued write converges
		if err := store.WriteRunContext(ctx, "default", "ork-root", raw); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.ReadRunContext(ctx, "default", "ork-root", sha)
	if err != nil {
		t.Fatal(err)
	}
	if got.Entries[1].ParentTaskID != "publish" {
		t.Fatalf("read %+v", got)
	}
	// Bytes that are not what the name says are invalid, never missing.
	key, err := RunContextKey("default", "ork-root", sha)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(key)), []byte(`{"format":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadRunContext(ctx, "default", "ork-root", sha); !errors.Is(err, ErrRunContextInvalid) {
		t.Fatalf("read of other bytes: %v, want ErrRunContextInvalid", err)
	}
	if err := store.WriteRunContext(ctx, "default", "ork-root", raw); err == nil {
		t.Fatal("a write over other bytes at the key succeeded")
	}
}

func FuzzDecodeRunContextObject(f *testing.F) {
	good, _, err := EncodeRunContextObject(sampleRunContextObject())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte(`{"format":1,"root":{"orkflow":{},"origin":{}},"entries":[{}]}`))
	f.Add([]byte(`{"format":1,"entries":[{},{"parent":0}]}`))
	f.Add([]byte("{"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		o, err := DecodeRunContextObject(raw)
		if err != nil {
			return
		}
		// Whatever decodes is canonical and consistent: it encodes back to
		// exactly its bytes.
		again, _, err := EncodeRunContextObject(o)
		if err != nil {
			t.Fatalf("decoded an object Encode refuses: %v", err)
		}
		if !bytes.Equal(again, raw) {
			t.Fatalf("decoded bytes that are not canonical:\n%q\n%q", raw, again)
		}
	})
}
