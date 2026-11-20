package artifacts

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestRunContextKey_IsUnderTheRootsSnapshotPrefix(t *testing.T) {
	sha := strings.Repeat("b", 64)
	key, err := RunContextKey("", "ork-root", sha)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := RunSnapshotPrefix("default", "ork-root")
	if err != nil {
		t.Fatal(err)
	}
	if key != prefix+"run-context-"+sha+".json" {
		t.Fatalf("key = %q", key)
	}
	if _, err := RunContextKey("default", "ork-root", "BAD"); err == nil {
		t.Fatal("a key for a non-SHA name")
	}
}

func TestRunContextStore_WriteIsCreateOnlyAndReadVerifies(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalRunSnapshotStore(root)
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
