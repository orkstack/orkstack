package artifacts

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func reclaimArchive(t *testing.T) (*LocalRunArchive, string) {
	t.Helper()
	root := t.TempDir()
	store, err := NewLocalRunArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, root
}

func writeRaw(t *testing.T, root, key string, body []byte) {
	t.Helper()
	name := filepath.Join(root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(name), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func rawExists(t *testing.T, root, key string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(root, filepath.FromSlash(key)))
	if err == nil {
		return true
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return false
}

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestProducingRunFromObjectKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
		ok   bool
	}{
		{"s3 layout", "orkflows/flow/ork-abc/task/out.txt/" + testDigest, "ork-abc", true},
		{"legacy run id", "orkflows/flow/ork-1755302400123456789-1/t/n/" + testDigest, "ork-1755302400123456789-1", true},
		{"unattributed", "orkflows/flow/_unattributed/t/n/" + testDigest, "", false},
		{"sanitized run segment", "orkflows/flow/ork~0123456789abcdef/t/n/" + testDigest, "", false},
		{"local layout", "objects/name/" + testDigest, "", false},
		{"short", "orkflows/flow/ork-abc/" + testDigest, "", false},
		{"not a digest", "orkflows/flow/ork-abc/t/n/xyz", "", false},
		{"dot segment", "orkflows/flow/../t/n/" + testDigest, "", false},
		{"empty", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ProducingRunFromObjectKey(tt.key)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("ProducingRunFromObjectKey(%q) = %q, %v; want %q, %v", tt.key, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func FuzzProducingRunFromObjectKey(f *testing.F) {
	for _, seed := range []string{
		"orkflows/flow/ork-abc/task/out.txt/" + testDigest,
		"orkflows/flow/_unattributed/t/n/" + testDigest,
		"orkflows/flow/ork~0123456789abcdef/t/n/" + testDigest,
		"objects/name/" + testDigest, "", "orkflows/////",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		run, ok := ProducingRunFromObjectKey(key)
		if !ok {
			if run != "" {
				t.Fatalf("not ok but run %q", run)
			}
			return
		}
		// A producer is only ever the exact, clean run segment of an s3
		// object key: rendering an object for that run puts it back there.
		segments, valid := splitS3ObjectKey(key)
		if !valid || segments[2] != run || !isCleanS3Component(run) || run == s3UnattributedComponent {
			t.Fatalf("ProducingRunFromObjectKey(%q) = %q from a key that does not carry it", key, run)
		}
	})
}

func TestPutRun_ListIsAUnion(t *testing.T) {
	store, _ := reclaimArchive(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	first := RunRecord{RunID: "ork-run", Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: at,
		JSONL: []byte("one\n"), ObjectIDs: []string{objectKey("a", testDigest), objectKey("b", testDigest)}}
	if err := store.PutRun(ctx, first); err != nil {
		t.Fatal(err)
	}
	// A second write naming fewer objects (an older engine, a re-archive) must
	// never shrink the list: the list is what delegation reads.
	second := first
	second.ObjectIDs = []string{objectKey("c", testDigest)}
	if err := store.PutRun(ctx, second); err != nil {
		t.Fatal(err)
	}
	got, err := store.RunObjectList(ctx, "ork-run")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{objectKey("a", testDigest), objectKey("b", testDigest), objectKey("c", testDigest)}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("list = %v, want %v", got, want)
	}
	for _, id := range want {
		refs, err := store.ListObjectReferences(ctx, id)
		if err != nil || !slices.Equal(refs, []string{"ork-run"}) {
			t.Fatalf("markers of %s = %v, %v", id, refs, err)
		}
	}
}

func TestPutRun_UnreadableListFailsTheWrite(t *testing.T) {
	store, root := reclaimArchive(t)
	ctx := context.Background()
	listKey, _ := RunObjectListKey("ork-run")
	writeRaw(t, root, listKey, []byte("{not a list"))
	rec := RunRecord{RunID: "ork-run", Namespace: "default", Orkflow: "flow", Status: "succeeded",
		Time: time.Now(), JSONL: []byte("x\n"), ObjectIDs: []string{objectKey("a", testDigest)}}
	if err := store.PutRun(ctx, rec); err == nil {
		t.Fatal("PutRun replaced a list it could not read")
	}
}

func TestRetireRunArtifacts_KeepsReferences(t *testing.T) {
	for _, keep := range []bool{true, false} {
		t.Run(map[bool]string{true: "keep", false: "release"}[keep], func(t *testing.T) {
			store, root := reclaimArchive(t)
			ctx := context.Background()
			obj := objectKey("a", testDigest)
			rec := RunRecord{RunID: "ork-run", Namespace: "default", Orkflow: "flow", Status: "succeeded",
				Time: time.Now(), JSONL: []byte("x\n"), ObjectIDs: []string{obj}}
			if err := store.PutRun(ctx, rec); err != nil {
				t.Fatal(err)
			}
			commits, _ := RunCommitPrefix("default", "ork-run")
			writeRaw(t, root, commits+"task/"+CommitObjectName, []byte("commit"))
			if _, err := store.RetireRunArtifacts(ctx, "default", "ork-run", keep); err != nil {
				t.Fatal(err)
			}
			if rawExists(t, root, commits+"task/"+CommitObjectName) {
				t.Fatal("commit survived retirement")
			}
			marker, _ := RunObjectRefKey(obj, "ork-run")
			listKey, _ := RunObjectListKey("ork-run")
			if rawExists(t, root, marker) != keep || rawExists(t, root, listKey) != keep {
				t.Fatalf("keep=%v: marker %v list %v", keep, rawExists(t, root, marker), rawExists(t, root, listKey))
			}
		})
	}
}

// TestCleanupSidecar_ReportsTheLatestInstantOfTheStoreStamp: a whole-second
// stamp (S3 truncates) is read as the last instant of its second, so the GC
// never reads a retirement as older than it is.
func TestCleanupSidecar_ReportsTheLatestInstantOfTheStoreStamp(t *testing.T) {
	store, root := reclaimArchive(t)
	ctx := context.Background()
	if _, _, err := store.CleanupSidecar(ctx, "ork-run"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing sidecar: %v", err)
	}
	if err := store.MarkArtifactsCleaned(ctx, "ork-run", time.Now()); err != nil {
		t.Fatal(err)
	}
	key, _ := RunCleanupKey("ork-run")
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(key)), stamp, stamp); err != nil {
		t.Fatal(err)
	}
	state, modified, err := store.CleanupSidecar(ctx, "ork-run")
	if err != nil || !state.ArtifactsCleaned || state.RunID != "ork-run" {
		t.Fatalf("state = %+v, %v", state, err)
	}
	if want := stamp.Add(time.Second - time.Nanosecond); !modified.Equal(want) {
		t.Fatalf("modified = %v, want the end of the store's second %v", modified, want)
	}
}

func TestCleanupSidecar_ForeignRunIDIsAnError(t *testing.T) {
	store, root := reclaimArchive(t)
	key, _ := RunCleanupKey("ork-run")
	body, _ := json.Marshal(RunCleanupState{RunID: "ork-other", ArtifactsCleaned: true})
	writeRaw(t, root, key, body)
	if _, _, err := store.CleanupSidecar(context.Background(), "ork-run"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("a sidecar naming another run was accepted: %v", err)
	}
}

// TestStoreNow_IsALowerBoundOfTheStoresClock: on one shared clock (the
// local backend), the GC's store-now is never ahead of it, and at most the
// probe's one-second guard and its round trip behind.
func TestStoreNow_IsALowerBoundOfTheStoresClock(t *testing.T) {
	store, root := reclaimArchive(t)
	start := time.Now()
	now, err := store.StoreNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	end := time.Now()
	if now.After(end) {
		t.Fatalf("store now %v is ahead of the clock it was read from (%v)", now, end)
	}
	if floor := start.Add(-time.Second - 100*time.Millisecond); now.Before(floor) {
		t.Fatalf("store now %v is more than the guard behind the clock (%v)", now, floor)
	}
	if !rawExists(t, root, StoreClockKey) {
		t.Fatal("no clock probe object")
	}
}

func TestListUnder_BoundsAndTruncation(t *testing.T) {
	store, root := reclaimArchive(t)
	for _, k := range []string{"p/a", "p/b", "p/c", "q/a"} {
		writeRaw(t, root, k, []byte(k))
	}
	ctx := context.Background()
	got, more, err := store.ListUnder(ctx, "p/", 2)
	if err != nil || !more || len(got) != 2 || got[0].Key != "p/a" || got[1].Key != "p/b" {
		t.Fatalf("bounded = %+v more=%v err=%v", got, more, err)
	}
	got, more, err = store.ListUnder(ctx, "p/", 3)
	if err != nil || more || len(got) != 3 {
		t.Fatalf("exact = %+v more=%v err=%v", got, more, err)
	}
	got, more, err = store.ListUnder(ctx, "z/", 1)
	if err != nil || more || len(got) != 0 {
		t.Fatalf("empty = %+v more=%v err=%v", got, more, err)
	}
	if _, _, err := store.ListUnder(ctx, "p/", 0); err == nil {
		t.Fatal("an unbounded listing was accepted")
	}
}

// ListUnderAfter resumes after a key, exclusively, and refuses a resume key
// outside the prefix it lists.
func TestListUnderAfter_ResumesAfterTheKey(t *testing.T) {
	store, root := reclaimArchive(t)
	for _, k := range []string{"p/a", "p/b", "p/c", "q/a"} {
		writeRaw(t, root, k, []byte(k))
	}
	ctx := context.Background()
	got, more, err := store.ListUnderAfter(ctx, "p/", "p/a", 1)
	if err != nil || !more || len(got) != 1 || got[0].Key != "p/b" {
		t.Fatalf("after p/a = %+v more=%v err=%v; want [p/b], more", got, more, err)
	}
	got, more, err = store.ListUnderAfter(ctx, "p/", "p/b", 5)
	if err != nil || more || len(got) != 1 || got[0].Key != "p/c" {
		t.Fatalf("after p/b = %+v more=%v err=%v; want [p/c], no more", got, more, err)
	}
	if _, _, err := store.ListUnderAfter(ctx, "p/", "q/a", 5); err == nil {
		t.Fatal("a resume key outside the prefix was accepted")
	}
}

// A cleanup record's key gives back the run id it was written for, and no
// other key names a run.
func TestRunIDFromCleanupKey(t *testing.T) {
	for _, id := range []string{"ork-01jq3k6m8b7x2t5v9c4n1r0hzd", "ork-1786856120616785000-1", "a run/with odd bytes"} {
		key, err := RunCleanupKey(id)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := RunIDFromCleanupKey(key); !ok || got != id {
			t.Errorf("RunIDFromCleanupKey(%q) = %q, %v; want %q", key, got, ok, id)
		}
	}
	for _, key := range []string{"", RunCleanupNamespace + "/", RunCleanupNamespace + "/ork-1", RunCleanupNamespace + "/a/b.json",
		RunCleanupNamespace + "/.json", "metadata/runs/ork-1.json", RunCleanupNamespace + "x/ork-1.json"} {
		if got, ok := RunIDFromCleanupKey(key); ok {
			t.Errorf("RunIDFromCleanupKey(%q) = %q; want no run", key, got)
		}
	}
}

// FuzzRunIDFromCleanupKey holds the parse of a key listed back from the store
// to what RunCleanupKey writes: a key it accepts is exactly the key its run
// id renders.
func FuzzRunIDFromCleanupKey(f *testing.F) {
	for _, seed := range []string{RunCleanupNamespace + "/ork-1.json", RunCleanupNamespace + "/a/b.json", RunCleanupNamespace + "/.json", "", "metadata/runs/ork-1.json"} {
		f.Add(seed)
	}
	if key, err := RunCleanupKey("a run/with odd bytes"); err == nil {
		f.Add(key)
	}
	f.Fuzz(func(t *testing.T, key string) {
		id, ok := RunIDFromCleanupKey(key)
		if !ok {
			return
		}
		again, err := RunCleanupKey(id)
		if err != nil || again != key {
			t.Fatalf("RunIDFromCleanupKey(%q) = %q, which renders %q (%v)", key, id, again, err)
		}
	})
}

func TestReclaimRecord_CreateOnlyAndRoundTrip(t *testing.T) {
	store, _ := reclaimArchive(t)
	ctx := context.Background()
	if _, err := store.ReclaimRecord(ctx, "ork-run"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing record: %v", err)
	}
	first := ReclaimRecord{RunID: "ork-run", ReclaimedAt: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		ObjectsDeleted: 3, Kept: []string{"orkflows/f/ork-run/t/n/" + testDigest}}
	if err := store.CreateReclaimRecord(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ObjectsDeleted = 99
	if err := store.CreateReclaimRecord(ctx, second); err != nil {
		t.Fatalf("a second create must be a no-op: %v", err)
	}
	got, err := store.ReclaimRecord(ctx, "ork-run")
	if err != nil || got.ObjectsDeleted != 3 || !slices.Equal(got.Kept, first.Kept) || !got.ReclaimedAt.Equal(first.ReclaimedAt) {
		t.Fatalf("record = %+v, %v", got, err)
	}
	var ids []string
	if err := store.EachReclaimRecord(ctx, func(runID string) error { ids = append(ids, runID); return nil }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids, []string{"ork-run"}) {
		t.Fatalf("listed %v", ids)
	}
}

func FuzzDecodeReclaimRecord(f *testing.F) {
	for _, seed := range []string{
		`{"run_id":"ork-run","reclaimed_at":"2026-11-01T00:00:00Z","objects_deleted":3}`,
		`{"run_id":"","late_commit":true}`, `{}`, `[]`, `null`, `{"run_id":"ork-run","kept":[1]}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		rec, err := decodeReclaimRecord("ork-run", body)
		if err != nil {
			return
		}
		// A decoded record is always this run's, never another's.
		if rec.RunID != "ork-run" {
			t.Fatalf("decoded a record of %q", rec.RunID)
		}
	})
}

func TestReclaimObject_RemovesObjectCompanionAndMarkers(t *testing.T) {
	d := newS3Double(t)
	store := s3CleanupArchive(t, d)
	ctx := context.Background()
	// A sanitized name has a name manifest beside its object.
	obj := "orkflows/flow/ork-run/task/odd~0123456789abcdef/" + testDigest
	d.mu.Lock()
	d.objects[obj] = []byte("bytes")
	d.objects[s3ManifestKey(obj)] = []byte("manifest")
	d.mu.Unlock()
	rec := RunRecord{RunID: "ork-run", Namespace: "default", Orkflow: "flow", Status: "succeeded",
		Time: time.Now(), JSONL: []byte("x\n"), ObjectIDs: []string{obj}}
	if err := store.PutRun(ctx, rec); err != nil {
		t.Fatal(err)
	}
	info, err := store.StatObject(ctx, obj)
	if err != nil || info.Size != 5 {
		t.Fatalf("stat = %+v, %v", info, err)
	}
	if err := store.RemoveReclaimedObject(ctx, obj); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	_, objLeft := d.objects[obj]
	_, manifestLeft := d.objects[s3ManifestKey(obj)]
	d.mu.Unlock()
	if objLeft || manifestLeft {
		t.Fatal("object or its name manifest survived")
	}
	if _, err := store.StatObject(ctx, obj); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stat after delete: %v", err)
	}
	if err := store.RemoveReclaimedObject(ctx, obj); err != nil {
		t.Fatalf("a repeated delete is not a no-op: %v", err)
	}
	if err := store.RemoveObjectReferences(ctx, obj, []string{"ork-run"}); err != nil {
		t.Fatal(err)
	}
	refs, err := store.ListObjectReferences(ctx, obj)
	if err != nil || len(refs) != 0 {
		t.Fatalf("markers after removal: %v, %v", refs, err)
	}
	if err := store.RemoveRunObjectList(ctx, "ork-run"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RunObjectList(ctx, "ork-run"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("list after removal: %v", err)
	}
}

func TestReclaimObject_RefusesANonCanonicalID(t *testing.T) {
	local, _ := reclaimArchive(t)
	if err := local.RemoveReclaimedObject(context.Background(), objectKey("a", testDigest)); err == nil {
		t.Fatal("removed a local-layout object")
	}
	store := s3CleanupArchive(t, newS3Double(t))
	for _, id := range []string{"metadata/runs/x.jsonl", "", "objects/../x/" + testDigest, objectKey("a", testDigest),
		"orkflows/flow/_unattributed/t/n/" + testDigest} {
		if err := store.RemoveReclaimedObject(context.Background(), id); err == nil {
			t.Fatalf("removed non-canonical %q", id)
		}
	}
}
