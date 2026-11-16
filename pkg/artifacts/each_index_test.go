package artifacts_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// checkGetRange is the GetRange contract every Store holds: the bytes at
// [offset, offset+length), an empty read at zero length, ErrRange past the
// end, ErrNotFound for a missing object. Shared by the local unit test and
// the S3 integration test.
func checkGetRange(t *testing.T, st artifacts.Store, prefix string) {
	t.Helper()
	ctx := context.Background()
	ref := mustPut(t, st, prefix+"/ranges/source.txt", []byte("abcdefghij"), artifacts.PutOptions{})
	read := func(offset, length int64) (string, error) {
		r, err := st.GetRange(ctx, ref.URI, offset, length)
		if err != nil {
			return "", err
		}
		defer r.Close()
		b, err := io.ReadAll(r)
		return string(b), err
	}
	for _, tc := range []struct {
		offset, length int64
		want           string
	}{
		{2, 3, "cde"},
		{0, 10, "abcdefghij"},
		{8, 2, "ij"},
		{10, 0, ""},
		{4, 0, ""},
	} {
		got, err := read(tc.offset, tc.length)
		if err != nil || got != tc.want {
			t.Errorf("GetRange(%d, %d) = %q, %v; want %q", tc.offset, tc.length, got, err, tc.want)
		}
	}
	for _, tc := range []struct{ offset, length int64 }{{9, 5}, {11, 0}, {10, 1}} {
		if _, err := read(tc.offset, tc.length); !errors.Is(err, artifacts.ErrRange) {
			t.Errorf("GetRange(%d, %d) past the end = %v, want ErrRange", tc.offset, tc.length, err)
		}
	}
	for _, tc := range []struct{ offset, length int64 }{{-1, 2}, {0, -1}, {math.MaxInt64, 10}, {math.MaxInt64 - 2, 5}} {
		if _, err := read(tc.offset, tc.length); err == nil {
			t.Errorf("GetRange(%d, %d) = nil error, want a refusal", tc.offset, tc.length)
		}
	}
	missing := strings.Replace(ref.URI, ref.SHA256, strings.Repeat("0", 64), 1)
	if _, err := st.GetRange(ctx, missing, 0, 1); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("GetRange of a missing object = %v, want ErrNotFound", err)
	}
	if _, err := st.GetRange(ctx, "gopher://nowhere/x", 0, 1); !errors.Is(err, artifacts.ErrInvalidURI) {
		t.Errorf("GetRange of a foreign URI = %v, want ErrInvalidURI", err)
	}
}

func TestLocalStore_GetRange(t *testing.T) {
	st, _ := newLocalStore(t)
	checkGetRange(t, st, "local")
}

func indexRef(task, sha string) artifacts.EachLinesIndexRef {
	return artifacts.EachLinesIndexRef{Namespace: "ml", RunID: "ork-01jx", Task: task, SourceSHA256: sha, Format: "jsonl"}
}

var sourceSHA = strings.Repeat("ab", 32)

func TestEachLinesIndexKey_LivesWithTheRunsCommits(t *testing.T) {
	key, err := artifacts.EachLinesIndexKey(indexRef("convert", sourceSHA))
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := artifacts.RunCommitPrefix("ml", "ork-01jx")
	if err != nil {
		t.Fatal(err)
	}
	commit, err := artifacts.CommitKey("ml", "ork-01jx", "convert")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key, prefix) || path.Dir(path.Dir(key)) != path.Dir(commit) {
		t.Errorf("EachLinesIndexKey = %q, want it beside the task's commit %q, under the run's prefix %q", key, commit, prefix)
	}
	if !strings.Contains(key, sourceSHA) || !strings.Contains(key, "jsonl") {
		t.Errorf("EachLinesIndexKey = %q names neither the source digest nor the format", key)
	}
	other, err := artifacts.EachLinesIndexKey(artifacts.EachLinesIndexRef{Namespace: "ml", RunID: "ork-01jx", Task: "convert", SourceSHA256: sourceSHA, Format: "lines"})
	if err != nil || other == key {
		t.Errorf("a lines index of the same source = %q, %v; want its own key", other, err)
	}
	for name, ref := range map[string]artifacts.EachLinesIndexRef{
		"short digest":   indexRef("convert", "abc"),
		"upper digest":   indexRef("convert", strings.ToUpper(sourceSHA)),
		"bad format":     {Namespace: "ml", RunID: "ork-01jx", Task: "convert", SourceSHA256: sourceSHA, Format: "csv"},
		"traversal task": indexRef("../x", sourceSHA),
		"empty run":      {Namespace: "ml", Task: "convert", SourceSHA256: sourceSHA, Format: "jsonl"},
	} {
		if k, err := artifacts.EachLinesIndexKey(ref); err == nil {
			t.Errorf("%s: EachLinesIndexKey = %q, want a refusal", name, k)
		}
	}
}

// checkEachIndexStore is the EachIndexStore contract, for the local unit test
// and the S3 integration test.
func checkEachIndexStore(t *testing.T, st *artifacts.EachIndexStore, runID string) {
	t.Helper()
	ctx := context.Background()
	body := []byte("ORKLIDX\x01 an index's bytes")
	sum := sha256.Sum256(body)
	at := func(task string) artifacts.EachLinesIndexRef {
		r := indexRef(task, sourceSHA)
		r.RunID = runID
		return r
	}
	ref := at("convert")

	got, created, err := st.CreateIfAbsent(ctx, ref, body, 1<<20)
	if err != nil || !created {
		t.Fatalf("first CreateIfAbsent = %+v, created %v, %v; want created", got, created, err)
	}
	if got.SHA256 != hex.EncodeToString(sum[:]) || got.SizeBytes != int64(len(body)) || got.URI == "" {
		t.Errorf("CreateIfAbsent ref = %+v, want the body's digest and size and a URI", got)
	}
	read, readRef, err := st.Get(ctx, got.URI, 1<<20)
	if err != nil || !bytes.Equal(read, body) || readRef.SHA256 != got.SHA256 {
		t.Fatalf("Get = %q, %+v, %v; want the bytes written", read, readRef, err)
	}

	// A second writer finds the first's index and is told what is there.
	other := []byte("a different index")
	again, created, err := st.CreateIfAbsent(ctx, ref, other, 1<<20)
	if err != nil || created || again.SHA256 != got.SHA256 || again.URI != got.URI {
		t.Errorf("CreateIfAbsent over an existing index = %+v, created %v, %v; want the existing one, not created", again, created, err)
	}

	// Concurrent writers: exactly one creates.
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, created, err := st.CreateIfAbsent(ctx, at("concurrent"), body, 1<<20)
			if err != nil {
				t.Errorf("concurrent CreateIfAbsent: %v", err)
			}
			if created {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Errorf("%d concurrent writers created the index, want exactly 1", wins)
	}

	if _, _, err := st.Get(ctx, got.URI, int64(len(body))-1); err == nil {
		t.Error("Get past its byte bound = nil error, want a refusal")
	}
	if b, _, err := st.Get(ctx, got.URI, int64(len(body))); err != nil || !bytes.Equal(b, body) {
		t.Errorf("Get at exactly its byte bound = %q, %v; want the index", b, err)
	}
	for _, bound := range []int64{0, -1, math.MaxInt64} {
		if _, _, err := st.Get(ctx, got.URI, bound); err == nil {
			t.Errorf("Get with byte bound %d = nil error, want a refusal", bound)
		}
	}
	// A body over the bound is never written, and an existing index over it
	// is never read back whole.
	if _, _, err := st.CreateIfAbsent(ctx, at("too-large"), body, int64(len(body))-1); err == nil {
		t.Error("CreateIfAbsent of a body over its bound = nil error, want a refusal")
	}
	if _, _, err := st.CreateIfAbsent(ctx, ref, []byte("x"), int64(len(body))-1); err == nil {
		t.Error("CreateIfAbsent that lost to an index over its bound = nil error, want a refusal")
	}
	missing, err := st.URI(at("never-written"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Get(ctx, missing, 1<<20); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("Get of an index never written = %v, want ErrNotFound", err)
	}
}

// TestEachIndexStore_RefusesForeignKeys: the store reads and writes only keys
// EachLinesIndexKey renders, so an index URI can never name a commit record
// or anything else in the bucket.
func TestEachIndexStore_RefusesForeignKeys(t *testing.T) {
	dir := t.TempDir()
	st, err := artifacts.NewLocalEachIndexStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	index, err := st.URI(indexRef("convert", sourceSHA))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := artifacts.CommitKey("ml", "ork-01jx", "convert")
	if err != nil {
		t.Fatal(err)
	}
	for _, uri := range []string{
		strings.Replace(index, path.Base(index), "commit.json", 1),
		strings.Replace(index, path.Dir(commit), "metadata/memo/ml", 1),
		strings.Replace(index, "jsonl", "csv", 1),
		"s3://bucket/" + commit,
		"",
	} {
		if _, _, err := st.Get(context.Background(), uri, 1<<20); !errors.Is(err, artifacts.ErrInvalidURI) {
			t.Errorf("Get(%q) = %v, want ErrInvalidURI", uri, err)
		}
	}
}

func TestLocalEachIndexStore(t *testing.T) {
	st, err := artifacts.NewLocalEachIndexStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	checkEachIndexStore(t, st, "ork-01jx")
}

// FuzzParseEachLinesIndexKey: a key read back from a URI is accepted only as
// the renderer spells it, never panics, and an accepted key renders again
// byte for byte.
func FuzzParseEachLinesIndexKey(f *testing.F) {
	key, err := artifacts.EachLinesIndexKey(indexRef("convert", sourceSHA))
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range []string{key, strings.Replace(key, "jsonl", "csv", 1), path.Dir(key) + "/commit.json",
		"metadata/commits/ml/x/y/each-lines/../../z.idx", "", "metadata/commits/"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		ref, ok := artifacts.ParseEachLinesIndexKey(key)
		if !ok {
			return
		}
		again, err := artifacts.EachLinesIndexKey(ref)
		if err != nil || again != key {
			t.Fatalf("accepted %q as %+v, which renders %q, %v", key, ref, again, err)
		}
	})
}
