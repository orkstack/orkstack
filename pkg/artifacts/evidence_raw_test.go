package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
)

// A1: a commit record carries the exact bytes its SHA256 is the digest of, so
// a reader that must copy the record verbatim (run evidence) never has to
// re-marshal it.
func TestCommitRecord_RawIsTheBytesSHA256Digests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	local, err := NewLocalCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	localURI := "file://" + filepath.ToSlash(filepath.Join(dir, "metadata", "commits", "payments", "run", "migrate", CommitObjectName))
	memory := NewMemoryCommitStore()
	memoryURI := "mem://commits/metadata/commits/payments/run/migrate/commit.json"

	cases := []struct {
		name   string
		store  CommitStore
		uri    string
		stored func() []byte
	}{
		{"memory", memory, memoryURI, func() []byte {
			memory.mu.Lock()
			defer memory.mu.Unlock()
			return bytes.Clone(memory.objects[memoryURI])
		}},
		{"local", local, localURI, func() []byte {
			b, err := os.ReadFile(filepath.Join(dir, "metadata", "commits", "payments", "run", "migrate", CommitObjectName))
			if err != nil {
				t.Fatal(err)
			}
			return b
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			created, err := tc.store.Create(ctx, tc.uri, sampleCommit())
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			got, err := tc.store.Get(ctx, tc.uri)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			stored := tc.stored()
			for label, rec := range map[string]CommitRecord{"create": created, "get": got} {
				sum := sha256.Sum256(rec.Raw)
				if hex.EncodeToString(sum[:]) != rec.SHA256 {
					t.Errorf("%s: sha256(Raw) = %x, SHA256 = %s", label, sum, rec.SHA256)
				}
				if int64(len(rec.Raw)) != rec.Bytes {
					t.Errorf("%s: len(Raw) = %d, Bytes = %d", label, len(rec.Raw), rec.Bytes)
				}
				if !bytes.Equal(rec.Raw, stored) {
					t.Errorf("%s: Raw differs from the stored bytes", label)
				}
			}
		})
	}

	t.Run("undecodable", func(t *testing.T) {
		uri := "mem://commits/metadata/commits/payments/run/torn/commit.json"
		memory.PutRaw(uri, []byte("{not json"))
		rec, err := memory.Get(ctx, uri)
		if !errors.Is(err, ErrCommitUndecodable) {
			t.Fatalf("Get undecodable = %v, want ErrCommitUndecodable", err)
		}
		if rec.Raw != nil {
			t.Fatalf("a record returned with an error carries Raw %q, want nil", rec.Raw)
		}
	})
}

// A2: the raw half of the attempt store carries each record's stored bytes,
// and ListAttempts is exactly its projection.
func TestAttemptStore_ReadAttemptDocumentsCarriesTheStoredBytes(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	memory := NewMemoryAttemptStore()

	type store interface {
		AttemptStore
		AttemptDocumentReader
	}
	cases := []struct {
		name  string
		store store
		plant func(key string, document []byte)
	}{
		{"memory", memory, func(key string, document []byte) {
			memory.mu.Lock()
			defer memory.mu.Unlock()
			memory.objects[key] = document
		}},
		{"local", local, func(key string, document []byte) {
			full := filepath.Join(local.Dir(), filepath.FromSlash(key))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, document, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Put out of order: the reader must sort.
			for _, n := range []int32{3, 1, 2} {
				if err := tc.store.PutAttempt(ctx, sampleAttempt("ork-raw-1", "build", n)); err != nil {
					t.Fatal(err)
				}
			}
			// A foreign object under the task's prefix is skipped.
			prefix, err := AttemptTaskPrefix("ork-raw-1", "build")
			if err != nil {
				t.Fatal(err)
			}
			tc.plant(prefix+"notes.txt", []byte("not a record"))

			docs, err := tc.store.ReadAttemptDocuments(ctx, "ork-raw-1", "build", NoItem)
			if err != nil {
				t.Fatalf("ReadAttemptDocuments: %v", err)
			}
			if len(docs) != 3 {
				t.Fatalf("documents = %d, want 3", len(docs))
			}
			records, err := tc.store.ListAttempts(ctx, "ork-raw-1", "build", NoItem)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != len(docs) {
				t.Fatalf("ListAttempts = %d records, documents = %d", len(records), len(docs))
			}
			for i, doc := range docs {
				want := int32(i + 1)
				if doc.Ref.Attempt != want || doc.Ref.RunID != "ork-raw-1" || doc.Ref.TaskID != "build" {
					t.Errorf("doc %d ref = %+v, want attempt %d of ork-raw-1/build", i, doc.Ref, want)
				}
				key, _ := AttemptKey(doc.Ref)
				if doc.Key != key {
					t.Errorf("doc %d key = %q, want %q", i, doc.Key, key)
				}
				_, expected, err := encodeAttemptForStore(sampleAttempt("ork-raw-1", "build", want))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(doc.Raw, expected) {
					t.Errorf("doc %d Raw differs from the stored object", i)
				}
				decoded, err := UnmarshalAttempt(doc.Raw)
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(decoded, doc.Record) {
					t.Errorf("doc %d Record is not UnmarshalAttempt(Raw)", i)
				}
				if !proto.Equal(records[i], doc.Record) {
					t.Errorf("ListAttempts[%d] is not documents[%d].Record", i, i)
				}
			}

			empty, err := tc.store.ReadAttemptDocuments(ctx, "ork-raw-1", "never-ran", NoItem)
			if err != nil || len(empty) != 0 {
				t.Fatalf("empty task = %v, %v; want empty, nil", empty, err)
			}
		})
	}
}
