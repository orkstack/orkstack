package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func recordedCommitKey(t *testing.T) string {
	t.Helper()
	key, err := CommitKey("ns", "ork-1", "build")
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// TestRecordCommit_TheEngineWritesItCreateOnlyUnderTheAttestedPrefix: the
// first write creates the record at the commit key's attested twin; a
// second, of the same bytes or of another attempt's, creates nothing and
// answers the record already there, byte for byte.
func TestRecordCommit_TheEngineWritesItCreateOnlyUnderTheAttestedPrefix(t *testing.T) {
	s := newFastRecordStore(t)
	ctx := context.Background()
	key := recordedCommitKey(t)
	first := sampleCommit()
	got, err := s.RecordCommit(ctx, key, first)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Created || got.Key != "metadata/attested/ns/ork-1/build/commit.json" {
		t.Fatalf("first write = created %v at %q", got.Created, got.Key)
	}
	if got.Record.SHA256 == "" || got.Record.Commit.GetRunId() != first.GetRunId() {
		t.Fatalf("first record = %+v", got.Record)
	}
	again, err := s.RecordCommit(ctx, key, first)
	if err != nil || again.Created || again.Record.SHA256 != got.Record.SHA256 {
		t.Fatalf("a repeat = %+v, %v; want the first record, not created", again, err)
	}
	other := sampleCommit()
	other.Attempt = first.GetAttempt() + 1
	lost, err := s.RecordCommit(ctx, key, other)
	if err != nil || lost.Created || lost.Record.SHA256 != got.Record.SHA256 || lost.Record.Commit.GetAttempt() != first.GetAttempt() {
		t.Fatalf("another attempt's write = %+v, %v; want the first record", lost, err)
	}
}

// TestRecordCommit_AnItemAndAScopeKeepTheirSegments: the attested key is
// the commit key's own segments, an item's included.
func TestRecordCommit_AnItemAndAScopeKeepTheirSegments(t *testing.T) {
	s := newFastRecordStore(t)
	key, err := ItemCommitKey("ns", "ork-1", "convert", 3)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.RecordCommit(context.Background(), key, sampleCommit())
	if err != nil || got.Key != "metadata/attested/ns/ork-1/convert/00003/commit.json" {
		t.Fatalf("item record = %q, %v", got.Key, err)
	}
}

// TestRecordCommit_Refusals: a key that is not a commit key, a commit over
// the commit's own bound and an empty commit write nothing; a record
// already there that does not decode is never taken for a result; a store
// that does not answer is unanswered, never "created".
func TestRecordCommit_Refusals(t *testing.T) {
	ctx := context.Background()
	t.Run("not a commit key", func(t *testing.T) {
		s := newFastRecordStore(t)
		for _, key := range []string{"metadata/runs/ns/ork-1/build/commit.json", "metadata/attested/ns/ork-1/build/commit.json",
			"metadata/commits/ns/ork-1/build/values.json", "metadata/commits/../x/build/commit.json", ""} {
			if _, err := s.RecordCommit(ctx, key, sampleCommit()); !errors.Is(err, ErrInvalidRecordKey) {
				t.Errorf("RecordCommit(%q) = %v, want ErrInvalidRecordKey", key, err)
			}
		}
	})
	t.Run("too large", func(t *testing.T) {
		s := newFastRecordStore(t)
		c := sampleCommit()
		c.OrkflowName = strings.Repeat("x", CommitMaxBytes)
		if _, err := s.RecordCommit(ctx, recordedCommitKey(t), c); !errors.Is(err, ErrCommitTooLarge) {
			t.Fatalf("an oversized commit = %v, want ErrCommitTooLarge", err)
		}
	})
	t.Run("no commit", func(t *testing.T) {
		s := newFastRecordStore(t)
		if _, err := s.RecordCommit(ctx, recordedCommitKey(t), nil); err == nil {
			t.Fatal("a nil commit was recorded")
		}
	})
	t.Run("an undecodable record there", func(t *testing.T) {
		s := newFastRecordStore(t)
		key := recordedCommitKey(t)
		attested, _ := AttestedCommitKey(key)
		if err := s.b.create(ctx, attested, "application/json", []byte("{torn")); err != nil {
			t.Fatal(err)
		}
		if got, err := s.RecordCommit(ctx, key, sampleCommit()); !errors.Is(err, ErrCommitUndecodable) || got.Created {
			t.Fatalf("over a torn record = %+v, %v; want ErrCommitUndecodable", got, err)
		}
	})
	t.Run("the store does not answer", func(t *testing.T) {
		s, down := newFastRecordStoreDown(t)
		down.down = true
		got, err := s.RecordCommit(ctx, recordedCommitKey(t), sampleCommit())
		var unanswered *RecordStoreUnanswered
		if !errors.As(err, &unanswered) || got.Created {
			t.Fatalf("a store down = %+v, %v; want *RecordStoreUnanswered", got, err)
		}
	})
}

// TestCommitStore_ReadsAnAttestedRecordAndNeverCreatesOne: an Orker's
// commit store reads a record under the attested prefix, where it adopts
// and stages from, and refuses to create one there, where only an engine
// writes.
func TestCommitStore_ReadsAnAttestedRecordAndNeverCreatesOne(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	records := newRecordStore(mustLocalBlobs(t, root))
	key := recordedCommitKey(t)
	written, err := records.RecordCommit(ctx, key, sampleCommit())
	if err != nil {
		t.Fatal(err)
	}
	commitRoot, err := CommitRoot(Config{Backend: BackendLocal, LocalDir: root})
	if err != nil {
		t.Fatal(err)
	}
	uri := strings.TrimSuffix(commitRoot, "/") + "/" + written.Key
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, uri)
	if err != nil || got.SHA256 != written.Record.SHA256 {
		t.Fatalf("Get(attested) = %+v, %v", got, err)
	}
	if _, err := store.Create(ctx, uri, sampleCommit()); !errors.Is(err, ErrInvalidURI) {
		t.Fatalf("Create(attested) = %v, want ErrInvalidURI", err)
	}
	mem := NewMemoryCommitStore()
	if _, err := mem.Create(ctx, uri, &orkstackv1.ActivityCommit{SchemaVersion: 1}); !errors.Is(err, ErrInvalidURI) {
		t.Fatalf("memory Create(attested) = %v, want ErrInvalidURI", err)
	}
}

func mustLocalBlobs(t *testing.T, root string) blobs {
	t.Helper()
	b, err := openLocalBlobs("test", root)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
