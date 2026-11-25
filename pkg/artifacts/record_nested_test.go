package artifacts

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestCommitStore_ARecordIsReadOnlyAtItsOwnPrefix: an object an Orker can
// write, under metadata/commits/, is never read as an engine's record,
// whatever its key ends in and however the uri spells it.
func TestCommitStore_ARecordIsReadOnlyAtItsOwnPrefix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewLocalCommitStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const nested = "metadata/commits/ns/ork-1/x/metadata/attested/ns/ork-1/build/commit.json"
	doc, err := MarshalCommit(&orkstackv1.ActivityCommit{SchemaVersion: CommitSchemaVersion, Recorded: &orkstackv1.CommitRecording{Pool: "main"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, nested)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, nested), doc, 0o644); err != nil {
		t.Fatal(err)
	}
	root, err := CommitRoot(Config{Backend: BackendLocal, LocalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	root = strings.TrimSuffix(root, "/")
	s3Store := &S3CommitStore{bucket: "b"}
	for _, key := range []string{nested, strings.Replace(nested, "metadata/commits", "metadata%2Fcommits", 1),
		strings.Replace(nested, "metadata/attested", "metadata%2Fattested", 1)} {
		if rec, err := store.Get(ctx, root+"/"+key); !errors.Is(err, ErrInvalidURI) {
			t.Errorf("local Get(%q) = %v, %v; want ErrInvalidURI", key, rec.Commit, err)
		}
		if got, err := s3Store.commitKey("s3://b/"+key, true); !errors.Is(err, ErrInvalidURI) {
			t.Errorf("s3 commitKey(%q) = %q, %v; want ErrInvalidURI", key, got, err)
		}
	}
	for _, key := range []string{"metadata/commits/ns/ork-1/build/commit.json", "metadata/attested/ns/ork-1/build/commit.json"} {
		if got, err := s3Store.commitKey("s3://b/"+key, true); err != nil || got != key {
			t.Errorf("s3 commitKey(%q) = %q, %v", key, got, err)
		}
	}
}
