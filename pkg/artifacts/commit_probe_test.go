package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestCommitProbeKeyTracksCommitSchemaVersion(t *testing.T) {
	want := fmt.Sprintf("%s/schema-v%d/conditional-create/%s", CommitProbePrefix, CommitSchemaVersion, CommitObjectName)
	if CommitProbeKey != want {
		t.Fatalf("CommitProbeKey = %q, want %q; a schema bump must move the durable probe key", CommitProbeKey, want)
	}
}

func TestProbeCommitStoreRepeatedCallsUseOneObjectOutsideCommitPrefix(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := ProbeCommitStore(context.Background(), store); err != nil {
			t.Fatalf("probe %d: %v", i+1, err)
		}
	}

	var files []string
	if err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			rel, err := filepath.Rel(root, name)
			if err != nil {
				return err
			}
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != CommitProbeKey {
		t.Fatalf("files after three probes = %v, want only %q", files, CommitProbeKey)
	}
	if strings.HasPrefix(files[0], CommitPrefix+"/") {
		t.Fatalf("probe object %q polluted completion prefix %q", files[0], CommitPrefix)
	}
}

func TestProbeCommitStoreStillRejectsBackendWithoutConditionalCreate(t *testing.T) {
	store := &nonConditionalProbeStore{}
	if err := ProbeCommitStore(context.Background(), store); err == nil {
		t.Fatal("ProbeCommitStore accepted a backend where both writers overwrite")
	}
}

type nonConditionalProbeStore struct {
	mu     sync.Mutex
	commit *orkstackv1.ActivityCommit
}

func (s *nonConditionalProbeStore) commitProbeURI() string {
	return "memory://probe/" + CommitProbeKey
}

func (s *nonConditionalProbeStore) Create(_ context.Context, _ string, commit *orkstackv1.ActivityCommit) (CommitRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commit = commit
	return CommitRecord{Commit: commit}, nil
}

func (s *nonConditionalProbeStore) Get(_ context.Context, _ string) (CommitRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commit == nil {
		return CommitRecord{}, ErrNotFound
	}
	return CommitRecord{Commit: s.commit}, nil
}

func TestLocalCommitStoreAcceptsOnlyTheFixedProbeKeyOutsideCommitPrefix(t *testing.T) {
	root := t.TempDir()
	store, err := NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	probeURI := store.commitProbeURI()
	candidate := commitProbeCandidate()
	if _, err := store.Create(context.Background(), probeURI, candidate); err != nil {
		t.Fatalf("fixed probe Create: %v", err)
	}

	bad := (&url.URL{Scheme: "file", Path: filepath.Join(root, "metadata", "probes", "other", CommitObjectName)}).String()
	if _, err := store.Create(context.Background(), bad, candidate); !errors.Is(err, ErrInvalidURI) {
		t.Fatalf("Create outside fixed probe key = %v, want ErrInvalidURI", err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(CommitProbeKey))); err != nil {
		t.Fatalf("fixed probe object: %v", err)
	}
}
