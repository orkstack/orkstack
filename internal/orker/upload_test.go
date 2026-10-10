package orker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordingPutStore records every Put's name and options.
type recordingPutStore struct {
	artifacts.Store
	mu   sync.Mutex
	puts map[string]artifacts.PutOptions
}

func (s *recordingPutStore) Put(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	s.mu.Lock()
	if s.puts == nil {
		s.puts = map[string]artifacts.PutOptions{}
	}
	s.puts[name] = opts
	s.mu.Unlock()
	return s.Store.Put(ctx, name, r, opts)
}

// TestCommit_PassesTheDigestItAlreadyComputedToThePut is
// F-orker-storage-perf-3's dispatcher half: an output artifact's Put carries
// the HashFile digest and size step 3 computed, values.json carries its own,
// and every Put names the Orker's reaped spool directory.
func TestCommit_PassesTheDigestItAlreadyComputedToThePut(t *testing.T) {
	h := newCacheHarness(t)
	recording := &recordingPutStore{Store: h.store}
	h.dispatcher.Store = recording
	in := cachedTaskInput(t, "ork-upload")
	in.Cache = nil
	if _, err := h.dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	spool := filepath.Join(h.dispatcher.WorkRoot, SpoolDirName)
	content := []byte("1\n2\n3\n")
	for name, want := range map[string]struct {
		sha  string
		size int64
	}{
		"features.txt": {HashBytes(content), int64(len(content))},
		"values.json":  {HashBytes([]byte(`{"rows":3}`)), int64(len(`{"rows":3}`))},
	} {
		opts, ok := recording.puts[name]
		if !ok {
			t.Fatalf("no Put for %s: %v", name, recording.puts)
		}
		if opts.ExpectedSHA256 != want.sha || opts.ExpectedSize != want.size {
			t.Fatalf("%s Put expected %q/%d; want %q/%d", name, opts.ExpectedSHA256, opts.ExpectedSize, want.sha, want.size)
		}
		if opts.SpoolDir != spool {
			t.Fatalf("%s Put spool dir = %q; want %q", name, opts.SpoolDir, spool)
		}
	}
	if opts := recording.puts[StdoutLogName]; opts.ExpectedSHA256 != "" || opts.SpoolDir != spool {
		t.Fatalf("stdout.log Put = %+v; want no expectation (it is hashed in one pass) and the spool dir", opts)
	}
}

// TestBoot_ReapsTheSpoolDirectory: a spool file a killed Orker left behind is
// removed at the next boot.
func TestBoot_ReapsTheSpoolDirectory(t *testing.T) {
	root := t.TempDir()
	leftover := filepath.Join(root, SpoolDirName, "ork-artifact-123")
	if err := os.MkdirAll(filepath.Dir(leftover), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, []byte("half an upload"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := NewActivitiesWithOptions(nil, Options{
		OrkerID: "orker-spool-reap", WorkRoot: root, Runtime: &reapRecorder{labelled: map[string]int{}},
	})
	t.Cleanup(a.Close)
	if err := a.Boot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("the spool leftover survived boot: %v", err)
	}
}
