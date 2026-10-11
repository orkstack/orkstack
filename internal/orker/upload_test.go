package orker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

// manyOutputsRuntime is a workload that writes values.json and n artifacts.
type manyOutputsRuntime struct {
	cacheRuntime
	n int
}

func (r *manyOutputsRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	for _, m := range spec.Mounts {
		if m.Destination != ContainerOutput {
			continue
		}
		if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(`{}`), 0o644); err != nil {
			return "", err
		}
		for i := 0; i < r.n; i++ {
			name := fmt.Sprintf("out-%02d.txt", i)
			if err := os.WriteFile(filepath.Join(m.Source, "artifacts", name), []byte(name), 0o644); err != nil {
				return "", err
			}
		}
	}
	return "many", nil
}

// slowPutStore delays each artifact Put by a per-name amount and fails the
// names it is told to.
type slowPutStore struct {
	artifacts.Store
	delay func(name string) time.Duration
	fail  map[string]bool
	flight
}

// flight measures how many calls are in progress at once.
type flight struct {
	mu   sync.Mutex
	now  int
	peak int
}

func (f *flight) enter() {
	f.mu.Lock()
	f.now++
	if f.now > f.peak {
		f.peak = f.now
	}
	f.mu.Unlock()
}

func (f *flight) leave() {
	f.mu.Lock()
	f.now--
	f.mu.Unlock()
}

func (f *flight) max() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

func (s *slowPutStore) Put(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	s.enter()
	defer s.leave()
	if s.delay != nil {
		time.Sleep(s.delay(name))
	}
	if s.fail[name] {
		return artifacts.ArtifactRef{}, fmt.Errorf("put %s: 503 slow down", name)
	}
	return s.Store.Put(ctx, name, r, opts)
}

func manyOutputsHarness(t *testing.T, n int) (*cacheHarness, *slowPutStore) {
	t.Helper()
	h := newCacheHarness(t)
	h.dispatcher.Runtime = &manyOutputsRuntime{n: n}
	slow := &slowPutStore{Store: h.store}
	h.dispatcher.Store = slow
	return h, slow
}

// TestCommit_UploadsAreBoundedParallelAndKeepScanOrder is
// F-orker-storage-perf-4's upload half: 24 artifacts go up eight at a time —
// never more, and more than one — and the commit lists them in scan order
// even though the later ones finish first.
func TestCommit_UploadsAreBoundedParallelAndKeepScanOrder(t *testing.T) {
	h, slow := manyOutputsHarness(t, 24)
	slow.delay = func(name string) time.Duration {
		if strings.HasPrefix(name, "out-0") {
			return 30 * time.Millisecond
		}
		return 15 * time.Millisecond
	}
	in := cachedTaskInput(t, "ork-many")
	in.Cache = nil
	result, err := h.dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if peak := slow.max(); peak < 2 || peak > transferParallelism {
		t.Fatalf("peak concurrent uploads = %d; want 2..%d", peak, transferParallelism)
	}
	if len(result.GetArtifacts()) != 24 {
		t.Fatalf("committed %d artifacts", len(result.GetArtifacts()))
	}
	for i, ref := range result.GetArtifacts() {
		if want := fmt.Sprintf("out-%02d.txt", i); ref.GetName() != want {
			t.Fatalf("artifact %d is %q; want %q (scan order)", i, ref.GetName(), want)
		}
	}
}

// TestCommit_AnUploadFailureNamesTheLowestIndex: with uploads 3 and 7 failing,
// and 7 failing first, the attempt's failure names out-03.txt.
func TestCommit_AnUploadFailureNamesTheLowestIndex(t *testing.T) {
	h, slow := manyOutputsHarness(t, 10)
	slow.fail = map[string]bool{"out-03.txt": true, "out-07.txt": true}
	slow.delay = func(name string) time.Duration {
		if name == "out-03.txt" {
			return 40 * time.Millisecond
		}
		return time.Millisecond
	}
	in := cachedTaskInput(t, "ork-many-fail")
	in.Cache = nil
	_, err := h.dispatcher.Execute(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "out-03.txt") || !strings.Contains(err.Error(), ReasonArtifactUploadFailed) {
		t.Fatalf("Execute = %v; want ARTIFACT_UPLOAD_FAILED naming out-03.txt", err)
	}
}
