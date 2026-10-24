package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// gatedGetStore counts Gets per URI, optionally holds every Get until gate
// closes (so two fetches provably overlap), delays each Get, and fails the
// URIs it is told to.
type gatedGetStore struct {
	artifacts.Store
	flight
	gate  chan struct{}
	delay func(uri string) time.Duration
	fail  map[string]bool
	mu    sync.Mutex
	gets  map[string]int
	total atomic.Int32
}

func (s *gatedGetStore) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	s.mu.Lock()
	if s.gets == nil {
		s.gets = map[string]int{}
	}
	s.gets[uri]++
	s.mu.Unlock()
	s.total.Add(1)
	s.enter()
	defer s.leave()
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.delay != nil {
		select {
		case <-time.After(s.delay(uri)):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.fail[uri] {
		return nil, fmt.Errorf("%s: %w", uri, artifacts.ErrNotFound)
	}
	return s.Store.Get(ctx, uri)
}

func (s *gatedGetStore) getsOf(uri string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[uri]
}

// stagingDispatcher is a dispatcher over a local store, with a gated store in
// front of it.
func stagingDispatcher(t *testing.T) (*OCIDispatcher, *artifacts.LocalStore, *gatedGetStore) {
	t.Helper()
	local, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	gated := &gatedGetStore{Store: local}
	return &OCIDispatcher{Store: gated, WorkRoot: t.TempDir(), Policy: DefaultSecurityPolicy()}, local, gated
}

func putRef(t *testing.T, store artifacts.Store, name string, data []byte) *orkstackv1.ArtifactRef {
	t.Helper()
	ref, err := store.Put(context.Background(), name, bytes.NewReader(data), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return refPtr(ref)
}

func stageEntries(t *testing.T, d *OCIDispatcher) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(d.WorkRoot, StageDirName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestStagingCache_ConcurrentFetchesOfOneDigestDownloadOnce is
// F-orker-storage-perf-2: two attempts staging the same digest at the same
// time hit the store once, and both input trees hold the verified bytes.
func TestStagingCache_ConcurrentFetchesOfOneDigestDownloadOnce(t *testing.T) {
	d, local, gated := stagingDispatcher(t)
	data := bytes.Repeat([]byte("shared input "), 10000)
	ref := putRef(t, local, "shared.bin", data)
	gated.gate = make(chan struct{})

	attempts := []*stagedInputs{{}, {}}
	dests := []string{filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")}
	failures := make([]*Failure, 2)
	var wg sync.WaitGroup
	for i := range attempts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			failures[i] = d.fetchInto(context.Background(), attempts[i], ref, dests[i])
		}(i)
	}
	// Both are in: one downloading (held at the gate), one waiting on it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		d.stagingCache().mu.Lock()
		entry := d.stagingCache().entries[ref.GetSha256()]
		refs := 0
		if entry != nil {
			refs = entry.refs
		}
		d.stagingCache().mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the two fetches never shared an entry (refs=%d)", refs)
		}
		time.Sleep(time.Millisecond)
	}
	close(gated.gate)
	wg.Wait()
	for i, failure := range failures {
		if failure != nil {
			t.Fatalf("fetch %d: %v", i, failure)
		}
		got, err := os.ReadFile(dests[i])
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("attempt %d holds %d bytes (%v); want the verified %d", i, len(got), err, len(data))
		}
	}
	if n := gated.getsOf(ref.GetUri()); n != 1 {
		t.Fatalf("Store.Get called %d times for one digest; want 1", n)
	}

	// The entry lives while either attempt does, and goes with the last.
	attempts[0].releaseStaging()
	if names := stageEntries(t, d); len(names) != 1 || names[0] != ref.GetSha256() {
		t.Fatalf("after one release the cache holds %v; want the digest", names)
	}
	attempts[1].releaseStaging()
	if names := stageEntries(t, d); len(names) != 0 || d.stagingCache().size() != 0 {
		t.Fatalf("after the last release the cache holds %v (%d entries)", names, d.stagingCache().size())
	}
	// The attempts' own links are untouched by the release.
	if got, _ := os.ReadFile(dests[1]); !bytes.Equal(got, data) {
		t.Fatal("releasing the cache entry changed an attempt's input")
	}
}

// TestStagingCache_ADigestMismatchFailsEveryWaiterAndLeavesNoEntry: bytes that
// do not hash to the reference are never linked, by the downloader or by an
// attempt waiting on it.
func TestStagingCache_ADigestMismatchFailsEveryWaiterAndLeavesNoEntry(t *testing.T) {
	d, local, gated := stagingDispatcher(t)
	ref := putRef(t, local, "lies.bin", []byte("real bytes"))
	ref.Sha256 = strings.Repeat("e", 64)
	gated.gate = make(chan struct{})

	failures := make([]*Failure, 3)
	var wg sync.WaitGroup
	for i := range failures {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			failures[i] = d.fetchInto(context.Background(), &stagedInputs{}, ref, filepath.Join(t.TempDir(), "x"))
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(gated.gate)
	wg.Wait()
	for i, failure := range failures {
		if failure == nil || failure.Reason != ReasonInputCorrupt {
			t.Fatalf("fetch %d = %v; want INPUT_ARTIFACT_CORRUPT", i, failure)
		}
	}
	if names := stageEntries(t, d); len(names) != 0 || d.stagingCache().size() != 0 {
		t.Fatalf("a corrupt download left %v in the cache (%d entries)", names, d.stagingCache().size())
	}
}

// TestStagingCache_FallsBackToADirectDownloadWhenTheLinkFails: on a work root
// that cannot hard-link, every attempt downloads its own copy, as before.
func TestStagingCache_FallsBackToADirectDownloadWhenTheLinkFails(t *testing.T) {
	d, local, gated := stagingDispatcher(t)
	data := []byte("bytes on a filesystem without links")
	ref := putRef(t, local, "f.bin", data)
	d.stagingCache().link = func(string, string) error {
		return &os.LinkError{Op: "link", Err: syscall.EXDEV}
	}
	staged := &stagedInputs{}
	dest := filepath.Join(t.TempDir(), "f.bin")
	if failure := d.fetchInto(context.Background(), staged, ref, dest); failure != nil {
		t.Fatal(failure)
	}
	if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("fallback staged %q (%v)", got, err)
	}
	if len(staged.leases) != 0 || d.stagingCache().size() != 0 {
		t.Fatalf("a fallback kept a hold: leases=%d entries=%d", len(staged.leases), d.stagingCache().size())
	}
	if n := gated.getsOf(ref.GetUri()); n != 2 {
		t.Fatalf("Gets = %d; want the cache's download plus the direct one", n)
	}
}

// TestBoot_ReapsTheStagingCache: an entry a killed Orker left behind is
// removed at the next boot.
func TestBoot_ReapsTheStagingCache(t *testing.T) {
	root := t.TempDir()
	leftover := filepath.Join(root, StageDirName, strings.Repeat("a", 64))
	if err := os.MkdirAll(filepath.Dir(leftover), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, []byte("stale"), 0o444); err != nil {
		t.Fatal(err)
	}
	a := NewActivitiesWithOptions(nil, Options{
		OrkerID: proof.OrkerID("orker-stage-reap"), WorkRoot: root, Runtime: &reapRecorder{labelled: map[string]int{}},
	})
	t.Cleanup(a.Close)
	if err := a.Boot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("the stale staging entry survived boot: %v", err)
	}
}

// fanInInput is an input with one plain upstream carrying n artifacts and a
// values.json.
func fanInInput(t *testing.T, store artifacts.Store, n int) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := baseExportInput("consume")
	up := &orkstackv1.UpstreamRef{TaskId: "produce", Values: putRef(t, store, "values.json", []byte(`{"n":1}`))}
	for i := 0; i < n; i++ {
		up.Artifacts = append(up.Artifacts, putRef(t, store, fmt.Sprintf("part-%02d.txt", i), []byte(fmt.Sprintf("part %d\n", i))))
	}
	in.Upstream = []*orkstackv1.UpstreamRef{up}
	return in
}

// TestStageInputs_IsBoundedParallel is F-orker-storage-perf-4: 64 inputs at
// 20 ms each stage eight at a time — never more, and more than one.
func TestStageInputs_IsBoundedParallel(t *testing.T) {
	d, local, gated := stagingDispatcher(t)
	in := fanInInput(t, local, 64)
	gated.delay = func(string) time.Duration { return 20 * time.Millisecond }
	ws, err := NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	staged, failure := d.stageInputs(context.Background(), in, ws, false)
	if failure != nil {
		t.Fatal(failure)
	}
	defer staged.releaseStaging()
	if peak := gated.max(); peak < 2 || peak > transferParallelism {
		t.Fatalf("peak concurrent fetches = %d; want 2..%d (serial is 1)", peak, transferParallelism)
	}
	if len(staged.artifacts) != 64 {
		t.Fatalf("staged %d artifacts", len(staged.artifacts))
	}
}

// TestStageInputs_ParallelIsByteIdenticalToSerial: whatever order the
// downloads finish in, everything task.json and the memo key are built from is
// exactly what a serial staging builds.
func TestStageInputs_ParallelIsByteIdenticalToSerial(t *testing.T) {
	stage := func(transfers int, delay func(string) time.Duration) *stagedInputs {
		d, local, gated := stagingDispatcher(t)
		d.transfers = transfers
		in := fanInInput(t, local, 24)
		gated.delay = delay
		ws, err := NewWorkspace(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		staged, failure := d.stageInputs(context.Background(), in, ws, false)
		if failure != nil {
			t.Fatal(failure)
		}
		staged.releaseStaging()
		return staged
	}
	serial := stage(1, nil)
	// Later refs finish first.
	parallel := stage(8, func(uri string) time.Duration {
		if strings.Contains(uri, "part-0") {
			return 15 * time.Millisecond
		}
		return time.Millisecond
	})
	for name, pair := range map[string][2]any{
		"artifacts":      {serial.artifacts, parallel.artifacts},
		"upstream":       {serial.upstream, parallel.upstream},
		"inputFiles":     {serial.inputFiles, parallel.inputFiles},
		"paths":          {serial.paths, parallel.paths},
		"upstreamValues": {serial.upstreamValues, parallel.upstreamValues},
	} {
		if !reflect.DeepEqual(pair[0], pair[1]) {
			t.Errorf("%s differs between serial and parallel staging:\n%v\n%v", name, pair[0], pair[1])
		}
	}
}

// TestStageInputs_ReportsTheLowestFailingIndex: with refs 3 and 7 of 10
// missing, and 7 failing first, the failure names ref 3 — the one a serial
// staging reports.
func TestStageInputs_ReportsTheLowestFailingIndex(t *testing.T) {
	d, local, gated := stagingDispatcher(t)
	in := fanInInput(t, local, 10)
	refs := in.GetUpstream()[0].GetArtifacts()
	gated.fail = map[string]bool{refs[3].GetUri(): true, refs[7].GetUri(): true}
	gated.delay = func(uri string) time.Duration {
		if uri == refs[3].GetUri() {
			return 40 * time.Millisecond
		}
		return time.Millisecond
	}
	ws, err := NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, failure := d.stageInputs(context.Background(), in, ws, false)
	if failure == nil || failure.Reason != ReasonInputUnavailable || !strings.Contains(failure.Message, "part-03.txt") {
		t.Fatalf("failure = %v; want INPUT_ARTIFACT_UNAVAILABLE naming part-03.txt", failure)
	}
	if d.stagingCache().size() != 0 {
		t.Fatalf("a failed staging kept %d cache entries", d.stagingCache().size())
	}
}
