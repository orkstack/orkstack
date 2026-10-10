package orker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestEachBounded_RunsAtMostLimitAtOnce: the pool never exceeds its bound and
// runs every index exactly once.
func TestEachBounded_RunsAtMostLimitAtOnce(t *testing.T) {
	var inFlight, peak atomic.Int32
	ran := make([]atomic.Int32, 40)
	at, err := eachBounded(context.Background(), len(ran), 8, func(_ context.Context, i int) error {
		now := inFlight.Add(1)
		for {
			old := peak.Load()
			if now <= old || peak.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
		ran[i].Add(1)
		return nil
	})
	if err != nil || at != -1 {
		t.Fatalf("eachBounded = %d, %v", at, err)
	}
	if peak.Load() > 8 || peak.Load() < 2 {
		t.Fatalf("peak = %d; want 2..8", peak.Load())
	}
	for i := range ran {
		if ran[i].Load() != 1 {
			t.Fatalf("index %d ran %d times", i, ran[i].Load())
		}
	}
}

// TestEachBounded_ReportsTheLowestFailingIndex: whatever order failures
// arrive in, the answer is the one a serial loop would have given, and the
// indexes after it are cancelled rather than waited for.
func TestEachBounded_ReportsTheLowestFailingIndex(t *testing.T) {
	at, err := eachBounded(context.Background(), 10, 8, func(ctx context.Context, i int) error {
		switch i {
		case 3:
			// Fails LATE, after index 5 already failed.
			time.Sleep(30 * time.Millisecond)
			return fmt.Errorf("index %d broke", i)
		case 5:
			return fmt.Errorf("index %d broke", i)
		case 6, 7:
			<-ctx.Done() // cancelled by index 5's failure
			return ctx.Err()
		}
		return nil
	})
	if at != 3 || err == nil || err.Error() != "index 3 broke" {
		t.Fatalf("eachBounded = %d, %v; want index 3", at, err)
	}
}

// slowStatStore delays every Stat and reports missing the URIs it was told.
type slowStatStore struct {
	artifacts.Store
	delay   time.Duration
	missing map[string]bool
	calls   atomic.Int32
}

func (s *slowStatStore) Stat(ctx context.Context, uri string) (artifacts.ArtifactRef, error) {
	s.calls.Add(1)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return artifacts.ArtifactRef{}, ctx.Err()
	}
	if s.missing[uri] {
		return artifacts.ArtifactRef{}, fmt.Errorf("%s: %w", uri, artifacts.ErrNotFound)
	}
	return artifacts.ArtifactRef{URI: uri}, nil
}

func sourceArtifacts(n int) []*orkstackv1.ArtifactRef {
	refs := make([]*orkstackv1.ArtifactRef, n)
	for i := range refs {
		refs[i] = &orkstackv1.ArtifactRef{Name: fmt.Sprintf("a%02d", i), Uri: fmt.Sprintf("mem://a%02d", i)}
	}
	return refs
}

// TestConfirmSourceArtifacts_IsBoundedParallel is F-orker-storage-perf-8: a
// cache hit's existence checks run eight at a time, so 64 artifacts at 20 ms
// each take about 160 ms rather than 1.28 s.
func TestConfirmSourceArtifacts_IsBoundedParallel(t *testing.T) {
	store := &slowStatStore{delay: 20 * time.Millisecond}
	began := time.Now()
	at, err := confirmSourceArtifacts(context.Background(), store, sourceArtifacts(64))
	elapsed := time.Since(began)
	if err != nil || at != -1 {
		t.Fatalf("confirm = %d, %v", at, err)
	}
	if store.calls.Load() != 64 {
		t.Fatalf("Stat calls = %d; want 64", store.calls.Load())
	}
	if elapsed > 640*time.Millisecond {
		t.Fatalf("64 checks at 20 ms took %s; serial is 1.28 s, eight at a time is ~160 ms", elapsed)
	}
}

// TestConfirmSourceArtifacts_NamesTheLowestMissingIndex: a deleted source
// artifact is a miss, naming the same artifact a serial check would.
func TestConfirmSourceArtifacts_NamesTheLowestMissingIndex(t *testing.T) {
	refs := sourceArtifacts(64)
	store := &slowStatStore{delay: time.Millisecond, missing: map[string]bool{
		refs[40].GetUri(): true, refs[55].GetUri(): true}}
	at, err := confirmSourceArtifacts(context.Background(), store, refs)
	if at != 40 || !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("confirm = %d, %v; want index 40 not found", at, err)
	}
}
