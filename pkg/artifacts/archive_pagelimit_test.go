package artifacts

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"
)

// A caller's Limit says how many entries it WANTS, not how many the archive
// holds, and it arrives unclamped from the ListRuns RPC's page_size and from
// the cleanup worker's page limit. These tests pin that it can never size an
// allocation.
//
// Before the fix both backends reserved make([]RunIndexEntry, 0, q.Limit).
// A RunIndexEntry is 88 bytes, so Limit=MaxInt32 reserved 176 GiB, and a
// negative Limit reached make() and panicked with "makeslice: cap out of
// range" — in library code, and for cleanup on a bare goroutine that takes the
// engine process with it.
//
// Every test that existed alongside that defect passed a small limit, which is
// exactly why the whole suite stayed green, so these cases carry the awkward
// limits deliberately.
var pageLimitCases = []struct {
	name  string
	limit int
	// want is how many of the three archived runs the page should return.
	want int
}{
	{name: "normal page", limit: 2, want: 2},
	{name: "page larger than the archive", limit: 10, want: 3},
	{name: "zero means no limit", limit: 0, want: 3},
	{name: "negative means no limit", limit: -2, want: 3},
	{name: "int64 wrap", limit: math.MinInt + 1, want: 3},
	{name: "unclamped wire page size", limit: math.MaxInt32, want: 3},
}

func pageLimitArchive(t *testing.T) (*LocalRunArchive, context.Context) {
	t.Helper()
	store, err := NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	base := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	for i := 0; i < 3; i++ {
		rec := RunRecord{
			RunID:     fmt.Sprintf("ork-page-%d", i),
			Namespace: "default",
			Orkflow:   "flow",
			Status:    "succeeded",
			Time:      base.Add(time.Duration(i) * time.Minute),
			JSONL:     []byte("body\n"),
		}
		if err := store.PutRun(ctx, rec); err != nil {
			t.Fatalf("PutRun %q: %v", rec.RunID, err)
		}
	}
	return store, ctx
}

// A non-positive limit means "no limit" in finish() and in the key-order page
// guard, so it must mean the same thing at the allocation and never panic.
func TestLocalRunArchive_PageLimitIsHonouredWithoutPanicking(t *testing.T) {
	for _, tc := range pageLimitCases {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := pageLimitArchive(t)
			for _, keyOrder := range []bool{false, true} {
				entries, err := store.ListRuns(ctx, RunQuery{KeyOrder: keyOrder, Limit: tc.limit})
				if err != nil {
					t.Fatalf("ListRuns(KeyOrder=%v, Limit=%d): %v", keyOrder, tc.limit, err)
				}
				if len(entries) != tc.want {
					t.Errorf("ListRuns(KeyOrder=%v, Limit=%d) returned %d entries, want %d",
						keyOrder, tc.limit, len(entries), tc.want)
				}
			}
		})
	}
}

// The page must not reserve memory proportional to what the caller asked for.
//
// Asserting "did not panic" is not enough: the 176 GiB reservation did not
// panic either. So this measures the heap the call actually takes and holds it
// to a bound the pre-fix code could not have met — MaxInt32 entries reserve
// 188,978,560,936 bytes, roughly six orders of magnitude above this ceiling.
func TestLocalRunArchive_PageLimitDoesNotSizeTheAllocation(t *testing.T) {
	store, ctx := pageLimitArchive(t)
	const ceiling = 1 << 20 // 1 MiB for a three-entry answer is already generous

	for _, limit := range []int{math.MaxInt32, math.MaxInt} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		entries, err := store.ListRuns(ctx, RunQuery{KeyOrder: true, Limit: limit})
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatalf("ListRuns(Limit=%d): %v", limit, err)
		}
		if len(entries) != 3 {
			t.Fatalf("ListRuns(Limit=%d) returned %d entries, want 3", limit, len(entries))
		}
		used := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("EVIDENCE local_limit=%d entries=%d heap_delta_bytes=%d ceiling_bytes=%d",
			limit, len(entries), used, ceiling)
		if used > ceiling {
			t.Errorf("ListRuns(Limit=%d) grew the heap by %d bytes, want at most %d: "+
				"the limit is sizing the allocation again", limit, used, ceiling)
		}
	}
}

// pageCapacity is the single place the two backends share this property, so it
// is pinned directly as well as through them.
func TestRunQuery_PageCapacityNeverFollowsTheCaller(t *testing.T) {
	for _, tc := range []struct {
		limit int
		want  int
	}{
		{limit: 0, want: runPageCapacityHint},
		{limit: -1, want: runPageCapacityHint},
		{limit: math.MinInt, want: runPageCapacityHint},
		{limit: 1, want: 1},
		{limit: runPageCapacityHint - 1, want: runPageCapacityHint - 1},
		{limit: runPageCapacityHint, want: runPageCapacityHint},
		{limit: math.MaxInt32, want: runPageCapacityHint},
		{limit: math.MaxInt, want: runPageCapacityHint},
	} {
		if got := (RunQuery{Limit: tc.limit}).pageCapacity(); got != tc.want {
			t.Errorf("RunQuery{Limit: %d}.pageCapacity() = %d, want %d", tc.limit, got, tc.want)
		}
		if got := (RunQuery{Limit: tc.limit}).pageCapacity(); got < 0 {
			t.Errorf("RunQuery{Limit: %d}.pageCapacity() = %d, which make() rejects", tc.limit, got)
		}
	}
}

// The cursor is rebuilt by re-rendering a scanned entry, so the rebuild must
// reproduce the stored key byte for byte. It did not: RunIndexKey re-applies
// NormalizeRunStatus, which strips a "status_" prefix, so a status that itself
// begins with "status_" rendered one segment short. The drift skips runs
// rather than repeating them — "status_succeeded" < "succeeded" — which is
// silent loss no assertion on a returned page could see.
func TestRunIndexKeyForEntry_RoundTripsTheStoredKeyExactly(t *testing.T) {
	base := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	// "status_" itself is absent on purpose: NormalizeRunStatus reduces it to
	// the empty segment, so RunIndexKey refuses it and no such key can be
	// stored to round-trip.
	for _, status := range []string{
		"succeeded",
		"failed",
		"canceled",
		"STATUS_SUCCEEDED",
		"STATUS_STATUS_SUCCEEDED",
		"status_succeeded",
		"Status_Weird",
	} {
		t.Run(status, func(t *testing.T) {
			rec := RunRecord{
				RunID: "ork-1786856120616785000-1", Namespace: "default",
				Orkflow: "flow", Status: status, Time: base, JSONL: []byte("body\n"),
			}
			stored, err := RunIndexKey(rec)
			if err != nil {
				t.Fatalf("RunIndexKey(%q): %v", status, err)
			}
			entry, ok := ParseRunIndexKey(stored)
			if !ok {
				t.Fatalf("ParseRunIndexKey(%q) reported the key foreign", stored)
			}
			rebuilt, err := RunIndexKeyForEntry(entry)
			if err != nil {
				t.Fatalf("RunIndexKeyForEntry(%+v): %v", entry, err)
			}
			if rebuilt != stored {
				t.Errorf("cursor rebuild drifted:\n stored  = %q\n rebuilt = %q", stored, rebuilt)
			}
		})
	}
}

// deadS3Archive builds a real S3RunArchive pointed at a closed port. Client
// construction performs no network I/O, so this exercises the production
// listing code without infrastructure — and the allocation under test happens
// before the first request, which is precisely the point: the 176 GiB
// reservation landed before the engine ever reached S3.
func deadS3Archive(t *testing.T) *S3RunArchive {
	t.Helper()
	archive, err := NewS3RunArchive(context.Background(), Config{
		S3Endpoint:  "http://127.0.0.1:1",
		S3Region:    DefaultS3Region,
		S3Bucket:    "page-limit-probe",
		S3AccessKey: "probe",
		S3SecretKey: "probe-secret",
		S3PathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	return archive
}

// The S3 archive is the production backend, and it carried the same
// reservation. It must refuse to size an allocation from the caller's limit
// and must fail with a transport error rather than a panic.
func TestS3RunArchive_PageLimitDoesNotSizeTheAllocation(t *testing.T) {
	archive := deadS3Archive(t)
	const ceiling = 1 << 20

	for _, tc := range pageLimitCases {
		t.Run(tc.name, func(t *testing.T) {
			// The slice is reserved before the first request, so a short
			// deadline still exercises the allocation while keeping the unit
			// suite off the SDK's retry backoff.
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			// The endpoint is closed, so this must return a transport error.
			// Reaching that error at all proves make() did not panic, and the
			// heap delta proves it did not reserve tc.limit entries first.
			_, err := archive.ListRuns(ctx, RunQuery{KeyOrder: true, Limit: tc.limit})
			runtime.ReadMemStats(&after)
			if err == nil {
				t.Fatalf("ListRuns(Limit=%d) against a closed port returned no error", tc.limit)
			}
			used := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			t.Logf("EVIDENCE s3_limit=%d heap_delta_bytes=%d ceiling_bytes=%d err=%v",
				tc.limit, used, ceiling, err != nil)
			if used > ceiling {
				t.Errorf("ListRuns(Limit=%d) grew the heap by %d bytes, want at most %d: "+
					"the limit is sizing the allocation again", tc.limit, used, ceiling)
			}
		})
	}
}
