package eachlines

import (
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
)

// repeatReader serves pattern over and over until total bytes are served,
// allocating nothing; total is a multiple of len(pattern).
type repeatReader struct {
	pattern []byte
	pos     int
	left    int64
}

func (r *repeatReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && r.left > 0 {
		m := copy(p[n:], r.pattern[r.pos:])
		if int64(m) > r.left {
			m = int(r.left)
		}
		n += m
		r.left -= int64(m)
		r.pos = (r.pos + m) % len(r.pattern)
	}
	return n, nil
}

// jsonlLine is one jsonl line of exactly size bytes of content, plus its LF.
func jsonlLine(size int) []byte {
	if size < 8 {
		panic("jsonlLine: too small")
	}
	return []byte("{\"k\":\"" + strings.Repeat("x", size-8) + "\"}\n")
}

type shape struct {
	name     string
	line     []byte
	lines    int64 // per pattern
	k        int
	maxItems int
	total    int64
}

func (s shape) run(sample func(read, lines int64)) (*Table, error) {
	const valuesMax = 262144 // the default values_max_bytes
	o := Options{
		Format:         FormatJSONL,
		MaxItems:       s.maxItems,
		ValuesMaxBytes: valuesMax,
		SliceMaxBytes:  SliceMax(s.k, valuesMax, 64<<20),
		SourceSize:     s.total,
		Progress:       sample,
	}
	pattern := make([]byte, 0, int(s.lines)*len(s.line))
	for i := int64(0); i < s.lines; i++ {
		pattern = append(pattern, s.line...)
	}
	r := &repeatReader{pattern: pattern, left: s.total}
	return Index(context.Background(), r, ReadCap(s.maxItems, o.SliceMaxBytes, 5<<30), s.k, o)
}

// TestIndex_HeapStaysBounded holds the indexer to 16 MiB of live heap growth
// on 256 MiB, for K = 1 and K = 1,000, and the slice staging of a batch to
// the same bound. The heap is sampled live (after a collection) at every
// buffer the indexer reads, so the result does not depend on the collector's
// timing. It is skipped under -short only because 256 MiB hashed and
// validated costs seconds, and tens of seconds under the race detector.
func TestIndex_HeapStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("256 MiB through the indexer takes seconds, and far longer under the race detector")
	}
	const bound = 16 << 20
	var base runtime.MemStats
	measure := func(t *testing.T, run func(sample func(int64, int64)) error) {
		t.Helper()
		runtime.GC()
		runtime.ReadMemStats(&base)
		var peak uint64
		sample := func(int64, int64) {
			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			peak = max(peak, m.HeapAlloc)
		}
		if err := run(sample); err != nil {
			t.Fatal(err)
		}
		if peak == 0 {
			t.Fatal("the indexer never reported progress")
		}
		if peak > base.HeapAlloc && peak-base.HeapAlloc > bound {
			t.Fatalf("heap grew by %d bytes, over %d", peak-base.HeapAlloc, bound)
		}
		t.Logf("heap growth %d KiB", (int64(peak)-int64(base.HeapAlloc))>>10)
	}
	t.Run("K=1 with 64 KiB lines", func(t *testing.T) {
		s := shape{line: jsonlLine(65535), lines: 1, k: 1, maxItems: 5000, total: 4096 * 65536}
		measure(t, func(sample func(int64, int64)) error { _, err := s.run(sample); return err })
	})
	t.Run("K=1000 with short lines", func(t *testing.T) {
		s := shape{line: jsonlLine(255), lines: 1000, k: 1000, maxItems: 1000, total: 1000 * 256 * 1000}
		measure(t, func(sample func(int64, int64)) error { _, err := s.run(sample); return err })
	})
	t.Run("K=1000 slice staging", func(t *testing.T) {
		line := jsonlLine(32767)
		span := make([]byte, 0, 1000*len(line))
		for i := 0; i < 1000; i++ {
			span = append(span, line...)
		}
		measure(t, func(sample func(int64, int64)) error {
			r := &sampledReader{r: &repeatReader{pattern: span, left: int64(len(span))}, sample: sample}
			n, err := SliceTo(io.Discard, r, 1000)
			if err == nil && n != 1000 {
				t.Fatalf("staged %d lines", n)
			}
			return err
		})
	})
}

// sampledReader calls sample as bytes are read, a stand-in for Progress.
type sampledReader struct {
	r      io.Reader
	sample func(int64, int64)
	read   int64
	next   int64
}

func (s *sampledReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.read += int64(n)
	if s.read >= s.next {
		s.sample(s.read, 0)
		s.next = s.read + 1<<20
	}
	return n, err
}

// BenchmarkIndex_2_5GiB indexes about 2.5 GiB from a generated reader:
// 10,000 lines of 256 KiB at K = 1, and 10 million short lines at K = 1,000.
// It reports MB/s and the peak heap (live and not yet collected, sampled at
// every buffer). Run it once: go test -run '^$' -bench BenchmarkIndex_2_5GiB -benchtime 1x.
func BenchmarkIndex_2_5GiB(b *testing.B) {
	for _, s := range []shape{
		{name: "10000x256KiB_K1", line: jsonlLine(262143), lines: 1, k: 1, maxItems: 10000, total: 10000 * 262144},
		{name: "10Mx250B_K1000", line: jsonlLine(249), lines: 1000, k: 1000, maxItems: 10000, total: 10_000_000 * 250},
	} {
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(s.total)
			var peak, base uint64
			for range b.N {
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				base = m.HeapAlloc
				tab, err := s.run(func(int64, int64) {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					peak = max(peak, m.HeapAlloc)
				})
				if err != nil {
					b.Fatal(err)
				}
				if int(tab.Count) != s.maxItems {
					b.Fatalf("%d items", tab.Count)
				}
			}
			b.ReportMetric(float64(peak-min(peak, base))/(1<<20), "peak-heap-growth-MiB")
		})
	}
}
