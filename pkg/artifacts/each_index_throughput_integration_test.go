//go:build integration

package artifacts_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// linesSource generates n JSON lines of exactly width bytes each, LF
// included, without holding them: a 256 MiB source the test never keeps in
// memory.
type linesSource struct {
	n, width, i int
	line        []byte
	pending     []byte
}

func newLinesSource(n, width int) *linesSource {
	return &linesSource{n: n, width: width, line: make([]byte, 0, width)}
}

func (s *linesSource) Read(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		if len(s.pending) == 0 {
			if s.i == s.n {
				if written == 0 {
					return 0, io.EOF
				}
				return written, nil
			}
			s.line = append(s.line[:0], fmt.Sprintf(`{"n":%d,"pad":"`, s.i)...)
			for len(s.line) < s.width-3 {
				s.line = append(s.line, 'x')
			}
			s.line = append(s.line, '"', '}', '\n')
			s.pending = s.line
			s.i++
		}
		c := copy(p[written:], s.pending)
		s.pending = s.pending[c:]
		written += c
	}
	return written, nil
}

// rangedSource reads an object in fixed-size ranged reads, as the Orker's
// indexer does (internal/orker, rangeReader).
type rangedSource struct {
	ctx        context.Context
	store      artifacts.Store
	uri        string
	size, off  int64
	body       io.ReadCloser
	rangeBytes int64
}

func (r *rangedSource) Read(p []byte) (int, error) {
	for {
		if r.body == nil {
			if r.off >= r.size {
				return 0, io.EOF
			}
			body, err := r.store.GetRange(r.ctx, r.uri, r.off, min(r.rangeBytes, r.size-r.off))
			if err != nil {
				return 0, err
			}
			r.body = body
		}
		n, err := r.body.Read(p)
		r.off += int64(n)
		if errors.Is(err, io.EOF) {
			_ = r.body.Close() // a fully read range: its close says nothing new
			r.body = nil
			if n == 0 {
				continue
			}
			return n, nil
		}
		return n, err
	}
}

// TestIntegration_EachLinesIndexThroughput: 256 MiB of short JSON lines,
// committed to the object store, indexed in batches of 1,000 through 8 MiB
// ranged reads and written as an index create-if-absent. It logs the
// throughput the store and the indexer sustain together and holds the
// indexer's heap growth to the 16 MiB bound PR-0 set; the throughput is a
// measurement, never a bound (the plan pins a quarter of what PR-0 measured).
func TestIntegration_EachLinesIndexThroughput(t *testing.T) {
	const (
		width    = 256
		lines    = 256 << 20 / width
		k        = 1000
		maxItems = (lines + k - 1) / k
		bound    = 16 << 20
		sampleAt = 16 << 20
	)
	st, cfg := newS3Store(t)
	ctx := context.Background()
	ref, err := st.Put(ctx, uniquePrefix(t)+"/rows.jsonl", newLinesSource(lines, width), artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("Put the source: %v", err)
	}
	if ref.SizeBytes != lines*width {
		t.Fatalf("committed %d bytes, want %d", ref.SizeBytes, lines*width)
	}
	sum, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(sum) != 32 {
		t.Fatalf("source digest %q: %v", ref.SHA256, err)
	}
	var digest [32]byte
	copy(digest[:], sum)

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak uint64
	samples := int64(0)
	progress := func(read, _ int64) {
		// One sample per 16 MiB read: a GC on every 1 MiB read would be what
		// the test measured.
		if read/sampleAt <= samples {
			return
		}
		samples = read / sampleAt
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		peak = max(peak, m.HeapAlloc)
	}
	started := time.Now()
	r := &rangedSource{ctx: ctx, store: st, uri: ref.URI, size: ref.SizeBytes, rangeBytes: 8 << 20}
	table, err := eachlines.Index(ctx, r, ref.SizeBytes, k, eachlines.Options{
		Format: eachlines.FormatJSONL, MaxItems: maxItems, ValuesMaxBytes: 1 << 10, SliceMaxBytes: 64 << 20,
		SourceSize: ref.SizeBytes, SourceSHA256: &digest, Progress: progress,
	})
	if err != nil {
		t.Fatalf("Index: %v", err)
	}
	elapsed := time.Since(started)
	if table.Count != maxItems || table.TotalLines != lines {
		t.Fatalf("indexed %d items of %d lines; want %d of %d", table.Count, table.TotalLines, maxItems, lines)
	}
	if samples == 0 {
		t.Fatal("the indexer reported no progress to sample")
	}
	growth := int64(peak) - int64(base.HeapAlloc)
	if growth > bound {
		t.Fatalf("heap grew by %d bytes while indexing, over %d", growth, bound)
	}

	indexes, err := artifacts.NewS3EachIndexStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3EachIndexStore: %v", err)
	}
	body := table.Encode()
	if _, created, err := indexes.CreateIfAbsent(ctx, artifacts.EachLinesIndexRef{
		Namespace: "default", RunID: "ork-it-" + uniqueToken(t), Task: "convert", SourceSHA256: ref.SHA256, Format: "jsonl",
	}, body, int64(len(body))); err != nil || !created {
		t.Fatalf("CreateIfAbsent = created %v, %v", created, err)
	}
	t.Logf("indexed %d MiB (%d lines, %d items of %d) in %s: %.1f MB/s; heap growth %d KiB of %d KiB allowed",
		ref.SizeBytes>>20, lines, table.Count, k, elapsed.Round(time.Millisecond),
		float64(ref.SizeBytes)/1e6/elapsed.Seconds(), growth>>10, bound>>10)
}
