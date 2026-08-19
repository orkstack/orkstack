package orker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordingLogStore captures what the sink actually uploaded, in the order it
// uploaded it. It is the whole observation: a sink that kept everything in
// memory and wrote at the end would pass a test that only checked the final
// bytes, and that sink is exactly the one this milestone exists to replace.
type recordingLogStore struct {
	mu     sync.Mutex
	chunks []artifacts.LogChunk
	fail   func(seq int64) error
}

func (s *recordingLogStore) PutLogChunk(_ context.Context, chunk artifacts.LogChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		if err := s.fail(chunk.Seq); err != nil {
			return err
		}
	}
	stored := chunk
	stored.Data = append([]byte(nil), chunk.Data...)
	s.chunks = append(s.chunks, stored)
	return nil
}

func (s *recordingLogStore) ReadLogChunks(
	_ context.Context, ref artifacts.LogRef, afterSeq int64, limit int,
) ([]artifacts.LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []artifacts.LogChunk
	for _, chunk := range s.chunks {
		if chunk.LogRef == ref && chunk.Seq > afterSeq {
			out = append(out, chunk)
		}
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *recordingLogStore) LatestLogAttempt(context.Context, string, string) (int32, error) {
	return 0, artifacts.ErrNotFound
}

func (s *recordingLogStore) stored() []artifacts.LogChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]artifacts.LogChunk(nil), s.chunks...)
}

func (s *recordingLogStore) body() string {
	var b strings.Builder
	for _, chunk := range s.stored() {
		b.Write(chunk.Data)
	}
	return b.String()
}

func testRef() artifacts.LogRef {
	return artifacts.LogRef{RunID: "ork-1-1", TaskID: "emit", Attempt: 1}
}

// A chatty task must not write one object per line: the size bound is what
// keeps a 100k-line task to a few hundred uploads instead of 100k.
func TestLogChunkSink_FlushesOnSize(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:    64,
		FlushInterval: time.Hour, // only the size bound may fire
		MaxBytes:      1 << 20,
	})
	defer sink.Close()

	// Ten 10-byte lines: 100 bytes, so the 64-byte bound trips once during the
	// writes and the rest is still buffered.
	for i := 0; i < 10; i++ {
		sink.Write(fmt.Sprintf("line-%04d", i))
	}
	chunks := store.stored()
	if len(chunks) == 0 {
		t.Fatal("nothing was flushed; a chatty task would be invisible until it ended")
	}
	if len(chunks) > 2 {
		t.Fatalf("100 bytes produced %d chunks; the size bound is not batching", len(chunks))
	}
	if got := len(chunks[0].Data); got < 64 {
		t.Errorf("first chunk is %d bytes, want at least the 64-byte flush bound", got)
	}
}

// A slow task must appear promptly. Without the time bound, a task emitting
// one line a second would be invisible until it produced 64 KiB — which for
// most tasks is never.
func TestLogChunkSink_FlushesOnElapsedTime(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:    1 << 20, // far above anything this test writes
		FlushInterval: 25 * time.Millisecond,
		MaxBytes:      1 << 20,
	})
	defer sink.Close()

	sink.Write("first")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.stored()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	chunks := store.stored()
	if len(chunks) == 0 {
		t.Fatal("a single short line never reached the store; the elapsed-time bound does not fire")
	}
	if got := string(chunks[0].Data); got != "first\n" {
		t.Errorf("first chunk = %q, want %q", got, "first\n")
	}

	// And the timer re-arms: a second line must not wait for the first to be
	// written again.
	sink.Write("second")
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.stored()) > 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(store.stored()) < 2 {
		t.Fatal("the flush timer did not re-arm after the first flush")
	}
}

// Sequences are dense and 1-based, because that is the resume contract the
// reader hands clients back as from_seq.
func TestLogChunkSink_NumbersChunksDenselyFromOne(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 8, FlushInterval: time.Hour, MaxBytes: 1 << 20,
	})
	for i := 0; i < 20; i++ {
		sink.Write(fmt.Sprintf("%08d", i))
	}
	sink.Close()

	chunks := store.stored()
	if len(chunks) < 5 {
		t.Fatalf("20 nine-byte lines produced %d chunks; expected several", len(chunks))
	}
	for i, chunk := range chunks {
		if want := int64(i + 1); chunk.Seq != want {
			t.Fatalf("chunk %d has seq %d, want %d: sequences must be dense and 1-based", i, chunk.Seq, want)
		}
		if chunk.LogRef != testRef() {
			t.Fatalf("chunk %d carries %+v, want %+v", i, chunk.LogRef, testRef())
		}
	}
}

// Chunks end on line boundaries, so a reader never renders half a line as a
// whole one.
func TestLogChunkSink_ChunksEndOnLineBoundaries(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 16, FlushInterval: time.Hour, MaxBytes: 1 << 20, MaxLineBytes: 1 << 20,
	})
	for i := 0; i < 12; i++ {
		sink.Write(fmt.Sprintf("line %d", i))
	}
	sink.Close()

	for i, chunk := range store.stored() {
		if len(chunk.Data) == 0 {
			t.Fatalf("chunk %d is empty", i)
		}
		if chunk.Data[len(chunk.Data)-1] != '\n' {
			t.Errorf("chunk %d ends %q, not on a line boundary", i, chunk.Data)
		}
	}
	if got, want := store.body(), lines(12); got != want {
		t.Errorf("reassembled log = %q, want %q", got, want)
	}
}

// One line longer than the chunk limit is split rather than buffered whole:
// a single unbounded line is the other way a task exhausts the Orker's memory.
func TestLogChunkSink_SplitsAnOverlongLine(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:   32,
		MaxLineBytes: 32,
		MaxBytes:     1 << 20, FlushInterval: time.Hour,
	})
	long := strings.Repeat("x", 200)
	sink.Write(long)
	sink.Close()

	chunks := store.stored()
	if len(chunks) < 6 {
		t.Fatalf("a 201-byte line produced %d chunks with a 32-byte limit; it was not split", len(chunks))
	}
	for i, chunk := range chunks {
		if len(chunk.Data) > 32 {
			t.Errorf("chunk %d is %d bytes, above the 32-byte limit", i, len(chunk.Data))
		}
	}
	if got, want := store.body(), long+"\n"; got != want {
		t.Errorf("the split line does not reassemble: got %d bytes, want %d", len(got), len(want))
	}
}

// The cap is the memory bound, and it must be VISIBLE: a task whose output was
// cut says so in its own stream, in words, once.
func TestLogChunkSink_TruncatesAtTheCapWithAVisibleNotice(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 16, FlushInterval: time.Hour, MaxBytes: 64, MaxLineBytes: 1 << 20,
	})
	for i := 0; i < 100; i++ {
		sink.Write(fmt.Sprintf("line %03d", i)) // 9 bytes each
	}
	sink.Close()

	chunks := store.stored()
	if len(chunks) == 0 {
		t.Fatal("nothing was stored at all")
	}
	var notices []artifacts.LogChunk
	var stored int64
	for _, chunk := range chunks {
		if chunk.Truncated {
			notices = append(notices, chunk)
			continue
		}
		stored += int64(len(chunk.Data))
	}
	if stored > 64 {
		t.Errorf("stored %d log bytes with a 64-byte cap; the cap does not bound the sink", stored)
	}
	if len(notices) != 1 {
		t.Fatalf("the stream carries %d truncation notices, want exactly 1", len(notices))
	}
	notice := notices[0]
	if notice.Seq != chunks[len(chunks)-1].Seq {
		t.Errorf("the notice is at seq %d but the last chunk is %d; it must be last",
			notice.Seq, chunks[len(chunks)-1].Seq)
	}
	if !strings.Contains(string(notice.Data), "truncated") {
		t.Errorf("the notice reads %q; truncation must be visible in words", notice.Data)
	}
	if !strings.Contains(string(notice.Data), "64") {
		t.Errorf("the notice reads %q; it must name the cap it hit", notice.Data)
	}
	if !sink.Truncated() {
		t.Error("the sink does not report itself truncated")
	}
}

// Writing after the cap is a no-op, so a runaway task cannot keep the Orker
// busy uploading notices.
func TestLogChunkSink_StopsUploadingAfterTheCap(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 8, FlushInterval: time.Hour, MaxBytes: 32,
	})
	for i := 0; i < 20; i++ {
		sink.Write("aaaaaaaaaaaaaaaa") // 17 bytes with the newline
	}
	before := len(store.stored())
	for i := 0; i < 1000; i++ {
		sink.Write("more and more and more")
	}
	sink.Close()
	if after := len(store.stored()); after != before {
		t.Errorf("1000 lines past the cap wrote %d more chunks, want 0", after-before)
	}
}

// Close is the durability barrier the whole "end the stream cleanly" contract
// rests on: everything buffered must be in the store before ExecuteTask
// returns, because the task's terminal event comes after that.
func TestLogChunkSink_CloseFlushesWhatIsBuffered(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 1 << 20, FlushInterval: time.Hour, MaxBytes: 1 << 20,
	})
	sink.Write("only line")
	if len(store.stored()) != 0 {
		t.Fatal("the sink flushed before either bound was reached")
	}
	sink.Close()
	if got := store.body(); got != "only line\n" {
		t.Fatalf("after Close the store holds %q, want %q", got, "only line\n")
	}
	// Close is idempotent: the deferred close and an explicit one must not
	// write the same bytes twice.
	sink.Close()
	if got := store.body(); got != "only line\n" {
		t.Fatalf("a second Close changed the store to %q", got)
	}
}

// A store that fails must not fail the task — logging is observability — but
// it must not silently renumber either: the sequences that DID land stay dense,
// and the loss is reported in the stream.
func TestLogChunkSink_AFailedUploadKeepsSequencesDenseAndSaysSo(t *testing.T) {
	var attempts int
	store := &recordingLogStore{}
	store.fail = func(int64) error {
		attempts++
		if attempts <= logChunkUploadTries { // every try of the first chunk fails
			return errors.New("bucket unavailable")
		}
		return nil
	}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 8, FlushInterval: time.Hour, MaxBytes: 1 << 20,
	})
	sink.Write("first one")
	sink.Write("second one")
	sink.Close()

	chunks := store.stored()
	if len(chunks) == 0 {
		t.Fatal("a single failing chunk stopped the sink permanently")
	}
	for i, chunk := range chunks {
		if want := int64(i + 1); chunk.Seq != want {
			t.Fatalf("chunk %d has seq %d, want %d: a dropped chunk consumed a sequence and left a gap",
				i, chunk.Seq, want)
		}
	}
	if !strings.Contains(store.body(), "dropped") {
		t.Errorf("the stream reads %q; a dropped chunk must be visible, not silent", store.body())
	}
}

// The shell plugin writes stdout and stderr from two goroutines, so the sink is
// used concurrently by construction.
func TestLogChunkSink_IsSafeUnderConcurrentWriters(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 64, FlushInterval: 5 * time.Millisecond, MaxBytes: 1 << 20,
	})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				sink.Write(fmt.Sprintf("worker %d line %d", worker, i))
			}
		}(w)
	}
	wg.Wait()
	sink.Close()

	chunks := store.stored()
	for i, chunk := range chunks {
		if want := int64(i + 1); chunk.Seq != want {
			t.Fatalf("chunk %d has seq %d, want %d under concurrent writers", i, chunk.Seq, want)
		}
	}
	if got := strings.Count(store.body(), "\n"); got != 800 {
		t.Errorf("the store holds %d lines, want 800", got)
	}
}

// The defaults are the documented ones. A sink built with a zero LogOptions is
// what production gets when nothing is configured.
func TestLogOptions_DefaultsAreTheDocumentedBounds(t *testing.T) {
	opts := LogOptions{}.withDefaults()
	if opts.FlushBytes != DefaultLogFlushBytes {
		t.Errorf("FlushBytes = %d, want %d", opts.FlushBytes, DefaultLogFlushBytes)
	}
	if opts.FlushInterval != DefaultLogFlushInterval {
		t.Errorf("FlushInterval = %v, want %v", opts.FlushInterval, DefaultLogFlushInterval)
	}
	if opts.MaxBytes != DefaultLogMaxBytes {
		t.Errorf("MaxBytes = %d, want %d", opts.MaxBytes, DefaultLogMaxBytes)
	}
	if opts.MaxLineBytes != DefaultLogMaxLineBytes {
		t.Errorf("MaxLineBytes = %d, want %d", opts.MaxLineBytes, DefaultLogMaxLineBytes)
	}
}

func TestLogOptionsFromEnv_ReadsEachBound(t *testing.T) {
	t.Setenv(LogFlushBytesVar, "128")
	t.Setenv(LogFlushIntervalVar, "250ms")
	t.Setenv(LogMaxBytesVar, "4096")
	opts := LogOptionsFromEnv()
	if opts.FlushBytes != 128 {
		t.Errorf("FlushBytes = %d, want 128", opts.FlushBytes)
	}
	if opts.FlushInterval != 250*time.Millisecond {
		t.Errorf("FlushInterval = %v, want 250ms", opts.FlushInterval)
	}
	if opts.MaxBytes != 4096 {
		t.Errorf("MaxBytes = %d, want 4096", opts.MaxBytes)
	}
}

func lines(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "line %d\n", i)
	}
	return b.String()
}
