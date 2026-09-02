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
// bytes, and that is exactly the sink this one must never be.
type recordingLogStore struct {
	mu     sync.Mutex
	chunks []artifacts.LogChunk
	stamps []time.Time
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
	s.stamps = append(s.stamps, time.Now())
	return nil
}

func (s *recordingLogStore) ReadLogChunks(
	_ context.Context, ref artifacts.LogRef, afterSeq int64, limit int, maxBytes int64,
) ([]artifacts.LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []artifacts.LogChunk
	read := int64(0)
	for _, chunk := range s.chunks {
		if chunk.LogRef == ref && chunk.Seq > afterSeq {
			out = append(out, chunk)
			read += int64(len(chunk.Data))
		}
		if limit > 0 && len(out) == limit {
			break
		}
		// The first chunk is always whole; the budget is spent after it
		// (pkg/artifacts/logs.go).
		if maxBytes > 0 && read >= maxBytes {
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

func (s *recordingLogStore) uploadTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.stamps...)
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

// unpaced makes every size-triggered flush eligible, by reading a clock that
// has always just advanced a full interval.
//
// It is for tests about how a chunk is COMPOSED — where its boundaries fall,
// how the sequences run, what happens at the cap — which need the size bound
// to fire on demand and say nothing about pacing. The pace itself is what
// TestLogChunkSink_ABytesBoundOfOneStillBatchesAChattyTask measures, on a
// clock it moves a step at a time.
func unpaced(sink *logChunkSink) *logChunkSink {
	base := time.Now()
	var ticks int64
	sink.now = func() time.Time {
		ticks++
		return base.Add(time.Duration(ticks) * time.Hour)
	}
	return sink
}

// testClock is a clock the test moves and the sink reads. The sink reads it
// under its own lock, from whichever goroutine is writing; the test advances
// it from the test goroutine. So it carries a lock of its own.
type testClock struct {
	mu sync.Mutex
	at time.Time
}

func newTestClock() *testClock { return &testClock{at: time.Now()} }

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// A chatty task must not write one object per line: the size bound is what
// keeps a 100k-line task to a few hundred uploads instead of 100k.
func TestLogChunkSink_FlushesOnSize(t *testing.T) {
	store := &recordingLogStore{}
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:    64,
		FlushInterval: time.Hour, // only the size bound may fire
		MaxBytes:      1 << 20,
	}))
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

// TestLogChunkSink_ABytesBoundOfOneStillBatchesAChattyTask.
//
// The two bounds have to hold TOGETHER, and this is the case where asking the
// size bound alone gives the wrong answer: `ORK_ORKER_LOG_FLUSH_BYTES=1` is
// what a test sets to force chunk boundaries, and it must not mean one PUT per
// line — two thousand lines must not become two thousand objects, a write
// storm produced by a knob an operator may legitimately turn.
//
// The rule: however small the size bound, normal writes cost
// at most once per FlushInterval (plus the one immediate first chunk, which
// nothing should wait for, and the one final chunk Close writes). What arrives
// is unchanged — every byte, in order.
//
// THE PACE IS MEASURED ON THE SINK'S CLOCK, SO THIS TEST MOVES THAT CLOCK
// RATHER THAN RACING IT. Written against the real one with a one-millisecond
// interval, the assertion was "my write loop finished inside three
// milliseconds": the same policy cost 2 uploads on an idle machine and 9 to 10
// under `-race`, which is a fact about the machine and not about the flush
// policy. Every upload below is one the clock was moved for, and none of the
// discriminating power is lost — a per-line sink writes four thousand and one.
func TestLogChunkSink_ABytesBoundOfOneStillBatchesAChattyTask(t *testing.T) {
	store := &recordingLogStore{}
	// An hour, so no armed timer can fire while the test runs. What is being
	// measured is which writes the POLICY lets through, not when a timer lands.
	const interval = time.Hour
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:    1, // "flush every byte"
		FlushInterval: interval,
		MaxBytes:      1 << 20,
	})
	clock := newTestClock()
	sink.now = clock.now

	const lines = 2000
	var want strings.Builder
	write := func(prefix string) {
		for i := 0; i < lines; i++ {
			line := fmt.Sprintf("%s-%06d", prefix, i)
			sink.Write(line)
			want.WriteString(line)
			want.WriteByte('\n')
		}
	}

	// The first upload is the deliberate exception — whoever is watching a
	// task start waits on that one chunk and on nothing else. Every line after
	// it is buffered, at a size bound of ONE byte, because the pace has not
	// elapsed.
	write("first")
	if got := len(store.stored()); got != 1 {
		t.Fatalf("%d lines cost %d upload(s) inside one interval, want exactly 1 "+
			"(the immediate first chunk): the size bound is writing per line "+
			"instead of batching under the elapsed-time bound", lines, got)
	}

	// Batching is not silence. One interval on, the next write goes out and
	// carries everything that accumulated — without waiting for Close.
	clock.advance(interval)
	sink.Write("after-the-pace")
	want.WriteString("after-the-pace\n")
	if got := len(store.stored()); got != 2 {
		t.Fatalf("a full interval after the first chunk the sink had made %d "+
			"upload(s), want 2: buffered output must go out at the pace, not "+
			"only at Close", got)
	}

	// And the pace re-anchors on that upload: two thousand more lines inside
	// the same interval are two thousand more buffered lines, not 2000 PUTs.
	write("second")
	if got := len(store.stored()); got != 2 {
		t.Fatalf("%d further lines inside one interval cost %d uploads, want 2",
			lines, got)
	}

	sink.Close()

	chunks := store.stored()
	// A fixed bound, not a budget that grows with the test's elapsed time:
	// the immediate first chunk, the one the clock was advanced for, and the
	// terminal one Close writes.
	const maxChunks = 3
	if len(chunks) > maxChunks {
		t.Fatalf("%d lines produced %d chunks, want at most %d: the size bound "+
			"is writing per line instead of batching under the elapsed-time bound",
			2*lines+1, len(chunks), maxChunks)
	}
	if len(chunks) == 0 {
		t.Fatal("nothing was written at all; batching is not silence")
	}
	// Batching may not lose or reorder a byte.
	if got := store.body(); got != want.String() {
		t.Fatalf("the reassembled log is %d bytes, want %d: batching changed what "+
			"the reader gets", len(got), want.Len())
	}
	for i, chunk := range chunks {
		if want := int64(i + 1); chunk.Seq != want {
			t.Fatalf("chunk %d has seq %d, want %d", i, chunk.Seq, want)
		}
	}
	t.Logf("%d lines (%d bytes) cost %d upload(s)",
		2*lines+1, want.Len(), len(chunks))
}

// TestLogChunkSink_HoldsNoMoreThanTheCeilingWhileItWaitsOutThePace is the
// other side of the pace: it may never become a memory leak or a write storm.
//
// Traffic well past the ceiling must apply backpressure until the interval
// permits the next upload. The timestamps make the failure visible: the old
// ceiling escape produced several immediate uploads even with a one-hour
// interval.
func TestLogChunkSink_HoldsNoMoreThanTheCeilingWhileItWaitsOutThePace(t *testing.T) {
	const interval = 20 * time.Millisecond
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:    1,
		FlushInterval: interval,
		MaxBytes:      64 << 20,
		MaxLineBytes:  1 << 20,
	})

	line := strings.Repeat("y", 1023) // 1 KiB with the newline
	for i := 0; i < 400; i++ {        // 400 KiB, well past a 64 KiB ceiling
		sink.Write(line)
	}
	beforeClose := store.stored()
	stamps := store.uploadTimes()
	if len(beforeClose) < 5 {
		t.Fatalf("400 KiB produced %d chunks before Close; traffic past the ceiling was not uploaded while it ran", len(beforeClose))
	}
	for i := 1; i < len(stamps); i++ {
		gap := stamps[i].Sub(stamps[i-1])
		if gap+2*time.Millisecond < interval {
			t.Fatalf("uploads %d and %d were only %s apart, want at least %s: the ceiling bypassed pacing", i, i+1, gap, interval)
		}
	}
	for i, chunk := range beforeClose {
		if len(chunk.Data) > DefaultLogFlushBytes+len(line)+1 {
			t.Errorf("chunk %d is %d bytes, above the ceiling plus one line", i, len(chunk.Data))
		}
	}
	sink.Close()
}

// Sequences are dense and 1-based, because that is the resume contract the
// reader hands clients back as from_seq.
func TestLogChunkSink_NumbersChunksDenselyFromOne(t *testing.T) {
	store := &recordingLogStore{}
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 8, FlushInterval: time.Hour, MaxBytes: 1 << 20,
	}))
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
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 16, FlushInterval: time.Hour, MaxBytes: 1 << 20, MaxLineBytes: 1 << 20,
	}))
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
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:   32,
		MaxLineBytes: 32,
		MaxBytes:     1 << 20, FlushInterval: time.Hour,
	}))
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
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 16, FlushInterval: time.Hour, MaxBytes: 64, MaxLineBytes: 1 << 20,
	}))
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
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 8, FlushInterval: time.Hour, MaxBytes: 32,
	}))
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
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes: 8, FlushInterval: time.Hour, MaxBytes: 1 << 20,
	}))
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

// THE LAST FLUSH IS THE ONE WITH NO NEXT CHUNK. A loss is reported on the
// front of the following chunk, so the failure above is visible; the one that
// hits the FINAL upload has nowhere to be carried, and a stream that ended
// with an ordinary terminator would leave the reader unable to tell a
// complete log from a cut one. The Orker's own warn log is not the stream: the
// person reading the task's output is the one who has to be told.
func TestLogChunkSink_ALossOnTheLastFlushIsStillReportedInTheStream(t *testing.T) {
	store := &recordingLogStore{}
	sink := newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:    1 << 20, // nothing flushes until Close
		FlushInterval: time.Hour,
		MaxBytes:      1 << 20,
	})
	var reported []error
	sink.withErrorReporter(func(err error) { reported = append(reported, err) })
	sink.Write("the only line this task wrote")

	// Every try of the buffer's one and only upload fails, and it is Close
	// that performs it.
	failed := 0
	store.fail = func(int64) error {
		failed++
		if failed <= logChunkUploadTries {
			return errors.New("bucket unavailable")
		}
		return nil
	}
	sink.Close()

	if len(reported) == 0 {
		t.Fatal("the failed upload was not reported to the Orker's logger either")
	}
	body := store.body()
	if !strings.Contains(body, "dropped") {
		t.Fatalf("the attempt's stored stream reads %q; a reader is told nothing about "+
			"the bytes that never landed", body)
	}
	// And the sequences that landed are still dense, which is what a resume
	// rests on.
	for i, chunk := range store.stored() {
		if want := int64(i + 1); chunk.Seq != want {
			t.Fatalf("chunk %d has seq %d, want %d", i, chunk.Seq, want)
		}
	}
}

// The same loss, at the other end that has no next chunk: the truncation
// notice is the last thing an attempt stores, so it carries the loss rather
// than leaving it for a chunk that will never be written.
func TestLogChunkSink_ALossBeforeTruncationRidesTheTruncationNotice(t *testing.T) {
	store := &recordingLogStore{}
	// The cap is reached by the flush that FAILS, so the truncation notice is
	// the very next thing stored — and the only thing that can carry the loss.
	failed := 0
	store.fail = func(int64) error {
		failed++
		if failed <= logChunkUploadTries {
			return errors.New("bucket unavailable")
		}
		return nil
	}
	sink := unpaced(newLogChunkSink(context.Background(), store, testRef(), LogOptions{
		FlushBytes:    1 << 20, // only the cap decides when a chunk is written
		FlushInterval: time.Hour,
		MaxBytes:      32,
		MaxLineBytes:  16,
	}))
	sink.Write(strings.Repeat("x", 40))
	sink.Close()

	var notice string
	for _, chunk := range store.stored() {
		if chunk.Truncated {
			notice = string(chunk.Data)
		}
	}
	if notice == "" {
		t.Fatal("no truncation notice was stored")
	}
	if !strings.Contains(notice, "dropped") {
		t.Fatalf("the truncation notice reads %q, and the bytes lost before it are "+
			"reported nowhere in the stream", notice)
	}
	// Nothing follows the notice: it says the rest was not stored, and a chunk
	// after it would contradict its own words.
	stored := store.stored()
	if last := stored[len(stored)-1]; !last.Truncated {
		t.Fatalf("chunk %d (seq %d) was stored after the truncation notice",
			len(stored)-1, last.Seq)
	}
}

// The runtime streams stdout and stderr from two goroutines, so the sink is
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
