package orker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The live log sink: the write half of M10.5.
//
// A task's output is bytes, so it goes to the artifact store and nowhere else —
// never into a Temporal payload, never onto the Event stream (AGENTS.md §1.4).
// The sink buffers lines and flushes them as numbered chunks WHILE the task
// runs, which is the whole difference between a log you can watch and an
// artifact you can read afterwards.
// Two bounds decide when a flush happens, and they decide it TOGETHER:
//
//   - SIZE says how much may accumulate before a chunk goes out, so a task
//     printing a hundred thousand lines writes a few hundred objects rather
//     than a hundred thousand.
//   - ELAPSED TIME says how long anything may wait before it is visible, so a
//     task printing one line a minute appears within half a second rather
//     than when it finally reaches 64 KiB — which for most tasks is never.
//
// The elapsed-time bound is ALSO the minimum gap between two uploads, and that
// is what makes the pair a pair. Asked alone, the size bound bounds the
// upload count by throughput ÷ FlushBytes, which is not a rate at all: with
// ORK_ORKER_LOG_FLUSH_BYTES=1 every line met the threshold and every line
// became a PUT, so the setting a test uses to force chunk boundaries turned
// batching off completely. A size-triggered flush now waits until
// FlushInterval has passed since the last upload; below that the buffer keeps
// filling and the armed timer writes it at the deadline. The upload rate is
// therefore 1/FlushInterval at EVERY setting, and the size bound decides how
// big a chunk is rather than how often the store is written.
//
// A third bound is the memory guarantee, and it outranks the pace. The buffer
// never exceeds flushCeiling plus one line, a single over-long line is split
// rather than accumulated, and the attempt as a whole stops at MaxBytes with a
// notice chunk that says so. A runaway task therefore costs one bounded buffer
// in the Orker and one bounded object count in the bucket, and its reader is
// told the output was cut instead of quietly receiving a prefix.
const (
	// DefaultLogFlushBytes is the size bound: buffered bytes that trip a flush.
	DefaultLogFlushBytes = 64 << 10
	// DefaultLogFlushInterval is the elapsed-time bound: the longest anything
	// waits unflushed, and the shortest gap between two uploads.
	DefaultLogFlushInterval = 500 * time.Millisecond
	// DefaultLogMaxBytes caps one (run, task, attempt). Past it the sink writes
	// one truncation notice and stops uploading.
	DefaultLogMaxBytes = 16 << 20
	// DefaultLogMaxLineBytes is the longest single line stored whole. A longer
	// one is split across chunks, so one line cannot outgrow the buffer.
	DefaultLogMaxLineBytes = 1 << 20

	// logChunkUploadTries is how many times one chunk's upload is attempted
	// before the bytes are dropped. A log must never fail a task, and it must
	// never block one either: a store that is down stays down for longer than
	// a task can afford to wait.
	logChunkUploadTries = 3
	// logChunkUploadBackoff spaces those tries.
	logChunkUploadBackoff = 100 * time.Millisecond
	// logChunkFinalFlushTimeout bounds a flush that runs after the task's own
	// context was cancelled, so a dead store cannot hold the activity open.
	logChunkFinalFlushTimeout = 5 * time.Second
)

// Environment variables tuning the sink. They are the Orker's, because the
// Orker is the process that buffers and uploads.
const (
	LogFlushBytesVar    = "ORK_ORKER_LOG_FLUSH_BYTES"
	LogFlushIntervalVar = "ORK_ORKER_LOG_FLUSH_INTERVAL"
	LogMaxBytesVar      = "ORK_ORKER_LOG_MAX_BYTES"
)

// LogOptions bounds the live log sink. The zero value is the documented
// default in every field.
type LogOptions struct {
	// FlushBytes is the size bound: how much may accumulate before a chunk is
	// written. It decides how BIG a chunk is; FlushInterval decides how often
	// one is written.
	FlushBytes int
	// FlushInterval is the elapsed-time bound, and does double duty: nothing
	// waits unflushed longer than this, and no two uploads are closer
	// together than this.
	FlushInterval time.Duration
	// MaxBytes caps one attempt's stored output.
	MaxBytes int64
	// MaxLineBytes is the longest line stored whole.
	MaxLineBytes int
}

func (o LogOptions) withDefaults() LogOptions {
	if o.FlushBytes <= 0 {
		o.FlushBytes = DefaultLogFlushBytes
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = DefaultLogFlushInterval
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = DefaultLogMaxBytes
	}
	if o.MaxLineBytes <= 0 {
		o.MaxLineBytes = DefaultLogMaxLineBytes
	}
	return o
}

// LogOptionsFromEnv reads the three tunables an operator may set. Anything
// unset or unparsable keeps its default, because a typo in a log bound must
// not stop an Orker from running tasks.
func LogOptionsFromEnv() LogOptions {
	opts := LogOptions{}
	if raw := os.Getenv(LogFlushBytesVar); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			opts.FlushBytes = parsed
		}
	}
	if raw := os.Getenv(LogFlushIntervalVar); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			opts.FlushInterval = parsed
		}
	}
	if raw := os.Getenv(LogMaxBytesVar); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			opts.MaxBytes = parsed
		}
	}
	return opts.withDefaults()
}

// logChunkSink turns a task attempt's log lines into numbered store chunks.
//
// It is safe for concurrent use: the shell plugin tees stdout and stderr from
// two goroutines, so concurrency is the normal case rather than an edge one.
// The flush happens under the same lock, which is deliberate — a writer blocks
// while a chunk uploads, and that backpressure is what makes the memory bound
// a bound rather than a hope.
type logChunkSink struct {
	ctx   context.Context
	store artifacts.LogStore
	ref   artifacts.LogRef
	opts  LogOptions
	// onError reports an upload that could not be completed. It exists so the
	// Orker's own logger can say so; a nil value discards.
	onError func(error)
	// now reads the clock the minimum-gap bound is measured on. Tests replace
	// it; nothing else does.
	now func() time.Time

	mu sync.Mutex
	// buf holds bytes not yet uploaded. It never exceeds flushCeiling plus one
	// MaxLineBytes piece.
	buf bytes.Buffer
	// seq is the sequence of the LAST chunk stored. It advances only on a
	// successful upload, so the stored sequence space stays dense and a
	// reader's from_seq resume can never skip a hole that was never filled.
	seq int64
	// stored counts uploaded log bytes, against MaxBytes.
	stored int64
	// dropped counts bytes lost to failed uploads, reported in the next chunk.
	dropped int64
	// timer is the elapsed-time bound, armed at the first unflushed byte.
	timer *time.Timer
	// lastFlush is when this sink last wrote to the store, and the anchor the
	// minimum gap is measured from. Zero means nothing has been written yet,
	// which is why a task's FIRST chunk is never delayed by the pace: the
	// person watching a task start is waiting on that one chunk and on
	// nothing else.
	lastFlush time.Time
	// truncated latches once the cap is hit; nothing is written afterwards.
	truncated bool
	closed    bool
}

// newLogChunkSink builds a sink for one attempt. A nil store yields a nil
// sink, which every method tolerates — an Orker with no log store configured
// still runs tasks, it just has nothing to stream them to.
func newLogChunkSink(
	ctx context.Context,
	store artifacts.LogStore,
	ref artifacts.LogRef,
	opts LogOptions,
) *logChunkSink {
	if store == nil {
		return nil
	}
	return &logChunkSink{
		ctx:   ctx,
		store: store,
		ref:   ref,
		opts:  opts.withDefaults(),
		now:   time.Now,
	}
}

// withErrorReporter attaches a callback for uploads that could not complete.
func (s *logChunkSink) withErrorReporter(report func(error)) *logChunkSink {
	if s != nil {
		s.onError = report
	}
	return s
}

// Write adds one line to the stream. A trailing newline is supplied when the
// caller omitted one, so a chunk always ends on a line boundary and a reader
// never renders half a line as a whole one.
func (s *logChunkSink) Write(line string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.truncated {
		return
	}
	data := []byte(line)
	if len(data) == 0 || data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}
	for len(data) > 0 {
		piece := data
		if len(piece) > s.opts.MaxLineBytes {
			piece = piece[:s.opts.MaxLineBytes]
		}
		data = data[len(piece):]
		if s.stored+int64(s.buf.Len())+int64(len(piece)) > s.opts.MaxBytes {
			// Everything up to the cap is made durable first, so the notice is
			// the LAST chunk and a reader sees the prefix it actually got.
			s.flushLocked()
			s.noticeLocked()
			return
		}
		if s.buf.Len() == 0 {
			s.armLocked()
		}
		s.buf.Write(piece)
		switch {
		case s.buf.Len() >= s.flushCeiling():
			// The memory guarantee is a guarantee and outranks the pace: a
			// writer this fast is one the ceiling exists for.
			s.flushLocked()
		case s.buf.Len() >= s.opts.FlushBytes && s.pacedLocked():
			s.flushLocked()
		}
		// Neither: the buffer keeps filling and the timer armed at the first
		// unflushed byte writes it at the deadline. That deferral IS the
		// batching — without it a small FlushBytes turns every line into a
		// PUT (see the package comment above).
	}
}

// Truncated reports whether this attempt's output hit the cap.
func (s *logChunkSink) Truncated() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.truncated
}

// Sequence reports the last stored chunk sequence, for the Orker's own logging.
func (s *logChunkSink) Sequence() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

// Close flushes what is buffered, reports any loss that no later chunk can
// carry, and stops the timer. It is the durability barrier the reader's
// termination rule rests on: ExecuteTask closes the sink BEFORE it returns, so
// every chunk is in the store before the task's terminal event exists, and
// "terminal event observed → one final sweep → end" cannot miss anything.
//
// It is idempotent, so a deferred close and an explicit one are the same close.
func (s *logChunkSink) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.flushLocked()
	// THE LAST FLUSH IS THE ONE WHOSE LOSS HAS NOWHERE TO GO. Loss is
	// otherwise reported on the front of the NEXT chunk (flushLocked), which
	// is a chunk that no longer exists once this returns: a stream whose final
	// upload failed would end with an ordinary terminator and a reader would
	// have no way to tell a complete log from a cut one. So the notice goes
	// out on its own, as the last thing this attempt stores.
	if notice := s.dropNoticeLocked(); notice != nil {
		s.putLocked(notice, false)
	}
	s.disarmLocked()
	s.closed = true
}

// pacedLocked reports whether enough time has passed since the last upload for
// a size-triggered flush to be taken.
//
// It gates ONLY the size trigger. The timer, the cap and Close all flush
// unconditionally: the timer IS the pace, and the other two are the moments
// where holding bytes back would lose them rather than batch them.
func (s *logChunkSink) pacedLocked() bool {
	if s.lastFlush.IsZero() {
		return true
	}
	return s.clock().Sub(s.lastFlush) >= s.opts.FlushInterval
}

// flushCeiling is the most this sink holds while a size-triggered flush waits
// out the pace.
//
// It is the operator's size bound, or the default when that is smaller. The
// two numbers answer different questions: FlushBytes is how big a chunk this
// deployment WANTS, and the ceiling is how much this process is willing to
// hold. They are the same until someone tunes FlushBytes below the default —
// which is what a test does to force chunk boundaries, and what used to turn
// batching off. Below the default, shrinking FlushBytes buys smaller chunks
// and never more uploads per second.
func (s *logChunkSink) flushCeiling() int {
	if s.opts.FlushBytes > DefaultLogFlushBytes {
		return s.opts.FlushBytes
	}
	return DefaultLogFlushBytes
}

func (s *logChunkSink) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// armLocked starts the elapsed-time bound for the buffer that is about to
// receive its first byte.
func (s *logChunkSink) armLocked() {
	s.disarmLocked()
	s.timer = time.AfterFunc(s.opts.FlushInterval, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.flushLocked()
	})
}

func (s *logChunkSink) disarmLocked() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

// flushLocked uploads the buffer as the next chunk. An empty buffer is a
// no-op, which is what makes a timer that fired just after a size-triggered
// flush harmless.
func (s *logChunkSink) flushLocked() {
	s.disarmLocked()
	if s.buf.Len() == 0 {
		return
	}
	notice := s.dropNoticeLocked()
	data := make([]byte, 0, s.buf.Len()+len(notice))
	data = append(data, notice...)
	data = append(data, s.buf.Bytes()...)
	s.buf.Reset()
	s.putLocked(data, false)
}

// dropNoticeLocked returns the loss owed to the reader, clearing the counter,
// or nil when nothing was lost.
//
// Loss is reported in the stream rather than only in the Orker's own log,
// because the person reading the task's output is the one who needs to know a
// piece of it is missing.
func (s *logChunkSink) dropNoticeLocked() []byte {
	if s.dropped == 0 {
		return nil
	}
	notice := fmt.Sprintf(
		"[ork] %d log byte(s) dropped: the log store rejected a chunk\n", s.dropped)
	s.dropped = 0
	return []byte(notice)
}

// noticeLocked writes the single truncation chunk and latches the sink shut.
//
// It carries any outstanding loss with it, because it is the LAST chunk this
// attempt stores: a notice written after it would contradict its own words,
// and one left for a chunk that never comes is a loss the reader never hears
// about.
func (s *logChunkSink) noticeLocked() {
	if s.truncated {
		return
	}
	s.truncated = true
	notice := string(s.dropNoticeLocked()) + fmt.Sprintf(
		"[ork] log truncated: this task attempt passed its %d-byte limit (%s); "+
			"the rest was not stored\n", s.opts.MaxBytes, LogMaxBytesVar)
	s.putLocked([]byte(notice), true)
}

// putLocked uploads one chunk, advancing the sequence only on success.
//
// A failed chunk is DROPPED rather than retried forever or held in memory: a
// log must never fail a task and must never stall one. Because the sequence
// does not advance, what is stored stays dense — a reader resuming at the last
// sequence it saw asks for the next one, and the next one is the next chunk
// that actually landed.
func (s *logChunkSink) putLocked(data []byte, truncated bool) {
	if len(data) == 0 {
		return
	}
	// The pace is anchored on every ATTEMPTED write, not on every successful
	// one. A store that is refusing chunks is the last thing to hammer at the
	// writer's own pace, and each try here already costs the retry loop below.
	s.lastFlush = s.clock()
	chunk := artifacts.LogChunk{
		LogRef:    s.ref,
		Seq:       s.seq + 1,
		Data:      data,
		Truncated: truncated,
	}
	ctx, cancel := s.uploadContext()
	defer cancel()
	var err error
	for try := 0; try < logChunkUploadTries; try++ {
		if try > 0 {
			time.Sleep(logChunkUploadBackoff)
		}
		if err = s.store.PutLogChunk(ctx, chunk); err == nil {
			s.seq = chunk.Seq
			s.stored += int64(len(data))
			return
		}
	}
	s.dropped += int64(len(data))
	if s.onError != nil {
		s.onError(fmt.Errorf("orker: store log chunk %d of task %q in run %q: %w",
			chunk.Seq, s.ref.TaskID, s.ref.RunID, err))
	}
}

// uploadContext is the context a chunk is uploaded under.
//
// A cancelled task must still get its buffered output stored — the log of a
// cancelled run is exactly what somebody reads afterwards — so the final flush
// does not inherit the cancellation that caused it. The replacement is bounded
// by time instead, so a dead store cannot hold the activity open either.
func (s *logChunkSink) uploadContext() (context.Context, context.CancelFunc) {
	base := s.ctx
	if base == nil {
		base = context.Background()
	}
	if base.Err() == nil {
		return context.WithCancel(base)
	}
	return context.WithTimeout(context.WithoutCancel(base), logChunkFinalFlushTimeout)
}
