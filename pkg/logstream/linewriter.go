package logstream

import (
	"bytes"
	"strings"
	"sync"
)

// Logger streams one line of a task's live output. A LineWriter's whole job is
// to call one of these, so the two live together.
//
// The line goes to the task's LOG stream — numbered chunks in the artifact
// store, readable while the task runs through RunService.StreamTaskLogs — and
// never onto the run's event stream: task output is bytes, and bytes travel as
// stored objects rather than through Temporal (AGENTS.md §2.2).
//
// It is printf-shaped, so a caller streaming a line it did not compose passes
// it as data: Log("%s", line), never Log(line).
type Logger func(format string, args ...any)

// StderrMarker prefixes every line the Orker streams from a task's STDERR, and
// StdoutMarker is the escape that keeps that marker unambiguous.
//
// One task attempt has ONE log stream — one Logger, one sink, one dense
// sequence — because that is what makes a resume cursor a single number and
// the ordering total and gap-free. So the two file descriptors are
// told apart IN BAND, per line, rather than by a field on the chunk: a field
// would force either one chunk per fd segment (a chunk per line, in an
// interleaving stream) or a sequence space per fd (two cursors, and every
// reader merging them).
//
// stdout travels bare — UNLESS the line would read as a marked one. A stdout
// line that itself begins with "[stderr] " or "[stdout] " is emitted behind
// "[stdout] ", and that one escape is what makes the encoding invertible:
//
//	stream  line the application wrote  line on the wire
//	------  -------------------------   ----------------
//	stdout  hello                       hello
//	stdout  [stderr] hello              [stdout] [stderr] hello
//	stdout  [stdout] hello              [stdout] [stdout] hello
//	stderr  hello                       [stderr] hello
//	stderr  [stderr] hello              [stderr] [stderr] hello
//
// DecodeStreamLine is the inverse, and no two (stream, line) pairs produce the
// same wire line — which is the whole property: an application that prints
// "[stderr] same text" on stdout while another writes "same text" on stderr
// would otherwise render identically, and no reader, CLI or UI, could tell
// their origins apart. The cost is one prefix on a line that was already going
// to be confusing, and stdout that does not collide is untouched.
//
// The Orker's own lifecycle lines travel bare — they are the Orker's voice,
// its `[ork] ` notices — and the Orker does not write itself a line beginning
// with either marker.
//
// The byte-exact separation of the two streams is, as ever, the pair of
// capture artifacts, which this does not touch: an escape belongs to the
// rendered log, never to the file.
const (
	StderrMarker = "[stderr] "
	StdoutMarker = "[stdout] "
)

// DecodeStreamLine reverses the wire shape above: it reports which of a task's
// file descriptors a streamed line came from, and the line the application
// actually wrote.
//
// It is exported because every reader of a task log needs the same answer — a
// pane that tints stderr, a CLI that filters it, a test that counts it — and a
// second implementation of this rule is a second place for it to be wrong. A
// line no LineWriter framed (the Orker's own `[ork] ` notices, a lifecycle
// line) carries no marker and reads as stdout, which is where an unmarked line
// belongs.
func DecodeStreamLine(line string) (text string, stderr bool) {
	if rest, found := strings.CutPrefix(line, StderrMarker); found {
		return rest, true
	}
	if rest, found := strings.CutPrefix(line, StdoutMarker); found {
		return rest, false
	}
	return line, false
}

// MaxStreamedLine bounds one streamed piece.
//
// An application that writes megabytes with no terminator is the writer-side
// way to exhaust memory, so a LineWriter emits a piece every MaxStreamedLine
// bytes instead of waiting for a terminator that may never come. It sits below
// the Orker sink's own per-line limit (1 MiB), so the split happens ONCE, here,
// where the bytes are.
const MaxStreamedLine = 64 << 10

// LineWriter turns a byte stream into whole lines on a Logger.
//
// It is a writer rather than a scanner over a pipe, so it inherits the copying
// goroutine exec.Cmd already runs: a line reaches the logger when the process
// writes it, with no second goroutine to join and nothing to leak if the
// process is killed. It is added to a MultiWriter beside the capture file and
// the payload head buffer — a third reader of the same bytes, never a
// replacement for either.
//
// FRAMING, which is the whole of the contract:
//
//   - '\n' ends a line. So does a bare '\r', and "\r\n" counts once. Bare CR
//     is a terminator on purpose: a redrawing tool (docker pull, pip, curl -#)
//     writes a whole session with no '\n' at all, and treating that as one
//     line would deliver 64 KiB blobs minutes late. The cost is that a log
//     line is no longer byte-identical to the console for CR streams; the
//     capture artifact still is.
//   - A line split across Writes is joined and emitted exactly once, at the
//     Write that completes it. Bytes are never dropped and never doubled,
//     wherever the read boundary falls.
//   - A run of MaxStreamedLine bytes with no terminator is emitted as a piece.
//     A line longer than that is cut into pieces of MaxStreamedLine bytes
//     however its bytes arrive, in one Write or a byte at a time, and the
//     terminator that follows a piece ends no empty line of its own: a line
//     of exactly MaxStreamedLine bytes is one piece, with or without a read
//     boundary at its end.
//   - Close emits whatever is left, which is the final line of a process that
//     ended without a terminator.
//
// BUFFERING POLICY. The writer holds strictly less than MaxStreamedLine bytes:
// one unfinished line, emitted the moment it completes or reaches the limit.
// Cost per line is one mutex acquisition and one buffer append in the sink
// downstream; store writes happen at the SINK's flush bounds (64 KiB or
// 500 ms), so a million-line application costs a few hundred uploads rather
// than a million. The flush there runs under the sink's lock, so a writer
// BLOCKS while a chunk uploads; the block propagates back through the
// MultiWriter to the copying goroutine, the process's pipe fills, and the
// application itself blocks on write(2). That chain is the bound, and it is
// why no queue, no dropping and no per-line upload appear here.
//
// Writes never fail — a log must not be able to fail a task — so Write always
// reports the full length and a nil Logger (or a nil *LineWriter) is a working
// no-op. It is safe for concurrent use, and safe to write to after Close: the
// copying goroutine of a killed process may still be draining a pipe when the
// runtime has already flushed.
//
// It lives in this package rather than beside one caller so that every writer
// frames lines the same way.
type LineWriter struct {
	log    Logger
	prefix string

	mu  sync.Mutex
	buf []byte
	// swallowLF is set right after a '\r' ended a line, so the '\n' of a
	// "\r\n" pair is consumed instead of ending a second, empty one. It is a
	// flag rather than a look-ahead because a look-ahead would have to WAIT
	// for the byte after a CR, and the whole point of ending a line on CR is
	// that a redraw streams immediately.
	swallowLF bool
	// pieceEnded is set when a piece was emitted because the buffer reached
	// MaxStreamedLine with no terminator seen, and cleared when the line it
	// belongs to ends. A terminator that arrives before any other byte then
	// ends that line without emitting an empty one, so the lines out do not
	// depend on where the reads happened to split the stream.
	pieceEnded bool
}

// NewLineWriter builds a LineWriter that emits every line to log, each carrying
// prefix (use StderrMarker for a task's stderr, "" for its stdout). A nil log
// yields a writer that accepts and discards.
//
// A writer with no prefix is the one that ESCAPES: a bare line that would read
// as a marked one is emitted behind StdoutMarker (see the wire shape above).
// A writer that already prefixes every line it emits needs no escape — its
// prefix claims the line, and DecodeStreamLine strips exactly one.
func NewLineWriter(log Logger, prefix string) *LineWriter {
	return &LineWriter{log: log, prefix: prefix}
}

// Write frames p into lines. It never returns an error and never reports a
// short write, so it is safe inside an io.MultiWriter beside writers that can.
func (w *LineWriter) Write(p []byte) (int, error) {
	if w == nil || w.log == nil {
		return len(p), nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	w.drainLocked()
	if len(w.buf) == 0 {
		// Reclaim the backing array once it is consumed, so a long stream does
		// not hold on to the largest slice it ever saw.
		w.buf = nil
	}
	return len(p), nil
}

// Close emits the final unterminated line, if there is one. It is idempotent —
// a deferred close and the explicit one before an exit code is interpreted are
// the same close — and the writer keeps working afterwards.
func (w *LineWriter) Close() {
	if w == nil || w.log == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		w.emitLocked(w.buf)
		w.buf = nil
	}
	w.swallowLF = false
	w.pieceEnded = false
}

// buffered reports how many bytes are held unemitted. It exists for the test
// that pins the memory bound.
func (w *LineWriter) buffered() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buf)
}

// drainLocked emits every complete line the buffer now holds, plus any piece
// that reached the limit without a terminator.
func (w *LineWriter) drainLocked() {
	for {
		if w.swallowLF {
			if len(w.buf) == 0 {
				return
			}
			if w.buf[0] == '\n' {
				w.buf = w.buf[1:]
			}
			w.swallowLF = false
		}
		at, kind := indexTerminator(w.buf)
		switch {
		case at >= 0 && at <= MaxStreamedLine:
			if at > 0 || !w.pieceEnded {
				w.emitLocked(w.buf[:at])
			}
			w.buf = w.buf[at+1:]
			w.swallowLF = kind == '\r'
			w.pieceEnded = false
		case len(w.buf) >= MaxStreamedLine:
			// No terminator within the first MaxStreamedLine bytes: cut a
			// piece here, whether or not a terminator comes later in the
			// buffer, so a line is the same pieces however it arrived.
			w.emitLocked(w.buf[:MaxStreamedLine])
			w.buf = w.buf[MaxStreamedLine:]
			w.pieceEnded = true
		default:
			return
		}
	}
}

// emitLocked hands one line to the logger as DATA. Logger is printf-shaped, so
// a line carrying a percent sign must go through %s or a task printing "100%"
// streams "100%!(NOVERB)".
func (w *LineWriter) emitLocked(line []byte) {
	w.log("%s", w.prefix+w.escape(string(line)))
}

// escape keeps a bare line from reading as a marked one. It applies only to
// the writer that emits bare lines, and only to a line that begins with a
// marker — every other line, which is every line a normal application writes,
// travels byte for byte as it always did.
func (w *LineWriter) escape(line string) string {
	if w.prefix != "" {
		return line
	}
	if strings.HasPrefix(line, StderrMarker) || strings.HasPrefix(line, StdoutMarker) {
		return StdoutMarker + line
	}
	return line
}

// indexTerminator reports the first '\n' or '\r' in b, and which it is.
func indexTerminator(b []byte) (int, byte) {
	lf := bytes.IndexByte(b, '\n')
	cr := bytes.IndexByte(b, '\r')
	switch {
	case lf < 0 && cr < 0:
		return -1, 0
	case lf < 0:
		return cr, '\r'
	case cr < 0:
		return lf, '\n'
	case cr < lf:
		return cr, '\r'
	default:
		return lf, '\n'
	}
}
