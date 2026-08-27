package logstream

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// recorder collects what a LineWriter emitted. It locks because the two
// streams of one process are written from two goroutines, which is the normal
// case rather than an edge one.
type recorder struct {
	mu    sync.Mutex
	lines []string
}

func (r *recorder) logger() Logger {
	return func(format string, args ...any) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.lines = append(r.lines, fmt.Sprintf(format, args...))
	}
}

func (r *recorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lines...)
}

func equalLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The framing table of the design (v004-design.md §1.4). Each row is a
// sequence of Writes — the split points matter, because a docker read boundary
// falls wherever it likes — followed by Close.
func TestLineWriter_FramesLinesAcrossWriteBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   []string
	}{
		{
			name:   "whole lines in one write",
			writes: []string{"a\nb\n"},
			want:   []string{"a", "b"},
		},
		{
			name:   "one line split across two writes lands exactly once",
			writes: []string{"hal", "f-and-half\n"},
			want:   []string{"half-and-half"},
		},
		{
			name:   "a newline arriving on its own does not duplicate the line",
			writes: []string{"trailing", "\n", "next\n"},
			want:   []string{"trailing", "next"},
		},
		{
			name:   "an unterminated final line is flushed on Close",
			writes: []string{"x"},
			want:   []string{"x"},
		},
		{
			name:   "CRLF is one terminator, not two",
			writes: []string{"p\r\n"},
			want:   []string{"p"},
		},
		{
			name:   "CRLF split between writes is still one terminator",
			writes: []string{"p\r", "\n"},
			want:   []string{"p"},
		},
		{
			name:   "a bare CR ends a line, so a redraw streams instead of piling up",
			writes: []string{"p1\rp2\rp3\n"},
			want:   []string{"p1", "p2", "p3"},
		},
		{
			name:   "a trailing bare CR is a line of its own at Close",
			writes: []string{"done\r"},
			want:   []string{"done"},
		},
		{
			name:   "empty lines are output too",
			writes: []string{"\n\n"},
			want:   []string{"", ""},
		},
		{
			name:   "CRLF empty lines count once each",
			writes: []string{"\r\n\r\n"},
			want:   []string{"", ""},
		},
		{
			name:   "nothing written emits nothing",
			writes: nil,
			want:   nil,
		},
		{
			name:   "a stream of only a terminator is one empty line",
			writes: []string{"\n"},
			want:   []string{""},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			w := NewLineWriter(rec.logger(), "")
			for _, chunk := range tc.writes {
				n, err := w.Write([]byte(chunk))
				if err != nil {
					t.Fatalf("Write(%q): %v", chunk, err)
				}
				if n != len(chunk) {
					t.Fatalf("Write(%q) reported %d bytes, want %d", chunk, n, len(chunk))
				}
			}
			w.Close()
			if got := rec.got(); !equalLines(got, tc.want) {
				t.Errorf("emitted %q, want %q", got, tc.want)
			}
		})
	}
}

// Byte-for-byte: every byte written that is not a terminator comes out exactly
// once, whatever the write boundaries are. This is the property a drop or a
// duplicate breaks, and the table above only samples it.
func TestLineWriter_LosesAndDuplicatesNothingAcrossEveryWriteBoundary(t *testing.T) {
	const stream = "alpha\nbeta\r\ngamma\rdelta\n\nomega"
	want := []string{"alpha", "beta", "gamma", "delta", "", "omega"}

	for split := 0; split <= len(stream); split++ {
		rec := &recorder{}
		w := NewLineWriter(rec.logger(), "")
		if _, err := w.Write([]byte(stream[:split])); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if _, err := w.Write([]byte(stream[split:])); err != nil {
			t.Fatalf("Write: %v", err)
		}
		w.Close()
		if got := rec.got(); !equalLines(got, want) {
			t.Fatalf("split at %d emitted %q, want %q", split, got, want)
		}
	}
}

// The marker is what makes stderr legible in a stream both fds share. It is
// per line, so a multi-line stderr burst is marked on every line rather than
// only on the first.
func TestLineWriter_PrefixesEveryLineItEmits(t *testing.T) {
	rec := &recorder{}
	w := NewLineWriter(rec.logger(), StderrMarker)
	if _, err := w.Write([]byte("first\nsecond\nthird")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	w.Close()

	want := []string{
		StderrMarker + "first",
		StderrMarker + "second",
		StderrMarker + "third",
	}
	if got := rec.got(); !equalLines(got, want) {
		t.Errorf("emitted %q, want %q", got, want)
	}
}

// The property the marker exists for, stated as the case that used to break
// it: an application printing "[stderr] same text" on STDOUT while another
// writes "same text" on STDERR. Both used to render as "[stderr] same text",
// so no reader — orkctl, the UI pane, or a test — could tell their origins
// apart. They must not render alike, and each must decode back to the stream
// and the text it came from.
func TestLineWriter_StdoutThatLooksLikeStderrStaysDistinguishable(t *testing.T) {
	rec := &recorder{}
	out := NewLineWriter(rec.logger(), "")
	errs := NewLineWriter(rec.logger(), StderrMarker)

	if _, err := out.Write([]byte(StderrMarker + "same text\n")); err != nil {
		t.Fatalf("Write(stdout): %v", err)
	}
	if _, err := errs.Write([]byte("same text\n")); err != nil {
		t.Fatalf("Write(stderr): %v", err)
	}
	out.Close()
	errs.Close()

	lines := rec.got()
	if len(lines) != 2 {
		t.Fatalf("emitted %q, want two lines", lines)
	}
	if lines[0] == lines[1] {
		t.Fatalf("stdout and stderr both render as %q; a log reader cannot tell "+
			"their origins apart", lines[0])
	}

	text, stderr := DecodeStreamLine(lines[0])
	if stderr || text != StderrMarker+"same text" {
		t.Errorf("the stdout line decodes as (%q, stderr=%v), want (%q, false)",
			text, stderr, StderrMarker+"same text")
	}
	text, stderr = DecodeStreamLine(lines[1])
	if !stderr || text != "same text" {
		t.Errorf("the stderr line decodes as (%q, stderr=%v), want (%q, true)",
			text, stderr, "same text")
	}
}

// The encoding is invertible for EVERY line, not only the colliding one: what
// a reader decodes is the stream the bytes came from and the bytes themselves.
// A table of the lines that could confuse it, each written on each stream.
func TestLineWriter_EveryLineDecodesBackToItsStreamAndItsText(t *testing.T) {
	for _, line := range []string{
		"plain",
		"",
		StderrMarker + "looks like stderr",
		StdoutMarker + "looks like stdout",
		StderrMarker + StderrMarker + "doubly so",
		StdoutMarker + StderrMarker + "one of each",
		"[stderr]no space, so not the marker",
		"trailing " + StderrMarker,
	} {
		for _, stream := range []struct {
			name   string
			prefix string
			stderr bool
		}{
			{name: "stdout", prefix: "", stderr: false},
			{name: "stderr", prefix: StderrMarker, stderr: true},
		} {
			t.Run(fmt.Sprintf("%s/%q", stream.name, line), func(t *testing.T) {
				rec := &recorder{}
				w := NewLineWriter(rec.logger(), stream.prefix)
				if _, err := w.Write([]byte(line + "\n")); err != nil {
					t.Fatalf("Write: %v", err)
				}
				w.Close()

				got := rec.got()
				if len(got) != 1 {
					t.Fatalf("emitted %q, want one line", got)
				}
				text, stderr := DecodeStreamLine(got[0])
				if stderr != stream.stderr || text != line {
					t.Errorf("%q on %s went out as %q and decodes as (%q, stderr=%v)",
						line, stream.name, got[0], text, stderr)
				}
			})
		}
	}
}

// An ordinary stdout line pays nothing for the escape: the bytes on the wire
// are the bytes the application wrote. The escape is for lines that would
// otherwise be ambiguous, and for no others.
func TestLineWriter_LeavesOrdinaryStdoutLinesBare(t *testing.T) {
	rec := &recorder{}
	w := NewLineWriter(rec.logger(), "")
	if _, err := w.Write([]byte("out-1\n[ork] a notice\n  [stderr] indented\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	w.Close()

	want := []string{"out-1", "[ork] a notice", "  [stderr] indented"}
	if got := rec.got(); !equalLines(got, want) {
		t.Errorf("emitted %q, want %q", got, want)
	}
}

// A log line is DATA. A task printing a percent sign must not have it read as
// a verb — that is how "100%" becomes "100%!(NOVERB)".
func TestLineWriter_EmitsLinesAsDataNotFormatStrings(t *testing.T) {
	rec := &recorder{}
	w := NewLineWriter(rec.logger(), StderrMarker)
	if _, err := w.Write([]byte("progress: 100% (%d of %s)\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	w.Close()

	want := []string{StderrMarker + "progress: 100% (%d of %s)"}
	if got := rec.got(); !equalLines(got, want) {
		t.Errorf("emitted %q, want %q", got, want)
	}
}

// A run of bytes with no terminator is the plugin-side way to exhaust memory.
// It is emitted in pieces as it accumulates, so the writer holds less than one
// piece at any moment and every byte still arrives exactly once.
func TestLineWriter_SplitsAnUnterminatedRunAtTheStreamedLineLimit(t *testing.T) {
	const total = MaxStreamedLine*2 + 17
	rec := &recorder{}
	w := NewLineWriter(rec.logger(), "")

	written := 0
	for written < total {
		piece := 7919 // a prime, so no write boundary lines up with the limit
		if written+piece > total {
			piece = total - written
		}
		if _, err := w.Write([]byte(strings.Repeat("x", piece))); err != nil {
			t.Fatalf("Write: %v", err)
		}
		written += piece
		if held := w.buffered(); held >= MaxStreamedLine {
			t.Fatalf("the writer holds %d bytes, at or above the %d-byte piece limit",
				held, MaxStreamedLine)
		}
	}
	w.Close()

	lines := rec.got()
	sum := 0
	for i, line := range lines {
		if strings.Trim(line, "x") != "" {
			t.Fatalf("piece %d is not the written bytes: %q", i, line)
		}
		if len(line) > MaxStreamedLine {
			t.Errorf("piece %d is %d bytes, above the %d-byte limit",
				i, len(line), MaxStreamedLine)
		}
		sum += len(line)
	}
	if len(lines) < 3 {
		t.Fatalf("%d bytes with no terminator produced %d piece(s); it was not split",
			total, len(lines))
	}
	if sum != total {
		t.Errorf("the pieces total %d bytes, want %d: bytes were lost or duplicated", sum, total)
	}
}

// A plugin with no logger still runs: nothing about streaming may fail a task,
// and a nil writer is what a plugin that never built one holds.
func TestLineWriter_WithoutALoggerIsAWorkingNoOp(t *testing.T) {
	for name, w := range map[string]*LineWriter{
		"nil logger": NewLineWriter(nil, ""),
		"nil writer": nil,
	} {
		t.Run(name, func(t *testing.T) {
			n, err := w.Write([]byte("anything\n"))
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			if n != len("anything\n") {
				t.Fatalf("Write reported %d bytes, want %d", n, len("anything\n"))
			}
			w.Close()
		})
	}
}

// Close is the flush the caller may reach twice — a deferred one and an
// explicit one before the exit code is read. The second must not re-emit the
// line the first flushed.
func TestLineWriter_CloseIsIdempotent(t *testing.T) {
	rec := &recorder{}
	w := NewLineWriter(rec.logger(), "")
	if _, err := w.Write([]byte("only-once")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	w.Close()
	w.Close()

	if got := rec.got(); !equalLines(got, []string{"only-once"}) {
		t.Errorf("emitted %q, want the line exactly once", got)
	}
}

// A write that arrives after Close cannot be lost silently and cannot panic:
// exec.Cmd's copy goroutine may still be draining a pipe when the plugin has
// already flushed.
func TestLineWriter_KeepsStreamingAfterClose(t *testing.T) {
	rec := &recorder{}
	w := NewLineWriter(rec.logger(), "")
	w.Close()
	if _, err := w.Write([]byte("late\n")); err != nil {
		t.Fatalf("Write after Close: %v", err)
	}
	if got := rec.got(); !equalLines(got, []string{"late"}) {
		t.Errorf("emitted %q, want the late line", got)
	}
}
