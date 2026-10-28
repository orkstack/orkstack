package logstream

import (
	"strconv"
	"strings"
	"testing"
)

// frame is the framing contract written without the LineWriter: '\n' ends a
// line, so does a bare '\r', "\r\n" counts once, a run of MaxStreamedLine
// bytes with no terminator is emitted as a piece, and a final unterminated
// piece is emitted at Close.
func frame(stream string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(stream); i++ {
		switch {
		case stream[i] == '\n' || stream[i] == '\r':
			lines = append(lines, splitLong(stream[start:i])...)
			if stream[i] == '\r' && i+1 < len(stream) && stream[i+1] == '\n' {
				i++
			}
			start = i + 1
		}
	}
	if start < len(stream) {
		lines = append(lines, splitLong(stream[start:])...)
	}
	return lines
}

// splitLong cuts a terminator-free run into pieces of MaxStreamedLine bytes;
// a run that is an exact multiple leaves no empty tail.
func splitLong(run string) []string {
	if len(run) < MaxStreamedLine {
		return []string{run}
	}
	var pieces []string
	for len(run) >= MaxStreamedLine {
		pieces = append(pieces, run[:MaxStreamedLine])
		run = run[MaxStreamedLine:]
	}
	if run != "" {
		pieces = append(pieces, run)
	}
	return pieces
}

// shape renders lines compactly for a failure: their count and lengths, and
// the text itself when it is short.
func shape(lines []string) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(lines)) + " lines [")
	for i, line := range lines {
		if i > 0 {
			b.WriteString(" ")
		}
		if len(line) <= 24 {
			b.WriteString(strconv.Quote(line))
		} else {
			b.WriteString(strconv.Itoa(len(line)) + " bytes")
		}
	}
	return b.String() + "]"
}

// FuzzLineWriter_FramesAnyStreamAtAnyWriteBoundary: wherever the reads split a
// stream, the lines out are the stream's lines — none dropped, none doubled —
// and every line a writer emits decodes back to the stream it came from and
// the exact line the application wrote, so two writers can never produce one
// wire line.
func FuzzLineWriter_FramesAnyStreamAtAnyWriteBoundary(f *testing.F) {
	for _, seed := range []string{
		"", "hello\n", "a\r\nb\rc\nd", "[stderr] x\n", "[stdout] y\n[stderr] z", "100%\n", "\r\r\n\n", "no terminator", "\n\n\n", "a\r", "\r\nb",
		"[stderr] ", "[stdout] ", "[stderr]\n", "\xff\xfe\n", strings.Repeat("x", MaxStreamedLine) + "\n", strings.Repeat("y", MaxStreamedLine+3),
	} {
		f.Add(seed, uint32(0), uint32(3))
		f.Add(seed, uint32(1), uint32(1))
		f.Add(seed, uint32(MaxStreamedLine), uint32(1))
		f.Add(seed, uint32(MaxStreamedLine-1), uint32(2))
	}
	f.Fuzz(func(t *testing.T, stream string, cutA, cutB uint32) {
		if len(stream) > 4*MaxStreamedLine {
			return
		}
		want := frame(stream)
		for _, prefix := range []string{"", StderrMarker} {
			var whole, split recorder
			one := NewLineWriter(whole.logger(), prefix)
			one.Write([]byte(stream))
			one.Close()

			// Three writes, cut where the fuzz arguments say.
			a := int(cutA % uint32(len(stream)+1))
			b := a + int(cutB%uint32(len(stream)-a+1))
			many := NewLineWriter(split.logger(), prefix)
			many.Write([]byte(stream[:a]))
			many.Write([]byte(stream[a:b]))
			many.Write([]byte(stream[b:]))
			many.Close()

			if !equalLines(whole.got(), split.got()) {
				t.Fatalf("prefix %q: one write gave %s, writes cut at %d and %d gave %s", prefix, shape(whole.got()), a, b, shape(split.got()))
			}
			got := whole.got()
			if len(got) != len(want) {
				t.Fatalf("prefix %q: a stream of %d bytes framed as %s, want %s", prefix, len(stream), shape(got), shape(want))
			}
			for i, wire := range got {
				text, stderr := DecodeStreamLine(wire)
				if text != want[i] || stderr != (prefix == StderrMarker) {
					t.Fatalf("prefix %q: line %d is %q on the wire, which decodes to (%q, stderr=%v), want (%q, stderr=%v)",
						prefix, i, wire, text, stderr, want[i], prefix == StderrMarker)
				}
			}
		}
	})
}

// FuzzDecodeStreamLine_IsInjectiveOverWhatWritersEmit: no two (stream, line)
// pairs share a wire line.
func FuzzDecodeStreamLine_IsInjectiveOverWhatWritersEmit(f *testing.F) {
	for _, seed := range []string{"same text", "[stderr] same text", "[stdout] same text", "[stdout] [stderr] x", "", "[stderr]", "[stdout]"} {
		f.Add(seed, "same text")
	}
	f.Fuzz(func(t *testing.T, line, other string) {
		if strings.ContainsAny(line+other, "\r\n") || len(line) >= MaxStreamedLine || len(other) >= MaxStreamedLine {
			return
		}
		emit := func(prefix, text string) string {
			var r recorder
			w := NewLineWriter(r.logger(), prefix)
			w.Write([]byte(text + "\n"))
			w.Close()
			if got := r.got(); len(got) == 1 {
				return got[0]
			}
			t.Fatalf("%q with prefix %q was not one line: %q", text, prefix, r.got())
			return ""
		}
		type pair struct {
			stderr bool
			text   string
		}
		wires := map[string]pair{}
		for _, p := range []pair{{false, line}, {true, line}, {false, other}, {true, other}} {
			prefix := ""
			if p.stderr {
				prefix = StderrMarker
			}
			wire := emit(prefix, p.text)
			if text, stderr := DecodeStreamLine(wire); text != p.text || stderr != p.stderr {
				t.Fatalf("%+v is %q on the wire, which decodes to (%q, %v)", p, wire, text, stderr)
			}
			if previous, clash := wires[wire]; clash && previous != p {
				t.Fatalf("%+v and %+v are both %q on the wire", previous, p, wire)
			}
			wires[wire] = p
		}
	})
}
