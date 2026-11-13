package eachlines

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// Options are what Index needs from the plan and the committed source.
type Options struct {
	Format Format
	// MaxItems is the fan-out's max_items: items, not lines.
	MaxItems int
	// ValuesMaxBytes is the plan's values_max_bytes: a line's content limit.
	ValuesMaxBytes int64
	// SliceMaxBytes is SliceMax's value: the largest span of one item.
	SliceMaxBytes int64
	// SourceSize is the committed size of the source in bytes. Index never
	// reads past it, nor past its cap.
	SourceSize int64
	// SourceSHA256, when set, is the committed digest every byte read is held to.
	SourceSHA256 *[32]byte
	// Progress, when set, is called after each read with the bytes read and the
	// non-empty lines seen so far; the Orker heartbeats from it.
	Progress func(bytesRead, lines int64)
}

// gapFlushBytes is how much of the bytes after a line's content, which belong
// to its item's span only if another line of the item follows, are held before
// they are hashed ahead of that knowledge.
const gapFlushBytes = 64 << 10

// Index reads r, the source, in one pass and builds its line index for batches
// of k lines. It reads at most min(cap, o.SourceSize) bytes: a source larger
// than cap is refused (EACH_SOURCE_TOO_LARGE) without a read, and the scan
// stops, reading no further, at the first refusal: a line passing the limit,
// an invalid line, line max_items*k + 1, or a span over the slice limit.
// Memory is one read buffer plus one line (at most values_max_bytes + 2 bytes)
// and the entries, whatever the source. It checks ctx once per read, and a
// reader's own error comes back wrapped and is not a refusal.
func Index(ctx context.Context, r io.Reader, cap int64, k int, o Options) (*Table, error) {
	switch {
	case !o.Format.valid():
		return nil, fmt.Errorf("eachlines: unknown format %d", o.Format)
	case k < 1 || k > MaxBatch:
		return nil, fmt.Errorf("eachlines: batch %d is outside 1 ... %d", k, MaxBatch)
	case o.MaxItems < 1 || int64(o.MaxItems)*int64(k) > math.MaxUint32:
		return nil, fmt.Errorf("eachlines: max_items %d with batch %d is out of range", o.MaxItems, k)
	case o.ValuesMaxBytes < 1 || o.ValuesMaxBytes > 1<<31 || o.SliceMaxBytes < 1 || o.SourceSize < 0:
		return nil, errors.New("eachlines: limits out of range")
	}
	if o.SourceSize > cap {
		return nil, refuse(ReasonSourceTooLarge,
			"the source is %d bytes, over the %d bytes a fan-out of %d items of %d lines reads; the limit counts bytes, so long runs of empty lines count too",
			o.SourceSize, cap, o.MaxItems, k)
	}
	s := &scanner{o: o, k: k, src: sha256.New(), span: sha256.New(), buf: make([]byte, ReadBufferSize), lineStart: 0, physLine: 1}
	src := io.LimitReader(r, o.SourceSize)
	var read int64
	var refusal error // a line or batch refusal, held while the rest is hashed
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := src.Read(s.buf)
		if n > 0 {
			read += int64(n)
			s.src.Write(s.buf[:n])
			if refusal == nil {
				if serr := s.feed(s.buf[:n]); serr != nil {
					if !drains(serr) || o.SourceSHA256 == nil {
						return nil, serr
					}
					refusal = serr
				}
			}
			if o.Progress != nil {
				o.Progress(read, s.nonEmpty)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading the source: %w", err)
		}
	}
	if read != o.SourceSize {
		return nil, refuse(ReasonExpansionFailed, "the source ended after %d bytes, but %d were committed", read, o.SourceSize)
	}
	if refusal != nil {
		// Whose fault the line is depends on whether the source is the committed one.
		if sum := s.src.Sum(nil); !bytes.Equal(sum, o.SourceSHA256[:]) {
			return nil, refuse(ReasonExpansionFailed, "the source's bytes do not match its committed digest")
		}
		return nil, refusal
	}
	if err := s.finish(); err != nil {
		return nil, err
	}
	t := &Table{Entries: s.entries}
	copy(t.SourceSHA256[:], s.src.Sum(nil))
	if o.SourceSHA256 != nil && *o.SourceSHA256 != t.SourceSHA256 {
		return nil, refuse(ReasonExpansionFailed, "the source's bytes do not match its committed digest")
	}
	t.SourceSize = uint64(o.SourceSize)
	t.Format = o.Format
	t.Batch = uint32(k)
	t.Count = uint32(len(s.entries))
	t.TotalLines = uint32(s.nonEmpty)
	if t.Entries == nil {
		t.Entries = []Entry{}
	}
	return t, nil
}

// drains reports whether a refusal is held while the rest of the source is
// hashed (so a corrupted source is told from a bad line); a source with too
// many items is refused at once.
func drains(err error) bool {
	r := ReasonOf(err)
	return r == ReasonLineInvalid || r == ReasonBatchTooLarge
}

// scanner is the state of one pass.
type scanner struct {
	o   Options
	k   int
	src hash.Hash // the whole source
	buf []byte

	head      []byte // the first bytes, until the BOM question is settled
	started   bool
	off       int64  // offset of the next byte to consume
	lineStart int64  // offset of the first byte of the current line
	physLine  int64  // 1-based number of the current line, empty lines counted
	line      []byte // the current line so far, when it spans reads
	nonEmpty  int64

	// the open item
	inBatch   int
	span      hash.Hash // the open item's span so far
	spanStart int64
	spanEnd   int64 // end of the last content in the item
	first     int64
	gap       []byte // the terminators after spanEnd, not yet hashed
	snap      []byte // span's state at spanEnd, once a gap was hashed ahead
	entries   []Entry
}

// feed consumes the next bytes of the source.
func (s *scanner) feed(data []byte) error {
	if !s.started {
		s.head = append(s.head, data...)
		if len(s.head) < 3 {
			return nil
		}
		s.started = true
		data = s.head
		s.head = nil
		if bytes.HasPrefix(data, []byte("\xef\xbb\xbf")) {
			s.off, s.lineStart = 3, 3
			data = data[3:]
		}
		// data may alias s.head, which is not touched again.
	}
	return s.consume(data)
}

func (s *scanner) consume(data []byte) error {
	limit := int(s.o.ValuesMaxBytes) + 1 // a line's content, and the CR after it
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			if len(s.line)+len(data) > limit {
				return s.tooLong()
			}
			s.line = append(s.line, data...)
			s.off += int64(len(data))
			return nil
		}
		raw := data[:i]
		if len(s.line)+i > limit {
			return s.tooLong()
		}
		if len(s.line) > 0 {
			s.line = append(s.line, raw...)
			raw = s.line
		}
		if err := s.handle(raw, true); err != nil {
			return err
		}
		s.line = s.line[:0]
		s.off += int64(i) + 1
		s.lineStart = s.off
		s.physLine++
		data = data[i+1:]
	}
	return nil
}

func (s *scanner) tooLong() error {
	return refuse(ReasonLineInvalid, "line %d is longer than values_max_bytes (%d bytes)", s.physLine, s.o.ValuesMaxBytes)
}

// finish ends the scan at the end of the source.
func (s *scanner) finish() error {
	if !s.started { // fewer than three bytes in all: no BOM possible
		s.started = true
		data := s.head
		s.head = nil
		if err := s.consume(data); err != nil {
			return err
		}
	}
	if len(s.line) > 0 {
		if err := s.handle(s.line, false); err != nil {
			return err
		}
	}
	if s.inBatch > 0 {
		if s.snap != nil { // a gap was hashed ahead; it is not in the span
			if err := s.span.(encoding.BinaryUnmarshaler).UnmarshalBinary(s.snap); err != nil {
				return fmt.Errorf("restoring a hash: %w", err)
			}
		}
		return s.emit()
	}
	return nil
}

// handle processes one line: raw is the line without its LF.
func (s *scanner) handle(raw []byte, hasLF bool) error {
	content, term := raw, "\n"
	if !hasLF {
		term = ""
	}
	if n := len(raw); n > 0 && raw[n-1] == '\r' {
		content = raw[:n-1]
		term = "\r" + term
	}
	if len(content) == 0 {
		s.addGap(term)
		return nil
	}
	s.nonEmpty++
	if s.nonEmpty > int64(s.o.MaxItems)*int64(s.k) {
		return refuse(ReasonTooManyItems, "more than max_items %d batches of %d lines", s.o.MaxItems, s.k)
	}
	if int64(len(content)) > s.o.ValuesMaxBytes {
		return s.tooLong()
	}
	if err := s.validate(content); err != nil {
		return err
	}
	if s.k == 1 {
		if err := s.checkItem(content); err != nil {
			return err
		}
	}
	if s.physLine > math.MaxUint32 {
		return refuse(ReasonSourceTooLarge, "the source has more than %d lines, empty ones counted", uint32(math.MaxUint32))
	}
	end := s.lineStart + int64(len(content))
	if s.inBatch == 0 {
		s.span.Reset()
		s.spanStart, s.first = s.lineStart, s.physLine
	} else {
		s.span.Write(s.gap)
	}
	s.gap, s.snap = s.gap[:0], nil
	if end-s.spanStart > s.o.SliceMaxBytes {
		return refuse(ReasonBatchTooLarge, "the batch holding line %d is over %d bytes; lower batch", s.physLine, s.o.SliceMaxBytes)
	}
	s.span.Write(content)
	s.inBatch++
	s.spanEnd = end
	if s.inBatch == s.k {
		return s.emit()
	}
	s.addGap(term)
	return nil
}

// addGap records terminator bytes after the open item's last content.
func (s *scanner) addGap(term string) {
	if s.inBatch == 0 || term == "" {
		return
	}
	s.gap = append(s.gap, term...)
	if len(s.gap) >= gapFlushBytes {
		// The bytes are in the span only if another line of the item follows.
		// Remember the span as it stands, and hash them: a following line keeps
		// them, the end of the source takes the remembered state back.
		if s.snap == nil {
			s.snap, _ = s.span.(encoding.BinaryMarshaler).MarshalBinary()
		}
		s.span.Write(s.gap)
		s.gap = s.gap[:0]
	}
}

func (s *scanner) emit() error {
	length := s.spanEnd - s.spanStart
	if length > s.o.SliceMaxBytes || length > int64(s.inBatch)*(s.o.ValuesMaxBytes+2) {
		return refuse(ReasonBatchTooLarge, "the batch starting at line %d is %d bytes, over its limit; lower batch", s.first, length)
	}
	e := Entry{
		Offset:    uint64(s.spanStart),
		Length:    uint32(length),
		FirstLine: uint32(s.first),
		Lines:     uint32(s.inBatch),
	}
	copy(e.SHA256[:], s.span.Sum(nil))
	s.entries = append(s.entries, e)
	s.inBatch = 0
	s.gap, s.snap = s.gap[:0], nil
	return nil
}

// validate holds a line's content to the format.
func (s *scanner) validate(content []byte) error {
	if !utf8.Valid(content) {
		return refuse(ReasonLineInvalid, "line %d is not valid UTF-8", s.physLine)
	}
	if s.o.Format != FormatJSONL {
		return nil
	}
	if jsontext.Value(content).IsValid() {
		return nil
	}
	return refuse(ReasonLineInvalid, "line %d is not one valid JSON value: %s", s.physLine, whyInvalid(content))
}

// checkItem holds an unbatched item's encoded value to values_max_bytes: a
// jsonl line compacted is never longer than the line, but a text line
// encoded as a JSON string can be, by its quotes and escapes.
func (s *scanner) checkItem(content []byte) error {
	if s.o.Format != FormatLines || int64(len(content))*6+2 <= s.o.ValuesMaxBytes {
		return nil // six bytes a character is the most an escape takes
	}
	enc, err := encodeText(content)
	if err != nil {
		return fmt.Errorf("encoding line %d: %w", s.physLine, err)
	}
	if int64(len(enc)) > s.o.ValuesMaxBytes {
		return refuse(ReasonLineInvalid, "line %d encodes to %d bytes as an item, over values_max_bytes (%d bytes)", s.physLine, len(enc), s.o.ValuesMaxBytes)
	}
	return nil
}

// whyInvalid words a line's JSON error, naming a key written twice.
func whyInvalid(content []byte) string {
	if len(bytes.TrimSpace(content)) == 0 {
		return "it holds only whitespace"
	}
	var v jsontext.Value
	if err := json.Unmarshal(content, &v); err != nil {
		return stablejson.Explain(err)
	}
	return "it is not a single JSON value"
}

func isJSON(b []byte) bool { return jsontext.Value(b).IsValid() }
