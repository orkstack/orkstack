package eachlines

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// Index format v1 (all integers little-endian):
//
//	header 64 B: magic "ORKLIDX" [7] | version u8 = 1 | source sha256 [32] |
//	             source size u64 | format u8 | reserved [3] = 0 | batch u32 |
//	             count u32 (items) | total_lines u32
//	entry  56 B: offset u64 | length u32 | first file line u32 | lines u32 |
//	             reserved u32 = 0 | sha256 of the span [32]
//
// Its size is 64 + 56 * count whatever K is. An entry's first file line is the
// 1-based number of the physical line, empty ones counted, its span starts on.
const (
	HeaderSize = 64
	EntrySize  = 56

	magic   = "ORKLIDX"
	version = 1
)

// Header is the index's header.
type Header struct {
	SourceSHA256 [32]byte
	SourceSize   uint64
	Format       Format
	Batch        uint32 // K
	Count        uint32 // items
	TotalLines   uint32 // non-empty lines
}

// Entry is one item: where its span lies and what it holds.
type Entry struct {
	Offset    uint64
	Length    uint32
	FirstLine uint32
	Lines     uint32
	SHA256    [32]byte // of exactly the span's raw bytes
}

// Table is a decoded line index: item i is Entries[i].
type Table struct {
	Header
	Entries []Entry
}

// Encode is the v1 encoding of t, deterministic.
func (t *Table) Encode() []byte {
	b := make([]byte, HeaderSize+EntrySize*len(t.Entries))
	copy(b, magic)
	b[7] = version
	copy(b[8:40], t.SourceSHA256[:])
	binary.LittleEndian.PutUint64(b[40:], t.SourceSize)
	b[48] = byte(t.Format)
	binary.LittleEndian.PutUint32(b[52:], t.Batch)
	binary.LittleEndian.PutUint32(b[56:], t.Count)
	binary.LittleEndian.PutUint32(b[60:], t.TotalLines)
	for i, e := range t.Entries {
		p := b[HeaderSize+EntrySize*i:]
		binary.LittleEndian.PutUint64(p[0:], e.Offset)
		binary.LittleEndian.PutUint32(p[8:], e.Length)
		binary.LittleEndian.PutUint32(p[12:], e.FirstLine)
		binary.LittleEndian.PutUint32(p[16:], e.Lines)
		copy(p[24:56], e.SHA256[:])
	}
	return b
}

// errCorrupt is what Decode wraps: the bytes are not a valid v1 index.
var errCorrupt = errors.New("not a valid line index")

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errCorrupt, fmt.Sprintf(format, args...))
}

// IsCorrupt reports whether err is Decode's refusal of the bytes.
func IsCorrupt(err error) bool { return errors.Is(err, errCorrupt) }

// Decode reads a v1 index and refuses anything that contradicts itself: bad
// magic, an unknown version or format, a non-zero reserved field, batch 0, a
// length other than 64 + 56 * count, count other than ceil(total_lines /
// batch), an entry whose lines are not batch (the last: 1 ... batch), lines
// that do not add up to total_lines, offsets that do not increase or spans that
// overlap, a span past the source or too short to hold its lines, and a first
// line that goes backwards. The reader's own limit on a span is CheckLimits.
func Decode(b []byte) (*Table, error) {
	if len(b) < HeaderSize {
		return nil, corrupt("%d bytes, shorter than the %d-byte header", len(b), HeaderSize)
	}
	if !bytes.Equal(b[:7], []byte(magic)) {
		return nil, corrupt("bad magic")
	}
	if b[7] != version {
		return nil, corrupt("unknown version %d", b[7])
	}
	t := &Table{}
	copy(t.SourceSHA256[:], b[8:40])
	t.SourceSize = binary.LittleEndian.Uint64(b[40:])
	t.Format = Format(b[48])
	if !t.Format.valid() {
		return nil, corrupt("unknown format %d", b[48])
	}
	if b[49] != 0 || b[50] != 0 || b[51] != 0 {
		return nil, corrupt("reserved header bytes are not zero")
	}
	t.Batch = binary.LittleEndian.Uint32(b[52:])
	t.Count = binary.LittleEndian.Uint32(b[56:])
	t.TotalLines = binary.LittleEndian.Uint32(b[60:])
	if t.Batch == 0 {
		return nil, corrupt("batch is 0")
	}
	if want := uint64(HeaderSize) + uint64(EntrySize)*uint64(t.Count); uint64(len(b)) != want {
		return nil, corrupt("%d bytes for %d items, want %d", len(b), t.Count, want)
	}
	if want := (uint64(t.TotalLines) + uint64(t.Batch) - 1) / uint64(t.Batch); uint64(t.Count) != want {
		return nil, corrupt("%d items for %d lines in batches of %d, want %d", t.Count, t.TotalLines, t.Batch, want)
	}
	t.Entries = make([]Entry, t.Count)
	var sum, prevEnd, prevFirst, prevLines uint64
	for i := range t.Entries {
		p := b[HeaderSize+EntrySize*i:]
		e := &t.Entries[i]
		e.Offset = binary.LittleEndian.Uint64(p[0:])
		e.Length = binary.LittleEndian.Uint32(p[8:])
		e.FirstLine = binary.LittleEndian.Uint32(p[12:])
		e.Lines = binary.LittleEndian.Uint32(p[16:])
		if binary.LittleEndian.Uint32(p[20:]) != 0 {
			return nil, corrupt("item %d: reserved field is not zero", i)
		}
		copy(e.SHA256[:], p[24:56])
		lines := uint64(e.Lines)
		last := i == len(t.Entries)-1
		if lines < 1 || lines > uint64(t.Batch) || (!last && lines != uint64(t.Batch)) {
			return nil, corrupt("item %d holds %d lines in batches of %d", i, e.Lines, t.Batch)
		}
		if i > 0 && e.Offset <= prevEnd { // at least the LF between two spans
			return nil, corrupt("item %d starts at %d, inside or against the item before", i, e.Offset)
		}
		if e.Offset > t.SourceSize || uint64(e.Length) > t.SourceSize-e.Offset {
			return nil, corrupt("item %d ends past the source's %d bytes", i, t.SourceSize)
		}
		if uint64(e.Length) < 2*lines-1 {
			return nil, corrupt("item %d: %d bytes cannot hold %d lines", i, e.Length, e.Lines)
		}
		if e.FirstLine < 1 || uint64(e.FirstLine) < sum+1 || (i > 0 && uint64(e.FirstLine) < prevFirst+prevLines) {
			return nil, corrupt("item %d: first line %d does not follow the items before", i, e.FirstLine)
		}
		sum += lines
		prevEnd, prevFirst, prevLines = e.Offset+uint64(e.Length), uint64(e.FirstLine), lines
	}
	if sum != uint64(t.TotalLines) {
		return nil, corrupt("items hold %d lines, header says %d", sum, t.TotalLines)
	}
	return t, nil
}

// CheckLimits refuses an index holding a span longer than its lines could
// make, lines x (valuesMaxBytes + 2): the reader's own limit, from its plan,
// which no index can state for it.
func (t *Table) CheckLimits(valuesMaxBytes int64) error {
	if valuesMaxBytes < 0 || valuesMaxBytes > 1<<31 {
		return fmt.Errorf("values_max_bytes %d out of range", valuesMaxBytes)
	}
	for i, e := range t.Entries {
		if int64(e.Length) > int64(e.Lines)*(valuesMaxBytes+2) {
			return corrupt("item %d spans %d bytes, more than %d lines of at most %d bytes", i, e.Length, e.Lines, valuesMaxBytes)
		}
	}
	return nil
}
