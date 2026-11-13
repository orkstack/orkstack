package eachlines

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// refLines is the independent reference for the line rules: the non-empty
// lines of data, in order, and whether every one is acceptable to format.
func refLines(data []byte, f Format, k, vmax int) (lines [][]byte, valid bool) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	valid = true
	for len(data) > 0 {
		var line []byte
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			line, data = data[:i], data[i+1:]
		} else {
			line, data = data, nil
		}
		line = bytes.TrimSuffix(line, []byte("\r"))
		if len(line) == 0 {
			continue
		}
		lines = append(lines, line)
		if len(line) > vmax || !utf8.Valid(line) {
			valid = false
		}
		if f == FormatJSONL && !jsontext.Value(line).IsValid() {
			valid = false
		}
		if f == FormatLines && k == 1 && utf8.Valid(line) {
			if enc, _ := json.Marshal(string(line), stablejson.Unescaped); len(enc) > vmax {
				valid = false // an unbatched item's encoded value is bounded too
			}
		}
	}
	return lines, valid
}

func FuzzEachLinesSplit(f *testing.F) {
	seeds := []string{
		"", "a\n", "a\nb", "a\r\nb\r\n", "\xef\xbb\xbfa\n", "a\n\n\r\nb\r", "{\"a\":1}\n[2]\n3\n",
		" \n", "\xff\n", "{\"a\":1,\"a\":2}\n", "a\rb\n", "\r\r\n", "1 2\n", "\n\n", "x\r",
	}
	for _, s := range seeds {
		for _, k := range []byte{1, 2, 3} {
			f.Add([]byte(s), k, false)
			f.Add([]byte(s), k, true)
		}
	}
	const vmax = 64
	f.Fuzz(func(t *testing.T, data []byte, kb byte, jsonl bool) {
		k := int(kb)%64 + 1
		format := FormatLines
		if jsonl {
			format = FormatJSONL
		}
		const maxItems = 1 << 16
		o := opts(format, k, maxItems, data)
		o.ValuesMaxBytes = vmax
		o.SliceMaxBytes = SliceMax(k, vmax, testPlanSlice)
		capBytes := ReadCap(maxItems, o.SliceMaxBytes, 5<<30)
		reader := &countingReaderOver{data: data}
		tab, err := Index(context.Background(), reader, capBytes, k, o)
		if reader.read > int64(len(data)) || reader.read > capBytes {
			t.Fatalf("read %d bytes of %d (cap %d)", reader.read, len(data), capBytes)
		}
		want, valid := refLines(data, format, k, vmax)
		if err != nil {
			switch ReasonOf(err) {
			case ReasonBatchTooLarge:
				if !valid {
					return // an invalid line may come first
				}
				if !someSpanExceeds(data, k, vmax, o.SliceMaxBytes) {
					t.Fatalf("refused a batch no span of which exceeds its limit: %v", err)
				}
				return
			case ReasonLineInvalid:
				if valid {
					t.Fatalf("refused an acceptable file: %v", err)
				}
				return
			}
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Fatal("accepted a file with an invalid line")
		}
		if int(tab.TotalLines) != len(want) || int(tab.Count) != (len(want)+k-1)/k {
			t.Fatalf("total %d count %d for %d lines at K=%d", tab.TotalLines, tab.Count, len(want), k)
		}
		var joined, got bytes.Buffer
		for _, l := range want {
			joined.Write(l)
			joined.WriteByte('\n')
		}
		prevEnd := uint64(0)
		for i, e := range tab.Entries {
			if i < len(tab.Entries)-1 && int(e.Lines) != k || e.Lines < 1 || int(e.Lines) > k {
				t.Fatalf("entry %d holds %d lines at K=%d", i, e.Lines, k)
			}
			if e.Offset < prevEnd || e.Offset+uint64(e.Length) > uint64(len(data)) {
				t.Fatalf("entry %d out of order or out of the input", i)
			}
			prevEnd = e.Offset + uint64(e.Length)
			span := spanOf(data, e)
			if e.SHA256 != sha256.Sum256(span) {
				t.Fatalf("entry %d: sha256 is not its span's", i)
			}
			s, n, err := Slice(span, k)
			if err != nil || n != int(e.Lines) {
				t.Fatalf("entry %d: Slice: %d lines, %v", i, n, err)
			}
			got.Write(s)
			if k == 1 && jsonl {
				v, err := Value(span, format)
				var compact bytes.Buffer
				if cerr := stablejson.Compact(&compact, want[i]); err != nil || cerr != nil || !bytes.Equal(v, compact.Bytes()) {
					t.Fatalf("entry %d: Value %q (%v) vs Compact %q (%v)", i, v, err, compact.Bytes(), cerr)
				}
			}
		}
		if !bytes.Equal(got.Bytes(), joined.Bytes()) {
			t.Fatalf("slices %q, want %q", got.Bytes(), joined.Bytes())
		}
		back, err := Decode(tab.Encode())
		if err != nil || !bytes.Equal(back.Encode(), tab.Encode()) {
			t.Fatalf("round trip: %v", err)
		}
		if err := tab.CheckLimits(vmax); err != nil {
			t.Fatalf("CheckLimits: %v", err)
		}
	})
}

// countingReaderOver serves data and counts the bytes served.
type countingReaderOver struct {
	data []byte
	pos  int
	read int64
}

func (c *countingReaderOver) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, ioEOF
	}
	n := copy(p, c.data[c.pos:])
	c.pos += n
	c.read += int64(n)
	return n, nil
}

func FuzzEachLinesIndexDecode(f *testing.F) {
	for _, in := range []struct {
		data string
		k    int
	}{{"", 1}, {"a\n", 1}, {"a\nb\nc\nd\ne\n", 2}, {"a\r\n\r\nb\nc\n", 3}} {
		tab, err := indexBytes([]byte(in.data), FormatLines, in.k, 100)
		if err != nil {
			f.Fatal(err)
		}
		enc := tab.Encode()
		f.Add(enc)
		if len(enc) > HeaderSize {
			bad := bytes.Clone(enc)
			bad[HeaderSize+9] ^= 0xff
			f.Add(bad)
			f.Add(enc[:len(enc)-1])
		}
	}
	f.Add([]byte("ORKLIDX"))
	f.Fuzz(func(t *testing.T, b []byte) {
		tab, err := Decode(b)
		if err != nil {
			return
		}
		if !bytes.Equal(tab.Encode(), b) {
			t.Fatal("an accepted index does not re-encode identically")
		}
		batch := uint64(tab.Batch)
		if batch == 0 || uint64(tab.Count) != (uint64(tab.TotalLines)+batch-1)/batch || len(tab.Entries) != int(tab.Count) {
			t.Fatalf("header invariants: %+v", tab.Header)
		}
		var sum, prevEnd uint64
		var prevFirst uint64
		for i, e := range tab.Entries {
			if i < len(tab.Entries)-1 && uint64(e.Lines) != batch || e.Lines < 1 || uint64(e.Lines) > batch {
				t.Fatalf("entry %d lines %d, batch %d", i, e.Lines, batch)
			}
			if e.Offset < prevEnd || e.Offset+uint64(e.Length) > tab.SourceSize {
				t.Fatalf("entry %d span", i)
			}
			if uint64(e.FirstLine) < prevFirst+1 {
				t.Fatalf("entry %d first line", i)
			}
			prevEnd, prevFirst = e.Offset+uint64(e.Length), uint64(e.FirstLine)
			sum += uint64(e.Lines)
		}
		if sum != uint64(tab.TotalLines) {
			t.Fatal("lines do not add up")
		}
	})
}

// someSpanExceeds is the reference for EACH_BATCH_TOO_LARGE: whether some
// item's span is over sliceMax or over its lines x (vmax + 2).
func someSpanExceeds(data []byte, k, vmax int, sliceMax int64) bool {
	off := int64(0)
	if bytes.HasPrefix(data, []byte("\xef\xbb\xbf")) {
		off = 3
	}
	var start, end int64
	n := 0
	exceeds := func() bool {
		return n > 0 && (end-start > sliceMax || end-start > int64(n)*int64(vmax+2))
	}
	for off < int64(len(data)) {
		lineEnd := int64(len(data))
		if i := bytes.IndexByte(data[off:], '\n'); i >= 0 {
			lineEnd = off + int64(i)
		}
		content := data[off:lineEnd]
		next := lineEnd + 1
		if len(content) > 0 && content[len(content)-1] == '\r' {
			content = content[:len(content)-1]
		}
		if len(content) > 0 {
			if n == 0 {
				start = off
			}
			end = off + int64(len(content))
			n++
			if n == k {
				if exceeds() {
					return true
				}
				n = 0
			}
		}
		off = next
	}
	return exceeds()
}

// FuzzEachLinesSliceTo holds the streaming slice to the in-memory one with a
// buffer small enough that CRs and long lines cross its boundaries.
func FuzzEachLinesSliceTo(f *testing.F) {
	for _, s := range []string{"", "a\r\nb", "a\r\r\nb\r", "\r\n\r\n", "a\n\nb\r\nc", strings.Repeat("x\r", 40) + "\r\ny"} {
		f.Add([]byte(s), byte(3), byte(0))
	}
	f.Fuzz(func(t *testing.T, span []byte, kb, bufb byte) {
		k := int(kb)%8 + 1
		want, wantN, wantErr := Slice(span, k)
		var got bytes.Buffer
		n, err := sliceTo(&got, bytes.NewReader(span), k, 16+int(bufb)%32)
		if (err != nil) != (wantErr != nil) {
			t.Fatalf("errors differ: %v vs %v", err, wantErr)
		}
		if err == nil && (n != wantN || !bytes.Equal(got.Bytes(), want)) {
			t.Fatalf("SliceTo %q (%d) vs Slice %q (%d)", got.Bytes(), n, want, wantN)
		}
	})
}
