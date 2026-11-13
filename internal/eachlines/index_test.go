package eachlines

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"
)

func validIndex(t *testing.T) ([]byte, []byte) {
	t.Helper()
	data := []byte("a\nb\nc\nd\ne\nf\ng\n")
	tab, err := indexBytes(data, FormatLines, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	return data, tab.Encode()
}

func TestIndex_RefusesCorrupt(t *testing.T) {
	_, good := validIndex(t)
	if _, err := Decode(good); err != nil {
		t.Fatalf("the base index must decode: %v", err)
	}
	entry := func(i int) int { return HeaderSize + EntrySize*i }
	u32 := func(b []byte, at int, v uint32) { binary.LittleEndian.PutUint32(b[at:], v) }
	u64 := func(b []byte, at int, v uint64) { binary.LittleEndian.PutUint64(b[at:], v) }
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"empty", func(b []byte) []byte { return nil }},
		{"shorter than a header", func(b []byte) []byte { return b[:HeaderSize-1] }},
		{"bad magic", func(b []byte) []byte { b[0] = 'X'; return b }},
		{"unknown version", func(b []byte) []byte { b[7] = 2; return b }},
		{"version zero", func(b []byte) []byte { b[7] = 0; return b }},
		{"format zero", func(b []byte) []byte { b[48] = 0; return b }},
		{"unknown format", func(b []byte) []byte { b[48] = 3; return b }},
		{"header reserved byte 1", func(b []byte) []byte { b[49] = 1; return b }},
		{"header reserved byte 3", func(b []byte) []byte { b[51] = 1; return b }},
		{"batch zero", func(b []byte) []byte { u32(b, 52, 0); return b }},
		{"truncated entry", func(b []byte) []byte { return b[:len(b)-1] }},
		{"trailing byte", func(b []byte) []byte { return append(b, 0) }},
		{"a whole entry missing", func(b []byte) []byte { return b[:len(b)-EntrySize] }},
		{"count too high", func(b []byte) []byte { u32(b, 56, 4); return b }},
		{"count huge", func(b []byte) []byte { u32(b, 56, 1<<31); return b }},
		{"count not ceil(total/batch)", func(b []byte) []byte { u32(b, 60, 100); return b }},
		{"total lines off", func(b []byte) []byte { u32(b, 60, 8); return b }},
		{"middle entry holds fewer than batch", func(b []byte) []byte { u32(b, entry(1)+16, 2); return b }},
		{"last entry holds zero", func(b []byte) []byte { u32(b, entry(2)+16, 0); return b }},
		{"last entry holds more than batch", func(b []byte) []byte { u32(b, entry(2)+16, 4); return b }},
		{"entries do not add up", func(b []byte) []byte { u32(b, entry(0)+16, 2); return b }},
		{"entry reserved", func(b []byte) []byte { u32(b, entry(1)+20, 1); return b }},
		{"offsets decrease", func(b []byte) []byte { u64(b, entry(1), 0); return b }},
		{"offsets equal", func(b []byte) []byte { u64(b, entry(1), 0); u64(b, entry(0), 0); return b }},
		{"spans overlap", func(b []byte) []byte { u32(b, entry(0)+8, 9); return b }},
		{"span past the source", func(b []byte) []byte { u64(b, entry(2), 1000); return b }},
		{"span ends past the source", func(b []byte) []byte { u32(b, entry(2)+8, 1000); return b }},
		{"span shorter than its lines", func(b []byte) []byte { u32(b, entry(0)+8, 2); return b }},
		{"offset overflow", func(b []byte) []byte { u64(b, entry(2), ^uint64(0)-1); return b }},
		{"first line zero", func(b []byte) []byte { u32(b, entry(0)+12, 0); return b }},
		{"first line goes backwards", func(b []byte) []byte { u32(b, entry(1)+12, 1); return b }},
		{"first line below the lines before it", func(b []byte) []byte { u32(b, entry(2)+12, 5); return b }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.mutate(bytes.Clone(good))
			if tab, err := Decode(b); err == nil {
				t.Fatalf("accepted a corrupt index: %+v", tab)
			}
		})
	}
}

func TestIndex_CheckLimits(t *testing.T) {
	data := []byte(strings.Repeat("abcdefghij\n", 3))
	tab, err := indexBytes(data, FormatLines, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	// Three lines of ten bytes, two LFs between: 32 bytes.
	if err := tab.CheckLimits(10); err != nil {
		t.Fatalf("within 3 x (10+2): %v", err)
	}
	if err := tab.CheckLimits(7); err == nil { // 3 x 9 = 27 < 32
		t.Fatal("a span over lines x (values_max_bytes + 2) must be refused")
	}
}

func TestIndex_ReadCapStopsReading(t *testing.T) {
	// A source over the cap is refused with no read at all.
	r := &countingReader{pattern: []byte("a\n")}
	o := Options{Format: FormatLines, MaxItems: 10, ValuesMaxBytes: testValuesMax, SliceMaxBytes: 1026, SourceSize: 5000}
	_, err := Index(context.Background(), r, 4000, 1, o)
	if ReasonOf(err) != ReasonSourceTooLarge || r.read != 0 {
		t.Fatalf("err %v, read %d", err, r.read)
	}
	// A reader longer than the committed size is never read past it.
	r = &countingReader{pattern: []byte("a\n")}
	o.SourceSize = 3000
	_, _ = Index(context.Background(), r, 4000, 1, o)
	if r.read > 3000 {
		t.Fatalf("read %d bytes of a 3000-byte source", r.read)
	}
	// Nor past the cap when the cap is the smaller.
	r = &countingReader{pattern: []byte("a\n")}
	o.SourceSize = 4000
	_, _ = Index(context.Background(), r, 4000, 1, o)
	if r.read > 4000 {
		t.Fatalf("read %d bytes past a 4000-byte cap", r.read)
	}
}

func TestIndex_StopsAtMaxItemsPlusOne(t *testing.T) {
	r := &countingReader{pattern: []byte("x\n")}
	o := Options{Format: FormatLines, MaxItems: 5, ValuesMaxBytes: testValuesMax, SliceMaxBytes: SliceMax(1, testValuesMax, testPlanSlice), SourceSize: 1 << 40}
	_, err := Index(context.Background(), r, 1<<40, 1, o)
	if ReasonOf(err) != ReasonTooManyItems || !strings.Contains(err.Error(), "max_items 5") {
		t.Fatalf("err = %v", err)
	}
	if r.read > ReadBufferSize {
		t.Fatalf("read %d bytes: the scan must stop within the buffer holding line 6", r.read)
	}
	// Empty lines do not count toward the ceiling.
	data := []byte("a\n\n\nb\n\n")
	if _, err := indexBytes(data, FormatLines, 1, 2); err != nil {
		t.Fatalf("two lines, ceiling 2: %v", err)
	}
}

func TestIndex_StopsAtMaxItemsTimesKPlusOne(t *testing.T) {
	r := &countingReader{pattern: []byte("x\n")}
	o := Options{Format: FormatLines, MaxItems: 2, ValuesMaxBytes: testValuesMax, SliceMaxBytes: SliceMax(3, testValuesMax, testPlanSlice), SourceSize: 1 << 40}
	_, err := Index(context.Background(), r, 1<<40, 3, o)
	if ReasonOf(err) != ReasonTooManyItems || !strings.Contains(err.Error(), "2 batches of 3 lines") {
		t.Fatalf("err = %v", err)
	}
	if r.read > ReadBufferSize {
		t.Fatalf("read %d bytes", r.read)
	}
	// Exactly max_items x K lines is accepted.
	data := []byte("1\n2\n3\n4\n5\n6\n")
	tab, err := indexBytes(data, FormatLines, 3, 2)
	if err != nil || tab.Count != 2 {
		t.Fatalf("six lines, 2 x 3: %v", err)
	}
	if _, err := indexBytes(append(data, "7\n"...), FormatLines, 3, 2); ReasonOf(err) != ReasonTooManyItems {
		t.Fatalf("seven lines: %v", err)
	}
}

func TestIndex_StopsOnOverlongLine(t *testing.T) {
	for _, vmax := range []int64{testValuesMax, 3 << 20} {
		r := &countingReader{pattern: []byte("a")}
		o := Options{Format: FormatLines, MaxItems: 10, ValuesMaxBytes: vmax, SliceMaxBytes: SliceMax(1, vmax, testPlanSlice), SourceSize: 1 << 40}
		_, err := Index(context.Background(), r, 1<<40, 1, o)
		if ReasonOf(err) != ReasonLineInvalid {
			t.Fatalf("vmax %d: err = %v", vmax, err)
		}
		if limit := vmax + 2 + ReadBufferSize; r.read > limit {
			t.Fatalf("vmax %d: read %d bytes of a file with no LF, limit %d", vmax, r.read, limit)
		}
	}
	// A line longer than one read buffer, within the limit, is fine.
	vmax := int64(3 << 20)
	line := strings.Repeat("a", 2<<20)
	data := []byte(line + "\n" + line + "\r\n")
	o := opts(FormatLines, 1, 10, data)
	o.ValuesMaxBytes = vmax
	o.SliceMaxBytes = SliceMax(1, vmax, testPlanSlice)
	tab, err := Index(context.Background(), bytes.NewReader(data), 1<<40, 1, o)
	if err != nil || tab.Count != 2 {
		t.Fatalf("two 2 MiB lines: %v", err)
	}
}

func TestIndex_BatchTooLarge(t *testing.T) {
	data := []byte("aaaaaaaa\naaaaaaaa\naaaaaaaa\n") // span of 3 lines: 8*3 + 2 = 26
	o := opts(FormatLines, 3, 10, data)
	o.SliceMaxBytes = 26
	if _, err := Index(context.Background(), bytes.NewReader(data), 1<<20, 3, o); err != nil {
		t.Fatalf("a span of exactly slice_max: %v", err)
	}
	o.SliceMaxBytes = 25
	_, err := Index(context.Background(), bytes.NewReader(data), 1<<20, 3, o)
	if ReasonOf(err) != ReasonBatchTooLarge {
		t.Fatalf("err = %v", err)
	}
	// Empty lines inside a span count toward it; those between batches do not.
	data = []byte("a\n\n\n\n\n\nb\n")
	o = opts(FormatLines, 2, 10, data)
	o.SliceMaxBytes = 5
	if _, err := Index(context.Background(), bytes.NewReader(data), 1<<20, 2, o); ReasonOf(err) != ReasonBatchTooLarge {
		t.Fatalf("blank lines inside a span: %v", err)
	}
	data = []byte("a\nb\n\n\n\n\n\nc\nd\n")
	o = opts(FormatLines, 2, 10, data)
	o.SliceMaxBytes = 3
	if _, err := Index(context.Background(), bytes.NewReader(data), 1<<20, 2, o); err != nil {
		t.Fatalf("blank lines between batches: %v", err)
	}
}

func TestSliceMaxAndReadCap(t *testing.T) {
	if got := SliceMax(1, 10, testPlanSlice); got != 12 {
		t.Errorf("K=1: %d", got)
	}
	if got := SliceMax(3, 10, testPlanSlice); got != 36 {
		t.Errorf("K=3: %d", got)
	}
	if got := SliceMax(3, 10, 20); got != 20 {
		t.Errorf("plan smaller: %d", got)
	}
	if got := ReadCap(5, 36, 1<<30); got != 5*36+3 {
		t.Errorf("cap: %d", got)
	}
	if got := ReadCap(5, 36, 100); got != 100 {
		t.Errorf("artifact_max smaller: %d", got)
	}
}

func TestSlice_NormalisesSpan(t *testing.T) {
	tests := []struct {
		name  string
		span  string
		k     int
		want  string
		lines int
		err   bool
	}{
		{"plain", "a\nb", 2, "a\nb\n", 2, false},
		{"CRLF", "a\r\nb\r\nc", 3, "a\nb\nc\n", 3, false},
		{"empty lines dropped", "a\r\n\r\n\nb", 2, "a\nb\n", 2, false},
		{"one line", "abc", 1, "abc\n", 1, false},
		{"content CR kept", "a\r\r\nb\rc", 2, "a\r\nb\rc\n", 2, false},
		{"content ends in CR", "a\r", 1, "a\r\n", 1, false},
		{"fewer than K", "a\nb", 5, "a\nb\n", 2, false},
		{"more than K", "a\nb\nc", 2, "", 0, true},
		{"empty span", "", 2, "", 0, false},
		{"K below one", "a", 0, "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, n, err := Slice([]byte(tt.span), tt.k)
			if (err != nil) != tt.err {
				t.Fatalf("err = %v", err)
			}
			if string(got) != tt.want || n != tt.lines {
				t.Fatalf("got %q, %d lines; want %q, %d", got, n, tt.want, tt.lines)
			}
			if tt.err {
				return
			}
			// The streaming form agrees, whatever the reader's chunking.
			var buf bytes.Buffer
			n2, err := SliceTo(&buf, strings.NewReader(tt.span), tt.k)
			if err != nil || n2 != n || buf.String() != tt.want {
				t.Fatalf("SliceTo = %q, %d, %v", buf.String(), n2, err)
			}
		})
	}
	// A line longer than the streaming buffer.
	long := strings.Repeat("x", 3*ReadBufferSize/2)
	var buf bytes.Buffer
	n, err := SliceTo(&buf, strings.NewReader(long+"\r\n\r\n"+long), 2)
	if err != nil || n != 2 || buf.String() != long+"\n"+long+"\n" {
		t.Fatalf("long lines: n %d err %v len %d", n, err, buf.Len())
	}
}

// TestIndex_LongBlankRuns covers blank runs longer than the indexer holds back
// before hashing: inside a span they are hashed with it, after the last item's
// last line they are not.
func TestIndex_LongBlankRuns(t *testing.T) {
	blank := strings.Repeat("\r\n", 100<<10)
	for _, in := range []string{"a\n" + blank + "b\n", "a\n" + blank + "b\n" + blank, "a\n" + blank, "a\n" + blank + "b\nc\n" + blank + "d"} {
		data := []byte(in)
		o := opts(FormatLines, 2, 10, data)
		o.SliceMaxBytes, o.ValuesMaxBytes = 1<<30, 1<<20
		tab, err := Index(context.Background(), bytes.NewReader(data), 1<<30, 2, o)
		if err != nil {
			t.Fatal(err)
		}
		for i, e := range tab.Entries {
			if e.SHA256 != sha256.Sum256(spanOf(data, e)) {
				t.Errorf("%.12q...: item %d: sha256 is not its span's", in, i)
			}
			if span := spanOf(data, e); span[len(span)-1] == '\n' || span[len(span)-1] == '\r' && len(span) > 1 && span[len(span)-2] == '\n' {
				t.Errorf("item %d span ends in a terminator", i)
			}
		}
	}
}
