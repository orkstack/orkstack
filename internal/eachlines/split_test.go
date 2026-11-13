package eachlines

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

// TestSplit_Table holds one case per row of the line rules (design 4.3), at K = 1.
func TestSplit_Table(t *testing.T) {
	long := strings.Repeat("a", testValuesMax)
	fits := strings.Repeat("a", testValuesMax-2) // quotes included, an item is values_max_bytes
	tests := []struct {
		name   string
		format Format
		in     string
		want   []string // each item's normalised text, newline terminated
		reason string   // set: the refusal
	}{
		{"terminator LF", FormatLines, "a\nb\n", []string{"a\n", "b\n"}, ""},
		{"terminator CRLF", FormatLines, "a\r\nb\r\n", []string{"a\n", "b\n"}, ""},
		{"one CR before LF only", FormatLines, "a\r\r\n", []string{"a\r\n"}, ""},
		{"CR inside a line is content", FormatLines, "a\rb\n", []string{"a\rb\n"}, ""},
		{"CR at EOF stripped", FormatLines, "a\nb\r", []string{"a\n", "b\n"}, ""},
		{"last line without LF counts", FormatLines, "a\nb", []string{"a\n", "b\n"}, ""},
		{"trailing newline adds no empty line", FormatLines, "a\n", []string{"a\n"}, ""},
		{"empty lines skipped", FormatLines, "\na\n\n\r\n\nb\n\n", []string{"a\n", "b\n"}, ""},
		{"whitespace-only is a line in lines", FormatLines, "a\n \n\t\nb\n", []string{"a\n", " \n", "\t\n", "b\n"}, ""},
		{"whitespace-only refused in jsonl", FormatJSONL, "1\n \n2\n", nil, ReasonLineInvalid},
		{"BOM skipped at byte 0", FormatLines, "\xef\xbb\xbfa\nb\n", []string{"a\n", "b\n"}, ""},
		{"BOM skipped at byte 0 in jsonl", FormatJSONL, "\xef\xbb\xbf1\n2\n", []string{"1\n", "2\n"}, ""},
		{"BOM later is content", FormatLines, "a\n\xef\xbb\xbfb\n", []string{"a\n", "\xef\xbb\xbfb\n"}, ""},
		{"BOM later refused in jsonl", FormatJSONL, "1\n\xef\xbb\xbf2\n", nil, ReasonLineInvalid},
		{"invalid UTF-8 in lines", FormatLines, "a\n\xff\n", nil, ReasonLineInvalid},
		{"invalid UTF-8 in a jsonl string", FormatJSONL, "\"\xff\"\n", nil, ReasonLineInvalid},
		{"jsonl value is any JSON", FormatJSONL, "1\n\"s\"\n[1, 2]\n{\"a\": 1}\nnull\n", []string{"1\n", "\"s\"\n", "[1, 2]\n", "{\"a\": 1}\n", "null\n"}, ""},
		{"jsonl invalid JSON", FormatJSONL, "{\"a\":1}\n{oops\n", nil, ReasonLineInvalid},
		{"jsonl two values on a line", FormatJSONL, "1 2\n", nil, ReasonLineInvalid},
		{"jsonl duplicate key", FormatJSONL, "{\"a\":1,\"a\":2}\n", nil, ReasonLineInvalid},
		{"invalid line late fails the whole scan", FormatJSONL, "1\n2\n3\nx\n", nil, ReasonLineInvalid},
		{"line whose item is exactly values_max_bytes", FormatLines, fits + "\r\n" + fits, []string{fits + "\n", fits + "\n"}, ""},
		{"jsonl line of exactly values_max_bytes", FormatJSONL, "\"" + fits + "\"\r\n", []string{"\"" + fits + "\"\n"}, ""},
		{"line over values_max_bytes", FormatLines, long + "b\n", nil, ReasonLineInvalid},
		{"line over values_max_bytes at EOF", FormatLines, "a\n" + long + "b", nil, ReasonLineInvalid},
		{"encoded string over values_max_bytes: quotes", FormatLines, strings.Repeat("a", testValuesMax), nil, ReasonLineInvalid},
		{"encoded string at values_max_bytes", FormatLines, strings.Repeat("a", testValuesMax-2), []string{strings.Repeat("a", testValuesMax-2) + "\n"}, ""},
		{"encoded string: backslashes double", FormatLines, strings.Repeat("\\", testValuesMax/2), nil, ReasonLineInvalid},
		{"encoded string: control characters take six bytes", FormatLines, strings.Repeat("\x01", testValuesMax/6+1), nil, ReasonLineInvalid},
		{"encoded string: quotes double", FormatLines, strings.Repeat("\"", testValuesMax/2), nil, ReasonLineInvalid},
		{"encoded string: less-than stays one byte", FormatLines, strings.Repeat("<", testValuesMax-2), []string{strings.Repeat("<", testValuesMax-2) + "\n"}, ""},
		{"encoded string: less-than at the limit", FormatLines, strings.Repeat("<", testValuesMax), nil, ReasonLineInvalid},
		{"empty file", FormatLines, "", []string{}, ""},
		{"only a BOM", FormatLines, "\xef\xbb\xbf", []string{}, ""},
		{"only empty lines", FormatJSONL, "\n\r\n\n", []string{}, ""},
		{"too many", FormatLines, "a\nb\nc\nd\n", nil, ReasonTooManyItems},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxItems := 1000
			if tt.reason == ReasonTooManyItems {
				maxItems = 3
			}
			tab, err := indexBytes([]byte(tt.in), tt.format, 1, maxItems)
			if tt.reason != "" {
				if ReasonOf(err) != tt.reason {
					t.Fatalf("want reason %s, got %v", tt.reason, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := texts(t, []byte(tt.in), tab, 1)
			if strings.Join(got, "|") != strings.Join(tt.want, "|") || len(got) != len(tt.want) {
				t.Fatalf("items = %q, want %q", got, tt.want)
			}
			if int(tab.Count) != len(tt.want) || int(tab.TotalLines) != len(tt.want) {
				t.Fatalf("count %d total %d, want %d", tab.Count, tab.TotalLines, len(tt.want))
			}
		})
	}
}

func TestSplit_DigestMismatchIsExpansionFailed(t *testing.T) {
	data := []byte("a\nb\n")
	o := opts(FormatLines, 1, 10, data)
	o.SourceSHA256 = &[32]byte{1}
	_, err := Index(context.Background(), bytes.NewReader(data), 1<<20, 1, o)
	if ReasonOf(err) != ReasonExpansionFailed {
		t.Fatalf("got %v", err)
	}
	// A reader that ends before the committed size is the same refusal.
	o = opts(FormatLines, 1, 10, data)
	o.SourceSize = int64(len(data)) + 5
	_, err = Index(context.Background(), bytes.NewReader(data), 1<<20, 1, o)
	if ReasonOf(err) != ReasonExpansionFailed {
		t.Fatalf("short read: got %v", err)
	}
}

func TestSplit_ValueOfAnItem(t *testing.T) {
	tests := []struct {
		format Format
		span   string
		want   string
	}{
		{FormatJSONL, `{"a": [1,  2]}`, `{"a":[1,2]}`},
		{FormatJSONL, `"x"`, `"x"`},
		{FormatLines, `he said "hi"`, `"he said \"hi\""`},
		{FormatLines, `<a&b>`, `"<a&b>"`},
	}
	for _, tt := range tests {
		got, err := Value([]byte(tt.span), tt.format)
		if err != nil || string(got) != tt.want {
			t.Errorf("Value(%q) = %q, %v; want %q", tt.span, got, err, tt.want)
		}
	}
	if _, err := Value([]byte("{oops"), FormatJSONL); err == nil {
		t.Error("an invalid jsonl span must be refused")
	}
}

// TestSplitBatches_Table covers batching: spans, line numbers and slices.
func TestSplitBatches_Table(t *testing.T) {
	type item struct {
		offset, length, first, lines uint64
		text                         string
	}
	tests := []struct {
		name   string
		format Format
		k      int
		in     string
		want   []item
	}{
		{"K = 1", FormatLines, 1, "a\nb\n", []item{{0, 1, 1, 1, "a\n"}, {2, 1, 2, 1, "b\n"}}},
		{"K = 3 with a remainder", FormatLines, 3, "a\nb\nc\nd\ne\n", []item{
			{0, 5, 1, 3, "a\nb\nc\n"}, {6, 3, 4, 2, "d\ne\n"}}},
		{"an exact multiple", FormatLines, 2, "a\nb\nc\nd\n", []item{
			{0, 3, 1, 2, "a\nb\n"}, {4, 3, 3, 2, "c\nd\n"}}},
		{"empty lines and CRLF inside a span", FormatLines, 3, "a\r\n\r\n\nb\r\nc\n", []item{
			{0, 10, 1, 3, "a\nb\nc\n"}}},
		{"empty lines between batches are in neither", FormatLines, 2, "a\nb\n\n\r\nc\n", []item{
			{0, 3, 1, 2, "a\nb\n"}, {7, 1, 5, 1, "c\n"}}},
		{"a BOM in the first batch", FormatLines, 2, "\xef\xbb\xbfa\r\nb\r\n", []item{
			{3, 4, 1, 2, "a\nb\n"}}},
		{"K above the line count", FormatLines, 100, "a\nb\nc", []item{
			{0, 5, 1, 3, "a\nb\nc\n"}}},
		{"a trailing CR is not in the span", FormatLines, 2, "a\nb\r", []item{
			{0, 3, 1, 2, "a\nb\n"}}},
		{"jsonl batch", FormatJSONL, 2, "{\"a\":1}\n[2]\n3\n", []item{
			{0, 11, 1, 2, "{\"a\":1}\n[2]\n"}, {12, 1, 3, 1, "3\n"}}},
		{"empty file", FormatLines, 5, "", []item{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(tt.in)
			tab, err := indexBytes(data, tt.format, tt.k, 100)
			if err != nil {
				t.Fatal(err)
			}
			if len(tab.Entries) != len(tt.want) || int(tab.Count) != len(tt.want) {
				t.Fatalf("%d entries (count %d), want %d", len(tab.Entries), tab.Count, len(tt.want))
			}
			if int(tab.Batch) != tt.k {
				t.Fatalf("batch = %d", tab.Batch)
			}
			for i, w := range tt.want {
				e := tab.Entries[i]
				if e.Offset != w.offset || uint64(e.Length) != w.length || uint64(e.FirstLine) != w.first || uint64(e.Lines) != w.lines {
					t.Errorf("entry %d = {off %d len %d first %d lines %d}, want %+v", i, e.Offset, e.Length, e.FirstLine, e.Lines, w)
				}
				if e.SHA256 != sha256.Sum256(spanOf(data, e)) {
					t.Errorf("entry %d: sha256 is not the hash of its span", i)
				}
			}
			got := texts(t, data, tab, tt.k)
			for i, w := range tt.want {
				if got[i] != w.text {
					t.Errorf("item %d slice = %q, want %q", i, got[i], w.text)
				}
			}
		})
	}
}

// TestIndex_BatchHeaderInvariants checks the header and entry invariants of
// design 5.4 on every index Index builds, over many shapes.
func TestIndex_BatchHeaderInvariants(t *testing.T) {
	for lines := 0; lines <= 40; lines++ {
		for _, k := range []int{1, 2, 3, 7, 40, 41} {
			var b strings.Builder
			for i := 0; i < lines; i++ {
				b.WriteString("x\n")
				if i%5 == 0 {
					b.WriteString("\n")
				}
			}
			data := []byte(b.String())
			tab, err := indexBytes(data, FormatLines, k, 1000)
			if err != nil {
				t.Fatal(err)
			}
			wantCount := (lines + k - 1) / k
			if int(tab.Count) != wantCount || len(tab.Entries) != wantCount || int(tab.TotalLines) != lines {
				t.Fatalf("lines %d K %d: count %d entries %d total %d", lines, k, tab.Count, len(tab.Entries), tab.TotalLines)
			}
			sum := 0
			for i, e := range tab.Entries {
				if i < len(tab.Entries)-1 && int(e.Lines) != k {
					t.Fatalf("lines %d K %d: entry %d holds %d", lines, k, i, e.Lines)
				}
				if e.Lines < 1 || int(e.Lines) > k {
					t.Fatalf("lines %d K %d: last entry holds %d", lines, k, e.Lines)
				}
				sum += int(e.Lines)
			}
			if sum != lines {
				t.Fatalf("lines %d K %d: entries hold %d", lines, k, sum)
			}
			if tab.SourceSize != uint64(len(data)) || tab.SourceSHA256 != sha256.Sum256(data) {
				t.Fatal("header does not describe the source")
			}
			back, err := Decode(tab.Encode())
			if err != nil {
				t.Fatalf("lines %d K %d: decode: %v", lines, k, err)
			}
			if len(tab.Encode()) != HeaderSize+EntrySize*wantCount {
				t.Fatal("size is not 64 + 56*count")
			}
			if !bytes.Equal(back.Encode(), tab.Encode()) {
				t.Fatal("re-encode differs")
			}
		}
	}
}

func TestIndex_RoundTrip(t *testing.T) {
	data := []byte("\xef\xbb\xbf{\"a\":1}\r\n\r\n[2]\n3\n\"four\"\nnull")
	tab, err := indexBytes(data, FormatJSONL, 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	enc := tab.Encode()
	if string(enc[:7]) != "ORKLIDX" || enc[7] != 1 {
		t.Fatalf("magic/version = %q %d", enc[:7], enc[7])
	}
	back, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if back.Header != tab.Header || len(back.Entries) != len(tab.Entries) {
		t.Fatalf("header or entries differ: %+v vs %+v", back, tab)
	}
	for i := range back.Entries {
		if back.Entries[i] != tab.Entries[i] {
			t.Fatalf("entry %d differs", i)
		}
	}
	if err := back.CheckLimits(testValuesMax); err != nil {
		t.Fatal(err)
	}
}

func TestIndex_Deterministic(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 5000; i++ {
		b.WriteString(strings.Repeat("z", i%97))
		b.WriteString("q\r\n")
		if i%13 == 0 {
			b.WriteString("\n")
		}
	}
	data := []byte(b.String())
	base, err := indexBytes(data, FormatLines, 7, 5000)
	if err != nil {
		t.Fatal(err)
	}
	for name, wrap := range map[string]func(*bytes.Reader) io.Reader{
		"one byte": func(r *bytes.Reader) io.Reader { return iotest.OneByteReader(r) },
		"half":     func(r *bytes.Reader) io.Reader { return iotest.HalfReader(r) },
		"data err": func(r *bytes.Reader) io.Reader { return iotest.DataErrReader(r) },
	} {
		o := opts(FormatLines, 7, 5000, data)
		got, err := Index(context.Background(), wrap(bytes.NewReader(data)), 1<<30, 7, o)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got.Encode(), base.Encode()) {
			t.Fatalf("%s: index differs with how the reader chunks", name)
		}
	}
	again, _ := indexBytes(data, FormatLines, 7, 5000)
	if !bytes.Equal(again.Encode(), base.Encode()) {
		t.Fatal("two runs differ")
	}
}

// TestSplit_CorruptSourceIsNotTheAuthors: a refusal of a line does not hide a
// source whose bytes are not the committed ones.
func TestSplit_CorruptSourceIsNotTheAuthors(t *testing.T) {
	good := []byte("a\nb\nc\n")
	sum := sha256.Sum256(good)
	flipped := []byte("a\n\xff\nc\n") // invalid UTF-8, and not the committed bytes
	for name, tc := range map[string]struct {
		data   []byte
		digest [32]byte
		want   string
	}{
		"wrong digest": {flipped, sum, ReasonExpansionFailed},
		"right digest": {flipped, sha256.Sum256(flipped), ReasonLineInvalid},
	} {
		o := opts(FormatLines, 1, 10, tc.data)
		o.SourceSHA256 = &tc.digest
		_, err := Index(context.Background(), bytes.NewReader(tc.data), 1<<20, 1, o)
		if ReasonOf(err) != tc.want {
			t.Errorf("%s: got %v, want %s", name, err, tc.want)
		}
	}
	// Likewise a batch over its limit.
	data := []byte("aaaaaaaa\naaaaaaaa\n")
	wrong := sha256.Sum256([]byte("other"))
	o := opts(FormatLines, 2, 10, data)
	o.SliceMaxBytes, o.SourceSHA256 = 5, &wrong
	if _, err := Index(context.Background(), bytes.NewReader(data), 1<<20, 2, o); ReasonOf(err) != ReasonExpansionFailed {
		t.Errorf("batch: %v", err)
	}
	// The drain stays inside the committed size.
	r := &countingReader{pattern: []byte("\xff")}
	o = Options{Format: FormatLines, MaxItems: 10, ValuesMaxBytes: testValuesMax, SliceMaxBytes: 1026, SourceSize: 3 << 20, SourceSHA256: &wrong}
	if _, err := Index(context.Background(), r, 4<<20, 1, o); ReasonOf(err) != ReasonExpansionFailed || r.read != 3<<20 {
		t.Errorf("drain: %v, read %d", err, r.read)
	}
}
