package eachlines

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"
)

const (
	testValuesMax = 1024
	testPlanSlice = 64 << 20
)

// opts builds the options a caller derives from a plan for data of this size.
func opts(f Format, k, maxItems int, data []byte) Options {
	sum := sha256.Sum256(data)
	return Options{
		Format:         f,
		MaxItems:       maxItems,
		ValuesMaxBytes: testValuesMax,
		SliceMaxBytes:  SliceMax(k, testValuesMax, testPlanSlice),
		SourceSize:     int64(len(data)),
		SourceSHA256:   &sum,
	}
}

// indexBytes indexes data with the cap a caller would derive.
func indexBytes(data []byte, f Format, k, maxItems int) (*Table, error) {
	o := opts(f, k, maxItems, data)
	return Index(context.Background(), bytes.NewReader(data), ReadCap(maxItems, o.SliceMaxBytes, 5<<30), k, o)
}

func spanOf(data []byte, e Entry) []byte {
	return data[e.Offset : e.Offset+uint64(e.Length)]
}

// texts returns each item's lines, normalised, one string per item.
func texts(t *testing.T, data []byte, tab *Table, k int) []string {
	t.Helper()
	var out []string
	for i, e := range tab.Entries {
		got, n, err := Slice(spanOf(data, e), k)
		if err != nil {
			t.Fatalf("item %d: Slice: %v", i, err)
		}
		if n != int(e.Lines) {
			t.Fatalf("item %d: Slice counted %d lines, entry says %d", i, n, e.Lines)
		}
		out = append(out, string(got))
	}
	return out
}

// countingReader serves an endless repeat of pattern and counts what it served.
type countingReader struct {
	pattern []byte
	pos     int
	read    int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		m := copy(p[n:], c.pattern[c.pos:])
		n += m
		c.pos = (c.pos + m) % len(c.pattern)
	}
	c.read += int64(n)
	return n, nil
}

var _ io.Reader = (*countingReader)(nil)

var ioEOF = io.EOF
