package artifacts

import (
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

func TestCheckRange_NeverOverflows(t *testing.T) {
	for _, tc := range []struct {
		offset, length, size int64
		ok                   bool
	}{
		{0, 0, 0, true},
		{0, 10, 10, true},
		{10, 0, 10, true},
		{10, 1, 10, false},
		{11, 0, 10, false},
		{math.MaxInt64, 1, -1, false},
		{math.MaxInt64, 10, 100, false},
		{1, math.MaxInt64, -1, false},
		{math.MaxInt64, 0, -1, true},
		{-1, 0, -1, false},
	} {
		err := checkRange(tc.offset, tc.length, tc.size)
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, ErrRange)) {
			t.Errorf("checkRange(%d, %d, %d) = %v, want ok %v", tc.offset, tc.length, tc.size, err, tc.ok)
		}
	}
}

func TestParseContentRange(t *testing.T) {
	for _, tc := range []struct {
		header             string
		first, last, total int64
		ok                 bool
	}{
		{"bytes 2-4/10", 2, 4, 10, true},
		{"bytes 0-0/1", 0, 0, 1, true},
		{"bytes */10", 0, 0, 0, false},
		{"bytes 2-4/*", 0, 0, 0, false},
		{"bytes 4-2/10", 0, 0, 0, false},
		{"bytes 2-10/10", 0, 0, 0, false},
		{"bytes -1-4/10", 0, 0, 0, false},
		{"2-4/10", 0, 0, 0, false},
		{"", 0, 0, 0, false},
	} {
		first, last, total, ok := parseContentRange(tc.header)
		if ok != tc.ok || (ok && (first != tc.first || last != tc.last || total != tc.total)) {
			t.Errorf("parseContentRange(%q) = %d, %d, %d, %v; want %d, %d, %d, %v",
				tc.header, first, last, total, ok, tc.first, tc.last, tc.total, tc.ok)
		}
	}
}

// TestExactReader_YieldsExactlyItsLengthOrFails: never a byte past want, and
// a body that ends early is io.ErrUnexpectedEOF, never a short success.
func TestExactReader_YieldsExactlyItsLengthOrFails(t *testing.T) {
	read := func(body string, want int64) (string, error) {
		b, err := io.ReadAll(newExactReader(io.NopCloser(strings.NewReader(body)), want))
		return string(b), err
	}
	if got, err := read("abcdef", 3); got != "abc" || err != nil {
		t.Errorf("a longer body = %q, %v; want abc", got, err)
	}
	if got, err := read("abc", 3); got != "abc" || err != nil {
		t.Errorf("an exact body = %q, %v; want abc", got, err)
	}
	if _, err := read("ab", 3); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a short body = %v, want io.ErrUnexpectedEOF", err)
	}
	if got, err := read("", 0); got != "" || err != nil {
		t.Errorf("zero length = %q, %v", got, err)
	}
}
