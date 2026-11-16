package artifacts

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

// checkRange refuses a negative offset or length, a range whose end does not
// fit an int64, and a range that ends past size (when size is known,
// non-negative). It never computes offset+length before knowing it fits.
func checkRange(offset, length, size int64) error {
	if offset < 0 || length < 0 || offset > math.MaxInt64-length {
		return fmt.Errorf("offset %d, length %d: %w", offset, length, ErrRange)
	}
	if size >= 0 && offset > size-length {
		return fmt.Errorf("[%d, %d) of %d bytes: %w", offset, offset+length, size, ErrRange)
	}
	return nil
}

// parseContentRange reads a 206 answer's Content-Range, "bytes first-last/total".
// An unsatisfied range ("bytes */total") or an unknown total ("/*") is not one.
func parseContentRange(header string) (first, last, total int64, ok bool) {
	spec, found := strings.CutPrefix(header, "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	span, size, found := strings.Cut(spec, "/")
	if !found {
		return 0, 0, 0, false
	}
	a, b, found := strings.Cut(span, "-")
	if !found {
		return 0, 0, 0, false
	}
	var err error
	if first, err = strconv.ParseInt(a, 10, 64); err != nil || first < 0 {
		return 0, 0, 0, false
	}
	if last, err = strconv.ParseInt(b, 10, 64); err != nil || last < first {
		return 0, 0, 0, false
	}
	if total, err = strconv.ParseInt(size, 10, 64); err != nil || last >= total {
		return 0, 0, 0, false
	}
	return first, last, total, true
}

// exactReader yields exactly want bytes of r: more are never read, and an
// end before want is io.ErrUnexpectedEOF, so a short body never passes for
// the range.
type exactReader struct {
	r    io.ReadCloser
	left int64
}

func newExactReader(r io.ReadCloser, want int64) io.ReadCloser {
	return &exactReader{r: r, left: want}
}

func (e *exactReader) Read(p []byte) (int, error) {
	if e.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > e.left {
		p = p[:e.left]
	}
	n, err := e.r.Read(p)
	e.left -= int64(n)
	if err == io.EOF {
		if e.left > 0 {
			return n, io.ErrUnexpectedEOF
		}
		return n, io.EOF
	}
	return n, err
}

func (e *exactReader) Close() error { return e.r.Close() }

// RangeOfBytes is GetRange over bytes already in memory, for a Store that
// holds its objects so (a test's, a replay's).
func RangeOfBytes(b []byte, offset, length int64) (io.ReadCloser, error) {
	if err := checkRange(offset, length, int64(len(b))); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b[offset : offset+length])), nil
}

// RangeOfFile is GetRange over an open file, which it closes with the reader
// it returns (and only then: on an error the caller still owns it).
func RangeOfFile(f *os.File, offset, length int64) (io.ReadCloser, error) {
	if err := checkRange(offset, length, -1); err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	if err := checkRange(offset, length, info.Size()); err != nil {
		return nil, err
	}
	return newExactReader(struct {
		io.Reader
		io.Closer
	}{io.NewSectionReader(f, offset, length), f}, length), nil
}
