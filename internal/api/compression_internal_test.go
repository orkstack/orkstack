package api

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"
)

// A gzip message stops inflating at the cap: reading it costs at most limit
// bytes of output however far it would inflate, and a message within the
// cap reads whole, to its end.
func TestBoundedGzip_StopsInflatingAtTheCap(t *testing.T) {
	compress := func(n int) []byte {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		if _, err := w.Write(bytes.Repeat([]byte("x"), n)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	const limit = 64 << 10
	d := &boundedGzip{limit: limit}

	bomb := compress(64 << 20)
	if err := d.Reset(bytes.NewReader(bomb)); err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, d)
	if !errors.Is(err, errInflatedPastCap) || n != limit {
		t.Fatalf("%d bytes of gzip inflating to 64 MiB: read %d bytes, err %v; want %d and errInflatedPastCap", len(bomb), n, err, limit)
	}

	for _, size := range []int{0, 1, limit - 1, limit} {
		if err := d.Reset(bytes.NewReader(compress(size))); err != nil {
			t.Fatal(err)
		}
		if n, err := io.Copy(io.Discard, d); err != nil || n != int64(size) {
			t.Fatalf("a message of %d bytes after a refusal: read %d, err %v", size, n, err)
		}
	}
}
