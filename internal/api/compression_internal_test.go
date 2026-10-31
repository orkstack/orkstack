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

// FuzzBoundedGzip: a request's compressed body is input OrkStack does not
// control. Whatever it holds, a gzip stream or not, reading it never yields
// more than the limit; a gzip stream that inflates to the limit or less
// reads whole and exactly; and one that inflates past it fails with
// errInflatedPastCap after exactly the limit. The seeds are the table
// test's sizes around a limit, and bytes that are not gzip at all.
func FuzzBoundedGzip(f *testing.F) {
	for _, seed := range []struct {
		data  []byte
		limit uint16
		raw   bool
	}{
		{nil, 0, false}, {[]byte("x"), 1, false}, {bytes.Repeat([]byte("x"), 1023), 1024, false},
		{bytes.Repeat([]byte("x"), 1024), 1024, false}, {bytes.Repeat([]byte("x"), 1025), 1024, false},
		{bytes.Repeat([]byte("ab"), 40_000), 512, false}, {[]byte("not gzip"), 64, true},
		{[]byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff}, 64, true},
	} {
		f.Add(seed.data, seed.limit, seed.raw)
	}
	f.Fuzz(func(t *testing.T, data []byte, limit uint16, raw bool) {
		body := data
		if !raw {
			var b bytes.Buffer
			w := gzip.NewWriter(&b)
			_, _ = w.Write(data)
			_ = w.Close()
			body = b.Bytes()
		}
		d := &boundedGzip{limit: int64(limit)}
		if err := d.Reset(bytes.NewReader(body)); err != nil {
			if !raw {
				t.Fatalf("Reset on a gzip stream: %v", err)
			}
			return
		}
		var out bytes.Buffer
		n, err := io.Copy(&out, d)
		if n > int64(limit) {
			t.Fatalf("read %d bytes past the limit %d", n, limit)
		}
		if raw {
			return
		}
		switch {
		case len(data) <= int(limit):
			if err != nil || !bytes.Equal(out.Bytes(), data) {
				t.Fatalf("%d bytes under a limit of %d: read %d, err %v", len(data), limit, n, err)
			}
		case !errors.Is(err, errInflatedPastCap) || n != int64(limit):
			t.Fatalf("%d bytes over a limit of %d: read %d, err %v; want %d and errInflatedPastCap", len(data), limit, n, err, limit)
		}
	})
}
