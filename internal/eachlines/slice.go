package eachlines

import (
	"bufio"
	"bytes"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// Value is the value of one unbatched item whose span is given: a jsonl line
// compacted, or a lines line as a JSON string. It refuses a span the indexer
// would have refused.
func Value(span []byte, f Format) ([]byte, error) {
	if !utf8.Valid(span) {
		return nil, refuse(ReasonLineInvalid, "the line is not valid UTF-8")
	}
	switch f {
	case FormatJSONL:
		var out bytes.Buffer
		if err := stablejson.Compact(&out, span); err != nil || !isJSON(span) {
			return nil, refuse(ReasonLineInvalid, "the line is not one valid JSON value")
		}
		return out.Bytes(), nil
	case FormatLines:
		return encodeText(span)
	}
	return nil, fmt.Errorf("eachlines: unknown format %d", f)
}

// encodeText is a text line as the JSON string an unbatched item carries. The
// HTML characters stay as they are, as in an array's items.
func encodeText(line []byte) ([]byte, error) {
	return json.Marshal(string(line), stablejson.Unescaped)
}

// Slice is what a batched item is staged as: the non-empty lines of its span,
// each with the CR before its LF removed and an LF after it, and how many there
// are. A span ends on its last line's content, so its last line is kept whole.
// More than k lines is an error: the index and the source disagree.
func Slice(span []byte, k int) ([]byte, int, error) {
	if k < 1 {
		return nil, 0, fmt.Errorf("eachlines: batch %d is below 1", k)
	}
	var out []byte
	lines := 0
	for len(span) > 0 {
		var line []byte
		if i := bytes.IndexByte(span, '\n'); i >= 0 {
			line, span = span[:i], span[i+1:]
			line = bytes.TrimSuffix(line, []byte("\r"))
		} else {
			line, span = span, nil
		}
		if len(line) == 0 {
			continue
		}
		if lines++; lines > k {
			return nil, 0, refuse(ReasonItemUnavailable, "the span holds more than the %d lines its index says", k)
		}
		out = append(out, line...)
		out = append(out, '\n')
	}
	return out, lines, nil
}

// SliceTo is Slice from a reader to a writer, in memory of one read buffer
// whatever the span's size; it returns the lines written.
func SliceTo(dst io.Writer, src io.Reader, k int) (int, error) {
	return sliceTo(dst, src, k, ReadBufferSize)
}

func sliceTo(dst io.Writer, src io.Reader, k, bufSize int) (int, error) {
	if k < 1 {
		return 0, fmt.Errorf("eachlines: batch %d is below 1", k)
	}
	br := bufio.NewReaderSize(src, bufSize)
	var (
		lines, lineLen int
		heldCR         bool // a CR ended the last piece; it is content unless an LF follows at once
	)
	write := func(p []byte) error { _, err := dst.Write(p); return err }
	for {
		chunk, err := br.ReadSlice('\n')
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
			return lines, fmt.Errorf("reading the span: %w", err)
		}
		final := errors.Is(err, io.EOF)
		terminated := err == nil
		body := chunk
		if terminated {
			body = body[:len(body)-1]
		}
		switch {
		case terminated && len(body) == 0 && heldCR:
			heldCR = false // the CR was the one before this LF
		case terminated && len(body) > 0 && heldCR:
			if err := write([]byte{'\r'}); err != nil {
				return lines, err
			}
			lineLen++
			heldCR = false
			fallthrough
		case terminated:
			body = bytes.TrimSuffix(body, []byte("\r"))
		case !final:
			if heldCR {
				if err := write([]byte{'\r'}); err != nil {
					return lines, err
				}
				lineLen++
				heldCR = false
			}
			if n := len(body); n > 0 && body[n-1] == '\r' {
				heldCR, body = true, body[:n-1]
			}
		default: // the end, in a line without an LF: all of it is content
			if heldCR {
				if err := write([]byte{'\r'}); err != nil {
					return lines, err
				}
				lineLen++
				heldCR = false
			}
		}
		if len(body) > 0 {
			if err := write(body); err != nil {
				return lines, err
			}
			lineLen += len(body)
		}
		if terminated || final {
			if lineLen > 0 {
				if lines++; lines > k {
					return lines, refuse(ReasonItemUnavailable, "the span holds more than the %d lines its index says", k)
				}
				if err := write([]byte{'\n'}); err != nil {
					return lines, err
				}
			}
			lineLen = 0
		}
		if final {
			return lines, nil
		}
	}
}
