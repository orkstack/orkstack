package artifacts

import (
	"context"
	"io"
	"net/http"
)

// sniffLen is the number of leading bytes http.DetectContentType inspects.
const sniffLen = 512

// defaultContentType is what a store records when nothing better is known.
const defaultContentType = "application/octet-stream"

// sniffWriter captures the first sniffLen bytes of a stream for content-type
// detection while the stream is being written elsewhere.
type sniffWriter struct{ head []byte }

func (w *sniffWriter) Write(p []byte) (int, error) {
	if n := sniffLen - len(w.head); n > 0 {
		if n > len(p) {
			n = len(p)
		}
		w.head = append(w.head, p[:n]...)
	}
	return len(p), nil
}

// resolveContentType prefers an explicit content type and otherwise sniffs the
// captured head bytes.
func resolveContentType(explicit string, head []byte) string {
	if explicit != "" {
		return explicit
	}
	if detected := http.DetectContentType(head); detected != "" {
		return detected
	}
	return defaultContentType
}

// copyMetadata defensively copies caller metadata so a stored reference cannot
// be mutated behind the store's back. An empty map becomes nil so references
// stay JSON-clean.
func copyMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// ctxReader aborts a long copy when the caller's context is cancelled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
