package main

import (
	"errors"
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// archivedEventStream is the small part of a Connect stream the archive
// reader needs. Keeping it here lets the terminal-error rule have a
// deterministic unit test instead of relying on a live connection to drop.
type archivedEventStream interface {
	Receive() bool
	Msg() *orkstackv1.Event
	Err() error
	Close() error
}

var errArchivedEventStreamEmpty = errors.New("archived event stream completed with no events")

type archivedEventStreamError struct {
	bytes int
	err   error
}

func (e *archivedEventStreamError) Error() string {
	return fmt.Sprintf("archived event stream ended after %d byte(s): %v", e.bytes, e.err)
}

func (e *archivedEventStreamError) Unwrap() error { return e.err }

// readArchivedEventText consumes one stream. A terminal stream error discards
// every prefix and returns an error: a partial archive must never be accepted
// as a complete one. The caller may retry an empty, clean stream while an
// archive is still being written.
func readArchivedEventText(stream archivedEventStream) (string, error) {
	if stream == nil {
		return "", errors.New("nil archived event stream")
	}
	defer func() { _ = stream.Close() }()

	var out strings.Builder
	marshaler := protojson.MarshalOptions{Multiline: false}
	for stream.Receive() {
		rendered, err := marshaler.Marshal(stream.Msg())
		if err != nil {
			return "", fmt.Errorf("render archived event: %w", err)
		}
		out.Write(rendered)
		out.WriteByte('\n')
	}
	if err := stream.Err(); err != nil {
		return "", &archivedEventStreamError{bytes: out.Len(), err: err}
	}
	if out.Len() == 0 {
		return "", errArchivedEventStreamEmpty
	}
	return out.String(), nil
}
