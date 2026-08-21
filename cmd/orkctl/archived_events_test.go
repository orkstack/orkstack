package main

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

type fakeArchivedEventStream struct {
	events  []*orkstackv1.Event
	current *orkstackv1.Event
	err     error
	i       int
	closed  bool
}

func (s *fakeArchivedEventStream) Receive() bool {
	if s.i == len(s.events) {
		return false
	}
	s.current = s.events[s.i]
	s.i++
	return true
}

func (s *fakeArchivedEventStream) Msg() *orkstackv1.Event { return s.current }
func (s *fakeArchivedEventStream) Err() error             { return s.err }
func (s *fakeArchivedEventStream) Close() error {
	s.closed = true
	return nil
}

func TestReadArchivedEventText_RejectsPrefixWhenStreamEndsWithError(t *testing.T) {
	sentinel := errors.New("connection dropped")
	stream := &fakeArchivedEventStream{
		events: []*orkstackv1.Event{{Message: "partial"}},
		err:    sentinel,
	}

	got, err := readArchivedEventText(stream)
	if err == nil {
		t.Fatal("readArchivedEventText accepted a stream with a terminal error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want terminal stream error %v", err, sentinel)
	}
	if got != "" {
		t.Fatalf("text = %q, want no partial prefix after a dropped stream", got)
	}
	if !stream.closed {
		t.Fatal("readArchivedEventText did not close the failed stream")
	}
	if strings.Contains(got, "partial") {
		t.Fatal("a dropped stream leaked its prefix to the caller")
	}
}

func TestReadArchivedEventTextReturnsCompleteStream(t *testing.T) {
	stream := &fakeArchivedEventStream{
		events: []*orkstackv1.Event{{Message: "complete"}},
	}

	got, err := readArchivedEventText(stream)
	if err != nil {
		t.Fatalf("readArchivedEventText: %v", err)
	}
	if !strings.Contains(got, "complete") {
		t.Fatalf("text = %q, want the complete event", got)
	}
}
