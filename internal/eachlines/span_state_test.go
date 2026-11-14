package eachlines

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

var errNoState = errors.New("no state")

// stateless is a span hash whose state cannot be saved.
type stateless struct{ spanHash }

func (stateless) MarshalBinary() ([]byte, error) { return nil, errNoState }

// A span hash whose state cannot be saved fails the pass with that error
// instead of hashing a gap ahead with nothing to take back.
func TestIndex_ASpanStateThatCannotBeSavedFailsThePass(t *testing.T) {
	data := append([]byte("a\n"), bytes.Repeat([]byte("\n"), gapFlushBytes+1)...)
	data = append(data, "b\n"...)
	o := opts(FormatLines, 2, 10, data)
	s := newScanner(o, 2, stateless{sha256.New().(spanHash)})
	err := s.feed(data)
	if !errors.Is(err, errNoState) {
		t.Fatalf("feed = %v, want the hash's own error", err)
	}
	if ReasonOf(err) != "" {
		t.Fatalf("feed's error carries reason %q; a hash failure is not a refusal of the source", ReasonOf(err))
	}
}
