package dsl

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// SecondDocumentLine reports whether source holds a second YAML document after
// a first that parses, and the line it begins on. Pure. The second document
// need not parse, and need not hold anything: whatever follows the first
// document would otherwise be ignored in silence.
//
// It reads the text for the line a second document begins on and parses only
// what stands before it, because the parser cannot be asked about a second
// document it cannot read: decoding the first looks one token ahead, so a tab
// just after a marker fails the first document, on the second's line. A marker
// inside the first document's own text is no second document: YAML forbids one
// at the start of a line in a scalar, and what stands before it then does not
// parse. A file whose first document does not parse has no second document to
// report, because the parser's own refusal is the one to read.
func SecondDocumentLine(source []byte) (int, bool) {
	if line, offset, ok := secondDocumentStart(source); ok {
		var first yaml.Node
		if err := yaml.NewDecoder(bytes.NewReader(source[:offset])).Decode(&first); err != nil {
			return 0, false
		}
		return line, true
	}
	// No line reads as a second document's start, and the parser still decides.
	decoder := yaml.NewDecoder(bytes.NewReader(source))
	var first, second yaml.Node
	if err := decoder.Decode(&first); err != nil {
		return 0, false
	}
	switch err := decoder.Decode(&second); {
	case errors.Is(err, io.EOF):
		return 0, false
	case err == nil && second.Line > 0:
		return second.Line, true
	default:
		return 1, true
	}
}

// SecondDocumentMessage is the refusal of a second document in a file that may
// hold one of what. Every reader of an authored document says this, after the
// file and the line where it knows them.
func SecondDocumentMessage(what string) string {
	return "one " + what + " per file: a second YAML document begins here; put it in a file of its own"
}

// refuseSecondDocument is the refusal of a second document in source, or nil.
func refuseSecondDocument(source []byte, what string) error {
	line, found := SecondDocumentLine(source)
	if !found {
		return nil
	}
	return validationError("", fmt.Errorf("line %d: %s", line, SecondDocumentMessage(what)))
}

// secondDocumentStart finds the line a second document begins on, and that
// line's offset: a start marker once a document has begun, or anything but a
// comment or a directive after an end marker.
func secondDocumentStart(source []byte) (line, offset int, found bool) {
	begun, ended := false, false
	for number, start := 1, 0; start < len(source); number++ {
		end, next := yamlLineEnd(source, start)
		text := source[start:end]
		rest := bytes.TrimLeft(text, " \t")
		switch {
		case len(rest) == 0 || rest[0] == '#':
		case text[0] == '%' && (!begun || ended):
		case !begun:
			begun = !isDocumentMarker(text, "...")
		case isDocumentMarker(text, "---"):
			return number, start, true
		case isDocumentMarker(text, "..."):
			ended = true
		case ended:
			return number, start, true
		}
		start = next
	}
	return 0, 0, false
}

// isDocumentMarker reports whether a line is the marker followed by white
// space or nothing, the only places a marker stands.
func isDocumentMarker(text []byte, marker string) bool {
	if !bytes.HasPrefix(text, []byte(marker)) {
		return false
	}
	rest := text[len(marker):]
	return len(rest) == 0 || rest[0] == ' ' || rest[0] == '\t'
}

// yamlLineEnd is where the line at start ends and where the next begins, on
// every line break the parser counts: LF, CR, CR LF, NEL, LS and PS.
func yamlLineEnd(source []byte, start int) (end, next int) {
	for i := start; i < len(source); i++ {
		switch {
		case source[i] == '\n':
			return i, i + 1
		case source[i] == '\r':
			if i+1 < len(source) && source[i+1] == '\n' {
				return i, i + 2
			}
			return i, i + 1
		case source[i] == 0xc2 && i+1 < len(source) && source[i+1] == 0x85:
			return i, i + 2
		case source[i] == 0xe2 && i+2 < len(source) && source[i+1] == 0x80 && (source[i+2] == 0xa8 || source[i+2] == 0xa9):
			return i, i + 3
		}
	}
	return len(source), len(source)
}
