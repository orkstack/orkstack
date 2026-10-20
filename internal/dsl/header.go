package dsl

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// APIVersion is the one apiVersion every authored OrkStack document names.
const APIVersion = "orkstack/v1"

// The kinds an authored document may be.
const (
	KindOrkflow    = "Orkflow"
	KindAutomation = "Automation"
)

// OrkflowHeader is the canonical header, exactly as OrkStack writes it.
const OrkflowHeader = "apiVersion: " + APIVersion + "\nkind: " + KindOrkflow + "\n"

// orkflowHeaderCRLF is OrkflowHeader in a document whose lines end in CRLF.
const orkflowHeaderCRLF = "apiVersion: " + APIVersion + "\r\nkind: " + KindOrkflow + "\r\n"

// HeaderLines is the sentence every surface uses to name the two lines an
// Orkflow begins with, so the CLI and the engine say the same thing about
// the same two lines.
const HeaderLines = `"apiVersion: ` + APIVersion + `" and "kind: ` + KindOrkflow + `"`

// byteOrderMark is the UTF-8 byte order mark. It is never a line of the
// preamble: it is not text an author wrote, so the header algorithms set it
// aside and put it back in front.
const byteOrderMark = "\xEF\xBB\xBF"

// DescribeNode names what a node is, for a type error: "a mapping",
// "a list", "null", "the number 1", "the boolean true", "an alias", or a
// string as Go's %q of its text. It is the one vocabulary every authored
// document's messages use (the Orkflow header, the strict mapping, and the
// automation parser).
func DescribeNode(node *yaml.Node) string {
	switch node.Kind {
	case yaml.MappingNode:
		return "a mapping"
	case yaml.SequenceNode:
		return "a list"
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!null":
			return "null"
		case "!!int", "!!float":
			return "the number " + node.Value
		case "!!bool":
			return "the boolean " + node.Value
		}
		return fmt.Sprintf("%q", node.Value)
	case yaml.AliasNode:
		return "an alias"
	}
	return "an unsupported node"
}

// The header refusals (docs/DSL.md validation rule 45). Each names the two
// lines, or the one line, to write.
var (
	headerMissing = "the document has no apiVersion or kind: an Orkflow begins with the two lines " + HeaderLines +
		` — add them at the top, after any leading comments (docs/DSL.md, "The document header")`
)

// Rule 45: the document begins with `apiVersion: orkstack/v1` and
// `kind: Orkflow`, checked before any other rule; a version stored before
// this rule is never re-checked, because no read path compiles a stored
// source.
//
// checkHeader refuses a document that is not an Orkflow of this apiVersion.
// It reads the root mapping's top-level keys, taking the first occurrence of
// each; a duplicate is refused later by the struct decode. It runs before
// the strict struct decode, so a document of another kind gets the kind
// message rather than "field settings not found".
func checkHeader(source []byte) error {
	var root yaml.Node
	if err := yaml.NewDecoder(bytes.NewReader(source)).Decode(&root); err != nil {
		if errors.Is(err, io.EOF) {
			return validationError("apiVersion", errors.New(headerMissing))
		}
		return fmt.Errorf("parse yaml: %w", err)
	}
	if len(root.Content) == 0 {
		return validationError("apiVersion", errors.New(headerMissing))
	}
	body := root.Content[0]
	if body.Kind != yaml.MappingNode {
		return fmt.Errorf("an Orkflow is a YAML mapping that begins with %q and %q, got %s",
			"apiVersion: "+APIVersion, "kind: "+KindOrkflow, DescribeNode(body))
	}
	var apiVersion, kind *yaml.Node
	for i := 0; i+1 < len(body.Content); i += 2 {
		switch body.Content[i].Value {
		case "apiVersion":
			if apiVersion == nil {
				apiVersion = body.Content[i+1]
			}
		case "kind":
			if kind == nil {
				kind = body.Content[i+1]
			}
		}
	}
	switch {
	case apiVersion == nil && kind == nil:
		return validationError("apiVersion", errors.New(headerMissing))
	case apiVersion == nil:
		return validationError("apiVersion", fmt.Errorf("apiVersion is required: add the line %q above %q",
			"apiVersion: "+APIVersion, "kind: "+KindOrkflow))
	case kind == nil:
		return validationError("kind", fmt.Errorf("kind is required: add the line %q below %q",
			"kind: "+KindOrkflow, "apiVersion: "+APIVersion))
	}
	switch {
	case !stringScalarNode(apiVersion):
		return validationError("apiVersion", fmt.Errorf("apiVersion must be the string %s, got %s",
			APIVersion, DescribeNode(apiVersion)))
	case apiVersion.Value != APIVersion:
		return validationError("apiVersion", fmt.Errorf("apiVersion %q is not supported: this engine reads apiVersion: %s",
			apiVersion.Value, APIVersion))
	case !stringScalarNode(kind):
		return validationError("kind", fmt.Errorf("kind must be the string %s, got %s",
			KindOrkflow, DescribeNode(kind)))
	case kind.Value == KindAutomation:
		return validationError("kind", fmt.Errorf("kind: %s is an automation document, not an Orkflow: "+
			"apply it with orkctl apply or Admin → Automation (docs/AUTOMATION.md); an Orkflow says kind: %s",
			KindAutomation, KindOrkflow))
	case kind.Value != KindOrkflow:
		return validationError("kind", fmt.Errorf("kind %q is not a kind OrkStack reads: an Orkflow says kind: %s, "+
			"an automation document kind: %s", kind.Value, KindOrkflow, KindAutomation))
	}
	return nil
}

// stringScalarNode reports a scalar that resolves to a string: plain,
// quoted, or tagged !!str.
func stringScalarNode(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.Tag == "!!str"
}

// preambleLine reports a line that may stand above the header: blank, a comment,
// a directive (%YAML …), or the document start marker "---" alone or with a comment.
func preambleLine(line string) bool {
	t := strings.TrimRight(line, " \t\r\n")
	switch {
	case t == "", strings.HasPrefix(strings.TrimLeft(t, " \t"), "#"), strings.HasPrefix(t, "%"):
		return true
	case t == "---", strings.HasPrefix(t, "--- #"):
		return true
	}
	return false
}

// AddHeader inserts the canonical header into a document that has none,
// directly above its first top-level key, after the leading comment block,
// in that line's own line ending. It reports false, and returns the source
// unchanged, for a source that is not valid UTF-8, does not decode, is not a
// block mapping, already names apiVersion or kind at its top level, or has
// anything but preamble lines (blank, comment, directive, ---) before its
// first key. A leading byte order mark stays in front.
func AddHeader(source string) (string, bool) {
	if !utf8.ValidString(source) {
		return source, false
	}
	if rest, ok := strings.CutPrefix(source, byteOrderMark); ok {
		added, ok := addHeader(rest)
		if !ok {
			return source, false
		}
		return byteOrderMark + added, true
	}
	return addHeader(source)
}

func addHeader(source string) (string, bool) {
	var doc yaml.Node
	if err := yaml.NewDecoder(strings.NewReader(source)).Decode(&doc); err != nil || len(doc.Content) == 0 {
		return source, false
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode || root.Style&yaml.FlowStyle != 0 || len(root.Content) == 0 {
		return source, false
	}
	for i := 0; i < len(root.Content); i += 2 {
		if key := root.Content[i].Value; key == "apiVersion" || key == "kind" {
			return source, false
		}
	}
	first := root.Content[0]
	if first.Column != 1 {
		return source, false
	}
	lines := strings.SplitAfter(source, "\n")
	offset := 0
	for i := 0; i < first.Line-1; i++ {
		if i >= len(lines) || !preambleLine(lines[i]) {
			return source, false
		}
		offset += len(lines[i])
	}
	header := OrkflowHeader
	rest := source[offset:]
	if end := strings.IndexByte(rest, '\n'); end > 0 && rest[end-1] == '\r' {
		header = orkflowHeaderCRLF
	}
	out := source[:offset] + header + rest
	// The line arithmetic above counts "\n" as the only line break; YAML also
	// breaks a line at a lone "\r" (and yaml.v3 at NEL and the Unicode line
	// and paragraph separators). Rather than model each, the result is held
	// to what it must be — the header as the first two keys, every original
	// key after them, and the identity unchanged — and a source where it is
	// not is shown as stored.
	if !headerPlaced(out, len(root.Content)/2) || string(VersionedSource([]byte(out))) != source {
		return source, false
	}
	return out, true
}

// headerPlaced reports a document whose first two top-level keys are the
// header, at column 1, followed by exactly keys more.
func headerPlaced(source string, keys int) bool {
	var doc yaml.Node
	if err := yaml.NewDecoder(strings.NewReader(source)).Decode(&doc); err != nil || len(doc.Content) == 0 {
		return false
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode || len(root.Content) != 2*(keys+2) {
		return false
	}
	api, kind := root.Content[0], root.Content[2]
	return api.Value == "apiVersion" && api.Column == 1 && root.Content[1].Value == APIVersion &&
		kind.Value == "kind" && kind.Column == 1 && root.Content[3].Value == KindOrkflow
}

// VersionedSource is a document's identity: the source with the two header
// lines removed when they are its first YAML lines — the first lines that are
// not blank, a comment, a directive or "---" — whether comments stand above,
// among or below them. A derived version hashes it, and the store compares it
// (pkg/artifacts.SameStoredOrkflow), so the two read the same bytes. Any other
// form of the header (later, quoted, reordered, with a trailing comment) is
// part of the identity like any other line. It needs no parse.
func VersionedSource(source []byte) []byte {
	if rest, ok := bytes.CutPrefix(source, []byte(byteOrderMark)); ok {
		out := versionedSource(rest)
		if len(out) == len(rest) {
			return source
		}
		return append([]byte(byteOrderMark), out...)
	}
	return versionedSource(source)
}

func versionedSource(source []byte) []byte {
	offset := 0
	for offset < len(source) {
		end := bytes.IndexByte(source[offset:], '\n')
		line := source[offset:]
		if end >= 0 {
			line = source[offset : offset+end+1]
		}
		if !preambleLine(string(line)) {
			break
		}
		offset += len(line)
	}
	rest := source[offset:]
	for _, header := range []string{OrkflowHeader, orkflowHeaderCRLF} {
		if bytes.HasPrefix(rest, []byte(header)) {
			out := make([]byte, 0, len(source)-len(header))
			out = append(out, source[:offset]...)
			return append(out, rest[len(header):]...)
		}
	}
	return source
}

// EmittedSource is how a stored source is SHOWN: with the header, added after
// its leading comment block when it was stored without one (AddHeader). It is
// never used to compare. A source that declares the header as its first YAML
// lines is returned unchanged, and so is one AddHeader cannot place (a flow
// mapping, text before its first key, a header in another form, not YAML).
// VersionedSource(EmittedSource(s)) == VersionedSource(s) for every s, so what
// OrkStack shows is the stored document.
func EmittedSource(source string) string {
	if source == "" {
		return ""
	}
	if string(VersionedSource([]byte(source))) != source {
		return source
	}
	if added, ok := AddHeader(source); ok {
		return added
	}
	return source
}

// HeaderOutOfForm reports a source that names a top-level apiVersion or kind
// but not as the exact two lines written first: another position, another
// order, quoted, a trailing comment, a comment line between the two. It only
// words a refusal (VersionExistsAdvice).
func HeaderOutOfForm(source string) bool {
	if string(VersionedSource([]byte(source))) != source {
		return false
	}
	var doc yaml.Node
	if err := yaml.NewDecoder(strings.NewReader(source)).Decode(&doc); err != nil || len(doc.Content) == 0 {
		return false
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i < len(root.Content); i += 2 {
		if key := root.Content[i].Value; key == "apiVersion" || key == "kind" {
			return true
		}
	}
	return false
}

// VersionExistsAdvice is the advice a refusal to rewrite a stored version
// gives: today's words, unless the incoming document's header is written in
// a form its identity does not leave out, which is then the likely cause.
func VersionExistsAdvice(incomingSource string) string {
	if HeaderOutOfForm(incomingSource) {
		return "its header is not the two lines " + HeaderLines +
			" written first (after any leading comments) — the one form a version leaves out; " +
			"if the header is all you changed, write it that way; otherwise a version is immutable, so publish a new version"
	}
	return "a version is immutable, so publish a new version"
}
