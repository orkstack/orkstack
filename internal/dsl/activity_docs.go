package dsl

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Activity docs (docs/ACTIVITY_IMAGES.md, "Describing the activity").
//
// An image MAY describe itself, its ops, its with: keys and its values.json
// keys in plain text, in one OCI image label beside its interface — a label
// of its own for the reason the icon has one (activity_icon.go). The docs are
// keyed by the interface's own keys, and the engine checks that every op and
// key they describe is one the interface declares.

// DocsLabel is the OCI image label that carries an ActivityDocs.
const DocsLabel = "org.orkstack.activity.docs"

// Bounds. Docs are config-blob content an untrusted image supplies. A
// description is bounded in characters (Unicode code points), the unit a
// client shows it in; the label as a whole in bytes.
const (
	MaxDocsLabelBytes           = 64 << 10 // 64 KiB of label text, as the interface's
	MaxActivityDescriptionChars = 1000     // the activity's own description
	MaxDescriptionChars         = 500      // an op's, a key's or a value's
)

// ParseDocsLabel turns the label's text into an ActivityDocs valid on its
// own; CheckDocsAgainstInterface then holds it to the interface it
// describes. An empty value declares no docs (nil, nil). The error is the
// reason; ActivityInfo.docs_error is it through SafeReason. Pure.
func ParseDocsLabel(raw string) (*orkstackv1.ActivityDocs, error) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > MaxDocsLabelBytes {
		return nil, fmt.Errorf("label is %d bytes, limit is %d", len(raw), MaxDocsLabelBytes)
	}
	docs := &orkstackv1.ActivityDocs{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(raw), docs); err != nil {
		return nil, fmt.Errorf("label is not the JSON encoding of an ActivityDocs: %v", err)
	}
	if err := validateDocs(docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// validateDocs holds decoded docs to every rule the label states on its own.
func validateDocs(docs *orkstackv1.ActivityDocs) error {
	if docs.GetSchemaVersion() != 1 {
		return fmt.Errorf("schemaVersion %d is not supported (this engine reads 1)", docs.GetSchemaVersion())
	}
	if text := docs.GetDescription(); text != "" {
		if err := checkText("description", text, MaxActivityDescriptionChars); err != nil {
			return err
		}
	}
	if n := len(docs.GetOps()); n > MaxInterfaceOps {
		return fmt.Errorf("describes %d ops, limit is %d", n, MaxInterfaceOps)
	}
	if err := checkTextMap("fields", docs.GetFields()); err != nil {
		return err
	}
	if err := checkTextMap("values", docs.GetValues()); err != nil {
		return err
	}
	for _, op := range mapKeys(docs.GetOps()) {
		if !opNamePattern.MatchString(op) {
			return fmt.Errorf("op %q is not a lowercase op name (%s)", op, opNamePattern.String())
		}
		described := docs.GetOps()[op]
		if text := described.GetDescription(); text != "" {
			if err := checkText("ops."+op+".description", text, MaxDescriptionChars); err != nil {
				return err
			}
		}
		if err := checkTextMap("ops."+op+".fields", described.GetFields()); err != nil {
			return err
		}
	}
	return nil
}

// checkTextMap validates one key → text map; where names it in messages.
func checkTextMap(where string, texts map[string]string) error {
	if n := len(texts); n > MaxInterfaceFields {
		return fmt.Errorf("%s describes %d keys, limit is %d", where, n, MaxInterfaceFields)
	}
	for _, key := range mapKeys(texts) {
		if !interfaceKeyPattern.MatchString(key) {
			return fmt.Errorf("%s: key %q is not a valid with: key", where, key)
		}
		if err := checkText(where+"."+key, texts[key], MaxDescriptionChars); err != nil {
			return err
		}
	}
	return nil
}

// checkText holds one description to the plain-text rules: present, valid
// UTF-8, at most limit characters, one paragraph with no control character
// (a line break, a tab and the line and paragraph separators included), no
// format character (bidirectional controls, zero-width and invisible
// characters, tags), which would make a client show text other than the one
// it holds, and no noncharacter.
func checkText(where, text string, limit int) error {
	switch {
	case strings.TrimSpace(text) == "":
		return fmt.Errorf("%s is empty: omit a description that says nothing", where)
	case !utf8.ValidString(text):
		return fmt.Errorf("%s is not valid UTF-8", where)
	}
	if n := utf8.RuneCountInString(text); n > limit {
		return fmt.Errorf("%s is %d characters, limit is %d", where, n, limit)
	}
	for _, r := range text {
		switch {
		case unicode.IsControl(r), r == 0x2028, r == 0x2029:
			return fmt.Errorf("%s holds the control character %U; a description is one paragraph of plain text", where, r)
		case unicode.Is(unicode.Cf, r):
			// Bidirectional marks and overrides, zero-width and invisible
			// characters, the tag characters: each makes text show other
			// than it reads.
			return fmt.Errorf("%s holds the format character %U; a description is plain text", where, r)
		case r >= 0xfdd0 && r <= 0xfdef, r&0xfffe == 0xfffe:
			return fmt.Errorf("%s holds the noncharacter %U", where, r)
		}
	}
	return nil
}

// CheckDocsAgainstInterface holds valid docs to the interface they describe:
// every described op is a declared op, every described key a key declared in
// the same place, every described value a declared value. iface nil — no
// interface label, or one this engine cannot read — checks nothing: the two
// labels fail independently, and docs that cannot be checked are still
// readable. Pure; the first mismatch, in sorted order.
func CheckDocsAgainstInterface(docs *orkstackv1.ActivityDocs, iface *orkstackv1.ActivityInterface) error {
	if iface == nil {
		return nil
	}
	undeclared := func(what string) error {
		return fmt.Errorf("describes %s, which the interface does not declare; "+
			"an image built FROM another inherits every label it does not set", what)
	}
	for _, key := range mapKeys(docs.GetFields()) {
		if _, ok := iface.GetFields()[key]; !ok {
			return undeclared(fmt.Sprintf("the shared with: key %q", key))
		}
	}
	for _, op := range mapKeys(docs.GetOps()) {
		declared, ok := iface.GetOps()[op]
		if !ok {
			return undeclared(fmt.Sprintf("the op %q", op))
		}
		for _, key := range mapKeys(docs.GetOps()[op].GetFields()) {
			if _, ok := declared.GetFields()[key]; !ok {
				return undeclared(fmt.Sprintf("the with: key %q of op %q", key, op))
			}
		}
	}
	for _, key := range mapKeys(docs.GetValues()) {
		if _, ok := iface.GetValues()[key]; !ok {
			return undeclared(fmt.Sprintf("the values.json key %q", key))
		}
	}
	return nil
}

// lowestDocsSchemaVersion is the docs label schemaVersion the packer writes:
// the lowest that expresses the docs. Version 1 is the only one.
func lowestDocsSchemaVersion(*orkstackv1.ActivityDocs) int32 { return 1 }

// PackDocsLabel is the label value that declares docs, by
// PackInterfaceLabel's canonical encoding, held to every rule the engine
// reads the label by — iface is the interface the same image declares. Nil
// docs pack to "", the value that declares none.
func PackDocsLabel(docs *orkstackv1.ActivityDocs, iface *orkstackv1.ActivityInterface) (string, error) {
	if docs == nil {
		return "", nil
	}
	out := proto.Clone(docs).(*orkstackv1.ActivityDocs)
	out.SchemaVersion = lowestDocsSchemaVersion(out)
	if err := validateDocs(out); err != nil {
		return "", err
	}
	if err := CheckDocsAgainstInterface(out, iface); err != nil {
		return "", err
	}
	return packLabel(out, MaxDocsLabelBytes)
}
