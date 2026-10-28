package dsl

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// APIVersion is the one apiVersion every authored OrkStack document names.
const APIVersion = "orkstack/v1"

// The kinds an authored document may be.
const (
	KindOrkflow    = "Orkflow"
	KindAutomation = "Automation"
	// KindActivityInterface is an image's with: contract, authored: it is
	// packed into the image's interface label at build time and never
	// applied (docs/ACTIVITY_IMAGES.md, "Declaring the interface").
	KindActivityInterface = "ActivityInterface"
)

// OrkflowHeader is the canonical header, exactly as OrkStack writes it.
const OrkflowHeader = "apiVersion: " + APIVersion + "\nkind: " + KindOrkflow + "\n"

// HeaderLines is the sentence every surface uses to name the two lines an
// Orkflow begins with, so the CLI and the engine say the same thing about
// the same two lines.
const HeaderLines = `"apiVersion: ` + APIVersion + `" and "kind: ` + KindOrkflow + `"`

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
// `kind: Orkflow`, checked before any other rule. No read path compiles a
// stored source, so the rule applies when a document is applied, never again.
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
	case kind.Value == KindActivityInterface:
		return validationError("kind", fmt.Errorf("kind: %s is an activity interface, not an Orkflow: "+
			"pack it into an image's label with orkctl activity interface pack (docs/ACTIVITY_IMAGES.md); "+
			"an Orkflow says kind: %s", KindActivityInterface, KindOrkflow))
	case kind.Value != KindOrkflow:
		return validationError("kind", fmt.Errorf("kind %q is not a kind OrkStack reads: an Orkflow says kind: %s, "+
			"an automation document kind: %s, an activity interface kind: %s",
			kind.Value, KindOrkflow, KindAutomation, KindActivityInterface))
	}
	return nil
}

// stringScalarNode reports a scalar that resolves to a string: plain,
// quoted, or tagged !!str.
func stringScalarNode(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.Tag == "!!str"
}
