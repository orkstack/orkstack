package dsl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Activity interface documents (docs/ACTIVITY_IMAGES.md, "Declaring the
// interface").
//
// The interface label stays the ABI: the engine reads the label and nothing
// else. A person writes the same declaration as YAML, kind: ActivityInterface
// (AGENTS.md §3.1), and PackInterfaceLabel turns it into the label's value at
// build time — through `orkctl activity interface pack`, `orkctl activity
// build --interface`, or the generated LABEL line of a first-party
// Dockerfile. The document is never read at run time and never stored.

// MaxInterfaceDocumentBytes bounds an authored interface document. It is
// looser than the label's bound because comments and indentation are not
// packed; the packed label is still held to MaxInterfaceLabelBytes.
const MaxInterfaceDocumentBytes = 1 << 20

// interfaceHeaderLines names the two lines an activity interface begins
// with.
const interfaceHeaderLines = `"apiVersion: ` + APIVersion + `" and "kind: ` + KindActivityInterface + `"`

const interfaceHeaderMissing = "the document has no apiVersion or kind: an activity interface begins with the two lines " +
	interfaceHeaderLines + " — add them at the top, after any leading comments"

// interfaceDocument is kind: ActivityInterface as authored: the
// ActivityInterface message's keys, in lowerCamel as the label spells them,
// and no schemaVersion — the header names the document's format, and the
// packer writes the label's version (lowestSchemaVersion). Its yaml tags are
// the key table the document's JSON Schema is generated from.
type interfaceDocument struct {
	APIVersion string                            `yaml:"apiVersion"`
	Kind       string                            `yaml:"kind"`
	Fields     map[string]interfaceFieldDocument `yaml:"fields"`
	Ops        map[string]interfaceOpDocument    `yaml:"ops"`
	DefaultOp  string                            `yaml:"defaultOp"`
	Values     map[string]interfaceFieldDocument `yaml:"values"`
}

// interfaceOpDocument is one op: the keys it reads beside fields.
type interfaceOpDocument struct {
	Fields map[string]interfaceFieldDocument `yaml:"fields"`
}

// interfaceFieldDocument is one with: key's declaration.
type interfaceFieldDocument struct {
	Type string `yaml:"type"`
	// Required is read through the strict mapping as well: yaml.v3 decodes
	// yes, on and y into a Go bool, and YAML 1.2 readers see a string.
	Required yaml.Node `yaml:"required"`
	// Default is authored data, so it becomes JSON through the strict
	// mapping (AuthoredJSON), never a lenient decode.
	Default yaml.Node `yaml:"default"`
}

// ParseInterfaceDocument reads an authored activity interface: the header,
// the strict mapping for every default, and the rules a label is held to
// (validateInterface). The interface it returns carries the schemaVersion
// the packer writes. Pure.
func ParseInterfaceDocument(source []byte) (*orkstackv1.ActivityInterface, error) {
	if len(source) > MaxInterfaceDocumentBytes {
		return nil, fmt.Errorf("the document is %d bytes, limit is %d", len(source), MaxInterfaceDocumentBytes)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(source))
	var root yaml.Node
	if err := decoder.Decode(&root); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, validationError("apiVersion", errors.New(interfaceHeaderMissing))
		}
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	var second yaml.Node
	switch err := decoder.Decode(&second); {
	case err == nil:
		return nil, errors.New("one activity interface per file: a second YAML document (---) is not allowed")
	case !errors.Is(err, io.EOF):
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if len(root.Content) == 0 {
		return nil, validationError("apiVersion", errors.New(interfaceHeaderMissing))
	}
	body := root.Content[0]
	if err := checkInterfaceHeader(body); err != nil {
		return nil, err
	}
	// Anchors, aliases, merge keys and tags are refused anywhere in the
	// document, not only in a default: the declaration is read by editors,
	// the packer and reviewers alike, and each must see the same keys.
	if err := refuseStructural(body, ""); err != nil {
		return nil, err
	}
	for i := 0; i+1 < len(body.Content); i += 2 {
		if body.Content[i].Value == "schemaVersion" {
			return nil, validationError("schemaVersion", errors.New("schemaVersion is not written in an activity interface: "+
				"apiVersion names the document's format, and orkctl activity interface pack writes the label's schemaVersion"))
		}
	}

	// Strict: a key the document does not define is refused, never ignored,
	// as an Orkflow's are.
	var doc interfaceDocument
	strict := yaml.NewDecoder(bytes.NewReader(source))
	strict.KnownFields(true)
	if err := strict.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	iface := &orkstackv1.ActivityInterface{DefaultOp: doc.DefaultOp}
	var err error
	if iface.Fields, err = interfaceFields("fields", doc.Fields); err != nil {
		return nil, err
	}
	if iface.Values, err = interfaceFields("values", doc.Values); err != nil {
		return nil, err
	}
	if len(doc.Ops) > 0 {
		iface.Ops = make(map[string]*orkstackv1.OpSchema, len(doc.Ops))
		for _, name := range mapKeys(doc.Ops) {
			fields, err := interfaceFields("ops."+name+".fields", doc.Ops[name].Fields)
			if err != nil {
				return nil, err
			}
			iface.Ops[name] = &orkstackv1.OpSchema{Fields: fields}
		}
	}
	iface.SchemaVersion = lowestSchemaVersion(iface)
	if err := validateInterface(iface); err != nil {
		return nil, err
	}
	return iface, nil
}

// interfaceFields converts one fields/values map; where names it in errors.
func interfaceFields(where string, fields map[string]interfaceFieldDocument) (map[string]*orkstackv1.FieldSchema, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	out := make(map[string]*orkstackv1.FieldSchema, len(fields))
	for _, key := range mapKeys(fields) {
		field := fields[key]
		schema := &orkstackv1.FieldSchema{Type: field.Type}
		if field.Required.Kind != 0 {
			path := where + "." + key + ".required"
			value, err := AuthoredJSON(&field.Required, path)
			if err != nil {
				return nil, err
			}
			required, ok := value.(bool)
			if !ok {
				return nil, &AuthoredDataError{Path: path, Line: field.Required.Line,
					Reason: "required is true or false, got " + DescribeNode(&field.Required)}
			}
			schema.Required = required
		}
		if field.Default.Kind != 0 {
			value, err := AuthoredJSON(&field.Default, where+"."+key+".default")
			if err != nil {
				return nil, err
			}
			if schema.Default, err = structpb.NewValue(value); err != nil {
				return nil, fmt.Errorf("%s.%s.default: %w", where, key, err)
			}
		}
		out[key] = schema
	}
	return out, nil
}

// checkInterfaceHeader is rule 45's header check for kind: ActivityInterface:
// the two lines, read off the root mapping's first occurrences, each refusal
// naming the line to write.
func checkInterfaceHeader(body *yaml.Node) error {
	if body.Kind != yaml.MappingNode {
		return fmt.Errorf("an activity interface is a YAML mapping that begins with %q and %q, got %s",
			"apiVersion: "+APIVersion, "kind: "+KindActivityInterface, DescribeNode(body))
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
		return validationError("apiVersion", errors.New(interfaceHeaderMissing))
	case apiVersion == nil:
		return validationError("apiVersion", fmt.Errorf("apiVersion is required: add the line %q above %q",
			"apiVersion: "+APIVersion, "kind: "+KindActivityInterface))
	case kind == nil:
		return validationError("kind", fmt.Errorf("kind is required: add the line %q below %q",
			"kind: "+KindActivityInterface, "apiVersion: "+APIVersion))
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
			KindActivityInterface, DescribeNode(kind)))
	case kind.Value == KindOrkflow:
		return validationError("kind", fmt.Errorf("kind: %s is an Orkflow, not an activity interface: "+
			"apply it with orkctl apply; an activity interface says kind: %s", KindOrkflow, KindActivityInterface))
	case kind.Value == KindAutomation:
		return validationError("kind", fmt.Errorf("kind: %s is an automation document, not an activity interface: "+
			"apply it with orkctl apply; an activity interface says kind: %s", KindAutomation, KindActivityInterface))
	case kind.Value != KindActivityInterface:
		return validationError("kind", fmt.Errorf("kind %q is not an activity interface: an activity interface says kind: %s",
			kind.Value, KindActivityInterface))
	}
	return nil
}

// refuseStructural refuses an alias, anchor, merge key or tag anywhere under
// node, naming the path it stands at.
func refuseStructural(node *yaml.Node, path string) error {
	if err := structural(node, path); err != nil {
		return err
	}
	switch node.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			if err := structural(key, path); err != nil {
				return err
			}
			child := key.Value
			if path != "" {
				child = path + "." + key.Value
			}
			if err := refuseStructural(value, child); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for i, item := range node.Content {
			if err := refuseStructural(item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// lowestSchemaVersion is the label schemaVersion the packer writes for
// iface: the lowest that expresses it, so a newer orkctl never writes a
// label deployed engines read as unreadable while an older version says the
// same thing. Version 1 is the only one and expresses every interface; a
// later version is written only for a document that uses what it adds.
func lowestSchemaVersion(*orkstackv1.ActivityInterface) int32 { return 1 }

// PackInterfaceLabel is the label value that declares iface: its protojson
// encoding (lowerCamel names, field order, map keys sorted) with every
// insignificant space removed and every single quote escaped as \u0027, so
// the value fits a Dockerfile's single-quoted LABEL as it is. It writes the
// lowest schemaVersion into a copy, holds the copy to every rule the engine
// reads a label by, and refuses a value past MaxInterfaceLabelBytes. The
// bytes are canonical: protojson's deliberately unstable whitespace never
// reaches them, so one interface always packs to one label, and one label
// to one image config digest.
func PackInterfaceLabel(iface *orkstackv1.ActivityInterface) (string, error) {
	out := &orkstackv1.ActivityInterface{}
	if iface != nil {
		out = proto.Clone(iface).(*orkstackv1.ActivityInterface)
	}
	out.SchemaVersion = lowestSchemaVersion(out)
	if err := validateInterface(out); err != nil {
		return "", err
	}
	raw, err := protojson.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("encode the label: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return "", fmt.Errorf("encode the label: %w", err)
	}
	label := strings.ReplaceAll(compact.String(), "'", `\u0027`)
	if len(label) > MaxInterfaceLabelBytes {
		return "", fmt.Errorf("label is %d bytes, limit is %d", len(label), MaxInterfaceLabelBytes)
	}
	return label, nil
}
