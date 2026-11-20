package dsl

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"

	"go.yaml.in/yaml/v4"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// Service interface documents (docs/ORKER_SERVICE_CONTRACT.md, "A service
// image").
//
// A service image's interface label (eventsources.InterfaceLabel) is the
// ABI the engine reads; a person writes the same declaration as YAML,
// kind: ServiceInterface (AGENTS.md §3.1), by convention service.ork.yaml
// beside the Dockerfile, and `orkctl activity interface pack` turns it into
// the label's value. The document is never applied, stored or read at run
// time.

// serviceInterfaceHeaderLines names the two lines a service interface
// begins with.
const serviceInterfaceHeaderLines = `"apiVersion: ` + APIVersion + `" and "kind: ` + KindServiceInterface + `"`

// serviceInterfaceDocument is kind: ServiceInterface as authored. Its yaml
// tags are the key table its JSON Schema is generated from.
type serviceInterfaceDocument struct {
	APIVersion  string                          `yaml:"apiVersion"`
	Kind        string                          `yaml:"kind"`
	Profile     yaml.Node                       `yaml:"profile"`
	With        map[string]serviceFieldDocument `yaml:"with"`
	EventSource *eventSourceDeclarationDocument `yaml:"event_source"`
}

// serviceFieldDocument is one with: key's declaration, in the activity
// interface's grammar (decision D2): a service interface has no docs label,
// so it takes no description.
type serviceFieldDocument struct {
	Type     string    `yaml:"type"`
	Required yaml.Node `yaml:"required"`
	Default  yaml.Node `yaml:"default"`
}

// eventSourceDeclarationDocument is the event-source profile's block.
type eventSourceDeclarationDocument struct {
	Delivery       yaml.Node `yaml:"delivery"`
	MaxConcurrency yaml.Node `yaml:"max_concurrency"`
	Ordering       yaml.Node `yaml:"ordering"`
	DeadLetter     yaml.Node `yaml:"dead_letter"`
	ExtendsLeases  yaml.Node `yaml:"extends_leases"`
}

// ParseServiceInterfaceDocument reads an authored service interface: the
// header, the strict mapping for every value, and the rules the engine
// reads the packed label by (eventsources.ValidateInterface). The message
// it returns carries the schemaVersion the packer writes. Pure.
func ParseServiceInterfaceDocument(source []byte) (*orkstackv1.ServiceInterface, error) {
	if len(source) > MaxInterfaceDocumentBytes {
		return nil, fmt.Errorf("the document is %d bytes, limit is %d", len(source), MaxInterfaceDocumentBytes)
	}
	if err := refuseSecondDocument(source, "service interface"); err != nil {
		return nil, err
	}
	var root yaml.Node
	if err := yaml.NewDecoder(bytes.NewReader(source)).Decode(&root); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, validationError("apiVersion", errors.New(serviceInterfaceHeaderMissing()))
		}
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if len(root.Content) == 0 {
		return nil, validationError("apiVersion", errors.New(serviceInterfaceHeaderMissing()))
	}
	body := root.Content[0]
	if err := checkServiceInterfaceHeader(body); err != nil {
		return nil, err
	}
	if err := refuseStructural(body, ""); err != nil {
		return nil, err
	}
	for i := 0; i+1 < len(body.Content); i += 2 {
		key, value := body.Content[i], body.Content[i+1]
		switch key.Value {
		case "schemaVersion", "schema_version":
			return nil, validationError(key.Value, errors.New("schemaVersion is not written in a service interface: "+
				"apiVersion names the document's format, and orkctl activity interface pack writes the label's schemaVersion"))
		case "with":
			if err := withGrammarHint(value); err != nil {
				return nil, err
			}
		}
	}
	var doc serviceInterfaceDocument
	if err := mergeTagRefusal(source, doc); err != nil {
		return nil, err
	}
	strict := yaml.NewDecoder(bytes.NewReader(source))
	strict.KnownFields(true)
	if err := strict.Decode(&doc); err != nil {
		return nil, strictDecodeError(source, reflect.TypeOf(doc), "service interface", err)
	}

	profile, err := documentString(&doc.Profile, "profile")
	if err != nil {
		return nil, err
	}
	if profile == "" {
		return nil, validationError("profile", fmt.Errorf("profile is required: a source image says profile: %s", eventsources.ProfileEventSource))
	}
	iface := &orkstackv1.ServiceInterface{SchemaVersion: 1, Profile: profile}
	if n := len(doc.With); n > MaxInterfaceFields {
		return nil, validationError("with", fmt.Errorf("with declares %d keys, limit is %d", n, MaxInterfaceFields))
	}
	fields := make(map[string]interfaceFieldDocument, len(doc.With))
	for key, f := range doc.With {
		if !interfaceKeyPattern.MatchString(key) {
			return nil, validationError("with", fmt.Errorf("with: key %q must match %s", key, interfaceKeyPattern))
		}
		fields[key] = interfaceFieldDocument{Type: f.Type, Required: f.Required, Default: f.Default}
	}
	if iface.With, err = interfaceFields("with", fields); err != nil {
		return nil, err
	}
	if doc.EventSource != nil {
		if profile != eventsources.ProfileEventSource {
			return nil, validationError("event_source", fmt.Errorf("event_source: is the %s profile's block; this document's profile is %q",
				eventsources.ProfileEventSource, profile))
		}
		if iface.EventSource, err = eventSourceDeclaration(doc.EventSource); err != nil {
			return nil, err
		}
	}
	if err := eventsources.ValidateInterface(iface); err != nil {
		return nil, validationError("", err)
	}
	return iface, nil
}

// PackServiceInterfaceLabel is the label value that declares iface, in the
// canonical encoding every interface label shares (packLabel), held to the
// rules the engine reads it by and to eventsources.MaxInterfaceLabelBytes.
func PackServiceInterfaceLabel(iface *orkstackv1.ServiceInterface) (string, error) {
	if iface == nil {
		return "", errors.New("no service interface to pack")
	}
	if err := eventsources.ValidateInterface(iface); err != nil {
		return "", err
	}
	return packLabel(iface, eventsources.MaxInterfaceLabelBytes)
}

// withGrammarHint refuses the JSON-Schema-like shape (`required: [...]`,
// `properties:`) with the grammar to write instead: with: names each key
// with its own type, required and default.
func withGrammarHint(with *yaml.Node) error {
	if with.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(with.Content); i += 2 {
		key, value := with.Content[i], with.Content[i+1]
		if (key.Value == "properties" || key.Value == "required") && value.Kind != yaml.MappingNode {
			return validationError("with."+key.Value, fmt.Errorf("line %d: with: names each key with its own declaration, "+
				"as an activity interface's fields: do: `with: {stream: {type: string, required: true}}`, not required: and properties: lists",
				key.Line))
		}
		if key.Value == "properties" && value.Kind == yaml.MappingNode {
			if _, hasType := mappingValue(value, "type"); !hasType {
				return validationError("with.properties", fmt.Errorf("line %d: with: names each key directly, "+
					"with no properties: level: write `with: {stream: {type: string}}`", key.Line))
			}
		}
	}
	return nil
}

func mappingValue(node *yaml.Node, key string) (*yaml.Node, bool) {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1], true
		}
	}
	return nil, false
}

// eventSourceDeclaration reads the event-source block through the strict
// mapping: every value is a JSON value of its own type, never a lenient
// YAML reading.
func eventSourceDeclaration(doc *eventSourceDeclarationDocument) (*orkstackv1.EventSourceDeclaration, error) {
	out := &orkstackv1.EventSourceDeclaration{}
	delivery, err := documentString(&doc.Delivery, "event_source.delivery")
	if err != nil {
		return nil, err
	}
	switch delivery {
	case "":
	case "at-least-once":
		out.Delivery = orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_LEAST_ONCE
	case "at-most-once":
		out.Delivery = orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE
	default:
		return nil, &AuthoredDataError{Path: "event_source.delivery", Line: doc.Delivery.Line,
			Reason: fmt.Sprintf("delivery is at-least-once or at-most-once, got %q", delivery)}
	}
	if doc.MaxConcurrency.Kind != 0 {
		value, err := AuthoredJSON(&doc.MaxConcurrency, "event_source.max_concurrency")
		if err != nil {
			return nil, err
		}
		n, ok := value.(float64)
		if !ok || n != math.Trunc(n) || n < 1 || n > eventsources.MaxConcurrency {
			return nil, &AuthoredDataError{Path: "event_source.max_concurrency", Line: doc.MaxConcurrency.Line,
				Reason: fmt.Sprintf("max_concurrency is a whole number from 1 to %d, got %s", eventsources.MaxConcurrency, DescribeNode(&doc.MaxConcurrency))}
		}
		out.MaxConcurrency = int32(n)
	}
	flag := func(node *yaml.Node, path string) (*bool, error) {
		if node.Kind == 0 {
			return nil, nil
		}
		value, err := AuthoredJSON(node, path)
		if err != nil {
			return nil, err
		}
		b, ok := value.(bool)
		if !ok {
			return nil, &AuthoredDataError{Path: path, Line: node.Line, Reason: "want true or false, got " + DescribeNode(node)}
		}
		return &b, nil
	}
	ordering, err := flag(&doc.Ordering, "event_source.ordering")
	if err != nil {
		return nil, err
	}
	out.Ordering = ordering != nil && *ordering
	if out.DeadLetter, err = flag(&doc.DeadLetter, "event_source.dead_letter"); err != nil {
		return nil, err
	}
	if out.ExtendsLeases, err = flag(&doc.ExtendsLeases, "event_source.extends_leases"); err != nil {
		return nil, err
	}
	return out, nil
}

func serviceInterfaceHeaderMissing() string {
	return "the document has no apiVersion or kind: a service interface begins with the two lines " +
		serviceInterfaceHeaderLines + " — add them at the top, after any leading comments"
}

// checkServiceInterfaceHeader is rule 45's header check for
// kind: ServiceInterface, each refusal naming the line to write and a
// document of another kind named for what it is.
func checkServiceInterfaceHeader(body *yaml.Node) error {
	if body.Kind != yaml.MappingNode {
		return fmt.Errorf("a service interface is a YAML mapping that begins with %q and %q, got %s",
			"apiVersion: "+APIVersion, "kind: "+KindServiceInterface, DescribeNode(body))
	}
	for i := 0; i+1 < len(body.Content); i += 2 {
		if key := body.Content[i]; !plainTopLevelKey(key) {
			return validationError(topLevelKeyName(key), fmt.Errorf(
				"a top-level key must be a plain string, not %s: write each key out, with no merge key (<<), alias, anchor or tag",
				describeTopLevelKey(key)))
		}
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
		return validationError("apiVersion", errors.New(serviceInterfaceHeaderMissing()))
	case apiVersion == nil:
		return validationError("apiVersion", fmt.Errorf("apiVersion is required: add the line %q above %q",
			"apiVersion: "+APIVersion, "kind: "+KindServiceInterface))
	case kind == nil:
		return validationError("kind", fmt.Errorf("kind is required: add the line %q below %q",
			"kind: "+KindServiceInterface, "apiVersion: "+APIVersion))
	case !stringScalarNode(apiVersion):
		return validationError("apiVersion", fmt.Errorf("apiVersion must be the string %s, got %s", APIVersion, DescribeNode(apiVersion)))
	case apiVersion.Value != APIVersion:
		return validationError("apiVersion", fmt.Errorf("apiVersion %q is not supported: this release reads apiVersion: %s",
			apiVersion.Value, APIVersion))
	case !stringScalarNode(kind):
		return validationError("kind", fmt.Errorf("kind must be the string %s, got %s", KindServiceInterface, DescribeNode(kind)))
	case kind.Value == KindActivityInterface:
		return validationError("kind", fmt.Errorf("kind: %s is an activity interface, not a service interface: "+
			"an activity image declares its with: in it; a service image says kind: %s", KindActivityInterface, KindServiceInterface))
	case kind.Value == KindOrkflow, kind.Value == KindAutomation, kind.Value == KindEventSource:
		return validationError("kind", fmt.Errorf("kind: %s is not a service interface: a service interface says kind: %s",
			kind.Value, KindServiceInterface))
	case kind.Value != KindServiceInterface:
		return validationError("kind", fmt.Errorf("kind %q is not a service interface: a service interface says kind: %s",
			kind.Value, KindServiceInterface))
	}
	return nil
}
