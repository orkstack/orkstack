// Package schemas generates the JSON Schemas of OrkStack's authored
// documents (docs/schemas/): orkflow.v1.json, automation.v1.json,
// activityinterface.v1.json, and the dispatcher orkstack.v1.json, which
// selects one by kind. They are generated,
// never hand-written: `go run ./internal/schemas/cmd/genschemas` writes them,
// and TestSchemas_AreGenerated refuses a stale one.
package schemas

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/dsl"
)

// Dir is where the schemas live, relative to the repository root.
const Dir = "docs/schemas"

// The files, under Dir.
const (
	OrkflowFile           = "orkflow.v1.json"
	AutomationFile        = "automation.v1.json"
	ActivityInterfaceFile = "activityinterface.v1.json"
	DispatcherFile        = "orkstack.v1.json"
)

// InterfaceDocument is the file an image directory declares its with:
// contract in (docs/ACTIVITY_IMAGES.md, "Declaring the interface").
const InterfaceDocument = "interface.ork.yaml"

// labelLine is the interface label's line in a Dockerfile.
var labelLine = regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(dsl.InterfaceLabel) + `='([^']*)'$`)

// readInterface parses one image directory's interface document.
func readInterface(dir string) (*orkstackv1.ActivityInterface, error) {
	path := filepath.Join(dir, InterfaceDocument)
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("schemas: read %s: %w", path, err)
	}
	iface, err := dsl.ParseInterfaceDocument(source)
	if err != nil {
		return nil, fmt.Errorf("schemas: %s: %w", path, err)
	}
	return iface, nil
}

// FirstPartyInterfaces reads each builtin catalog image's interface from
// images/activity-<name>/interface.ork.yaml, the source of its label.
func FirstPartyInterfaces(repoRoot string) (map[string]*orkstackv1.ActivityInterface, error) {
	out := map[string]*orkstackv1.ActivityInterface{}
	for _, name := range dsl.BuiltinActivityCatalog().Names() {
		iface, err := readInterface(filepath.Join(repoRoot, "images", "activity-"+name))
		if err != nil {
			return nil, err
		}
		out[name] = iface
	}
	return out, nil
}

// Dockerfiles returns the Dockerfile of every image directory under images/
// and examples/ that holds an interface.ork.yaml, with its one interface
// LABEL line replaced by the line PackInterfaceLabel writes for that
// document — keyed by path from the repository root. Every other line is the
// Dockerfile's own. A Dockerfile without exactly one such line is refused:
// the line is where the author put it, and the generator never guesses.
func Dockerfiles(repoRoot string) (map[string][]byte, error) {
	var documents []string
	for _, parent := range []string{"images", "examples"} {
		matches, err := filepath.Glob(filepath.Join(repoRoot, parent, "*", InterfaceDocument))
		if err != nil {
			return nil, err
		}
		documents = append(documents, matches...)
	}
	out := map[string][]byte{}
	for _, document := range documents {
		dir := filepath.Dir(document)
		iface, err := readInterface(dir)
		if err != nil {
			return nil, err
		}
		label, err := dsl.PackInterfaceLabel(iface)
		if err != nil {
			return nil, fmt.Errorf("schemas: %s: %w", document, err)
		}
		path := filepath.Join(dir, "Dockerfile")
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("schemas: read %s: %w", path, err)
		}
		lines := labelLine.FindAllIndex(raw, -1)
		if len(lines) != 1 {
			return nil, fmt.Errorf("schemas: %s carries %d interface LABEL lines, want exactly one for %s to replace",
				path, len(lines), InterfaceDocument)
		}
		line := "LABEL " + dsl.InterfaceLabel + "='" + label + "'"
		generated := append(append(append([]byte{}, raw[:lines[0][0]]...), line...), raw[lines[0][1]:]...)
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return nil, err
		}
		out[filepath.ToSlash(rel)] = generated
	}
	return out, nil
}

// Trees are the three schemas as JSON trees, keyed by file name.
func Trees(repoRoot string) (map[string]map[string]any, error) {
	firstParty, err := FirstPartyInterfaces(repoRoot)
	if err != nil {
		return nil, err
	}
	orkflow, err := dsl.OrkflowSchema(firstParty)
	if err != nil {
		return nil, err
	}
	auto, err := automation.Schema()
	if err != nil {
		return nil, err
	}
	iface, err := dsl.ActivityInterfaceSchema()
	if err != nil {
		return nil, err
	}
	return map[string]map[string]any{
		OrkflowFile:           orkflow,
		AutomationFile:        auto,
		ActivityInterfaceFile: iface,
		DispatcherFile:        dispatcher(orkflow, auto, iface),
	}, nil
}

// body is a schema's root without its file-level keys.
func body(schema map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range schema {
		switch k {
		case "$schema", "$comment", "definitions":
			continue
		}
		out[k] = v
	}
	return out
}

// dispatcher holds every kind's definitions in one flat table and selects
// the kind's root by kind, so one *.ork.yaml editor mapping serves them all.
func dispatcher(orkflow, auto, iface map[string]any) map[string]any {
	definitions := map[string]any{"orkflow": body(orkflow), "automation": body(auto), "activityinterface": body(iface)}
	for _, schema := range []map[string]any{orkflow, auto, iface} {
		for name, def := range schema["definitions"].(map[string]any) {
			definitions[name] = def
		}
	}
	selects := func(kind, def string) map[string]any {
		return map[string]any{
			"if":   map[string]any{"required": []any{"kind"}, "properties": map[string]any{"kind": map[string]any{"const": kind}}},
			"then": map[string]any{"$ref": "#/definitions/" + def},
		}
	}
	return map[string]any{
		"$schema":  dsl.SchemaDialect,
		"$comment": dsl.SchemaComment,
		"title":    "OrkStack authored document (" + dsl.APIVersion + ")",
		"description": "An Orkflow (kind: Orkflow, docs/DSL.md), an automation document (kind: Automation, docs/AUTOMATION.md) " +
			"or an activity interface (kind: ActivityInterface, docs/ACTIVITY_IMAGES.md); apiVersion and kind select the schema.",
		"type":     "object",
		"required": []any{"apiVersion", "kind"},
		"properties": map[string]any{
			"apiVersion": map[string]any{"const": dsl.APIVersion},
			"kind":       map[string]any{"enum": []any{dsl.KindActivityInterface, dsl.KindAutomation, dsl.KindOrkflow}},
		},
		"allOf": []any{selects(dsl.KindOrkflow, "orkflow"), selects(dsl.KindAutomation, "automation"),
			selects(dsl.KindActivityInterface, "activityinterface")},
		"definitions": definitions,
	}
}

// Encode is a schema's file bytes: two-space indent, keys sorted, no HTML
// escaping, a trailing newline. Deterministic.
func Encode(schema map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(schema); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Files returns every generated schema, keyed by its path under docs/schemas/.
func Files(repoRoot string) (map[string][]byte, error) {
	trees, err := Trees(repoRoot)
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for name, tree := range trees {
		encoded, err := Encode(tree)
		if err != nil {
			return nil, fmt.Errorf("schemas: encode %s: %w", name, err)
		}
		out[strings.TrimPrefix(filepath.ToSlash(filepath.Join(Dir, name)), "./")] = encoded
	}
	return out, nil
}
