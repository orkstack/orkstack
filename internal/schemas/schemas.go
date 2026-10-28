// Package schemas generates the JSON Schemas of OrkStack's authored
// documents (docs/schemas/): orkflow.v1.json, automation.v1.json, and the
// dispatcher orkstack.v1.json, which selects one by kind. They are generated,
// never hand-written: `go run ./internal/schemas/cmd/genschemas` writes them,
// and TestSchemas_AreGenerated refuses a stale one.
//
// The generator also writes the two kinds' schemas into the UI
// (ui/src/lib/schemas/), byte for byte: the UI's editor validates, completes
// and explains from them, and the UI is built from ui/ alone
// (deploy/Dockerfile). Not under ui/src/lib/gen, which `make proto` empties
// (buf.gen.yaml, clean: true).
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

// UIDir is the UI's generated copy of UIFiles, relative to the repository root.
const UIDir = "ui/src/lib/schemas"

// The files, under Dir.
const (
	OrkflowFile    = "orkflow.v1.json"
	AutomationFile = "automation.v1.json"
	DispatcherFile = "orkstack.v1.json"
)

// UIFiles are the schemas the UI's editor reads: one per kind. The dispatcher
// is for editors that do not know a document's kind; the UI always does.
var UIFiles = []string{OrkflowFile, AutomationFile}

// labelLine is the interface label's line in a first-party Dockerfile.
var labelLine = regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(dsl.InterfaceLabel) + `='([^']*)'$`)

// FirstPartyInterfaces reads each builtin catalog image's interface label
// from images/activity-<name>/Dockerfile.
func FirstPartyInterfaces(repoRoot string) (map[string]*orkstackv1.ActivityInterface, error) {
	out := map[string]*orkstackv1.ActivityInterface{}
	for _, name := range dsl.BuiltinActivityCatalog().Names() {
		path := filepath.Join(repoRoot, "images", "activity-"+name, "Dockerfile")
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("schemas: read %s: %w", path, err)
		}
		match := labelLine.FindSubmatch(raw)
		if match == nil {
			return nil, fmt.Errorf("schemas: %s carries no %s label", path, dsl.InterfaceLabel)
		}
		iface, err := dsl.ParseInterfaceLabel(string(match[1]))
		if err != nil {
			return nil, fmt.Errorf("schemas: %s: %w", path, err)
		}
		out[name] = iface
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
	return map[string]map[string]any{
		OrkflowFile:    orkflow,
		AutomationFile: auto,
		DispatcherFile: dispatcher(orkflow, auto),
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

// dispatcher holds both kinds' definitions in one flat table and selects the
// kind's root by kind, so one *.ork.yaml editor mapping serves both.
func dispatcher(orkflow, auto map[string]any) map[string]any {
	definitions := map[string]any{"orkflow": body(orkflow), "automation": body(auto)}
	for _, schema := range []map[string]any{orkflow, auto} {
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
		"$schema":     dsl.SchemaDialect,
		"$comment":    dsl.SchemaComment,
		"title":       "OrkStack authored document (" + dsl.APIVersion + ")",
		"description": "An Orkflow (kind: Orkflow, docs/DSL.md) or an automation document (kind: Automation, docs/AUTOMATION.md); apiVersion and kind select the schema.",
		"type":        "object",
		"required":    []any{"apiVersion", "kind"},
		"properties": map[string]any{
			"apiVersion": map[string]any{"const": dsl.APIVersion},
			"kind":       map[string]any{"enum": []any{dsl.KindAutomation, dsl.KindOrkflow}},
		},
		"allOf":       []any{selects(dsl.KindOrkflow, "orkflow"), selects(dsl.KindAutomation, "automation")},
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

// Files returns every generated file, keyed by its path from the repository
// root: each schema under Dir, and UIFiles again under UIDir.
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
	for _, name := range UIFiles {
		out[UIDir+"/"+name] = out[Dir+"/"+name]
	}
	return out, nil
}
