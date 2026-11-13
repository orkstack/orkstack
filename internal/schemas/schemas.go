// Package schemas generates the JSON Schemas of OrkStack's authored
// documents (docs/schemas/): orkflow.v1.json, automation.v1.json,
// activityinterface.v1.json, and the dispatcher orkstack.v1.json, which
// selects one by kind. They are generated,
// never hand-written: `go run ./internal/schemas/cmd/genschemas` writes them,
// and TestSchemas_AreGenerated refuses a stale one.
//
// The generator also writes the Orkflow and Automation schemas into the UI
// (ui/src/lib/schemas/), byte for byte: the UI's editor validates, completes
// and explains from them, and the UI is built from ui/ alone
// (deploy/Dockerfile). Not under ui/src/lib/gen, which `make proto` empties
// (buf.gen.yaml, clean: true).
package schemas

import (
	"bytes"
	"encoding/json/jsontext"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/automation"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// Dir is where the schemas live, relative to the repository root.
const Dir = "docs/schemas"

// UIDir is the UI's generated copy of UIFiles, relative to the repository root.
const UIDir = "ui/src/lib/schemas"

// The files, under Dir.
const (
	OrkflowFile           = "orkflow.v1.json"
	AutomationFile        = "automation.v1.json"
	ActivityInterfaceFile = "activityinterface.v1.json"
	DispatcherFile        = "orkstack.v1.json"
)

// UIFiles are the schemas the UI's editor reads: one per kind it edits
// (Orkflow and Automation; an activity interface is authored beside an
// image, never in the UI). The dispatcher is for editors that do not know a
// document's kind; the UI always does.
var UIFiles = []string{OrkflowFile, AutomationFile}

// InterfaceDocument is the file an image directory declares its with:
// contract in (docs/ACTIVITY_IMAGES.md, "Declaring the interface").
const InterfaceDocument = "interface.ork.yaml"

// MaxDockerfileLine is the longest Dockerfile line BuildKit reads. A label
// near MaxInterfaceLabelBytes fits a --label argument, not a LABEL line.
const MaxDockerfileLine = 65535

// activityLabel is one label an interface document packs into, and its
// line in a Dockerfile.
type activityLabel struct {
	name string
	line *regexp.Regexp
}

func newActivityLabel(name string) activityLabel {
	return activityLabel{name, regexp.MustCompile(`(?m)^LABEL ` + regexp.QuoteMeta(name) + `='([^']*)'$`)}
}

// activityLabels are the labels in the order packAll returns their values.
var activityLabels = []activityLabel{
	newActivityLabel(dsl.InterfaceLabel), newActivityLabel(dsl.IconLabel), newActivityLabel(dsl.DocsLabel),
}

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
// and examples/ that holds an interface.ork.yaml, with its three activity
// LABEL lines — interface, icon and docs — each replaced by the line its
// packer writes for that document (an icon or docs the document does not
// declare is the empty value, which clears a label a base image set) — keyed
// by path from the repository root. Every other line is the Dockerfile's own.
// A Dockerfile without exactly one line per label is refused: the lines are
// where the author put them, and the generator never guesses.
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
		source, err := os.ReadFile(document)
		if err != nil {
			return nil, fmt.Errorf("schemas: read %s: %w", document, err)
		}
		decl, err := dsl.ParseActivityDocument(source)
		if err != nil {
			return nil, fmt.Errorf("schemas: %s: %w", document, err)
		}
		values, err := packAll(decl)
		if err != nil {
			return nil, fmt.Errorf("schemas: %s: %w", document, err)
		}
		path := filepath.Join(dir, "Dockerfile")
		generated, err := os.ReadFile(path) // the Dockerfile, each label's line replaced in turn
		if err != nil {
			return nil, fmt.Errorf("schemas: read %s: %w", path, err)
		}
		for i, entry := range activityLabels {
			label := entry.name
			lines := entry.line.FindAllIndex(generated, -1)
			if len(lines) != 1 {
				return nil, fmt.Errorf("schemas: %s carries %d %s LABEL lines, want exactly one for %s to replace: "+
					"one line, LABEL %s='…', single-quoted and unbroken ('' when the document declares none)",
					path, len(lines), label, InterfaceDocument, label)
			}
			line := "LABEL " + label + "='" + values[i] + "'"
			if len(line) > MaxDockerfileLine {
				return nil, fmt.Errorf("schemas: %s: the %s LABEL line would be %d bytes, and BuildKit refuses a Dockerfile "+
					"line over %d; build the image with orkctl activity build --interface instead", path, label, len(line), MaxDockerfileLine)
			}
			generated = append(append(append([]byte{}, generated[:lines[0][0]]...), line...), generated[lines[0][1]:]...)
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return nil, err
		}
		out[filepath.ToSlash(rel)] = generated
	}
	return out, nil
}

// packAll is the value of each of activityLabels for one document, in order.
func packAll(decl *dsl.ActivityDeclaration) ([]string, error) {
	iface, err := dsl.PackInterfaceLabel(decl.Interface)
	if err != nil {
		return nil, err
	}
	icon, err := dsl.PackIconLabel(decl.Icon)
	if err != nil {
		return nil, err
	}
	docs, err := dsl.PackDocsLabel(decl.Docs, decl.Interface)
	if err != nil {
		return nil, err
	}
	return []string{iface, icon, docs}, nil
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
	if err := stablejson.Encode(&buf, schema, stablejson.Unescaped, jsontext.WithIndent("  ")); err != nil {
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
	events, err := webhookEvents()
	if err != nil {
		return nil, err
	}
	out[WebhookEventsFile] = events
	return out, nil
}

// WebhookEventsFile is the UI's copy of the webhook event lists, relative to
// the repository root. ui/test/webhookEvents.test.ts holds the UI's own lists
// to it, so an event added to pkg/webhooks cannot be missing from the Admin page.
const WebhookEventsFile = "ui/src/lib/webhook-events.json"

// webhookEvents renders {known, run, task} from pkg/webhooks.
func webhookEvents() ([]byte, error) {
	names := func(events []webhooks.Event) []string {
		out := make([]string, len(events))
		for i, event := range events {
			out[i] = string(event)
		}
		return out
	}
	var buf bytes.Buffer
	err := stablejson.Encode(&buf, struct {
		Known []string `json:"known"`
		Run   []string `json:"run"`
		Task  []string `json:"task"`
	}{names(webhooks.KnownEvents()), names(webhooks.RunEvents()), names(webhooks.TaskEvents())},
		stablejson.Unescaped, jsontext.WithIndent("  "))
	if err != nil {
		return nil, fmt.Errorf("schemas: encode webhook events: %w", err)
	}
	return buf.Bytes(), nil
}
