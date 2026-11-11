package dsl

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const describedDocument = `apiVersion: orkstack/v1
kind: ActivityInterface
description: >
  Copies files,
  one at a time.
icon: |
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" stroke="currentColor">
    <path d="M4 7h8"/>
    <rect x="2" y="2" width="12" height="12" fill="none"/>
  </svg>
fields:
  verbose:
    type: boolean
    description: Prints each file.
ops:
  copy:
    description: Copies one file.
    fields:
      src:
        type: string
        required: true
        description: >-
          The file to copy, under /orkstack/input.
  move:
    fields:
      dst:
        type: string
        description: Where the file goes.
  check: {}
defaultOp: copy
values:
  path:
    type: string
    description: The copy.
  size:
    type: number
`

func TestParseActivityDocument_ReadsTheIconAndEveryDescription(t *testing.T) {
	decl, err := ParseActivityDocument([]byte(describedDocument))
	must.OK(t, err)
	wantIcon := &orkstackv1.ActivityIcon{Attributes: attrs("stroke", "currentColor"), Elements: []*orkstackv1.IconElement{
		{Name: "path", Attributes: attrs("d", "M4 7h8")},
		{Name: "rect", Attributes: attrs("x", "2", "y", "2", "width", "12", "height", "12", "fill", "none")}}}
	if !proto.Equal(decl.Icon, wantIcon) {
		t.Fatalf("icon = %v, want %v", decl.Icon, wantIcon)
	}
	// Every text is trimmed: a folded scalar's final line break is not part
	// of it; an op or key without a description is absent from the docs,
	// and an op with no description of its own keeps its keys'.
	wantDocs := &orkstackv1.ActivityDocs{SchemaVersion: 1, Description: "Copies files, one at a time.",
		Fields: map[string]string{"verbose": "Prints each file."},
		Ops: map[string]*orkstackv1.OpDocs{
			"copy": {Description: "Copies one file.", Fields: map[string]string{"src": "The file to copy, under /orkstack/input."}},
			"move": {Fields: map[string]string{"dst": "Where the file goes."}}},
		Values: map[string]string{"path": "The copy."}}
	if !proto.Equal(decl.Docs, wantDocs) {
		t.Fatalf("docs = %v, want %v", decl.Docs, wantDocs)
	}
	iface, err := ParseInterfaceDocument([]byte(describedDocument))
	must.OK(t, err)
	if !proto.Equal(iface, decl.Interface) {
		t.Fatalf("ParseInterfaceDocument = %v, want ParseActivityDocument's interface %v", iface, decl.Interface)
	}
	if got := iface.GetFields()["verbose"]; !proto.Equal(got, &orkstackv1.FieldSchema{Type: "boolean"}) {
		t.Fatalf("fields.verbose = %v: a description never enters the interface", got)
	}
}

func TestParseActivityDocument_DeclaresNoIconOrDocsWithoutThem(t *testing.T) {
	decl, err := ParseActivityDocument([]byte("apiVersion: orkstack/v1\nkind: ActivityInterface\nfields:\n  a:\n    type: string\n"))
	must.OK(t, err)
	if decl.Icon != nil || decl.Docs != nil {
		t.Fatalf("icon = %v, docs = %v; want neither", decl.Icon, decl.Docs)
	}
}

func TestParseActivityDocument_RefusesAtTheDocumentsOwnPath(t *testing.T) {
	const head = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	long := strings.Repeat("a", MaxDescriptionChars+1)
	for _, tc := range []struct{ name, body, field, want string }{
		{"a description that is a boolean", "description: true\n", "description", "description must be a string, got"},
		{"a description that is a number", "fields:\n  a:\n    type: string\n    description: 12\n",
			"fields.a.description", "fields.a.description must be a string"},
		{"a null description", "ops:\n  x:\n    description:\n", "ops.x.description", "ops.x.description must be a string"},
		{"a description that is a mapping", "values:\n  a:\n    type: string\n    description:\n      text: x\n",
			"values.a.description", "values.a.description must be a string"},
		{"a long key description", "fields:\n  a:\n    type: string\n    description: " + long + "\n",
			"fields.a.description", "fields.a.description is 501 characters, limit is 500"},
		{"a blank op description", "ops:\n  x:\n    description: \"  \"\n", "ops.x.description", "ops.x.description is empty"},
		{"an empty description", "fields:\n  a:\n    type: string\n    description: \"\"\n", "fields.a.description",
			"fields.a.description is empty"},
		{"a tab in a description", "description: \"a\\tb\"\n", "description", "control character U+0009"},
		{"a literal block of two lines", "description: |\n  one\n  two\n", "description", "control character U+000A"},
		{"an icon that is a mapping", "icon:\n  paths: []\n", "icon", "icon must be a string"},
		{"an empty icon", "icon: \"\"\n", "icon", "icon is empty"},
		{"a blank icon", "icon: |\n  \n", "icon", "icon is empty"},
		{"an icon that is markup but not SVG", "icon: <b>x</b>\n", "icon", `icon: has the root element "b"`},
		{"an icon with a script", "icon: '<svg viewBox=\"0 0 16 16\"><script/></svg>'\n", "icon",
			`icon: holds the element "script"`},
		{"an icon colour", "icon: '<svg viewBox=\"0 0 16 16\" fill=\"red\"/>'\n", "icon", `icon: svg fill is "red"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decl, err := ParseActivityDocument([]byte(head + tc.body))
			if err == nil {
				t.Fatalf("ParseActivityDocument accepted %q as %+v", tc.body, decl)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseActivityDocument = %v, want an error containing %q", err, tc.want)
			}
			var located *ValidationError
			if tc.field != "" && (!errors.As(err, &located) || located.Field != tc.field) {
				t.Fatalf("the error %v is not located at %s", err, tc.field)
			}
		})
	}
}

// interfaceDocuments are every interface document this repository ships,
// keyed by its path from the repository root.
func interfaceDocuments(t *testing.T) map[string][]byte {
	t.Helper()
	var paths []string
	for _, parent := range []string{"images", "examples"} {
		matches, err := filepath.Glob(filepath.Join("..", "..", parent, "*", "interface.ork.yaml"))
		must.OK(t, err)
		paths = append(paths, matches...)
	}
	if len(paths) < 8 {
		t.Fatalf("found %d interface documents, want the six first-party images' and the examples'", len(paths))
	}
	out := map[string][]byte{}
	for _, path := range paths {
		source, err := os.ReadFile(path)
		must.OK(t, err)
		out[filepath.ToSlash(strings.TrimPrefix(path, filepath.Join("..", "..")+string(filepath.Separator)))] = source
	}
	return out
}

// stripDescribed removes the icon: and every description: from a document,
// at the YAML node level and by path — the top level, each shared, op and
// value key, each op — leaving the declaration as an image that predates
// them would write it. A with: key that happens to be named description is
// kept.
func stripDescribed(t *testing.T, source []byte) []byte {
	t.Helper()
	var root yaml.Node
	if err := yaml.Unmarshal(source, &root); err != nil {
		t.Fatal(err)
	}
	drop := func(node *yaml.Node, keys ...string) {
		if node == nil || node.Kind != yaml.MappingNode {
			return
		}
		kept := node.Content[:0]
		for i := 0; i+1 < len(node.Content); i += 2 {
			if !slices.Contains(keys, node.Content[i].Value) {
				kept = append(kept, node.Content[i], node.Content[i+1])
			}
		}
		node.Content = kept
	}
	value := func(node *yaml.Node, key string) *yaml.Node {
		for i := 0; node != nil && node.Kind == yaml.MappingNode && i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == key {
				return node.Content[i+1]
			}
		}
		return nil
	}
	each := func(node *yaml.Node, f func(*yaml.Node)) {
		for i := 1; node != nil && node.Kind == yaml.MappingNode && i < len(node.Content); i += 2 {
			f(node.Content[i])
		}
	}
	doc := root.Content[0]
	drop(doc, "description", "icon")
	each(value(doc, "fields"), func(field *yaml.Node) { drop(field, "description") })
	each(value(doc, "values"), func(field *yaml.Node) { drop(field, "description") })
	each(value(doc, "ops"), func(op *yaml.Node) {
		drop(op, "description")
		each(value(op, "fields"), func(field *yaml.Node) { drop(field, "description") })
	})
	out, err := yaml.Marshal(&root)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestInterfaceLabel_IsUnchangedByAnIconOrDescriptions: what a document
// packs into the interface label is the same bytes whether or not it
// declares an icon or descriptions, so a new image's interface reads on an
// engine that predates them exactly as before (docs/ACTIVITY_IMAGES.md,
// "Older engines").
func TestInterfaceLabel_IsUnchangedByAnIconOrDescriptions(t *testing.T) {
	for path, source := range interfaceDocuments(t) {
		t.Run(path, func(t *testing.T) {
			described, err := ParseActivityDocument(source)
			must.OK(t, err)
			if strings.HasPrefix(path, "images/") && (described.Icon == nil || described.Docs == nil) {
				t.Fatalf("%s declares no icon or no descriptions; every first-party image declares both", path)
			}
			bare, err := ParseActivityDocument(stripDescribed(t, source))
			must.OK(t, err)
			if bare.Icon != nil || bare.Docs != nil {
				t.Fatal("stripping left an icon or a description behind")
			}
			got, err := PackInterfaceLabel(described.Interface)
			must.OK(t, err)
			want, err := PackInterfaceLabel(bare.Interface)
			must.OK(t, err)
			if got != want {
				t.Fatalf("the interface label changes with descriptions:\n%s\nwithout them:\n%s", got, want)
			}
		})
	}
}

// TestFirstPartyImages_DescribeEveryOpAndKey: a first-party image says what
// it does, what each op does, and what every with: and values.json key it
// declares means, so the editor never meets an option it cannot explain.
func TestFirstPartyImages_DescribeEveryOpAndKey(t *testing.T) {
	for path, source := range interfaceDocuments(t) {
		if !strings.HasPrefix(path, "images/") {
			continue
		}
		decl, err := ParseActivityDocument(source)
		must.OKf(t, err, "%s", path)
		iface, docs := decl.Interface, decl.Docs
		var missing []string
		if docs.GetDescription() == "" {
			missing = append(missing, "description")
		}
		for key := range iface.GetFields() {
			if docs.GetFields()[key] == "" {
				missing = append(missing, "fields."+key)
			}
		}
		for op, schema := range iface.GetOps() {
			if docs.GetOps()[op].GetDescription() == "" {
				missing = append(missing, "ops."+op)
			}
			for key := range schema.GetFields() {
				if docs.GetOps()[op].GetFields()[key] == "" {
					missing = append(missing, "ops."+op+".fields."+key)
				}
			}
		}
		for key := range iface.GetValues() {
			if docs.GetValues()[key] == "" {
				missing = append(missing, "values."+key)
			}
		}
		if len(missing) > 0 {
			t.Errorf("%s describes none of: %s", path, strings.Join(missing, ", "))
		}
	}
}

// TestFirstPartyImages_IconSourceIsAlreadyCanonical: each first-party
// icon's source in its interface.ork.yaml is already the canonical form, so
// the label the image carries is those bytes, unchanged.
func TestFirstPartyImages_IconSourceIsAlreadyCanonical(t *testing.T) {
	for path, source := range interfaceDocuments(t) {
		if !strings.HasPrefix(path, "images/") {
			continue
		}
		var raw struct {
			Icon string `yaml:"icon"`
		}
		if err := yaml.Unmarshal(source, &raw); err != nil {
			t.Fatal(err)
		}
		decl, err := ParseActivityDocument(source)
		must.OKf(t, err, "%s", path)
		label, err := PackIconLabel(decl.Icon)
		must.OKf(t, err, "%s", path)
		if label == "" || label != strings.TrimSpace(raw.Icon) {
			t.Errorf("%s: the icon packs to\n%s\nnot its source\n%s", path, label, raw.Icon)
		}
	}
}

// iconPackingTo is a canonical icon of exactly n bytes, and the same icon
// written without xmlns, which is 43 bytes shorter as source.
func iconPackingTo(t *testing.T, n int) (canonical, source string) {
	t.Helper()
	of := func(d string) string {
		return CanonicalIconSVG(&orkstackv1.ActivityIcon{Elements: []*orkstackv1.IconElement{
			{Name: "path", Attributes: attrs("d", d)}}})
	}
	canonical = of("M0 0" + strings.Repeat(" ", n-len(of("M0 0"))))
	if len(canonical) != n {
		t.Fatalf("built %d bytes, want %d", len(canonical), n)
	}
	return canonical, strings.Replace(canonical, ` xmlns="http://www.w3.org/2000/svg"`, "", 1)
}

// TestParseIconSVG_TheLabelBoundIsExact: an icon of exactly 2048 bytes is
// read, one of 2049 is not.
func TestParseIconSVG_TheLabelBoundIsExact(t *testing.T) {
	at, _ := iconPackingTo(t, MaxIconLabelBytes)
	past, _ := iconPackingTo(t, MaxIconLabelBytes+1)
	if _, err := ParseIconSVG(at); err != nil {
		t.Fatalf("an icon of exactly %d bytes: %v", MaxIconLabelBytes, err)
	}
	if _, err := ParseIconSVG(past); err == nil {
		t.Fatalf("an icon of %d bytes was read", MaxIconLabelBytes+1)
	}
}

// TestParseActivityDocument_HoldsTheIconToTheBoundOfWhatItPacksTo: the label
// holds the canonical form, which can be longer than the source; a document
// is refused where the author wrote it when that form passes the bound,
// even though the source alone is under it, and accepted at exactly it.
func TestParseActivityDocument_HoldsTheIconToTheBoundOfWhatItPacksTo(t *testing.T) {
	document := func(source string) []byte {
		return []byte("apiVersion: orkstack/v1\nkind: ActivityInterface\nicon: '" + source + "'\n")
	}
	_, atSource := iconPackingTo(t, MaxIconLabelBytes)
	decl, err := ParseActivityDocument(document(atSource))
	must.OKf(t, err, "an icon packing to exactly %d bytes", MaxIconLabelBytes)
	if label, err := PackIconLabel(decl.Icon); err != nil || len(label) != MaxIconLabelBytes {
		t.Fatalf("it packs to %d bytes (%v)", len(label), err)
	}
	_, pastSource := iconPackingTo(t, MaxIconLabelBytes+1)
	if _, err := ParseIconSVG(pastSource); err != nil {
		t.Fatalf("the source alone, %d bytes, should read: %v", len(pastSource), err)
	}
	_, err = ParseActivityDocument(document(pastSource))
	var located *ValidationError
	if err == nil || !errors.As(err, &located) || located.Field != "icon" || !strings.Contains(err.Error(), "2049") {
		t.Fatalf("a %d-byte source packing to %d bytes = %v, want a refusal at icon naming the size",
			len(pastSource), MaxIconLabelBytes+1, err)
	}
}

// TestStripDescribed_KeepsAKeyNamedDescription: the stripper the
// compatibility test relies on removes descriptions by place, not by name.
func TestStripDescribed_KeepsAKeyNamedDescription(t *testing.T) {
	source := []byte("apiVersion: orkstack/v1\nkind: ActivityInterface\ndescription: x\nfields:\n  description:\n    type: string\n    description: y\n")
	decl, err := ParseActivityDocument(stripDescribed(t, source))
	must.OK(t, err)
	if decl.Docs != nil || decl.Interface.GetFields()["description"] == nil {
		t.Fatalf("stripped to docs %v and fields %v; want no docs and the key named description kept", decl.Docs, decl.Interface.GetFields())
	}
}
