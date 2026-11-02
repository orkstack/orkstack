package dsl_test

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// iconPathClass is every byte an accepted icon path may hold: command
// letters, number characters and SVG separators. It is the safety property —
// nothing in it can form markup, a URL, an entity or CSS — and the oracle is
// written from it, not from the scanner.
var iconPathClass = regexp.MustCompile(`^[MmZzLlHhVvCcSsQqTtAa0-9eE.,+\- \t\n\f\r]+$`)

// FuzzCheckIconPathData: a d the scanner accepts holds only the bytes of
// iconPathClass, and SVG whitespace around it changes nothing.
func FuzzCheckIconPathData(f *testing.F) {
	for _, seed := range []string{
		"M4 7h16", "M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z", "M3 3a1 1 0 0118 0", "M1-2-3-4", "M.5.5.5.5",
		"M1e2 1E-2l+3.5-.5", "M2 2q3 3 6 0t6 0Q1 1 2 2T4 4", "M2 2H10V10h-8v-8Z M4 4z",
		// SVGO-minified paths, as an author pastes them.
		"M12 2a10 10 0 100 20 10 10 0 000-20zm0 18a8 8 0 110-16 8 8 0 010 16z",
		"M9 4H8a2 2 0 00-2 2v4l-2 2 2 2v4a2 2 0 002 2h1",
		"", "1 2", "M1", "M1 2Z3", "M1 2,", "M,1 2", "M1 2\"/><script>", "M1 2 url(#x)", "M1 1e400", "M1 2a1 1 0 2 0 3 3",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, d string) {
		err := dsl.CheckIconPathData(d)
		if err == nil && !iconPathClass.MatchString(d) {
			t.Fatalf("CheckIconPathData accepted %q, which holds a byte outside the path class", d)
		}
		if err != nil && err.Error() == "" {
			t.Fatalf("CheckIconPathData refused %q without a reason", d)
		}
		if again := dsl.CheckIconPathData(" \t" + d + "\n "); (again == nil) != (err == nil) {
			t.Fatalf("surrounding whitespace changed the verdict on %q: %v, then %v", d, err, again)
		}
	})
}

// iconNames are the element names and attribute names an icon may hold,
// written from the allowlist's statement, not from the parser's tables.
var (
	iconElementNames   = map[string]bool{"path": true, "circle": true, "ellipse": true, "rect": true, "line": true, "polyline": true, "polygon": true, "g": true}
	iconAttributeNames = map[string]bool{
		"d": true, "cx": true, "cy": true, "r": true, "rx": true, "ry": true, "x": true, "y": true, "width": true,
		"height": true, "points": true, "x1": true, "y1": true, "x2": true, "y2": true, "transform": true,
		"fill": true, "stroke": true, "stroke-width": true, "stroke-linecap": true, "stroke-linejoin": true,
		"fill-rule": true, "clip-rule": true,
	}
	// iconNumberValue is a plain number; iconTransformValue a list of the six
	// transform functions of numbers.
	iconNumberValue    = regexp.MustCompile(`^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$`)
	iconTransformValue = regexp.MustCompile(`^([ \t\n\f\r,]*(matrix|translate|scale|rotate|skewX|skewY)[ \t\n\f\r]*\([0-9eE.,+\- \t\n\f\r]*\))+[ \t\n\f\r,]*$`)
	iconKeywords       = map[string]map[string]bool{
		"fill":            {"none": true, "currentColor": true},
		"stroke":          {"none": true, "currentColor": true},
		"stroke-linecap":  {"butt": true, "round": true, "square": true},
		"stroke-linejoin": {"miter": true, "round": true, "bevel": true},
		"fill-rule":       {"nonzero": true, "evenodd": true},
		"clip-rule":       {"nonzero": true, "evenodd": true},
	}
)

// iconGeometryOf is, for each geometry attribute, the elements it belongs
// to — the spec's table read the other way round.
var iconGeometryOf = func() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for element, names := range map[string]string{
		"path": "d", "circle": "cx cy r", "ellipse": "cx cy rx ry", "rect": "x y width height rx ry",
		"line": "x1 y1 x2 y2", "polyline": "points", "polygon": "points", "g": "transform",
	} {
		for _, name := range strings.Fields(names) {
			if out[name] == nil {
				out[name] = map[string]bool{}
			}
			out[name][element] = true
		}
	}
	return out
}()

// iconValueOK is the allowlist's statement of what each attribute's value
// may be, written apart from the parser: a keyword from its own set, path
// data in the path class, a transform list, or a plain number within
// +/-10000 — never a URL, a colour or anything else.
func iconValueOK(name, value string) bool {
	switch name {
	case "d":
		return iconPathClass.MatchString(value)
	case "points":
		return iconPathClass.MatchString(value) && !strings.ContainsAny(value, "MmZzLlHhVvCcSsQqTtAa")
	case "transform":
		return iconTransformValue.MatchString(value)
	}
	if words, keyword := iconKeywords[name]; keyword {
		return words[value]
	}
	if !iconNumberValue.MatchString(value) {
		return false
	}
	n, err := strconv.ParseFloat(value, 64)
	return err == nil && n >= -10000 && n <= 10000
}

// requireSafeReason is the property every refusal's reported reason holds:
// valid UTF-8, no control or format character, bounded.
func requireSafeReason(t *testing.T, err error) {
	t.Helper()
	reason := dsl.SafeReason(err.Error())
	if !utf8.ValidString(reason) || len(reason) > 512 {
		t.Fatalf("reason %q is not valid UTF-8 or too long", reason)
	}
	for _, r := range reason {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == 0x2028 || r == 0x2029 {
			t.Fatalf("reason %q holds %U", reason, r)
		}
	}
}

// FuzzParseIconLabel: an icon label is refused with a safe reason, or yields
// a tree of allowlisted names whose values hold no markup character, within
// the bounds, whose canonical label is one line with no single quote and
// reads back as the same tree.
func FuzzParseIconLabel(f *testing.F) {
	for _, seed := range []string{
		`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M8 1.8 13.5 5v6L8 14.2 2.5 11V5zM2.5 5 8 8.2 13.5 5M8 8.2v6" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>`,
		`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><circle cx="8" cy="8" r="6" fill="none" stroke="currentColor" stroke-width="1.4"/><ellipse cx="8" cy="8" rx="2.5" ry="6" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M2.2 8h11.6" stroke="currentColor" stroke-width="1.4"/></svg>`,
		`<?xml version="1.0"?><!-- c --><svg viewBox="0 0 16 16" width="16"><title>t</title><g transform="translate(1 1)"><rect x="1" y="1" width="2" height="2"/><polyline points="1,1 2,2"/></g></svg>`,
		`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><script>alert(1)</script></svg>`,
		`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M0 0" onload="x"/></svg>`,
		`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M0 0&quot;/&gt;"/></svg>`,
		`<!DOCTYPE svg [<!ENTITY x "y">]><svg viewBox="0 0 16 16"/>`,
		`<svg viewBox="0 0 16 16"><![CDATA[x]]></svg>`,
		`<svg viewBox="0 0 16 16"><g transform="translate(1"><path d="M0 0h1"/></g><g transform="scale 2)"/></svg>`,
		`<?xml-stylesheet href="a.css"?><svg viewBox="0 0 16 16"><path d="M0 0h1"/></svg>`,
		"\x1b]0;pwn\x07<svg/>", `{"schemaVersion":1}`, ``, `<svg/>`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		icon, err := dsl.ParseIconLabel(raw)
		if err != nil {
			if icon != nil || err.Error() == "" {
				t.Fatalf("ParseIconLabel refused %q as %v with %v", raw, err, icon)
			}
			requireSafeReason(t, err)
			return
		}
		if raw == "" {
			if icon != nil {
				t.Fatalf("an empty label declared %v", icon)
			}
			return
		}
		if len(raw) > dsl.MaxIconLabelBytes {
			t.Fatalf("ParseIconLabel accepted %d bytes", len(raw))
		}
		count := 0
		var walk func(elements []*orkstackv1.IconElement, depth int)
		checkAttributes := func(where string, attributes []*orkstackv1.IconAttribute) {
			for _, attr := range attributes {
				if !iconAttributeNames[attr.GetName()] || !iconValueOK(attr.GetName(), attr.GetValue()) {
					t.Fatalf("%s accepted %s=%q", where, attr.GetName(), attr.GetValue())
				}
			}
		}
		walk = func(elements []*orkstackv1.IconElement, depth int) {
			for _, element := range elements {
				count++
				if !iconElementNames[element.GetName()] || depth > dsl.MaxIconDepth ||
					(element.GetName() != "g" && len(element.GetChildren()) > 0) {
					t.Fatalf("ParseIconLabel accepted the element %q at depth %d in %q", element.GetName(), depth, raw)
				}
				checkAttributes(element.GetName(), element.GetAttributes())
				for _, attr := range element.GetAttributes() {
					if geometry, ok := iconGeometryOf[attr.GetName()]; ok && !geometry[element.GetName()] {
						t.Fatalf("ParseIconLabel accepted %s on %s in %q", attr.GetName(), element.GetName(), raw)
					}
				}
				walk(element.GetChildren(), depth+1)
			}
		}
		checkAttributes("svg", icon.GetAttributes())
		walk(icon.GetElements(), 0)
		if count > dsl.MaxIconElements {
			t.Fatalf("ParseIconLabel accepted %d elements", count)
		}
		label, err := dsl.PackIconLabel(icon)
		if err != nil {
			if strings.Contains(err.Error(), "limit is") {
				return // the canonical form may be longer than its source
			}
			t.Fatalf("PackIconLabel refused an accepted icon %v: %v", icon, err)
		}
		if strings.ContainsAny(label, "'\n") {
			t.Fatalf("PackIconLabel(%v) = %q", icon, label)
		}
		if back, err := dsl.ParseIconLabel(label); err != nil || !proto.Equal(back, icon) {
			t.Fatalf("the packed label %q reads back as %v (%v), not %v", label, back, err, icon)
		}
	})
}

// isPlainText is the docs label's text rule, written from its statement:
// valid UTF-8, not blank, at most limit characters, one paragraph with no
// control character, no format character (bidirectional controls,
// zero-width and invisible characters, tags), no noncharacter.
func isPlainText(text string, limit int) bool {
	if !utf8.ValidString(text) || strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > limit {
		return false
	}
	for _, r := range text {
		switch {
		case unicode.IsControl(r), r == 0x2028, r == 0x2029, unicode.Is(unicode.Cf, r),
			r >= 0xfdd0 && r <= 0xfdef, r&0xfffe == 0xfffe:
			return false
		}
	}
	return true
}

// FuzzParseDocsLabel: a docs label is refused with a reason, or yields docs
// within every bound, every key and op well formed, every text plain, whose
// packed label holds no single quote and reads back as the same docs; and
// docs describe an interface that declares nothing only when they describe
// no op, key or value.
func FuzzParseDocsLabel(f *testing.F) {
	esc := func(code string) string { return `\` + "u" + code }
	for _, seed := range []string{
		`{"schemaVersion":1,"description":"Copies files."}`,
		`{"schemaVersion":1,"description":"It` + esc("0027") + `s \"quoted\".","fields":{"a":"x"},"ops":{"copy":{"description":"y","fields":{"src":"z"}}},"values":{"path":"w"}}`,
		`{"schemaVersion":1,"description":"a` + esc("202e") + `b"}`,
		`{"schemaVersion":1,"fields":{"a":"a` + esc("2028") + `b"}}`,
		`{"schemaVersion":1,"fields":{"a":"one\ntwo"}}`,
		`{"schemaVersion":1,"fields":{"a":"\t"}}`,
		`{"schemaVersion":1,"fields":{"a b":"x"}}`,
		`{"schemaVersion":1,"ops":{"Copy":{}}}`,
		`{"schemaVersion":1,"values":{"a":""}}`,
		`{"schemaVersion":2}`, `{"schemaVersion":1,"title":"x"}`, ``, `{}`, `[]`,
	} {
		f.Add(seed)
	}
	keyPattern := regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	opPattern := regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	f.Fuzz(func(t *testing.T, raw string) {
		docs, err := dsl.ParseDocsLabel(raw)
		if err != nil {
			if docs != nil || err.Error() == "" {
				t.Fatalf("ParseDocsLabel refused %q as %v with %v", raw, err, docs)
			}
			requireSafeReason(t, err)
			return
		}
		if raw == "" {
			if docs != nil {
				t.Fatalf("an empty label declared %v", docs)
			}
			return
		}
		if len(raw) > dsl.MaxDocsLabelBytes || docs.GetSchemaVersion() != 1 || len(docs.GetOps()) > dsl.MaxInterfaceOps {
			t.Fatalf("ParseDocsLabel accepted %q beyond its bounds", raw)
		}
		text := func(where, s string, limit int) {
			if !isPlainText(s, limit) {
				t.Fatalf("ParseDocsLabel accepted %s = %q", where, s)
			}
		}
		texts := func(where string, m map[string]string) {
			if len(m) > dsl.MaxInterfaceFields {
				t.Fatalf("%s holds %d keys", where, len(m))
			}
			for key, s := range m {
				if !keyPattern.MatchString(key) {
					t.Fatalf("ParseDocsLabel accepted the key %q in %s", key, where)
				}
				text(where+"."+key, s, dsl.MaxDescriptionChars)
			}
		}
		if d := docs.GetDescription(); d != "" {
			text("description", d, dsl.MaxActivityDescriptionChars)
		}
		texts("fields", docs.GetFields())
		texts("values", docs.GetValues())
		for op, described := range docs.GetOps() {
			if !opPattern.MatchString(op) {
				t.Fatalf("ParseDocsLabel accepted the op %q", op)
			}
			if d := described.GetDescription(); d != "" {
				text("ops."+op+".description", d, dsl.MaxDescriptionChars)
			}
			texts("ops."+op+".fields", described.GetFields())
		}
		describesAny := len(docs.GetFields()) > 0 || len(docs.GetOps()) > 0 || len(docs.GetValues()) > 0
		empty := &orkstackv1.ActivityInterface{SchemaVersion: 1}
		if err := dsl.CheckDocsAgainstInterface(docs, empty); (err != nil) != describesAny {
			t.Fatalf("against an empty interface, docs %v checked as %v", docs, err)
		}
		if err := dsl.CheckDocsAgainstInterface(docs, nil); err != nil {
			t.Fatalf("docs with no interface to check them against were refused: %v", err)
		}
		label, err := dsl.PackDocsLabel(docs, nil)
		if err != nil {
			if strings.Contains(err.Error(), "limit is") {
				return // escaping a quote can lengthen a label near its bound
			}
			t.Fatalf("PackDocsLabel refused accepted docs %v: %v", docs, err)
		}
		if strings.Contains(label, "'") || len(label) > dsl.MaxDocsLabelBytes {
			t.Fatalf("PackDocsLabel(%v) = %q", docs, label)
		}
		if back, err := dsl.ParseDocsLabel(label); err != nil || !proto.Equal(back, docs) {
			t.Fatalf("the packed label %q reads back as %v (%v), not %v", label, back, err, docs)
		}
	})
}

// FuzzParseActivityDocument: an authored document is refused with a reason,
// or every label it packs into — interface, icon and docs — is deterministic,
// holds no single quote, is within its bound, and is read back by the engine
// as what the document declared: what pack accepts, the engine reads.
func FuzzParseActivityDocument(f *testing.F) {
	const header = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	for _, seed := range []string{
		header + "description: >\n  Copies `files`.\nicon: |\n  <svg viewBox=\"0 0 16 16\"><path d=\"M4 7h8\" stroke=\"currentColor\"/></svg>\n" +
			"fields:\n  a:\n    type: string\n    description: It's \"a\".\n",
		header + "ops:\n  copy:\n    description: Copies.\n    fields:\n      src:\n        type: string\n        description: |\n          The file.\n" +
			"defaultOp: copy\nvalues:\n  path:\n    type: string\n    description: The copy.\n",
		header + "icon: '<svg viewBox=\"0 0 16 16\"><g><g><path d=\"M0 0h1\"/></g></g></svg>'\n",
		header + "icon: <path/>\n",
		header + "icon: '<svg viewBox=\"0 0 16 16\"><script/></svg>'\n",
		header + "description: true\n",
		header + "description: \"  \"\n",
		header + "description: |\n  one\n  two\n",
		header + "fields:\n  a:\n    type: string\n",
		header, "", "- a\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		decl, err := dsl.ParseActivityDocument([]byte(source))
		if err != nil {
			if decl != nil || err.Error() == "" {
				t.Fatalf("ParseActivityDocument refused %q as %v with %+v", source, err, decl)
			}
			return
		}
		// ok is false for a document that packs past a label's bound: pack
		// refuses it, which is what the bound is for.
		pack := func() (labels [3]string, ok bool) {
			iface, err := dsl.PackInterfaceLabel(decl.Interface)
			if err != nil {
				if strings.Contains(err.Error(), "limit is") {
					return labels, false
				}
				t.Fatalf("PackInterfaceLabel refused an accepted document %q: %v", source, err)
			}
			icon, err := dsl.PackIconLabel(decl.Icon)
			if err != nil {
				t.Fatalf("PackIconLabel refused an accepted document %q: %v", source, err)
			}
			docs, err := dsl.PackDocsLabel(decl.Docs, decl.Interface)
			if err != nil {
				if strings.Contains(err.Error(), "limit is") {
					return labels, false
				}
				t.Fatalf("PackDocsLabel refused an accepted document %q: %v", source, err)
			}
			return [3]string{iface, icon, docs}, true
		}
		labels, ok := pack()
		if !ok {
			return
		}
		if again, _ := pack(); again != labels {
			t.Fatalf("packing %q is not deterministic: %q, then %q", source, labels, again)
		}
		for _, label := range labels {
			if strings.Contains(label, "'") {
				t.Fatalf("a label of %q holds a single quote: %q", source, label)
			}
		}
		iface, err := dsl.ParseInterfaceLabel(labels[0])
		if err != nil || !proto.Equal(iface, decl.Interface) {
			t.Fatalf("the interface label of %q reads back as %v (%v)", source, iface, err)
		}
		icon, err := dsl.ParseIconLabel(labels[1])
		if err != nil || !proto.Equal(icon, decl.Icon) {
			t.Fatalf("the icon label of %q reads back as %v (%v)", source, icon, err)
		}
		docs, err := dsl.ParseDocsLabel(labels[2])
		if err == nil && docs != nil {
			err = dsl.CheckDocsAgainstInterface(docs, iface)
		}
		if err != nil || !proto.Equal(docs, decl.Docs) {
			t.Fatalf("the docs label of %q reads back as %v (%v)", source, docs, err)
		}
	})
}

// FuzzSafeReason: whatever a parser's message quotes, the reason a client is
// given is valid UTF-8, holds no control or format character, and is at most
// 512 bytes.
func FuzzSafeReason(f *testing.F) {
	for _, seed := range []string{
		"plain", "a\x1b]0;pwn\x07b", "\u009b31m\u202eevil", "\xff\xfe", strings.Repeat("日", 300), "", "a\u2028b\u2029",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, reason string) {
		requireSafeReason(t, errors.New(reason))
	})
}
