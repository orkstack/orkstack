package dsl

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// svgIcon wraps body in the root every icon has.
func svgIcon(body string) string {
	return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16">` + body + `</svg>`
}

func attrs(pairs ...string) []*orkstackv1.IconAttribute {
	var out []*orkstackv1.IconAttribute
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &orkstackv1.IconAttribute{Name: pairs[i], Value: pairs[i+1]})
	}
	return out
}

func TestParseIconSVG_ReadsEveryAllowedElementAsATree(t *testing.T) {
	icon, err := ParseIconSVG(`<?xml version="1.0"?>
<!-- an author's note -->
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" width="24" height="24" fill="none" stroke="currentColor" stroke-width="1.4">
  <title>A box</title>
  <path d="M8 1.8 13.5 5v6L8 14.2 2.5 11V5z" stroke-linejoin="round"/>
  <circle cx="8" cy="8" r="6"><desc>outline</desc></circle>
  <ellipse cx="8" cy="8" rx="2.5" ry="6"/>
  <rect x="1.75" y="1.75" width="12.5" height="12.5" rx="2.5"/>
  <line x1="2" y1="8" x2="14" y2="8" stroke-linecap="square"/>
  <polyline points="1,1 2,2 3,1"/>
  <polygon points="1 1 2 2 3 1" fill="currentColor" fill-rule="evenodd" clip-rule="nonzero"/>
  <g transform="translate(1 1) rotate(45 8 8)"><g transform="scale(.5)"><path d="M0 0h1"/></g></g>
</svg>`)
	must.OK(t, err)
	want := &orkstackv1.ActivityIcon{
		Attributes: attrs("fill", "none", "stroke", "currentColor", "stroke-width", "1.4"),
		Elements: []*orkstackv1.IconElement{
			{Name: "path", Attributes: attrs("d", "M8 1.8 13.5 5v6L8 14.2 2.5 11V5z", "stroke-linejoin", "round")},
			{Name: "circle", Attributes: attrs("cx", "8", "cy", "8", "r", "6")},
			{Name: "ellipse", Attributes: attrs("cx", "8", "cy", "8", "rx", "2.5", "ry", "6")},
			{Name: "rect", Attributes: attrs("x", "1.75", "y", "1.75", "width", "12.5", "height", "12.5", "rx", "2.5")},
			{Name: "line", Attributes: attrs("x1", "2", "y1", "8", "x2", "14", "y2", "8", "stroke-linecap", "square")},
			{Name: "polyline", Attributes: attrs("points", "1,1 2,2 3,1")},
			{Name: "polygon", Attributes: attrs("points", "1 1 2 2 3 1", "fill", "currentColor", "fill-rule", "evenodd", "clip-rule", "nonzero")},
			{Name: "g", Attributes: attrs("transform", "translate(1 1) rotate(45 8 8)"), Children: []*orkstackv1.IconElement{
				{Name: "g", Attributes: attrs("transform", "scale(.5)"), Children: []*orkstackv1.IconElement{
					{Name: "path", Attributes: attrs("d", "M0 0h1")}}}}},
		},
	}
	if !proto.Equal(icon, want) {
		t.Fatalf("icon =\n%v\nwant\n%v", icon, want)
	}
	// Comments, the declaration, space, title, desc and the root's size are
	// dropped; the canonical form is one line.
	canonical := CanonicalIconSVG(icon)
	if strings.ContainsAny(canonical, "\n'") || strings.Contains(canonical, "title") || strings.Contains(canonical, "width=\"24\"") {
		t.Fatalf("canonical form %s", canonical)
	}
}

// TestParseIconSVG_KeepsACanonicalIconByteForByte: line icons of the house
// style (the default icon and two of the UI's own glyphs, as published in
// docs/ACTIVITY_IMAGES.md and the UI's design) are read, and their
// canonical form is the source, unchanged.
func TestParseIconSVG_KeepsACanonicalIconByteForByte(t *testing.T) {
	for name, svg := range map[string]string{
		"default":  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M8 1.8 13.5 5v6L8 14.2 2.5 11V5zM2.5 5 8 8.2 13.5 5M8 8.2v6" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>`,
		"composed": `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><circle cx="3.5" cy="4" r="2" fill="none" stroke="currentColor" stroke-width="1.4"/><circle cx="12.5" cy="4" r="2" fill="none" stroke="currentColor" stroke-width="1.4"/><circle cx="8" cy="12.5" r="2" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M5.5 4h5M4.6 5.7 7 10.6M11.4 5.7 9 10.6" fill="none" stroke="currentColor" stroke-width="1.4"/></svg>`,
		"awaited":  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M2 9.5 3.6 3.5h8.8L14 9.5v3.5H2z" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/><path d="M2 9.5h3.4l1 1.6h3.2l1-1.6H14" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>`,
	} {
		icon, err := ParseIconSVG(svg)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got := CanonicalIconSVG(icon); got != svg {
			t.Errorf("%s: the canonical form of an icon already canonical changed:\n%s\n%s", name, got, svg)
		}
	}
}

func TestParseIconSVG_RefusesWhatTheAllowlistDoesNot(t *testing.T) {
	many := strings.Repeat(`<path d="M0 0h1"/>`, MaxIconElements+1)
	deep := strings.Repeat(`<g>`, MaxIconDepth+1) + strings.Repeat(`</g>`, MaxIconDepth+1)
	for _, tc := range []struct{ name, svg, want string }{
		{"too long", svgIcon(`<path d="M0 0` + strings.Repeat(" ", MaxIconLabelBytes) + `"/>`), "limit is 2048"},
		{"not SVG", `{"paths":[]}`, "holds the text"},
		{"not well formed", `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path>`, "is not well-formed SVG"},
		{"no root", `<!-- -->`, "holds no svg element"},
		{"two roots", svgIcon(`<path d="M0 0h1"/>`) + svgIcon(`<path d="M0 0h1"/>`), "a second root element"},
		{"an empty drawing", svgIcon(` <title>x</title> `), "draws nothing"},
		{"another root", `<g xmlns="http://www.w3.org/2000/svg"/>`, `has the root element "g"`},
		{"another namespace", `<svg xmlns="http://example.test/x" viewBox="0 0 16 16"/>`, "has the root element"},
		{"another grid", `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"/>`, `viewBox "0 0 24 24"`},
		{"no grid", `<svg xmlns="http://www.w3.org/2000/svg"/>`, "declares no viewBox"},
		{"a DOCTYPE", `<!DOCTYPE svg [<!ENTITY x "y">]>` + svgIcon(``), "DOCTYPE"},
		{"an entity", svgIcon(`<path d="&x;"/>`), "is not well-formed SVG"},
		{"a processing instruction", svgIcon(`<?php x ?>`), `processing instruction "php"`},
		{"a late declaration", `<!-- -->` + `<?xml version="1.0"?>` + svgIcon(``), `processing instruction "xml"`},
		{"CDATA", svgIcon(`<![CDATA[ ]]>`), "CDATA"},
		{"text", svgIcon(`hello`), `holds the text "hello"`},
		{"a script", svgIcon(`<script>alert(1)</script>`), `holds the element "script"`},
		{"a use", svgIcon(`<use href="#a"/>`), `holds the element`},
		{"an image", svgIcon(`<image href="data:image/png;base64,AA=="/>`), `holds the element`},
		{"text element", svgIcon(`<text>x</text>`), `holds the element`},
		{"a style element", svgIcon(`<style>*{}</style>`), `holds the element`},
		{"foreignObject", svgIcon(`<foreignObject/>`), `holds the element`},
		{"a gradient", svgIcon(`<linearGradient/>`), `holds the element`},
		{"an animation", svgIcon(`<path d="M0 0h1"><animate/></path>`), `holds the element`},
		{"an element in a title", svgIcon(`<title><script/></title>`), "a title or desc holds text alone"},
		{"a foreign title", svgIcon(`<x:title xmlns:x="http://example.test/x">t</x:title>`), `"http://example.test/x:title"`},
		{"an event attribute", svgIcon(`<path d="M0 0h1" onclick="x()"/>`), `"onclick"`},
		{"a style attribute", svgIcon(`<path d="M0 0h1" style="fill:red"/>`), `"style"`},
		{"a class", svgIcon(`<path d="M0 0h1" class="a"/>`), `"class"`},
		{"an id", svgIcon(`<path d="M0 0h1" id="a"/>`), `"id"`},
		{"an href", svgIcon(`<path d="M0 0h1" href="#a"/>`), `"href"`},
		{"an xlink href", `<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 16 16"/>`, "declares no other namespace"},
		{"a foreign element", svgIcon(`<x:path xmlns:x="http://example.test/x" d="M0 0h1"/>`), `"http://example.test/x:path"`},
		{"a prefixed geometry attribute", svgIcon(`<path foo:d="M0 0h1"/>`), "declares no other namespace"},
		{"an opacity", svgIcon(`<path d="M0 0h1" opacity=".5"/>`), `"opacity"`},
		{"a colour", svgIcon(`<path d="M0 0h1" fill="red"/>`), `fill is "red", not one of none, currentColor`},
		{"a url paint", svgIcon(`<path d="M0 0h1" stroke="url(#g)"/>`), `stroke is "url(#g)"`},
		{"a mask", svgIcon(`<path d="M0 0h1" mask="url(#m)"/>`), `"mask"`},
		{"a clip path", svgIcon(`<path d="M0 0h1" clip-path="url(#c)"/>`), `"clip-path"`},
		{"a marker", svgIcon(`<path d="M0 0h1" marker-end="url(#m)"/>`), `"marker-end"`},
		{"geometry out of place", svgIcon(`<path d="M0 0h1" r="2"/>`), `path has the attribute "r"`},
		{"a d on a circle", svgIcon(`<circle d="M0 0"/>`), `circle has the attribute "d"`},
		{"a transform on a shape", svgIcon(`<path d="M0 0h1" transform="scale(2)"/>`), `"transform"`},
		{"a transform on the root", `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" transform="scale(2)"/>`, `"transform"`},
		{"xmlns on a child", svgIcon(`<path d="M0 0h1" xmlns="http://www.w3.org/2000/svg"/>`), `"xmlns"`},
		{"a duplicate attribute", svgIcon(`<path d="M0 0h1" fill="none" fill="none"/>`), `path has the attribute "fill" twice`},
		{"a unit", svgIcon(`<circle cx="8px" cy="8" r="6"/>`), `cx is "8px", not a plain number`},
		{"a percent", svgIcon(`<rect width="50%"/>`), `width is "50%"`},
		{"a negative radius", svgIcon(`<circle r="-1"/>`), `r is "-1", a negative length`},
		{"a huge number", svgIcon(`<line x1="1e9"/>`), "not a number from -10000 to 10000"},
		{"a number one past the bound", svgIcon(`<line x1="10001"/>`), "not a number from -10000 to 10000"},
		{"a transform ending in a comma", svgIcon(`<g transform="translate(1 2),"><path d="M0 0h1"/></g>`), "ends with a comma"},
		{"an unclosed transform", svgIcon(`<g transform="translate(1"><path d="M0 0h1"/></g>`), "translate: ends where ) was expected"},
		{"a transform with no opening parenthesis", svgIcon(`<g transform="translate 1 2)"><path d="M0 0h1"/></g>`),
			"translate: has '1' at byte 10 where ( was expected"},
		{"an empty transform", svgIcon(`<g transform=" "><path d="M0 0h1"/></g>`), "is empty"},
		{"a leading stylesheet instruction", `<?xml-stylesheet href="a.css"?>` + svgIcon(`<path d="M0 0h1"/>`),
			`processing instruction "xml-stylesheet"`},
		{"a wide stroke", svgIcon(`<path d="M0 0h1" stroke-width="5"/>`), `stroke-width is "5", not from 0 to 4`},
		{"a bad linecap", svgIcon(`<path d="M0 0h1" stroke-linecap="inherit"/>`), `stroke-linecap is "inherit"`},
		{"a bad path", svgIcon(`<path d="M0 0 javascript:x"/>`), "path d has 'j'"},
		{"odd points", svgIcon(`<polyline points="1 2 3"/>`), "holds 3 numbers, not pairs"},
		{"empty points", svgIcon(`<polygon points=""/>`), "holds 0 numbers"},
		{"a bad transform", svgIcon(`<g transform="url(#x)"/>`), `names the transform "url"`},
		{"a transform of the wrong arity", svgIcon(`<g transform="rotate(1 2)"/>`), "rotate"},
		{"a negative stroke width", svgIcon(`<path d="M0 0h1" stroke-width="-1"/>`), `"-1"`},
		{"a stroke width just past 4", svgIcon(`<path d="M0 0h1" stroke-width="4.01"/>`), `"4.01"`},
		{"a duplicate attribute on the root", `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="none" fill="none"/>`, `"fill" twice`},
		{"a shape with children", svgIcon(`<path d="M0 0h1"><path d="M0 0h1"/></path>`), "path holds elements"},
		{"too many elements", svgIcon(many), "more than 32 elements"},
		{"too deep", svgIcon(deep), "nests g more than 4 deep"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			icon, err := ParseIconSVG(tc.svg)
			if err == nil {
				t.Fatalf("ParseIconSVG accepted %q as %v", tc.svg, icon)
			}
			if icon != nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseIconSVG = %v; want an error containing %q", err, tc.want)
			}
		})
	}
}

// TestParseIconLabel_AnEmptyValueDeclaresNone: an image built FROM an
// icon-bearing base clears the label it inherits with an empty value.
func TestParseIconLabel_AnEmptyValueDeclaresNone(t *testing.T) {
	icon, err := ParseIconLabel("")
	if icon != nil || err != nil {
		t.Fatalf("ParseIconLabel(\"\") = %v, %v; want nil, nil", icon, err)
	}
}

// TestPackIconLabel_WritesTheCanonicalSVG: pack writes one line the engine
// reads back as the same icon, and refuses an icon the engine would not
// read, or would read differently.
func TestPackIconLabel_WritesTheCanonicalSVG(t *testing.T) {
	icon, err := ParseIconSVG("<svg viewBox='0 0 16 16'\n  stroke='currentColor'>\n  <path d='M2 8h12'/>\n</svg>")
	must.OK(t, err)
	label, err := PackIconLabel(icon)
	must.OK(t, err)
	want := `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" stroke="currentColor"><path d="M2 8h12"/></svg>`
	if label != want {
		t.Fatalf("label = %s, want %s", label, want)
	}
	if back, err := ParseIconLabel(label); err != nil || !proto.Equal(back, icon) {
		t.Fatalf("the label reads back as %v (%v), not %v", back, err, icon)
	}
	if label, err := PackIconLabel(nil); label != "" || err != nil {
		t.Fatalf("PackIconLabel(nil) = %q, %v; want the empty value that declares none", label, err)
	}
	for name, bad := range map[string]*orkstackv1.ActivityIcon{
		"an element outside the allowlist": {Elements: []*orkstackv1.IconElement{{Name: "script"}}},
		"a value outside its grammar":      {Elements: []*orkstackv1.IconElement{{Name: "path", Attributes: attrs("d", `M0 0"/><script>`)}}},
		"a dropped element":                {Elements: []*orkstackv1.IconElement{{Name: "title"}}},
	} {
		if label, err := PackIconLabel(bad); err == nil {
			t.Errorf("%s: PackIconLabel packed %q", name, label)
		}
	}
	// Even an unchecked value never breaks out of its attribute.
	escaped := CanonicalIconSVG(&orkstackv1.ActivityIcon{Elements: []*orkstackv1.IconElement{
		{Name: "path", Attributes: attrs("d", `"/><script>`)}}})
	if strings.Contains(escaped, `"/><script>`) {
		t.Fatalf("CanonicalIconSVG wrote a value unescaped: %s", escaped)
	}
}

// TestParseIconSVG_AcceptsEveryBoundExactly: the bounds are reached, not
// only refused one past them — 32 elements, g nested 4 deep, numbers of
// +/-10000 — and a title, which is dropped, is not counted.
func TestParseIconSVG_AcceptsEveryBoundExactly(t *testing.T) {
	deep := strings.Repeat(`<g>`, MaxIconDepth) + `<path d="M0 0h1"/>` + strings.Repeat(`</g>`, MaxIconDepth)
	for name, svg := range map[string]string{
		"32 elements":                   svgIcon(strings.Repeat(`<path d="M0 0h1"/>`, MaxIconElements)),
		"32 elements and titles":        svgIcon(strings.Repeat(`<path d="M0 0h1"><title>t</title></path>`, MaxIconElements)),
		"g nested 4 deep":               svgIcon(deep),
		"the numbers at the edge":       svgIcon(`<line x1="-10000" y1="10000" x2="1e4" y2="-1E4"/><path d="M-10000 10000"/>`),
		"the stroke widths at the edge": svgIcon(`<path d="M0 0h1" stroke-width="0"/><path d="M0 1h1" stroke-width="4"/>`),
		"a zero radius and size":        svgIcon(`<circle r="0"/><rect width="0" height="0"/>`),
		"a transform list":              svgIcon(`<g transform="matrix(1 0 0 1 0 0), skewX(10)skewY(-10)"><path d="M0 0h1"/></g>`),
	} {
		if _, err := ParseIconSVG(svg); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestParseIconSVG_EachTransformTakesItsArguments: every transform function
// accepts exactly the argument counts SVG gives it, and no other count from
// none to seven.
func TestParseIconSVG_EachTransformTakesItsArguments(t *testing.T) {
	takes := map[string][]int{"matrix": {6}, "translate": {1, 2}, "scale": {1, 2}, "rotate": {1, 3}, "skewX": {1}, "skewY": {1}}
	for name, counts := range takes {
		for k := 0; k <= 7; k++ {
			svg := svgIcon(`<g transform="` + name + `(` + strings.TrimSpace(strings.Repeat(" 1", k)) + `)"><path d="M0 0h1"/></g>`)
			_, err := ParseIconSVG(svg)
			if want := slices.Contains(counts, k); (err == nil) != want {
				t.Errorf("%s with %d arguments: accepted %v, want %v (%v)", name, k, err == nil, want, err)
			}
		}
	}
}

// TestPackIconLabel_RefusesAnIconTheEngineWouldReadDifferently: an icon that
// parses but would read back as another — here a root width, which the
// parser drops — is refused, never packed into a label that means less.
func TestPackIconLabel_RefusesAnIconTheEngineWouldReadDifferently(t *testing.T) {
	icon := &orkstackv1.ActivityIcon{Attributes: attrs("width", "24"),
		Elements: []*orkstackv1.IconElement{{Name: "path", Attributes: attrs("d", "M0 0h1")}}}
	if label, err := PackIconLabel(icon); err == nil || !strings.Contains(err.Error(), "read back") {
		t.Fatalf("PackIconLabel = %q, %v; want a refusal: the label would read back without the width", label, err)
	}
}
