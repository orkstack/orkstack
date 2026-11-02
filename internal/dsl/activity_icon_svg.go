package dsl

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The SVG an activity icon may be (docs/ACTIVITY_IMAGES.md, "The icon").
//
// An icon is one small SVG document of an allowlisted subset: shapes, groups
// and the attributes that draw them in the current colour, nothing that can
// fetch, run, style or reference anything. Every name is held to the
// allowlist and every value to its own grammar, so what passes is a drawing
// and nothing else; anything outside it refuses the whole icon, never a part
// of it. The UI draws the tree this returns and shows its default icon for
// a refused one, so the engine's verdict is the only one.

// svgNamespace is the one namespace an icon's elements are in.
const svgNamespace = "http://www.w3.org/2000/svg"

// iconViewBox is the one grid an icon is drawn on.
const iconViewBox = "0 0 16 16"

// Bounds of an icon's SVG.
const (
	MaxIconLabelBytes  = 2048 // the label's SVG text
	MaxIconElements    = 32   // elements below the root
	MaxIconDepth       = 4    // g inside g
	maxIconStrokeWidth = 4    // on the 16-unit grid
)

// iconPaintAttributes are the presentation attributes the root, a g and
// every shape may carry.
var iconPaintAttributes = map[string]func(string) error{
	"fill":            iconWord("none", "currentColor"),
	"stroke":          iconWord("none", "currentColor"),
	"stroke-width":    checkIconStrokeWidth,
	"stroke-linecap":  iconWord("butt", "round", "square"),
	"stroke-linejoin": iconWord("miter", "round", "bevel"),
	"fill-rule":       iconWord("nonzero", "evenodd"),
	"clip-rule":       iconWord("nonzero", "evenodd"),
}

// iconGeometry is, for each element an icon may hold below its root, the
// geometry attributes it may carry and how each value is read. Geometry is
// accepted only where it belongs.
var iconGeometry = map[string]map[string]func(string) error{
	"path":     {"d": CheckIconPathData},
	"circle":   {"cx": checkIconNumber, "cy": checkIconNumber, "r": checkIconLength},
	"ellipse":  {"cx": checkIconNumber, "cy": checkIconNumber, "rx": checkIconLength, "ry": checkIconLength},
	"rect":     {"x": checkIconNumber, "y": checkIconNumber, "width": checkIconLength, "height": checkIconLength, "rx": checkIconLength, "ry": checkIconLength},
	"line":     {"x1": checkIconNumber, "y1": checkIconNumber, "x2": checkIconNumber, "y2": checkIconNumber},
	"polyline": {"points": checkIconPoints},
	"polygon":  {"points": checkIconPoints},
	"g":        {"transform": checkIconTransform},
}

// iconDropped are the elements dropped with their text, never refused: a
// title or desc names a drawing a client shows as decoration beside a name.
var iconDropped = map[string]bool{"title": true, "desc": true}

// ParseIconSVG holds an icon's SVG to the allowlist and returns its drawing
// as a tree. Comments, the XML declaration, space between elements, and
// title and desc elements are dropped; anything else outside the allowlist
// refuses the whole icon, and the error names the first such thing. Pure.
func ParseIconSVG(svg string) (*orkstackv1.ActivityIcon, error) {
	if len(svg) > MaxIconLabelBytes {
		return nil, fmt.Errorf("is %d bytes of SVG, limit is %d", len(svg), MaxIconLabelBytes)
	}
	// The decoder reads a CDATA section as text; refuse it before it can.
	if strings.Contains(svg, "<![CDATA[") {
		return nil, errors.New("holds a CDATA section; an icon is shapes alone")
	}
	decoder := xml.NewDecoder(strings.NewReader(svg))
	decoder.Strict = true
	parser := &iconParser{decoder: decoder}
	return parser.document()
}

// iconParser walks one icon's tokens.
type iconParser struct {
	decoder  *xml.Decoder
	elements int
}

func (p *iconParser) token() (xml.Token, error) {
	tok, err := p.decoder.Token()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("is not well-formed SVG: %v", err)
	}
	return tok, err
}

// document reads the optional XML declaration, the one svg root, and
// nothing after it but space and comments.
func (p *iconParser) document() (*orkstackv1.ActivityIcon, error) {
	var icon *orkstackv1.ActivityIcon
	first := true
	for {
		tok, err := p.token()
		if errors.Is(err, io.EOF) {
			if icon == nil {
				return nil, errors.New("holds no svg element")
			}
			return icon, nil
		}
		if err != nil {
			return nil, err
		}
		switch tok := tok.(type) {
		case xml.ProcInst:
			if !first || tok.Target != "xml" {
				return nil, fmt.Errorf("holds the processing instruction %q; an icon is shapes alone", tok.Target)
			}
		case xml.StartElement:
			if icon != nil {
				return nil, errors.New("holds a second root element; an icon is one svg element")
			}
			if icon, err = p.root(tok); err != nil {
				return nil, err
			}
		default:
			if err := p.between(tok); err != nil {
				return nil, err
			}
		}
		first = false
	}
}

// between drops what may stand between elements — space and comments — and
// refuses anything else.
func (p *iconParser) between(tok xml.Token) error {
	switch tok := tok.(type) {
	case xml.CharData:
		if strings.TrimLeft(string(tok), " \t\r\n") != "" {
			return fmt.Errorf("holds the text %q; an icon is shapes alone", truncateText(string(tok)))
		}
		return nil
	case xml.Comment:
		return nil
	case xml.Directive:
		return errors.New("holds a DOCTYPE or other directive; an icon is shapes alone")
	case xml.ProcInst:
		return fmt.Errorf("holds the processing instruction %q; an icon is shapes alone", tok.Target)
	}
	return fmt.Errorf("holds an unexpected %T", tok)
}

// inSVG reports a name in the SVG namespace, or in none (an svg written for
// inline use, without xmlns).
func inSVG(name xml.Name) bool { return name.Space == svgNamespace || name.Space == "" }

// root reads the svg element: its namespace, the one grid, its presentation
// attributes, and its children.
func (p *iconParser) root(start xml.StartElement) (*orkstackv1.ActivityIcon, error) {
	if start.Name.Local != "svg" || !inSVG(start.Name) {
		return nil, fmt.Errorf("has the root element %q; an icon is one svg element", qualified(start.Name))
	}
	icon := &orkstackv1.ActivityIcon{}
	var viewBox bool
	seen := map[string]bool{}
	for _, attr := range start.Attr {
		name := attr.Name.Local
		if attr.Name.Space != "" {
			return nil, fmt.Errorf("svg has the attribute %q; an icon declares no other namespace", qualified(attr.Name))
		}
		if seen[name] {
			return nil, fmt.Errorf("svg has the attribute %q twice", name)
		}
		seen[name] = true
		switch check, paint := iconPaintAttributes[name]; {
		case name == "xmlns":
			if attr.Value != svgNamespace {
				return nil, fmt.Errorf("svg has xmlns %q; an icon is in the %s namespace", attr.Value, svgNamespace)
			}
		case name == "viewBox":
			if attr.Value != iconViewBox {
				return nil, fmt.Errorf("svg has viewBox %q; an icon is drawn on viewBox=%q", attr.Value, iconViewBox)
			}
			viewBox = true
		case name == "width" || name == "height":
			// A client sizes the icon itself; the root's own size is dropped.
		case paint:
			if err := check(attr.Value); err != nil {
				return nil, fmt.Errorf("svg %s %w", name, err)
			}
			icon.Attributes = append(icon.Attributes, &orkstackv1.IconAttribute{Name: name, Value: attr.Value})
		default:
			return nil, fmt.Errorf("svg has the attribute %q, which an icon's root may not", name)
		}
	}
	if !viewBox {
		return nil, fmt.Errorf("svg declares no viewBox; an icon is drawn on viewBox=%q", iconViewBox)
	}
	children, err := p.children("svg", 0)
	if err != nil {
		return nil, err
	}
	if len(children) == 0 {
		return nil, errors.New("draws nothing: an icon holds at least one element, and an image without one omits the label")
	}
	icon.Elements = children
	return icon, nil
}

// children reads the elements under parent up to its end tag, dropping
// title and desc.
func (p *iconParser) children(parent string, depth int) ([]*orkstackv1.IconElement, error) {
	var out []*orkstackv1.IconElement
	for {
		// The decoder reports an end of input inside an open element as a
		// syntax error, so EOF never reaches here.
		tok, err := p.token()
		if err != nil {
			return nil, err
		}
		switch tok := tok.(type) {
		case xml.EndElement:
			return out, nil
		case xml.StartElement:
			if iconDropped[tok.Name.Local] && inSVG(tok.Name) {
				if err := p.skipText(tok.Name.Local); err != nil {
					return nil, err
				}
				continue
			}
			element, err := p.element(tok, depth)
			if err != nil {
				return nil, err
			}
			out = append(out, element)
		default:
			if err := p.between(tok); err != nil {
				return nil, err
			}
		}
	}
}

// skipText drops a title or desc: its text and comments, up to its end tag.
// An element inside one is refused, not dropped unseen.
func (p *iconParser) skipText(name string) error {
	for {
		tok, err := p.token()
		if err != nil {
			return err
		}
		switch tok := tok.(type) {
		case xml.EndElement:
			return nil
		case xml.CharData, xml.Comment:
		case xml.StartElement:
			return fmt.Errorf("%s holds the element %q; a title or desc holds text alone", name, qualified(tok.Name))
		default:
			if err := p.between(tok); err != nil {
				return err
			}
		}
	}
}

// element reads one element below the root.
func (p *iconParser) element(start xml.StartElement, depth int) (*orkstackv1.IconElement, error) {
	name := start.Name.Local
	geometry, allowed := iconGeometry[name]
	if !allowed || !inSVG(start.Name) {
		return nil, fmt.Errorf("holds the element %q; an icon holds only %s", qualified(start.Name),
			"path, circle, ellipse, rect, line, polyline, polygon and g")
	}
	if p.elements++; p.elements > MaxIconElements {
		return nil, fmt.Errorf("holds more than %d elements", MaxIconElements)
	}
	element := &orkstackv1.IconElement{Name: name}
	seen := map[string]bool{}
	for _, attr := range start.Attr {
		attrName := attr.Name.Local
		if attr.Name.Space != "" {
			return nil, fmt.Errorf("%s has the attribute %q; an icon declares no other namespace", name, qualified(attr.Name))
		}
		if seen[attrName] {
			return nil, fmt.Errorf("%s has the attribute %q twice", name, attrName)
		}
		seen[attrName] = true
		check, ok := geometry[attrName]
		if !ok {
			check, ok = iconPaintAttributes[attrName]
		}
		if !ok {
			return nil, fmt.Errorf("%s has the attribute %q, which an icon may not use there", name, attrName)
		}
		if err := check(attr.Value); err != nil {
			return nil, fmt.Errorf("%s %s %w", name, attrName, err)
		}
		element.Attributes = append(element.Attributes, &orkstackv1.IconAttribute{Name: attrName, Value: attr.Value})
	}
	if name != "g" {
		// A shape holds nothing but what is dropped: title, desc, space.
		children, err := p.children(name, depth)
		if err != nil {
			return nil, err
		}
		if len(children) > 0 {
			return nil, fmt.Errorf("%s holds elements; only a g may", name)
		}
		return element, nil
	}
	if depth+1 > MaxIconDepth {
		return nil, fmt.Errorf("nests g more than %d deep", MaxIconDepth)
	}
	children, err := p.children(name, depth+1)
	if err != nil {
		return nil, err
	}
	element.Children = children
	return element, nil
}

// qualified names an element or attribute in a message: its local name when
// it is in the SVG namespace or none, its namespace and name otherwise.
func qualified(name xml.Name) string {
	if inSVG(name) {
		return name.Local
	}
	return name.Space + ":" + name.Local
}

// truncateText is untrusted text for a message: at most 32 bytes.
func truncateText(s string) string {
	if len(s) > 32 {
		return s[:32] + "…"
	}
	return s
}

// iconWord is a value that is one of words, exactly.
func iconWord(words ...string) func(string) error {
	return func(value string) error {
		for _, word := range words {
			if value == word {
				return nil
			}
		}
		return fmt.Errorf("is %q, not one of %s", value, strings.Join(words, ", "))
	}
}

// iconNumber reads a whole value as one SVG number: no unit, no percent.
func iconNumber(value string) (float64, error) {
	p := &pathScanner{d: value}
	if err := p.number(); err != nil {
		if p.done() || p.i > 0 {
			return 0, err
		}
		return 0, fmt.Errorf("is %q, not a plain number", value)
	}
	if !p.done() {
		return 0, fmt.Errorf("is %q, not a plain number", value)
	}
	return strconv.ParseFloat(value, 64)
}

func checkIconNumber(value string) error {
	_, err := iconNumber(value)
	return err
}

// checkIconLength is a number that may not be negative: a radius or a size.
func checkIconLength(value string) error {
	n, err := iconNumber(value)
	if err == nil && n < 0 {
		return fmt.Errorf("is %q, a negative length", value)
	}
	return err
}

func checkIconStrokeWidth(value string) error {
	n, err := iconNumber(value)
	if err == nil && (n < 0 || n > maxIconStrokeWidth) {
		return fmt.Errorf("is %q, not from 0 to %d", value, maxIconStrokeWidth)
	}
	return err
}

// checkIconPoints is a points list: number pairs, at least one, separated
// by space or a comma.
func checkIconPoints(value string) error {
	p := &pathScanner{d: value}
	p.spaces()
	count := 0
	for !p.done() {
		if count > 0 {
			p.separator()
		}
		if err := p.number(); err != nil {
			return fmt.Errorf("is not a list of number pairs: %w", err)
		}
		count++
		p.spaces()
	}
	if count == 0 || count%2 != 0 {
		return fmt.Errorf("holds %d numbers, not pairs of them", count)
	}
	return nil
}

// iconTransforms is each transform function and the argument counts it takes.
var iconTransforms = map[string][]int{
	"matrix": {6}, "translate": {1, 2}, "scale": {1, 2}, "rotate": {1, 3}, "skewX": {1}, "skewY": {1},
}

// checkIconTransform is a transform list: functions of numbers, separated
// by space or a comma.
func checkIconTransform(value string) error {
	p := &pathScanner{d: value}
	p.spaces()
	if p.done() {
		return errors.New("is empty")
	}
	for !p.done() {
		start := p.i
		for !p.done() && (p.d[p.i] >= 'a' && p.d[p.i] <= 'z' || p.d[p.i] >= 'A' && p.d[p.i] <= 'Z') {
			p.i++
		}
		name := p.d[start:p.i]
		arities, known := iconTransforms[name]
		if !known {
			return fmt.Errorf("names the transform %q, not one of matrix, translate, scale, rotate, skewX, skewY", name)
		}
		p.spaces()
		if p.done() || p.d[p.i] != '(' {
			return fmt.Errorf("%s: %w", name, p.unexpected("("))
		}
		p.i++
		p.spaces()
		args := 0
		for !p.done() && p.d[p.i] != ')' {
			if args > 0 {
				p.separator()
			}
			if err := p.number(); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			args++
			p.spaces()
		}
		if p.done() {
			return fmt.Errorf("%s: %w", name, p.unexpected(")"))
		}
		p.i++
		fits := false
		for _, n := range arities {
			fits = fits || n == args
		}
		if !fits {
			return fmt.Errorf("%s takes %v numbers, not %d", name, arities, args)
		}
		if p.separator() && p.done() {
			return errors.New("ends with a comma")
		}
	}
	return nil
}

// CanonicalIconSVG is an icon's SVG as pack writes it: one line, the
// root's namespace and grid first, every attribute in its order, every value
// escaped, every element without children self-closed. One drawing is
// always one string. Names are written as they are: it is for an icon
// ParseIconSVG returned, and PackIconLabel reads its result back before any
// label carries it.
func CanonicalIconSVG(icon *orkstackv1.ActivityIcon) string {
	var b strings.Builder
	b.WriteString(`<svg xmlns="` + svgNamespace + `" viewBox="` + iconViewBox + `"`)
	writeIconAttributes(&b, icon.GetAttributes())
	b.WriteString(">")
	for _, element := range icon.GetElements() {
		writeIconElement(&b, element)
	}
	b.WriteString("</svg>")
	return b.String()
}

func writeIconElement(b *strings.Builder, element *orkstackv1.IconElement) {
	b.WriteString("<" + element.GetName())
	writeIconAttributes(b, element.GetAttributes())
	if len(element.GetChildren()) == 0 {
		b.WriteString("/>")
		return
	}
	b.WriteString(">")
	for _, child := range element.GetChildren() {
		writeIconElement(b, child)
	}
	b.WriteString("</" + element.GetName() + ">")
}

func writeIconAttributes(b *strings.Builder, attributes []*orkstackv1.IconAttribute) {
	for _, attr := range attributes {
		b.WriteString(" " + attr.GetName() + `="`)
		// A parsed value is held to its grammar, which admits none of the
		// characters escaped here; the escape is what makes the output safe
		// for any value at all. Writing to a strings.Builder cannot fail.
		_ = xml.EscapeText(b, []byte(attr.GetValue()))
		b.WriteString(`"`)
	}
}
