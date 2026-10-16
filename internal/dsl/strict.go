package dsl

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
)

// The strict YAML-to-JSON mapping (docs/JSON.md, "Data written in an authored
// document"). Wherever authored YAML becomes runtime JSON — an Orkflow's
// inputs:, every with:, an await's schema:, an automation schedule's input: —
// it is converted here, and never by a lenient decode.
//
// A plain (unquoted) scalar is accepted only when the three readers that
// matter agree on it — YAML 1.2's core schema (editors, yaml-language-server,
// Bun), YAML 1.1 as PyYAML resolves it, and yaml.v3 (what OrkStack decoded
// these positions with before) — and the JSON OrkStack writes for it keeps its
// value exactly. Including yaml.v3 makes one more guarantee checkable: a
// document the previous compiler accepted either compiles to the identical
// message or is refused, never to a different value.

// AuthoredDataError is a refusal of authored data: the path and line of the
// node, and why.
type AuthoredDataError struct {
	Path   string
	Line   int
	Reason string
}

func (e *AuthoredDataError) Error() string {
	return fmt.Sprintf("line %d: %s: %s", e.Line, e.Path, e.Reason)
}

// AuthoredJSON converts one node of authored data into the JSON value it means,
// or refuses it. path names the node for the error ("tasks[2].with.body").
// The value is nil, bool, float64, string, []any or map[string]any.
func AuthoredJSON(node *yaml.Node, path string) (any, error) {
	return authoredValue(node, path)
}

func refuse(node *yaml.Node, path, format string, args ...any) error {
	return &AuthoredDataError{Path: path, Line: node.Line, Reason: fmt.Sprintf(format, args...)}
}

// structural refuses what no position takes: an alias, an explicit tag, an
// anchor, a merge key.
func structural(node *yaml.Node, path string) error {
	switch {
	case node.Kind == yaml.AliasNode:
		name := node.Value
		if name == "" && node.Alias != nil {
			name = node.Alias.Anchor
		}
		return refuse(node, path, "the alias *%s is not allowed in authored data; write the value out where it is used", name)
	case node.Style&yaml.TaggedStyle != 0:
		return refuse(node, path, "the tag %s is not allowed in authored data; write the value plainly, or quote it for a string", node.Tag)
	case node.Anchor != "":
		return refuse(node, path, "the anchor &%s is not allowed in authored data; write the value out where it is used", node.Anchor)
	case node.Kind == yaml.ScalarNode && node.Tag == "!!merge" && node.Style == 0:
		return refuse(node, path, "a merge key (<<) is not allowed in authored data; write the keys out")
	}
	return nil
}

func authoredValue(node *yaml.Node, path string) (any, error) {
	if err := structural(node, path); err != nil {
		return nil, err
	}
	switch node.Kind {
	case yaml.MappingNode:
		out := make(map[string]any, len(node.Content)/2)
		lines := map[string]int{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			keyNode, valueNode := node.Content[i], node.Content[i+1]
			key, err := authoredKey(keyNode, path)
			if err != nil {
				return nil, err
			}
			at := joinPath(path, key)
			if first, dup := lines[key]; dup {
				return nil, refuse(keyNode, at, "the key %q is written twice (first at line %d)", key, first)
			}
			lines[key] = keyNode.Line
			value, err := authoredValue(valueNode, at)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
		return out, nil
	case yaml.SequenceNode:
		out := make([]any, 0, len(node.Content))
		for i, item := range node.Content {
			value, err := authoredValue(item, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	case yaml.ScalarNode:
		if node.Style&(yaml.SingleQuotedStyle|yaml.DoubleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
			return node.Value, nil
		}
		value, reason := plainScalar(node)
		if reason != "" {
			return nil, refuse(node, path, "%s", reason)
		}
		return value, nil
	}
	return nil, refuse(node, path, "%s is not authored data", DescribeNode(node))
}

// authoredKey is a mapping key's JSON name: its text, when every reader
// agrees on it (2.2 "Keys").
func authoredKey(node *yaml.Node, parent string) (string, error) {
	if node.Kind == yaml.AliasNode || node.Style&yaml.TaggedStyle != 0 || node.Anchor != "" ||
		(node.Kind == yaml.ScalarNode && node.Tag == "!!merge" && node.Style == 0) {
		return "", structural(node, joinPath(parent, node.Value))
	}
	if node.Kind != yaml.ScalarNode {
		return "", refuse(node, parent, "a key must be a string, got %s", DescribeNode(node))
	}
	at := joinPath(parent, node.Value)
	if node.Style&(yaml.SingleQuotedStyle|yaml.DoubleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
		return node.Value, nil
	}
	text := node.Value
	r := readAll(node)
	if r.core.kind == kindNull && r.y11.kind == kindNull && r.v3.kind == kindNull {
		return "", refuse(node, at, "the key %s is null, and a JSON object key is a string; quote it (%s) for the key %s", text, quoteFor(text), quoteFor(text))
	}
	value, reason := r.decide(text)
	if reason != "" {
		return "", refuse(node, at, "the key %s", reason)
	}
	switch v := value.(type) {
	case string:
		return v, nil
	case bool:
		if strconv.FormatBool(v) == text {
			return text, nil
		}
		return "", refuse(node, at, "the key %s is the boolean %t to YAML readers, and a JSON object key is a string; write %t, or quote it (%s) for the key %s",
			text, v, v, quoteFor(text), quoteFor(text))
	case float64:
		if r.core.integer && numberText(v) == text {
			return text, nil
		}
		return "", refuse(node, at, "the key %s is the number %s to YAML readers, and a JSON object key is a string; write %s, or quote it (%s) for the key %s",
			text, numberText(v), numberText(v), quoteFor(text), quoteFor(text))
	}
	return "", refuse(node, at, "the key %s is not a string; quote it (%s)", text, quoteFor(text))
}

// plainScalar applies the plain-scalar rule to one plain scalar node: its
// JSON value, or the reason it is refused.
func plainScalar(node *yaml.Node) (any, string) {
	return readAll(node).decide(node.Value)
}

type readKind int

const (
	kindString readKind = iota
	kindNull
	kindBool
	kindNumber
	kindTime
)

// reading is one reader's resolution of a plain scalar.
type reading struct {
	kind    readKind
	b       bool
	f       float64 // a number's value; ±Inf or NaN for core's specials
	integer bool    // the text is an integer form (decimal, hex, octal, base 60)
	whole   *big.Int
	special bool // core's .inf / .nan
	over    bool // core's float parse overflowed a finite literal
	s       string
}

type readings struct{ core, y11, v3 reading }

func readAll(node *yaml.Node) readings {
	text := node.Value
	return readings{core: readCore(text), y11: readY11(text), v3: readV3(node)}
}

var (
	coreNull  = regexp.MustCompile(`^(null|Null|NULL|~|)$`)
	coreBool  = regexp.MustCompile(`^(true|True|TRUE|false|False|FALSE)$`)
	coreDec   = regexp.MustCompile(`^[-+]?[0-9]+$`)
	coreOct   = regexp.MustCompile(`^0o[0-7]+$`)
	coreHex   = regexp.MustCompile(`^0x[0-9a-fA-F]+$`)
	coreFloat = regexp.MustCompile(`^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$`)
	coreInf   = regexp.MustCompile(`^[-+]?(\.inf|\.Inf|\.INF)$`)
	coreNaN   = regexp.MustCompile(`^(\.nan|\.NaN|\.NAN)$`)

	y11Bool      = regexp.MustCompile(`^(y|Y|yes|Yes|YES|n|N|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	y11True      = regexp.MustCompile(`^(y|Y|yes|Yes|YES|true|True|TRUE|on|On|ON)$`)
	y11Bin       = regexp.MustCompile(`^[-+]?0b[0-1_]+$`)
	y11Oct       = regexp.MustCompile(`^[-+]?0[0-7_]+$`)
	y11Dec       = regexp.MustCompile(`^[-+]?(0|[1-9][0-9_]*)$`)
	y11Hex       = regexp.MustCompile(`^[-+]?0x[0-9a-fA-F_]+$`)
	y11Sexa      = regexp.MustCompile(`^[-+]?[1-9][0-9_]*(:[0-5]?[0-9])+$`)
	y11Float     = regexp.MustCompile(`^[-+]?([0-9][0-9_]*)\.[0-9_]*([eE][-+][0-9]+)?$`)
	y11FloatDot  = regexp.MustCompile(`^\.[0-9][0-9_]*([eE][-+][0-9]+)?$`)
	y11FloatSexa = regexp.MustCompile(`^[-+]?[0-9][0-9_]*(:[0-5]?[0-9])+\.[0-9_]*$`)
	y11Inf       = regexp.MustCompile(`^[-+]?\.(inf|Inf|INF)$`)
	y11NaN       = regexp.MustCompile(`^\.(nan|NaN|NAN)$`)
	y11Date      = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	y11Time      = regexp.MustCompile(`^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}([Tt]|[ \t]+)[0-9]{1,2}:[0-9]{2}:[0-9]{2}(\.[0-9]*)?([ \t]*(Z|[-+][0-9]{1,2}(:[0-9]{2})?))?$`)

	// yesNoWords are the YAML 1.1 boolean words refused in any casing (R1).
	yesNoWords = map[string]bool{"y": true, "n": true, "yes": true, "no": true, "on": true, "off": true}
)

func numberReading(f float64, integer bool, whole *big.Int) reading {
	return reading{kind: kindNumber, f: f, integer: integer, whole: whole}
}

func intReading(whole *big.Int) reading {
	f, _ := new(big.Float).SetInt(whole).Float64()
	return numberReading(f, true, whole)
}

func readCore(text string) reading {
	switch {
	case coreNull.MatchString(text):
		return reading{kind: kindNull}
	case coreBool.MatchString(text):
		return reading{kind: kindBool, b: text[0] == 't' || text[0] == 'T'}
	case coreDec.MatchString(text):
		n, _ := new(big.Int).SetString(strings.TrimPrefix(text, "+"), 10)
		return intReading(n)
	case coreOct.MatchString(text):
		n, _ := new(big.Int).SetString(text[2:], 8)
		return intReading(n)
	case coreHex.MatchString(text):
		n, _ := new(big.Int).SetString(text[2:], 16)
		return intReading(n)
	case coreFloat.MatchString(text):
		f, err := strconv.ParseFloat(text, 64)
		r := numberReading(f, false, nil)
		r.over = err != nil && math.IsInf(f, 0)
		return r
	case coreInf.MatchString(text):
		r := numberReading(math.Inf(1), false, nil)
		if text[0] == '-' {
			r.f = math.Inf(-1)
		}
		r.special = true
		return r
	case coreNaN.MatchString(text):
		r := numberReading(math.NaN(), false, nil)
		r.special = true
		return r
	}
	return reading{kind: kindString, s: text}
}

func readY11(text string) reading {
	sign := func(s string) (string, bool) {
		switch {
		case strings.HasPrefix(s, "-"):
			return s[1:], true
		case strings.HasPrefix(s, "+"):
			return s[1:], false
		}
		return s, false
	}
	parseInt := func(digits string, base int, negative bool) reading {
		n, ok := new(big.Int).SetString(strings.ReplaceAll(digits, "_", ""), base)
		if !ok {
			return reading{kind: kindString, s: text}
		}
		if negative {
			n.Neg(n)
		}
		return intReading(n)
	}
	switch {
	case y11Bool.MatchString(text):
		return reading{kind: kindBool, b: y11True.MatchString(text)}
	case coreNull.MatchString(text):
		return reading{kind: kindNull}
	case y11Bin.MatchString(text):
		rest, negative := sign(text)
		return parseInt(rest[2:], 2, negative)
	case y11Dec.MatchString(text):
		rest, negative := sign(text)
		return parseInt(rest, 10, negative)
	case y11Oct.MatchString(text):
		rest, negative := sign(text)
		return parseInt(rest[1:], 8, negative)
	case y11Hex.MatchString(text):
		rest, negative := sign(text)
		return parseInt(rest[2:], 16, negative)
	case y11Sexa.MatchString(text):
		rest, negative := sign(text)
		n := new(big.Int)
		for _, part := range strings.Split(strings.ReplaceAll(rest, "_", ""), ":") {
			digit, _ := new(big.Int).SetString(part, 10)
			n.Mul(n, big.NewInt(60)).Add(n, digit)
		}
		if negative {
			n.Neg(n)
		}
		return intReading(n)
	case y11Float.MatchString(text), y11FloatDot.MatchString(text):
		f, err := strconv.ParseFloat(strings.ReplaceAll(text, "_", ""), 64)
		if err != nil && !math.IsInf(f, 0) {
			return reading{kind: kindString, s: text}
		}
		return numberReading(f, false, nil)
	case y11FloatSexa.MatchString(text):
		rest, negative := sign(text)
		parts := strings.Split(strings.ReplaceAll(rest, "_", ""), ":")
		value := 0.0
		for _, part := range parts {
			digit, err := strconv.ParseFloat(part, 64)
			if err != nil {
				return reading{kind: kindString, s: text}
			}
			value = value*60 + digit
		}
		if negative {
			value = -value
		}
		return numberReading(value, false, nil)
	case y11Inf.MatchString(text):
		if text[0] == '-' {
			return numberReading(math.Inf(-1), false, nil)
		}
		return numberReading(math.Inf(1), false, nil)
	case y11NaN.MatchString(text):
		return numberReading(math.NaN(), false, nil)
	case y11Date.MatchString(text), y11Time.MatchString(text):
		return reading{kind: kindTime}
	}
	return reading{kind: kindString, s: text}
}

// readV3 is yaml.v3 itself on the scalar: the decode these positions used
// before the strict mapping.
func readV3(node *yaml.Node) reading {
	// The text alone, as a plain scalar: yaml.v3 resolves it from scratch.
	plain := yaml.Node{Kind: yaml.ScalarNode, Value: node.Value}
	var v any
	if err := plain.Decode(&v); err != nil {
		return reading{kind: kindString, s: node.Value}
	}
	switch typed := v.(type) {
	case nil:
		return reading{kind: kindNull}
	case bool:
		return reading{kind: kindBool, b: typed}
	case int:
		return intReading(big.NewInt(int64(typed)))
	case int64:
		return intReading(big.NewInt(typed))
	case uint64:
		return intReading(new(big.Int).SetUint64(typed))
	case float64:
		return numberReading(typed, false, nil)
	case time.Time:
		return reading{kind: kindTime}
	case string:
		return reading{kind: kindString, s: typed}
	}
	return reading{kind: kindString, s: fmt.Sprint(v)}
}

// maxExactInteger is the largest integer every JSON number holds exactly.
var maxExactInteger = new(big.Int).Lsh(big.NewInt(1), 53)

// decide applies the rule: the common value when the readers agree and it is
// representable, otherwise the first matching refusal, worded for text.
func (r readings) decide(text string) (any, string) {
	core, y11, v3 := r.core, r.y11, r.v3
	q := quoteFor(text)
	switch {
	case (y11.kind == kindBool && core.kind == kindString) || yesNoWords[strings.ToLower(text)]:
		return nil, fmt.Sprintf("%s is a boolean to YAML 1.1 readers (yes, no, on, off, y and n, in any casing) and a string to YAML 1.2 readers; write true or false for a boolean, or quote it (%s) for the string", text, q)
	case y11.kind == kindTime || v3.kind == kindTime:
		return nil, fmt.Sprintf("%s is a timestamp to some YAML readers, and JSON has no timestamp; quote it (%s) for the string", text, q)
	case core.kind == kindNumber && core.special:
		return nil, fmt.Sprintf("%s is not a number JSON can hold; quote it (%s) for the string", text, q)
	case core.kind == kindNumber && core.over:
		return nil, fmt.Sprintf("%s is out of range for a JSON number; quote it (%s) for the string", text, q)
	}
	agree := core.kind == y11.kind && y11.kind == v3.kind
	if agree {
		switch core.kind {
		case kindNull:
			return nil, ""
		case kindBool:
			if core.b == y11.b && y11.b == v3.b {
				return core.b, ""
			}
		case kindString:
			if v3.s == text {
				return text, ""
			}
		case kindNumber:
			if core.f == y11.f && y11.f == v3.f {
				if core.integer && core.whole != nil && new(big.Int).Abs(core.whole).Cmp(maxExactInteger) > 0 {
					return nil, fmt.Sprintf("%s is beyond ±2^53, so a JSON number cannot hold it exactly; quote it (%s) for the string", text, q)
				}
				if !keepsExactly(text, core) {
					n := numberText(core.f)
					return nil, fmt.Sprintf("%s is not kept exactly by a JSON number: it would become %s; write %s for that number, or quote it (%s) for the string", text, n, n, q)
				}
				if core.integer {
					// An integer form is its integer: -0 is 0.
					return core.f + 0, ""
				}
				return core.f, ""
			}
		}
	}
	switch {
	case core.kind == kindNumber && y11.kind == kindNumber && core.f != y11.f:
		return nil, fmt.Sprintf("%s is the number %s to YAML 1.1 readers and %s to YAML 1.2 readers; write the number without a leading zero, or quote it (%s) for the string",
			text, numberText(y11.f), numberText(core.f), q)
	case y11.kind == kindNumber && core.kind == kindString:
		return nil, fmt.Sprintf("%s is the number %s to YAML 1.1 readers and a string to YAML 1.2 readers; write %s for the number, or quote it (%s) for the string",
			text, numberText(y11.f), numberText(y11.f), q)
	case core.kind == kindNumber && y11.kind == kindString:
		return nil, fmt.Sprintf("%s is the number %s to YAML 1.2 readers and a string to YAML 1.1 readers; write %s for the number, or quote it (%s) for the string",
			text, numberText(core.f), numberText(core.f), q)
	case v3.kind == kindNumber:
		return nil, fmt.Sprintf("%s was the number %s to earlier OrkStack releases and is a string to YAML 1.2 readers; write %s for the number, or quote it (%s) for the string",
			text, numberText(v3.f), numberText(v3.f), q)
	}
	return nil, fmt.Sprintf("%s is read differently by YAML readers (%s to YAML 1.2, %s to YAML 1.1, %s to earlier OrkStack releases); quote it (%s) for the string",
		text, core.describe(), y11.describe(), v3.describe(), q)
}

func (r reading) describe() string {
	switch r.kind {
	case kindNull:
		return "null"
	case kindBool:
		return "the boolean " + strconv.FormatBool(r.b)
	case kindNumber:
		return "the number " + numberText(r.f)
	case kindTime:
		return "a timestamp"
	}
	return "a string"
}

// keepsExactly reports whether the JSON OrkStack writes for a number — the
// shortest text that reads back as its float64 — has the text's exact decimal
// value.
func keepsExactly(text string, core reading) bool {
	if core.integer {
		return true // bounded by 2^53 above; every such integer is exact
	}
	written, ok := exactDecimal(strconv.FormatFloat(core.f, 'g', -1, 64))
	if !ok {
		return false
	}
	authored, ok := exactDecimal(text)
	return ok && written.Cmp(authored) == 0
}

// exactDecimal is the exact rational value of a decimal or hex number's text:
// sign, digits and exponent, a leading "+" dropped, "1." and ".5" read as 1.0
// and 0.5.
func exactDecimal(text string) (*big.Rat, bool) {
	t := strings.TrimPrefix(text, "+")
	negative := strings.HasPrefix(t, "-")
	t = strings.TrimPrefix(t, "-")
	var value *big.Rat
	if strings.HasPrefix(t, "0x") {
		n, ok := new(big.Int).SetString(t[2:], 16)
		if !ok {
			return nil, false
		}
		value = new(big.Rat).SetInt(n)
	} else {
		if strings.HasPrefix(t, ".") {
			t = "0" + t
		}
		t = strings.Replace(t, ".e", ".0e", 1)
		t = strings.Replace(t, ".E", ".0E", 1)
		t = strings.TrimSuffix(t, ".")
		r, ok := new(big.Rat).SetString(t)
		if !ok {
			return nil, false
		}
		value = r
	}
	if negative {
		value.Neg(value)
	}
	return value, true
}

// numberText is the JSON spelling of a number (canonicalNumber).
func numberText(f float64) string {
	return fmt.Sprint(canonicalNumber(f))
}

// quoteFor is s single-quoted, or double-quoted with Go escaping when s holds
// a single quote.
func quoteFor(s string) string {
	if strings.Contains(s, "'") {
		return strconv.Quote(s)
	}
	return "'" + s + "'"
}

// present reports a data position the author wrote: anything but an absent key
// (the zero Node) or a bare null (`with:`, `with: ~`, `with: null` — untagged, unanchored).
func present(n yaml.Node) bool {
	bareNull := n.Kind == yaml.ScalarNode && n.Tag == "!!null" && n.Style&yaml.TaggedStyle == 0 && n.Anchor == ""
	return n.Kind != 0 && !bareNull
}

// freeForm converts a data position (2.2, "Positions"). An absent key or a bare null is absent (nil, nil).
func freeForm(position, path string, node *yaml.Node) (*structpb.Struct, error) {
	if node.Kind == 0 {
		return nil, nil
	}
	if err := structural(node, path); err != nil {
		return nil, authoredError(err)
	}
	if !present(*node) {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, validationError(path, fmt.Errorf("line %d: %s must be a mapping of key: value, got %s", node.Line, position, DescribeNode(node)))
	}
	value, err := AuthoredJSON(node, path)
	if err != nil {
		return nil, authoredError(err)
	}
	out, err := structpb.NewStruct(value.(map[string]any))
	if err != nil {
		return nil, validationError(path, fmt.Errorf("line %d: %s: %w", node.Line, position, err))
	}
	return out, nil
}

func authoredError(err error) error {
	if e, ok := err.(*AuthoredDataError); ok {
		return validationError(e.Path, fmt.Errorf("line %d: %s", e.Line, e.Reason))
	}
	return err
}

// AuthoredNode renders a JSON value as the YAML node AuthoredJSON reads back to
// the same value: plain where the plain text reads back as itself, quoted
// otherwise, mappings with their keys sorted.
func AuthoredNode(v any) *yaml.Node {
	switch typed := v.(type) {
	// A null, a boolean and a number carry no tag: the encoder writes the
	// plain text, and every reader resolves it to the value (a tag here would
	// be written out whenever the text resolves to another one, as an
	// integral !!float does).
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Value: "null"}
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Value: strconv.FormatBool(typed)}
	case float64:
		return &yaml.Node{Kind: yaml.ScalarNode, Value: authoredNumber(typed)}
	case string:
		node := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: typed}
		if !plainReadsAs(typed) {
			node.Style = yaml.DoubleQuotedStyle
		}
		return node
	case []any:
		node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		if len(typed) == 0 {
			node.Style = yaml.FlowStyle
		}
		for _, item := range typed {
			node.Content = append(node.Content, AuthoredNode(item))
		}
		return node
	case map[string]any:
		node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		if len(typed) == 0 {
			node.Style = yaml.FlowStyle
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
			if !plainKeyReadsAs(key) {
				keyNode.Style = yaml.DoubleQuotedStyle
			}
			node.Content = append(node.Content, keyNode, AuthoredNode(typed[key]))
		}
		return node
	}
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: fmt.Sprint(v), Style: yaml.DoubleQuotedStyle}
}

// authoredNumber is a number's text that every reader reads as that number.
func authoredNumber(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) <= float64(1<<53) {
		if f == 0 {
			if math.Signbit(f) {
				return "-0.0"
			}
			return "0"
		}
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	if f != math.Trunc(f) && math.Abs(f) >= 1e-6 && math.Abs(f) < 1e21 {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	text := strconv.FormatFloat(f, 'e', -1, 64)
	if mantissa, exponent, ok := strings.Cut(text, "e"); ok && !strings.Contains(mantissa, ".") {
		text = mantissa + ".0e" + exponent
	}
	return text
}

// plainText matches what is written plain at all: printable text with no
// leading or trailing space. Anything else is double-quoted, so the encoder
// never chooses a block style (which, for a key, yaml.v3 cannot read back).
var plainText = regexp.MustCompile(`^[^\s\x00-\x1f\x7f\x{85}\x{2028}\x{2029}\x{feff}]([^\x00-\x1f\x7f\x{85}\x{2028}\x{2029}\x{feff}]*[^\s\x00-\x1f\x7f\x{85}\x{2028}\x{2029}\x{feff}])?$`)

// plainReadsAs reports whether s, written plain, reads back as the string s.
func plainReadsAs(s string) bool {
	if !plainText.MatchString(s) || s == "<<" {
		return false
	}
	node := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
	value, reason := plainScalar(node)
	if reason != "" {
		return false
	}
	text, ok := value.(string)
	return ok && text == s
}

func plainKeyReadsAs(s string) bool {
	if !plainText.MatchString(s) || s == "<<" {
		return false
	}
	key, err := authoredKey(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}, "")
	return err == nil && key == s
}
