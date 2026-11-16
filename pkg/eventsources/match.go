package eventsources

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestResult is one test of a source's filter, as it was answered.
type TestResult struct {
	Index    int
	Selector string
	Held     bool
	// Selected is the value the selector selected, as JSON text; "" when it
	// selected nothing.
	Selected string
}

// selectorText names what a test or a mapping reads.
func selectorText(attribute, pointer string, field orkstackv1.MessageField) string {
	switch {
	case attribute != "":
		return "attribute:" + attribute
	case field != orkstackv1.MessageField_MESSAGE_FIELD_UNSPECIFIED:
		return "field:" + fieldName(field)
	default:
		return "pointer:" + pointer
	}
}

// fieldName is a MessageField as a source document spells it.
func fieldName(f orkstackv1.MessageField) string {
	return strings.ToLower(strings.TrimPrefix(f.String(), "MESSAGE_FIELD_"))
}

// runTests answers every test of match, in order. A test never fails: a
// selector that selects nothing is a value the test answers.
func runTests(match []*orkstackv1.MatchRule, msg Message, sel Selected) []TestResult {
	out := make([]TestResult, 0, len(match))
	for i, rule := range match {
		raw, ok := selectValue(rule.GetAttribute(), rule.GetPointer(), rule.GetField(), msg, sel)
		res := TestResult{Index: i, Selector: selectorText(rule.GetAttribute(), rule.GetPointer(), rule.GetField())}
		if ok {
			res.Selected = string(raw)
		}
		switch {
		case rule.Exists != nil:
			res.Held = ok == rule.GetExists()
		case rule.GetEquals() != nil:
			res.Held = ok && equalJSON(raw, rule.GetEquals())
		default:
			for _, lit := range rule.GetIn() {
				if ok && equalJSON(raw, lit) {
					res.Held = true
					break
				}
			}
		}
		out = append(out, res)
	}
	return out
}

// equalJSON reports whether the selected JSON text is the literal scalar
// (docs/EVENT_SOURCES.md, "What equal means"): the same JSON type; a string
// by code point after JSON's escapes are undone, with no normalization; a
// number by its mathematical value, compared as exact decimals and never
// through a float; true, false and null by the literal. An object or an
// array equals nothing.
func equalJSON(raw []byte, lit *structpb.Value) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false
	}
	switch k := lit.GetKind().(type) {
	case *structpb.Value_StringValue:
		s, ok := strconvUnquote(raw)
		return ok && s == k.StringValue
	case *structpb.Value_NumberValue:
		if c := raw[0]; c != '-' && (c < '0' || c > '9') {
			return false
		}
		return equalNumbers(string(raw), strconv.FormatFloat(k.NumberValue, 'g', -1, 64))
	case *structpb.Value_BoolValue:
		if k.BoolValue {
			return string(raw) == "true"
		}
		return string(raw) == "false"
	case *structpb.Value_NullValue:
		return string(raw) == "null"
	}
	return false
}

// decimal is a number reduced to a sign, its significant digits with
// leading and trailing zeros dropped, and an exponent: the value is
// 0.digits × 10^exp. Zero has no digits and no sign.
type decimal struct {
	neg    bool
	digits string
	exp    *big.Int
}

// parseDecimal reduces a JSON number's text. Its cost is one read of the
// text: no exponent is ever expanded into digits.
func parseDecimal(s string) (decimal, bool) {
	var d decimal
	if strings.HasPrefix(s, "-") {
		d.neg = true
		s = s[1:]
	}
	mant, expText := s, ""
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mant, expText = s[:i], s[i+1:]
	}
	intPart, frac := mant, ""
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		intPart, frac = mant[:i], mant[i+1:]
	}
	if intPart == "" || !allDigits(intPart) || !allDigits(frac) {
		return decimal{}, false
	}
	digits := intPart + frac
	point := len(intPart)
	lead := len(digits) - len(strings.TrimLeft(digits, "0"))
	digits = digits[lead:]
	point -= lead
	digits = strings.TrimRight(digits, "0")
	if digits == "" {
		return decimal{}, true
	}
	d.digits = digits
	d.exp = big.NewInt(int64(point))
	if expText != "" {
		e, ok := new(big.Int).SetString(strings.TrimPrefix(expText, "+"), 10)
		if !ok {
			return decimal{}, false
		}
		d.exp.Add(d.exp, e)
	}
	return d, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// equalNumbers compares two JSON numbers' texts by value: by sign, then by
// exponent, then digit by digit.
func equalNumbers(a, b string) bool {
	x, okA := parseDecimal(a)
	y, okB := parseDecimal(b)
	if !okA || !okB {
		return false
	}
	if x.digits == "" || y.digits == "" {
		return x.digits == "" && y.digits == ""
	}
	return x.neg == y.neg && x.exp.Cmp(y.exp) == 0 && x.digits == y.digits
}

// firstFailing names the first test that did not hold.
func firstFailing(tests []TestResult) (TestResult, bool) {
	for _, t := range tests {
		if !t.Held {
			return t, true
		}
	}
	return TestResult{}, false
}

func describeTest(t TestResult) string {
	return fmt.Sprintf("match[%d] (%s) did not hold", t.Index, t.Selector)
}
