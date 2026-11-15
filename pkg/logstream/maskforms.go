package logstream

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/url"
	"strings"
	"unicode/utf8"
)

// The spellings of a secret a masker looks for (MaskForms), and their limits.
const (
	// MaskMinFormBytes is the shortest form registered: a shorter one would
	// mask ordinary words.
	MaskMinFormBytes = 8
	// maskLongValue is the length from which a value takes only its raw and
	// unpadded base64 forms; the padded forms contain the unpadded ones.
	maskLongValue = 256
	// maskLinesOnly is the length above which a value is registered by its
	// lines only.
	maskLinesOnly = 64 << 10
	// maskCoreMin is the shortest value whose base64 cores — the part of its
	// encoding that is the same wherever it sits inside a longer string — are
	// registered.
	maskCoreMin = 12
)

// formKind says how a form spells its value; the fuzz targets read it.
type formKind uint8

const (
	formRaw formKind = iota
	formLine
	formPEMBody
	formBase64Std
	formBase64StdRaw
	formBase64URL
	formBase64URLRaw
	formBase64Core
	formPercentPlus
	formPercent20
	formJSON
	formJSONHTML
	formHexLower
	formHexUpper
)

type form struct {
	kind formKind
	// align is a base64 core's alignment (0, 1 or 2); url says its alphabet.
	align int
	url   bool
	bytes []byte
}

// MaskForms returns the spellings of value a masker masks: the value, its
// base64 (standard and
// URL-safe, padded and not), for a value of 12 bytes or more the part of its
// base64 that is the same at each of the three alignments, its
// percent-encodings, its JSON-string escaping where that differs, its hex in
// both cases, and, for a value of several lines, each line separately; for a
// PEM block, its body lines and its body joined, never its armour lines.
// A value of 256 bytes or more takes only its raw and unpadded base64 forms,
// one over 64 KiB only its lines, so the set stays within its bounds: such a
// value is not caught in hex, percent-encoded, JSON-escaped, or base64-encoded
// as part of a longer string. A value shorter than MaskMinFormBytes has
// no form at all; no form is shorter than that or holds a line terminator,
// and none repeats.
func MaskForms(value []byte) [][]byte {
	forms := maskForms(value)
	out := make([][]byte, len(forms))
	for i, f := range forms {
		out[i] = f.bytes
	}
	return out
}

func maskForms(value []byte) []form {
	if len(value) < MaskMinFormBytes {
		return nil // under 8 bytes nothing is masked, in any spelling
	}
	var out []form
	seen := map[string]bool{}
	add := func(f form) {
		if len(f.bytes) < MaskMinFormBytes || bytes.ContainsAny(f.bytes, "\r\n") || seen[string(f.bytes)] {
			return
		}
		seen[string(f.bytes)] = true
		out = append(out, f)
	}
	lines := splitLines(value)
	multiline := bytes.ContainsAny(value, "\r\n")
	if multiline {
		body, pem := pemBody(lines)
		for _, line := range lines {
			if pem && isArmour(line) {
				continue
			}
			add(form{kind: formLine, bytes: line})
			if trimmed := bytes.TrimSpace(line); len(trimmed) != len(line) {
				add(form{kind: formLine, bytes: trimmed})
			}
		}
		if pem {
			add(form{kind: formPEMBody, bytes: body})
		}
	}
	if len(value) > maskLinesOnly {
		if !multiline {
			add(form{kind: formLine, bytes: value})
		}
		return out
	}
	if !multiline {
		add(form{kind: formRaw, bytes: value})
	}
	if len(value) >= maskLongValue {
		add(form{kind: formBase64StdRaw, bytes: []byte(base64.RawStdEncoding.EncodeToString(value))})
		add(form{kind: formBase64URLRaw, bytes: []byte(base64.RawURLEncoding.EncodeToString(value))})
		return out
	}
	add(form{kind: formBase64Std, bytes: []byte(base64.StdEncoding.EncodeToString(value))})
	add(form{kind: formBase64StdRaw, bytes: []byte(base64.RawStdEncoding.EncodeToString(value))})
	add(form{kind: formBase64URL, bytes: []byte(base64.URLEncoding.EncodeToString(value))})
	add(form{kind: formBase64URLRaw, bytes: []byte(base64.RawURLEncoding.EncodeToString(value))})
	if len(value) >= maskCoreMin {
		for _, enc := range []struct {
			e   *base64.Encoding
			url bool
		}{{base64.RawStdEncoding, false}, {base64.RawURLEncoding, true}} {
			for align := 0; align < 3; align++ {
				add(form{kind: formBase64Core, align: align, url: enc.url, bytes: base64Core(enc.e, value, align)})
			}
		}
	}
	if plus := url.QueryEscape(string(value)); plus != string(value) {
		add(form{kind: formPercentPlus, bytes: []byte(plus)})
		add(form{kind: formPercent20, bytes: []byte(strings.ReplaceAll(plus, "+", "%20"))})
	}
	if utf8.Valid(value) {
		for _, html := range []bool{false, true} {
			quoted, err := json.Marshal(string(value), jsontext.EscapeForHTML(html))
			if err == nil && len(quoted) >= 2 {
				escaped := quoted[1 : len(quoted)-1]
				if !bytes.Equal(escaped, value) {
					kind := formJSON
					if html {
						kind = formJSONHTML
					}
					add(form{kind: kind, bytes: escaped})
				}
			}
		}
	}
	lower := hex.EncodeToString(value)
	add(form{kind: formHexLower, bytes: []byte(lower)})
	add(form{kind: formHexUpper, bytes: []byte(strings.ToUpper(lower))})
	return out
}

// base64Core is the part of value's base64 that does not depend on what
// precedes or follows it, when it starts align bytes into a group of three:
// the characters whose six bits all come from value.
func base64Core(enc *base64.Encoding, value []byte, align int) []byte {
	padded := make([]byte, align+len(value))
	copy(padded[align:], value)
	encoded := enc.EncodeToString(padded)
	startBit, endBit := 8*align, 8*(align+len(value))
	first := (startBit + 5) / 6
	last := endBit / 6 // exclusive: the first character that reaches past value
	if first >= last || last > len(encoded) {
		return nil
	}
	return []byte(encoded[first:last])
}

// splitLines splits value at each "\n", "\r\n" or "\r", dropping empty lines.
func splitLines(value []byte) [][]byte {
	var lines [][]byte
	for len(value) > 0 {
		i := bytes.IndexAny(value, "\r\n")
		if i < 0 {
			lines = append(lines, value)
			break
		}
		if i > 0 {
			lines = append(lines, value[:i])
		}
		value = value[i+1:]
	}
	return lines
}

// isArmour reports whether line is a PEM armour line: "-----BEGIN …-----"
// or "-----END …-----".
func isArmour(line []byte) bool {
	s := strings.TrimSpace(string(line))
	return strings.HasSuffix(s, "-----") && (strings.HasPrefix(s, "-----BEGIN ") || strings.HasPrefix(s, "-----END "))
}

// pemBody returns the body lines of a PEM value joined, and whether value is
// PEM: it holds a BEGIN armour line and a later END one. Header lines inside
// the block ("Proc-Type: …") are body lines like any other: they rarely
// matter and masking them costs nothing.
func pemBody(lines [][]byte) ([]byte, bool) {
	var body []byte
	inside, seen := false, false
	for _, line := range lines {
		switch s := strings.TrimSpace(string(line)); {
		case isArmour(line) && strings.HasPrefix(s, "-----BEGIN "):
			inside = true
		case isArmour(line):
			if inside {
				seen = true
			}
			inside = false
		case inside:
			body = append(body, bytes.TrimSpace(line)...)
		}
	}
	return body, seen
}
