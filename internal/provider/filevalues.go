package provider

import (
	"bytes"
	"encoding/json/jsontext"
	"regexp"
)

// maxFileValues is the most values of one file masked by themselves: a file
// of thousands of strings must not spend the masker's bound.
const maxFileValues = 32

// fileValues are the values a bundle's file holds, for the masker: every
// string of a JSON file, at any depth, and every value of an INI file, each
// once; none is empty. A file that is neither holds none of its own: it is
// masked whole and by its lines. whole is false when it holds more than
// maxFileValues, and the rest are left out.
func fileValues(content []byte) (values [][]byte, whole bool) {
	content = bytes.TrimPrefix(content, []byte("\xef\xbb\xbf"))
	found, ok := jsonStrings(content)
	if !ok {
		found = iniValues(content)
	}
	seen := make(map[string]bool, len(found))
	for _, v := range found {
		if len(v) == 0 || seen[string(v)] {
			continue
		}
		if len(values) == maxFileValues {
			return values, false
		}
		seen[string(v)] = true
		values = append(values, v)
	}
	return values, true
}

// jsonStrings are content's strings, when it is one JSON object or array. It
// is a reader for masking alone, so what a strict reader refuses (a key
// written twice) is masked rather than skipped.
func jsonStrings(content []byte) ([][]byte, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(content), jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	switch dec.PeekKind() {
	case '{', '[':
	default:
		return nil, false
	}
	var out [][]byte
	for {
		// A name is not a value: it is told by where the decoder stands, an
		// object's even offset.
		kind, length := dec.StackIndex(dec.StackDepth())
		token, err := dec.ReadToken()
		if err != nil {
			return nil, false
		}
		if token.Kind() == '"' && (kind != '{' || length%2 == 1) {
			out = append(out, []byte(token.String()))
		}
		if dec.StackDepth() == 0 {
			break
		}
	}
	if len(bytes.TrimSpace(content[dec.InputOffset():])) > 0 {
		return nil, false
	}
	return out, true
}

// iniKey is a key as INI, .env and properties files write one.
var iniKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_. -]*$`)

// iniValues are the values of an INI file, one that has a [section], or of a
// file every line of which is blank, a comment or a key = value (a .env
// file). In an INI file a line that is neither is skipped. Readers of INI
// differ on quotes and on a comment after a value, so a value written with
// either is there as it stands and as each of them reads it.
func iniValues(content []byte) [][]byte {
	var out [][]byte
	section, other := false, false
	for line := range bytes.Lines(content) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' || line[0] == ';' {
			continue
		}
		if line[0] == '[' {
			section = true
			continue
		}
		key, value, ok := bytes.Cut(line, []byte("="))
		if !ok || !iniKey.Match(bytes.TrimSpace(key)) {
			other = true
			continue
		}
		value = bytes.TrimSpace(value)
		out = append(out, bytes.Clone(value), unquoted(value))
		for _, mark := range []string{" #", " ;", "\t#", "\t;"} {
			if i := bytes.Index(value, []byte(mark)); i >= 0 {
				bare := bytes.TrimSpace(value[:i])
				out = append(out, bytes.Clone(bare), unquoted(bare))
			}
		}
	}
	if !section && other {
		return nil
	}
	return out
}

// unquoted is value without the quotes around it.
func unquoted(value []byte) []byte {
	if n := len(value); n >= 2 && (value[0] == '"' || value[0] == '\'') && value[n-1] == value[0] {
		return bytes.Clone(value[1 : n-1])
	}
	return bytes.Clone(value)
}
