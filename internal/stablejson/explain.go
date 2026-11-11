package stablejson

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
)

// Explain says, in a sentence, why encoding/json/v2 refused a document: a key
// written twice names the key and the object that holds it, a key no field
// takes names the key, text that is not UTF-8 names the string. Anything else
// is the decoder's own message, which names its place in the document. It is
// the one wording every OrkStack door uses for a JSON refusal (docs/JSON.md,
// "Text the codec will not change").
func Explain(err error) string {
	if err == nil {
		return ""
	}
	var syntactic *jsontext.SyntacticError
	if errors.As(err, &syntactic) {
		switch {
		case errors.Is(syntactic.Err, jsontext.ErrDuplicateName):
			parent, key := splitPointer(syntactic.JSONPointer)
			return fmt.Sprintf("the key %q appears twice in %s; a JSON object holds each key once", key, objectAt(parent))
		case syntactic.Err != nil && strings.Contains(syntactic.Err.Error(), "surrogate"):
			return fmt.Sprintf("the string at %s holds half of a surrogate pair, not a character", valueAt(syntactic.JSONPointer))
		case syntactic.Err != nil && strings.Contains(syntactic.Err.Error(), "invalid UTF-8"):
			return fmt.Sprintf("the string at %s is not valid UTF-8", valueAt(syntactic.JSONPointer))
		}
		return strings.TrimPrefix(err.Error(), "jsontext: ")
	}
	var semantic *json.SemanticError
	if errors.As(err, &semantic) && semantic.Err != nil && strings.Contains(semantic.Err.Error(), "unknown object member name") {
		parent, key := splitPointer(semantic.JSONPointer)
		return fmt.Sprintf("%s has the key %q, which is not one it takes (keys match exactly, case included)", objectAt(parent), key)
	}
	return strings.TrimPrefix(strings.TrimPrefix(err.Error(), "json: "), "jsontext: ")
}

// splitPointer splits a JSON Pointer into its parent and the unescaped last
// token: "/x/0/b" is ("/x/0", "b").
func splitPointer(pointer jsontext.Pointer) (jsontext.Pointer, string) {
	s := string(pointer)
	i := strings.LastIndexByte(s, '/')
	if i < 0 {
		return "", ""
	}
	token := strings.NewReplacer("~1", "/", "~0", "~").Replace(s[i+1:])
	return jsontext.Pointer(s[:i]), token
}

func objectAt(pointer jsontext.Pointer) string {
	if pointer == "" {
		return "the top-level object"
	}
	return "the object at " + string(pointer)
}

func valueAt(pointer jsontext.Pointer) string {
	if pointer == "" {
		return "the top level"
	}
	return string(pointer)
}

// Explained is err worded by Explain, still unwrapping to err, for a caller
// that wraps it with %w. It is nil when err is.
func Explained(err error) error {
	if err == nil {
		return nil
	}
	return explained{err}
}

type explained struct{ err error }

func (e explained) Error() string { return Explain(e.err) }
func (e explained) Unwrap() error { return e.err }
