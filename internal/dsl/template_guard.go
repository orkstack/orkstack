package dsl

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"text/template"
)

// NoValueMarker is what text/template prints for a value that is present and
// nil. Rendering it would silently corrupt a command or a URL, so a rendered
// string that holds it is an error. The same four-and-six characters are also
// what a user's data, or an author's literal text, may legitimately spell, and
// those must render as themselves.
const NoValueMarker = "<no value>"

// noValueMask has the length of NoValueMarker and cannot be told apart from it
// by any template function that measures or slices text. It stands in for the
// marker, in the template's text and in every string it can read, while the
// template is run a second time to tell a nil value from data that spells the
// marker.
const noValueMask = "<no\x01value>"

// Stage names where a TemplateError arose.
const (
	StageParse  = "parse"
	StageRender = "render"
)

// ErrNoValue is the render-stage error for a template that printed a nil
// value.
var ErrNoValue = errors.New("template resolved to " + NoValueMarker)

// TemplateError is a template that did not render. Stage says whether it did
// not parse or did not execute; Err is the cause, ErrNoValue for a nil value.
type TemplateError struct {
	Stage string
	Err   error
}

func (e *TemplateError) Error() string { return e.Stage + ": " + e.Err.Error() }
func (e *TemplateError) Unwrap() error { return e.Err }

// ExecuteChecked builds the template for text, runs it against data and
// returns what it printed, refusing a nil value. build parses text with the
// caller's options and functions; the strings its functions return pass through
// shield, which the second pass uses to mask the marker in them too.
//
// The marker is looked for in the output, so it can arise three ways: a nil
// value (an error), a string in data that spells it, or text in the template
// that does (neither is). Only when the output holds it is the template run
// again with the marker masked in the text and in data; if the marker is still
// there it came from a nil value, and if not, the mask is put back. A template
// or data that already holds the mask itself is refused rather than guessed
// at.
func ExecuteChecked(text string, data map[string]any, build func(text string, shield func(string) string) (*template.Template, error)) (string, error) {
	identity := func(s string) string { return s }
	out, err := runTemplate(text, data, build, identity)
	if err != nil || !strings.Contains(out, NoValueMarker) {
		return out, err
	}
	if strings.Contains(text, noValueMask) || holdsString(data, noValueMask) {
		return "", &TemplateError{Stage: StageRender, Err: fmt.Errorf("the template or its data holds %q, which is reserved while a %s is told from a nil value", noValueMask, NoValueMarker)}
	}
	shield := func(s string) string { return strings.ReplaceAll(s, NoValueMarker, noValueMask) }
	masked, err := runTemplate(shield(text), maskStrings(data, shield).(map[string]any), build, shield)
	if err != nil {
		return "", err
	}
	if strings.Contains(masked, NoValueMarker) {
		return "", &TemplateError{Stage: StageRender, Err: ErrNoValue}
	}
	return strings.ReplaceAll(masked, noValueMask, NoValueMarker), nil
}

func runTemplate(text string, data map[string]any, build func(string, func(string) string) (*template.Template, error), shield func(string) string) (string, error) {
	tmpl, err := build(text, shield)
	if err != nil {
		return "", &TemplateError{Stage: StageParse, Err: err}
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", &TemplateError{Stage: StageRender, Err: err}
	}
	return buf.String(), nil
}

// maskStrings copies a tree of maps, slices and strings with shield applied to
// every string, key and value alike; any other value is kept.
func maskStrings(value any, shield func(string) string) any {
	switch typed := value.(type) {
	case string:
		return shield(typed)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, v := range typed {
			out[shield(k)] = maskStrings(v, shield)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, v := range typed {
			out[i] = maskStrings(v, shield)
		}
		return out
	default:
		return value
	}
}

// holdsString reports whether any string, key or value in the tree contains
// needle.
func holdsString(value any, needle string) bool {
	switch typed := value.(type) {
	case string:
		return strings.Contains(typed, needle)
	case map[string]any:
		for k, v := range typed {
			if strings.Contains(k, needle) || holdsString(v, needle) {
				return true
			}
		}
	case []any:
		for _, v := range typed {
			if holdsString(v, needle) {
				return true
			}
		}
	}
	return false
}
