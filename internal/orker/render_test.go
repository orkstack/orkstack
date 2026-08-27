package orker

import (
	"context"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func testRenderer(inputs map[string]any, values map[string]map[string]any, artifacts map[string][]*orkstackv1.ArtifactRef, paths map[string]string, allowlist map[string]struct{}) *renderer {
	return newRenderer("task", inputs, values, artifacts, paths, allowlist)
}

func TestRenderer_ResolvesRunInputReference(t *testing.T) {
	r := testRenderer(map[string]any{"url": "http://example.test/data"}, nil, nil, nil, nil)
	got, err := r.render(context.Background(), map[string]any{"url": "{{ .inputs.url }}"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["url"] != "http://example.test/data" {
		t.Fatalf("rendered url = %v, want the run input value", got["url"])
	}
}

func TestRenderer_ResolvesUpstreamTaskOutputField(t *testing.T) {
	r := testRenderer(nil, map[string]map[string]any{
		"transform": {"stdout": "hello world"},
	}, nil, nil, nil)
	got, err := r.render(context.Background(), map[string]any{
		"content": "value: {{ .tasks.transform.output.stdout }}",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["content"] != "value: hello world" {
		t.Fatalf("rendered content = %v, want the upstream stdout", got["content"])
	}
}

func TestRenderer_ArtifactHelperReturnsStagedContainerPath(t *testing.T) {
	refs := map[string][]*orkstackv1.ArtifactRef{
		"fetch": {{Name: "body", Uri: "s3://bucket/body", Sha256: strings.Repeat("a", 64)}},
	}
	paths := map[string]string{"fetch/body": "/orkstack/input/artifacts/fetch/body"}
	r := testRenderer(nil, nil, refs, paths, nil)
	got, err := r.render(context.Background(), map[string]any{
		"command": "cat {{ sh (artifact \"fetch/body\") }}",
		"again":   "{{ artifact \"fetch/body\" }}",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["command"] != "cat '/orkstack/input/artifacts/fetch/body'" {
		t.Fatalf("command = %q", got["command"])
	}
	if got["again"] != paths["fetch/body"] {
		t.Fatalf("second reference = %v, want %q", got["again"], paths["fetch/body"])
	}
}

func TestRenderer_UnknownReferencesFailWithClearErrors(t *testing.T) {
	r := testRenderer(map[string]any{}, map[string]map[string]any{
		"fetch": {"status": float64(200)},
	}, nil, map[string]string{"fetch/body": "/orkstack/input/artifacts/fetch/body"}, nil)
	cases := []struct {
		name     string
		with     map[string]any
		contains []string
	}{
		{"undeclared input", map[string]any{"url": "{{ .inputs.missing }}"}, []string{"task", "with.url", "missing"}},
		{"unknown task", map[string]any{"c": "{{ .tasks.ghost.output.stdout }}"}, []string{"with.c", "ghost"}},
		{"unknown output", map[string]any{"c": "{{ .tasks.fetch.output.nope }}"}, []string{"with.c", "nope"}},
		{"unknown root", map[string]any{"c": "{{ .secrets.token }}"}, []string{"with.c", "secrets"}},
		{"unstaged artifact", map[string]any{"c": "{{ artifact \"ghost/body\" }}"}, []string{"ghost/body", "not an input", "fetch/body"}},
		{"unparsable", map[string]any{"c": "{{ .inputs.url "}, []string{"template parse error"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := r.render(context.Background(), tc.with)
			if err == nil {
				t.Fatalf("render(%v) succeeded, want an error", tc.with)
			}
			for _, want := range tc.contains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

func TestRenderer_NilUpstreamValueFailsRatherThanRenderingNoValue(t *testing.T) {
	r := testRenderer(nil, map[string]map[string]any{"fetch": {"json": nil}}, nil, nil, nil)
	_, err := r.render(context.Background(), map[string]any{"c": "{{ .tasks.fetch.output.json }}"})
	if err == nil || !strings.Contains(err.Error(), noValueMarker) {
		t.Fatalf("error = %v, want it to mention %s", err, noValueMarker)
	}
}

func TestRenderer_PreservesValuesThatAreNotTemplates(t *testing.T) {
	r := testRenderer(map[string]any{"n": float64(2)}, nil, nil, nil, nil)
	with := map[string]any{
		"plain": "  keep {me} \n verbatim  ", "number": float64(3), "boolean": true, "null": nil,
		"list":   []any{"a", "{{ .inputs.n }}", float64(1)},
		"nested": map[string]any{"{{ raw key }}": "{{ .inputs.n }}"},
	}
	got, err := r.render(context.Background(), with)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := map[string]any{
		"plain": "  keep {me} \n verbatim  ", "number": float64(3), "boolean": true, "null": nil,
		"list":   []any{"a", "2", float64(1)},
		"nested": map[string]any{"{{ raw key }}": "2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rendered = %#v, want %#v", got, want)
	}
	if with["list"].([]any)[1] != "{{ .inputs.n }}" {
		t.Fatal("render mutated the input config")
	}
}

func TestRenderer_EnvHelperHonorsTheAllowlist(t *testing.T) {
	t.Setenv("ORK_TEST_TOKEN", "s3cret")
	t.Setenv("ORK_TEST_OTHER", "nope")
	r := testRenderer(nil, nil, nil, nil, parseAllowlist(" ORK_TEST_TOKEN , "))
	got, err := r.render(context.Background(), map[string]any{"c": "{{ env \"ORK_TEST_TOKEN\" }}"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["c"] != "s3cret" {
		t.Fatalf("rendered env value = %v, want s3cret", got["c"])
	}
	_, err = r.render(context.Background(), map[string]any{"c": "{{ env \"ORK_TEST_OTHER\" }}"})
	if err == nil || !strings.Contains(err.Error(), EnvAllowlistVar) {
		t.Fatalf("error = %v, want refusal naming %s", err, EnvAllowlistVar)
	}
}

func TestRenderer_ShHelperQuotesUntrustedTextAsOneShellArgument(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"single quote", "it's data"},
		{"embedded quotes", "a'b'c"},
		{"command substitution", "$(printf INJECTED)"},
		{"backticks", "`printf INJECTED`"},
		{"semicolon", "value; printf INJECTED"},
		{"newline", "first\nprintf INJECTED"},
		{"leading dash", "-n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := testRenderer(map[string]any{"value": tc.value}, nil, nil, nil, nil)
			got, err := r.render(context.Background(), map[string]any{"quoted": "{{ sh .inputs.value }}"})
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			quoted := got["quoted"].(string)
			cmd := exec.Command("/bin/sh", "-c", `set -- `+quoted+`; [ "$#" -eq 1 ] || exit 91; printf '%s' "$1"`)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("shell rejected quote %q: %v: %s", quoted, err, out)
			}
			if string(out) != tc.value {
				t.Fatalf("shell decoded %q as %q, want byte-identical data %q", quoted, out, tc.value)
			}
		})
	}
}

func TestSafeComponent_SanitizesArtifactNamesIntoOneSafeComponent(t *testing.T) {
	for _, name := range []string{"../../evil name", "..", "a/b", "☃"} {
		t.Run(name, func(t *testing.T) {
			component := safeComponent(name)
			if component == "" || component == "." || component == ".." || strings.ContainsAny(component, `/\\`) {
				t.Fatalf("safeComponent(%q) = %q, not one safe path component", name, component)
			}
			root := t.TempDir()
			path := filepath.Join(root, component)
			rel, err := filepath.Rel(root, path)
			if err != nil || rel != component {
				t.Fatalf("component %q escaped root %q (relative path %q)", component, root, rel)
			}
		})
	}
}
