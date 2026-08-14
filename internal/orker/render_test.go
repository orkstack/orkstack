package orker

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/plugin"
)

// newTestStore builds a local-filesystem artifact store rooted in a temp dir.
func newTestStore(t *testing.T) artifacts.Store {
	t.Helper()
	store, err := artifacts.New(artifacts.Config{
		Backend:  artifacts.BackendLocal,
		LocalDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new artifact store: %v", err)
	}
	return store
}

// putArtifact stores bytes and returns the ref an upstream task would report.
func putArtifact(t *testing.T, store artifacts.Store, name string, data []byte) plugin.ArtifactRef {
	t.Helper()
	ref, err := store.Put(context.Background(), name, bytes.NewReader(data), artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("put artifact %q: %v", name, err)
	}
	return ref
}

func TestRenderer_ResolvesRunInputReference(t *testing.T) {
	r := newRenderer("fetch", plugin.Payload{"url": "http://example.test/data"}, nil, nil, nil)

	got, err := r.render(context.Background(), map[string]any{"url": "{{ .inputs.url }}"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["url"] != "http://example.test/data" {
		t.Fatalf("rendered url = %v, want the run input value", got["url"])
	}
}

func TestRenderer_ResolvesUpstreamTaskOutputField(t *testing.T) {
	upstream := map[string]task.UpstreamResult{
		"transform": {Output: plugin.Payload{"stdout": "hello world"}},
	}
	r := newRenderer("save", nil, upstream, nil, nil)

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

func TestRenderer_ArtifactHelperMaterializesUpstreamArtifactToLocalFile(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("{\"slideshow\":\"data\"}\x00\xff no trailing newline")
	ref := putArtifact(t, store, "body", payload)

	workDir := t.TempDir()
	upstream := map[string]task.UpstreamResult{
		"fetch": {Artifacts: []plugin.ArtifactRef{ref}},
	}
	resolver := newUpstreamResolver(store, upstream, workDir)
	r := newRenderer("transform", nil, upstream, resolver, nil)

	got, err := r.render(context.Background(), map[string]any{
		"command": "cat \"{{ artifact \"fetch/body\" }}\"",
		"again":   "{{ artifact \"fetch/body\" }}",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	command, _ := got["command"].(string)
	path := strings.TrimSuffix(strings.TrimPrefix(command, "cat \""), "\"")
	if !filepath.IsAbs(path) {
		t.Fatalf("materialized path %q is not absolute", path)
	}
	if rel, err := filepath.Rel(workDir, path); err != nil || strings.HasPrefix(rel, "..") {
		t.Fatalf("materialized path %q is outside the workdir %q", path, workDir)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read materialized artifact: %v", err)
	}
	if !bytes.Equal(contents, payload) {
		t.Fatalf("materialized contents = %q, want the stored bytes %q", contents, payload)
	}
	if got["again"] != path {
		t.Fatalf("second reference resolved to %v, want the same path %q", got["again"], path)
	}
}

func TestRenderer_UnknownReferencesFailWithClearErrors(t *testing.T) {
	store := newTestStore(t)
	ref := putArtifact(t, store, "body", []byte("payload"))
	upstream := map[string]task.UpstreamResult{
		"fetch": {
			Output:    plugin.Payload{"status": float64(200)},
			Artifacts: []plugin.ArtifactRef{ref},
		},
	}

	cases := []struct {
		name     string
		with     map[string]any
		contains []string
	}{
		{
			name:     "undeclared input",
			with:     map[string]any{"url": "{{ .inputs.missing }}"},
			contains: []string{"transform", "with.url", "missing"},
		},
		{
			name:     "unknown task",
			with:     map[string]any{"c": "{{ .tasks.ghost.output.stdout }}"},
			contains: []string{"with.c", "ghost"},
		},
		{
			name:     "unknown output field",
			with:     map[string]any{"c": "{{ .tasks.fetch.output.nope }}"},
			contains: []string{"with.c", "nope"},
		},
		{
			name:     "unknown template root",
			with:     map[string]any{"c": "{{ .secrets.token }}"},
			contains: []string{"with.c", "secrets"},
		},
		{
			name:     "artifact from unknown task",
			with:     map[string]any{"c": "{{ artifact \"ghost/body\" }}"},
			contains: []string{"ghost/body", "not an upstream"},
		},
		{
			name:     "unknown artifact name",
			with:     map[string]any{"c": "{{ artifact \"fetch/missing\" }}"},
			contains: []string{"fetch/missing", "produced no artifact"},
		},
		{
			name:     "malformed artifact reference",
			with:     map[string]any{"c": "{{ artifact \"body\" }}"},
			contains: []string{"\"body\"", "<task>/<name>"},
		},
		{
			name:     "unparsable template",
			with:     map[string]any{"c": "{{ .inputs.url "},
			contains: []string{"template parse error"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			resolver := newUpstreamResolver(store, upstream, workDir)
			r := newRenderer("transform", plugin.Payload{}, upstream, resolver, nil)

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
	upstream := map[string]task.UpstreamResult{
		"fetch": {Output: plugin.Payload{"json": nil}},
	}
	r := newRenderer("save", nil, upstream, nil, nil)

	_, err := r.render(context.Background(), map[string]any{"c": "{{ .tasks.fetch.output.json }}"})
	if err == nil {
		t.Fatal("render succeeded, want an error for a nil upstream value")
	}
	if !strings.Contains(err.Error(), noValueMarker) {
		t.Fatalf("error = %q, want it to mention %s", err, noValueMarker)
	}
}

func TestRenderer_PreservesValuesThatAreNotTemplates(t *testing.T) {
	r := newRenderer("save", plugin.Payload{"n": float64(2)}, nil, nil, nil)

	with := map[string]any{
		"plain":   "  keep {me} \n verbatim  ",
		"number":  float64(3),
		"boolean": true,
		"null":    nil,
		"list":    []any{"a", "{{ .inputs.n }}", float64(1)},
		"nested":  map[string]any{"{{ raw key }}": "{{ .inputs.n }}"},
	}
	got, err := r.render(context.Background(), with)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["plain"] != "  keep {me} \n verbatim  " {
		t.Errorf("plain string = %q, want it byte-identical", got["plain"])
	}
	if got["number"] != float64(3) || got["boolean"] != true || got["null"] != nil {
		t.Errorf("scalars were not preserved: %#v", got)
	}
	list, ok := got["list"].([]any)
	if !ok || len(list) != 3 || list[0] != "a" || list[1] != "2" || list[2] != float64(1) {
		t.Errorf("list = %#v, want templates rendered and other items preserved", got["list"])
	}
	nested, ok := got["nested"].(map[string]any)
	if !ok || nested["{{ raw key }}"] != "2" {
		t.Errorf("nested = %#v, want the key preserved and the value rendered", got["nested"])
	}
	if with["list"].([]any)[1] != "{{ .inputs.n }}" {
		t.Error("render mutated the input config")
	}
}

func TestRenderer_EnvHelperHonorsTheAllowlist(t *testing.T) {
	t.Setenv("ORK_TEST_TOKEN", "s3cret")
	t.Setenv("ORK_TEST_OTHER", "nope")

	r := newRenderer("t", nil, nil, nil, parseAllowlist(" ORK_TEST_TOKEN , "))
	got, err := r.render(context.Background(), map[string]any{"c": "{{ env \"ORK_TEST_TOKEN\" }}"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got["c"] != "s3cret" {
		t.Fatalf("rendered env value = %v, want s3cret", got["c"])
	}

	_, err = r.render(context.Background(), map[string]any{"c": "{{ env \"ORK_TEST_OTHER\" }}"})
	if err == nil {
		t.Fatal("render succeeded for a non-allowlisted variable, want an error")
	}
	if !strings.Contains(err.Error(), EnvAllowlistVar) {
		t.Fatalf("error = %q, want it to name %s", err, EnvAllowlistVar)
	}
}

func TestResolver_SanitizesArtifactNamesIntoOneSafeComponent(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("escaped")
	ref := putArtifact(t, store, "../../evil name", payload)
	workDir := t.TempDir()
	upstream := map[string]task.UpstreamResult{"fetch": {Artifacts: []plugin.ArtifactRef{ref}}}
	resolver := newUpstreamResolver(store, upstream, workDir)

	path, err := resolver.Resolve(context.Background(), "fetch/../../evil name")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	rel, err := filepath.Rel(workDir, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		t.Fatalf("resolved path %q escaped the workdir %q", path, workDir)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read materialized artifact: %v", err)
	}
	if !bytes.Equal(contents, payload) {
		t.Fatalf("contents = %q, want %q", contents, payload)
	}
}
