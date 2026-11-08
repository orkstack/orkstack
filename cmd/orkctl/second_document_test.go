package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

const secondDocumentOrkflow = "apiVersion: orkstack/v1\nkind: Orkflow\nname: a\ntasks:\n  - id: t\n    await:\n      timeout_seconds: 60\n"

// TestAuthoredFile_ASecondDocumentIsRefusedBeforeItIsSent: each command that
// reads an authored file names the file and the line of a second document,
// whatever that document holds, and sends the engine nothing.
func TestAuthoredFile_ASecondDocumentIsRefusedBeforeItIsSent(t *testing.T) {
	const automation = "apiVersion: orkstack/v1\nkind: Automation\nnamespace: x\n"
	for _, tc := range []struct {
		name   string
		source string
		line   string
		args   func(path string) []string
	}{
		{"apply", secondDocumentOrkflow + "---\n" + secondDocumentOrkflow, "line 8", func(path string) []string { return []string{"apply", path} }},
		{"apply, the second not parsing", secondDocumentOrkflow + "---\nname: [unclosed\n", "line 8", func(path string) []string { return []string{"apply", path} }},
		{"apply, the second tripping the first read", secondDocumentOrkflow + "---\n\tname: a\n", "line 8", func(path string) []string { return []string{"apply", path} }},
		{"apply, an empty second", secondDocumentOrkflow + "---\n", "line 8", func(path string) []string { return []string{"apply", path} }},
		{"apply --dry-run", secondDocumentOrkflow + "---\n" + secondDocumentOrkflow, "line 8", func(path string) []string { return []string{"apply", "--dry-run", path} }},
		{"apply of a directory", secondDocumentOrkflow + "---\n" + secondDocumentOrkflow, "line 8", func(path string) []string { return []string{"apply", filepath.Dir(path)} }},
		{"apply of an automation document", automation + "---\n" + automation, "line 4", func(path string) []string { return []string{"apply", path} }},
		{"orkflow validate", secondDocumentOrkflow + "---\nname: [unclosed\n", "line 8", func(path string) []string { return []string{"orkflow", "validate", path} }},
		{"run plan --file", secondDocumentOrkflow + "---\nname: [unclosed\n", "line 8", func(path string) []string { return []string{"run", "plan", "--file", path} }},
		{"task run --file", secondDocumentOrkflow + "---\nname: [unclosed\n", "line 8", func(path string) []string { return []string{"task", "run", "--file", path, "--task", "t"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(serverEnvVar, "")
			t.Setenv(namespaceEnvVar, "")
			t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
			// No container runtime answers, as on a runner without Docker: the
			// refusal is the file's own and must not depend on the host's daemon.
			t.Setenv("DOCKER_HOST", "unix:///nonexistent")
			var requests atomic.Int64
			engine := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
			t.Cleanup(engine.Close)

			path := filepath.Join(t.TempDir(), "two.ork.yaml")
			if err := os.WriteFile(path, []byte(tc.source), 0o644); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			code := Execute(append(tc.args(path), "--server", engine.URL), &stdout, &stderr)
			if code != exitError {
				t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitError, stdout.String(), stderr.String())
			}
			want := path + ": " + tc.line + ": one document per file: a second YAML document begins here"
			if !strings.Contains(stderr.String(), want) {
				t.Fatalf("stderr = %q, want it to hold %q", stderr.String(), want)
			}
			if got := requests.Load(); got != 0 {
				t.Fatalf("the engine got %d requests: a refused file is not sent", got)
			}
		})
	}
}

func TestActivityInterfacePack_ASecondDocumentNamesTheFileAndTheLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "interface.ork.yaml")
	const source = "apiVersion: orkstack/v1\nkind: ActivityInterface\nfields:\n  src:\n    type: string\n---\nfields: [unclosed\n"
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := packDocumentFile(path)
	want := path + ": line 6: one activity interface per file: a second YAML document begins here"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("packDocumentFile = %v, want it to hold %q", err, want)
	}
}
