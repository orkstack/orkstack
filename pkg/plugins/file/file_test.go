package fileplugin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/plugin"
)

func execContext(t *testing.T, config map[string]any) *plugin.ExecContext {
	t.Helper()
	return &plugin.ExecContext{
		TaskID:  "save",
		Config:  config,
		WorkDir: t.TempDir(),
		Log:     func(string, ...any) {},
	}
}

func TestFilePlugin_WriteCreatesTheFileAndProducesAnArtifact(t *testing.T) {
	ec := execContext(t, map[string]any{
		"op":      "write",
		"path":    "nested/result.txt",
		"content": "hello artifact\n",
	})
	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.Output["path"] != "nested/result.txt" {
		t.Errorf("path = %v, want nested/result.txt", result.Output["path"])
	}
	if result.Output["size_bytes"] != float64(len("hello artifact\n")) {
		t.Errorf("size_bytes = %v, want %d", result.Output["size_bytes"], len("hello artifact\n"))
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].Name != "nested/result.txt" {
		t.Fatalf("artifacts = %#v, want one named after the written path", result.Artifacts)
	}
	contents, err := os.ReadFile(filepath.Join(ec.WorkDir, "nested/result.txt"))
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if string(contents) != "hello artifact\n" {
		t.Fatalf("file contents = %q, want the configured content", contents)
	}
}

func TestFilePlugin_CaptureAsOverridesTheArtifactName(t *testing.T) {
	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":         "write",
		"path":       "result.txt",
		"content":    "data",
		"capture_as": "final",
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Artifacts[0].Name != "final" {
		t.Fatalf("artifact name = %q, want final", result.Artifacts[0].Name)
	}
}

func TestFilePlugin_ReadReportsSizeAndConsumesAMaterializedArtifact(t *testing.T) {
	ec := execContext(t, nil)
	materialized := filepath.Join(ec.WorkDir, ".artifacts", "fetch")
	if err := os.MkdirAll(materialized, 0o755); err != nil {
		t.Fatalf("prepare materialized artifact: %v", err)
	}
	path := filepath.Join(materialized, "body")
	payload := []byte("upstream payload\x00\xff")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write materialized artifact: %v", err)
	}
	ec.Config = map[string]any{"op": "read", "path": path, "capture_as": "copy-of-body"}

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["size_bytes"] != float64(len(payload)) {
		t.Errorf("size_bytes = %v, want %d", result.Output["size_bytes"], len(payload))
	}
	if result.Output["path"] != ".artifacts/fetch/body" {
		t.Errorf("path = %v, want the workdir-relative path", result.Output["path"])
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].Name != "copy-of-body" {
		t.Fatalf("artifacts = %#v, want the read file captured", result.Artifacts)
	}
	contents, err := os.ReadFile(result.Artifacts[0].LocalPath)
	if err != nil {
		t.Fatalf("read produced artifact: %v", err)
	}
	if string(contents) != string(payload) {
		t.Fatalf("artifact contents = %q, want %q", contents, payload)
	}
}

func TestFilePlugin_ReadFailsForAMissingPath(t *testing.T) {
	_, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"op":   "read",
		"path": "absent.txt",
	}))
	if err == nil {
		t.Fatal("Execute succeeded reading a missing file, want an error")
	}
	assertPermanent(t, err)
}

func TestFilePlugin_CopyDuplicatesTheFile(t *testing.T) {
	ec := execContext(t, map[string]any{"op": "copy", "from": "src.txt", "to": "out/dst.txt"})
	if err := os.WriteFile(filepath.Join(ec.WorkDir, "src.txt"), []byte("copy me"), 0o644); err != nil {
		t.Fatalf("seed source file: %v", err)
	}

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["path"] != "out/dst.txt" {
		t.Errorf("path = %v, want out/dst.txt", result.Output["path"])
	}
	if result.Output["size_bytes"] != float64(len("copy me")) {
		t.Errorf("size_bytes = %v, want %d", result.Output["size_bytes"], len("copy me"))
	}
	contents, err := os.ReadFile(filepath.Join(ec.WorkDir, "out/dst.txt"))
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if string(contents) != "copy me" {
		t.Fatalf("copy contents = %q, want copy me", contents)
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].Name != "out/dst.txt" {
		t.Fatalf("artifacts = %#v, want the copy captured", result.Artifacts)
	}
}

func TestFilePlugin_DeleteRemovesTheFileAndIsIdempotent(t *testing.T) {
	ec := execContext(t, map[string]any{"op": "delete", "path": "gone.txt"})
	if err := os.WriteFile(filepath.Join(ec.WorkDir, "gone.txt"), []byte("bye"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["path"] != "gone.txt" || result.Output["size_bytes"] != float64(3) {
		t.Errorf("output = %#v, want gone.txt and the removed size", result.Output)
	}
	if len(result.Artifacts) != 0 {
		t.Errorf("artifacts = %#v, want none for delete", result.Artifacts)
	}
	if _, err := os.Stat(filepath.Join(ec.WorkDir, "gone.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file still exists after delete (stat error: %v)", err)
	}

	repeat, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("repeated delete failed: %v", err)
	}
	if repeat.Output["size_bytes"] != float64(0) {
		t.Errorf("repeated delete size_bytes = %v, want 0", repeat.Output["size_bytes"])
	}
}

func TestFilePlugin_RejectsPathsOutsideTheWorkDir(t *testing.T) {
	cases := []map[string]any{
		{"op": "write", "path": "../escape.txt", "content": "x"},
		{"op": "read", "path": "/etc/passwd"},
		{"op": "copy", "from": "src.txt", "to": "../escape.txt"},
		{"op": "delete", "path": "../../escape.txt"},
	}
	for _, config := range cases {
		_, err := New().Execute(context.Background(), execContext(t, config))
		if err == nil {
			t.Fatalf("Execute(%v) succeeded, want a containment error", config)
		}
		if !strings.Contains(err.Error(), "escapes the task work directory") {
			t.Errorf("error %q does not explain the containment rule", err)
		}
		assertPermanent(t, err)
	}
}

func TestFilePlugin_RejectsMissingOrUnknownOps(t *testing.T) {
	cases := []struct {
		config   map[string]any
		contains string
	}{
		{config: map[string]any{}, contains: "`op` is required"},
		{config: map[string]any{"op": "chmod"}, contains: "unknown op \"chmod\""},
		{config: map[string]any{"op": "write"}, contains: "`path` is required"},
		{config: map[string]any{"op": "copy", "from": "a"}, contains: "`to` is required"},
	}
	for _, tc := range cases {
		_, err := New().Execute(context.Background(), execContext(t, tc.config))
		if err == nil {
			t.Fatalf("Execute(%v) succeeded, want an error", tc.config)
		}
		if !strings.Contains(err.Error(), tc.contains) {
			t.Errorf("error %q does not mention %q", err, tc.contains)
		}
		assertPermanent(t, err)
	}
}

func TestFilePlugin_DescribeNamesThePlugin(t *testing.T) {
	p := New()
	if p.Name() != "file" || p.Describe().Name != "file" {
		t.Fatalf("plugin name = %q/%q, want file", p.Name(), p.Describe().Name)
	}
}

func assertPermanent(t *testing.T, err error) {
	t.Helper()
	var retryable *plugin.RetryableError
	if !errors.As(err, &retryable) || retryable.Retryable {
		t.Errorf("error %v is not marked permanent", err)
	}
}
