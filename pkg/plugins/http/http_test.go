package httpplugin

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/plugin"
)

// execContext builds a plugin context with a fresh workdir.
func execContext(t *testing.T, config map[string]any) *plugin.ExecContext {
	t.Helper()
	return &plugin.ExecContext{
		TaskID:  "fetch",
		Config:  config,
		WorkDir: t.TempDir(),
		Log:     func(string, ...any) {},
	}
}

func TestHTTPPlugin_CapturesResponseBodyAsArtifactWithExactBytes(t *testing.T) {
	body := []byte("{\"slideshow\":{\"title\":\"Sample\"}}\x00\xfe\xff no trailing newline")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	ec := execContext(t, map[string]any{"url": server.URL, "capture_body_as": "body"})
	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Artifacts) != 1 {
		t.Fatalf("artifacts = %#v, want exactly one", result.Artifacts)
	}
	artifact := result.Artifacts[0]
	if artifact.Name != "body" {
		t.Errorf("artifact name = %q, want body", artifact.Name)
	}
	if artifact.ContentType != "application/json" {
		t.Errorf("artifact content type = %q, want application/json", artifact.ContentType)
	}
	stored, err := os.ReadFile(artifact.LocalPath)
	if err != nil {
		t.Fatalf("read captured body: %v", err)
	}
	if string(stored) != string(body) {
		t.Fatalf("captured body = %q, want the exact response bytes %q", stored, body)
	}
}

func TestHTTPPlugin_OutputHasStatusParsedJSONAndDuration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Add("X-Multi", "one")
		w.Header().Add("X-Multi", "two")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"title":"Sample","count":2,"tags":["a","b"]}`))
	}))
	defer server.Close()

	result, err := New().Execute(context.Background(), execContext(t, map[string]any{"url": server.URL}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.Output["status"] != float64(http.StatusCreated) {
		t.Errorf("status = %v, want %d", result.Output["status"], http.StatusCreated)
	}
	parsed, ok := result.Output["json"].(map[string]any)
	if !ok {
		t.Fatalf("json = %#v, want a parsed object", result.Output["json"])
	}
	if parsed["title"] != "Sample" || parsed["count"] != float64(2) {
		t.Errorf("parsed json = %#v, want the response fields", parsed)
	}
	duration, ok := result.Output["duration_ms"].(float64)
	if !ok || duration < 0 {
		t.Errorf("duration_ms = %#v, want a non-negative number", result.Output["duration_ms"])
	}
	headers, ok := result.Output["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %#v, want a map", result.Output["headers"])
	}
	multi, ok := headers["X-Multi"].([]any)
	if !ok || len(multi) != 2 || multi[0] != "one" || multi[1] != "two" {
		t.Errorf("X-Multi header = %#v, want both values preserved", headers["X-Multi"])
	}
}

func TestHTTPPlugin_NonJSONBodySucceedsWithoutAJSONField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("plain text, definitely not json"))
	}))
	defer server.Close()

	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"url":             server.URL,
		"capture_body_as": "body",
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, present := result.Output["json"]; present {
		t.Errorf("json = %#v, want the field absent for a non-JSON body", result.Output["json"])
	}
	stored, err := os.ReadFile(result.Artifacts[0].LocalPath)
	if err != nil {
		t.Fatalf("read captured body: %v", err)
	}
	if string(stored) != "plain text, definitely not json" {
		t.Errorf("captured body = %q", stored)
	}
}

func TestHTTPPlugin_SendsMethodHeadersAndBody(t *testing.T) {
	var (
		gotMethod string
		gotHeader string
		gotBody   string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotHeader = r.Header.Get("X-Token")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"url":     server.URL,
		"method":  "post",
		"headers": map[string]any{"X-Token": "abc123"},
		"body":    "payload=1",
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotHeader != "abc123" {
		t.Errorf("X-Token = %q, want abc123", gotHeader)
	}
	if gotBody != "payload=1" {
		t.Errorf("body = %q, want payload=1", gotBody)
	}
	if result.Output["status"] != float64(http.StatusNoContent) {
		t.Errorf("status = %v, want 204", result.Output["status"])
	}
}

func TestHTTPPlugin_ReportsNon2xxStatusWithoutFailingTheTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("nope"))
	}))
	defer server.Close()

	result, err := New().Execute(context.Background(), execContext(t, map[string]any{"url": server.URL}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output["status"] != float64(http.StatusTeapot) {
		t.Fatalf("status = %v, want 418", result.Output["status"])
	}
}

func TestHTTPPlugin_TimeoutSecondsAbortsASlowResponse(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = w.Write([]byte("late"))
	}))
	defer server.Close()
	defer close(release)

	start := time.Now()
	_, err := New().Execute(context.Background(), execContext(t, map[string]any{
		"url":             server.URL,
		"timeout_seconds": float64(0.2),
	}))
	if err == nil {
		t.Fatal("Execute succeeded, want a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %s, want it to honor timeout_seconds", elapsed)
	}
	var permanent *plugin.RetryableError
	if errors.As(err, &permanent) && !permanent.Retryable {
		t.Fatalf("timeout error %v is marked permanent, want it retryable", err)
	}
}

func TestHTTPPlugin_LeavesNoBodyFileWhenCaptureIsNotRequested(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	defer server.Close()

	ec := execContext(t, map[string]any{"url": server.URL})
	result, err := New().Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Artifacts) != 0 {
		t.Errorf("artifacts = %#v, want none", result.Artifacts)
	}
	entries, err := os.ReadDir(ec.WorkDir)
	if err != nil {
		t.Fatalf("read workdir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, filepath.Base(entry.Name()))
		}
		t.Errorf("workdir still holds %v, want the temporary body file removed", names)
	}
}

func TestHTTPPlugin_RejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name     string
		config   map[string]any
		contains string
	}{
		{name: "missing url", config: map[string]any{}, contains: "`url` is required"},
		{
			name:     "non numeric timeout",
			config:   map[string]any{"url": "http://example.test", "timeout_seconds": "soon"},
			contains: "timeout_seconds",
		},
		{
			name:     "headers is not a map",
			config:   map[string]any{"url": "http://example.test", "headers": "X-Token: abc"},
			contains: "headers",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New().Execute(context.Background(), execContext(t, tc.config))
			if err == nil {
				t.Fatalf("Execute(%v) succeeded, want an error", tc.config)
			}
			if !strings.Contains(err.Error(), tc.contains) {
				t.Errorf("error %q does not mention %q", err, tc.contains)
			}
			var retryable *plugin.RetryableError
			if !errors.As(err, &retryable) || retryable.Retryable {
				t.Errorf("error %v is not marked permanent", err)
			}
		})
	}
}

func TestHTTPPlugin_DescribeNamesThePlugin(t *testing.T) {
	p := New()
	if p.Name() != "http" {
		t.Fatalf("Name() = %q, want http", p.Name())
	}
	if p.Describe().Name != "http" {
		t.Fatalf("Describe().Name = %q, want http", p.Describe().Name)
	}
}
