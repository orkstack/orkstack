package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/webui"
)

func TestUIFromEnv(t *testing.T) {
	build := t.TempDir()
	if err := os.WriteFile(filepath.Join(build, "index.html"), []byte("from disk"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, enabled, dir string
		want               uiConfig
		wantErr            string
	}{
		{name: "unset: the embedded UI", want: uiConfig{enabled: true}},
		{name: "off", enabled: "false", want: uiConfig{enabled: false}},
		{name: "on, explicitly", enabled: "true", want: uiConfig{enabled: true}},
		{name: "a directory", dir: build, want: uiConfig{enabled: true, dir: build}},
		{name: "not a boolean", enabled: "yes please", wantErr: "ORK_UI_ENABLED"},
		{name: "a directory for a UI that is off", enabled: "false", dir: build, wantErr: "ORK_UI_DIR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORK_UI_ENABLED", tc.enabled)
			t.Setenv("ORK_UI_DIR", tc.dir)
			got, err := uiFromEnv()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("uiFromEnv() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestUIHandler(t *testing.T) {
	serve := func(t *testing.T, h http.Handler) string {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/runs", nil))
		b, err := io.ReadAll(rec.Result().Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	t.Run("off: no handler", func(t *testing.T) {
		h, source, err := uiHandler(uiConfig{enabled: false})
		if err != nil || h != nil || source != "off" {
			t.Errorf("uiHandler(off) = %v, %q, %v; want nil, off, nil", h, source, err)
		}
	})
	t.Run("a directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("from disk"), 0o644); err != nil {
			t.Fatal(err)
		}
		h, source, err := uiHandler(uiConfig{enabled: true, dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		if page := serve(t, h); source != "directory" || page != "from disk" {
			t.Errorf("source %q, page %q; want the directory's", source, page)
		}
	})
	t.Run("a directory that is not a UI build is refused at boot", func(t *testing.T) {
		if _, _, err := uiHandler(uiConfig{enabled: true, dir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "ORK_UI_DIR") {
			t.Errorf("err = %v, want one naming ORK_UI_DIR", err)
		}
	})
	t.Run("embedded, or the placeholder in a binary built without the UI", func(t *testing.T) {
		h, source, err := uiHandler(uiConfig{enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		_, embedded := webui.Embedded()
		want := "placeholder"
		if embedded {
			want = "embedded"
		}
		if source != want {
			t.Errorf("source %q, want %q", source, want)
		}
		if page := serve(t, h); !embedded && !strings.Contains(page, "make ui") {
			t.Errorf("the placeholder page does not say how to build the UI: %q", page)
		}
	})
}

// The UI's requests are not API requests: tracing them would put a page
// load's thirty asset fetches into the API's latency metrics.
func TestTraceAPIRequest_ExcludesTheUI(t *testing.T) {
	for path, want := range map[string]bool{
		"/":                                false,
		"/runs/ork-1":                      false,
		"/_app/immutable/entry/start.a.js": false,
		"/apply/examples.json":             false,
		"/orkstack.v1.RunService/GetRun":   true,
		"/v1/triggers/trg-1":               true,
		"/ws/runs/ork-1":                   false,
	} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if got := traceAPIRequest(r); got != want {
			t.Errorf("traceAPIRequest(%q) = %v, want %v", path, got, want)
		}
	}
}
