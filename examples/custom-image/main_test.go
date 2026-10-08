package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveRead_Rules(t *testing.T) {
	ws := t.TempDir()
	if err := os.MkdirAll(filepath.Join(ws, "input", "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(ws, "input", "data.txt")
	if err := os.WriteFile(file, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(file, filepath.Join(ws, "input", "link.txt")); err != nil {
		t.Fatal(err)
	}

	accepted := map[string]string{
		"data.txt":             file,
		ws + "/input/data.txt": file,
	}
	for raw, want := range accepted {
		got, err := resolveRead(raw, ws)
		if err != nil || got != want {
			t.Errorf("resolveRead(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
	refused := map[string]string{
		"":             "empty",
		"/etc/passwd":  "must be inside",
		"a/../b":       "may not contain '..'",
		"../x":         "may not contain '..'",
		ws + "/../etc": "may not contain '..'",
		"dir":          "not a regular file",
		"link.txt":     "not a regular file",
		"missing.txt":  "no such file",
	}
	for raw, want := range refused {
		got, err := resolveRead(raw, ws)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("resolveRead(%q) = %q, %v; want an error containing %q", raw, got, err, want)
		}
	}
}

func TestCountText(t *testing.T) {
	cases := []struct {
		in                   string
		lines, words, nbytes int
	}{
		{"", 0, 0, 0},
		{"a", 1, 1, 1},
		{"a\n", 1, 1, 2},
		{"a b\n c", 2, 3, 6},
		{"\n\n", 2, 0, 2},
		{"\tone\vtwo\fthree\r\n", 1, 3, 16},
	}
	for _, tc := range cases {
		got := countText([]byte(tc.in))
		if got != (textStats{Lines: tc.lines, Words: tc.words, Bytes: tc.nbytes}) {
			t.Errorf("countText(%q) = %+v; want lines %d, words %d, bytes %d", tc.in, got, tc.lines, tc.words, tc.nbytes)
		}
	}
}

func TestCheckArtifactName(t *testing.T) {
	for _, bad := range []string{"", ".", "..", "a/b", strings.Repeat("x", 129)} {
		if err := checkArtifactName(bad); err == nil {
			t.Errorf("checkArtifactName(%q) accepted", bad)
		}
	}
	for _, good := range []string{"report.txt", strings.Repeat("x", 128), "a..b"} {
		if err := checkArtifactName(good); err != nil {
			t.Errorf("checkArtifactName(%q) = %v", good, err)
		}
	}
}

func TestLabelOf(t *testing.T) {
	str := &eachContext{Item: json.RawMessage(`"alpha"`)}
	obj := &eachContext{Item: json.RawMessage(`{"name":"alpha"}`)}
	cases := []struct {
		name string
		cfg  config
		each *eachContext
		want string
	}{
		{"with.label wins", config{Label: "from with"}, str, "from with"},
		{"a string item otherwise", config{}, str, "alpha"},
		{"a non-string item gives none", config{}, obj, ""},
		{"neither gives none", config{}, nil, ""},
	}
	for _, tc := range cases {
		if got := labelOf(tc.cfg, tc.each); got != tc.want {
			t.Errorf("%s: labelOf = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestCompactJSON pins the argv line of the report: one line, no HTML
// escaping, exactly as the shell image's `jq -c` prints it.
func TestCompactJSON(t *testing.T) {
	got, err := compactJSON([]string{"{{ .inputs.label }}", "a<b&c"})
	if err != nil || got != `["{{ .inputs.label }}","a<b&c"]` {
		t.Fatalf("compactJSON = %q, %v", got, err)
	}
	if got, _ := compactJSON([]string{}); got != `[]` {
		t.Fatalf("compactJSON(empty) = %q", got)
	}
}
