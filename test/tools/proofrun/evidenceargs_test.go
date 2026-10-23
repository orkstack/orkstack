package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// evidenceArgs runs cmdEvidenceArgs over body and returns its exit and
// stdout.
func evidenceArgs(t *testing.T, body string) (int, string) {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "body.md")
	if err := os.WriteFile(file, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	prevOut, prevErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, w
	code := cmdEvidenceArgs([]string{"-body", file})
	os.Stdout, os.Stderr = prevOut, prevErr
	_ = w.Close()
	out, _ := io.ReadAll(r)
	return code, string(out)
}

func TestEvidenceArgs_ParsesPRBodyLines(t *testing.T) {
	spec := filepath.Join(mustRepoRoot(), "ui", "e2e", "each-fan-out.spec.ts")
	body := "Fixes the pin lease.\n\n" +
		"Proof-tests: internal/engine:^TestIntegration_Pin$\n" +
		"  Proof-tests:   ./internal/dsl:TestParse.*\n" +
		"Proof-specs: " + spec + "\n" +
		"Not-a-proof-line: ignored\n"
	code, out := evidenceArgs(t, body)
	want := "--tests\ninternal/engine:^TestIntegration_Pin$\n--tests\n./internal/dsl:TestParse.*\n--specs\n" + spec + "\n"
	if code != 0 || out != want {
		t.Errorf("exit %d, output\n%q\nwant\n%q", code, out, want)
	}
	if code, out := evidenceArgs(t, "No proof lines here.\n"); code != 0 || out != "" {
		t.Errorf("a body with no lines: exit %d, %q", code, out)
	}
}

func TestEvidenceArgs_RefusesAMalformedLine(t *testing.T) {
	for name, line := range map[string]string{
		"no package":            "Proof-tests: TestX",
		"an empty regexp":       "Proof-tests: internal/engine:",
		"an invalid regexp":     "Proof-tests: internal/engine:Test(",
		"an empty spec":         "Proof-specs:",
		"a spec that is absent": "Proof-specs: ui/e2e/no-such.spec.ts",
		"a spec that is a dir":  "Proof-specs: " + os.TempDir(),
	} {
		t.Run(name, func(t *testing.T) {
			code, out := evidenceArgs(t, "Body.\n"+line+"\n")
			if code != 2 || !strings.Contains(out, "line 2") {
				t.Errorf("exit %d, %q", code, out)
			}
			if strings.Contains(out, "--tests") || strings.Contains(out, "--specs") {
				t.Errorf("a refused body still printed arguments: %q", out)
			}
		})
	}
}
