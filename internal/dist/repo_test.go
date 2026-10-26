package dist_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// readRepoFile reads a repository file, named by its slash-separated path
// from the root (repoRoot). The contract tests of the release images and the
// quickstart read the committed files, so they hold the files themselves and
// never a copy.
func readRepoFile(t *testing.T, rel string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return data
}

// splitColon splits s at the colons outside a ${...} reference, so
// "${A-/x}:/x:ro" is three parts and a default holding a colon stays whole.
func splitColon(s string) []string {
	var (
		parts []string
		start int
		depth int
	)
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '$' && i+1 < len(s) && s[i+1] == '{':
			depth++
			i++
		case s[i] == '}' && depth > 0:
			depth--
		case s[i] == ':' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

func TestSplitColon(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"127.0.0.1:8080:80", []string{"127.0.0.1", "8080", "80"}},
		{"127.0.0.1:${P-8233}:8233", []string{"127.0.0.1", "${P-8233}", "8233"}},
		{"${S-/var/run/docker.sock}:/var/run/docker.sock", []string{"${S-/var/run/docker.sock}", "/var/run/docker.sock"}},
		{"./:/work:ro", []string{"./", "/work", "ro"}},
		{"${A:-x:y}:/t", []string{"${A:-x:y}", "/t"}},
		{"${W-${HOME}/w}:${W-${HOME}/w}", []string{"${W-${HOME}/w}", "${W-${HOME}/w}"}},
		{"plain", []string{"plain"}},
	} {
		got := splitColon(tc.in)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("splitColon(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

var (
	requiredVariable = regexp.MustCompile(`\$\{([A-Z][A-Z0-9_]*):\?[^}]*\}`)
	// A default may name one plain variable, as ${V-${HOME}/x} does.
	optionalVariable = regexp.MustCompile(`\$\{([A-Z][A-Z0-9_]*)-((?:[^${}]|\$\{[A-Z][A-Z0-9_]*\})*)\}`)
)

// withoutComments drops whole-line comments, so a variable a comment names
// does not count as one the file reads.
func withoutComments(text string) string {
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// composeVariables returns the variables a compose file requires (${V:?...})
// and the optional ones with their defaults (${V-default}).
func composeVariables(text string) (required []string, optional map[string]string) {
	text = withoutComments(text)
	for _, m := range requiredVariable.FindAllStringSubmatch(text, -1) {
		required = append(required, m[1])
	}
	optional = map[string]string{}
	for _, m := range optionalVariable.FindAllStringSubmatch(text, -1) {
		optional[m[1]] = m[2]
	}
	return required, optional
}

// parseEnvFile reads a .env file: KEY=value lines, with comments and blank
// lines skipped. A line that is neither is a test failure.
func parseEnvFile(t *testing.T, name string, data []byte) map[string]string {
	t.Helper()
	env := map[string]string{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" || strings.ContainsAny(key, " \t") {
			t.Fatalf("%s:%d: %q is not KEY=value", name, i+1, line)
		}
		if _, dup := env[key]; dup {
			t.Fatalf("%s:%d: %s is set twice", name, i+1, key)
		}
		env[key] = value
	}
	return env
}
