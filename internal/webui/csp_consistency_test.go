package webui_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/webui"
)

// cspKeywords are the source keywords SvelteKit writes quoted (csp takes
// them bare); every other source it writes as it is given.
var cspKeywords = map[string]bool{
	"self": true, "none": true, "unsafe-eval": true, "unsafe-inline": true, "unsafe-hashes": true,
	"strict-dynamic": true, "report-sample": true, "wasm-unsafe-eval": true,
}

// buildDirectives reads the directives the UI's build writes into its pages
// (ui/vite.config.ts, the sveltekit plugin's csp), as SvelteKit renders
// them, without the
// hashes the config computes (an identifier, not a literal) or the build
// adds.
func buildDirectives(t *testing.T) map[string][]string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("..", "..", "ui", "vite.config.ts"))
	if err != nil {
		t.Fatal(err)
	}
	block := regexp.MustCompile(`(?s)csp:\s*\{\s*mode:\s*'hash',\s*directives:\s*\{([^}]*)\}`).FindSubmatch(source)
	if block == nil {
		t.Fatal("ui/vite.config.ts has no csp block in hash mode: the build's policy moved, and this test must follow it")
	}
	directives := map[string][]string{}
	for _, entry := range regexp.MustCompile(`'([a-z-]+)':\s*\[([^\]]*)\]`).FindAllSubmatch(block[1], -1) {
		var sources []string
		for value := range strings.SplitSeq(string(entry[2]), ",") {
			value = strings.TrimSpace(value)
			literal, quoted := strings.CutPrefix(value, "'")
			if !quoted {
				continue // An identifier: a hash the config computes.
			}
			literal = strings.TrimSuffix(literal, "'")
			if cspKeywords[literal] {
				literal = "'" + literal + "'"
			}
			sources = append(sources, literal)
		}
		directives[string(entry[1])] = sources
	}
	if len(directives) == 0 {
		t.Fatal("ui/vite.config.ts's csp names no directive")
	}
	return directives
}

// policyDirectives splits a policy into its directives' sources.
func policyDirectives(policy string) map[string][]string {
	directives := map[string][]string{}
	for directive := range strings.SplitSeq(policy, ";") {
		if fields := strings.Fields(directive); len(fields) > 0 {
			directives[fields[0]] = fields[1:]
		}
	}
	return directives
}

// The default page policy is the build's own (ui/vite.config.ts, csp)
// without its hashes and without 'unsafe-eval', opened with frame-ancestors
// 'none': every directive the build writes, with the same sources but
// those. A page the engine serves without a policy of its own runs under
// the app's rule, but no inline script and no eval (it is never the app,
// whose boot script needs a hash), and a change to either file that the
// other does not follow fails here.
func TestDefaultPagePolicy_IsTheBuildsPolicyWithoutItsHashesOrEval(t *testing.T) {
	want := buildDirectives(t)
	want["frame-ancestors"] = []string{"'none'"}
	if !slices.Contains(want["script-src"], "'unsafe-eval'") {
		t.Errorf("the build's script-src %q has no 'unsafe-eval': drop it from this test and from SECURITY_MODEL.md", want["script-src"])
	}
	want["script-src"] = slices.DeleteFunc(slices.Clone(want["script-src"]), func(s string) bool { return s == "'unsafe-eval'" })
	got := policyDirectives(webui.DefaultPagePolicy)
	for name, sources := range want {
		if strings.Join(got[name], " ") != strings.Join(sources, " ") {
			t.Errorf("%s: DefaultPagePolicy has %q, the build %q", name, got[name], sources)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s: DefaultPagePolicy has a directive the build does not write", name)
		}
	}
	if !strings.HasPrefix(webui.DefaultPagePolicy, "frame-ancestors 'none'; ") {
		t.Errorf("DefaultPagePolicy %q does not open with frame-ancestors 'none'", webui.DefaultPagePolicy)
	}
}
