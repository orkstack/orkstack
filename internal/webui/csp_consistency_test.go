package webui_test

import (
	"errors"
	"fmt"
	"maps"
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

var (
	cspBlock     = regexp.MustCompile(`(?s)csp:\s*\{\s*mode:\s*'hash',\s*directives:\s*\{([^}]*)\}`)
	cspDirective = regexp.MustCompile(`'([a-z-]+)':\s*\[([^\]]*)\]`)
)

// buildDirectives reads the directives the UI's build writes into its pages
// (ui/vite.config.ts, the sveltekit plugin's csp), as SvelteKit renders
// them, without the hashes the config computes (an identifier, not a
// literal) or the build adds. The unit gate has no UI build to read the
// rendered page from, so the config is the one source there is; a config
// this cannot read is an error, never a pass.
func buildDirectives(config []byte) (map[string][]string, error) {
	block := cspBlock.FindSubmatch(config)
	if block == nil {
		return nil, errors.New("no csp block in hash mode: the build's policy moved, and this test must follow it")
	}
	directives := map[string][]string{}
	for _, entry := range cspDirective.FindAllSubmatch(block[1], -1) {
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
		return nil, errors.New("the csp block names no directive")
	}
	return directives, nil
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

// policyDrift lists every way policy differs from what the page policy in
// config must become for a page that names none: the build's directives with
// the same sources, opened with frame-ancestors 'none', but with no hash. No
// page allows eval: a build that does is drift of its own. It is empty when
// they agree.
func policyDrift(config []byte, policy string) ([]string, error) {
	want, err := buildDirectives(config)
	if err != nil {
		return nil, err
	}
	want["frame-ancestors"] = []string{"'none'"}
	var drift []string
	if slices.Contains(want["script-src"], "'unsafe-eval'") {
		drift = append(drift, "the build's script-src allows 'unsafe-eval'")
		want["script-src"] = slices.DeleteFunc(want["script-src"], func(s string) bool { return s == "'unsafe-eval'" })
	}
	got := policyDirectives(policy)
	for _, name := range slices.Sorted(maps.Keys(want)) {
		if strings.Join(got[name], " ") != strings.Join(want[name], " ") {
			drift = append(drift, fmt.Sprintf("%s: the policy has %q, the build %q", name, got[name], want[name]))
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			drift = append(drift, name+": the policy has a directive the build does not write")
		}
	}
	slices.Sort(drift)
	if !strings.HasPrefix(policy, "frame-ancestors 'none'; ") {
		drift = append(drift, "the policy does not open with frame-ancestors 'none'")
	}
	return drift, nil
}

// The default page policy is the build's own (ui/vite.config.ts, csp)
// without its hashes, opened with frame-ancestors 'none': every directive
// the build writes, with the same sources but those. A page the engine serves
// without a policy of its own runs under the app's rule, but no inline script
// (it is never the app, whose boot script needs a hash), and a change to
// either file that the other does not follow fails here. Neither allows
// 'unsafe-eval': the YAML editor's validators are compiled ahead of time
// (ui/test/gen-validators.ts), and nothing else in the page evaluates code.
func TestDefaultPagePolicy_IsTheBuildsPolicyWithoutItsHashes(t *testing.T) {
	config, err := os.ReadFile(filepath.Join("..", "..", "ui", "vite.config.ts"))
	if err != nil {
		t.Fatal(err)
	}
	drift, err := policyDrift(config, webui.DefaultPagePolicy)
	if err != nil {
		t.Fatalf("ui/vite.config.ts: %v", err)
	}
	for _, d := range drift {
		t.Errorf("DefaultPagePolicy and ui/vite.config.ts disagree, %s", d)
	}
}

// What the comparison above accepts and refuses, on configs of its own: it
// holds the default to the build's directives, refuses eval in either, and a
// config it cannot read fails rather than passes.
func TestPolicyDrift(t *testing.T) {
	config := func(scriptSources string) string {
		return `sveltekit({
			csp: {
				mode: 'hash',
				directives: {
					'default-src': ['self'],
					'script-src': [` + scriptSources + `],
					'style-src': ['self', 'unsafe-inline'],
					'img-src': ['self', 'data:']
				}
			}
		})`
	}
	const matching = "frame-ancestors 'none'; default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:"
	for _, tc := range []struct {
		name, config, policy string
		wantErr              bool
		drift                []string // fragments, one per finding
	}{
		{name: "a build with eval, a default without it",
			config: config(`'self', 'unsafe-eval', themeHash`), policy: matching,
			drift: []string{"the build's script-src allows 'unsafe-eval'"}},
		{name: "a build without eval",
			config: config(`'self', themeHash`), policy: matching},
		{name: "a default that allows eval",
			config: config(`'self', themeHash`),
			policy: strings.Replace(matching, "script-src 'self'", "script-src 'self' 'unsafe-eval'", 1),
			drift:  []string{"script-src"}},
		{name: "a default that allows a hash",
			config: config(`'self', themeHash`),
			policy: strings.Replace(matching, "script-src 'self'", "script-src 'self' 'sha256-AAAA'", 1),
			drift:  []string{"script-src"}},
		{name: "a directive the build changed",
			config: config(`'self', themeHash`),
			policy: strings.Replace(matching, "img-src 'self' data:", "img-src 'self'", 1),
			drift:  []string{"img-src"}},
		{name: "a directive the build dropped",
			config: config(`'self', themeHash`),
			policy: matching + "; font-src 'self'",
			drift:  []string{"font-src: the policy has a directive the build does not write"}},
		{name: "a directive the default lacks",
			config: config(`'self', themeHash`),
			policy: strings.Replace(matching, "; img-src 'self' data:", "", 1),
			drift:  []string{"img-src"}},
		{name: "a default that may be framed",
			config: config(`'self', themeHash`),
			policy: strings.Replace(matching, "frame-ancestors 'none'; ", "", 1),
			drift:  []string{"frame-ancestors", "does not open with frame-ancestors 'none'"}},
		{name: "a config with no csp block", config: `sveltekit({})`, policy: matching, wantErr: true},
		{name: "a config whose csp names no directive",
			config: `csp: { mode: 'hash', directives: { } }`, policy: matching, wantErr: true},
	} {
		drift, err := policyDrift([]byte(tc.config), tc.policy)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: error %v, want error %v", tc.name, err, tc.wantErr)
			continue
		}
		report := strings.Join(drift, "\n")
		if len(drift) != len(tc.drift) {
			t.Errorf("%s: drift\n%s\nwant %d findings naming %q", tc.name, report, len(tc.drift), tc.drift)
			continue
		}
		for _, fragment := range tc.drift {
			if !strings.Contains(report, fragment) {
				t.Errorf("%s: drift\n%s\nlacks %q", tc.name, report, fragment)
			}
		}
	}
}
