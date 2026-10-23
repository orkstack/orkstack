package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestGlobRegexp(t *testing.T) {
	cases := []struct {
		glob, path string
		want       bool
	}{
		{"cmd/**", "cmd/ork/main.go", true},
		{"cmd/**", "cmd/main.go", true},
		{"cmd/ork/**", "cmd/orker/main.go", false},
		{"cmd/ork/**", "cmd/ork/testdata/x.yaml", true},
		{"**.md", "README.md", true},
		{"**.md", "internal/engine/NOTES.md", true},
		{"**.md", "internal/engine/notes.mdx", false},
		{"ui/e2e/*.spec.ts", "ui/e2e/admin.spec.ts", true},
		{"ui/e2e/*.spec.ts", "ui/e2e/sub/admin.spec.ts", false},
		{"internal/telemetry/**.go", "internal/telemetry/sink/x.go", true},
		{"internal/telemetry/**.go", "internal/telemetry/x.yaml", false},
		{".gitignore", "ui/.gitignore", true},
		{"go.mod", "go.mod", true},
		{"go.mod", "x/go.mod", true},
		{"deploy/**/observability*", "deploy/observability.yaml", true},
		{"a?c", "abc", true},
		{"a?c", "a/c", false},
	}
	for _, c := range cases {
		re, err := globRegexp(c.glob)
		if err != nil {
			t.Fatalf("%s: %v", c.glob, err)
		}
		if got := re.MatchString(c.path); got != c.want {
			t.Errorf("glob %q on %q = %v, want %v (re %s)", c.glob, c.path, got, c.want, re)
		}
	}
}

func TestParseScopeRefusesWhatItCannotRead(t *testing.T) {
	for _, bad := range []string{
		"cmd/**\n",              // no action
		"cmd/**\tgo\tspeedy\n",  // unknown flag
		"cmd/**\tcompile\n",     // unknown action
		"cmd/**\tgo,teleport\n", // one unknown action among known ones
	} {
		if _, err := parseScope(strings.NewReader(bad), "t"); err == nil {
			t.Errorf("parseScope(%q) accepted it", bad)
		}
	}
}

// The checked-in table decides what a feature proof runs; these rows are the
// contract AGENTS.md §11.2 describes, so a reordering that changes one fails
// here rather than in somebody's under-proven merge.
func TestRepositoryScopeTable(t *testing.T) {
	table, err := readScope(filepath.Join("..", "..", "..", "scripts", "proof-scope.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		path     string
		actions  string // "" means unmapped
		temporal bool
	}{
		// documentation
		{"docs/DSL.md", "none", false},
		{"AGENTS.md", "none", false},
		{"internal/engine/README.md", "none", false},
		{"images/activity-shell/README.md", "none", false},
		{"deploy/orkstack.defaults.yaml", "none", false},
		{".gitignore", "none", false},
		// how every suite runs
		{"go.mod", "full", false},
		{"go.sum", "full", false},
		{"Makefile", "full", false},
		{"images/Makefile", "full", false},
		{"deploy/docker-compose.yml", "full", false},
		{"scripts/proof.sh", "full", false},
		{"scripts/stack.sh", "full", false},
		{"scripts/proof-allowed-skips.txt", "full", false},
		{"scripts/proof-scope.tsv", "full", false},
		{"scripts/proof-slow-tests.txt", "none", false},
		{"scripts/proof-test-costs.tsv", "none", false},
		{"internal/testsupport/s3bucket/s3bucket.go", "full", false},
		{"test/tools/proofrun/select.go", "full", false},
		{".github/workflows/ci.yml", "full", false},
		{"buf.gen.yaml", "full", false},
		// Go
		{"internal/engine/workflow.go", "go", true},
		{"internal/engine/workflow_test.go", "go", true},
		{"internal/engine/testdata/history.json", "go", true},
		{"internal/orker/activity.go", "go", true},
		{"internal/api/runs.go", "go", true},
		{"internal/archive/cleaner.go", "go", true},
		{"internal/task/pool.go", "go", true},
		{"pkg/artifacts/s3.go", "go", true},
		{"cmd/ork/main.go", "go", true},
		{"cmd/orker/main.go", "go", true},
		{"cmd/orkctl/run.go", "go", true},
		{"internal/dsl/parse.go", "go", false},
		{"pkg/logstream/frame.go", "go", false},
		{"gen/go/orkstack/v1/run.pb.go", "go", false},
		{"proto/orkstack/v1/run.proto", "proto", false},
		// telemetry
		{"internal/telemetry/setup.go", "observability,go", false},
		{"internal/telemetry/testdata/sinks.yaml", "observability", false},
		{"scripts/proof-observability.sh", "observability", false},
		{"deploy/observability/prometheus.yml", "observability", false},
		// UI
		{"ui/src/routes/runs/[id]/+page.svelte", "ui", false},
		{"ui/src/lib/flakes.ts", "ui", false},
		{"ui/e2e/admin.spec.ts", "spec", false},
		{"ui/e2e/harness.ts", "all-specs", false},
		{"ui/e2e/test.ts", "all-specs", false},
		{"ui/e2e/uiHelpers.ts", "all-specs", false},
		{"ui/playwright.config.ts", "all-specs", false},
		{"ui/package.json", "all-specs", false},
		{"ui/bun.lock", "all-specs", false},
		{"ui/test/flakes.test.ts", "ui-unit", false},
		// images, examples, tools
		{"images/activity-shell/entry.sh", "images", false},
		{"test/fixtures/bash_activity_contract.sh", "images", false},
		{"examples/hello.ork.yaml", "examples", false},
		{"examples/custom-image/main.go", "examples,go", false},
		{"test/tools/cosign-sign/main.go", "tool", false},
		// fail closed
		{"Dockerfile", "", false},
		{"ui/README", "", false},
		{"tools/new-thing.sh", "", false},
	}
	for _, c := range cases {
		r := table.match(c.path)
		got := ""
		temporal := false
		if r != nil {
			got = strings.Join(r.Actions, ",")
			temporal = r.Temporal
		}
		if got != c.actions || temporal != c.temporal {
			t.Errorf("%s -> %q (temporal %v), want %q (temporal %v)", c.path, got, temporal, c.actions, c.temporal)
		}
	}
	if !table.temporalFacing("internal/engine") || table.temporalFacing("internal/dsl") {
		t.Error("temporalFacing disagrees with the table")
	}

	// Plan §10.1's cases: the CI, gate and documentation files this item
	// added, each with the row that must own it.
	for _, c := range []struct {
		path     string
		actions  string
		temporal bool
		docs     bool
	}{
		{".github/actions/proof-host/action.yml", "full", false, true},
		{".github/workflows/proof.yml", "full", false, true},
		{".github/pull_request_template.md", "none", false, false},
		{".github/CODEOWNERS", "full", false, true},
		{"internal/dsl/testdata/schema-vectors/every-feature.ork.yaml", "go", false, true},
		{"scripts/proof-nightly-tests.txt", "full", false, true},
		{"scripts/proof-quarantine.txt", "full", false, true},
		{"scripts/proof-section11-coverage.tsv", "full", false, true},
		{"scripts/proof-host-speed.tsv", "full", false, true},
		{"scripts/proof-ui-costs.tsv", "none", false, false},
		{"scripts/github-merge-gate.sh", "full", false, true},
		{"docs/PROOFS.md", "none", false, false},
		{"internal/docscheck/links_test.go", "go", false, false},
		{"cmd/orkctl/root.go", "go", true, true},
	} {
		r := table.match(c.path)
		if r == nil {
			t.Errorf("%s is unmapped, want %q", c.path, c.actions)
			continue
		}
		if got := strings.Join(r.Actions, ","); got != c.actions || r.Temporal != c.temporal || r.Docs != c.docs {
			t.Errorf("%s -> %q (temporal %v, docs %v; row %d %s), want %q (temporal %v, docs %v)",
				c.path, got, r.Temporal, r.Docs, r.Line, r.Pattern, c.actions, c.temporal, c.docs)
		}
	}
}

// TestRepositoryScopeTable_MapsEveryTrackedPath is the mechanical form of "a
// new top-level path needs a scope row": every tracked path matches a row, so
// a feature proof never meets an unmapped path it could have been told about.
// It opens the index first, so go test's cache keys on it and a cached pass
// never hides a newly tracked file (plan §2.8, §8.3).
func TestRepositoryScopeTable_MapsEveryTrackedPath(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %s: %v", strings.Join(args, " "), err)
		}
		return string(out)
	}
	index := strings.TrimSpace(git("rev-parse", "--git-path", "index"))
	if !filepath.IsAbs(index) {
		index = filepath.Join(root, index)
	}
	f, err := os.Open(index)
	if err != nil {
		t.Fatalf("opening the index so the test cache keys on it: %v", err)
	}
	f.Close()

	table, err := readScope(filepath.Join(root, "scripts", "proof-scope.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	paths := strings.Split(strings.TrimRight(git("ls-files", "-z"), "\x00"), "\x00")
	if len(paths) < 100 {
		t.Fatalf("git ls-files listed %d paths; the check would be vacuous", len(paths))
	}
	var unmapped []string
	for _, p := range paths {
		if p != "" && table.match(p) == nil {
			unmapped = append(unmapped, p)
		}
	}
	sort.Strings(unmapped)
	if len(unmapped) > 0 {
		t.Errorf("%d tracked path(s) match no row of scripts/proof-scope.tsv; add a row (docs/PROOFS.md, \"Add a scope row\"):\n  %s",
			len(unmapped), strings.Join(unmapped, "\n  "))
	}
	t.Logf("%d tracked paths, every one mapped", len(paths)-len(unmapped))
}
