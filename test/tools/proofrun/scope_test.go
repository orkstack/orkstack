package main

import (
	"path/filepath"
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
}
