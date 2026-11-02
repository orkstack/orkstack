package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// makeResult is what one run of make printed and how it ended.
type makeResult struct {
	out  string
	code int
}

// runMake runs the repository's Makefile (or makefile, when it is not empty)
// with its working directory in dir, the way a contributor's `make` would,
// with env added to the environment and none of an outer make's flags. A
// make that could not be started fails the test; one that ran and failed is
// a result.
func runMake(t *testing.T, makefile, dir string, env []string, args ...string) makeResult {
	t.Helper()
	if makefile == "" {
		makefile = filepath.Join(mustRepoRoot(), "Makefile")
	}
	cmd := exec.Command("make", append([]string{"--no-print-directory", "-f", makefile, "-C", dir}, args...)...)
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); k != "MAKEFLAGS" && k != "MFLAGS" && k != "MAKELEVEL" {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	// make runs a recipe line that names $(MAKE) even under -n, and the
	// Makefile has such lines that run docker and bun: nothing a run of it here
	// reaches is the host's own. docker and bun are stubs that succeed and do
	// nothing, first on PATH and in GOBIN (which the Makefile puts first on
	// PATH); a run that names its own in env has them instead.
	safe := t.TempDir()
	stubTool(t, safe, "docker", "exit 0\n")
	stubTool(t, safe, "bun", "exit 0\n")
	cmd.Env = append(cmd.Env, "PATH="+safe+string(os.PathListSeparator)+os.Getenv("PATH"), "GOBIN="+safe)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	res := makeResult{out: string(out)}
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("make could not be run: %v", err)
		}
		res.code = ee.ExitCode()
	}
	return res
}

// stubTool writes an executable named name in dir with body, a POSIX shell
// script.
func stubTool(t *testing.T, dir, name, body string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
}
