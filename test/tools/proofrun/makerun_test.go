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
	// nothing, and every other tool that reaches a registry or the network is
	// a tripwire that fails the test. They come before the host's PATH, in
	// GOBIN (which the Makefile puts first on PATH) unless the run names its
	// own, and after the run's own stubs, which a PATH in env names first.
	safe := t.TempDir()
	tripped := filepath.Join(safe, "tripped")
	stubTool(t, safe, "docker", "exit 0\n")
	stubTool(t, safe, "bun", "exit 0\n")
	for _, tool := range networkTools {
		stubTool(t, safe, tool, "echo "+tool+" >> '"+tripped+"'\nexit 99\n")
	}
	hostPath := os.Getenv("PATH")
	path := safe + string(os.PathListSeparator) + hostPath
	gobin := "GOBIN=" + safe
	var rest []string
	for _, kv := range env {
		switch {
		case strings.HasPrefix(kv, "PATH="):
			// The run's own stubs, then the safe ones, then the host's.
			own := strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(kv, "PATH="), hostPath), string(os.PathListSeparator))
			path = own + string(os.PathListSeparator) + path
		case strings.HasPrefix(kv, "GOBIN="):
			gobin = kv
		default:
			rest = append(rest, kv)
		}
	}
	cmd.Env = append(cmd.Env, "PATH="+path, gobin)
	cmd.Env = append(cmd.Env, rest...)
	out, err := cmd.CombinedOutput()
	if reached, readErr := os.ReadFile(tripped); readErr == nil {
		t.Fatalf("make reached the host's %s; stub it in the run's own PATH:\n%s", strings.Fields(string(reached)), out)
	}
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

// networkTools are the tools a Makefile recipe could reach a registry or the
// network with; runMake makes each a tripwire unless the run stubs it.
var networkTools = []string{"grype", "cosign", "gh", "skopeo", "crane", "curl", "wget", "govulncheck"}

// stubTool writes an executable named name in dir with body, a POSIX shell
// script.
func stubTool(t *testing.T, dir, name, body string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
}
