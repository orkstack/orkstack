package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

// shellFunctions are the text of the named top-level functions of a bash
// script (`name() {` to the `}` that closes it in column 0), so that a test
// can run them under bash with fakes on PATH instead of reading the script's
// text for what it says it does.
func shellFunctions(t *testing.T, script string, names ...string) string {
	t.Helper()
	lines := strings.Split(script, "\n")
	var out []string
	for _, name := range names {
		start := -1
		for i, l := range lines {
			if strings.HasPrefix(l, name+"() {") {
				start = i
				break
			}
		}
		if start < 0 {
			t.Fatalf("the script has no function %s", name)
		}
		end := -1
		for i := start + 1; i < len(lines); i++ {
			if lines[i] == "}" {
				end = i
				break
			}
		}
		if end < 0 {
			t.Fatalf("the function %s is not closed", name)
		}
		out = append(out, strings.Join(lines[start:end+1], "\n"))
	}
	return strings.Join(out, "\n")
}

// proofScriptFunctions are functions of scripts/proof.sh.
func proofScriptFunctions(t *testing.T, names ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	return shellFunctions(t, string(b), names...)
}

// needBash skips a test on a host with no bash or no /proc, which scripts/proof.sh
// needs (proc_start reads /proc/<pid>/stat).
func needBash(t *testing.T) string {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	if runtime.GOOS != "linux" {
		t.Skip("scripts/proof.sh reads /proc")
	}
	return bash
}

// runProofShell runs script under bash in dir, with the fake PATH the test
// installed, and returns what it printed. A stand-in for `say` is defined
// first: it prints its arguments.
func runProofShell(t *testing.T, dir, script string, env ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(needBash(t), "-c", "say() { echo \"$*\"; }\n"+script)
	// Its own process group, as the proof's: a script that signals its group
	// (on_exit) reaches nothing of the test.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// proofrunStandIn is a proofrun on disk for a script's $PR: this test binary,
// which runs the subcommand it is given (TestMain).
func proofrunStandIn(t *testing.T) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "proofrun")
	if err := os.WriteFile(path, []byte(fmt.Sprintf("#!/bin/sh\nPROOFRUN_AS_COMMAND=1 exec %q \"$@\"\n", self)), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}
