package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// groupPaths returns the argument path of every command below root that has
// subcommands of its own, nested groups included, read from the live tree so
// a future group cannot escape the tests that use it.
func groupPaths(root *cobra.Command) [][]string {
	var out [][]string
	var walk func(cmd *cobra.Command, path []string)
	walk = func(cmd *cobra.Command, path []string) {
		for _, child := range cmd.Commands() {
			childPath := append(append([]string(nil), path...), child.Name())
			if child.HasSubCommands() {
				out = append(out, childPath)
			}
			walk(child, childPath)
		}
	}
	walk(root, nil)
	return out
}

// runnableGroups are groups that are also commands of their own: bare, they
// run rather than print help.
var runnableGroups = map[string]bool{"stats": true}

// TestEveryGroupRefusesAnUnknownSubcommand: a mistyped subcommand is an
// error (exit 1, the unknown command named on stderr, nothing on stdout),
// never help and exit 0 — a script that typed `orkctl run lsit` must not read
// as having succeeded. The bare group still prints its help and exits 0.
func TestEveryGroupRefusesAnUnknownSubcommand(t *testing.T) {
	root, _ := newRootCommand()
	groups := groupPaths(root)
	if len(groups) < 13 {
		t.Fatalf("found %d groups in the command tree, want at least 13: %v", len(groups), groups)
	}
	for _, path := range groups {
		t.Run(strings.Join(path, "_"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Execute(append(append([]string(nil), path...), "zzbogus", "--server", "http://127.0.0.1:1"), &stdout, &stderr)
			if code != exitError {
				t.Fatalf("orkctl %v zzbogus exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
					path, code, exitError, stdout.String(), stderr.String())
			}
			want := `unknown command "zzbogus" for "orkctl ` + strings.Join(path, " ") + `"`
			if !strings.Contains(stderr.String(), want) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}

			if runnableGroups[strings.Join(path, " ")] {
				return
			}
			stdout.Reset()
			stderr.Reset()
			code = Execute(append(append([]string(nil), path...), "--server", "http://127.0.0.1:1"), &stdout, &stderr)
			if code != exitOK {
				t.Fatalf("bare orkctl %v exited %d, want 0\nstderr:\n%s", path, code, stderr.String())
			}
			if !strings.Contains(stdout.String(), "Available Commands:") {
				t.Fatalf("bare orkctl %v did not print its help:\n%s", path, stdout.String())
			}
		})
	}
}

// TestUnknownSubcommand_SuggestsTheNearestOne: a near miss names what was
// probably meant, as cobra does at the root.
func TestUnknownSubcommand_SuggestsTheNearestOne(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"run", "lst", "--server", "http://127.0.0.1:1"}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("exit code = %d, want %d", code, exitError)
	}
	for _, want := range []string{`unknown command "lst" for "orkctl run"`, "Did you mean this?", "list"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), want)
		}
	}
}
