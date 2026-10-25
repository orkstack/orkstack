package main

import (
	"bytes"
	"strings"
	"testing"
)

// executeAs runs the command tree as if the host were goos, and returns what
// it printed and the error the command ended with.
func executeAs(t *testing.T, goos string, args ...string) (string, error) {
	t.Helper()
	root, flags := newRootCommand()
	defer flags.closeIdleConnections()
	flags.goos = goos
	var out bytes.Buffer
	root.SetArgs(args)
	root.SetOut(&out)
	root.SetErr(&out)
	err := root.Execute()
	return out.String(), err
}

// TestLocalRun_RefusesOnWindows: `task run` and `activity test` drive a local
// Docker daemon through the Orker's dispatcher, which a Windows host cannot be
// asked to do. They refuse at the start, with the reason, before they look at
// a flag, so a Windows user is told the platform is the problem and not that
// a flag is missing. The same invocations on Linux and macOS get past the
// platform check; the arguments are chosen so that the next thing to fail is
// the flag check, which needs no container runtime.
func TestLocalRun_RefusesOnWindows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
	}{
		{"task run", []string{"task", "run", "--task", "build"}},
		{"activity test", []string{"activity", "test", "example.invalid/image"}},
	}
	for _, tc := range tests {
		t.Run(tc.name+" on windows", func(t *testing.T) {
			t.Parallel()
			_, err := executeAs(t, "windows", tc.args...)
			if err == nil {
				t.Fatal("the command ran on windows, want a refusal")
			}
			for _, want := range []string{tc.name, "Linux or macOS", "Docker"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not mention %q", err, want)
				}
			}
			if code := exitCodeFor(err); code != exitError {
				t.Errorf("exit code %d, want %d", code, exitError)
			}
		})
	}
	for _, goos := range []string{"linux", "darwin"} {
		t.Run("task run on "+goos, func(t *testing.T) {
			t.Parallel()
			// The arguments name no run and no file: a supported host gets as
			// far as the flag check, which proves the refusal above is the
			// platform's and not the arguments'.
			_, err := executeAs(t, goos, tests[0].args...)
			const want = "one of --from-run or --file is required"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("on %s: %v, want %q", goos, err, want)
			}
		})
	}
}

// TestLocalRun_RefusalIsPerCommand: the platform check is a function of the
// host and the command named, and nothing else on a Windows host is refused.
func TestLocalRun_RefusalIsPerCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		goos    string
		command string
		refused bool
	}{
		{"windows", "task run", true},
		{"windows", "activity test", true},
		{"linux", "task run", false},
		{"linux", "activity test", false},
		{"darwin", "task run", false},
		{"darwin", "activity test", false},
		{"freebsd", "activity test", false},
	}
	for _, tc := range tests {
		err := requireLocalRunHost(tc.goos, tc.command)
		if (err != nil) != tc.refused {
			t.Errorf("requireLocalRunHost(%q, %q) = %v, refused want %v", tc.goos, tc.command, err, tc.refused)
		}
	}
	// A command that needs no container is not refused on Windows.
	if _, err := executeAs(t, "windows", "version"); err != nil {
		t.Errorf("orkctl version on windows: %v", err)
	}
}
