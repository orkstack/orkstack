package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// runConfirming runs orkctl against baseURL with in as standard input and
// the terminal seam answering terminal, so both sides of the "is a person
// there" check can be driven.
func runConfirming(t *testing.T, baseURL string, in io.Reader, terminal bool, args ...string) result {
	t.Helper()
	root, flags := newRootCommand()
	defer flags.closeIdleConnections()
	flags.terminal = func() bool { return terminal }
	root.SetArgs(append(append([]string(nil), args...), "--server", baseURL))
	root.SetIn(in)
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	code := exitOK
	if err := root.ExecuteContext(context.Background()); err != nil {
		stderr.WriteString("Error: " + err.Error() + "\n")
		code = exitCodeFor(err)
	}
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

// destructiveCase is one command that must confirm: how to build a fixture
// it can succeed against, its arguments, the RPC that does the damage, what
// its refusal names and what its prompt states.
type destructiveCase struct {
	name    string
	fixture func(t *testing.T) *fixture
	args    []string
	rpc     string
	action  string
	prompt  []string
}

func destructiveCases() []destructiveCase {
	promotable := func(t *testing.T) *fixture {
		useReleaseClock(t, releaseClockStart)
		fleet := releaseFleetFor()
		fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-c3", LastSeen: releaseClockStart})
		fleet.described["orker-c3"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-c3", Pool: "build", BuildId: relC, MaxConcurrentPins: 1}
		return newReleaseFixture(t, newScriptedReleases(rolloutStep()), fleet)
	}
	rollbackable := func(t *testing.T) *fixture {
		useReleaseClock(t, releaseClockStart)
		fleet := releaseFleetFor()
		fleet.presence = append(fleet.presence, engine.OrkerPresence{Identity: "orker-p2", LastSeen: releaseClockStart})
		fleet.described["orker-p2"] = &orkstackv1.DescribeOrkerResult{OrkerId: "orker-p2", Pool: "gpu", BuildId: relP}
		return newReleaseFixture(t, newScriptedReleases(rolloutStep()), fleet)
	}
	return []destructiveCase{
		{
			name:    "orkflow delete",
			fixture: func(t *testing.T) *fixture { f, _ := deleteFixture(t); return f },
			args:    []string{"orkflow", "delete", deleteTestName, deleteNamespaceFlag, deleteTestNamespace},
			rpc:     "/orkstack.v1.OrkflowService/DeleteOrkflow",
			action:  `delete orkflow "hello-ork"`,
			prompt:  []string{"cannot be reversed", `Delete orkflow "hello-ork" in namespace "payments"?`},
		},
		{
			name: "namespace delete",
			fixture: func(t *testing.T) *fixture {
				f := newFixture(t)
				f.run("namespace", "create", "scratch").requireExit(t, exitOK)
				return f
			},
			args:   []string{"namespace", "delete", "scratch"},
			rpc:    "/orkstack.v1.NamespaceService/DeleteNamespace",
			action: `delete namespace "scratch"`,
			prompt: []string{`Delete namespace "scratch"?`, "refused while any orkflow is still stored in it"},
		},
		{
			name:    "cache purge",
			fixture: func(t *testing.T) *fixture { return cacheStatsFixture(t, true) },
			args:    []string{"cache", "purge", "--orkflow", "cached-pipeline", "--task", "train", "--older-than", "48h"},
			rpc:     "/orkstack.v1.CacheService/PurgeCache",
			action:  "purge cache memos",
			prompt: []string{`the orkflow-scoped memos of orkflow "cached-pipeline", task "train", in namespace "default"`,
				"except those written in the last 48h", "the next run of each of those cached tasks runs its container"},
		},
		{
			name:    "release promote",
			fixture: promotable,
			args:    []string{"release", "promote", relC},
			rpc:     "/orkstack.v1.SystemService/PromoteRelease",
			action:  `promote release "` + relC + `"`,
			prompt:  []string{"Promote release " + relC + "?", "current " + relA + " → " + relC, "every new run is routed to " + relC},
		},
		{
			name:    "release rollback",
			fixture: rollbackable,
			args:    []string{"release", "rollback", relP},
			rpc:     "/orkstack.v1.SystemService/PromoteRelease",
			action:  `roll back to release "` + relP + `"`,
			prompt:  []string{"Roll back to release " + relP + "?", "current " + relA + " → " + relP, "every new run is routed to " + relP},
		},
		{
			name:    "admin retention set (lowering)",
			fixture: func(t *testing.T) *fixture { return retentionFixture(t, &retentionAdmin{}, 72*time.Hour) },
			args:    []string{"admin", "retention", "set", "24h"},
			rpc:     "/orkstack.v1.SystemService/UpdateRetentionPolicy",
			action:  "lower the maximum run retention",
			prompt: []string{"Lowering the maximum run retention from 72h to 24h",
				"every finished run older than 24h\nin a namespace without its own run retention",
				"logs, attempt records, snapshots and commits", "next cleanup pass"},
		},
	}
}

func called(f *fixture, rpc string) int {
	n := 0
	for _, path := range f.requests() {
		if path == rpc {
			n++
		}
	}
	return n
}

// TestDestructiveCommands_Confirm: every command that cannot be undone asks
// first, refuses outright when no person can answer, treats anything but
// "yes" as a cancellation (exit 1), and does nothing until it is answered.
func TestDestructiveCommands_Confirm(t *testing.T) {
	for _, tc := range destructiveCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("no terminal refuses without the request", func(t *testing.T) {
				f := tc.fixture(t)
				got := runConfirming(t, f.baseURL, strings.NewReader("yes\n"), false, tc.args...).requireExit(t, exitError)
				got.requireStderrContains(t, "refusing to "+tc.action+
					" without confirmation: standard input is not a terminal; pass --yes")
				if n := called(f, tc.rpc); n != 0 {
					t.Fatalf("%s was called %d times; a refusal must send nothing", tc.rpc, n)
				}
			})
			t.Run("--json does not bypass the refusal", func(t *testing.T) {
				f := tc.fixture(t)
				got := runConfirming(t, f.baseURL, strings.NewReader(""), false, append([]string{"--json"}, tc.args...)...).
					requireExit(t, exitError)
				if got.stdout != "" {
					t.Fatalf("stdout = %q, want nothing", got.stdout)
				}
				if n := called(f, tc.rpc); n != 0 {
					t.Fatalf("%s was called %d times under --json without --yes", tc.rpc, n)
				}
			})
			t.Run("yes at the terminal proceeds", func(t *testing.T) {
				f := tc.fixture(t)
				got := runConfirming(t, f.baseURL, strings.NewReader("yes\n"), true, tc.args...).requireExit(t, exitOK)
				got.requireStderrContains(t, tc.prompt...)
				got.requireStderrContains(t, "Type 'yes' to confirm:")
				if n := called(f, tc.rpc); n != 1 {
					t.Fatalf("%s was called %d times, want once", tc.rpc, n)
				}
			})
			t.Run("--yes proceeds without asking", func(t *testing.T) {
				f := tc.fixture(t)
				got := runConfirming(t, f.baseURL, &failOnRead{}, false, append(tc.args, "--yes")...).requireExit(t, exitOK)
				if strings.Contains(got.stderr, "Type 'yes'") {
					t.Fatalf("--yes still asked:\n%s", got.stderr)
				}
				if n := called(f, tc.rpc); n != 1 {
					t.Fatalf("%s was called %d times, want once", tc.rpc, n)
				}
			})
			for _, answer := range []string{"no\n", "\n", ""} {
				t.Run("answer "+strings.TrimSpace(answer)+" cancels", func(t *testing.T) {
					f := tc.fixture(t)
					got := runConfirming(t, f.baseURL, strings.NewReader(answer), true, tc.args...).requireExit(t, exitError)
					got.requireStderrContains(t, tc.action+": cancelled")
					if got.stdout != "" {
						t.Fatalf("stdout = %q, want nothing for a cancelled command", got.stdout)
					}
					if n := called(f, tc.rpc); n != 0 {
						t.Fatalf("%s was called %d times after a cancellation", tc.rpc, n)
					}
				})
			}
		})
	}
}

// TestAdminRetentionSet_RaisingDoesNotAsk: only lowering the maximum loses
// data, so raising it (or setting it to what it already is) asks nothing,
// even with no terminal.
func TestAdminRetentionSet_RaisingDoesNotAsk(t *testing.T) {
	for _, value := range []string{"168h", "72h"} {
		f := retentionFixture(t, &retentionAdmin{}, 72*time.Hour)
		got := runConfirming(t, f.baseURL, &failOnRead{}, false, "admin", "retention", "set", value).requireExit(t, exitOK)
		if strings.Contains(got.stderr, "Type 'yes'") {
			t.Fatalf("set %s asked for confirmation:\n%s", value, got.stderr)
		}
		if n := called(f, "/orkstack.v1.SystemService/UpdateRetentionPolicy"); n != 1 {
			t.Fatalf("set %s: UpdateRetentionPolicy called %d times, want once", value, n)
		}
	}
}

// TestStdinIsTerminal_RefusesAPipe: the real check (no seam) reads a pipe as
// no terminal.
func TestStdinIsTerminal_RefusesAPipe(t *testing.T) {
	g := &globalFlags{}
	root, _ := newRootCommand()
	root.SetIn(strings.NewReader("yes\n"))
	if g.stdinIsTerminal(root) {
		t.Fatal("a strings.Reader was taken for a terminal")
	}
}
