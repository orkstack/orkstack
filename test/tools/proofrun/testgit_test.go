package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// testGitEnv is env for the git a test runs on a repository of its own: no
// configuration but the repository's (a developer's global config may sign
// every commit with a key, or name hooks), no repository but the one it runs
// in, no template a shell names (its hooks would run on every commit), and a
// fixed identity. The same isolation docscheck's isolateGit gives.
func testGitEnv(env []string) []string {
	out := make([]string, 0, len(env)+7)
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		switch {
		case strings.HasPrefix(name, "GIT_CONFIG"), name == "GIT_DIR", name == "GIT_WORK_TREE",
			name == "GIT_INDEX_FILE", name == "GIT_COMMON_DIR", name == "GIT_OBJECT_DIRECTORY",
			name == "GIT_ALTERNATE_OBJECT_DIRECTORIES", name == "GIT_TEMPLATE_DIR":
			continue
		}
		out = append(out, kv)
	}
	return append(out, "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
}

// TestTestGitEnv_ACommitIgnoresTheDevelopersGitConfig: under each polluted
// environment a developer's shell can hold, a commit a test makes in its own
// repository is still made, unsigned and by the test's identity. Each row
// asks git to sign with a key that does not exist, so a leak fails the commit.
func TestTestGitEnv_ACommitIgnoresTheDevelopersGitConfig(t *testing.T) {
	const unset = "\x00unset" // a row's variable to remove, not to set
	signing := "[commit]\n\tgpgsign = true\n[gpg]\n\tformat = ssh\n[user]\n\tsigningkey = /nonexistent/key\n"
	tests := []struct {
		name string
		env  func(t *testing.T) map[string]string
	}{
		{"GIT_CONFIG_GLOBAL names a config that signs every commit", func(t *testing.T) map[string]string {
			path := filepath.Join(t.TempDir(), "gitconfig")
			must.OK(t, os.WriteFile(path, []byte(signing), 0o600))
			return map[string]string{"GIT_CONFIG_GLOBAL": path}
		}},
		{"user.signingkey and gpg.format from the environment", func(t *testing.T) map[string]string {
			return map[string]string{"GIT_CONFIG_COUNT": "3",
				"GIT_CONFIG_KEY_0": "user.signingkey", "GIT_CONFIG_VALUE_0": "/nonexistent/key",
				"GIT_CONFIG_KEY_1": "gpg.format", "GIT_CONFIG_VALUE_1": "ssh",
				"GIT_CONFIG_KEY_2": "commit.gpgsign", "GIT_CONFIG_VALUE_2": "true"}
		}},
		{"commit.gpgsign in the home directory's .gitconfig", func(t *testing.T) map[string]string {
			home := t.TempDir()
			must.OK(t, os.WriteFile(filepath.Join(home, ".gitconfig"), []byte(signing), 0o600))
			return map[string]string{"HOME": home, "XDG_CONFIG_HOME": home, "GIT_CONFIG_GLOBAL": unset}
		}},
		{"GIT_TEMPLATE_DIR installs a hook that refuses every commit", func(t *testing.T) map[string]string {
			tmpl := t.TempDir()
			must.OK(t, os.MkdirAll(filepath.Join(tmpl, "hooks"), 0o755))
			must.OK(t, os.WriteFile(filepath.Join(tmpl, "hooks", "pre-commit"), []byte("#!/bin/sh\nexit 1\n"), 0o755))
			return map[string]string{"GIT_TEMPLATE_DIR": tmpl}
		}},
		{"GIT_DIR names another repository", func(t *testing.T) map[string]string {
			return map[string]string{"GIT_DIR": filepath.Join(t.TempDir(), "elsewhere.git")}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env(t) {
				if v != unset {
					t.Setenv(k, v) // restored when the test ends
					continue
				}
				t.Setenv(k, "") // restored when the test ends
				must.OK(t, os.Unsetenv(k))
			}
			dir := t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				cmd := exec.Command("git", args...)
				cmd.Dir = dir
				cmd.Env = testGitEnv(os.Environ())
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v: %s", args, err, out)
				}
				return strings.TrimSpace(string(out))
			}
			git("init", "-q", "-b", "main")
			must.OK(t, os.WriteFile(filepath.Join(dir, "a"), []byte("1"), 0o644))
			git("add", "a")
			git("commit", "-q", "-m", "chore: a commit")
			if got := git("log", "-1", "--format=%an %G?"); got != "t N" {
				t.Errorf("the commit is %q, want by t and unsigned (t N)", got)
			}
		})
	}
}
