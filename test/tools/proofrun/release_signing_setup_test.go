package main

// scripts/release-signing-setup.sh sets up the key release.yml signs with: it
// generates a cosign key pair, proves it, configures the `release`
// environment and its two secrets, and copies the public key to
// deploy/cosign.pub. These tests run the script as a maintainer would, from a
// checkout of its own (a scratch git repository holding a copy of the script,
// so this tree's deploy/ is never written), with gh and cosign as stubs on
// PATH: nothing reaches GitHub and no real key is made.
//
// Both stubs append their argv to one log, so the order of calls and the
// absence of the password from every argv can be read back. gh keeps the
// repository's state in a directory (the environment, its reviewers, its
// branch policies, each secret's value as it arrived on stdin), so a second
// run sees what the first one did. cosign makes a stub pair that binds the
// private key to a hash of COSIGN_PASSWORD, so a signature made with the wrong
// password, or verified with another pair's public key, fails as the real one
// would.

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const signingGHStub = `#!/usr/bin/env bash
set -uo pipefail
printf 'gh %s\n' "$*" >>"$STUB_LOG"
S="$GH_STATE"
notfound() { echo "gh: Not Found (HTTP 404)" >&2; exit 1; }
case "${1:-} ${2:-}" in
"auth status") exit "${FAKE_AUTH_STATUS:-0}" ;;
"secret set")
	[ -e "$S/env" ] || notfound
	cat >"$S/secret-$3"
	exit 0
	;;
"secret list")
	[ -e "$S/env" ] || notfound
	for f in "$S"/secret-*; do [ -e "$f" ] && basename "$f" | sed 's/^secret-//'; done
	exit 0
	;;
esac
[ "${1:-}" = api ] || { echo "gh stub: unexpected: $*" >&2; exit 1; }
shift
method=GET path="" name=""
while [ $# -gt 0 ]; do
	case "$1" in
	-X) method="$2"; shift 2 ;;
	--input | --jq) shift 2 ;;
	-f) case "$2" in name=*) name="${2#name=}" ;; esac; shift 2 ;;
	*) path="$1"; shift ;;
	esac
done
env="repos/$FAKE_REPO/environments/$FAKE_ENV"
case "$method $path" in
"GET repos/$FAKE_REPO") echo "${FAKE_ADMIN:-true}" ;;
"GET user") echo "maintainer 42" ;;
"GET $env")
	[ -e "$S/env" ] || notfound
	cat "$S/reviewers" 2>/dev/null || true
	;;
"PUT $env")
	n="$(ls "$S" | grep -c '^put-')"
	cat >"$S/put-$n"
	touch "$S/env"
	grep -oE '\{"type":"(User|Team)","id":[0-9]+\}' "$S/put-$n" | sed -E 's/.*"type":"([A-Za-z]+)","id":([0-9]+).*/\1 \2/' >"$S/reviewers"
	echo '{}'
	;;
"GET $env/deployment-branch-policies")
	[ -e "$S/env" ] || notfound
	cat "$S/policies" 2>/dev/null || true
	;;
"POST $env/deployment-branch-policies")
	[ -e "$S/env" ] || notfound
	echo "$name" >>"$S/policies"
	echo '{}'
	;;
*) echo "gh stub: unexpected: $method $path" >&2; exit 1 ;;
esac
`

const signingCosignStub = `#!/usr/bin/env bash
set -uo pipefail
printf 'cosign %s\n' "$*" >>"$STUB_LOG"
hash() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
arg() {
	local want="$1" prev=""
	shift
	for a in "$@"; do
		[ "$prev" = "$want" ] && { printf '%s' "$a"; return; }
		prev="$a"
	done
}
case "${1:-}" in
generate-key-pair)
	[ -n "${COSIGN_PASSWORD:-}" ] || { echo "cosign stub: no COSIGN_PASSWORD" >&2; exit 1; }
	p="$(arg --output-key-prefix "$@")"
	id="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
	printf -- '-----BEGIN ENCRYPTED SIGSTORE PRIVATE KEY-----\n%s %s\n-----END ENCRYPTED SIGSTORE PRIVATE KEY-----\n' "$id" "$(hash "$COSIGN_PASSWORD")" >"$p.key"
	printf -- '-----BEGIN PUBLIC KEY-----\n%s\n-----END PUBLIC KEY-----\n' "$id" >"$p.pub"
	;;
sign-blob)
	key="$(arg --key "$@")" out="$(arg --output-signature "$@")" file="${@: -1}"
	read -r id pw <<<"$(sed -n 2p "$key")"
	[ "$(hash "${COSIGN_PASSWORD:-}")" = "$pw" ] || { echo "cosign stub: decrypt: wrong password" >&2; exit 1; }
	printf 'sig %s %s\n' "$id" "$(sha256sum <"$file" | cut -d' ' -f1)" >"$out"
	;;
verify-blob)
	key="$(arg --key "$@")" sig="$(arg --signature "$@")" file="${@: -1}"
	[ "$(cat "$sig")" = "sig $(sed -n 2p "$key") $(sha256sum <"$file" | cut -d' ' -f1)" ] || { echo "cosign stub: invalid signature" >&2; exit 1; }
	;;
*) echo "cosign stub: unexpected: $*" >&2; exit 1 ;;
esac
`

// signingPassword is what each test types; no argv may ever carry it.
const signingPassword = "correct horse battery staple"

// typed is the password, twice, as a maintainer types it at the prompts.
const typed = signingPassword + "\n" + signingPassword + "\n"

type signingSandbox struct {
	t        *testing.T
	checkout string
	keys     string
	state    string
	log      string
	bin      string
	home     string
}

func newSigningSandbox(t *testing.T) *signingSandbox {
	t.Helper()
	for _, tool := range []string{"git", "sha256sum", "od"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("%s is not installed; the script and its stubs need it", tool)
		}
	}
	dir := t.TempDir()
	s := &signingSandbox{
		t:        t,
		checkout: filepath.Join(dir, "checkout"),
		keys:     filepath.Join(dir, "keys"),
		state:    filepath.Join(dir, "state"),
		log:      filepath.Join(dir, "calls.log"),
		bin:      filepath.Join(dir, "bin"),
		home:     filepath.Join(dir, "home"),
	}
	for _, d := range []string{filepath.Join(s.checkout, "scripts"), filepath.Join(s.checkout, "deploy"), s.state, s.bin, s.home} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script, err := os.ReadFile(filepath.Join(gateRepoRoot(t), "scripts", "release-signing-setup.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.checkout, "scripts", "release-signing-setup.sh"), script, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"gh": signingGHStub, "cosign": signingCosignStub} {
		if err := os.WriteFile(filepath.Join(s.bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("git", "init", "-q", s.checkout)
	cmd.Env = s.env(nil)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	return s
}

func (s *signingSandbox) env(extra map[string]string) []string {
	over := map[string]string{
		"PATH":      s.bin + ":" + os.Getenv("PATH"),
		"HOME":      s.home,
		"STUB_LOG":  s.log,
		"GH_STATE":  s.state,
		"FAKE_REPO": "o/r",
		"FAKE_ENV":  "release",
	}
	for k, v := range extra {
		over[k] = v
	}
	var out []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if _, ok := over[k]; ok || strings.HasPrefix(k, "GIT_") || strings.HasPrefix(k, "COSIGN_") {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range over {
		out = append(out, k+"="+v)
	}
	return out
}

// run invokes the script with the password lines on stdin; it returns what
// the script printed and its exit status.
func (s *signingSandbox) run(extra map[string]string, stdin string, args ...string) (string, int) {
	s.t.Helper()
	argv := append([]string{filepath.Join(s.checkout, "scripts", "release-signing-setup.sh"), "--repo", "o/r", "--dir", s.keys}, args...)
	cmd := exec.Command("bash", argv...)
	cmd.Env = s.env(extra)
	cmd.Dir = s.checkout
	cmd.Stdin = strings.NewReader(stdin)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return out.String(), 0
	case errors.As(err, &exit):
		return out.String(), exit.ExitCode()
	}
	s.t.Fatalf("running the script: %v\n%s", err, out.String())
	return "", 0
}

func (s *signingSandbox) calls() []string { return gateLogLines(s.t, s.log) }

func (s *signingSandbox) read(path string) string {
	s.t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		s.t.Fatal(err)
	}
	return string(b)
}

func (s *signingSandbox) write(path, body string) {
	s.t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		s.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		s.t.Fatal(err)
	}
}

// signingWrites are the calls that change something: a key, a GitHub
// resource, a secret.
func signingWrites(calls []string) []string {
	var out []string
	for _, c := range calls {
		if strings.HasPrefix(c, "cosign generate-key-pair") || strings.Contains(c, "-X PUT") ||
			strings.Contains(c, "-X POST") || strings.HasPrefix(c, "gh secret set") {
			out = append(out, c)
		}
	}
	return out
}

// expectNoSecretInArgv: the password reaches cosign through its environment
// and gh on stdin, never an argument, and is never printed.
func expectNoSecretInArgv(t *testing.T, calls []string, out string) {
	t.Helper()
	for _, c := range calls {
		if strings.Contains(c, signingPassword) {
			t.Errorf("an argv carries the password: %q", c)
		}
	}
	if strings.Contains(out, signingPassword) {
		t.Errorf("the script printed the password:\n%s", out)
	}
}

func TestReleaseSigningSetup_SetsUpTheKeyTheEnvironmentAndTheSecretsInOrder(t *testing.T) {
	s := newSigningSandbox(t)
	out, code := s.run(nil, typed)
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	calls := s.calls()
	expectNoSecretInArgv(t, calls, out)

	// Every read before any write, the pair proved before GitHub is written,
	// and the secrets set only once the environment exists.
	want := []string{
		"gh auth status",
		"gh api repos/o/r --jq .permissions.admin",
		"gh api user --jq",
		"gh api repos/o/r/environments/release --jq",
		"cosign generate-key-pair --output-key-prefix " + filepath.Join(s.keys, "cosign"),
		"cosign sign-blob --key " + filepath.Join(s.keys, "cosign.key") + " --new-bundle-format=false --use-signing-config=false --tlog-upload=false --yes --output-signature",
		"cosign verify-blob --key " + filepath.Join(s.keys, "cosign.pub") + " --insecure-ignore-tlog=true --signature",
		"gh api -X PUT repos/o/r/environments/release --input -",
		"gh api -X POST repos/o/r/environments/release/deployment-branch-policies -f name=main -f type=branch",
		"gh secret set COSIGN_PRIVATE_KEY --env release --repo o/r",
		"gh secret set COSIGN_PASSWORD --env release --repo o/r",
	}
	if len(calls) != len(want) {
		t.Fatalf("calls:\n  %s\nwant, in order, prefixes:\n  %s", strings.Join(calls, "\n  "), strings.Join(want, "\n  "))
	}
	for i := range want {
		if !strings.HasPrefix(calls[i], want[i]) {
			t.Errorf("call %d is %q, want %q…", i+1, calls[i], want[i])
		}
	}

	key, pub := s.read(filepath.Join(s.keys, "cosign.key")), s.read(filepath.Join(s.keys, "cosign.pub"))
	if got := s.read(filepath.Join(s.state, "secret-COSIGN_PRIVATE_KEY")); got != key {
		t.Errorf("COSIGN_PRIVATE_KEY is %q, want the key file %q", got, key)
	}
	if got := s.read(filepath.Join(s.state, "secret-COSIGN_PASSWORD")); got != signingPassword {
		t.Errorf("COSIGN_PASSWORD arrived on stdin as %q, want the password typed", got)
	}
	if got := s.read(filepath.Join(s.checkout, "deploy", "cosign.pub")); got != pub {
		t.Errorf("deploy/cosign.pub is %q, want the new public key %q", got, pub)
	}
	put := s.read(filepath.Join(s.state, "put-0"))
	for _, want := range []string{`"protected_branches":false`, `"custom_branch_policies":true`, `"reviewers":[{"type":"User","id":42}]`} {
		if !strings.Contains(put, want) {
			t.Errorf("the environment was written as %s, without %s", put, want)
		}
	}
	for path, mode := range map[string]os.FileMode{s.keys: 0o700, filepath.Join(s.keys, "cosign.key"): 0o600} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != mode {
			t.Errorf("%s: %v, want mode %v", path, err, mode)
		}
	}
	if !strings.Contains(out, "Commit deploy/cosign.pub through a pull request") || !strings.Contains(out, "offline") {
		t.Errorf("the script does not say what is left to do:\n%s", out)
	}
}

func TestReleaseSigningSetup_RefusesBeforeItWritesAnything(t *testing.T) {
	for _, c := range []struct {
		name   string
		setup  func(s *signingSandbox) []string // extra arguments
		env    map[string]string
		stdin  string
		code   int
		saying string
	}{
		{"a key directory inside a git work tree", func(s *signingSandbox) []string {
			return []string{"--dir", filepath.Join(s.checkout, "keys")}
		}, nil, typed, 1, "inside the git work tree"},
		{"an existing private key file", func(s *signingSandbox) []string {
			s.write(filepath.Join(s.keys, "cosign.key"), "keep me\n")
			return nil
		}, nil, typed, 1, "is never overwritten"},
		{"an existing public key file", func(s *signingSandbox) []string {
			s.write(filepath.Join(s.keys, "cosign.pub"), "keep me\n")
			return nil
		}, nil, typed, 1, "is never overwritten"},
		{"a key already in the environment", func(s *signingSandbox) []string {
			s.write(filepath.Join(s.state, "env"), "")
			s.write(filepath.Join(s.state, "secret-COSIGN_PRIVATE_KEY"), "old key\n")
			return nil
		}, nil, typed, 1, "already holds COSIGN_PRIVATE_KEY; pass --rotate"},
		{"a committed public key", func(s *signingSandbox) []string {
			s.write(filepath.Join(s.checkout, "deploy", "cosign.pub"), "old\n")
			return nil
		}, nil, typed, 1, "deploy/cosign.pub is already committed; pass --rotate"},
		{"another branch admitted", func(s *signingSandbox) []string {
			s.write(filepath.Join(s.state, "env"), "")
			s.write(filepath.Join(s.state, "policies"), "main\ndev\n")
			return nil
		}, nil, typed, 1, "also admits deployments from: dev"},
		{"a caller who cannot administer", nil, map[string]string{"FAKE_ADMIN": "false"}, typed, 1, "cannot administer o/r"},
		{"gh signed out", nil, map[string]string{"FAKE_AUTH_STATUS": "1"}, typed, 77, "gh is not signed in"},
		{"two different passwords", nil, nil, signingPassword + "\nanother\n", 1, "the two passwords differ"},
		{"an empty password", nil, nil, "\n\n", 1, "an empty password"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newSigningSandbox(t)
			var args []string
			if c.setup != nil {
				args = c.setup(s)
			}
			_, committed := os.Stat(filepath.Join(s.checkout, "deploy", "cosign.pub"))
			out, code := s.run(c.env, c.stdin, args...)
			if code != c.code || !strings.Contains(out, c.saying) {
				t.Fatalf("exit %d, want %d saying %q:\n%s", code, c.code, c.saying, out)
			}
			if w := signingWrites(s.calls()); len(w) > 0 {
				t.Errorf("it wrote before refusing:\n  %s", strings.Join(w, "\n  "))
			}
			if _, err := os.Stat(filepath.Join(s.checkout, "deploy", "cosign.pub")); err == nil && committed != nil {
				t.Error("it wrote deploy/cosign.pub before refusing")
			}
			expectNoSecretInArgv(t, s.calls(), out)
		})
	}
}

func TestReleaseSigningSetup_ASecondRunRepeatsNothingAndRotationExplainsItself(t *testing.T) {
	s := newSigningSandbox(t)
	if out, code := s.run(nil, typed); code != 0 {
		t.Fatalf("first run: exit %d:\n%s", code, out)
	}
	first := s.read(filepath.Join(s.checkout, "deploy", "cosign.pub"))

	// The same run again: nothing is replaced without --rotate, whose
	// refusal says what rotation costs the deployments that trust the key.
	before := len(s.calls())
	out, code := s.run(nil, typed)
	if code != 1 || !strings.Contains(out, "every deployment that trusts OrkStack's releases adds it to its image policy") {
		t.Fatalf("second run: exit %d, want a refusal that explains rotation:\n%s", code, out)
	}
	if w := signingWrites(s.calls()[before:]); len(w) > 0 {
		t.Errorf("the second run wrote before refusing:\n  %s", strings.Join(w, "\n  "))
	}

	// --rotate with the old pair still in --dir: a key file is never
	// overwritten.
	before = len(s.calls())
	if out, code := s.run(nil, typed, "--rotate"); code != 1 || !strings.Contains(out, "is never overwritten") {
		t.Fatalf("--rotate over the old pair: exit %d:\n%s", code, out)
	}
	if w := signingWrites(s.calls()[before:]); len(w) > 0 {
		t.Errorf("--rotate over the old pair wrote:\n  %s", strings.Join(w, "\n  "))
	}

	// --rotate into a fresh directory: a new pair, the environment updated in
	// place, its branch policy not added twice, the caller a reviewer once.
	before = len(s.calls())
	fresh := filepath.Join(filepath.Dir(s.keys), "keys-2")
	out, code = s.run(nil, typed, "--rotate", "--dir", fresh)
	if code != 0 {
		t.Fatalf("--rotate: exit %d:\n%s", code, out)
	}
	calls := s.calls()[before:]
	expectNoSecretInArgv(t, calls, out)
	for _, c := range calls {
		if strings.Contains(c, "-X POST") {
			t.Errorf("the rotation added the branch policy again: %q", c)
		}
	}
	if got := s.read(filepath.Join(s.state, "policies")); got != "main\n" {
		t.Errorf("the environment's branch policies are %q, want main once", got)
	}
	if put := s.read(filepath.Join(s.state, "put-1")); !strings.Contains(put, `"reviewers":[{"type":"User","id":42}]`) {
		t.Errorf("the second write of the environment is %s; want the caller as its one reviewer", put)
	}
	second := s.read(filepath.Join(s.checkout, "deploy", "cosign.pub"))
	if second == first || second != s.read(filepath.Join(fresh, "cosign.pub")) {
		t.Errorf("deploy/cosign.pub after --rotate is %q; want the new key, not the old %q", second, first)
	}
	if got := s.read(filepath.Join(s.state, "secret-COSIGN_PRIVATE_KEY")); got != s.read(filepath.Join(fresh, "cosign.key")) {
		t.Error("COSIGN_PRIVATE_KEY is not the rotated key")
	}
	if !strings.Contains(out, "Then tell deployments") {
		t.Errorf("a rotation does not tell the maintainer to warn deployments:\n%s", out)
	}
}

func TestReleaseSigningSetup_KeepsOtherReviewersAndCanLeaveTheCallerOut(t *testing.T) {
	s := newSigningSandbox(t)
	s.write(filepath.Join(s.state, "env"), "")
	s.write(filepath.Join(s.state, "reviewers"), "Team 7\nUser 42\n")
	if out, code := s.run(nil, typed); code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if put := s.read(filepath.Join(s.state, "put-0")); !strings.Contains(put, `"reviewers":[{"type":"Team","id":7},{"type":"User","id":42}]`) {
		t.Errorf("the environment was written as %s; want the team kept and the caller once", put)
	}

	s = newSigningSandbox(t)
	if out, code := s.run(nil, typed, "--no-reviewer"); code != 0 {
		t.Fatalf("--no-reviewer: exit %d:\n%s", code, out)
	}
	if put := s.read(filepath.Join(s.state, "put-0")); strings.Contains(put, "reviewers") {
		t.Errorf("--no-reviewer wrote reviewers: %s", put)
	}
}

func TestReleaseSigningSetup_AKeyThePasswordDoesNotOpenStopsBeforeGitHub(t *testing.T) {
	s := newSigningSandbox(t)
	// A cosign whose generate step binds the key to another password: the
	// proof's signature fails, as with a key the password does not open.
	stub := strings.Replace(signingCosignStub, `"$(hash "$COSIGN_PASSWORD")" >"$p.key"`, `"$(hash other)" >"$p.key"`, 1)
	if stub == signingCosignStub {
		t.Fatal("the stub no longer has the line this test replaces")
	}
	if err := os.WriteFile(filepath.Join(s.bin, "cosign"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	out, code := s.run(nil, typed)
	if code != 1 || !strings.Contains(out, "could not sign with the new key") {
		t.Fatalf("exit %d, want the proof to fail:\n%s", code, out)
	}
	for _, c := range s.calls() {
		if strings.HasPrefix(c, "gh secret set") || strings.Contains(c, "-X PUT") || strings.Contains(c, "-X POST") {
			t.Errorf("GitHub was written after the proof failed: %q", c)
		}
	}
	if _, err := os.Stat(filepath.Join(s.checkout, "deploy", "cosign.pub")); err == nil {
		t.Error("deploy/cosign.pub was written after the proof failed")
	}
}

func TestReleaseSigningSetup_ADryRunRunsNothing(t *testing.T) {
	s := newSigningSandbox(t)
	out, code := s.run(nil, "", "--dry-run")
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if calls := s.calls(); len(calls) > 0 {
		t.Errorf("a dry run ran:\n  %s", strings.Join(calls, "\n  "))
	}
	for _, path := range []string{s.keys, filepath.Join(s.checkout, "deploy", "cosign.pub")} {
		if _, err := os.Stat(path); err == nil {
			t.Errorf("a dry run wrote %s", path)
		}
	}
	for _, want := range []string{
		"would check that gh is signed in",
		"would generate the key pair",
		"would prove the pair",
		"would create or update environment release: deployments from main only, you a required reviewer",
		"would allow deployments to release from main",
		"would set release's COSIGN_PRIVATE_KEY",
		"would set release's COSIGN_PASSWORD (on stdin)",
		"would copy",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the dry run does not say %q:\n%s", want, out)
		}
	}
}

func TestReleaseSigningSetup_RefusesAnInvalidInvocation(t *testing.T) {
	s := newSigningSandbox(t)
	for _, args := range [][]string{{"--repo", "not-a-repo"}, {"--env", "bad env"}, {"--bogus"}, {"--dir"}} {
		if out, code := s.run(nil, typed, args...); code != 2 {
			t.Errorf("%v: exit %d, want 2:\n%s", args, code, out)
		}
	}
	if calls := s.calls(); len(calls) > 0 {
		t.Errorf("an invalid invocation ran:\n  %s", strings.Join(calls, "\n  "))
	}
}
