package main

// The GitHub merge gate is a shell script that runs gh, git, jq and go. These
// tests run it as an operator would: with those four tools on PATH. Every tool
// but bash and jq is a stub written here, so nothing talks to GitHub, and the
// embedded `go test` cannot recurse.
//
// The stubs answer for the repository o/r. gh answers from a map of argv glob
// to response file, git reads its trees and files from FAKE_* variables, and
// go only reports FAKE_GO_STATUS.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const (
	gateMainRuleset   = "scripts/github/ruleset-main.json"
	gateOwnersRuleset = "scripts/github/ruleset-owners.json"
	gateQueueRuleset  = "scripts/github/ruleset-queue.json"
)

// gateGHStub records every call, records each body posted with --input, and
// answers the first mapping row whose pattern matches the whole argv. The
// response file is printed, through jq when the call passes --jq.
const gateGHStub = `#!/usr/bin/env bash
set -uo pipefail
printf '%s\n' "$*" >>"$GH_LOG"

input=""
prev=""
for arg in "$@"; do
	if [ "$prev" = "--input" ]; then input="$arg"; fi
	prev="$arg"
done
if [ -n "$input" ]; then
	head=""
	for arg in "$@"; do
		if [ "$arg" = "--input" ]; then break; fi
		head="${head:+$head }$arg"
	done
	if command -v sha256sum >/dev/null 2>&1; then
		sum="$(sha256sum "$input" | cut -d' ' -f1)"
	else
		sum="$(shasum -a 256 "$input" | cut -d' ' -f1)"
	fi
	printf '%s\t%s\t%s\n' "$head" "$input" "$sum" >>"$GH_INPUTS"
fi

found=0
response=""
status=0
while IFS=$'\t' read -r pattern file st; do
	if [ -z "${pattern:-}" ]; then continue; fi
	if [[ "$*" == $pattern ]]; then
		response="$file"
		status="${st:-0}"
		found=1
		break
	fi
done <"$GH_MAP"
if [ "$found" -ne 1 ]; then
	printf 'gh stub: no mapping for: %s\n' "$*" >&2
	exit 1
fi

expr=""
prev=""
for arg in "$@"; do
	if [ "$prev" = "--jq" ]; then expr="$arg"; fi
	prev="$arg"
done
if [ -n "$response" ]; then
	if [ -n "$expr" ]; then
		jq -r "$expr" "$response"
	else
		cat "$response"
	fi
fi
exit "$status"
`

const gateGitStub = `#!/usr/bin/env bash
set -uo pipefail
printf '%s\n' "$*" >>"$GIT_LOG"
case "$*" in
"rev-parse origin/main^{tree}")
	printf '%s\n' "${FAKE_ORIGIN_TREE:-}"
	;;
"rev-parse main^{tree}")
	printf '%s\n' "${FAKE_LOCAL_TREE:-}"
	;;
"rev-parse "*)
	printf '%s\n' "${FAKE_GIT_DEFAULT:-}"
	;;
"fetch origin main"*)
	;;
"cat-file -e origin/main:.github/workflows/proof.yml")
	[ "${FAKE_PROOF_YML_EXISTS:-0}" = 1 ] || exit 1
	;;
"cat-file -e origin/main:.github/CODEOWNERS")
	[ "${FAKE_CODEOWNERS_EXISTS:-0}" = 1 ] || exit 1
	;;
*)
	printf '%s\n' "${FAKE_GIT_DEFAULT:-}"
	;;
esac
`

const gateGoStub = `#!/usr/bin/env bash
# The gate runs the proof workflow test; a stub keeps it from recursing.
exit "${FAKE_GO_STATUS:-0}"
`

// gateSandbox is one invocation of the gate against the stubs.
type gateSandbox struct {
	t        *testing.T
	root     string
	dir      string
	bin      string
	mapFile  string
	ghLog    string
	ghInputs string
	gitLog   string
	rows     []string
	env      map[string]string
}

func newGateSandbox(t *testing.T) *gateSandbox {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed, and the gate refuses to run without it")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &gateSandbox{
		t:        t,
		root:     gateRepoRoot(t),
		dir:      dir,
		bin:      bin,
		mapFile:  filepath.Join(dir, "gh.map"),
		ghLog:    filepath.Join(dir, "gh.log"),
		ghInputs: filepath.Join(dir, "gh.inputs"),
		gitLog:   filepath.Join(dir, "git.log"),
		env: map[string]string{
			"APPLY":                  "0",
			"QUEUE":                  "0",
			"CODEOWNER_REVIEW":       "1",
			"FAKE_ORIGIN_TREE":       "tree-main",
			"FAKE_LOCAL_TREE":        "tree-main",
			"FAKE_PROOF_YML_EXISTS":  "1",
			"FAKE_CODEOWNERS_EXISTS": "1",
			"FAKE_GO_STATUS":         "0",
		},
	}
	for name, body := range map[string]string{"gh": gateGHStub, "git": gateGitStub, "go": gateGoStub} {
		path := filepath.Join(bin, name)
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatalf("writing the %s stub: %v", name, err)
		}
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// answer maps an argv glob to a body, delivered on stdout with exit 0.
func (s *gateSandbox) answer(pattern, body string) {
	s.t.Helper()
	s.answerStatus(pattern, body, 0)
}

// answerStatus is answer with an explicit exit status, for a call the real gh
// would fail, such as a missing Actions variable.
func (s *gateSandbox) answerStatus(pattern, body string, status int) {
	s.t.Helper()
	path := filepath.Join(s.dir, fmt.Sprintf("response-%d", len(s.rows)))
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		s.t.Fatal(err)
	}
	row := pattern + "\t" + path
	if status != 0 {
		row += fmt.Sprintf("\t%d", status)
	}
	s.rows = append(s.rows, row)
}

func (s *gateSandbox) flush() {
	s.t.Helper()
	body := strings.Join(s.rows, "\n")
	if body != "" {
		body += "\n"
	}
	if err := os.WriteFile(s.mapFile, []byte(body), 0o644); err != nil {
		s.t.Fatal(err)
	}
}

func (s *gateSandbox) childEnv(extra map[string]string) []string {
	over := map[string]string{
		"PATH":      s.bin + ":" + os.Getenv("PATH"),
		"GH_MAP":    s.mapFile,
		"GH_LOG":    s.ghLog,
		"GH_INPUTS": s.ghInputs,
		"GIT_LOG":   s.gitLog,
	}
	for k, v := range s.env {
		over[k] = v
	}
	for k, v := range extra {
		over[k] = v
	}
	out := make([]string, 0, len(os.Environ())+len(over))
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			if _, ok := over[kv[:i]]; ok {
				continue
			}
		}
		out = append(out, kv)
	}
	for k, v := range over {
		out = append(out, k+"="+v)
	}
	return out
}

// run invokes the script with the sandbox environment and extra overrides.
func (s *gateSandbox) run(extra map[string]string, args ...string) (string, int) {
	s.t.Helper()
	s.flush()
	script := filepath.Join(s.root, "scripts", "github-merge-gate.sh")
	cmd := exec.Command("bash", append([]string{script}, args...)...)
	cmd.Env = s.childEnv(extra)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			s.t.Fatalf("running the gate: %v\n%s", err, out.String())
		}
		code = exit.ExitCode()
	}
	return out.String(), code
}

func (s *gateSandbox) ghCalls() []string { return gateLogLines(s.t, s.ghLog) }

// gatePost is one body the gate posted with gh api --input.
type gatePost struct {
	argv string
	file string
	sha  string
}

func (s *gateSandbox) posts() []gatePost {
	s.t.Helper()
	var out []gatePost
	for _, line := range gateLogLines(s.t, s.ghInputs) {
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			s.t.Fatalf("malformed posted-body record %q", line)
		}
		out = append(out, gatePost{argv: parts[0], file: s.postedFile(parts[1]), sha: parts[2]})
	}
	return out
}

func (s *gateSandbox) postedSummary() string {
	var lines []string
	for _, p := range s.posts() {
		lines = append(lines, p.argv+" < "+p.file)
	}
	if len(lines) == 0 {
		return "  (nothing posted)"
	}
	return "  " + strings.Join(lines, "\n  ")
}

func gateRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func gateRepoPath(t *testing.T, rel string) string {
	t.Helper()
	return filepath.Join(gateRepoRoot(t), filepath.FromSlash(rel))
}

// gateCommitted is a file of the tree the script applies.
func gateCommitted(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(gateRepoPath(t, rel))
	if err != nil {
		t.Fatalf("reading %s: %v", rel, err)
	}
	return b
}

func gateLogLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// gateFindPosted returns the argv that posted a body equal to rel's committed
// bytes, and those bytes. The stub's digest must match the body, so a test
// never compares against a file the gate did not hand to gh.
func gateFindPosted(t *testing.T, s *gateSandbox, rel string) (string, []byte) {
	t.Helper()
	want := gateCommitted(t, rel)
	for _, p := range s.posts() {
		body, err := os.ReadFile(p.file)
		if err != nil {
			t.Fatalf("reading the posted body %s: %v", p.file, err)
		}
		if sum := bytesSHA256(body); sum != p.sha {
			t.Fatalf("the body at %s hashes to %s, but the gh stub recorded %s", p.file, sum, p.sha)
		}
		if bytes.Equal(body, want) {
			return p.argv, body
		}
	}
	t.Fatalf("no posted body equals %s; posted:\n%s", rel, s.postedSummary())
	return "", nil
}

// gateRefuseWrites fails when the gate wrote anything through gh.
func gateRefuseWrites(t *testing.T, s *gateSandbox) {
	t.Helper()
	for _, call := range s.ghCalls() {
		for _, method := range []string{"--method POST", "--method PUT", "--method PATCH"} {
			if strings.Contains(call, method) {
				t.Errorf("gh was asked to write: %s", call)
			}
		}
	}
}

func gateFailures(out string) []string {
	var fails []string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "FAIL: ") {
			fails = append(fails, line)
		}
	}
	return fails
}

// gateState is what the stubs report about the repository; an empty string
// takes the passing default.
type gateState struct {
	visibility string // public
	ownerType  string // User
	nightly    string // active
	rulesets   string // []
	modeValue  string // unset (the variable does not exist)
}

// postedFile resolves a recorded body path. The gate passes the committed
// rulesets by relative path, and the stubs record it from the repository root
// the script cds to.
func (s *gateSandbox) postedFile(file string) string {
	if filepath.IsAbs(file) {
		return file
	}
	return filepath.Join(s.root, file)
}

// gateCheckable answers every read the script makes before it applies
// anything. Each test starts from this passing scenario and changes one fact.
func gateCheckable(t *testing.T, st gateState) *gateSandbox {
	t.Helper()
	if st.visibility == "" {
		st.visibility = "public"
	}
	if st.ownerType == "" {
		st.ownerType = "User"
	}
	if st.nightly == "" {
		st.nightly = "active"
	}
	if st.rulesets == "" {
		st.rulesets = "[]"
	}
	s := newGateSandbox(t)
	s.answer("auth status", "")
	s.answer("repo view --json owner --jq .owner.login", `{"owner":{"login":"o"}}`)
	s.answer("repo view --json name --jq .name", `{"name":"r"}`)
	s.answer("api repos/o/r", fmt.Sprintf(
		`{"visibility":%q,"owner":{"type":%q},"permissions":{"admin":true},"allow_merge_commit":true}`,
		st.visibility, st.ownerType))
	s.answer("api repos/o/r/actions/permissions --jq .enabled // false", `{"enabled":true}`)
	s.answer("api repos/o/r/actions/workflows/nightly.yml", fmt.Sprintf(`{"state":%q}`, st.nightly))
	s.answer("api repos/o/r/commits/origin/main/check-runs",
		`{"check_runs":[{"name":"proof","app":{"id":15368},"conclusion":"success","details_url":"https://github.com/o/r/actions/runs/7"}]}`)
	s.answer("api repos/o/r/rulesets", st.rulesets)
	if st.modeValue == "" {
		s.answerStatus("api repos/o/r/actions/variables/MERGE_GATE_MODE", "", 1)
	} else {
		s.answer("api repos/o/r/actions/variables/MERGE_GATE_MODE", fmt.Sprintf(`{"value":%q}`, st.modeValue))
	}
	return s
}

// gateAcceptsWrites answers the idempotent writes the apply mode makes;
// existing is the rulesets list the id lookup sees.
func gateAcceptsWrites(s *gateSandbox, existing string) {
	if existing == "" {
		existing = "[]"
	}
	s.answer("api repos/o/r/rulesets --jq *", existing)
	s.answer("api --method POST repos/o/r/rulesets --input *", "")
	s.answer("api --method PUT repos/o/r/rulesets/* --input *", "")
	s.answer("api --method DELETE repos/o/r/actions/variables/MERGE_GATE_MODE", "")
	s.answer("api --method POST repos/o/r/actions/variables -f *", "")
	s.answer("api --method PATCH repos/o/r/actions/variables/MERGE_GATE_MODE -f *", "")
}

func TestGitHubMergeGate_RefusesAPrivateRepository(t *testing.T) {
	s := gateCheckable(t, gateState{visibility: "private"})
	out, code := s.run(nil)
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	if !strings.Contains(out, "rulesets are refused on a private repository") {
		t.Errorf("output does not explain the refusal:\n%s", out)
	}
}

func TestGitHubMergeGate_AppliesTheQueueLessModeOnAUserOwnedPublicRepository(t *testing.T) {
	s := gateCheckable(t, gateState{})
	gateAcceptsWrites(s, "")
	out, code := s.run(map[string]string{"APPLY": "1"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}

	argv, posted := gateFindPosted(t, s, gateMainRuleset)
	if !strings.Contains(argv, "--method POST") {
		t.Errorf("the main ruleset was posted with %q, want --method POST", argv)
	}

	var body struct {
		Rules []struct {
			Type       string `json:"type"`
			Parameters struct {
				Strict bool `json:"strict_required_status_checks_policy"`
			} `json:"parameters"`
		} `json:"rules"`
	}
	if err := json.Unmarshal(posted, &body); err != nil {
		t.Fatalf("%s: %v", gateMainRuleset, err)
	}
	strict, queue := false, false
	for _, rule := range body.Rules {
		if rule.Type == "required_status_checks" && rule.Parameters.Strict {
			strict = true
		}
		if rule.Type == "merge_queue" {
			queue = true
		}
	}
	if !strict {
		t.Error("the posted queue-less body is not strict")
	}
	if queue {
		t.Error("the posted queue-less body carries a merge_queue rule")
	}
	for _, call := range s.ghCalls() {
		if strings.Contains(call, "--method POST") && strings.Contains(call, "actions/variables") {
			t.Errorf("the queue-less apply set an Actions variable: %s", call)
		}
	}
}

func TestGitHubMergeGate_RefusesTheQueueOnAUserOwnedRepository(t *testing.T) {
	s := gateCheckable(t, gateState{modeValue: "queue"})
	out, code := s.run(map[string]string{"APPLY": "1", "QUEUE": "1"})
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	for _, want := range []string{"needs an Organization-owned repository", "convert", "transfer"} {
		if !strings.Contains(out, want) {
			t.Errorf("output does not offer %q:\n%s", want, out)
		}
	}
	if fails := gateFailures(out); len(fails) != 1 {
		t.Errorf("%d failures, want only the ownership one:\n%s", len(fails), strings.Join(fails, "\n"))
	}
	gateRefuseWrites(t, s)
}

func TestGitHubMergeGate_RefusesAModeVariableThatDisagreesWithTheRuleset(t *testing.T) {
	s := gateCheckable(t, gateState{modeValue: "queue"})
	out, code := s.run(nil)
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	fails := gateFailures(out)
	if len(fails) != 1 || !strings.Contains(fails[0], "MERGE_GATE_MODE=queue") {
		t.Errorf("want only the mode disagreement, got:\n%s", strings.Join(fails, "\n"))
	}
}

func TestGitHubMergeGate_RefusesWhenOriginMainIsNotTheMergedTree(t *testing.T) {
	s := gateCheckable(t, gateState{})
	out, code := s.run(map[string]string{"FAKE_ORIGIN_TREE": "tree-other"})
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	fails := gateFailures(out)
	if len(fails) != 1 || !strings.Contains(fails[0], "is not local main") {
		t.Errorf("want only the tree mismatch, got:\n%s", strings.Join(fails, "\n"))
	}
}

func TestGitHubMergeGate_CheckModeOnlyReads(t *testing.T) {
	s := gateCheckable(t, gateState{})
	out, code := s.run(map[string]string{"APPLY": "0"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	if !strings.Contains(out, "every precondition holds") {
		t.Errorf("check mode did not report a passing scenario:\n%s", out)
	}
	gateRefuseWrites(t, s)
}

func TestGitHubMergeGate_ReportsADisabledNightly(t *testing.T) {
	s := gateCheckable(t, gateState{nightly: "disabled_manually"})
	out, code := s.run(nil)
	if code != 0 {
		t.Fatalf("exit = %d, want 0: a disabled nightly is a warning\n%s", code, out)
	}
	for _, want := range []string{"nightly.yml is disabled_manually", "gh workflow enable nightly.yml"} {
		if !strings.Contains(out, want) {
			t.Errorf("output does not say %q:\n%s", want, out)
		}
	}
}

func TestGitHubMergeGate_ApplyPostsTheCommittedBodies(t *testing.T) {
	s := gateCheckable(t, gateState{})
	gateAcceptsWrites(s, "")
	out, code := s.run(map[string]string{"APPLY": "1"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	if posts := s.posts(); len(posts) != 2 {
		t.Fatalf("posted %d bodies, want the main and owner-review rulesets:\n%s", len(posts), s.postedSummary())
	}
	for _, rel := range []string{gateMainRuleset, gateOwnersRuleset} {
		argv, _ := gateFindPosted(t, s, rel)
		if !strings.Contains(argv, "--method POST") || !strings.Contains(argv, "repos/o/r/rulesets") {
			t.Errorf("%s was posted with %q, want a POST to repos/o/r/rulesets", rel, argv)
		}
		if i := strings.Index(argv, "--input"); i >= 0 {
			t.Errorf("%s was posted against %q, not the committed file", rel, argv[i:])
		}
	}
}

func TestGitHubMergeGate_ApplyUpdatesAnExistingRulesetByName(t *testing.T) {
	const existing = `[{"name":"main-proof-gate","id":11},{"name":"gate-files-owner-review","id":22}]`
	s := gateCheckable(t, gateState{rulesets: existing})
	gateAcceptsWrites(s, existing)
	out, code := s.run(map[string]string{"APPLY": "1"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	for _, c := range []struct{ rel, id string }{
		{gateMainRuleset, "rulesets/11"},
		{gateOwnersRuleset, "rulesets/22"},
	} {
		argv, _ := gateFindPosted(t, s, c.rel)
		if !strings.Contains(argv, "--method PUT") || !strings.Contains(argv, c.id) {
			t.Errorf("%s was applied with %q, want a PUT to %s", c.rel, argv, c.id)
		}
	}
}

func TestGitHubMergeGate_RequiresCodeOwnerReviewByDefault(t *testing.T) {
	s := gateCheckable(t, gateState{})
	gateAcceptsWrites(s, "")
	out, code := s.run(map[string]string{"APPLY": "1"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	_, posted := gateFindPosted(t, s, gateOwnersRuleset)

	type bypassActor struct {
		ActorID    int    `json:"actor_id"`
		ActorType  string `json:"actor_type"`
		BypassMode string `json:"bypass_mode"`
	}
	var owners struct {
		BypassActors []bypassActor `json:"bypass_actors"`
	}
	if err := json.Unmarshal(posted, &owners); err != nil {
		t.Fatalf("%s: %v", gateOwnersRuleset, err)
	}
	want := []bypassActor{{ActorID: 5, ActorType: "RepositoryRole", BypassMode: "pull_request"}}
	if !reflect.DeepEqual(owners.BypassActors, want) {
		t.Errorf("bypass_actors = %+v, want exactly %+v", owners.BypassActors, want)
	}

	// The bypass above keeps the gate answering a pull request; turning the
	// review off must leave the ruleset out and only warn about CODEOWNERS.
	s = gateCheckable(t, gateState{})
	s.env["FAKE_CODEOWNERS_EXISTS"] = "0"
	gateAcceptsWrites(s, "")
	out, code = s.run(map[string]string{"APPLY": "1", "CODEOWNER_REVIEW": "0"})
	if code != 0 {
		t.Fatalf("exit = %d with CODEOWNER_REVIEW=0, want 0\n%s", code, out)
	}
	if !strings.Contains(out, "origin/main has no .github/CODEOWNERS; code-owner review is off") {
		t.Errorf("output does not warn about the missing CODEOWNERS:\n%s", out)
	}
	wantOwners := gateCommitted(t, gateOwnersRuleset)
	for _, p := range s.posts() {
		got, err := os.ReadFile(p.file)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(got, wantOwners) {
			t.Errorf("the owner-review ruleset was posted with CODEOWNER_REVIEW=0: %s", p.argv)
		}
		if strings.Contains(p.argv, "gate-files-owner-review") {
			t.Errorf("a body for gate-files-owner-review was posted: %s", p.argv)
		}
	}
}

func TestGitHubMergeGate_AppliesNoTagRuleset(t *testing.T) {
	queueLess := gateCheckable(t, gateState{})
	gateAcceptsWrites(queueLess, "")
	out, code := queueLess.run(map[string]string{"APPLY": "1"})
	if code != 0 {
		t.Fatalf("queue-less exit = %d, want 0\n%s", code, out)
	}

	queue := gateCheckable(t, gateState{ownerType: "Organization", modeValue: "queue"})
	gateAcceptsWrites(queue, "")
	out, code = queue.run(map[string]string{"APPLY": "1", "QUEUE": "1"})
	if code != 0 {
		t.Fatalf("queue exit = %d, want 0\n%s", code, out)
	}
	merged := false
	for _, p := range queue.posts() {
		body, err := os.ReadFile(p.file)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, []byte(`"merge_queue"`)) {
			merged = true
		}
	}
	if !merged {
		t.Errorf("the queue mode posted no merge_queue rule:\n%s", queue.postedSummary())
	}

	for _, run := range []struct {
		name string
		s    *gateSandbox
	}{{"queue-less", queueLess}, {"queue", queue}} {
		for _, call := range run.s.ghCalls() {
			if strings.Contains(call, "refs/tags") {
				t.Errorf("%s: gh call names a tag: %s", run.name, call)
			}
		}
		for _, p := range run.s.posts() {
			body, err := os.ReadFile(p.file)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(body, []byte("refs/tags")) {
				t.Errorf("%s: posted body %s names refs/tags", run.name, p.file)
			}
		}
	}
}

func TestGitHubRuleset_TheQueueBuildsOneGroupAtATime(t *testing.T) {
	var body struct {
		Rules []struct {
			Type       string `json:"type"`
			Parameters struct {
				MergeMethod                 string `json:"merge_method"`
				GroupingStrategy            string `json:"grouping_strategy"`
				MaxEntriesToMerge           int    `json:"max_entries_to_merge"`
				MaxEntriesToBuild           int    `json:"max_entries_to_build"`
				CheckResponseTimeoutMinutes int    `json:"check_response_timeout_minutes"`
			} `json:"parameters"`
		} `json:"rules"`
	}
	if err := json.Unmarshal(gateCommitted(t, gateQueueRuleset), &body); err != nil {
		t.Fatalf("%s: %v", gateQueueRuleset, err)
	}
	builds, merges, timeout := -1, -1, -1
	grouping, method, queues := "", "", 0
	for _, rule := range body.Rules {
		if rule.Type != "merge_queue" {
			continue
		}
		queues++
		builds = rule.Parameters.MaxEntriesToBuild
		merges = rule.Parameters.MaxEntriesToMerge
		timeout = rule.Parameters.CheckResponseTimeoutMinutes
		grouping = rule.Parameters.GroupingStrategy
		method = rule.Parameters.MergeMethod
	}
	if queues != 1 {
		t.Fatalf("%d merge_queue rules, want one", queues)
	}
	if builds != 1 || merges != 1 || grouping != "HEADGREEN" || method != "MERGE" || timeout != 60 {
		t.Errorf("queue = build %d, merge %d, %s, %s, %d min; want one group at a time, HEADGREEN, MERGE, 60 min",
			builds, merges, grouping, method, timeout)
	}
}

func TestGitHubRuleset_TheDefaultIsStrict(t *testing.T) {
	var body struct {
		BypassActors []json.RawMessage `json:"bypass_actors"`
		Rules        []struct {
			Type       string `json:"type"`
			Parameters struct {
				Strict       bool `json:"strict_required_status_checks_policy"`
				DoNotEnforce bool `json:"do_not_enforce_on_create"`
				Checks       []struct {
					Context       string `json:"context"`
					IntegrationID int    `json:"integration_id"`
				} `json:"required_status_checks"`
			} `json:"parameters"`
		} `json:"rules"`
	}
	if err := json.Unmarshal(gateCommitted(t, gateMainRuleset), &body); err != nil {
		t.Fatalf("%s: %v", gateMainRuleset, err)
	}
	if len(body.BypassActors) != 0 {
		t.Errorf("bypass_actors = %s, want none", body.BypassActors)
	}
	types := map[string]bool{}
	strict, sawChecks := false, false
	for _, rule := range body.Rules {
		types[rule.Type] = true
		if rule.Type != "required_status_checks" {
			continue
		}
		sawChecks = true
		strict = rule.Parameters.Strict
		if rule.Parameters.DoNotEnforce {
			t.Error("do_not_enforce_on_create is true: a new branch would bypass the proof")
		}
		checks := rule.Parameters.Checks
		if len(checks) != 1 || checks[0].Context != "proof" || checks[0].IntegrationID != 15368 {
			t.Errorf("required_status_checks = %+v, want exactly the proof context from app 15368", checks)
		}
	}
	if !sawChecks || !strict {
		t.Error("strict_required_status_checks_policy is not true: a stale branch could merge")
	}
	for _, want := range []string{"deletion", "non_fast_forward", "pull_request"} {
		if !types[want] {
			t.Errorf("the rules lack %q", want)
		}
	}
}

func TestCodeowners_CoverTheGateFiles(t *testing.T) {
	b, err := os.ReadFile(gateRepoPath(t, ".github/CODEOWNERS"))
	if err != nil {
		t.Fatal(err)
	}
	gate := map[string]bool{}
	for _, pattern := range gateFilePatterns {
		gate[pattern] = true
	}
	covered := map[string]bool{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		// A leading slash anchors the pattern at the root; the gate patterns
		// name root-relative paths with or without it.
		path := strings.TrimPrefix(fields[0], "/")
		if !gate[path] {
			t.Errorf("CODEOWNERS:%d names %q, which is not a gate file", i+1, fields[0])
		}
		covered[path] = true
		if len(fields) == 1 {
			t.Errorf("CODEOWNERS:%d names no owner", i+1)
		}
		for _, owner := range fields[1:] {
			if !strings.HasPrefix(owner, "@") {
				t.Errorf("CODEOWNERS:%d names owner %q, which does not start with @", i+1, owner)
			}
		}
	}
	for _, pattern := range gateFilePatterns {
		if !covered[pattern] {
			t.Errorf("gate file %q has no CODEOWNERS line", pattern)
		}
	}
}
