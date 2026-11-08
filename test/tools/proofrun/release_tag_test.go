package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ---- the tag step, run as written -------------------------------------------------
//
// publish's tag step pushes the version's tag with git and a token of the release
// environment's GitHub App, because the App is the one actor the `release tags`
// ruleset lets create a `v*` tag. These tests take the step out of release.yml and
// run it with the real git against git's own http-backend behind the token check
// GitHub makes; only the step that mints the token is stubbed, by handing the tag
// step a token. What the App's installation grants, and what GitHub's ruleset does
// with the push, are seen only when a real release runs (docs/PROOFS.md §10, "Set
// up the release's tag App").

// pushedTagProblems: a tag pushed with the App's token starts workflows, as one
// made with the job's own token does not. No workflow may start on one: a
// `push:` that names no branch filter runs for every tag, and one that names
// `tags:` asks to.
func pushedTagProblems(file string, w *wfFile) []string {
	raw, ok := w.On["push"]
	if !ok {
		return nil
	}
	push, _ := raw.(map[string]any)
	_, branches := push["branches"]
	_, ignored := push["branches-ignore"]
	_, tags := push["tags"]
	_, tagsIgnored := push["tags-ignore"]
	switch {
	case tags || tagsIgnored:
		return []string{fmt.Sprintf("%s's push trigger names tags: the release's tag would start it", file)}
	case !branches && !ignored:
		return []string{fmt.Sprintf("%s's push trigger names no branches: the release's tag would start it", file)}
	}
	return nil
}

func TestWorkflows_NoWorkflowStartsOnTheReleasesTag(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "..", ".github", "workflows", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no workflows found")
	}
	for _, path := range files {
		name := filepath.Base(path)
		expectNoProblems(t, name, pushedTagProblems(name, loadWorkflow(t, name)))
	}
	rule := func(w *wfFile) []string { return pushedTagProblems("ci.yml", w) }
	expectEachMutationRefused(t, "ci.yml", rule, []mutation{
		{"every push", func(w *wfFile) { w.On["push"] = nil }},
		{"no branch filter", func(w *wfFile) { w.On["push"] = map[string]any{"paths": []any{"x"}} }},
		{"the tags named", func(w *wfFile) {
			w.On["push"] = map[string]any{"branches": []any{"main"}, "tags": []any{"v*"}}
		}},
	})
}

// tagHarnessToken is the token the server below accepts: not a real one, shaped
// like one, and long enough that finding it anywhere means the token leaked.
const tagHarnessToken = "ghs_harnessTokenNotARealCredential0123456"

// tagGitShim stands in for git on the step's PATH: it records every command and
// whether the token is in its environment, then runs the real git.
const tagGitShim = `#!/usr/bin/env bash
printf 'git %s\n' "$*" >>"$STUB_LOG"
printf 'token-in-env %s\n' "${TAG_TOKEN:+yes}" >>"$STUB_LOG"
exec %REAL_GIT% "$@"
`

const tagGHStub = `#!/usr/bin/env bash
printf 'gh %s\n' "$*" >>"$STUB_LOG"
exit 3
`

// tagHarness is a local bare repository in GitHub's place, served over HTTP by
// git's own http-backend behind the token check GitHub makes; two commits on its
// main; and the environment the step runs in.
type tagHarness struct {
	t                 *testing.T
	dir, bin          string
	server, repo, log string
	temp              string
	first, tip        string
	env               []string
	realGit           string
	backend           http.Handler

	// readOnly answers a write as a token without contents write is answered.
	readOnly bool
	// raceTo makes the tag, on this commit, as the push asks what the server holds.
	raceTo string

	mu       sync.Mutex
	requests []string
}

func (h *tagHarness) record(what string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = append(h.requests, what)
}

func (h *tagHarness) served(what string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.requests {
		if r == what {
			n++
		}
	}
	return n
}

func (h *tagHarness) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "x-access-token" || pass != tagHarnessToken {
		h.record("refused")
		w.Header().Set("WWW-Authenticate", `Basic realm="GitHub"`)
		http.Error(w, "Invalid username or token.", http.StatusUnauthorized)
		return
	}
	asksToWrite := r.URL.Query().Get("service") == "git-receive-pack"
	if asksToWrite && h.readOnly {
		h.record("refused")
		http.Error(w, "Permission to o/r.git denied to the-app[bot].", http.StatusForbidden)
		return
	}
	if asksToWrite && h.raceTo != "" {
		cmd := exec.Command(h.realGit, "-C", h.repo, "update-ref", "refs/tags/v1.2.3", h.raceTo)
		cmd.Env = h.env
		if out, err := cmd.CombinedOutput(); err != nil {
			http.Error(w, fmt.Sprintf("the race's tag: %v: %s", err, out), http.StatusInternalServerError)
			return
		}
	}
	if strings.HasSuffix(r.URL.Path, "/git-receive-pack") {
		h.record("write")
	} else {
		h.record("read")
	}
	h.backend.ServeHTTP(w, r)
}

func newTagHarness(t *testing.T) *tagHarness {
	t.Helper()
	for _, tool := range []string{"jq", "base64"} {
		requireTool(t, tool)
	}
	h := &tagHarness{t: t, dir: t.TempDir(), realGit: requireTool(t, "git")}
	h.bin = filepath.Join(h.dir, "bin")
	h.server = filepath.Join(h.dir, "server")
	h.repo = filepath.Join(h.server, "o", "r.git")
	h.temp = filepath.Join(h.dir, "temp")
	h.log = filepath.Join(h.dir, "calls.log")
	home := filepath.Join(h.dir, "home")
	for _, d := range []string{h.bin, filepath.Dir(h.repo), filepath.Join(h.temp, "proof"), home} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, body := range map[string]string{"git": strings.ReplaceAll(tagGitShim, "%REAL_GIT%", h.realGit), "gh": tagGHStub} {
		if err := os.WriteFile(filepath.Join(h.bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Nothing of the host's git configuration: the runner has none that helps.
	config := []string{"HOME=" + home, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	h.backend = &cgi.Handler{
		Path: h.realGit,
		Args: []string{"http-backend"},
		Env: append([]string{
			"PATH=" + os.Getenv("PATH"), "GIT_PROJECT_ROOT=" + h.server, "GIT_HTTP_EXPORT_ALL=1", "REMOTE_USER=x-access-token",
		}, config...),
		Stderr: io.Discard,
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	h.env = append([]string{
		"PATH=" + h.bin + ":" + os.Getenv("PATH"), "RUNNER_TEMP=" + h.temp, "STUB_LOG=" + h.log,
		"VERSION=v1.2.3", "REPO=o/r", "SERVER_URL=" + srv.URL, "RUN_URL=https://github.com/o/r/actions/runs/77",
	}, config...)
	h.git("", "init", "--quiet", "--bare", "--initial-branch=main", h.repo)
	// GitHub serves a commit by its id, and refuses a push that needs a shallow
	// root on the server: receive.shallowUpdate stays at its default, false.
	h.git(h.repo, "config", "uploadpack.allowAnySHA1InWant", "true")
	work := filepath.Join(h.dir, "work")
	h.git("", "init", "--quiet", "--initial-branch=main", work)
	file := filepath.Join(work, "f")
	commit := func(text string) string {
		if err := os.WriteFile(file, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		h.git(work, "add", "f")
		h.git(work, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "--quiet", "-m", text)
		return h.git(work, "rev-parse", "HEAD")
	}
	h.first = commit("one")
	h.tip = commit("two")
	h.git(work, "push", "--quiet", h.repo, "main")
	proof := fmt.Sprintf(`{"commit":%q,"tree":"tree123","result":"pass"}`, h.tip)
	if err := os.WriteFile(filepath.Join(h.temp, "proof", "proof.json"), []byte(proof), 0o644); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *tagHarness) git(dir string, args ...string) string {
	h.t.Helper()
	cmd := exec.Command(h.realGit, args...)
	cmd.Dir = dir
	cmd.Env = h.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		h.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// runBash runs a step's script the way the runner runs a bash step.
func (h *tagHarness) runBash(script string, env ...string) (int, string) {
	h.t.Helper()
	cmd := exec.Command("bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script)
	cmd.Dir = h.temp
	cmd.Env = append(append([]string{}, h.env...), env...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	case err != nil:
		h.t.Fatal(err)
	}
	return 0, string(out)
}

// publishStep is publish's step running text, as release.yml writes it.
func publishStep(t *testing.T, text string) wfStep {
	t.Helper()
	job, ok := loadWorkflow(t, "release.yml").Jobs["publish"]
	if !ok {
		t.Fatal("release.yml has no publish job")
	}
	at := stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, text) })
	if at < 0 {
		t.Fatalf("release.yml's publish job has no step running %q", text)
	}
	return job.Steps[at]
}

// run runs the tag step on commit sha, handed token as the token step would hand
// it; that step is GitHub's action, and the harness stands in for what it gives.
func (h *tagHarness) run(sha, token string, extra ...string) (int, string) {
	h.t.Helper()
	return h.runBash(publishStep(h.t, tagRef).Run, append([]string{"SHA=" + sha, "TAG_TOKEN=" + token}, extra...)...)
}

func (h *tagHarness) calls() []string {
	h.t.Helper()
	raw, err := os.ReadFile(h.log)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		h.t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

func (h *tagHarness) called(prefix string) int {
	h.t.Helper()
	n := 0
	for _, c := range h.calls() {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

// remoteTag is what the server's refs/tags/v1.2.3 holds, or "".
func (h *tagHarness) remoteTag() string {
	h.t.Helper()
	cmd := exec.Command(h.realGit, "-C", h.repo, "rev-parse", "--verify", "--quiet", "refs/tags/v1.2.3")
	cmd.Env = h.env
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// plantTag makes an annotated tag on the server, as another run would have.
func (h *tagHarness) plantTag(sha, message string) string {
	h.t.Helper()
	h.git(h.repo, "-c", "user.name=other", "-c", "user.email=o@example.invalid", "tag", "-a", "v1.2.3", "-m", message, sha)
	return h.remoteTag()
}

// noAPIAndTagUnchanged refuses what no refusal of the step may do: reach for the
// API, or touch the tag the server already holds.
func (h *tagHarness) noAPIAndTagUnchanged(before string) {
	h.t.Helper()
	if n := h.called("gh "); n != 0 {
		h.t.Errorf("the step called gh %d times: it never falls back to the API\n%v", n, h.calls())
	}
	if got := h.remoteTag(); got != before {
		h.t.Errorf("refs/tags/v1.2.3 is %q, was %q: a version tag never moves", got, before)
	}
}

func TestReleaseTag_CreatesTheAnnotatedTagWithTheTokenAndKeepsItOnARerun(t *testing.T) {
	for _, c := range []struct {
		name string
		sha  func(h *tagHarness) string
	}{
		{"on main's tip", func(h *tagHarness) string { return h.tip }},
		{"on a commit main has moved past", func(h *tagHarness) string { return h.first }},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newTagHarness(t)
			sha := c.sha(h)
			code, out := h.run(sha, tagHarnessToken)
			if code != 0 || !strings.Contains(out, "v1.2.3: tagged "+sha) {
				t.Fatalf("exit %d, want 0, saying it tagged %s:\n%s", code, sha, out)
			}
			object := h.remoteTag()
			if object == "" {
				t.Fatal("the server holds no tag")
			}
			if got := h.git(h.repo, "cat-file", "-t", object); got != "tag" {
				t.Errorf("the tag is a %s, want an annotated tag", got)
			}
			if got := h.git(h.repo, "rev-parse", object+"^{commit}"); got != sha {
				t.Errorf("the tag is on %s, want %s", got, sha)
			}
			body := h.git(h.repo, "cat-file", "tag", object)
			for _, want := range []string{
				"tagger github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>",
				"OrkStack v1.2.3",
				"proof: https://github.com/o/r/actions/runs/77",
				"commit: " + h.tip,
				"tree: tree123",
				"result: pass",
			} {
				if !strings.Contains(body, want) {
					t.Errorf("the tag does not say %q:\n%s", want, body)
				}
			}
			if n := h.served("write"); n != 1 {
				t.Errorf("%d pushes, want 1", n)
			}
			if n := h.served("refused"); n != 0 {
				t.Errorf("%d requests came without the token", n)
			}

			// Where the token went: into git's environment as a header scoped to
			// the server and nowhere else. Not into a command line, not into any
			// file (the repository's config among them), not into the log but
			// for the line that tells the runner to mask the header.
			auth := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + tagHarnessToken))
			logged := []string{}
			for _, line := range strings.Split(out, "\n") {
				if line == "::add-mask::"+auth {
					logged = append(logged, line)
					continue
				}
				if strings.Contains(line, tagHarnessToken) || strings.Contains(line, auth) {
					t.Errorf("the token reached the step's output: %q", line)
				}
			}
			if len(logged) != 1 {
				t.Errorf("the header was masked %d times, want once:\n%s", len(logged), out)
			}
			if h.called("git ") == 0 {
				t.Fatal("the step never ran git")
			}
			for _, call := range h.calls() {
				if call == "token-in-env yes" {
					t.Error("git could read the token from TAG_TOKEN: the step drops it before git starts")
				}
			}
			err := filepath.WalkDir(h.dir, func(path string, d os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if !d.Type().IsRegular() {
					return nil
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if strings.Contains(string(raw), tagHarnessToken) || strings.Contains(string(raw), auth) {
					t.Errorf("the token reached the disk or a command line: %s", path)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			// A re-run of this job finds this run's tag and keeps it.
			code, out = h.run(sha, tagHarnessToken)
			if code != 0 || !strings.Contains(out, "this run's tag on "+sha+", kept") {
				t.Fatalf("the re-run: exit %d, want 0, keeping the tag:\n%s", code, out)
			}
			if n := h.served("write"); n != 1 {
				t.Errorf("the re-run pushed: %d pushes, want 1", n)
			}
			h.noAPIAndTagUnchanged(object)
		})
	}
}

func TestReleaseTag_RefusesWhatItCannotDoBeforeAnythingIsPushed(t *testing.T) {
	const thisRun = "OrkStack v1.2.3\nproof: https://github.com/o/r/actions/runs/77\n"
	for _, c := range []struct {
		name string
		// token is the token to run with; "" with noToken is none.
		token   string
		noToken bool
		// plant makes what the server holds beforehand.
		plant func(h *tagHarness)
		says  string
		// noRequest: refused before any connection.
		noRequest bool
		// raced: the tag the race made stands.
		raced bool
	}{
		{
			name:      "no token from the token step",
			noToken:   true,
			says:      "this step was handed no token: nothing is tagged",
			noRequest: true,
		},
		{
			name:  "a token the host refuses",
			token: "ghs_notTheTokenTheServerAccepts",
			says:  "could not read o/r with the App's token",
		},
		{
			name:  "a token that cannot write",
			plant: func(h *tagHarness) { h.readOnly = true },
			says:  "403",
		},
		{
			name: "another run's tag on this commit",
			plant: func(h *tagHarness) {
				h.plantTag(h.tip, "OrkStack v1.2.3\nproof: https://github.com/o/r/actions/runs/1\n")
			},
			says: "already exists and was not created by this run",
		},
		{
			name:  "this run's tag on another commit",
			plant: func(h *tagHarness) { h.plantTag(h.first, thisRun) },
			says:  "already exists and was not created by this run",
		},
		{
			name:  "a lightweight tag on this commit",
			plant: func(h *tagHarness) { h.git(h.repo, "update-ref", "refs/tags/v1.2.3", h.tip) },
			says:  "not an annotated tag this run made",
		},
		{
			name: "a proof that cannot be read",
			plant: func(h *tagHarness) {
				if err := os.WriteFile(filepath.Join(h.temp, "proof", "proof.json"), []byte("{"), 0o644); err != nil {
					h.t.Fatal(err)
				}
			},
			says: "parse error",
		},
		{
			name: "a proof that names no commit",
			plant: func(h *tagHarness) {
				if err := os.WriteFile(filepath.Join(h.temp, "proof", "proof.json"), []byte(`{"tree":"t","result":"pass"}`), 0o644); err != nil {
					h.t.Fatal(err)
				}
			},
		},
		{
			name:  "a tag made between the read and the push",
			plant: func(h *tagHarness) { h.raceTo = h.first },
			says:  "already exists",
			raced: true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newTagHarness(t)
			if c.plant != nil {
				c.plant(h)
			}
			before := h.remoteTag()
			token := tagHarnessToken
			switch {
			case c.noToken:
				token = ""
			case c.token != "":
				token = c.token
			}
			code, out := h.run(h.tip, token)
			if code == 0 {
				t.Fatalf("the step passed, want a refusal saying %q:\n%s", c.says, out)
			}
			if !strings.Contains(out, c.says) {
				t.Errorf("exit %d, not saying %q:\n%s", code, c.says, out)
			}
			if strings.Contains(out, tagHarnessToken) {
				t.Errorf("the refusal printed the token:\n%s", out)
			}
			if c.noRequest && (h.called("git ") != 0 || h.served("read")+h.served("refused") != 0) {
				t.Errorf("a connection was made before the refusal: %v", h.calls())
			}
			if c.raced {
				// The tag the race planted stands, and this run's never replaced it.
				if got := h.remoteTag(); got != h.first {
					t.Errorf("refs/tags/v1.2.3 is %q, the racer's %q was moved", got, h.first)
				}
				if n := h.called("gh "); n != 0 {
					t.Errorf("the step called gh: %v", h.calls())
				}
				return
			}
			h.noAPIAndTagUnchanged(before)
		})
	}
}

// The pre-receive hook is what a ruleset without the App's bypass does to the
// push: refused by the server, and the step fails with its words.
func TestReleaseTag_FailsWhenTheServerRefusesThePush(t *testing.T) {
	h := newTagHarness(t)
	hook := filepath.Join(h.repo, "hooks", "pre-receive")
	body := "#!/usr/bin/env bash\necho 'GH013: Repository rule violations found for refs/tags/v1.2.3.' >&2\nexit 1\n"
	if err := os.WriteFile(hook, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	code, out := h.run(h.tip, tagHarnessToken)
	if code == 0 || !strings.Contains(out, "GH013") {
		t.Fatalf("exit %d, want a failure saying what the server said:\n%s", code, out)
	}
	if strings.Contains(out, "workflows permission") {
		t.Errorf("a refusal for another reason blames the workflows permission:\n%s", out)
	}
	h.noAPIAndTagUnchanged("")
}

// GitHub refuses an App's push it reads as changing a workflow file, with these
// words, and reports say a tag on a commit main has moved past can be read so
// (docs/PROOFS.md §10, "Set up the release's tag App"). The App holds no such
// permission: the step says so, nothing is out, and a re-run tags once the
// refusal is gone.
func TestReleaseTag_SaysSoWhenGitHubAsksForTheWorkflowsPermission(t *testing.T) {
	h := newTagHarness(t)
	hook := filepath.Join(h.repo, "hooks", "pre-receive")
	body := "#!/usr/bin/env bash\necho 'refusing to allow a GitHub App to create or update workflow `.github/workflows/ci.yml` without `workflows` permission' >&2\nexit 1\n"
	if err := os.WriteFile(hook, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	code, out := h.run(h.first, tagHarnessToken)
	if code == 0 {
		t.Fatalf("the step passed, want a refusal:\n%s", out)
	}
	for _, want := range []string{"without `workflows` permission", "nothing is tagged", "main's tip"} {
		if !strings.Contains(out, want) {
			t.Errorf("exit %d, not saying %q:\n%s", code, want, out)
		}
	}
	h.noAPIAndTagUnchanged("")

	// A re-run is a new runner: the local tag of the refused push went with the old.
	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(h.temp, "tag.git")); err != nil {
		t.Fatal(err)
	}
	code, out = h.run(h.first, tagHarnessToken)
	if code != 0 || !strings.Contains(out, "v1.2.3: tagged "+h.first) {
		t.Fatalf("the re-run: exit %d, want 0, tagging:\n%s", code, out)
	}
}

// The step before the token is asked for: the action's own error names neither
// the secrets nor what to do, and a missing secret must stop the job here.
func TestReleaseTag_TheSecretsCheckRefusesWhatIsMissing(t *testing.T) {
	check := publishStep(t, tagSecretsCheck)
	const secretValue = "the-secret-value-of-this-test"
	for _, c := range []struct {
		name     string
		clientID string
		key      string
		passes   bool
	}{
		{name: "neither"},
		{name: "no client id", key: secretValue},
		{name: "no private key", clientID: secretValue},
		{name: "both", clientID: secretValue, key: secretValue, passes: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newTagHarness(t)
			code, out := h.runBash(check.Run, tagAppClientID+"="+c.clientID, tagAppKey+"="+c.key)
			if strings.Contains(out, secretValue) {
				t.Errorf("the check printed a secret:\n%s", out)
			}
			if c.passes {
				if code != 0 {
					t.Errorf("exit %d, want 0:\n%s", code, out)
				}
				return
			}
			if code == 0 {
				t.Fatalf("the check passed, want a refusal:\n%s", out)
			}
			for _, want := range []string{tagAppClientID, tagAppKey, "nothing is tagged", "the job's token never creates a version tag", "Set up the release's tag App"} {
				if !strings.Contains(out, want) {
					t.Errorf("exit %d, not saying %q:\n%s", code, want, out)
				}
			}
			if h.called("gh ")+h.called("git ") != 0 {
				t.Errorf("the check reached for git or the API: %v", h.calls())
			}
		})
	}
}
