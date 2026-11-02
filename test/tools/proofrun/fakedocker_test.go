package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
)

// fakeDockerContainer is one container the fake docker lists: its name and
// its labels.
type fakeDockerContainer struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

// fakeDockerView is what a --format template of `docker ps` is rendered
// against, as the real CLI renders it (Go's text/template): .Names and
// .Label KEY.
type fakeDockerView struct {
	Names  string
	labels map[string]string
}

func (v fakeDockerView) Label(key string) string { return v.labels[key] }

// fakeToolMain is a program of a test's fake PATH: the test binary run with
// PROOFRUN_AS_FAKE_TOOL=<program> (TestMain). It appends the program and its
// arguments, as one JSON array per line, to $FAKE_TOOL_LOG, and succeeds
// (unless $FAKE_TOOL_FAIL names it, when it exits 1).
// When the program is docker it also answers `ps` from $FAKE_DOCKER_CONTAINERS
// (a JSON list): `--filter label=KEY[=VALUE]` keeps the containers carrying
// that label, `--format` is rendered for each as docker renders it, and a `ps`
// with no format prints the names, as -q does ids. A template docker would
// refuse is refused. `stop NAME...` prints each name.
func fakeToolMain(program string, args []string) int {
	if path := os.Getenv("FAKE_TOOL_LOG"); path != "" {
		line, _ := json.Marshal(append([]string{program}, args...))
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fake tool:", err)
			return 2
		}
		fmt.Fprintf(f, "%s\n", line)
		f.Close()
	}
	if os.Getenv("FAKE_TOOL_FAIL") == program {
		return 1
	}
	if program != "docker" || len(args) == 0 {
		return 0
	}
	switch args[0] {
	case "ps":
		return fakeDockerPS(args[1:], os.Stdout, os.Stderr)
	case "stop":
		for i := 1; i < len(args); i++ {
			if args[i] == "-t" {
				i++ // the grace period, not a container
				continue
			}
			fmt.Println(args[i])
		}
	}
	return 0
}

func fakeDockerPS(args []string, out, errw io.Writer) int {
	var containers []fakeDockerContainer
	if raw := os.Getenv("FAKE_DOCKER_CONTAINERS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &containers); err != nil {
			fmt.Fprintln(errw, "fake docker: FAKE_DOCKER_CONTAINERS:", err)
			return 2
		}
	}
	var filters []string
	format := ""
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--filter":
			i++
			if i < len(args) {
				filters = append(filters, args[i])
			}
		case "--format":
			i++
			if i < len(args) {
				format = args[i]
			}
		}
	}
	var tmpl *template.Template
	if format != "" {
		var err error
		if tmpl, err = template.New("ps").Parse(format); err != nil {
			fmt.Fprintln(errw, "template parsing error:", err)
			return 1
		}
	}
	for _, c := range containers {
		keep := true
		for _, f := range filters {
			want, ok := strings.CutPrefix(f, "label=")
			if !ok {
				continue
			}
			key, value, hasValue := strings.Cut(want, "=")
			got, present := c.Labels[key]
			if !present || (hasValue && got != value) {
				keep = false
			}
		}
		if !keep {
			continue
		}
		if tmpl == nil {
			fmt.Fprintln(out, c.Name)
			continue
		}
		if err := tmpl.Execute(out, fakeDockerView{Names: c.Name, labels: c.Labels}); err != nil {
			fmt.Fprintln(errw, "template execution error:", err)
			return 1
		}
		fmt.Fprintln(out)
	}
	return 0
}

// fakeTools is a directory of stand-ins for the programs a script runs, each
// the test binary itself (fakeToolMain), recording every call.
type fakeTools struct {
	t   *testing.T
	dir string
	log string
}

// installFakeDocker puts a docker on PATH that lists containers (see
// fakeToolMain), and returns the tools it was installed with.
func installFakeDocker(t *testing.T, containers []fakeDockerContainer) *fakeTools {
	t.Helper()
	raw, err := json.Marshal(containers)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeTools{t: t, dir: t.TempDir()}
	f.log = filepath.Join(f.dir, "calls.jsonl")
	f.recorder("docker")
	t.Setenv("PATH", f.dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_TOOL_LOG", f.log)
	t.Setenv("FAKE_DOCKER_CONTAINERS", string(raw))
	return f
}

// recorder writes a stand-in named name, which records its calls and
// succeeds, and returns its path.
func (f *fakeTools) recorder(name string) string {
	f.t.Helper()
	self, err := os.Executable()
	if err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(f.dir, name)
	script := fmt.Sprintf("#!/bin/sh\nexec env -u PROOFRUN_AS_COMMAND PROOFRUN_AS_FAKE_TOOL=%s %q \"$@\"\n", name, self)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		f.t.Fatal(err)
	}
	return path
}

// calls are the program and arguments of every call the tools received, in
// order.
func (f *fakeTools) calls() [][]string {
	f.t.Helper()
	b, err := os.ReadFile(f.log)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	var out [][]string
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var call []string
		if err := json.Unmarshal([]byte(line), &call); err != nil {
			f.t.Fatalf("the fake tools' log holds %q: %v", line, err)
		}
		out = append(out, call)
	}
	return out
}

// verbs are the program and first word of each call: what was asked of whom,
// not how.
func (f *fakeTools) verbs() []string {
	var verbs []string
	for _, c := range f.calls() {
		verbs = append(verbs, strings.Join(c[:min(len(c), 2)], " "))
	}
	return verbs
}

// A fake docker that renders a template as docker does is itself checked once:
// the three forms the proofs use (a field, a label by key and a quoted tab),
// the label filter and a template docker would refuse.
func TestFakeDocker_RendersATemplateAsDockerDoes(t *testing.T) {
	t.Setenv("FAKE_DOCKER_CONTAINERS", `[{"name":"a","labels":{"k":"1","o":"x:1"}},{"name":"b","labels":{"o":"y"}},{"name":"c","labels":{"k":"2"}}]`)
	var out, errw strings.Builder
	if code := fakeDockerPS([]string{"--filter", "label=k", "--format", `{{.Names}}{{"\t"}}{{.Label "k"}}	{{.Label "o"}}`}, &out, &errw); code != 0 {
		t.Fatalf("exit %d: %s", code, errw.String())
	}
	if want := "a\t1\tx:1\nc\t2\t\n"; out.String() != want {
		t.Errorf("rendered %q, want %q", out.String(), want)
	}
	out.Reset()
	if code := fakeDockerPS([]string{"--filter", "label=k=2"}, &out, &errw); code != 0 || out.String() != "c\n" {
		t.Errorf("a label=KEY=VALUE filter printed %q, exit %d", out.String(), code)
	}
	if code := fakeDockerPS([]string{"--format", `{{.Names`}, &out, &errw); code == 0 {
		t.Error("a template docker would refuse was rendered")
	}
}
