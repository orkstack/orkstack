package main

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// proofsDoc is the reference this test holds to the tree: every job, make
// target, proofrun subcommand and flag, and gate list it names must exist,
// and every one that exists must be named (plan §8.3).
const proofsDoc = "../../../docs/PROOFS.md"

func TestProofsDoc_NamesOnlyWhatExists(t *testing.T) {
	doc := readProofsDoc(t)
	workflows := workflowJobs(t)
	targets := makeTargets(t)
	subs := subcommandNames()
	flags := map[string]map[string]bool{}
	for _, sub := range subs {
		flags[sub] = subcommandFlags(t, sub)
	}
	var bad []string
	for _, m := range regexp.MustCompile("`([A-Za-z0-9_.-]+\\.yml)`\\s*›\\s*`([A-Za-z0-9_-]+)`").FindAllStringSubmatch(doc, -1) {
		file, job := m[1], m[2]
		if _, ok := workflows[file]; !ok {
			bad = append(bad, "workflow does not exist: "+file)
		} else if !workflows[file][job] {
			bad = append(bad, "job does not exist: "+file+" › "+job)
		}
	}
	for _, span := range proofDocSpans(doc) {
		switch {
		case strings.HasPrefix(span, "make "):
			if target := makeTargetOf(span); target != "" && !targetExists(targets, target) {
				bad = append(bad, "make target does not exist: "+target)
			}
		case strings.HasPrefix(span, "proofrun "):
			fields := strings.Fields(span)
			if len(fields) < 2 {
				continue
			}
			sub := fields[1]
			if !contains(subs, sub) {
				bad = append(bad, "proofrun subcommand does not exist: "+sub)
				continue
			}
			for _, f := range fields[2:] {
				if !strings.HasPrefix(f, "-") || f == "-" {
					continue
				}
				name := strings.TrimPrefix(strings.SplitN(f, "=", 2)[0], "-")
				name = strings.TrimSuffix(name, ",")
				if !flags[sub][name] {
					bad = append(bad, "proofrun "+sub+" has no -"+name)
				}
			}
		case strings.HasPrefix(span, "scripts/proof-"):
			if _, err := os.Stat(filepath.Join("..", "..", "..", span)); err != nil {
				bad = append(bad, "gate file does not exist: "+span)
			}
		case strings.HasSuffix(span, ".yml") && !strings.Contains(span, " "):
			base := filepath.Base(span)
			if _, ok := workflows[base]; ok {
				continue
			}
			if _, err := os.Stat(filepath.Join("..", "..", "..", span)); err == nil {
				continue
			}
			if _, err := os.Stat(filepath.Join("..", "..", "..", base)); err == nil {
				continue
			}
			bad = append(bad, "YAML file does not exist: "+span)
		}
	}
	if len(bad) > 0 {
		t.Fatalf("docs/PROOFS.md names %d things that do not exist:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

func TestProofsDoc_MentionsEverythingThatExists(t *testing.T) {
	doc := readProofsDoc(t)
	workflows := workflowJobs(t)
	var bad []string
	for file, jobs := range workflows {
		for job := range jobs {
			if !strings.Contains(doc, "`"+job+"`") && !strings.Contains(doc, "`"+file+"` › `"+job+"`") &&
				!strings.Contains(doc, "`"+file+" › "+job+"`") {
				bad = append(bad, "CI job not mentioned: "+file+" › "+job)
			}
		}
	}
	for _, target := range evidenceTargets(t) {
		if !strings.Contains(doc, "`make "+target+"`") && !strings.Contains(doc, "`make -C images "+target+"`") {
			bad = append(bad, "make target not mentioned: "+target)
		}
	}
	for _, sub := range subcommandNames() {
		if !strings.Contains(doc, "`"+sub+"`") {
			bad = append(bad, "proofrun subcommand not mentioned: "+sub)
		}
		for flag := range subcommandFlags(t, sub) {
			if flag == "h" || flag == "help" {
				continue
			}
			if !strings.Contains(doc, "`-"+flag) {
				bad = append(bad, "proofrun "+sub+" flag not mentioned: -"+flag)
			}
		}
	}
	for _, file := range gateFiles(t) {
		if !strings.Contains(doc, file) {
			bad = append(bad, "gate file not mentioned: "+file)
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		t.Fatalf("docs/PROOFS.md omits %d things that exist:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

// The scope table's actions are a vocabulary a contributor reads in the table
// and in this document: every action the parser knows is named in §3.
func TestProofsDoc_NamesEveryScopeAction(t *testing.T) {
	doc := readProofsDoc(t)
	i := strings.Index(doc, "The scope table's actions:")
	if i < 0 {
		t.Fatal("docs/PROOFS.md does not list the scope table's actions")
	}
	paragraph := doc[i : i+strings.Index(doc[i:], "\n\n")]
	var missing []string
	for action := range knownActions {
		if !strings.Contains(paragraph, "`"+action+"`") {
			missing = append(missing, action)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("docs/PROOFS.md §3 omits the scope actions %v", missing)
	}
	table, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "proof-scope.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for action := range knownActions {
		if !regexp.MustCompile(`(?m)^#\s+(actions\s+)?` + regexp.QuoteMeta(action) + `\s`).Match(table) {
			t.Errorf("scripts/proof-scope.tsv's legend omits the action %s", action)
		}
	}
}

func TestProofsDoc_CountsWhatItChecks(t *testing.T) {
	workflows := workflowJobs(t)
	jobs := 0
	for _, w := range workflows {
		jobs += len(w)
	}
	flags := 0
	for _, sub := range subcommandNames() {
		flags += len(subcommandFlags(t, sub))
	}
	t.Logf("PROOFS.md contract: %d workflows, %d jobs, %d make targets in evidence, %d proofrun subcommands, %d flags, %d gate files",
		len(workflows), jobs, len(evidenceTargets(t)), len(subcommandNames()), flags, len(gateFiles(t)))
}

func readProofsDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(proofsDoc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// proofDocSpans returns the backticked spans the grammar defines.
func proofDocSpans(doc string) []string {
	var out []string
	for {
		i := strings.Index(doc, "`")
		if i < 0 {
			return out
		}
		doc = doc[i+1:]
		j := strings.Index(doc, "`")
		if j < 0 {
			return out
		}
		span := doc[:j]
		doc = doc[j+1:]
		if strings.Contains(span, "`") {
			continue
		}
		out = append(out, span)
	}
}

func workflowJobs(t *testing.T) map[string]map[string]bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "..", ".github", "workflows", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]bool{}
	for _, file := range files {
		var wf struct {
			Jobs map[string]any `yaml:"jobs"`
		}
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(b, &wf); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		jobs := map[string]bool{}
		for job := range wf.Jobs {
			jobs[job] = true
		}
		out[filepath.Base(file)] = jobs
	}
	return out
}

func makeTargets(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, file := range []string{"Makefile", "images/Makefile"} {
		b, err := os.ReadFile(filepath.Join("..", "..", "..", file))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range regexp.MustCompile(`(?m)^([A-Za-z0-9_.%/-]+):`).FindAllStringSubmatch(string(b), -1) {
			out[m[1]] = true
		}
	}
	return out
}

func evidenceTargets(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	start := strings.Index(doc, "## ---- evidence")
	if start < 0 {
		t.Fatal("Makefile has no evidence section")
	}
	var out []string
	for _, m := range regexp.MustCompile(`(?m)^([A-Za-z0-9_.-]+):`).FindAllStringSubmatch(doc[start:], -1) {
		if m[1] == ".PHONY" {
			continue
		}
		out = append(out, m[1])
	}
	out = append(out, "check")
	sort.Strings(out)
	return out
}

func gateFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "..", "scripts", "proof-*"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, file := range files {
		out = append(out, "scripts/"+filepath.Base(file))
	}
	sort.Strings(out)
	return out
}

func makeTargetOf(span string) string {
	fields := strings.Fields(span)
	if len(fields) < 2 {
		return ""
	}
	i := 1
	if fields[i] == "-C" && len(fields) > 2 && fields[i+1] == "images" {
		i += 2
	}
	if i >= len(fields) {
		return ""
	}
	target := fields[i]
	if strings.ContainsAny(target, "$()") {
		return ""
	}
	return target
}

func targetExists(targets map[string]bool, name string) bool {
	if targets[name] {
		return true
	}
	for pattern := range targets {
		if i := strings.Index(pattern, "%"); i >= 0 {
			if strings.HasPrefix(name, pattern[:i]) && strings.HasSuffix(name, pattern[i+1:]) {
				return true
			}
		}
	}
	return false
}

func subcommandNames() []string {
	var out []string
	for _, c := range subcommands {
		out = append(out, c.name)
	}
	return out
}

// subcommandFlags reads the flag set a subcommand defines, by running its
// `-h`: the flag package prints exactly the registered flags.
func subcommandFlags(t *testing.T, sub string) map[string]bool {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	for _, c := range subcommands {
		if c.name == sub {
			c.run([]string{"-h"})
			break
		}
	}
	_ = w.Close()
	os.Stderr = old
	out, _ := io.ReadAll(r)
	_ = r.Close()
	flags := map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		if m := regexp.MustCompile(`^\s+-([A-Za-z0-9][A-Za-z0-9-]*)`).FindStringSubmatch(line); m != nil {
			flags[m[1]] = true
		}
	}
	return flags
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
