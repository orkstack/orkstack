package main

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// The proto contract is checked against the first release on every push and
// pull request: ci.yml's proto job checks out every tag and runs `make
// proto-breaking`, which runs buf breaking from the image deploy/buf-image.txt
// pins by digest, with no network, against the tree of the tag PROTO_BASE
// (v0.1.0), under buf.yaml's breaking rules.

// protoBreakingProblems is the rule for ci.yml's proto job.
func protoBreakingProblems(w *wfFile) []string {
	job, ok := w.Jobs["proto"]
	if !ok {
		return []string{"ci.yml has no proto job"}
	}
	var p []string
	if job.If != "" {
		p = append(p, "ci.yml's proto job is conditional: "+job.If)
	}
	if job.ContinueOnError != nil {
		p = append(p, "ci.yml's proto job continues on error, so a breaking change would not turn it red")
	}
	for i, s := range job.Steps {
		if s.ContinueOnError != nil {
			p = append(p, fmt.Sprintf("ci.yml's proto job %s continues on error", s.label(i)))
		}
	}
	checkout := stepIndex(job.Steps, isCheckout)
	if checkout < 0 || withString(job.Steps[checkout], "fetch-depth") != "0" {
		p = append(p, "the proto job does not check out with fetch-depth: 0, so it would see no tag to compare with")
	}
	if at := stepIndex(job.Steps, func(s wfStep) bool { return strings.TrimSpace(s.Run) == "make proto-breaking" }); at < 0 {
		p = append(p, "the proto job never runs make proto-breaking")
	} else if at < checkout {
		p = append(p, "the proto job runs make proto-breaking before its checkout")
	}
	return p
}

func TestWorkflows_CIChecksTheProtoContractAgainstTheFirstRelease(t *testing.T) {
	expectEachMutationRefused(t, "ci.yml", protoBreakingProblems, []mutation{
		{"no proto job", func(w *wfFile) { delete(w.Jobs, "proto") }},
		{"only by hand", func(w *wfFile) { w.Jobs["proto"].If = "${{ github.event_name == 'workflow_dispatch' }}" }},
		{"a shallow checkout", func(w *wfFile) {
			for i := range w.Jobs["proto"].Steps {
				if isCheckout(w.Jobs["proto"].Steps[i]) {
					w.Jobs["proto"].Steps[i].With["fetch-depth"] = 1
				}
			}
		}},
		{"a job that continues on error", func(w *wfFile) { w.Jobs["proto"].ContinueOnError = true }},
		{"the check continues on error", func(w *wfFile) {
			for i := range w.Jobs["proto"].Steps {
				if strings.Contains(w.Jobs["proto"].Steps[i].Run, "make proto-breaking") {
					w.Jobs["proto"].Steps[i].ContinueOnError = true
				}
			}
		}},
		{"lint only", func(w *wfFile) {
			for i := range w.Jobs["proto"].Steps {
				w.Jobs["proto"].Steps[i].Run = strings.ReplaceAll(w.Jobs["proto"].Steps[i].Run, "make proto-breaking", "make proto")
			}
		}},
	})
}

// bufPinned is the only shape deploy/buf-image.txt may name: one exact buf
// release, by its multi-platform index digest.
var bufPinned = regexp.MustCompile(`^bufbuild/buf:([0-9]+\.[0-9]+\.[0-9]+)@sha256:[0-9a-f]{64}$`)

// bufPin is the image deploy/buf-image.txt names: its first line that is not
// blank or a comment.
func bufPin(t *testing.T) string {
	t.Helper()
	for _, line := range strings.Split(readRepoFileForPins(t, "deploy/buf-image.txt"), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			return line
		}
	}
	return ""
}

// TestMakefile_ProtoBreakingRunsThePinnedBufAgainstTheFirstRelease: the target
// CI runs compares with v0.1.0's own tree, through the pinned buf, offline.
func TestMakefile_ProtoBreakingRunsThePinnedBufAgainstTheFirstRelease(t *testing.T) {
	if pin := bufPin(t); !bufPinned.MatchString(pin) {
		t.Errorf("deploy/buf-image.txt names %q, not bufbuild/buf:X.Y.Z@sha256:<64 hex>", pin)
	}
	makefile := readRepoFileForPins(t, "Makefile")
	at := strings.Index(makefile, "\nproto-breaking:")
	if at < 0 {
		t.Fatal("the Makefile has no proto-breaking target")
	}
	end := strings.Index(makefile[at+1:], "\n\n")
	if end < 0 {
		end = len(makefile) - at - 1
	}
	recipe := makefile[at : at+1+end]
	for _, want := range []string{"PROTO_BASE ?= v0.1.0", "BUF_IMAGE = $(shell grep -v -e '^$(HASH)' -e '^$$' deploy/buf-image.txt | head -1)"} {
		if !strings.Contains(makefile, want) {
			t.Errorf("the Makefile never says %q", want)
		}
	}
	for _, want := range []string{
		`test -n "$(BUF_IMAGE)"`,
		`git archive "refs/tags/$(PROTO_BASE)" buf.yaml proto`,
		"--network none",
		"breaking --against /base",
	} {
		if !strings.Contains(recipe, want) {
			t.Errorf("make proto-breaking never says %q:\n%s", want, recipe)
		}
	}
	if !strings.Contains(readRepoFileForPins(t, "buf.yaml"), "breaking:") {
		t.Error("buf.yaml names no breaking rules")
	}
}

// bufInstall is the line of `make -n tools` that installs buf, and the version
// it names.
var bufInstall = regexp.MustCompile(`(?m)^go install github\.com/bufbuild/buf/cmd/buf@(\S*)$`)

// TestMakefile_ToolsInstallsTheBufTheImagePins: the buf `make tools` puts on
// PATH, which `make proto` lints and generates with, is the release
// deploy/buf-image.txt pins for `make proto-breaking`, never @latest.
func TestMakefile_ToolsInstallsTheBufTheImagePins(t *testing.T) {
	m := bufPinned.FindStringSubmatch(bufPin(t))
	if m == nil {
		t.Fatalf("deploy/buf-image.txt names %q, not bufbuild/buf:X.Y.Z@sha256:<64 hex>", bufPin(t))
	}
	out, err := exec.Command("make", "-n", "-s", "-C", mustRepoRoot(), "tools").CombinedOutput()
	if err != nil {
		t.Fatalf("make -n tools: %v\n%s", err, out)
	}
	got := bufInstall.FindAllStringSubmatch(string(out), -1)
	if len(got) != 1 {
		t.Fatalf("make tools installs buf %d times, want once:\n%s", len(got), out)
	}
	if want := "v" + m[1]; got[0][1] != want {
		t.Errorf("make tools installs buf@%s, and deploy/buf-image.txt pins buf %s:\n%s", got[0][1], want, out)
	}
}
