package docscheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// mirrorAction is the step that points a runner's Docker daemon at the
// Docker Hub mirror (docs/PROOFS.md §5).
const mirrorAction = "./.github/actions/registry-mirror"

// proofHostAction runs mirrorAction itself, with the DNS it needs, before
// anything of Docker's, so a job that uses it restarts the daemon once.
const proofHostAction = "./.github/actions/proof-host"

// mirrorExempt are the jobs that drive Docker and pull nothing from Docker
// Hub, with why.
var mirrorExempt = map[string]string{
	"release.yml/publish": "runs only the cosign (ghcr.io) and skopeo (quay.io) images, which no Docker Hub limit reaches",
}

// usesDocker matches a run script that drives a container engine, or one of
// the repository's scripts that does.
var usesDocker = regexp.MustCompile(`\b(docker|podman)\b|quickstart-smoke|kind-orker\.sh|integration\.sh`)

type workflowStep struct {
	ID   string         `yaml:"id"`
	Name string         `yaml:"name"`
	If   string         `yaml:"if"`
	Uses string         `yaml:"uses"`
	Run  string         `yaml:"run"`
	With map[string]any `yaml:"with"`
}

// makeCall is a make invocation in a run script: its directory, if -C names
// one, and its target.
var makeCall = regexp.MustCompile(`\bmake\s+(?:-C\s+(\S+)\s+)?([a-z][a-z0-9-]*)`)

// dockerTargets are the targets of the Makefile in dir whose recipe drives
// Docker, directly or through a script of the repository's that does.
func dockerTargets(t *testing.T, root, dir string) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, dir, "Makefile"))
	if err != nil {
		return nil
	}
	script := regexp.MustCompile(`scripts/[A-Za-z0-9_.-]+\.sh`)
	targets, current := map[string]bool{}, ""
	for _, line := range strings.Split(string(b), "\n") {
		if m := regexp.MustCompile(`^([a-z][a-z0-9-]*):`).FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		if current == "" || !strings.HasPrefix(line, "\t") {
			continue
		}
		hit := usesDocker.MatchString(line)
		for _, s := range script.FindAllString(line, -1) {
			if body, err := os.ReadFile(filepath.Join(root, s)); err == nil && usesDocker.MatchString(string(body)) {
				hit = true
			}
		}
		if hit {
			targets[current] = true
		}
	}
	return targets
}

// dockerStep reports whether a step starts a container, a builder or a pull:
// itself, or through a make target that does.
func dockerStep(s workflowStep, targets func(dir string) map[string]bool) bool {
	for _, m := range makeCall.FindAllStringSubmatch(s.Run, -1) {
		if targets(m[1])[m[2]] {
			return true
		}
	}
	switch {
	case strings.Contains(s.Uses, "/proof-host"),
		strings.Contains(s.Uses, "docker/setup-buildx-action"),
		strings.Contains(s.Uses, "docker/setup-qemu-action"):
		return true
	case strings.Contains(s.Uses, "/proof-fetch"):
		image, _ := s.With["image"].(string)
		return image != ""
	}
	return usesDocker.MatchString(s.Run)
}

// Every job of every workflow that drives Docker first points its daemon at
// the Docker Hub mirror, before any container, builder or pull of its own,
// unless mirrorExempt names it; and every buildx builder it starts reads the
// same mirror. Docker Hub limits anonymous pulls per IP, and a hosted
// runner's address is shared.
func TestWorkflows_DockerHubPullsGoThroughTheMirror(t *testing.T) {
	root := repoRoot(t)
	files, err := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	must.OK(t, err)
	makefiles := map[string]map[string]bool{}
	targets := func(dir string) map[string]bool {
		if _, ok := makefiles[dir]; !ok {
			makefiles[dir] = dockerTargets(t, root, dir)
		}
		return makefiles[dir]
	}
	if !targets("")["proto-breaking"] {
		t.Fatal("make proto-breaking, which runs buf's image, is not seen as driving Docker: the target scan is broken")
	}
	checked, seen := 0, map[string]bool{}
	for _, file := range files {
		var doc struct {
			Jobs map[string]struct {
				Steps []workflowStep `yaml:"steps"`
			} `yaml:"jobs"`
		}
		b, err := os.ReadFile(file)
		must.OK(t, err)
		must.OK(t, yaml.Unmarshal(b, &doc))
		for name, job := range doc.Jobs {
			key := filepath.Base(file) + "/" + name
			mirror, first, restarts := -1, -1, 0
			for i, s := range job.Steps {
				if s.Uses == mirrorAction || s.Uses == proofHostAction {
					restarts++
					if mirror < 0 {
						mirror = i
					}
					if s.If != "" {
						t.Errorf("%s runs %s only if %s; it must run unconditionally", key, s.Uses, s.If)
					}
				}
				if strings.Contains(s.Run, "systemctl restart docker") {
					t.Errorf("%s restarts Docker in a step of its own (%s); only %s does", key, s.Name, mirrorAction)
				}
				if dockerStep(s, targets) && first < 0 {
					first = i
				}
				if strings.Contains(s.Uses, "docker/setup-buildx-action") {
					config, _ := s.With["buildkitd-config-inline"].(string)
					if !strings.Contains(config, `[registry."docker.io"]`) || !strings.Contains(config, `mirrors = ["mirror.gcr.io"]`) {
						t.Errorf("%s: its buildx builder (%s) does not read the Docker Hub mirror in buildkitd-config-inline", key, s.Uses)
					}
				}
			}
			if first < 0 {
				continue
			}
			seen[key] = true
			if _, ok := mirrorExempt[key]; ok {
				continue
			}
			checked++
			if mirror < 0 || mirror > first {
				t.Errorf("%s drives Docker at step %d and does not run %s before it", key, first, mirrorAction)
			}
			if restarts > 1 {
				t.Errorf("%s restarts Docker %d times: docker.service may start 3 times in 60 s, the runner's boot among them; %s runs %s itself", key, restarts, proofHostAction, mirrorAction)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no workflow job drives Docker; the check has nothing to hold")
	}
	for key := range mirrorExempt {
		if !seen[key] {
			t.Errorf("mirrorExempt names %s, which drives no Docker", key)
		}
	}
}

// One action writes daemon.json and restarts Docker, once: docker.service may
// start 3 times in 60 s, the runner's boot among them. It clears that count
// first, verifies what it set, and says why when the restart fails. proof-host runs
// it with its DNS, before anything of Docker's, and a container then resolves
// the name that DNS serves. Podman, which reads no daemon.json, names the
// mirror in registries.conf.
func TestWorkflows_TheMirrorIsKeptAndVerified(t *testing.T) {
	root := repoRoot(t)
	action := readFile(t, filepath.Join(root, ".github", "actions", "registry-mirror", "action.yml"))
	last := -1
	for _, want := range []string{"https://mirror.gcr.io", ".dns = [$d]", "systemctl reset-failed docker", "systemctl restart docker", "journalctl", "RegistryConfig.Mirrors", "does not report the mirror"} {
		at := strings.Index(action, want)
		if at < 0 {
			t.Errorf("the registry-mirror action has no %q", want)
			continue
		}
		if at < last {
			t.Errorf("the registry-mirror action has %q too early", want)
		}
		last = at
	}
	if n := strings.Count(action, "systemctl restart docker"); n != 1 {
		t.Errorf("the registry-mirror action restarts Docker %d times; want once", n)
	}

	path := filepath.Join(root, ".github", "actions", "proof-host", "action.yml")
	var host struct {
		Runs struct {
			Steps []workflowStep `yaml:"steps"`
		} `yaml:"runs"`
	}
	must.OK(t, yaml.Unmarshal([]byte(readFile(t, path)), &host))
	mirror := -1
	for i, s := range host.Runs.Steps {
		if s.Uses == mirrorAction {
			mirror = i
			// The DNS is the one an earlier step of proof-host set, under the
			// name that step writes, and the container check runs on it.
			dns, _ := s.With["dns"].(string)
			ref := regexp.MustCompile(`^\$\{\{ steps\.([a-z]+)\.outputs\.dns \}\}$`).FindStringSubmatch(dns)
			if ref == nil {
				t.Fatalf("proof-host gives the registry-mirror action dns %q; want an earlier step's output", dns)
			}
			wrote, checked := false, false
			for j, o := range host.Runs.Steps {
				wrote = wrote || j < i && o.ID == ref[1] && strings.Contains(o.Run, `echo "dns=172.17.0.1" >>"$GITHUB_OUTPUT"`)
				checked = checked || j > i && o.If == "steps."+ref[1]+".outputs.dns != ''" && strings.Contains(o.Run, "getent hosts host.docker.internal")
			}
			if !wrote || !checked {
				t.Errorf("proof-host's DNS: set by step %q before the daemon's restart %v, resolved in a container after it %v", ref[1], wrote, checked)
			}
			if s.If != "" {
				t.Errorf("proof-host runs the registry-mirror action only if %s", s.If)
			}
			continue
		}
		if strings.Contains(s.Run, "daemon.json") || strings.Contains(s.Run, "systemctl restart docker") {
			t.Errorf("proof-host's step %q sets up the Docker daemon itself; only %s does", s.Name, mirrorAction)
		}
		if mirror < 0 && regexp.MustCompile(`\bdocker (run|pull|compose|load|image)\b`).MatchString(s.Run) {
			t.Errorf("proof-host's step %q drives Docker before it runs %s", s.Name, mirrorAction)
		}
	}
	if mirror < 0 {
		t.Errorf("proof-host does not run %s", mirrorAction)
	}

	podman := readFile(t, filepath.Join(root, ".github", "workflows", "podman.yml"))
	for _, want := range []string{`prefix = "docker.io"`, `location = "mirror.gcr.io"`} {
		if !strings.Contains(podman, want) {
			t.Errorf("podman.yml's registries.conf has no %s", want)
		}
	}
}
