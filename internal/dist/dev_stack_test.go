package dist_test

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/orker"
)

// TestDevCompose_PublishesOnlyOnLoopback: the development stack's services
// are a contributor's own. Temporal's frontend, the object store with its
// default key, an unauthenticated registry and Grafana with anonymous Admin
// are reachable from this host and from nowhere else, so every port the
// stack publishes, in every profile, is bound to 127.0.0.1. Nothing needs
// more: the host's processes, its Docker daemon pulling from the registry and
// the observability gate reach them on loopback, an activity container
// reaches only a test's own server on the host (host.docker.internal), and
// the observability profile's services find each other on the compose network
// (CONTRIBUTING.md, "Linux hosts").
func TestDevCompose_PublishesOnlyOnLoopback(t *testing.T) {
	t.Parallel()
	doc := parseCompose(t, devComposePath, readRepoFile(t, devComposePath))
	hostPort := regexp.MustCompile(`^[0-9]+$`)
	containerPort := regexp.MustCompile(`^[0-9]+(/(tcp|udp))?$`)
	published := 0
	for _, name := range doc.serviceNames() {
		for _, port := range doc.Services[name].Ports {
			parts := splitColon(port)
			if len(parts) != 3 || parts[0] != "127.0.0.1" || !hostPort.MatchString(parts[1]) || !containerPort.MatchString(parts[2]) {
				t.Errorf("service %s publishes %q; a published port is 127.0.0.1:<host port>:<container port>", name, port)
				continue
			}
			published++
		}
		if _, ok := doc.Services[name].Other["network_mode"]; ok {
			t.Errorf("service %s sets network_mode; a service on the host's network publishes every port it listens on", name)
		}
	}
	if published == 0 {
		t.Error("the development stack publishes no port; the rule above checked nothing")
	}
}

// resetRepo is a throwaway repository holding the committed scripts/reset.sh
// and deploy/docker-compose.yml, a stub scripts/stack.sh and a stand-in docker
// CLI first on PATH, so the script runs as `make reset-force` runs it and
// touches nothing of the host's: not the shared stack's containers and
// volumes, and not any process (AGENTS.md §11.1).
//
// The stand-in answers as Compose does for what the script asks. `compose …
// down` removes the services of the profiles it enables (every `--profile`,
// COMPOSE_PROFILES, `*` for all) and, with `--volumes`, their volumes; a
// profile it does not enable keeps its services' volumes, which `volume ls`
// then lists, as `orkstack_<profile>-data`. With a `recreate` flag, `volume
// ls` (which the script's check runs after its removals) also makes .data/
// again, as a process still writing there would.
type resetRepo struct {
	t    *testing.T
	root string
	bin  string
	logs string
}

func newResetRepo(t *testing.T) *resetRepo {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Fatalf("bash is required (CONTRIBUTING.md): %v", err)
	}
	r := &resetRepo{t: t, root: t.TempDir(), bin: t.TempDir(), logs: t.TempDir()}
	// What a failing run leaves read-only must not fail the test's own
	// cleanup, which runs after this one.
	t.Cleanup(func() { makeWritable(r.root) })
	r.write("scripts/reset.sh", string(readRepoFile(t, "scripts/reset.sh")), 0o755)
	r.write("deploy/docker-compose.yml", string(readRepoFile(t, devComposePath)), 0o644)
	r.write("scripts/stack.sh", "#!/bin/sh\necho \"$*\" >> "+shellQuote(filepath.Join(r.logs, "stack"))+"\n", 0o755)

	var profiles []string
	for _, svc := range parseCompose(t, devComposePath, readRepoFile(t, devComposePath)).Services {
		for _, p := range svc.Profiles {
			if !slices.Contains(profiles, p) {
				profiles = append(profiles, p)
			}
		}
	}
	if len(profiles) == 0 {
		t.Fatal("deploy/docker-compose.yml declares no profile; the stand-in would check nothing")
	}
	if err := os.WriteFile(filepath.Join(r.logs, "profiles"), []byte(strings.Join(profiles, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.logs, "repo"), []byte(r.root), 0o644); err != nil {
		t.Fatal(err)
	}
	docker := `#!/bin/sh
S=` + shellQuote(r.logs) + `
echo "$*" >> "$S/calls"
case "$1" in
compose)
  shift
  enabled=" $(printf '%s' "${COMPOSE_PROFILES:-}" | tr ',' ' ') "
  down=0 volumes=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --profile) enabled="$enabled $2 "; shift 2; continue ;;
      down) down=1 ;;
      --volumes|-v) volumes=1 ;;
    esac
    shift
  done
  [ "$down" = 1 ] || exit 0
  : > "$S/down"
  [ "$volumes" = 1 ] || echo orkstack_seaweedfs-data >> "$S/left"
  while read -r p; do
    case "$enabled" in *" * "*|*" $p "*) ;; *) echo "orkstack_$p-data" >> "$S/left" ;; esac
  done < "$S/profiles"
  exit 0 ;;
volume)
  [ ! -e "$S/recreate" ] || mkdir -p "$(cat "$S/repo")/.data/work"
  [ -e "$S/down" ] || echo orkstack_seaweedfs-data
  [ ! -e "$S/left" ] || cat "$S/left"
  exit 0 ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(r.bin, "docker"), []byte(docker), 0o755); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *resetRepo) write(rel, content string, mode os.FileMode) {
	r.t.Helper()
	path := filepath.Join(r.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		r.t.Fatal(err)
	}
}

// run runs `scripts/reset.sh --force` in the throwaway repository and returns
// its exit status and output. It runs as the account that owns the
// repository: the one running the tests, or, when that is root (whom no
// permission stops), an unprivileged account the repository is handed to.
func (r *resetRepo) run() (int, string) {
	r.t.Helper()
	cmd := exec.Command("bash", filepath.Join(r.root, "scripts", "reset.sh"), "--force")
	if os.Geteuid() == 0 {
		r.dropPrivileges(cmd)
	}
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "PATH", "COMPOSE_PROFILES", "COMPOSE_FILE", "COMPOSE_PROJECT_NAME", "ORK_ARTIFACT_DIR":
			continue
		}
		env = append(env, kv)
	}
	cmd.Env = append(env, "PATH="+r.bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	}
	r.t.Fatalf("run scripts/reset.sh: %v\n%s", err, out)
	return 0, ""
}

// downCalls are the `docker compose … down …` invocations the script made.
func (r *resetRepo) downCalls() []string {
	r.t.Helper()
	raw, err := os.ReadFile(filepath.Join(r.logs, "calls"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		r.t.Fatal(err)
	}
	var downs []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "compose ") && slices.Contains(strings.Fields(line), "down") {
			downs = append(downs, line)
		}
	}
	return downs
}

func makeWritable(root string) {
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			_ = os.Chmod(path, 0o755)
		}
		return nil
	})
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// TestResetScript_RemovesWorkspacesTheOrkerSealed: an Orker seals a
// workspace's input/ read-only (0555), and one that ended uncleanly — an e2e
// Orker killed mid-task — leaves it under .data/work. `make reset` removes it
// all the same, and its own check finds no state left. (root removes a
// read-only directory anyway, so a test run as root hands the script to an
// unprivileged account that owns the repository, as the contributor is:
// resetRepo.run.)
func TestResetScript_RemovesWorkspacesTheOrkerSealed(t *testing.T) {
	t.Parallel()
	r := newResetRepo(t)
	ws, err := orker.NewWorkspace(filepath.Join(r.root, ".data", "work", "e2e-run-page-p1-abc", "ork-run-1-task-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws.Input, "input.json"), []byte("{}"), orker.ModeInputFile); err != nil {
		t.Fatal(err)
	}
	if err := ws.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := ws.SealSecrets(); err != nil {
		t.Fatal(err)
	}
	// A directory a workload made in its output/ that its owner can only
	// enter: neither rm nor a chmod that adds write alone can empty it.
	shut := filepath.Join(ws.Output, "tree")
	r.write(strings.TrimPrefix(filepath.Join(shut, "result.json"), r.root+string(filepath.Separator)), "{}", 0o644)
	if err := os.Chmod(shut, 0o100); err != nil {
		t.Fatal(err)
	}
	r.write(".data/artifacts/orkflows/hello.json", "{}", 0o644)

	code, out := r.run()

	if code != 0 {
		t.Fatalf("scripts/reset.sh --force exited %d over a sealed workspace; want 0:\n%s", code, out)
	}
	if _, err := os.Lstat(filepath.Join(r.root, ".data")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf(".data survived the reset (%v):\n%s", err, out)
	}
	if !strings.Contains(out, "Vanilla.") {
		t.Errorf("the reset did not report a vanilla machine:\n%s", out)
	}
}

// TestResetScript_TakesDownEveryProfile: `compose down --volumes` without a
// profile leaves that profile's services and their volumes (the observability
// profile's Grafana, Loki, Prometheus and Tempo), and the reset's own check
// then refuses. The reset takes every profile down, whichever the compose
// file declares.
func TestResetScript_TakesDownEveryProfile(t *testing.T) {
	t.Parallel()
	r := newResetRepo(t)

	code, out := r.run()

	downs := r.downCalls()
	if code != 0 {
		t.Fatalf("scripts/reset.sh --force exited %d; want 0 (downs %q):\n%s", code, downs, out)
	}
	if len(downs) != 1 || !slices.Contains(strings.Fields(downs[0]), "--volumes") {
		t.Errorf("compose downs = %q; want one, with --volumes", downs)
	}
}

// TestResetScript_RefusesWhenAVolumeSurvives is the positive control of the
// test above: the reset's own check bites on a volume that survives the down,
// so a pass there is the down's doing, not a check that looks at nothing.
func TestResetScript_RefusesWhenAVolumeSurvives(t *testing.T) {
	t.Parallel()
	r := newResetRepo(t)
	if err := os.WriteFile(filepath.Join(r.logs, "left"), []byte("orkstack_stuck-data\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	code, out := r.run()

	if code == 0 || !strings.Contains(out, "volumes survived:") || !strings.Contains(out, "orkstack_stuck-data") {
		t.Fatalf("scripts/reset.sh --force exited %d with a surviving volume; want a refusal naming it:\n%s", code, out)
	}
}

// TestResetScript_RefusesWhenStateSurvives is the positive control of the
// removals: the reset's own check bites on state found again after them (a
// process still writing .data/), so a pass above is the removal's doing, not
// a check that looks at nothing.
func TestResetScript_RefusesWhenStateSurvives(t *testing.T) {
	t.Parallel()
	r := newResetRepo(t)
	if err := os.WriteFile(filepath.Join(r.logs, "recreate"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	code, out := r.run()

	if code == 0 || !strings.Contains(out, "state survived: "+filepath.Join(r.root, ".data")) {
		t.Fatalf("scripts/reset.sh --force exited %d with .data/ written again after its removal; want a refusal naming it:\n%s", code, out)
	}
}
