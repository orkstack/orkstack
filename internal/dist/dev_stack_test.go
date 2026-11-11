package dist_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/config"
	"github.com/orkstack/orkstack/internal/orker"

	"github.com/orkstack/orkstack/internal/testsupport/must"
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

// TestDevStack_EngineListensOnlyOnLoopback: the engine `make up` starts is a
// contributor's own too. Its API has no login, and whatever reaches it can start
// a run on this host's Docker, so it listens on 127.0.0.1 unless the contributor
// asks for more. Nothing needs more: orkctl and the UI's dev server run on this
// host, no activity container calls the API, and the suites and the proofs start
// engines of their own at addresses of their own, never through scripts/stack.sh.
func TestDevStack_EngineListensOnlyOnLoopback(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	code, out := s.up()

	if code != 0 {
		t.Fatalf("scripts/stack.sh up exited %d; want 0:\n%s", code, out)
	}
	if got := s.engineAddr(); got != "127.0.0.1:8080" {
		t.Errorf("the engine is told to listen on %q; want 127.0.0.1:8080, which no other machine reaches", got)
	}
	s.assertClientsDial("http://127.0.0.1:8080")
}

// TestDevStack_TheEngineListensWhereTheContributorSays is the positive control
// of the test above: the address it reads is the one the script chose, so a pass
// there is the default's doing. ORK_HTTP_ADDR is how a contributor opens the
// engine to other machines (CONTRIBUTING.md), and the script's own clients
// follow its port.
func TestDevStack_TheEngineListensWhereTheContributorSays(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	code, out := s.up("ORK_HTTP_ADDR=:9090")

	if code != 0 {
		t.Fatalf("scripts/stack.sh up exited %d; want 0:\n%s", code, out)
	}
	if got := s.engineAddr(); got != ":9090" {
		t.Errorf("the engine is told to listen on %q; want :9090, the address the contributor set", got)
	}
	s.assertClientsDial("http://127.0.0.1:9090")
}

// stackRepo is a throwaway repository holding the committed scripts/stack.sh,
// with stand-ins for the docker CLI, make, curl, sleep and bun first on PATH and
// for ork, orker and orkctl in its bin/, so `scripts/stack.sh up` runs as `make
// up` runs it and touches nothing of the host's: not the shared stack's
// containers, and no process but the stand-ins it starts, which the test stops
// by the pids the script recorded (AGENTS.md §11.1).
type stackRepo struct {
	t    *testing.T
	root string
	logs string
	path string
}

func newStackRepo(t *testing.T) *stackRepo {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Fatalf("bash is required (CONTRIBUTING.md): %v", err)
	}
	realSleep, err := exec.LookPath("sleep")
	must.OKf(t, err, "sleep is required")
	s := &stackRepo{t: t, root: t.TempDir(), logs: t.TempDir()}
	standIns := t.TempDir()
	s.path = standIns + string(os.PathListSeparator) + os.Getenv("PATH")
	t.Cleanup(s.stopStandIns)

	s.write(filepath.Join(s.root, "scripts", "stack.sh"), string(readRepoFile(t, "scripts/stack.sh")))
	// Present, so the script installs nothing.
	must.OK(t, os.MkdirAll(filepath.Join(s.root, "ui", "node_modules"), 0o755))
	// A process the script starts stays up until the test stops it, or until
	// this runs out if the test could not.
	stay := "exec " + shellQuote(realSleep) + " 120\n"
	record := func(name, what string) string {
		return `echo "` + what + `" >> ` + shellQuote(filepath.Join(s.logs, name)) + "\n"
	}
	for path, body := range map[string]string{
		// `docker info` fails, which skips the script's mount probe.
		filepath.Join(standIns, "docker"): `[ "$1" != info ] || exit 1` + "\n",
		// One activity, whose image curl does not find, so the script writes no
		// catalog.
		filepath.Join(standIns, "make"): `case "$*" in *print-activities*) echo shell ;; esac` + "\n",
		filepath.Join(standIns, "curl"): `case "$*" in
*/manifests/*) exit 22 ;;
*/ws/runs/*) printf 404 ;;
*http_code*) printf 200 ;;
esac
`,
		filepath.Join(standIns, "sleep"):      "",
		filepath.Join(standIns, "bun"):        record("ui", "$ORK_SERVER") + stay,
		filepath.Join(s.root, "bin", "ork"):   record("ork", "$*") + stay,
		filepath.Join(s.root, "bin", "orker"): stay,
		filepath.Join(s.root, "bin", "orkctl"): record("orkctl", "$*") +
			`echo '"liveOrkerCount": 1'` + "\n",
	} {
		s.write(path, "#!/bin/sh\n"+body)
	}
	return s
}

func (s *stackRepo) write(path, content string) {
	s.t.Helper()
	must.OK(s.t, os.MkdirAll(filepath.Dir(path), 0o755))
	must.OK(s.t, os.WriteFile(path, []byte(content), 0o755))
}

// up runs `scripts/stack.sh up` in the throwaway repository, with env and
// nothing of the test's own environment but PATH: an ORK_* or OTEL_* variable
// there would change what the script writes.
func (s *stackRepo) up(env ...string) (int, string) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(s.root, "scripts", "stack.sh"), "up")
	cmd.Env = append([]string{"PATH=" + s.path, "HOME=" + s.logs}, env...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	}
	s.t.Fatalf("run scripts/stack.sh up: %v\n%s", err, out)
	return 0, ""
}

// engineAddr is the address the engine reads from the configuration the script
// started it with, read as cmd/ork reads it.
func (s *stackRepo) engineAddr() string {
	s.t.Helper()
	file := filepath.Join(s.root, ".run", "orkstack.yaml")
	if got, want := strings.TrimSpace(s.await("ork")), "--config "+file; got != want {
		s.t.Fatalf("the engine was started with %q; want %q", got, want)
	}
	doc, err := config.Load(file, config.BinaryOrk)
	must.OK(s.t, err)
	addr := ""
	unset := func(string) (string, bool) { return "", false }
	if _, err := doc.Apply(unset, func(name, value string) error {
		if name == "ORK_HTTP_ADDR" {
			addr = value
		}
		return nil
	}); err != nil {
		s.t.Fatal(err)
	}
	return addr
}

// assertClientsDial: the script's own readiness checks and the UI's dev server
// both reach the engine at url.
func (s *stackRepo) assertClientsDial(url string) {
	s.t.Helper()
	calls, err := os.ReadFile(filepath.Join(s.logs, "orkctl"))
	must.OK(s.t, err)
	if !strings.Contains(string(calls), "--server "+url+" ") {
		s.t.Errorf("orkctl was called with %q; want --server %s", calls, url)
	}
	if got := strings.TrimSpace(s.await("ui")); got != url {
		s.t.Errorf("the UI's dev server proxies to %q; want %s", got, url)
	}
}

// await is what a stand-in the script started in the background recorded, once
// it has: `up` does not wait for one to get that far.
func (s *stackRepo) await(name string) string {
	s.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		raw, err := os.ReadFile(filepath.Join(s.logs, name))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			s.t.Fatal(err)
		}
		if strings.HasSuffix(string(raw), "\n") {
			return string(raw)
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("the %s stand-in recorded nothing within a minute", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stopStandIns stops the processes the script recorded, by those pids. Each
// stand-in ended in `exec sleep`, so its pid is the one process it holds.
func (s *stackRepo) stopStandIns() {
	pidfiles, err := filepath.Glob(filepath.Join(s.root, ".run", "*.pid"))
	if err != nil {
		s.t.Error(err)
	}
	for _, pidfile := range pidfiles {
		raw, err := os.ReadFile(pidfile)
		if err != nil {
			s.t.Error(err)
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			s.t.Errorf("%s: %v", pidfile, err)
			continue
		}
		if p, err := os.FindProcess(pid); err == nil {
			_ = p.Kill()
		}
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
	must.OK(t, os.WriteFile(filepath.Join(r.logs, "profiles"), []byte(strings.Join(profiles, "\n")+"\n"), 0o644))
	must.OK(t, os.WriteFile(filepath.Join(r.logs, "repo"), []byte(r.root), 0o644))
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
	must.OK(t, os.WriteFile(filepath.Join(r.bin, "docker"), []byte(docker), 0o755))
	return r
}

func (r *resetRepo) write(rel, content string, mode os.FileMode) {
	r.t.Helper()
	path := filepath.Join(r.root, filepath.FromSlash(rel))
	must.OK(r.t, os.MkdirAll(filepath.Dir(path), 0o755))
	must.OK(r.t, os.WriteFile(path, []byte(content), mode))
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
	must.OK(t, err)
	must.OK(t, os.WriteFile(filepath.Join(ws.Input, "input.json"), []byte("{}"), orker.ModeInputFile))
	must.OK(t, ws.Seal())
	must.OK(t, ws.SealSecrets())
	// A directory a workload made in its output/ that its owner can only
	// enter: neither rm nor a chmod that adds write alone can empty it.
	shut := filepath.Join(ws.Output, "tree")
	r.write(strings.TrimPrefix(filepath.Join(shut, "result.json"), r.root+string(filepath.Separator)), "{}", 0o644)
	must.OK(t, os.Chmod(shut, 0o100))
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
	must.OK(t, os.WriteFile(filepath.Join(r.logs, "left"), []byte("orkstack_stuck-data\n"), 0o644))

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
	must.OK(t, os.WriteFile(filepath.Join(r.logs, "recreate"), nil, 0o644))

	code, out := r.run()

	if code == 0 || !strings.Contains(out, "state survived: "+filepath.Join(r.root, ".data")) {
		t.Fatalf("scripts/reset.sh --force exited %d with .data/ written again after its removal; want a refusal naming it:\n%s", code, out)
	}
}
