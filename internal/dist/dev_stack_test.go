package dist_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/config"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/orker"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
)

// TestDevCompose_PublishesOnlyOnLoopback: the development stack's services
// are a contributor's own. Temporal's frontend, the object store with its
// default key, an unauthenticated registry and Grafana with an anonymous Viewer
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

// TestDevStack_IdentityEnforces: by default make up writes a configuration
// on which the engine enforces, trusting the development issuer, with dev-admin in
// auth.admins and the Orker bound by auth.orkers and reaching the engine
// with its own token; the tokens are signed afresh, and the script's own
// orkctl calls present the admin's (CONTRIBUTING.md).
func TestDevStack_IdentityEnforces(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	code, out := s.up()

	if code != 0 {
		t.Fatalf("scripts/stack.sh up exited %d; want 0:\n%s", code, out)
	}
	file := filepath.Join(s.root, ".run", "orkstack.yaml")
	s.await("ork")
	read := func(bin config.Binary) map[string]string {
		doc, err := config.Load(file, bin)
		must.OK(t, err)
		env := map[string]string{}
		unset := func(string) (string, bool) { return "", false }
		if _, err := doc.Apply(unset, func(name, value string) error { env[name] = value; return nil }); err != nil {
			t.Fatal(err)
		}
		return env
	}
	ork := read(config.BinaryOrk)
	if ork["ORK_AUTH_MODE"] != "enforce" || !strings.Contains(ork["ORK_AUTH_ADMINS"], "claims:dev:sub=dev-admin") ||
		!strings.Contains(ork["ORK_AUTH_ORKERS"], "claims:dev:sub=dev-orker") || !strings.Contains(ork["ORK_AUTH_ISSUERS"], "jwks_file:") {
		t.Fatalf("the engine's auth settings = %v", ork)
	}
	// enforce needs a signing key, which signs what engines hand each other,
	// and the tokens of the task identity issuer: a name nothing resolves, so
	// the engine never asks it whether it serves the key, and serves the
	// discovery itself, on its own loopback address. The Orker's storage key
	// is its own, so an unscoped one is refused, the default.
	if !strings.HasSuffix(ork["ORK_IDENTITY_SIGNING_KEY_FILES"], filepath.Join(".data", "identity", "engine-signing.key")) ||
		ork["ORK_IDENTITY_SIGNING_ALGORITHM"] != "ES256" || ork["ORK_IDENTITY_ISSUER"] != "https://orkstack-dev.invalid" ||
		ork["ORK_IDENTITY_SERVE_DISCOVERY"] != "true" || ork["ORK_IDENTITY_SIGNING_VERIFY_PUBLISHED"] != "false" ||
		ork["ORK_IDENTITY_SIGNING_ACTIVATION_DELAY"] != "0s" || ork["ORK_IDENTITY_UNSCOPED_ORKERS"] != "" {
		t.Fatalf("the engine's identity settings = %v", ork)
	}
	if _, err := identity.CheckIssuerURL(ork["ORK_IDENTITY_ISSUER"]); err != nil {
		t.Fatalf("the engine would refuse its issuer: %v", err)
	}
	orkerEnv := read(config.BinaryOrker)
	settings := orker.EngineSettingsFromEnv(func(k string) string { return orkerEnv[k] })
	if err := settings.Validate(); err != nil || settings.Method != orker.EngineAuthTokenFile || orkerEnv["ORK_ORKER_ID"] != "dev-orker" {
		t.Fatalf("the Orker's engine settings = %+v (%v), id %q", settings, err, orkerEnv["ORK_ORKER_ID"])
	}
	// The Orker signs to the store with a key of its own, the engine with the
	// store's administrator's.
	if ork["ORK_S3_ACCESS_KEY"] != "orkstack" || ork["ORK_S3_SECRET_KEY"] != "orkstack-secret" ||
		!strings.HasPrefix(orkerEnv["ORK_S3_ACCESS_KEY"], "orkstack-dev-orker-") || len(orkerEnv["ORK_S3_SECRET_KEY"]) != 48 {
		t.Fatalf("the storage keys: engine %q, Orker %q (secret of %d)", ork["ORK_S3_ACCESS_KEY"], orkerEnv["ORK_S3_ACCESS_KEY"], len(orkerEnv["ORK_S3_SECRET_KEY"]))
	}
	signed, err := os.ReadFile(filepath.Join(s.logs, "devtoken"))
	must.OK(t, err)
	if strings.Count(string(signed), "sign ") != 2 || !strings.Contains(string(signed), "init ") {
		t.Fatalf("devtoken was called %q; want init and two signs", signed)
	}
	token, err := os.ReadFile(filepath.Join(s.logs, "orkctl-token"))
	must.OK(t, err)
	if !strings.Contains(string(token), filepath.Join(".data", "identity", "admin.token")) {
		t.Fatalf("the script's orkctl calls presented %q; want the admin's token", token)
	}
	if !strings.Contains(out, "export ORK_TOKEN_FILE=") {
		t.Fatalf("up printed no ORK_TOKEN_FILE line:\n%s", out)
	}
	// People sign in at Dex: the issuer people offers sign-in with a refresh
	// token (offline_access), the sign-in key exists, and Dex was started for
	// the UI's callback, its container recorded for make down.
	issuers, err := authn.ParseIssuers([]byte(ork["ORK_AUTH_ISSUERS"]))
	must.OK(t, err)
	if people, ok := issuers.ByName("people"); !ok || people.Login == nil || people.Login.CLIClientID != "orkctl" ||
		!strings.HasSuffix(people.Issuer, "/dex") || !slices.Contains(people.Login.Scopes, "offline_access") {
		t.Fatalf("the people issuer = %+v", people)
	}
	if !strings.HasSuffix(ork["ORK_AUTH_SIGNIN_KEY_FILES"], filepath.Join(".data", "identity", "signin.key")) || ork["ORK_AUTH_PUBLIC_URL"] == "" {
		t.Fatalf("the sign-in settings = %v", ork)
	}
	if info, err := os.Stat(filepath.Join(s.root, ".data", "identity", "signin.key")); err != nil || info.Size() != 32 || info.Mode().Perm() != 0o600 {
		t.Fatalf("the sign-in key: %v %v", info, err)
	}
	started, err := os.ReadFile(filepath.Join(s.logs, "dexrun"))
	must.OK(t, err)
	if !strings.Contains(string(started), "start ") || !strings.Contains(string(started), "/v1/auth/callback/people") {
		t.Fatalf("dexrun was called with %q", started)
	}
	cid, err := os.ReadFile(filepath.Join(s.root, ".run", "dex.cid"))
	if err != nil || strings.TrimSpace(string(cid)) != "0123abcd" {
		t.Fatalf("the Dex container recorded for make down: %q %v", cid, err)
	}
}

// storeGrant is the s3.configure make up gives the store for its Orker: one
// identity named for the worktree, whose access key is its name, a secret of
// 24 random bytes and the documented Orker policy on the stack's bucket.
var storeGrant = regexp.MustCompile(`^s3\.configure -user=(orkstack-dev-orker-[0-9a-f]{12}) -access_key=(orkstack-dev-orker-[0-9a-f]{12}) -secret_key=([0-9a-f]{48}) -actions=(\S+) -apply$`)

// TestDevStack_TheOrkerHasAStorageKeyOfItsOwn: with IDENTITY=1, make up
// gives the store one identity for this worktree's Orker, scoped to the
// stack's bucket, and the same one again at the next make up; it never names
// the administrator's key; the default namespace may ask for the task
// audience; make down removes that identity and no other (CONTRIBUTING.md,
// "Task identity on the development stack").
func TestDevStack_TheOrkerHasAStorageKeyOfItsOwn(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	for i := 1; i <= 2; i++ {
		if code, out := s.up(); code != 0 {
			t.Fatalf("make up %d exited %d:\n%s", i, code, out)
		}
	}
	raw, err := os.ReadFile(filepath.Join(s.logs, "store"))
	must.OK(t, err)
	grants := changes(raw)
	if len(grants) != 2 || grants[0] != grants[1] {
		t.Fatalf("two make ups gave the store %q; want one grant, twice the same", grants)
	}
	m := storeGrant.FindStringSubmatch(grants[0])
	if m == nil || m[1] != m[2] || m[4] != strings.Join(s3scope.OrkerPolicy().SeaweedActions("orkstack-artifacts"), ",") {
		t.Fatalf("make up gave the store %q", grants[0])
	}
	secret, err := os.ReadFile(filepath.Join(s.root, ".data", "identity", "orker-store.secret"))
	must.OK(t, err)
	if info, err := os.Stat(filepath.Join(s.root, ".data", "identity", "orker-store.secret")); err != nil ||
		info.Mode().Perm() != 0o600 || string(secret) != m[3] {
		t.Fatalf("the Orker's secret file: %v %v", info, err)
	}
	if mark, err := os.ReadFile(filepath.Join(s.root, ".run", "orker-store-user")); err != nil || strings.TrimSpace(string(mark)) != m[1] {
		t.Fatalf("make up recorded %q (%v) for make down; want %s", mark, err, m[1])
	}
	calls, err := os.ReadFile(filepath.Join(s.logs, "orkctl"))
	must.OK(t, err)
	if !strings.Contains(string(calls), "namespace update default --audience dev.orkstack.invalid") {
		t.Fatalf("orkctl was called %q; want the default namespace given the task audience", calls)
	}

	code, out := s.run("down")
	if code != 0 {
		t.Fatalf("make down exited %d:\n%s", code, out)
	}
	raw, err = os.ReadFile(filepath.Join(s.logs, "store"))
	must.OK(t, err)
	if lines := changes(raw); len(lines) != 3 || lines[2] != "s3.configure -user="+m[1]+" -delete -apply" {
		t.Fatalf("make down gave the store %q; want the Orker's identity deleted, and nothing else", lines[2:])
	}
	if _, err := os.Stat(filepath.Join(s.root, ".run", "orker-store-user")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("make down left its record of the Orker's key: %v", err)
	}
	if strings.Contains(string(raw), "-user=orkstack ") || strings.Contains(string(raw), "-access_key=orkstack ") {
		t.Fatalf("the stack named the store's administrator: %q", raw)
	}
}

// changes is the identity changes the stand-in store was given, without
// the listings make up and make down read back.
func changes(raw []byte) []string {
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.HasPrefix(line, "s3.configure -user=") {
			out = append(out, line)
		}
	}
	return out
}

var (
	seaweedscopeOnce sync.Once
	seaweedscopeBin  string
	seaweedscopeErr  error
)

// seaweedscope is test/tools/seaweedscope, built once for the stand-in
// stacks: make up grants the actions it prints.
func seaweedscope(t *testing.T) string {
	t.Helper()
	seaweedscopeOnce.Do(func() {
		dir, err := os.MkdirTemp("", "seaweedscope-")
		if err != nil {
			seaweedscopeErr = err
			return
		}
		seaweedscopeBin = filepath.Join(dir, "seaweedscope")
		cmd := exec.Command("go", "build", "-o", seaweedscopeBin, "./test/tools/seaweedscope")
		cmd.Dir = repoRoot(t)
		out, err := cmd.CombinedOutput()
		if err != nil {
			seaweedscopeErr = fmt.Errorf("go build ./test/tools/seaweedscope: %v: %s", err, out)
		}
	})
	must.OK(t, seaweedscopeErr)
	return seaweedscopeBin
}

// TestDevStack_AStoreThatIgnoresTheGrantFailsUp: weed shell says nothing
// of a command that failed, so make up asks the store, and a store that does
// not list the Orker's key fails it, naming the key.
func TestDevStack_AStoreThatIgnoresTheGrantFailsUp(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	code, out := s.up("STORE_IGNORES=1")

	if code == 0 || !strings.Contains(out, "does not list the Orker's key orkstack-dev-orker-") {
		t.Fatalf("make up on a store that ignores the grant: exit %d:\n%s", code, out)
	}
}

// TestDevStack_DownKeepsTheRecordOfAKeyItCouldNotRemove: a store that is not
// running, or still lists the key, leaves make down's record of it, so a
// later make down removes it; only a store that no longer lists it clears it.
func TestDevStack_DownKeepsTheRecordOfAKeyItCouldNotRemove(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)
	if code, out := s.up(); code != 0 {
		t.Fatalf("make up exited %d:\n%s", code, out)
	}
	mark := filepath.Join(s.root, ".run", "orker-store-user")

	if code, out := s.run("down", "STORE_STOPPED=1"); code != 0 || !strings.Contains(out, "the store is not running") {
		t.Fatalf("make down with the store stopped: exit %d:\n%s", code, out)
	}
	if _, err := os.Stat(mark); err != nil {
		t.Fatalf("make down with the store stopped dropped its record: %v", err)
	}
	if code, out := s.run("down", "STORE_IGNORES=1"); code == 0 || !strings.Contains(out, "still lists the Orker's storage key") {
		t.Fatalf("make down on a store that ignores the delete: exit %d:\n%s", code, out)
	}
	if _, err := os.Stat(mark); err != nil {
		t.Fatalf("make down that could not remove the key dropped its record: %v", err)
	}
	if code, out := s.run("down"); code != 0 {
		t.Fatalf("make down exited %d:\n%s", code, out)
	}
	if _, err := os.Stat(mark); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("make down left its record of a removed key: %v", err)
	}
}

// TestDevStack_GrantsNothingOnAProofsBucket: make up refuses a bucket shaped
// like a proof's own, and tells the store nothing.
func TestDevStack_GrantsNothingOnAProofsBucket(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	code, out := s.up("ORK_S3_BUCKET=orkstack-it-1234-567")

	if code == 0 || !strings.Contains(out, "shaped like a proof's own bucket") {
		t.Fatalf("make up on a proof's bucket: exit %d:\n%s", code, out)
	}
	// Temporal's health check is a docker exec too, with nothing on stdin.
	if raw, err := os.ReadFile(filepath.Join(s.logs, "store")); err != nil && !errors.Is(err, fs.ErrNotExist) || len(raw) != 0 {
		t.Fatalf("make up told the store %q (%v)", raw, err)
	}
}

// TestDevStack_TheIdentityTaskAsksForTheStacksAudience: the task make up
// names holds an identity for the one audience the stack's default namespace
// allows.
func TestDevStack_TheIdentityTaskAsksForTheStacksAudience(t *testing.T) {
	t.Parallel()
	m := regexp.MustCompile(`(?m)^TASK_AUDIENCE="([^"]+)"$`).FindSubmatch(readRepoFile(t, "scripts/stack.sh"))
	if m == nil {
		t.Fatal("scripts/stack.sh sets no TASK_AUDIENCE")
	}
	flow, err := dsl.CompileFile(filepath.Join(repoRoot(t), "deploy", "dev", "identity.ork.yaml"))
	must.OK(t, err)
	tasks := flow.GetTasks()
	if len(tasks) != 1 || len(tasks[0].GetIdentities()) != 1 || tasks[0].GetIdentities()[0].GetAudience() != string(m[1]) {
		t.Fatalf("deploy/dev/identity.ork.yaml's tasks: %v; want one, holding one identity for %s", tasks, m[1])
	}
}

// TestDevStack_IdentityZeroIsAsserted: IDENTITY=0 runs the stack asserted:
// no Dex, no sign-in settings, no development tokens.
func TestDevStack_IdentityZeroIsAsserted(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	code, out := s.up("IDENTITY=0")

	if code != 0 {
		t.Fatalf("scripts/stack.sh up exited %d; want 0:\n%s", code, out)
	}
	s.engineAddr() // the engine was started on the script's configuration
	conf, err := os.ReadFile(filepath.Join(s.root, ".run", "orkstack.yaml"))
	must.OK(t, err)
	if strings.Contains(string(conf), "auth:") || strings.Contains(string(conf), "mode: enforce") || strings.Contains(string(conf), "identity:") {
		t.Fatalf("IDENTITY=0 wrote auth settings:\n%s", conf)
	}
	if _, err := os.Stat(filepath.Join(s.logs, "dexrun")); err == nil {
		t.Fatal("IDENTITY=0 started Dex")
	}
}

// TestDevStack_IdentityRefusesOtherValues: IDENTITY is 1 or 0; anything else
// is refused before anything starts, rather than read as one of them.
func TestDevStack_IdentityRefusesOtherValues(t *testing.T) {
	t.Parallel()
	s := newStackRepo(t)

	code, out := s.up("IDENTITY=yes")

	if code != 2 || !strings.Contains(out, "IDENTITY is 1") {
		t.Fatalf("IDENTITY=yes: exit %d:\n%s", code, out)
	}
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
	s.write(filepath.Join(s.root, "deploy", "dex-image.txt"), string(readRepoFile(t, "deploy/dex-image.txt")))
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
		// `docker exec -i ... weed shell` is a store of one identity list,
		// kept in a file, that records every command it was given.
		filepath.Join(standIns, "docker"): `state=` + shellQuote(filepath.Join(s.logs, "identities")) + `
case "$1" in
info) exit 1 ;;
container) [ "$2" != inspect ] || { [ -n "${STORE_STOPPED:-}" ] && echo false || echo true; exit 0; } ;;
exec) in="$(cat)"; [ -n "$in" ] || exit 0
	printf '%s\n' "$in" >> ` + shellQuote(filepath.Join(s.logs, "store")) + `
	[ -z "${STORE_IGNORES:-}" ] || in=s3.configure
	case "$in" in
	"s3.configure -user="*" -delete -apply") : > "$state" ;;
	"s3.configure -user="*" -apply") u="${in#*-user=}"; a="${in#*-actions=}"; printf '%s %s\n' "${u%% *}" "${a% -apply}" > "$state" ;;
	s3.configure) if [ -s "$state" ]; then read -r u a < "$state"
		printf '{"identities": [{"name": "%s", "actions": ["%s"]}]}\n' "$u" "$(printf '%s' "$a" | sed 's/,/", "/g')"
		else echo '{"identities": []}'; fi ;;
	esac ;;
esac
` + "\n",
		// One activity, whose image curl does not find, so the script writes no
		// catalog.
		filepath.Join(standIns, "make"): `case "$*" in *print-activities*) echo shell ;; esac` + "\n",
		// A read the Orker's own key may make of a missing key: not found.
		filepath.Join(standIns, "curl"): `case "$*" in
*/metadata/probes/*) if [ -s ` + shellQuote(filepath.Join(s.logs, "identities")) + ` ]; then printf 404; else printf 403; fi ;;
*/manifests/*) exit 22 ;;
*/ws/runs/*) printf 404 ;;
*http_code*) printf 200 ;;
esac
`,
		filepath.Join(standIns, "sleep"): "",
		// `go build -o OUT` writes a stand-in that records its calls: devtoken's
		// returns; dexrun's prints the container it started, as dexrun does.
		filepath.Join(standIns, "go"): `out=""; while [ $# -gt 0 ]; do [ "$1" = -o ] && { out="$2"; shift; }; shift; done
case "$out" in
*/seaweedscope) cp ` + shellQuote(seaweedscope(t)) + ` "$out" ;;
*/dexrun) printf '#!/bin/sh\necho "$*" >> ` + shellQuote(filepath.Join(s.logs, "dexrun")) + `\necho {\\"id\\":\\"0123abcd\\"}\n' > "$out" ;;
*) printf '#!/bin/sh\necho "$*" >> ` + shellQuote(filepath.Join(s.logs, "devtoken")) + `\n' > "$out" ;;
esac
chmod +x "$out"
`,
		filepath.Join(standIns, "bun"):        record("ui", "$ORK_SERVER") + stay,
		filepath.Join(s.root, "bin", "ork"):   record("ork", "$*") + stay,
		filepath.Join(s.root, "bin", "orker"): stay,
		filepath.Join(s.root, "bin", "orkctl"): record("orkctl-token", "$ORK_TOKEN_FILE") + record("orkctl", "$*") +
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
	return s.run("up", env...)
}

// run runs `scripts/stack.sh COMMAND` as up does.
func (s *stackRepo) run(command string, env ...string) (int, string) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(s.root, "scripts", "stack.sh"), command)
	cmd.Env = append([]string{"PATH=" + s.path, "HOME=" + s.logs}, env...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	}
	s.t.Fatalf("run scripts/stack.sh %s: %v\n%s", command, err, out)
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
		fields := strings.Fields(string(raw))
		if len(fields) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
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

// TestDevCompose_ObservabilityPublishesTwoPorts: the observability profile
// publishes the Collector's OTLP/http and Grafana alone, and Grafana's
// anonymous visitor is a Viewer on localhost; the backends' ports are
// published only by the override the proofs add, on loopback.
func TestDevCompose_ObservabilityPublishesTwoPorts(t *testing.T) {
	t.Parallel()
	doc := parseCompose(t, devComposePath, readRepoFile(t, devComposePath))
	published := map[string][]string{}
	for _, name := range doc.serviceNames() {
		svc := doc.Services[name]
		if slices.Contains(svc.Profiles, "observability") && len(svc.Ports) > 0 {
			published[name] = svc.Ports
		}
	}
	want := map[string][]string{"otel-collector": {"127.0.0.1:4318:4318"}, "grafana": {"127.0.0.1:3000:3000"}}
	if len(published) != len(want) {
		t.Errorf("the observability profile publishes %v; want only %v", published, want)
	}
	for name, ports := range want {
		if !slices.Equal(published[name], ports) {
			t.Errorf("%s publishes %v; want %v", name, published[name], ports)
		}
	}
	env := doc.Services["grafana"].Environment
	for key, value := range map[string]string{
		"GF_AUTH_ANONYMOUS_ORG_ROLE": "Viewer", "GF_SERVER_DOMAIN": "localhost", "GF_SERVER_ENFORCE_DOMAIN": "true",
	} {
		if env[key] != value {
			t.Errorf("grafana %s = %q; want %q", key, env[key], value)
		}
	}

	const backends = "deploy/observability/compose.backends.yaml"
	override := parseCompose(t, backends, readRepoFile(t, backends))
	for _, name := range override.serviceNames() {
		if !slices.Contains([]string{"loki", "tempo", "prometheus"}, name) {
			t.Errorf("%s names %s; it publishes the backends alone", backends, name)
		}
		for _, port := range override.Services[name].Ports {
			if !strings.HasPrefix(port, "127.0.0.1:") {
				t.Errorf("%s: %s publishes %q off loopback", backends, name, port)
			}
		}
	}
}
