package orker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

var (
	podmanCLI    = dialect{cli: CLIPodman, engine: CLIPodman, rootless: true}
	podmanSocket = dialect{cli: CLIDocker, engine: CLIPodman, rootless: true}
)

func renderedIn(t *testing.T, spec ContainerSpec, spoken dialect) string {
	t.Helper()
	args, err := createArgs(spec, spoken)
	if err != nil {
		t.Fatalf("createArgs(%+v): %v", spoken, err)
	}
	return strings.Join(args, " ")
}

// TestPodmanEngine_OmitsTheSeccompKeywordItReadsAsAPath is G1: the Podman
// engine, behind its CLI and its Docker-compatible socket alike, reads
// `seccomp=builtin` as a file and refuses every container. On that engine
// the default profile is spelled by saying nothing, which the engine fills
// with its own default and the boot probe checks is a filter; no other
// profile can be spelled there at all. Docker keeps `builtin` by name.
func TestPodmanEngine_OmitsTheSeccompKeywordItReadsAsAPath(t *testing.T) {
	if got := renderedIn(t, validSpec(), dockerDialect); !strings.Contains(got, "--security-opt seccomp="+SeccompBuiltin) {
		t.Errorf("docker: %s; want seccomp=%s spelled", got, SeccompBuiltin)
	}
	for name, spoken := range map[string]dialect{"podman CLI": podmanCLI, "docker CLI on the Podman socket": podmanSocket} {
		if got := renderedIn(t, validSpec(), spoken); strings.Contains(got, "seccomp=") {
			t.Errorf("%s: %s; the Podman engine reads any seccomp= value as a file", name, got)
		}
		if spoken.seccompName() != SeccompEngineDefault {
			t.Errorf("%s reports seccomp %q; want %q, what was spelled", name, spoken.seccompName(), SeccompEngineDefault)
		}
		spec := validSpec()
		spec.SeccompProfile = "/etc/custom.json"
		if _, err := createArgs(spec, spoken); err == nil {
			t.Errorf("%s accepted a seccomp profile it has no spelling for", name)
		}
	}
}

// TestPodmanCLI_KeepsTheOrkersProxyOutOfTheContainer is G22: the podman CLI
// forwards its own process's proxy variables, credentials included, into
// every container unless told not to. The docker CLI forwards none and has
// no such flag, so it is not spelled there.
func TestPodmanCLI_KeepsTheOrkersProxyOutOfTheContainer(t *testing.T) {
	if got := renderedIn(t, validSpec(), podmanCLI); !strings.Contains(got, "--http-proxy=false") {
		t.Errorf("podman CLI: %s; want --http-proxy=false", got)
	}
	for name, spoken := range map[string]dialect{"docker": dockerDialect, "docker CLI on the Podman socket": podmanSocket} {
		if got := renderedIn(t, validSpec(), spoken); strings.Contains(got, "--http-proxy") {
			t.Errorf("%s: %s; the docker CLI has no --http-proxy", name, got)
		}
	}
}

// TestPodmanCLI_RunsAsTheOrkerInAUserNamespaceThatKeepsIt is G10: under a
// rootless podman CLI the Orker's uid inside the container is a subordinate
// uid on the host unless the user namespace keeps it, measured to land as
// the Orker with --userns=keep-id. Only the escape hatch asks for it, and
// only on a rootless engine, as the engine itself says: rootful Podman maps
// nothing, so the uid is the host's already, whoever runs the CLI.
func TestPodmanCLI_RunsAsTheOrkerInAUserNamespaceThatKeepsIt(t *testing.T) {
	spec := validSpec()
	spec.RunAsOrker = true
	user := fmt.Sprintf("--user %d:%d", os.Getuid(), os.Getgid())
	got := renderedIn(t, spec, podmanCLI)
	if !strings.Contains(got, user) || !strings.Contains(got, "--userns=keep-id") {
		t.Fatalf("rootless podman CLI: %s; want --userns=keep-id %s", got, user)
	}
	rootful := podmanCLI
	rootful.rootless = false
	if got := renderedIn(t, spec, rootful); strings.Contains(got, "--userns") || !strings.Contains(got, user) {
		t.Errorf("rootful podman CLI: %s; want %s and no --userns", got, user)
	}
	if got := renderedIn(t, spec, podmanSocket); strings.Contains(got, "--userns") {
		t.Errorf("docker CLI on the Podman socket: %s; the docker CLI cannot say keep-id", got)
	}
	if got := renderedIn(t, validSpec(), podmanCLI); strings.Contains(got, "--userns") || strings.Contains(got, "--user ") {
		t.Errorf("podman CLI without the hatch: %s; the image's own user runs", got)
	}
	if got := renderedIn(t, spec, dockerDialect); strings.Contains(got, "--userns") || !strings.Contains(got, user) {
		t.Errorf("docker: %s; want %s and no --userns", got, user)
	}
}

// TestPodmanEngine_GetsReadOnlyTmpfsWhereItsReadOnlyRootLeavesThemWritable is
// G2: the Podman engine's --read-only leaves /run and /var/tmp writable, so
// it gets an empty read-only tmpfs over each, behind either CLI. Docker's
// covers both, and a tmpfs there would follow an image's symlink.
func TestPodmanEngine_GetsReadOnlyTmpfsWhereItsReadOnlyRootLeavesThemWritable(t *testing.T) {
	for name, spoken := range map[string]dialect{"podman CLI": podmanCLI, "docker CLI on the Podman socket": podmanSocket} {
		got := renderedIn(t, validSpec(), spoken)
		for _, want := range []string{"--tmpfs /run:ro,size=1m", "--tmpfs /var/tmp:ro,size=1m", "--tmpfs /tmp:rw,nosuid,nodev,noexec"} {
			if !strings.Contains(got, want) {
				t.Errorf("%s: %s; want %q", name, got, want)
			}
		}
	}
	if got := renderedIn(t, validSpec(), dockerDialect); strings.Contains(got, "/run:") || strings.Contains(got, "/var/tmp:") {
		t.Errorf("docker: %s; want no tmpfs at /run or /var/tmp", got)
	}
}

// TestDialect_ReadsEachEnginesRefusalsWhateverTheirCase is G13: the CLIs and
// engines spell one refusal differently, and a capital letter is not a
// different fact. The podman CLI's statfs refusal of a missing bind source
// is an unbindable path; the same words from Docker are not claimed as one.
func TestDialect_ReadsEachEnginesRefusalsWhateverTheirCase(t *testing.T) {
	statfs := "Error: statfs /work/attempt/input: no such file or directory"
	if !podmanCLI.unbindable(statfs) {
		t.Errorf("podman CLI: unbindable(%q) = false; it is a bind source the engine cannot see", statfs)
	}
	if dockerDialect.unbindable(statfs) {
		t.Errorf("docker: unbindable(%q) = true; that is not Docker's refusal", statfs)
	}
	if !dockerDialect.unbindable("DOCKER: ERROR RESPONSE FROM DAEMON: BIND SOURCE PATH DOES NOT EXIST") {
		t.Error("docker: an upper-case refusal of a bind is not read as one")
	}
	for _, said := range []string{
		"Error response from daemon: container abc is not running",
		"Error: can only kill running containers. abc is in state exited: container state improper",
		`Error response from daemon: no container with name or ID "abc" found: no such container`,
		"Error: No such container: abc",
	} {
		for name, spoken := range map[string]dialect{"docker": dockerDialect, "podman CLI": podmanCLI, "podman socket": podmanSocket} {
			if !spoken.notRunning(said) {
				t.Errorf("%s: notRunning(%q) = false", name, said)
			}
		}
	}
	if !podmanCLI.gone(`Error: no container with name or ID "abc" found: no such container`) {
		t.Error("podman CLI: a missing container is not read as gone")
	}
	if dockerDialect.notRunning("Error response from daemon: permission denied") {
		t.Error("an ordinary refusal is read as a container that stopped")
	}
}

// TestDockerRuntime_SignalOfAStoppedPodmanContainerIsNotAFailure is G13 on
// the call it decides: Podman answers a kill of an exited container with
// "can only kill running containers", the normal race with a workload that
// already exited, and Signal reports no failure for it.
func TestDockerRuntime_SignalOfAStoppedPodmanContainerIsNotAFailure(t *testing.T) {
	fake := newCountingDocker(t)
	script := fake.bin + ".podman"
	body := "#!/bin/sh\necho \"$*\" >> " + shellQuote(fake.dir+"/calls") + "\n" +
		"echo 'Error: can only kill running containers. c1 is in state exited: container state improper' >&2\nexit 125\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	runtime := &DockerRuntime{Bin: script, CLI: CLIPodman}
	if err := runtime.Signal(context.Background(), "c1", SIGTERM); err != nil {
		t.Fatalf("Signal = %v; a stopped container is not a failure", err)
	}
}

// TestDockerRuntime_AsksTheEngineBehindTheDockerCLI is G1's detection and
// G17: the docker CLI drives whichever engine answers its socket, and says
// which in `version`; Describe reports the ENGINE, and the seccomp profile
// it spells. The podman CLI only drives Podman and is not asked.
func TestDockerRuntime_AsksTheEngineBehindTheDockerCLI(t *testing.T) {
	tests := []struct {
		name, cli, flag         string
		wantEngine, wantSeccomp string
		wantAsks                int
	}{
		{name: "docker CLI on Docker", cli: CLIDocker, wantEngine: CLIDocker, wantSeccomp: SeccompBuiltin, wantAsks: 1},
		{name: "docker CLI on the Podman socket", cli: CLIDocker, flag: "podmanengine", wantEngine: CLIPodman, wantSeccomp: SeccompEngineDefault, wantAsks: 1},
		{name: "podman CLI", cli: CLIPodman, wantEngine: CLIPodman, wantSeccomp: SeccompEngineDefault, wantAsks: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newCountingDocker(t)
			if tt.flag != "" {
				fake.set(tt.flag, "")
			}
			runtime := &DockerRuntime{Bin: fake.bin, CLI: tt.cli}
			spoken, err := runtime.dialect(context.Background())
			if err != nil || spoken.engine != tt.wantEngine || spoken.cli != tt.cli {
				t.Fatalf("dialect = %+v, %v; want engine %s on CLI %s", spoken, err, tt.wantEngine, tt.cli)
			}
			if got := countPrefix(fake.calls(), "version --format {{json .Server.Components}}"); got != tt.wantAsks {
				t.Errorf("asked the engine %d times; want %d", got, tt.wantAsks)
			}
			info, err := runtime.Describe(context.Background())
			if err != nil || info.Name != tt.wantEngine || info.CLI != tt.cli || info.Seccomp != tt.wantSeccomp {
				t.Fatalf("Describe = %+v, %v; want engine %s, CLI %s, seccomp %s", info, err, tt.wantEngine, tt.cli, tt.wantSeccomp)
			}
			args, err := createArgs(validSpec(), spoken)
			if err != nil {
				t.Fatal(err)
			}
			if spelled := strings.Contains(strings.Join(args, " "), "seccomp="+SeccompBuiltin); spelled != (tt.wantSeccomp == SeccompBuiltin) {
				t.Errorf("seccomp=%s spelled %t; Describe says %s", SeccompBuiltin, spelled, tt.wantSeccomp)
			}
		})
	}

	if _, err := engineFromComponents("Cannot connect to the Docker daemon"); err == nil {
		t.Error("an answer that is not the engine's components was read as an engine")
	}
}

// TestDockerRuntime_ReadsRootlessFromTheCLIsOwnField is G8: the podman CLI has
// no SecurityOptions field, and its template error was swallowed into "not
// rootless". It is asked its own field; and on any CLI a detection that
// failed is an error, never "not rootless".
func TestDockerRuntime_ReadsRootlessFromTheCLIsOwnField(t *testing.T) {
	fake := newCountingDocker(t)
	fake.set("rootless", "true\n")
	runtime := &DockerRuntime{Bin: fake.bin, CLI: CLIPodman}
	info, err := runtime.Describe(context.Background())
	if err != nil || !info.Rootless {
		t.Fatalf("Describe = %+v, %v; want rootless", info, err)
	}
	if calls := fake.calls(); countPrefix(calls, "info --format {{.Host.Security.Rootless}}") != 1 {
		t.Errorf("calls = %q; want the podman CLI asked {{.Host.Security.Rootless}} once", calls)
	}
	if spoken, err := runtime.dialect(context.Background()); err != nil || !spoken.rootless {
		t.Errorf("dialect = %+v, %v; want the rootless answer kept with the engine, which decides keep-id", spoken, err)
	}

	for name, setup := range map[string]func(*countingDocker){
		"an info that fails":                  func(f *countingDocker) { f.set("infofail", "") },
		"an answer that is not true or false": func(f *countingDocker) { f.set("rootless", "<no value>\n") },
	} {
		t.Run(name, func(t *testing.T) {
			fake := newCountingDocker(t)
			setup(fake)
			if info, err := (&DockerRuntime{Bin: fake.bin, CLI: CLIPodman}).Describe(context.Background()); err == nil {
				t.Fatalf("Describe = %+v, nil; a rootless detection that failed must be an error", info)
			}
		})
	}
	fake = newCountingDocker(t)
	fake.set("infofail", "")
	if _, err := (&DockerRuntime{Bin: fake.bin}).Describe(context.Background()); err == nil {
		t.Error("the docker CLI's failed detection was read as not rootless")
	}
}

// TestDockerRuntime_TheCLISettingDecidesTheGrammarAndTheDefaultBinary: the
// field over ORK_DOCKER_CLI over the default, the podman CLI's binary is
// `podman` on PATH unless ORK_DOCKER_BIN says otherwise, and a value that
// names no CLI fails every command, naming the setting.
func TestDockerRuntime_TheCLISettingDecidesTheGrammarAndTheDefaultBinary(t *testing.T) {
	t.Setenv(DockerBinVar, "")
	t.Setenv(CLIVar, "podman")
	if cli, err := (&DockerRuntime{}).cli(); err != nil || cli != CLIPodman {
		t.Errorf("cli = %q, %v; want %s from %s", cli, err, CLIPodman, CLIVar)
	}
	if cli, err := (&DockerRuntime{CLI: "docker"}).cli(); err != nil || cli != CLIDocker {
		t.Errorf("cli = %q, %v; want the field over the environment", cli, err)
	}
	t.Setenv("PATH", t.TempDir())
	if _, err := (&DockerRuntime{}).bin(); err == nil || !strings.Contains(err.Error(), `"podman"`) {
		t.Errorf("bin with the podman CLI and nothing on PATH = %v; want it to look for podman", err)
	}
	t.Setenv(CLIVar, "nerdctl")
	_, err := (&DockerRuntime{Bin: "/bin/true"}).bin()
	if err == nil || !strings.Contains(err.Error(), CLIVar) {
		t.Errorf("bin with %s=nerdctl = %v; want a refusal naming the setting", CLIVar, err)
	}
}

// TestStartGate_NamesTheStatesAndWaitsOnOneItDoesNotKnow is G25: `wait` on a
// container that has not started returns at once on both engines, so the
// gate reads started only from a state named as started. Podman's
// configured and initialized are pre-start, its stopped is a container that
// ran, and a state on neither list keeps the gate polling rather than being
// read as started.
func TestStartGate_NamesTheStatesAndWaitsOnOneItDoesNotKnow(t *testing.T) {
	tests := []struct {
		state   string
		started bool
	}{
		{"created", false}, {"configured", false}, {"initialized", false}, {"", false},
		{"running", true}, {"exited", true}, {"stopped", true}, {"paused", true}, {"dead", true},
		{"hibernating", false},
	}
	for _, tt := range tests {
		fake := newCountingDocker(t)
		fake.set("state", tt.state+"\n")
		started, err := (&DockerRuntime{Bin: fake.bin}).leftCreated(context.Background(), "c1")
		if err != nil || started != tt.started {
			t.Errorf("leftCreated with state %q = %t, %v; want %t", tt.state, started, err, tt.started)
		}
	}
}

// TestDockerRuntime_TheProbeSpeaksTheEnginesSeccomp: the boot probe renders
// its container in the runtime's dialect, so on the Podman engine it runs
// without the keyword that engine refuses, and on Docker with it.
func TestDockerRuntime_TheProbeSpeaksTheEnginesSeccomp(t *testing.T) {
	for _, podman := range []bool{false, true} {
		fake := newCountingDocker(t)
		fake.set("present", "")
		if podman {
			fake.set("podmanengine", "")
		}
		if _, err := (&DockerRuntime{Bin: fake.bin}).RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir())); err != nil {
			t.Fatalf("RunBootProbe (podman engine %t) = %v", podman, err)
		}
		var run string
		for _, call := range fake.calls() {
			if strings.HasPrefix(call, "run ") {
				run = call
			}
		}
		if spelled := strings.Contains(run, "seccomp="); spelled == podman {
			t.Errorf("podman engine %t: seccomp spelled %t in %q", podman, spelled, run)
		}
	}
	// A daemon that cannot be reached is reported in its own words; only the
	// podman CLI's template error, which is the podman-docker shim answering
	// a docker query, is told to set the CLI.
	fake := newCountingDocker(t)
	fake.set("versionfail", "")
	_, err := (&DockerRuntime{Bin: fake.bin}).RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir()))
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "Cannot connect") || strings.Contains(err.Error(), CLIVar) {
		t.Errorf("RunBootProbe with an unreachable daemon = %v; want ErrPostureRefused quoting the daemon and no CLI hint", err)
	}
	shim := fake.bin + ".shim"
	body := "#!/bin/sh\necho \"Error: template: version:1:7: executing at <.Components>: can't evaluate field Components in type *define.Version\" >&2\nexit 125\n"
	if err := os.WriteFile(shim, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = (&DockerRuntime{Bin: shim}).RunBootProbe(context.Background(), probeSpecUnder(t, t.TempDir()))
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), CLIVar+"="+CLIPodman) {
		t.Errorf("RunBootProbe behind the podman-docker shim = %v; want ErrPostureRefused telling to set %s", err, CLIVar)
	}
}
