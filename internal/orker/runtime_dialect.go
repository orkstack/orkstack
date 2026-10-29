// This file and runtime_docker.go are the runtime files: the only places in
// the repository that may name a Docker or Podman concept (AGENTS.md §5).
package orker

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// CLIVar names the environment variable that says which command-line grammar
// the runtime's CLI speaks: "docker" (the docker CLI, against Docker or
// against Podman's Docker-compatible socket) or "podman" (the podman CLI).
// It is read where ORK_DOCKER_BIN is, so the Orker, `orkctl activity test`
// and `orkctl task run` all speak the CLI they were pointed at.
const CLIVar = "ORK_DOCKER_CLI"

// The CLIs and engines a DockerRuntime drives. Exported so internal/config's
// Schema can bind ORK_DOCKER_CLI's documented default to the same constant
// the runtime falls back to.
const (
	CLIDocker        = "docker"
	CLIPodman        = "podman"
	DefaultDockerCLI = CLIDocker
)

// SeccompEngineDefault is how a runtime reports the seccomp profile it did
// not spell because its engine has no name for the default: the engine
// applies its own default profile, and the boot probe checks that a filter is
// active (Seccomp: 2).
const SeccompEngineDefault = "engine-default"

// dialect is the spellings that differ between the CLIs and engines a
// DockerRuntime drives, on two separate axes. The CLI decides the flag
// grammar: --http-proxy and --userns exist only on the podman CLI. The engine
// decides what the daemon understands: `seccomp=builtin` is a docker-CLI
// keyword that the Podman engine reads as a file path, whichever CLI sends
// it. It is a value, not an interface: the argument vector stays one pure
// function of a spec and a dialect.
type dialect struct {
	cli    string
	engine string
	// rootless is the engine's own answer (rootlessQuery), asked with the
	// engine: whether the escape hatch needs a user namespace that keeps the
	// Orker's uid is a fact to observe, not to infer from the Orker's uid.
	rootless bool
}

// dockerDialect is the docker CLI against Docker, the one shape that shipped
// before the dialect existed.
var dockerDialect = dialect{cli: CLIDocker, engine: CLIDocker}

// parseCLI reads a CLI setting. Empty is the default; anything else that is
// not a known CLI is refused by name rather than guessed at.
func parseCLI(raw string) (string, error) {
	switch value := strings.ToLower(strings.TrimSpace(raw)); value {
	case "":
		return DefaultDockerCLI, nil
	case CLIDocker, CLIPodman:
		return value, nil
	default:
		return "", fmt.Errorf("%s=%q is neither %q nor %q", CLIVar, raw, CLIDocker, CLIPodman)
	}
}

// engineFromComponents names the engine behind the docker CLI from
// `version --format {{json .Server.Components}}`: Podman's Docker-compatible
// API reports a "Podman Engine" component, Docker an "Engine" one. Output
// that does not decode is an error, never a guess.
func engineFromComponents(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	var components []struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal([]byte(raw), &components); err != nil {
		return "", fmt.Errorf("read the engine's components from %q: %w", truncateString(raw, 200), err)
	}
	for _, component := range components {
		if strings.Contains(strings.ToLower(component.Name), "podman") {
			return CLIPodman, nil
		}
	}
	return CLIDocker, nil
}

// seccompFlags spells the spec's seccomp profile for this engine. Docker
// takes `builtin` by name. The Podman engine has no name for its default and
// refuses `builtin` as a missing file, so the default is spelled by saying
// nothing, and no other profile can be spelled at all.
func (x dialect) seccompFlags(profile string) ([]string, error) {
	if x.engine != CLIPodman {
		return []string{"--security-opt", "seccomp=" + profile}, nil
	}
	if profile != SeccompBuiltin {
		return nil, fmt.Errorf("create container: seccomp profile %q has no spelling on the Podman engine, "+
			"which only applies its own default", profile)
	}
	return nil, nil
}

// seccompName is what seccompFlags spells for the default profile, for the
// security profile an Orker reports.
func (x dialect) seccompName() string {
	if x.engine == CLIPodman {
		return SeccompEngineDefault
	}
	return SeccompBuiltin
}

// cliFlags are the flags this CLI needs so that a container gets nothing the
// spec did not ask for. The podman CLI forwards its own process's proxy
// variables — credentials included — into every container unless told not
// to; the docker CLI does not.
func (x dialect) cliFlags() []string {
	if x.cli == CLIPodman {
		return []string{"--http-proxy=false"}
	}
	return nil
}

// readOnlyTmpfsFlags make /run and /var/tmp read-only on an engine whose
// --read-only leaves them writable. The Podman engine's does, behind either
// CLI, and its containers.conf setting for it had no effect; Docker's covers
// both, and gets nothing here, because a mount target follows the image's
// symlinks: on an image whose /var/tmp links to /tmp, the tmpfs makes /tmp
// read-only. On Podman that is the price of a read-only /run and /var/tmp,
// which the boot probe refuses to start without.
func (x dialect) readOnlyTmpfsFlags() []string {
	if x.engine != CLIPodman {
		return nil
	}
	var flags []string
	for _, path := range readOnlyRootfsPaths {
		flags = append(flags, "--tmpfs", path+":ro,size=1m")
	}
	return flags
}

// runAsOrkerFlags render ContainerSpec.RunAsOrker: the Orker's own uid and
// gid. A rootless podman CLI maps that uid to a subordinate uid on the host
// unless the container's user namespace keeps it (--userns=keep-id), measured
// to land as the Orker. Rootful Podman maps nothing, so the uid is the
// host's already. The docker CLI has no way to say keep-id; behind a rootless
// engine its hatch lands elsewhere, which the boot probe finds.
func (x dialect) runAsOrkerFlags() []string {
	user := []string{"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())}
	if x.cli == CLIPodman && x.rootless {
		return append([]string{"--userns=keep-id"}, user...)
	}
	return user
}

// rootlessQuery is the `info` template that answers "is the engine rootless"
// on this CLI, and isRootless reads its answer. The docker CLI reports it in
// the security options (against Docker and against Podman's socket alike);
// the podman CLI has no SecurityOptions field, and the template error was
// once swallowed into "not rootless".
func (x dialect) rootlessQuery() string {
	if x.cli == CLIPodman {
		return "{{.Host.Security.Rootless}}"
	}
	return "{{.SecurityOptions}}"
}

func (x dialect) isRootless(answer string) (bool, error) {
	answer = strings.TrimSpace(answer)
	if x.cli != CLIPodman {
		return strings.Contains(answer, "name=rootless"), nil
	}
	switch answer {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("the podman CLI answered %q to %s, not true or false", truncateString(answer, 200), x.rootlessQuery())
	}
}

// The runtime's own words, matched case-insensitively: the CLIs and engines
// spell the same refusal differently, and one spelling's capital letter is
// not a different fact.
var (
	// unbindablePhrases are a bind source the daemon cannot bind. The last
	// is Docker's when it cannot see the Orker's directory and tries to make
	// it on its own side.
	unbindablePhrases = []string{
		"bind source path does not exist",
		"mounts denied",
		"is not shared from the host",
		"error while creating mount source path",
	}
	// notRunningPhrases are a signal to a container that already stopped,
	// which is the normal race between the grace timer and a well-behaved
	// workload: Docker's, then Podman's (its CLI and its socket alike).
	notRunningPhrases = []string{
		"is not running",
		"can only kill running containers",
		"container state improper",
	}
	// goneSubstring is a container that no longer exists, which both
	// engines say in these words ("no container with name or ID … found: no
	// such container" on Podman).
	goneSubstring = "no such container"
)

func containsFold(s, substring string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substring))
}

func containsAnyFold(s string, substrings []string) bool {
	for _, substring := range substrings {
		if containsFold(s, substring) {
			return true
		}
	}
	return false
}

// unbindable recognizes this dialect's words for a bind source the daemon
// cannot bind. It separates WORKSPACE_NOT_BINDABLE (a host misconfiguration
// retrying will not fix) from RUNTIME_REFUSED (which is retryable). The
// podman CLI checks a bind source with statfs at create and says so; its
// Docker-compatible socket creates a missing source instead, which the boot
// probe's nonce finds.
func (x dialect) unbindable(message string) bool {
	if containsAnyFold(message, unbindablePhrases) {
		return true
	}
	return x.cli == CLIPodman && containsFold(message, "statfs") && containsFold(message, "no such file or directory")
}

// notRunning recognizes a container that has already stopped, or is gone.
func (x dialect) notRunning(message string) bool {
	return containsAnyFold(message, notRunningPhrases) || x.gone(message)
}

// gone recognizes a container that no longer exists.
func (x dialect) gone(message string) bool {
	return containsFold(message, goneSubstring)
}

// Container states, named rather than inferred. `docker wait` and `podman
// wait` on a container that has not started return at once, so a state the
// gate does not know must never be read as started: it keeps the gate
// polling to its deadline instead.
var (
	// preStartStates are a container the runtime has not started yet:
	// Docker's created, Podman's configured, created and initialized, and the
	// empty state a runtime may report before it has one.
	preStartStates = map[string]bool{"": true, "created": true, "configured": true, "initialized": true}
	// startedStates are a container that has run, or is running: both
	// engines' running, paused, restarting, removing, exited and dead, and
	// Podman's stopping and stopped (a stopped container ran and exited).
	startedStates = map[string]bool{
		"running": true, "paused": true, "restarting": true, "removing": true, "exited": true, "dead": true,
		"stopping": true, "stopped": true,
	}
)
