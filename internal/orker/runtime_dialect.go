// This file and runtime_docker.go are the runtime files: the only places in
// the repository that may name a Docker or Podman concept (AGENTS.md §5).
package orker

import (
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
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

// stateQuery is the `info` template that names the directories the engine
// keeps its state in — images, layers, every container's writable layer and
// its log — wherever the engine's own settings put them, and statePaths
// reads its answer. The docker CLI reports DockerRootDir: dockerd's
// data-root, and Podman's graphroot behind its Docker-compatible socket. The
// podman CLI reports its graphroot and its runroot, one a line. Neither
// reports containerd's own root.
func (x dialect) stateQuery() string {
	if x.cli == CLIPodman {
		return "{{.Store.GraphRoot}}\n{{.Store.RunRoot}}"
	}
	return "{{.DockerRootDir}}"
}

// stateSettings names the engine settings that move what stateQuery
// reports, for an operator told to check them.
func (x dialect) stateSettings() string {
	if x.engine == CLIPodman {
		return "graphroot and runroot in the engine's storage.conf"
	}
	return "the daemon's data-root"
}

// statePaths reads stateQuery's answer, one directory a line. dirs are the
// lines that name a directory the Orker can hold as its own; refused is every
// other line that is not blank: a field the engine leaves unset
// ("<no value>"), a relative path, one holding a control character, and the
// root of the file system, which would make every file the Orker's own.
func statePaths(answer string) (dirs, refused []string) {
	for _, line := range strings.Split(answer, "\n") {
		switch line = strings.TrimSpace(line); {
		case line == "":
		case filepath.IsAbs(line) && filepath.Clean(line) != "/" && !strings.ContainsFunc(line, unicode.IsControl):
			dirs = append(dirs, line)
		default:
			refused = append(refused, line)
		}
	}
	return dirs, refused
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
	// inspectGoneSubstring is Docker's word for a container its untyped
	// `inspect` cannot find ("Error: No such object: …").
	inspectGoneSubstring = "no such object"
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

// unreachable recognizes the CLI's own word that no engine answered it, as
// against an answer the engine gave.
func (x dialect) unreachable(message string) bool {
	return containsAnyFold(message, unreachablePhrases)
}

// unreachablePhrases are the docker and podman CLIs' words that no engine
// answered them: their own, so never a path, an image or a registry's.
var unreachablePhrases = []string{
	"cannot connect to the docker daemon",
	"while trying to connect to the docker daemon socket",
	"while trying to connect to the docker api",
	"error during connect:",
	"cannot connect to podman",
	"unable to connect to podman",
}

// gone recognizes a container that no longer exists.
func (x dialect) gone(message string) bool {
	return containsFold(message, goneSubstring)
}

// inspectGone recognizes an inspect that found no such container: an answer
// about the container, never a daemon that could not be asked.
func (x dialect) inspectGone(message string) bool {
	return x.gone(message) || containsFold(message, inspectGoneSubstring)
}

// eventArgs is the command that follows one container's events, one a line: the
// word, and the docker CLI's signal number after a kill. since is an instant on
// the engine's own clock (createdAnchor), which covers a container that ran before
// the stream connected and keeps the engine from scanning its whole event log. A
// relative time would not do: each CLI works it out on the Orker's clock, and an
// engine whose clock is behind by more answers a time in its future with nothing.
func (x dialect) eventArgs(id, since string) []string {
	format := `{{.Action}} {{index .Actor.Attributes "signal"}}`
	if x.cli == CLIPodman {
		format = "{{.Status}}"
	}
	return []string{"events", "--since", since, "--filter", "container=" + id, "--format", format}
}

// stoppedByUserQuery is the inspect template that reads the engine's own record
// of a stop or kill asked of it, where the CLI has one, and empty where it has
// none. libpod sets State.StoppedByUser in every stop and every kill before the
// container ends, and the podman CLI's inspect answers it. Its event stream
// cannot stand in: libpod writes a stop's `stop` only if the container is still
// stopping once its process is gone, which it almost never is (conmon has
// reaped it and written its exit file by then, Podman 5.7.0 to 5.8.9), so a stop
// says just what a container that exits by itself says. The docker CLI's
// inspect, against Docker or Podman's socket, reads Docker's state, which has
// no such field.
func (x dialect) stoppedByUserQuery() string {
	if x.cli == CLIPodman {
		return "{{.State.StoppedByUser}}"
	}
	return ""
}

// endTellsNothing reports that an end with no stop or kill heard says nothing
// about why the container ended: the docker CLI against Podman's
// Docker-compatible socket. There the stream may or may not say `stop` (Podman
// 5.8.9 was measured to say it nearly every time; see stoppedByUserQuery for
// why libpod may not), and the CLI's inspect has no StoppedByUser to ask, so a
// stop that was not said reads word for word as an exit. Its `kill` comes
// before the end and is heard. Against Docker the stream says `stop` before
// `die`, and the podman CLI asks StoppedByUser.
func (x dialect) endTellsNothing() bool {
	return x.cli == CLIDocker && x.engine == CLIPodman
}

// stopSeen is what one container's event stream has said (stopWatch).
type stopSeen struct {
	// stopped: the engine was asked to stop it, or sent it a signal that ends a
	// process, before it ended.
	stopped bool
	// ended: the engine said it ended.
	ended bool
}

// see takes one line of the stream and reports whether the question is settled.
// The docker CLI says kill, stop, die, against Docker and against Podman's
// Docker-compatible socket alike; the podman CLI says kill, stop, died. A `stop`
// is what a daemon's shutdown and an operator's stop say, whichever stop signal
// the image chose. A `kill` that names a signal counts only for TERM and KILL,
// because an operator's USR1 to a workload that handles it stops nothing. Podman
// names none, so there every kill counts. Every other word is not about how it
// ended (exec_die is an exec's), and nothing after the end is why it ended.
func (s *stopSeen) see(line string) (settled bool) {
	if s.ended || s.stopped {
		return true
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false
	}
	switch fields[0] {
	case "stop":
		s.stopped = true
	case "kill":
		s.stopped = len(fields) == 1 || fields[1] == "15" || fields[1] == "9"
	case "die", "died":
		s.ended = true
	}
	return s.ended || s.stopped
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

// cgroupLeaf reports whether seg, under parent, is the cgroup a container
// with this id runs in, as the engine names it: docker-<id>.scope (systemd
// driver) or /docker/<id> (cgroupfs) for Docker, libpod-<id>.scope (systemd)
// or libpod-<id> (cgroupfs) for Podman, or the "container" child of either.
// Only the exact leaf: a parent, any other child or a longer id is not it
// (internal/orker/usage_cgroup.go).
func (x dialect) cgroupLeaf(id, parent, seg string) bool {
	if id == "" {
		return false
	}
	if x.engine == CLIPodman {
		// The systemd driver may hold the processes in the scope's
		// "container" child, which is where cgroup.procs lists them.
		return seg == "libpod-"+id+".scope" || seg == "libpod-"+id ||
			(seg == "container" && (parent == "libpod-"+id+".scope" || parent == "libpod-"+id))
	}
	return seg == "docker-"+id+".scope" || (parent == "docker" && seg == id)
}

// answerTmpfsOptions spells a service's answer filesystem for this CLI. The
// docker CLI takes the inode count and the owner as mount options. The podman
// CLI refuses both as unknown options: it gives the mount to the container's
// own user with U (the Orker's uid, which the container runs as), and states
// no inode count, so there the container's memory limit, to which the kernel
// charges a tmpfs's inodes, is what bounds them (docs/DEPLOY.md, "Source
// hosts").
func (x dialect) answerTmpfsOptions(t ServiceTmpfs) string {
	if x.cli == CLIPodman {
		return fmt.Sprintf("rw,nosuid,nodev,noexec,size=%d,mode=0700,U", t.Bytes)
	}
	return fmt.Sprintf("rw,nosuid,nodev,noexec,size=%d,nr_inodes=%d,mode=0700,uid=%d,gid=%d", t.Bytes, t.Inodes, t.UID, t.GID)
}

// runtimeQuery is the `info` template that answers, as one JSON object, which
// OCI runtime the engine runs containers with by default and where its binary
// is: the docker CLI reports the default's name and the registered runtimes'
// paths, the podman CLI its OCI runtime's name and path. The runtime is the
// one a CDI spec's hooks run under (design §4.10 item 2).
func (x dialect) runtimeQuery() string {
	if x.cli == CLIPodman {
		return "{{json .Host.OCIRuntime}}"
	}
	return `{"name":{{json .DefaultRuntime}},"runtimes":{{json .Runtimes}}}`
}

// runtimeFacts is the engine's default OCI runtime as it reports it.
type runtimeFacts struct {
	Name string
	Path string
}

// parseRuntimeFacts reads runtimeQuery's answer. An answer that is not what
// the query asks for, or that names no path for the default runtime, is an
// error: the rule below is held to a path, never to a name alone.
func (x dialect) parseRuntimeFacts(answer string) (runtimeFacts, error) {
	var raw struct {
		Name     string `json:"name"`
		Path     string `json:"path"`
		Runtimes map[string]struct {
			Path string `json:"path"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(answer)), &raw); err != nil {
		return runtimeFacts{}, fmt.Errorf("the container engine answered %q to %s: %w", truncateString(strings.TrimSpace(answer), 200), x.runtimeQuery(), err)
	}
	facts := runtimeFacts{Name: strings.TrimSpace(raw.Name), Path: strings.TrimSpace(raw.Path)}
	if x.cli != CLIPodman {
		facts.Path = strings.TrimSpace(raw.Runtimes[facts.Name].Path)
	}
	return facts, nil
}

// checkDefaultRuntime holds the engine's default runtime to runc or crun, by
// name and by the binary the name points at: a wrapper such as the NVIDIA
// runtime registered under the name runc, or a sandboxed runtime, would run a
// CDI spec's hooks and devices under rules this audit never read.
func checkDefaultRuntime(f runtimeFacts) error {
	switch f.Name {
	case "runc", "crun":
	default:
		return fmt.Errorf("the container engine's default runtime is %q; a profile needs runc or crun", truncateString(f.Name, 200))
	}
	switch filepath.Base(f.Path) {
	case "runc", "crun":
		return nil
	}
	return fmt.Errorf("the container engine's default runtime %q is the binary %q, not runc or crun; a profile needs runc or crun (its path is reported, and is held to the name)",
		f.Name, truncateString(f.Path, 200))
}

// hostAlias is the name this engine gives its containers for the host, ""
// for none. Podman resolves host.containers.internal to an address that
// leads to the host; a rootless Podman copies the host's own address into
// the container's namespace, where it leads nowhere, so this name is the one
// way to the host there.
func (x dialect) hostAlias() string {
	if x.engine == CLIPodman {
		return "host.containers.internal"
	}
	return ""
}
