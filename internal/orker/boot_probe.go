package orker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/quantity"
)

// The boot probe (§3.1, §9.2).
//
// Before an Orker accepts work it runs one container built exactly as a
// task's container is — the same spec builder, the same mounts, the same
// rendering — and checks, from inside it and from the host, what the runtime
// actually gave it. The probe used to run a weaker argv than a task (no
// seccomp option, no --init, no limits, no tmpfs), so a runtime that refused
// or quietly weakened the §9.2 posture was found by every task and never at
// boot, and a bind the daemon satisfied with a directory of its own (Podman's
// Docker-compatible socket creates a missing source) passed it.
//
// What it judges is Linux, not a runtime: the runtime only runs the container
// and hands back its stdout (RunBootProbe), and everything below reads what
// the kernel reported.

// bootProber is the runtime capability Boot probes with. A runtime without it
// is not probed.
type bootProber interface {
	// RunBootProbe runs spec's container once, to its end, and returns its
	// stdout. Its errors match ErrProbeImageUnavailable, ErrProbeTimedOut,
	// ErrNotBindable or ErrPostureRefused, or are the context's own.
	RunBootProbe(ctx context.Context, spec ContainerSpec) (string, error)
	// StartBootProbe starts spec's container detached, removed by the daemon
	// when it exits, and returns its name. Its errors are RunBootProbe's.
	StartBootProbe(ctx context.Context, spec ContainerSpec) (ContainerID, error)
	// bindProbeTimeout is the probe's budget for each step that waits on the
	// daemon (ORK_DOCKER_BIND_PROBE_TIMEOUT).
	bindProbeTimeout() time.Duration
}

const (
	// bootProbeTaskID names the probe's container (ork-bindprobe-…), the
	// name an operator looks for when a probe container is left behind.
	bootProbeTaskID = "bindprobe"
	// bootProbeNonceFile is written by the Orker into input/ and by the
	// container into output/, each with a nonce the other side reads back.
	bootProbeNonceFile = "probe-nonce"
	// bootProbeAsOrkerFile is the 0600 file the container writes, as the
	// Orker's user, for the Orker to read back.
	bootProbeAsOrkerFile = "probe-as-orker"
	// signalProbeTaskID names the SIGTERM probe's container (ork-sigprobe-…).
	signalProbeTaskID = "sigprobe"
	// signalProbeReadyFile is written into output/ by the SIGTERM probe's
	// workload once its trap is set, with what it runs under.
	signalProbeReadyFile = "probe-ready"
	// signalProbeHeardFile is written into output/ by its trap.
	signalProbeHeardFile = "probe-heard-term"
)

// bootProbeScript is what the probe's container runs, as `sh -c`, with the
// container's own nonce as $1. It prints sections, each opened by a line
// "@@name", and judges nothing: every judgement is made on the host, where
// it can name exactly what was weaker. It always exits 0, so a non-zero exit
// can only be the runtime's own. What it could not read or write is printed
// in the section it concerns, not lost on stderr: a container denied its own
// bind (an SELinux label it was not given) is told apart from one that was
// given another directory. Environment variables are printed by NAME only: a
// proxy variable can carry credentials, and this output can end up in a boot
// refusal.
const bootProbeScript = `umask 022
echo '@@nonce'; cat /orkstack/input/` + bootProbeNonceFile + ` 2>&1; echo
echo '@@write'; { printf '%s' "$1" > /orkstack/output/` + bootProbeNonceFile + `; } 2>&1
umask 077
{ printf '%s' "$1" > /orkstack/output/` + bootProbeAsOrkerFile + `; } 2>&1
echo '@@status'; cat /proc/self/status
echo '@@mounts'; cat /proc/mounts
echo '@@env'; env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p'
echo '@@cgroup'; cat /proc/self/cgroup
cg=$(sed -n 's/^0:://p' /proc/self/cgroup)
d=/sys/fs/cgroup${cg%/}
for f in memory.max pids.max cpu.max; do echo "@@$f"; cat "$d/$f"; done
exit 0
`

// signalProbeScript is the SIGTERM probe's workload, as `sh -c`: one that
// cleans up when it is cancelled, as the contract asks a workload to. It sets
// its trap first and only then says it is ready, with what the host cannot see
// from outside: the security label it runs under and its own signal masks. The
// ready file is renamed into place, so the Orker never reads half of it. It
// waits in `wait`, which a trapped signal interrupts at once, and it ends by
// itself, exit 3, if SIGTERM never comes, so an Orker killed while it probes
// leaves no container behind: the daemon removes it when it exits.
const signalProbeScript = `trap 'echo heard > /orkstack/output/` + signalProbeHeardFile + `; exit 0' TERM
o=/orkstack/output/` + signalProbeReadyFile + `
{ echo '@@label'; cat /proc/$$/attr/current; echo; echo '@@signals'; grep '^Sig' /proc/$$/status; } > "$o.tmp" 2>&1
mv "$o.tmp" "$o"
sleep 300 & wait $!
exit 3
`

// runAsOrkerFinding is what the boot probe found about the §3.1 escape hatch:
// err is nil when a file the container wrote as the Orker's user was the
// Orker's to read.
type runAsOrkerFinding struct {
	err error
}

// runAsOrkerUnavailable is the boot probe's reason the escape hatch does not
// work on this host, or nil: it works, or no probe ran.
func (d *OCIDispatcher) runAsOrkerUnavailable() error {
	if finding := d.runAsOrker.Load(); finding != nil {
		return finding.err
	}
	return nil
}

// ProbeRuntime runs the boot probe under root and reports the first thing it
// found wrong, wrapped so errors.Is names its kind:
//
//   - ErrNotBindable: no probe directory could be made under root, the daemon
//     refused a bind in its own words, or the container did not see the
//     Orker's directory (the file the Orker wrote was not there, or the file
//     the container wrote did not reach the Orker): the daemon bound some
//     other directory of the same name;
//   - ErrPostureRefused: the runtime would not run a task's container, or ran
//     it with a weaker posture than §9.2, each weakness named, or a workload
//     cancelled as a task is never heard SIGTERM (probeSignalDelivery);
//   - ErrProbeImageUnavailable and ErrProbeTimedOut, as RunBootProbe reports
//     them.
//
// What it finds about the §3.1 escape hatch is not a failure of the probe: it
// is kept, and a task that asks for the hatch is refused by it. A runtime
// with no RunBootProbe is not probed.
func (d *OCIDispatcher) ProbeRuntime(ctx context.Context, root string) error {
	prober, ok := d.Runtime.(bootProber)
	if !ok {
		return nil
	}
	dir, err := os.MkdirTemp(root, "ork-bindprobe-*")
	if err != nil {
		return fmt.Errorf("%w: cannot create a probe directory under %s: %w", ErrNotBindable, root, err)
	}
	defer func() {
		if err := removeSealedTree(dir); err != nil {
			d.logger().Warn("boot probe: its directory could not be removed and is left on disk",
				"path", dir, "error", err)
		}
	}()
	ws, err := NewWorkspace(dir)
	if err != nil {
		return fmt.Errorf("%w: cannot build the probe's workspace under %s: %w", ErrNotBindable, root, err)
	}
	hostNonce, containerNonce := probeNonce(), probeNonce()
	if err := os.WriteFile(filepath.Join(ws.Input, bootProbeNonceFile), []byte(hostNonce), ModeInputFile); err != nil {
		return fmt.Errorf("%w: cannot write the probe's input under %s: %w", ErrNotBindable, root, err)
	}
	// Sealed as a task's input is, so the container meets the modes a
	// workload meets.
	if err := ws.Seal(); err != nil {
		return fmt.Errorf("%w: seal the probe's input under %s: %w", ErrNotBindable, root, err)
	}
	if err := ws.SealSecrets(); err != nil {
		return fmt.Errorf("%w: seal the probe's secrets under %s: %w", ErrNotBindable, root, err)
	}
	spec, err := d.bootProbeSpec(ws, containerNonce)
	if err != nil {
		return err
	}

	if err := d.judgeBootProbe(ctx, prober, root, ws, spec, hostNonce, containerNonce); err != nil {
		return err
	}
	// The SIGTERM check only once the first container has passed: it reads
	// the bind the first one proved, and starts its container on an engine
	// already identified, from an image already local.
	if err := d.probeSignalDelivery(ctx, prober, root, ws); err != nil {
		return err
	}
	d.runAsOrker.Store(&runAsOrkerFinding{err: runAsOrkerVerdict(ws, containerNonce)})
	return nil
}

// judgeBootProbe runs the boot probe's first container and judges it: the
// bind both ways, then the posture it was given.
func (d *OCIDispatcher) judgeBootProbe(ctx context.Context, prober bootProber, root string, ws *Workspace,
	spec ContainerSpec, hostNonce, containerNonce string,
) error {
	stdout, err := prober.RunBootProbe(ctx, spec)
	if err != nil {
		return fmt.Errorf("boot probe under %s: %w", root, err)
	}
	facts := parseProbeFacts(stdout)

	// The bind first: what the container saw is only worth judging once it
	// is known to be what the Orker gave it.
	if err := facts.sameDirectory(ws, hostNonce, containerNonce); err != nil {
		return fmt.Errorf("%w: the runtime did not bind %s itself into the probe's container: %v; "+
			"it bound a directory of the same name that the Orker cannot see, as a daemon that does not share "+
			"the Orker's filesystem does, or the container may not read the one it was given, as under an "+
			"SELinux or AppArmor label the bind does not carry", ErrNotBindable, root, err)
	}
	violations, unverified := facts.postureViolations(postureWantOf(spec))
	for _, note := range unverified {
		d.logger().Warn("boot probe: a part of the container's posture could not be checked", "detail", note)
	}
	if len(violations) > 0 {
		return fmt.Errorf("%w: the boot probe ran a container built as a task's is, and from inside it: %s",
			ErrPostureRefused, strings.Join(violations, "; "))
	}
	return nil
}

// bootProbeSpec is the probe's container: built by containerSpec, which every
// task's container is built by, for a task that requests nothing and runs as
// the Orker. It differs from a task's only in what a task supplies — its
// image, its command, its labels — and in its network, none, because the
// probe needs none and an unreachable default network is every task's
// RUNTIME_REFUSED, not a reason to refuse an Orker that runs network-less
// tasks.
//
// It runs as the Orker's user, the §3.1 escape hatch, so that one container
// also shows where that user lands. That does not weaken what it shows about
// the posture: the bounding set, NoNewPrivs, seccomp and the mount table read
// the same for every uid.
func (d *OCIDispatcher) bootProbeSpec(ws *Workspace, containerNonce string) (ContainerSpec, error) {
	return d.probeContainerSpec(ws, bootProbeTaskID, []string{"/bin/sh", "-c", bootProbeScript, "ork-probe", containerNonce})
}

// signalProbeSpec is the SIGTERM probe's container: the boot probe's, with its
// own command and name.
func (d *OCIDispatcher) signalProbeSpec(ws *Workspace) (ContainerSpec, error) {
	return d.probeContainerSpec(ws, signalProbeTaskID, []string{"/bin/sh", "-c", signalProbeScript})
}

func (d *OCIDispatcher) probeContainerSpec(ws *Workspace, taskID string, command []string) (ContainerSpec, error) {
	cpu, memory, failure := d.Policy.ResolveResources(nil)
	if failure != nil {
		return ContainerSpec{}, fmt.Errorf("%w: the probe's resources: %s", ErrPostureRefused, failure.Message)
	}
	in := &orkstackv1.ExecuteTaskInput{TaskId: taskID, Command: command}
	image := &orkstackv1.ImageRef{Pinned: BindProbeImage}
	spec, failure := d.containerSpec(in, ws, image, 1, NetworkNone, cpu, memory, true)
	if failure != nil {
		return ContainerSpec{}, fmt.Errorf("%w: the probe's container: %s", ErrPostureRefused, failure.Message)
	}
	// Its own label only: no ork.orker, so no reaper and no test counting an
	// Orker's containers ever meets a probe that is already being removed.
	spec.Labels = map[string]string{"ork.task": taskID}
	return spec, nil
}

// probeSignalDelivery is the boot probe's SIGTERM check: a second container,
// built as a task's is, whose workload traps SIGTERM, cancelled exactly as a
// cancelled task's container is (TerminateContainer: SIGTERM through the
// runtime, the grace, then SIGKILL). The first container cannot show it: it
// runs to its end and is never signalled, and a runtime that gives a
// container its whole posture can still drop the one signal a cancelled task
// needs. Rootful Podman under AppArmor 5 runs the workload under a stacked
// label that its profile does not let the container's init signal, so every
// cancelled task was killed at the end of its grace without its cleanup, and
// nothing said so.
//
// What decides is the file the trap writes into the output bind, which the
// Orker reads itself: no daemon stands between the verdict and what the
// workload did. A workload that never heard SIGTERM refuses boot as
// ErrPostureRefused, naming what it ran under. Starting the container is
// StartBootProbe, with the first container's budget, retry and
// classification; a workload that does not say it is ready within the budget
// is ErrProbeTimedOut.
func (d *OCIDispatcher) probeSignalDelivery(ctx context.Context, prober bootProber, root string, ws *Workspace) error {
	spec, err := d.signalProbeSpec(ws)
	if err != nil {
		return err
	}
	id, err := prober.StartBootProbe(ctx, spec)
	if err != nil {
		return fmt.Errorf("boot probe's SIGTERM container under %s: %w", root, err)
	}
	// The daemon removes the container once it exits. One that may still be
	// running when the check ends is removed here, on a context of its own,
	// as a task's container is; a removal that fails is reported, not fatal:
	// what the check found stands.
	running := true
	defer func() {
		if !running {
			return
		}
		if err := d.Runtime.Remove(context.WithoutCancel(ctx), id); err != nil {
			d.logger().Warn("boot probe: the SIGTERM probe's container could not be removed and may remain",
				"container", id, "error", err)
		}
	}()

	budget := prober.bindProbeTimeout()
	readyCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	readyPath := filepath.Join(ws.Output, signalProbeReadyFile)
	if !awaitProbeFile(readyCtx, readyPath) {
		if ctx.Err() != nil {
			return fmt.Errorf("the boot probe was interrupted: %w", ctx.Err())
		}
		return probeTimedOut(fmt.Sprintf("let the SIGTERM probe's workload say it is ready within %s", budget), nil, nil)
	}
	raw, _ := os.ReadFile(readyPath)
	facts := parseProbeFacts(string(raw))

	heardPath := filepath.Join(ws.Output, signalProbeHeardFile)
	heard := make(chan struct{})
	watchCtx, stopWatching := context.WithCancel(ctx)
	go func() {
		defer close(heard)
		awaitProbeFile(watchCtx, heardPath)
	}()
	grace := d.signalProbeGrace
	if grace <= 0 {
		grace = DefaultCancellationGrace
	}
	signals := &signalRecorder{ContainerRuntime: d.Runtime}
	TerminateContainer(ctx, signals, id, grace, heard)
	stopWatching()
	<-heard
	if _, err := os.Stat(heardPath); err == nil {
		// Its trap exits, and the daemon removes the container.
		running = false
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("the boot probe was interrupted: %w", ctx.Err())
	}
	// A SIGKILL the runtime took ends the container, which the daemon
	// then removes.
	running = signals.failed(SIGKILL) != nil
	return signalNotHeard(grace, signals.failed(SIGTERM), facts)
}

// signalRecorder is the runtime as TerminateContainer sees it, keeping what
// each Signal call answered: a SIGTERM the runtime itself refused is not a
// workload that did not hear it.
type signalRecorder struct {
	ContainerRuntime
	mu      sync.Mutex
	answers map[Signal]error
}

func (r *signalRecorder) Signal(ctx context.Context, id ContainerID, sig Signal) error {
	err := r.ContainerRuntime.Signal(ctx, id, sig)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.answers == nil {
		r.answers = map[Signal]error{}
	}
	r.answers[sig] = err
	return err
}

// failed is the runtime's refusal of sig, or nil: it took it, or it was never
// sent.
func (r *signalRecorder) failed(sig Signal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.answers[sig]
}

// awaitProbeFile reports whether path exists, polling until it does or ctx
// ends, and looking once more after that. It polls a real condition with a
// deadline, not a sleep standing in for synchronization, and backs off as
// awaitLeftCreated does.
func awaitProbeFile(ctx context.Context, path string) bool {
	exists := func() bool {
		_, err := os.Stat(path)
		return err == nil
	}
	poll := startStatePoll
	for {
		if exists() {
			return true
		}
		select {
		case <-ctx.Done():
			return exists()
		case <-time.After(poll):
		}
		if poll *= 2; poll > startStatePollMax {
			poll = startStatePollMax
		}
	}
}

// sigtermBit is SIGTERM's bit in /proc/<pid>/status's signal masks.
const sigtermBit = 1 << (15 - 1)

// signalNotHeard is the refusal of a runtime whose cancelled workload never
// heard SIGTERM, with every cause it can point at: the runtime's own refusal
// of the signal, then what the workload reported.
func signalNotHeard(grace time.Duration, termRefused error, facts probeFacts) error {
	var msg strings.Builder
	fmt.Fprintf(&msg, "the boot probe ran a container built as a task's is, whose workload traps SIGTERM, and cancelled it as "+
		"a task is cancelled (SIGTERM through the runtime, then SIGKILL after %s), and its workload never heard SIGTERM: "+
		"its trap had not run at the end of the grace. "+
		"Every cancelled task here would be killed at the end of its grace without its cleanup", grace)
	if termRefused != nil {
		fmt.Fprintf(&msg, "; the runtime refused the SIGTERM itself: %v", termRefused)
	} else {
		for _, cause := range signalCauses(facts) {
			msg.WriteString("; ")
			msg.WriteString(cause)
		}
	}
	return fmt.Errorf("%w: %s", ErrPostureRefused, msg.String())
}

// signalCauses reads the SIGTERM probe's ready file for why its workload did
// not hear SIGTERM: its own signal masks, then the security label it ran
// under.
func signalCauses(facts probeFacts) []string {
	var causes []string
	masks := map[string]string{}
	for _, line := range facts["signals"] {
		if key, value, ok := strings.Cut(line, ":"); ok {
			masks[key] = strings.TrimSpace(value)
		}
	}
	has := func(mask string) bool {
		value, err := strconv.ParseUint(masks[mask], 16, 64)
		return err == nil && value&sigtermBit != 0
	}
	switch {
	case has("SigIgn"):
		causes = append(causes, fmt.Sprintf("its workload started with SIGTERM ignored (SigIgn %s), which a shell cannot "+
			"trap: whatever starts the engine's containers hands SIGTERM on ignored", masks["SigIgn"]))
	case has("SigBlk"):
		causes = append(causes, fmt.Sprintf("its workload runs with SIGTERM blocked (SigBlk %s)", masks["SigBlk"]))
	case masks["SigCgt"] != "" && !has("SigCgt"):
		causes = append(causes, fmt.Sprintf("its trap on SIGTERM is not set (SigCgt %s)", masks["SigCgt"]))
	}

	label := strings.TrimSpace(strings.Join(facts["label"], " "))
	switch {
	case strings.Contains(label, "//&"):
		causes = append(causes, fmt.Sprintf("its workload runs under the stacked AppArmor label %q, and an AppArmor "+
			"profile that lets a process signal only its own exact label keeps the container's init from passing SIGTERM "+
			"on to it; the host's kernel log has the denial (apparmor=\"DENIED\" operation=\"signal\"). "+
			"docs/DEPLOY.md, \"Rootful Podman under AppArmor\", gives the host's fix, or run the engine rootless", label))
	case label != "" && label != "unconfined" && !strings.Contains(label, ": "):
		causes = append(causes, fmt.Sprintf("its workload runs under the security label %q, and the host's kernel log "+
			"names any signal that label's policy denied", label))
	}
	if len(causes) == 0 {
		causes = append(causes, "nothing inside the container says why: the runtime, the container's init or a security "+
			"policy dropped it, and the host's kernel log names any denial")
	}
	return causes
}

func probeNonce() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// probeFacts is the probe container's stdout, by section.
type probeFacts map[string][]string

func parseProbeFacts(stdout string) probeFacts {
	facts := probeFacts{}
	section := ""
	for _, line := range strings.Split(stdout, "\n") {
		if name, ok := strings.CutPrefix(line, "@@"); ok {
			section = name
			facts[section] = nil
			continue
		}
		if section != "" {
			facts[section] = append(facts[section], line)
		}
	}
	for name, lines := range facts {
		for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
			lines = lines[:len(lines)-1]
		}
		facts[name] = lines
	}
	return facts
}

// sameDirectory proves the bind both ways: the container read the nonce the
// Orker wrote into input/, and the Orker reads the nonce the container wrote
// into output/.
func (f probeFacts) sameDirectory(ws *Workspace, hostNonce, containerNonce string) error {
	if _, ran := f["nonce"]; !ran {
		return errors.New("the container reported nothing of what it saw")
	}
	if got := strings.Join(f["nonce"], "\n"); got != hostNonce {
		return fmt.Errorf("the container read %q at %s/%s, not the nonce the Orker wrote at %s",
			truncateString(got, 300), ContainerInput, bootProbeNonceFile, filepath.Join(ws.Input, bootProbeNonceFile))
	}
	path := filepath.Join(ws.Output, bootProbeNonceFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		said := ""
		if wrote := strings.Join(f["write"], " "); wrote != "" {
			said = fmt.Sprintf(" (the container said %q)", truncateString(wrote, 300))
		}
		return fmt.Errorf("the file the container wrote at %s/%s is not at %s%s: %v",
			ContainerOutput, bootProbeNonceFile, path, said, err)
	}
	if string(raw) != containerNonce {
		return fmt.Errorf("%s holds %q, not the nonce the container wrote", path, truncateString(string(raw), 80))
	}
	return nil
}

// postureWant is what the probe's container asked the runtime for.
type postureWant struct {
	cpus        float64
	memoryBytes int64
	pids        int64
	tmp         string
}

func postureWantOf(spec ContainerSpec) postureWant {
	cpus, _ := quantity.ParseCPU(spec.CPUs)
	memory, _ := quantity.ParseMemory(spec.Memory)
	return postureWant{cpus: cpus, memoryBytes: int64(memory), pids: spec.PidsLimit, tmp: spec.TmpfsPath}
}

// maxPageBytes is the largest page a Linux kernel uses (arm64's 64 KiB): a
// memory limit reads back at most one page below the request.
const maxPageBytes = 64 << 10

// cpuTolerance is how far cpu.max may be from the request: systemd, which
// writes it on a host whose runtime uses the systemd cgroup driver, rounds a
// quota to 10 ms per second, which is 0.01 of a cpu.
const cpuTolerance = 0.011

// postureViolations is every way the container's posture was weaker than
// §9.2, one sentence each, and what this host could not show at all.
func (f probeFacts) postureViolations(want postureWant) (violations, unverified []string) {
	status := map[string]string{}
	for _, line := range f["status"] {
		if key, value, ok := strings.Cut(line, ":"); ok {
			status[key] = strings.TrimSpace(value)
		}
	}
	if got := status["NoNewPrivs"]; got != "1" {
		violations = append(violations, fmt.Sprintf("NoNewPrivs is %q, not 1: its processes can gain privileges", got))
	}
	if got := status["Seccomp"]; got != "2" {
		violations = append(violations, fmt.Sprintf("Seccomp is %q, not 2: no seccomp filter is applied", got))
	}
	for _, set := range []string{"CapEff", "CapPrm", "CapBnd"} {
		raw, ok := status[set]
		value, err := strconv.ParseUint(raw, 16, 64)
		if !ok || err != nil || value != 0 {
			violations = append(violations, fmt.Sprintf("%s is %q, not 0: it holds capabilities", set, raw))
		}
	}

	mounts := parseMounts(f["mounts"])
	readOnly := []string{"/"}
	readOnly = append(readOnly, readOnlyRootfsPaths...)
	readOnly = append(readOnly, ContainerInput, ContainerSecrets)
	for _, path := range readOnly {
		if options, ok := mounts.effective(path); !ok || !options["ro"] {
			violations = append(violations, fmt.Sprintf("%s is writable", path))
		}
	}
	if want.tmp != "" {
		options, ok := mounts.effective(want.tmp)
		for _, required := range []string{"nosuid", "nodev", "noexec"} {
			if !ok || !options[required] {
				violations = append(violations, fmt.Sprintf("%s is not mounted %s", want.tmp, required))
			}
		}
	}

	var proxies []string
	for _, name := range f["env"] {
		if strings.HasSuffix(strings.ToLower(name), "_proxy") {
			proxies = append(proxies, name)
		}
	}
	if len(proxies) > 0 {
		violations = append(violations, fmt.Sprintf("its environment holds %s, which it was never given: "+
			"a task's environment is its own env: and nothing else", strings.Join(proxies, ", ")))
	}

	cgroup := f["cgroup"]
	if len(cgroup) != 1 || !strings.HasPrefix(cgroup[0], "0::") {
		unverified = append(unverified, fmt.Sprintf("the container's cgroup is %q, which is not cgroup v2 alone, "+
			"so its cpu, memory and pids limits were not read", strings.Join(cgroup, " | ")))
		return violations, unverified
	}
	violations = append(violations, cgroupViolations(f, want)...)
	return violations, unverified
}

func cgroupViolations(f probeFacts, want postureWant) []string {
	var violations []string
	one := func(file string) (string, bool) {
		lines := f[file]
		if len(lines) != 1 || strings.TrimSpace(lines[0]) == "" {
			violations = append(violations, fmt.Sprintf("its cgroup's %s could not be read, so that limit is not enforced", file))
			return "", false
		}
		return strings.TrimSpace(lines[0]), true
	}

	if raw, ok := one("memory.max"); ok {
		// The kernel keeps the limit in its pages, so a byte count that is not
		// a whole number of them reads back rounded down — by the daemon's
		// kernel's page, which a VM-hosted daemon does not share with the
		// Orker's host, so any page up to the largest Linux uses is allowed.
		if got, err := strconv.ParseInt(raw, 10, 64); err != nil || got > want.memoryBytes || want.memoryBytes-got >= maxPageBytes {
			violations = append(violations, fmt.Sprintf("memory.max is %q, not the %d bytes it asked for", raw, want.memoryBytes))
		}
	}
	if raw, ok := one("pids.max"); ok {
		if got, err := strconv.ParseInt(raw, 10, 64); err != nil || got != want.pids {
			violations = append(violations, fmt.Sprintf("pids.max is %q, not the %d it asked for", raw, want.pids))
		}
	}
	if raw, ok := one("cpu.max"); ok {
		fields := strings.Fields(raw)
		var got float64
		valid := len(fields) == 2
		if valid {
			quota, qerr := strconv.ParseFloat(fields[0], 64)
			period, perr := strconv.ParseFloat(fields[1], 64)
			valid = qerr == nil && perr == nil && period > 0
			got = quota / period
		}
		if !valid || math.Abs(got-want.cpus) > cpuTolerance {
			violations = append(violations, fmt.Sprintf("cpu.max is %q, not the %g cpus it asked for", raw, want.cpus))
		}
	}
	return violations
}

// mountTable is /proc/mounts in order: a later mount at a target hides an
// earlier one.
type mountTable []struct {
	target  string
	options map[string]bool
}

func parseMounts(lines []string) mountTable {
	var table mountTable
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		options := map[string]bool{}
		for _, option := range strings.Split(fields[3], ",") {
			options[option] = true
		}
		table = append(table, struct {
			target  string
			options map[string]bool
		}{target: unescapeMountPath(fields[1]), options: options})
	}
	return table
}

// effective is the options of the mount a path lies on: the deepest target
// that is the path or one of its parents, and of those the last mounted.
func (t mountTable) effective(path string) (map[string]bool, bool) {
	best := -1
	for i, m := range t {
		covers := m.target == path || m.target == "/" ||
			strings.HasPrefix(path, strings.TrimSuffix(m.target, "/")+"/")
		if covers && (best < 0 || len(m.target) >= len(t[best].target)) {
			best = i
		}
	}
	if best < 0 {
		return nil, false
	}
	return t[best].options, true
}

// unescapeMountPath undoes /proc/mounts' octal escapes (\040 for a space).
func unescapeMountPath(raw string) string {
	if !strings.Contains(raw, `\`) {
		return raw
	}
	var b strings.Builder
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' && i+3 < len(raw) {
			if n, err := strconv.ParseUint(raw[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(raw[i])
	}
	return b.String()
}

// runAsOrkerVerdict reads back the 0600 file the probe's container wrote as
// the Orker's user. The hatch exists so the Orker can read what a workload
// writes whatever its umask, and that is exactly what is checked: a user
// namespace that maps the Orker's uid to another host user leaves a file the
// Orker cannot open.
func runAsOrkerVerdict(ws *Workspace, containerNonce string) error {
	path := filepath.Join(ws.Output, bootProbeAsOrkerFile)
	raw, err := os.ReadFile(path)
	if err == nil && string(raw) == containerNonce {
		return nil
	}
	if err == nil {
		err = fmt.Errorf("it holds %q, not what the container wrote", truncateString(string(raw), 80))
	}
	return fmt.Errorf("%w: a 0600 file the boot probe's container wrote while running as this Orker's user "+
		"(uid %d, gid %d) cannot be read back by the Orker (%s): %v; the runtime maps that uid to another host user, "+
		"as a rootless or user-namespaced daemon does", ErrRunAsOrkerUnavailable, os.Getuid(), os.Getgid(),
		describeUnreadable(path), err)
}
