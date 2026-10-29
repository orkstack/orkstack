package orker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// TestBootProbe_ItsContainerIsATasks is G16: the boot probe's container is
// rendered with every flag a task's container is, by the same builder and the
// same rendering, so what the runtime refuses or weakens for it, it refuses
// or weakens for every task. It differs from a task only in what a task
// supplies: its image, its command, its labels and its network.
func TestBootProbe_ItsContainerIsATasks(t *testing.T) {
	probe := probeSpecUnder(t, t.TempDir())
	d := &OCIDispatcher{Policy: DefaultSecurityPolicy(), OrkerID: "orker-1"}
	in := baseExportInput("build")
	// A task's grace period is its own (its --stop-timeout); the probe asks
	// for none, as a task that sets none does.
	in.CancellationGracePeriodSeconds = 0
	ws, err := NewWorkspace(filepath.Join(t.TempDir(), "attempt"))
	if err != nil {
		t.Fatal(err)
	}
	cpu, memory, _ := d.Policy.ResolveResources(nil)
	task, failure := d.containerSpec(in, ws, in.GetImage(), 1, NetworkNone, cpu, memory, true)
	if failure != nil {
		t.Fatal(failure)
	}

	posture := func(spec ContainerSpec) []string {
		flags, err := dockerContainerFlags(spec, "ork-x", dockerDialect)
		if err != nil {
			t.Fatalf("dockerContainerFlags: %v", err)
		}
		var kept []string
		for i := 0; i < len(flags); i++ {
			switch flags[i] {
			case "--label":
				i++
				continue
			case "--mount":
				// The source is the attempt's own directory; the
				// destination and the options are the posture.
				_, rest, _ := strings.Cut(flags[i+1], ",destination=")
				kept = append(kept, "--mount", rest)
				i++
				continue
			case spec.Image:
				return kept
			}
			kept = append(kept, flags[i])
		}
		return kept
	}
	if got, want := posture(probe), posture(task); !slices.Equal(got, want) {
		t.Fatalf("the probe's container is not a task's:\n probe %q\n  task %q", got, want)
	}
	rendered := strings.Join(posture(probe), " ")
	for _, want := range []string{
		"--security-opt seccomp=" + SeccompBuiltin, "--init", "--cap-drop=ALL", "--read-only",
		"--tmpfs /tmp:rw,nosuid,nodev,noexec", "--pids-limit", "--memory", "--cpus",
		fmt.Sprintf("--user %d:%d", os.Getuid(), os.Getgid()),
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the probe's container lacks %q:\n%s", want, rendered)
		}
	}
	if probe.Image != BindProbeImage {
		t.Errorf("the probe runs %q; want %q", probe.Image, BindProbeImage)
	}
	if _, ok := probe.Labels["ork.orker"]; ok {
		t.Errorf("the probe's container carries ork.orker (%v): a reaper would meet a probe still being removed", probe.Labels)
	}
}

// TestBootProbe_RefusesEachWeakenedPosture is G16's judgement, one row per
// way a runtime can run a task's container with less than §9.2 says: each is
// named, and a posture that is all there passes. A host whose cgroup is not
// v2 alone is not a weaker posture the probe saw: its limits could not be
// read, which the probe says rather than claims.
func TestBootProbe_RefusesEachWeakenedPosture(t *testing.T) {
	policy := DefaultSecurityPolicy()
	good := GoodBootProbeFacts(policy)
	want := postureWant{cpus: 1, memoryBytes: 1 << 30, pids: policy.PidsLimit, tmp: ContainerScratchTmp}
	replace := func(old, new string) string {
		t.Helper()
		if !strings.Contains(good, old) {
			t.Fatalf("the good facts hold no %q", old)
		}
		return strings.Replace(good, old, new, 1)
	}
	tests := []struct {
		name       string
		facts      string
		violation  string // "" passes
		unverified string
	}{
		{name: "the whole posture", facts: good},
		{name: "privileges can be gained", facts: replace("NoNewPrivs:\t1", "NoNewPrivs:\t0"), violation: "NoNewPrivs"},
		{name: "no seccomp filter", facts: replace("Seccomp:\t2", "Seccomp:\t0"), violation: "Seccomp"},
		{name: "effective capabilities", facts: replace("CapEff:\t0000000000000000", "CapEff:\t00000000a80425fb"), violation: "CapEff"},
		{name: "a bounding set that was not dropped", facts: replace("CapBnd:\t0000000000000000", "CapBnd:\t000001ffffffffff"), violation: "CapBnd"},
		{name: "no status reported", facts: replace("NoNewPrivs:\t1\n", ""), violation: "NoNewPrivs"},
		{name: "a writable root filesystem", facts: replace("overlay / overlay ro,", "overlay / overlay rw,"), violation: "/ is writable"},
		{
			name:      "a writable /run, as Podman's --read-only leaves it",
			facts:     replace("tmpfs /run tmpfs ro,", "tmpfs /run tmpfs rw,"),
			violation: "/run is writable",
		},
		{
			name:  "a /var/tmp the runtime left out, on a root that is read-only",
			facts: replace("tmpfs /var/tmp tmpfs ro,nosuid,nodev,noexec,relatime,size=1024k 0 0\n", ""),
		},
		{
			name:      "a later writable mount over /var/tmp",
			facts:     replace("@@env", "tmpfs /var/tmp tmpfs rw,nosuid 0 0\n@@env"),
			violation: "/var/tmp is writable",
		},
		{name: "a writable input", facts: replace("/orkstack/input ext4 ro,", "/orkstack/input ext4 rw,"), violation: "/orkstack/input is writable"},
		{
			name:      "a /tmp a workload can execute from, as Podman's --tmpfs leaves it",
			facts:     replace("tmpfs /tmp tmpfs rw,nosuid,nodev,noexec,", "tmpfs /tmp tmpfs rw,nosuid,nodev,"),
			violation: "/tmp is not mounted noexec",
		},
		{
			name:      "the runtime's own proxy variables, as the podman CLI forwards them",
			facts:     replace("HOME\n", "HOME\nHTTP_PROXY\nno_proxy\n"),
			violation: "HTTP_PROXY, no_proxy",
		},
		{name: "no memory limit", facts: replace("@@memory.max\n1073741824", "@@memory.max\nmax"), violation: "memory.max"},
		{name: "another memory limit", facts: replace("@@memory.max\n1073741824", "@@memory.max\n536870912"), violation: "memory.max"},
		{
			// The daemon's kernel keeps the limit in its own pages, which
			// need not be the Orker's host's (a VM-hosted daemon).
			name:  "a memory limit rounded down to a page of the daemon's kernel",
			facts: replace("@@memory.max\n1073741824", "@@memory.max\n1073729536"),
		},
		{name: "a memory limit above the request", facts: replace("@@memory.max\n1073741824", "@@memory.max\n1073745920"), violation: "memory.max"},
		{name: "no pids limit", facts: replace("@@pids.max\n512", "@@pids.max\nmax"), violation: "pids.max"},
		{name: "no cpu limit", facts: replace("@@cpu.max\n100000 100000", "@@cpu.max\nmax 100000"), violation: "cpu.max"},
		{name: "another cpu limit", facts: replace("@@cpu.max\n100000 100000", "@@cpu.max\n200000 100000"), violation: "cpu.max"},
		{name: "a cpu limit systemd rounded", facts: replace("@@cpu.max\n100000 100000", "@@cpu.max\n101000 100000")},
		{
			name:      "a cgroup with no cpu controller, as a rootless runtime without delegation has",
			facts:     replace("@@cpu.max\n100000 100000\n", "@@cpu.max\n"),
			violation: "cpu.max could not be read",
		},
		{
			name:       "a cgroup v1 host",
			facts:      replace("@@cgroup\n0::/\n", "@@cgroup\n12:pids:/docker/x\n0::/\n"),
			unverified: "not cgroup v2 alone",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			violations, unverified := parseProbeFacts("@@nonce\nn\n" + tt.facts).postureViolations(want)
			switch {
			case tt.violation == "" && len(violations) > 0:
				t.Fatalf("violations = %q; want none", violations)
			case tt.violation != "" && !strings.Contains(strings.Join(violations, "; "), tt.violation):
				t.Fatalf("violations = %q; want one naming %q", violations, tt.violation)
			}
			if got := strings.Join(unverified, "; "); (tt.unverified == "") != (got == "") || !strings.Contains(got, tt.unverified) {
				t.Fatalf("unverified = %q; want %q", unverified, tt.unverified)
			}
		})
	}
}

// TestBootProbe_ABindItCouldNotUseSaysWhy: a container denied its own bind —
// an SELinux label the bind does not carry, an ACL — reads and writes nothing
// there, exactly as one given another directory does. The refusal quotes
// what the container said, so the two are told apart.
func TestBootProbe_ABindItCouldNotUseSaysWhy(t *testing.T) {
	ws, err := NewWorkspace(filepath.Join(t.TempDir(), "probe"))
	if err != nil {
		t.Fatal(err)
	}
	denied := "cat: can't open '/orkstack/input/probe-nonce': Permission denied"
	err = parseProbeFacts("@@nonce\n"+denied+"\n").sameDirectory(ws, "host-nonce", "container-nonce")
	if err == nil || !strings.Contains(err.Error(), "Permission denied") {
		t.Fatalf("sameDirectory = %v; want it to quote what the container could not read", err)
	}

	unwritable := "sh: can't create /orkstack/output/probe-nonce: Permission denied"
	err = parseProbeFacts("@@nonce\nhost-nonce\n@@write\n"+unwritable+"\n").sameDirectory(ws, "host-nonce", "container-nonce")
	if err == nil || !strings.Contains(err.Error(), "can't create") {
		t.Fatalf("sameDirectory = %v; want it to quote what the container could not write", err)
	}
}

// TestBootProbe_ReadsTheMountAPathLiesOn: /proc/mounts lists mounts in the
// order they were made, and a path lies on the deepest one that covers it,
// the last of those mounted at the same target.
func TestBootProbe_ReadsTheMountAPathLiesOn(t *testing.T) {
	table := parseMounts([]string{
		"overlay / overlay ro 0 0",
		"tmpfs /run tmpfs ro 0 0",
		"tmpfs /run tmpfs rw 0 0",
		"/dev/x /orkstack/in\\040put ext4 rw 0 0",
	})
	tests := []struct {
		path   string
		wantRO bool
	}{
		{"/", true}, {"/etc", true}, {"/run", false}, {"/run/lock", false}, {"/runner", true},
		{"/orkstack/in put", false}, {"/orkstack/input", true},
	}
	for _, tt := range tests {
		options, ok := table.effective(tt.path)
		if !ok || options["ro"] != tt.wantRO {
			t.Errorf("effective(%q) ro = %v (found %v); want %v", tt.path, options["ro"], ok, tt.wantRO)
		}
	}
}

// TestBoot_RunAsOrkerThatDoesNotLandAsTheOrker is G10: the boot probe's
// container runs as the Orker's user and writes a 0600 file, and when the
// Orker cannot read it back the escape hatch does not land as the Orker on
// this host. An Orker forced to run every task that way (ORK_ORKER_FORCE_USER)
// refuses to start; any other Orker starts, says so, and fails only the tasks
// that ask for the hatch.
func TestBoot_RunAsOrkerThatDoesNotLandAsTheOrker(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprintf("force_user=%t", force), func(t *testing.T) {
			fake := newCountingDocker(t)
			fake.set("present", "")
			fake.set("asorkerwrong", "")
			policy := SecurityPolicyFromEnv()
			policy.ForceUser = force
			var logs bytes.Buffer
			activities := NewActivitiesWithOptions(nil, Options{
				OrkerID:  fmt.Sprintf("orker-hatch-%d-%t", os.Getpid(), force),
				WorkRoot: t.TempDir(),
				Runtime:  &DockerRuntime{Bin: fake.bin},
				Policy:   &policy,
				Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
			})
			t.Cleanup(activities.Close)

			err := activities.Boot(context.Background())

			if force {
				if err == nil || !strings.HasPrefix(err.Error(), ReasonRunAsOrkerUnavailable+":") ||
					!strings.Contains(err.Error(), ForceUserVar) {
					t.Fatalf("Boot = %v; want a refusal starting %s: that names %s", err, ReasonRunAsOrkerUnavailable, ForceUserVar)
				}
				return
			}
			if err != nil {
				t.Fatalf("Boot = %v; an Orker that does not force the hatch boots", err)
			}
			if !strings.Contains(logs.String(), ReasonRunAsOrkerUnavailable) {
				t.Errorf("the boot log does not say the hatch is unavailable:\n%s", logs.String())
			}
			if !errors.Is(activities.dispatcher.runAsOrkerUnavailable(), ErrRunAsOrkerUnavailable) {
				t.Errorf("runAsOrkerUnavailable = %v; want the probe's finding kept", activities.dispatcher.runAsOrkerUnavailable())
			}
		})
	}
}

// specRecordingRuntime is a phaseCapturingRuntime that keeps every spec it
// was asked to create, and can make the work root unwritable while the
// container runs, so the attempt's own directory cannot be removed after it.
type specRecordingRuntime struct {
	*phaseCapturingRuntime
	lockRoot string

	mu    sync.Mutex
	specs []ContainerSpec
}

func (r *specRecordingRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	return r.phaseCapturingRuntime.Create(ctx, spec)
}

func (r *specRecordingRuntime) Wait(ctx context.Context, id ContainerID) (ExitStatus, error) {
	if r.lockRoot != "" {
		if err := os.Chmod(r.lockRoot, 0o555); err != nil {
			return ExitStatus{}, err
		}
	}
	return r.phaseCapturingRuntime.Wait(ctx, id)
}

func (r *specRecordingRuntime) created() []ContainerSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ContainerSpec(nil), r.specs...)
}

// TestExecute_RunAsOrkerIsRefusedWhereTheProbeFoundItDoesNotLand is G10's
// task half: on an Orker whose boot probe found the hatch does not land as
// the Orker, a task that asks for it fails RUN_AS_ORKER_UNAVAILABLE before a
// container is created, and is not retried; a task that does not ask for it
// runs, as the image's own user.
func TestExecute_RunAsOrkerIsRefusedWhereTheProbeFoundItDoesNotLand(t *testing.T) {
	runtime := &specRecordingRuntime{phaseCapturingRuntime: &phaseCapturingRuntime{}}
	d := newPhaseDispatcher(t, nil, nil)
	d.Runtime = runtime
	d.runAsOrker.Store(&runAsOrkerFinding{err: fmt.Errorf("%w: test", ErrRunAsOrkerUnavailable)})

	asked := baseExportInput("as-orker")
	asked.RunAsOrkerUser = true
	_, err := d.Execute(context.Background(), asked)
	if err == nil || !strings.Contains(err.Error(), ReasonRunAsOrkerUnavailable) {
		t.Fatalf("Execute of a task that asks for the hatch = %v; want %s", err, ReasonRunAsOrkerUnavailable)
	}
	if RetryableReason(ReasonRunAsOrkerUnavailable) {
		t.Errorf("%s is retryable; the host it found does not change between attempts", ReasonRunAsOrkerUnavailable)
	}
	if specs := runtime.created(); len(specs) != 0 {
		t.Fatalf("a container was created for a task the hatch refused: %+v", specs)
	}

	if _, err := d.Execute(context.Background(), baseExportInput("as-image")); err != nil {
		t.Fatalf("Execute of a task that does not ask for the hatch = %v", err)
	}
	if specs := runtime.created(); len(specs) != 1 || specs[0].RunAsOrker {
		t.Fatalf("specs = %+v; want one, not run as the Orker", specs)
	}
}

// TestExecute_RunAsOrkerIsTheSpecsIntent is G10's rendering half: the
// dispatcher asks for "as the Orker" and leaves the uid to the runtime.
func TestExecute_RunAsOrkerIsTheSpecsIntent(t *testing.T) {
	runtime := &specRecordingRuntime{phaseCapturingRuntime: &phaseCapturingRuntime{}}
	d := newPhaseDispatcher(t, nil, nil)
	d.Runtime = runtime
	in := baseExportInput("as-orker")
	in.RunAsOrkerUser = true
	if _, err := d.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute = %v", err)
	}
	specs := runtime.created()
	if len(specs) != 1 || !specs[0].RunAsOrker {
		t.Fatalf("specs = %+v; want one that runs as the Orker", specs)
	}
	args := strings.Join(mustCreateArgs(t, specs[0]), " ")
	if want := fmt.Sprintf("--user %d:%d", os.Getuid(), os.Getgid()); !strings.Contains(args, want) {
		t.Fatalf("the runtime rendered %q; want %q", args, want)
	}
}

func mustCreateArgs(t *testing.T, spec ContainerSpec) []string {
	t.Helper()
	args, err := dockerCreateArgs(spec)
	if err != nil {
		t.Fatalf("dockerCreateArgs: %v", err)
	}
	return args
}

// TestExecute_ReportsAnAttemptDirectoryItCouldNotRemove is G11: an attempt
// whose own directory cannot be removed when it ends is not discarded in
// silence. The Orker logs the directory and why, and the observer is told,
// which is what ork.orker.workspace_cleanup_failures counts. The attempt's
// own outcome is unchanged.
func TestExecute_ReportsAnAttemptDirectoryItCouldNotRemove(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root, which can remove a directory from one it cannot write; the check is about a non-root Orker")
	}
	obs := &fakePhaseObserver{}
	runtime := &specRecordingRuntime{phaseCapturingRuntime: &phaseCapturingRuntime{}}
	d := newPhaseDispatcher(t, nil, obs)
	d.Runtime = runtime
	var logs bytes.Buffer
	d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	runtime.lockRoot = d.WorkRoot
	t.Cleanup(func() { _ = os.Chmod(d.WorkRoot, 0o755) })

	if _, err := d.Execute(context.Background(), baseExportInput("leaky")); err != nil {
		t.Fatalf("Execute = %v; a directory left behind does not fail the attempt", err)
	}

	_, _, ends, outcome := obs.recorded()
	if ends != 1 || outcome.CleanupErr == nil || outcome.Reason != "" {
		t.Fatalf("outcome = %+v after %d End calls; want a success that reports its cleanup failure", outcome, ends)
	}
	if !strings.Contains(outcome.CleanupErr.Error(), d.WorkRoot) {
		t.Errorf("CleanupErr = %v; want it to name the directory", outcome.CleanupErr)
	}
	for _, want := range []string{"attempt cleanup", "leaky", "permission denied"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the log does not say %q:\n%s", want, logs.String())
		}
	}
}
