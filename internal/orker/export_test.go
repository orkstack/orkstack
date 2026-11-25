package orker

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/quantity"
)

// GoodBootProbeFacts is the stdout, after its nonce, of a boot probe's
// container that a runtime gave the whole §9.2 posture under policy: the
// facts a stand-in runtime prints to pass the probe. It is exported for the
// integration tests' stand-ins, which live in package orker_test.
func GoodBootProbeFacts(policy SecurityPolicy) string {
	memory, _ := quantity.ParseMemory(policy.DefaultMemory)
	page := int64(os.Getpagesize())
	bytes := int64(memory) - int64(memory)%page
	cpus, _ := quantity.ParseCPU(policy.DefaultCPU)
	return strings.Join([]string{
		"@@status",
		"Uid:\t1000\t1000\t1000\t1000",
		"CapInh:\t0000000000000000",
		"CapPrm:\t0000000000000000",
		"CapEff:\t0000000000000000",
		"CapBnd:\t0000000000000000",
		"CapAmb:\t0000000000000000",
		"NoNewPrivs:\t1",
		"Seccomp:\t2",
		"Seccomp_filters:\t1",
		"@@mounts",
		"overlay / overlay ro,relatime,lowerdir=/l,upperdir=/u,workdir=/w 0 0",
		"proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0",
		"tmpfs /run tmpfs ro,nosuid,nodev,noexec,relatime,size=1024k 0 0",
		"tmpfs /tmp tmpfs rw,nosuid,nodev,noexec,relatime,size=65536k 0 0",
		"/dev/root /orkstack/input ext4 ro,relatime 0 0",
		"/dev/root /orkstack/output ext4 rw,relatime 0 0",
		"/dev/root /orkstack/tmp ext4 rw,relatime 0 0",
		"tmpfs /var/tmp tmpfs ro,nosuid,nodev,noexec,relatime,size=1024k 0 0",
		"/dev/root /orkstack/input/secrets ext4 ro,relatime 0 0",
		"@@env",
		"HOSTNAME",
		"HOME",
		"PATH",
		"@@cgroup",
		"0::/",
		"@@memory.max",
		strconv.FormatInt(bytes, 10),
		"@@pids.max",
		strconv.FormatInt(policy.PidsLimit, 10),
		"@@cpu.max",
		fmt.Sprintf("%d 100000", int64(math.Round(cpus*100000))),
	}, "\n") + "\n"
}

// RetryRemovals runs what the periodic retry of unremoved containers runs,
// once, for the integration tests in package orker_test.
func (a *Activities) RetryRemovals(ctx context.Context) { a.retryRemovals(ctx) }

// SetRemovalRetryEvery replaces the interval of the periodic retry, before
// Boot starts it, so a test that drives RetryRemovals itself decides when one
// runs.
func SetRemovalRetryEvery(a *Activities, every time.Duration) { a.removalRetryEvery = every }

// SetSignalProbeGrace shortens the boot probe's SIGTERM grace for the
// integration tests' Orkers, which live in package orker_test.
func SetSignalProbeGrace(a *Activities, grace time.Duration) {
	a.dispatcher.signalProbeGrace = grace
}

// MetadataTargetForTest is one address the metadata probe asks, for the
// integration tests' Orkers: a plain GET of /, and with TokenRequest EC2's
// token PUT after it.
type MetadataTargetForTest struct {
	Address, Host string
	Port          int
	TokenRequest  bool
}

// SetMetadataTargets points the metadata probe at stand-ins.
func SetMetadataTargets(a *Activities, targets []MetadataTargetForTest) {
	a.dispatcher.metadataTargets = nil
	for _, t := range targets {
		host := t.Host
		if strings.Contains(host, ":") {
			host = "[" + host + "]"
		}
		reqs := []metadataRequest{httpGet(host, "/", "")}
		if t.TokenRequest {
			reqs = append(reqs, ec2TokenRequest(host))
		}
		a.dispatcher.metadataTargets = append(a.dispatcher.metadataTargets,
			metadataTarget{Address: t.Address, Host: t.Host, Port: t.Port, Requests: reqs})
	}
}

// MetadataFindings is what Boot's metadata probe found, nil when none ran.
func MetadataFindings(a *Activities) []MetadataFinding {
	if f := a.metadata.Load(); f != nil {
		return *f
	}
	return nil
}

// UsageReading is one read of a container's cgroup, as the usage sampler
// takes it, exported for the integration tests in package orker_test.
type UsageReading struct {
	CPUSeconds float64
	Memory     int64
	MemoryPeak int64
	PeakRead   bool
	Limit      int64
}

// ErrNoCgroup is errNoCgroup: this setup gives a container no readable cgroup.
var ErrNoCgroup = errNoCgroup

// LocateUsage finds container id's cgroup on this host exactly as the usage
// sampler does (usageLocator over hostCgroups) and returns its reader and the
// method's name ("cgroup-v2" or "cgroup-v1").
func LocateUsage(ctx context.Context, d *DockerRuntime, id ContainerID) (func() (UsageReading, error), string, error) {
	read, method, err := d.usageLocator(id, hostCgroups())(ctx)
	if err != nil {
		return nil, "", err
	}
	name := map[usageMethod]string{usageCgroupV2: "cgroup-v2", usageCgroupV1: "cgroup-v1"}[method]
	return func() (UsageReading, error) {
		r, err := read(false)
		return UsageReading{CPUSeconds: r.cpuSeconds, Memory: r.memory, MemoryPeak: r.memoryPeak, PeakRead: r.peakRead, Limit: r.limit}, err
	}, name, nil
}

// CheckReleaseProbeImage is the profile probes' image check on the release
// path (probe_image.go): release resolved, its signature verified, then
// pulled by digest and its build label held to build.
func CheckReleaseProbeImage(ctx context.Context, rt *DockerRuntime, release *ReleaseProbe, build string) (string, orkstackv1.ProfileCordonReason, string, error) {
	return checkProbeImage(ctx, rt, newProbeImageSource("", release, time.Second), build)
}

// ListInterruptions makes d's attempts see types as the NonRetryableErrorTypes
// of their retry policy, as an attempt of a run that pinned the interruption
// class does. It is exported for the integration tests, which run a dispatcher
// outside a Temporal activity and so have no retry policy to read.
func ListInterruptions(d *OCIDispatcher, types ...string) {
	d.listed = func(context.Context) []string { return types }
}

// ErrHookBuildUnknown is the audit's answer for a hook binary the table lacks,
// for the integration tests' errors.Is.
var ErrHookBuildUnknown = errHookBuildUnknown

// AuditNVIDIASpecForTest audits an NVIDIA CDI spec, as written (a host's
// paths), for units, on a host that has nvidia-ctk's mock nodes and library
// files (nvidiaTestHost) and whose hook binary has the digest given, on arch.
// It returns the hook binaries the audit recorded, by path. It is exported
// for TestIntegration_NVIDIAEmulation, which lives in package orker_test and
// holds the real binaries it fetched to the table by digest.
func AuditNVIDIASpecForTest(t testing.TB, spec, arch, hookSHA256 string, units ...string) (map[string]string, error) {
	t.Helper()
	h := nvidiaTestHost(t)
	h.arch = arch
	h.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: hookSHA256}, nil }
	audit, err := auditGPUSpecs([]string{specDir(t, map[string]string{"nvidia.yaml": spec})}, "nvidia.com/gpu", units, h)
	return audit.hooks, err
}

// HookTableRowsForTest lists the table's builds for arch as "version-arch-packaging",
// with each one's binary digest, for the same test.
func HookTableRowsForTest(arch string) map[string]string {
	rows := map[string]string{}
	for _, b := range cdiHookTable {
		if b.Arch == arch {
			rows[b.Version+"-"+b.Arch+"-"+b.Packaging] = b.BinarySHA256
		}
	}
	return rows
}

// CheckDefaultRuntimeForTest is the default-runtime rule on an engine's
// answer, for TestIntegration_NVIDIAEmulation, which asks a real dockerd.
func CheckDefaultRuntimeForTest(name, path string) error {
	return checkDefaultRuntime(runtimeFacts{Name: name, Path: path})
}

// RuntimeQueryForTest is the `docker info --format` template that asks an
// engine for its default runtime, and RuntimeFactsForTest reads its answer, for
// the same test.
func RuntimeQueryForTest() string { return dockerDialect.runtimeQuery() }

func RuntimeFactsForTest(answer string) (name, path string, err error) {
	facts, err := dockerDialect.parseRuntimeFacts(answer)
	return facts.Name, facts.Path, err
}

// NVIDIASpecsDriftForTest audits two NVIDIA specs (each with the hook digest
// given) as one Orker's boot audit and its later periodic audit would, and
// says whether the Orker would latch a drift between them: the specs' bytes,
// the nodes, the mount sources or the hook binary differ.
func NVIDIASpecsDriftForTest(t testing.TB, boot, later, arch, bootHook, laterHook string, units ...string) (bool, error) {
	t.Helper()
	audit := func(spec, hook string) (profileAudit, error) {
		h := nvidiaTestHost(t)
		h.arch = arch
		h.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: hook}, nil }
		found, err := auditGPUSpecs([]string{specDir(t, map[string]string{"nvidia.yaml": spec})}, "nvidia.com/gpu", units, h)
		return gpuAuditOf("id", found), err
	}
	a, err := audit(boot, bootHook)
	if err != nil {
		return false, err
	}
	b, err := audit(later, laterHook)
	if err != nil {
		return false, err
	}
	// The spec bytes are at different paths in the two temp directories; the
	// periodic audit compares them by path, so the digests are compared here.
	for _, x := range a.gpu.files {
		a.gpu.files = map[string]string{"f": x}
	}
	for _, y := range b.gpu.files {
		b.gpu.files = map[string]string{"f": y}
	}
	return !sameGPUAudit(a, b), nil
}

// hangBound bounds a call under ExpireOnHang that is neither answered nor held.
const hangBound = 2 * time.Minute

// ExpireOnHang makes each of d's CLI calls end, as one whose budget ran out
// ends, when the stand-in CLI appends a line to the file at path, which it does
// where it holds a call for ever. What ends a call is then the stand-in, not how
// soon a loaded host runs it. A held call ends every call then in flight, so it
// is for a runtime that makes one call at a time, as a boot probe does.
func ExpireOnHang(d *DockerRuntime, path string) {
	lines := func() int {
		raw, _ := os.ReadFile(path)
		return bytes.Count(raw, []byte("\n"))
	}
	d.callBound = func(ctx context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
		if ctx.Value(hangKey{}) != nil {
			// A call inside a bounded question is ended with it.
			return context.WithCancel(ctx)
		}
		bound, cancel := context.WithTimeout(ctx, hangBound)
		h := &hangContext{Context: bound, done: make(chan struct{})}
		before := lines()
		go func() {
			defer close(h.done)
			for {
				select {
				case <-bound.Done():
					h.end(bound.Err())
					return
				case <-time.After(2 * time.Millisecond):
				}
				if lines() > before {
					h.end(context.DeadlineExceeded)
					return
				}
			}
		}()
		return h, cancel
	}
}

type hangKey struct{}

// hangContext is a context whose deadline passes when ExpireOnHang says so.
type hangContext struct {
	context.Context
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func (h *hangContext) end(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.err = err
}

func (h *hangContext) Done() <-chan struct{} { return h.done }

func (h *hangContext) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *hangContext) Value(key any) any {
	if key == (hangKey{}) {
		return true
	}
	return h.Context.Value(key)
}
