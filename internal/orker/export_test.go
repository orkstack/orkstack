package orker

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

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
