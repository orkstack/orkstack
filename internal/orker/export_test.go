package orker

import (
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

// SetSignalProbeGrace shortens the boot probe's SIGTERM grace for the
// integration tests' Orkers, which live in package orker_test.
func SetSignalProbeGrace(a *Activities, grace time.Duration) {
	a.dispatcher.signalProbeGrace = grace
}
