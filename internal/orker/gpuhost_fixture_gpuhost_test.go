//go:build integration && gpuhost

package orker

// The GPU-host tests (scripts/proof-gpu-tests.txt): each drives this host's
// real GPUs through the real Docker daemon, the CDI spec
// scripts/gpu-host-setup.sh installs in /etc/cdi (kind
// test.orkstack.dev/gpu-<hash>, units amd and intel), the real GPU host lock
// directory /run/orkstack/gpu-orker, and the real render nodes. They never
// skip: the GPU-host tier's preflight exits 77 before they run on a host
// that is not set up, so a test that finds a prerequisite missing fails,
// naming what to run. They touch the GPUs only through the unit probe's
// driver-version ioctls and opens of a render node, never with compute.
//
// They are internal tests because what they prove is the Orker's own GPU
// machinery — the ledger, the bind at dispatch, the GPU host lock, the
// lock-scoped reap and the unit probes — and a GPU Orker cannot boot
// without Temporal. Each builds the Orker's gpu half the way Boot does
// (newGPUOrker) and runs tasks through the real dispatcher.

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// gpuHostProfile is the gpu profile these tests declare over the test spec.
const gpuHostProfile = "gpu"

// gpuHostTaskImage is the image a GPU-host test's tasks run, pinned to the
// digest the host has (gpuHostPinnedImage).
const gpuHostTaskImage = "docker.io/library/alpine:3.24"

// gpuHostSetupHint is what a test that finds the host not set up says to do.
const gpuHostSetupHint = "run scripts/gpu-host-setup.sh and the root steps it prints (docs/PROOFS.md, \"The GPU-host tier\")"

// gpuHostSpec is the test spec this tree's gpu-host-setup.sh installs: its
// path in /etc/cdi and its CDI kind.
type gpuHostSpec struct {
	path, kind string
}

// requireGPUHost fails unless this host is set up as the GPU host: Docker
// answers, the tree's test spec is installed and the GPU host lock directory
// exists. It returns the spec.
func requireGPUHost(t *testing.T) gpuHostSpec {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, gpuHostCLI(t), "version", "--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		t.Fatalf("the Docker daemon does not answer: %v: %s", err, out)
	}
	root := gpuHostRepoRoot(t)
	out, err := exec.CommandContext(ctx, filepath.Join(root, "scripts", "gpu-host-setup.sh"), "--name").Output()
	if err != nil {
		t.Fatalf("scripts/gpu-host-setup.sh --name: %v; %s", err, gpuHostSetupHint)
	}
	name := strings.TrimSpace(string(out))
	hash, ok := strings.CutPrefix(strings.TrimSuffix(name, ".yaml"), "orkstack-test-gpu-")
	if !ok || hash == "" {
		t.Fatalf("scripts/gpu-host-setup.sh --name printed %q, not orkstack-test-gpu-<hash>.yaml", name)
	}
	spec := gpuHostSpec{path: filepath.Join("/etc/cdi", name), kind: "test.orkstack.dev/gpu-" + hash}
	want, err := exec.CommandContext(ctx, filepath.Join(root, "scripts", "gpu-host-setup.sh"), "--print").Output()
	if err != nil {
		t.Fatalf("scripts/gpu-host-setup.sh --print: %v", err)
	}
	have, err := os.ReadFile(spec.path)
	if err != nil || !bytes.Equal(have, want) {
		t.Fatalf("%s is not this tree's test spec (%v); %s", spec.path, err, gpuHostSetupHint)
	}
	if info, err := os.Stat(gpuLockDirPath); err != nil || !info.IsDir() {
		t.Fatalf("the GPU host lock directory %s is not there (%v); %s", gpuLockDirPath, err, gpuHostSetupHint)
	}
	return spec
}

func gpuHostRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func gpuHostCLI(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("docker")
	if err != nil {
		t.Fatalf("docker is not on PATH: %v", err)
	}
	return path
}

// gpuHostDocker runs the docker CLI and returns its trimmed stdout, failing
// the test on an error.
func gpuHostDocker(ctx context.Context, t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, gpuHostCLI(t), args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// gpuHostPinnedImage is the task image pinned to the digest this host has,
// pulled first when it has none.
var gpuHostPinnedImage = sync.OnceValues(func() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "image", "inspect", gpuHostTaskImage).Run(); err != nil {
		if out, err := exec.CommandContext(ctx, "docker", "pull", "--quiet", gpuHostTaskImage).CombinedOutput(); err != nil {
			return "", fmt.Errorf("pull %s: %v: %s", gpuHostTaskImage, err, out)
		}
	}
	out, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", gpuHostTaskImage).Output()
	if err != nil {
		return "", fmt.Errorf("%s has no repository digest: %v", gpuHostTaskImage, err)
	}
	return strings.TrimSpace(string(out)), nil
})

func gpuHostImage(t *testing.T) string {
	t.Helper()
	pinned, err := gpuHostPinnedImage()
	if err != nil {
		t.Fatal(err)
	}
	return pinned
}

// gpuHostWorkRoot is a work root of this test's own under the repository's
// .data/work, which the daemon can bind.
func gpuHostWorkRoot(t *testing.T) string {
	t.Helper()
	parent := os.Getenv(WorkRootVar)
	if parent == "" {
		parent = filepath.Join(gpuHostRepoRoot(t), ".data", "work")
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(parent, proof.WorkRootPattern("ork-gpuhost"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

// gpuOrkerOptions is how newGPUOrker builds one Orker's gpu half.
type gpuOrkerOptions struct {
	units   []string // the profile's units; default amd and intel
	maxGPU  int64    // orker.resources.max_gpu; default 1
	slots   int      // max_concurrent; default 2
	lockDir string   // the GPU host lock directory; default /run/orkstack/gpu-orker
	// runtime wraps the real DockerRuntime, for a test that injects a fault
	// the daemon cannot produce on demand; nil runs the real one.
	runtime func(*DockerRuntime) ContainerRuntime
	// inspect is the dispatcher's device inspector; nil is the host's.
	inspect func(string) (hostDevice, error)
	// waitForLock: the lock may be held by another process, so boot does
	// not require the units offered.
	waitForLock bool
	label       string // the Orker identity's base; default the test's name
}

// gpuOrker is one Orker's gpu half, built as Boot builds it: the profile
// audited against this host's /etc/cdi, the ledger with its units, and the
// GPU host lock taken by bootGPU, which runs the lock-scoped reap.
type gpuOrker struct {
	a        *Activities
	d        *OCIDispatcher
	c        *Capacity
	s        *profileState
	spec     gpuHostSpec
	identity string // the profile's structural identity, as a run pins it
	orkerID  string
}

func newGPUOrker(ctx context.Context, t *testing.T, spec gpuHostSpec, o gpuOrkerOptions) *gpuOrker {
	t.Helper()
	if o.units == nil {
		o.units = []string{"amd", "intel"}
	}
	if o.maxGPU == 0 {
		o.maxGPU = 1
	}
	if o.slots == 0 {
		o.slots = 2
	}
	if o.lockDir == "" {
		o.lockDir = "/run/orkstack/gpu-orker"
	}
	if o.label == "" {
		o.label = "gpuhost"
	}
	old := gpuLockDirPath
	gpuLockDirPath = o.lockDir
	t.Cleanup(func() { gpuLockDirPath = old })

	declared := Profiles{List: []Profile{{Name: gpuHostProfile, Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
		GPU: &GPUProfile{CDIKind: spec.kind, Units: o.units}}}}
	s := newProfileState(declared, "gpuhost-test")
	s.useCDI(DefaultCDISpecDirs, runtimeSocketPath())
	if err := s.bootAuditProfiles(); err != nil {
		t.Fatalf("the boot audit of %s refused the test spec: %v", spec.path, err)
	}
	audit, ok := s.audit(gpuHostProfile)
	if !ok || audit.gpu == nil {
		t.Fatalf("the boot audit found no spec of kind %s in %v; %s", spec.kind, DefaultCDISpecDirs, gpuHostSetupHint)
	}

	policy := DefaultSecurityPolicy()
	policy.MaxGPU = o.maxGPU
	workRoot := gpuHostWorkRoot(t)
	policy = policy.WithWorkRoot(workRoot)
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var rt ContainerRuntime = NewDockerRuntime()
	if o.runtime != nil {
		rt = o.runtime(NewDockerRuntime())
	}
	orkerID := proof.OrkerID(o.label)
	d := &OCIDispatcher{
		Runtime:       rt,
		Store:         store,
		Commits:       artifacts.NewMemoryCommitStore(),
		Refs:          artifacts.MemoryRefPredictor{},
		Policy:        policy,
		WorkRoot:      workRoot,
		OrkerID:       orkerID,
		BuildID:       "gpuhost-test",
		profiles:      s,
		inspectDevice: o.inspect,
	}
	ceiling, err := policy.ceiling()
	if err != nil {
		t.Fatal(err)
	}
	c := newCapacity(amount{}, ceiling.cpuMemory(), o.slots)
	c.useUnits(gpuHostProfile, len(o.units), int(o.maxGPU), unitHealthReporter(s, gpuHostProfile, time.Now))
	a := &Activities{dispatcher: d, profiles: s, capacity: c}
	if err := a.bootGPU(ctx); err != nil {
		t.Fatalf("bootGPU: %v", err)
	}
	g := &gpuOrker{a: a, d: d, c: c, s: s, spec: spec, identity: audit.identity, orkerID: orkerID}
	t.Cleanup(g.close)
	if !o.waitForLock && d.gpu.Load() == nil {
		t.Fatalf("the GPU host lock %s is held by another process: %s", o.lockDir, g.lockHolder())
	}
	t.Cleanup(func() { removeLabelled(t, "ork.orker="+orkerID) })
	return g
}

// close releases the GPU host lock this Orker holds or waits on, as its
// process's exit would.
func (g *gpuOrker) close() {
	if offer := g.d.gpu.Load(); offer != nil {
		if l, ok := offer.lock.(*gpuHostLock); ok {
			l.close()
		}
	}
	if w := g.a.gpuWait; w != nil && w.lock != nil {
		w.lock.close()
	}
}

func (g *gpuOrker) lockHolder() string {
	if w := g.a.gpuWait; w != nil && w.lock != nil {
		return w.lock.holder().String()
	}
	return "unknown"
}

func (g *gpuOrker) lockIdentity(t *testing.T) string {
	t.Helper()
	offer := g.d.gpu.Load()
	if offer == nil {
		t.Fatal("this Orker offers no units: it does not hold the GPU host lock")
	}
	return offer.lock.identity()
}

// task is a task of the gpu profile asking for n units, running command in
// the pinned task image.
func (g *gpuOrker) task(t *testing.T, id string, n int32, command ...string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	pinned := gpuHostImage(t)
	_, digest, _ := strings.Cut(pinned, "@")
	runID := fmt.Sprintf("ork-gpuhost-%s-%d", id, time.Now().UnixNano())
	return &orkstackv1.ExecuteTaskInput{
		RunId: runID, OrkflowName: "gpuhost", OrkflowVersion: "1.0.0", Namespace: "default", Pool: "main",
		TaskId: id, IdempotencyKey: "default/" + runID + "/" + id,
		Image:                          &orkstackv1.ImageRef{Reference: gpuHostTaskImage, Pinned: pinned, IndexDigest: digest},
		Command:                        command,
		TimeoutSeconds:                 120,
		CancellationGracePeriodSeconds: 2,
		Network:                        orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		Devices:                        []*orkstackv1.ProfileRef{{Name: gpuHostProfile, IdentityDigest: g.identity}},
		Resources:                      &orkstackv1.ResourceRequest{Gpu: n},
		CommitUri:                      "mem://commits/" + runID + "/" + id + "/commit.json",
		Limits: &orkstackv1.WorkspaceLimits{
			ValuesMaxBytes: 262144, ArtifactMaxCount: 256, ArtifactMaxBytes: 1 << 30,
			LogMaxBytes: 16 << 20, CommitMaxBytes: 4 << 20,
		},
	}
}

// poll is a polled task's admission, as the cordon interceptor makes it:
// the poll granted, then trimmed to the task's resolved request. done ends
// the activity and releases the poll's permit, as the SDK does.
func (g *gpuOrker) poll(t *testing.T, in *orkstackv1.ExecuteTaskInput) (*Admission, func()) {
	t.Helper()
	a, release := pollTask(t, g.c, amountFromProto(g.d.Policy.ReservedResources(in.GetResources())))
	var once sync.Once
	return a, func() { once.Do(func() { a.Done(); release() }) }
}

// run is one polled attempt of in through the real dispatcher, under a.
func (g *gpuOrker) run(ctx context.Context, t *testing.T, a *Admission, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	t.Helper()
	return g.d.Execute(withAdmission(ctx, a), in)
}

// gpuValues is a task's values.json as the shell tasks here write it.
func gpuValues(t *testing.T, result *orkstackv1.ExecuteTaskResult, err error) map[string]any {
	t.Helper()
	if err != nil {
		t.Fatalf("the task failed: %v", err)
	}
	var v map[string]any
	if err := json.Unmarshal(result.GetValuesJson(), &v); err != nil {
		t.Fatalf("values.json %q: %v", result.GetValuesJson(), err)
	}
	return v
}

// renderNodesTask writes the render nodes the container sees, and whether it
// sees /dev/kfd, to values.json, after holding the units for hold seconds.
func renderNodesTask(hold int) []string {
	return []string{"/bin/sh", "-c", fmt.Sprintf(`sleep %d; `+
		`nodes=$(cd /dev/dri 2>/dev/null && ls -1 | tr '\n' ' '); `+
		`kfd=false; [ -c /dev/kfd ] && kfd=true; `+
		`printf '{"nodes":"%%s","kfd":%%s}' "$nodes" "$kfd" > /orkstack/output/values.json`, hold)}
}

// unitContainer starts a container bound to the unit named, labelled as a
// dispatched task's would be under lock (its identity) with the indices
// given, sleeping until it is removed. The test removes it at cleanup.
func unitContainer(ctx context.Context, t *testing.T, spec gpuHostSpec, orkerID, lock, unit string, index int) string {
	t.Helper()
	id := gpuHostDocker(ctx, t, "run", "--detach", "--network", "none", "--pids-limit", "16",
		"--label", "ork.orker="+orkerID,
		"--label", labelUnitsLock+"="+lock,
		"--label", labelUnits+"="+unitsLabel(gpuHostProfile, []int{index}),
		"--device", spec.kind+"="+unit,
		gpuHostImage(t), "sleep", "600")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "--force", id).Run() })
	return id
}

// containerState is the daemon's state word for a container, or "" when it
// is gone.
func containerState(ctx context.Context, t *testing.T, id string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, gpuHostCLI(t), "container", "inspect", "--format", "{{.State.Status}}", id).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// labelled is the ids of the containers carrying label.
func labelled(ctx context.Context, t *testing.T, label string) []string {
	t.Helper()
	out := gpuHostDocker(ctx, t, "ps", "--all", "--quiet", "--filter", "label="+label)
	return strings.Fields(out)
}

// removeLabelled removes every container this test labelled.
func removeLabelled(t *testing.T, label string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, _ := exec.CommandContext(ctx, "docker", "ps", "--all", "--quiet", "--filter", "label="+label).Output()
	for _, id := range strings.Fields(string(out)) {
		_ = exec.CommandContext(ctx, "docker", "rm", "--force", id).Run()
	}
}

// gpuProbeImage is the orker image of this test binary's tree, built locally
// for the unit probe: the probe runs `orker probe unit` in it. It is tagged
// with this process's own token and removed when the process's tests end.
var gpuProbeImageOnce struct {
	sync.Mutex
	image string
}

func gpuProbeImage(ctx context.Context, t *testing.T) string {
	t.Helper()
	gpuProbeImageOnce.Lock()
	defer gpuProbeImageOnce.Unlock()
	if gpuProbeImageOnce.image != "" {
		return gpuProbeImageOnce.image
	}
	root := gpuHostRepoRoot(t)
	orkerBin := testbin.StaticPath(t, "./cmd/orker")
	contextDir := t.TempDir()
	license, err := os.ReadFile(filepath.Join(root, "LICENSE"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contextDir, "LICENSE"), license, 0o644); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(contextDir, "dist", "bin", "linux-"+runtime.GOARCH)
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(orkerBin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "orker"), binary, 0o755); err != nil {
		t.Fatal(err)
	}
	tag := fmt.Sprintf("orkstack-test/gpu-probe:p%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	gpuHostDocker(ctx, t, "build", "--quiet", "--provenance=false", "-f", filepath.Join(root, "deploy", "Dockerfile"),
		"--target", "orker", "--build-arg", "BUILD=gpuhost-test", "-t", tag, contextDir)
	gpuProbeImageOnce.image = tag
	return tag
}

// probeOnce is one probe of unit i, as probeUnits makes it: the unit held
// under the ledger, the real `orker probe unit` run in image over the
// unit's audited nodes, and its health flipped with endProbe. It returns
// whether the probe found the unit healthy.
func (g *gpuOrker) probeOnce(ctx context.Context, t *testing.T, image string, i int) (bool, string) {
	t.Helper()
	offer := g.d.gpu.Load()
	audit, _ := g.s.audit(gpuHostProfile)
	probe, err := g.c.probeUnit(ctx, i)
	if err != nil {
		t.Fatalf("probeUnit(%d): %v", i, err)
	}
	healthy, detail, err := g.d.probeGPUUnit(ctx, g.d.Runtime.(bootProber), g.d.WorkRoot, image, offer, i, audit.unitNodes[i])
	if err != nil {
		g.c.abandonProbe(probe)
		t.Fatalf("probe unit %d: %v", i, err)
	}
	g.c.endProbe(probe, healthy)
	return healthy, detail
}
