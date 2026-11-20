// Package cdidind starts a rootful Docker-in-Docker for the CDI tests of
// E91's NVIDIA profiles (design §7.4): a dockerd of the release's pinned
// version, with CDI on, in which a real nvidia-cdi-hook runs as a CDI spec's
// hook without a GPU, over nodes that are 1:3 (/dev/null) under NVIDIA's
// names.
//
// It is test infrastructure only, never part of an Orker or of a proof's
// activity containers (AGENTS.md §6). The dind is privileged, because a
// dockerd needs it, and it sees the host's /dev; it is given no host mount
// and never the host's Docker socket, and it is removed by its own id when
// the test ends. It carries the proofs' own labels (orkstack.proof and
// orkstack.proof.owner), so `proofrun clean` removes one a dead process left.
//
// NVIDIA ships nvidia-cdi-hook as a glibc binary, and docker:dind is Alpine
// (musl), where it does not execute (gcompat is not enough: it fails on the
// hook's NVML symbols). So Start copies glibc's loader and the four libraries
// the hook links (libc, libdl, libpthread, libresolv) into the dind from a
// pinned glibc image (GlibcImage), at the paths a Debian loader reads, and
// the hook then runs as the spec names it, with no wrapper. Nothing on the
// host changes.
//
// Every image is pinned by digest and never pulled here: a host without one
// is a missing prerequisite (skip), which the proof host's setup provides.
package cdidind

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/dextest"
)

// DindImage is the dind the release's privileged Orker manifest pins
// (deploy/kubernetes/orker-privileged/orker.yaml), held to it by
// TestDindImage_IsTheManifestsPin.
const DindImage = "docker:29.9.3-dind@sha256:7bf70dcb85c822e3fa6f2d332143e474b99147b9d7b39102b0cfe8b04a7dd61b"

// GlibcImage is the glibc the hook is given inside the musl dind: distroless
// base on Debian 13, the generation of the release images' own distroless
// bases.
const GlibcImage = "gcr.io/distroless/base-debian13:nonroot@sha256:a08d9054afbaf1af9b90fd02cdc7b32236319b5376d374505a41548d42b0c5db"

// WorkloadImage is the container the tests run under a CDI device: the
// Orker's own pinned boot-probe base.
const WorkloadImage = "docker.io/library/alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf"

// HookPath is where the hook is installed, as NVIDIA's packages install it
// and its generated specs name it.
const HookPath = "/usr/bin/nvidia-cdi-hook"

// glibcFile is one object copied from GlibcImage: from its path there, to
// its path in the dind.
type glibcFile struct{ from, to string }

// glibcFiles are the loader at the path the hook's ELF names as its
// interpreter, and the libraries at the paths that loader searches, for arch
// (runtime.GOARCH's spelling).
func glibcFiles(arch string) ([]glibcFile, error) {
	var triplet, loader, interp string
	switch arch {
	case "amd64":
		triplet, loader, interp = "x86_64-linux-gnu", "ld-linux-x86-64.so.2", "/lib64/ld-linux-x86-64.so.2"
	case "arm64":
		triplet, loader, interp = "aarch64-linux-gnu", "ld-linux-aarch64.so.1", "/lib/ld-linux-aarch64.so.1"
	default:
		return nil, fmt.Errorf("cdidind: no glibc layout for %s", arch)
	}
	dir := "/usr/lib/" + triplet
	files := []glibcFile{{from: path.Join(dir, loader), to: interp}}
	for _, lib := range []string{"libc.so.6", "libdl.so.2", "libpthread.so.0", "libresolv.so.2"} {
		files = append(files, glibcFile{from: path.Join(dir, lib), to: path.Join(dir, lib)})
	}
	return files, nil
}

// runArgs is the `docker` command line that starts the dind: privileged,
// never pulling, with no mount, no published port and no network, labelled
// for proofrun's cleanup. With DOCKER_TLS_CERTDIR empty the dind's dockerd
// also listens on an unauthenticated TCP port, which --network none leaves
// nothing to reach: every call to it is `docker exec`, `docker cp` or a
// `docker load` through exec.
func runArgs(proofID, owner string) []string {
	args := []string{"run", "-d", "--privileged", "--pull", "never", "--network", "none",
		"-e", "DOCKER_TLS_CERTDIR=",
		"--label", "orkstack.test=cdi-dind", "--label", "orkstack.proof=" + proofID}
	if owner != "" {
		args = append(args, "--label", "orkstack.proof.owner="+owner)
	}
	return append(args, DindImage)
}

// Dind is one started dind.
type Dind struct {
	t  testing.TB
	id string
}

// ProofID is the proof this process belongs to, as the other suites name it.
func ProofID() string {
	if id := os.Getenv("ORK_PROOF_ID"); id != "" {
		return id
	}
	return fmt.Sprintf("test-%d", os.Getpid())
}

// Start starts a dind, waits (bounded) for its dockerd, gives it glibc and
// loads WorkloadImage into it, and removes it by its id at test end. skip is
// the suite's infraAbsent: Docker or a pinned image missing from the host.
func Start(t testing.TB, skip func(format string, args ...any)) *Dind {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		skip("the docker CLI is unavailable, so no dind can start: %v", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, image := range []string{DindImage, GlibcImage, WorkloadImage} {
		if out, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", image).CombinedOutput(); err != nil {
			skip("the image %s is not on this host: %v: %s; the proof host's setup pulls it", image, err, strings.TrimSpace(string(out)))
			return nil
		}
	}
	cmd := exec.CommandContext(ctx, "docker", runArgs(ProofID(), dextest.ProcessOwner())...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		skip("cannot start a dind from %s: %v: %s", DindImage, err, stderr.String())
		return nil
	}
	d := &Dind{t: t, id: strings.TrimSpace(string(out))}
	// -v: the dind's anonymous /var/lib/docker volume goes with it.
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", d.id).Run() })
	d.waitReady(ctx)
	d.installGlibc(ctx)
	d.loadWorkload(ctx)
	return d
}

// ID is the dind's container id.
func (d *Dind) ID() string { return d.id }

func (d *Dind) waitReady(ctx context.Context) {
	d.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		try, cancel := context.WithTimeout(ctx, 10*time.Second)
		ok := exec.CommandContext(try, "docker", "exec", d.id, "docker", "info").Run() == nil
		cancel()
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			d.t.Fatalf("the dind's dockerd: %v", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	d.t.Fatalf("the dind's dockerd did not answer within 90s")
}

// installGlibc copies glibcFiles out of GlibcImage, through a created (never
// started) container, into the dind.
func (d *Dind) installGlibc(ctx context.Context) {
	d.t.Helper()
	files, err := glibcFiles(runtime.GOARCH)
	if err != nil {
		d.t.Fatal(err)
	}
	out, err := exec.CommandContext(ctx, "docker", "create", "--pull", "never", GlibcImage, "/none").CombinedOutput()
	if err != nil {
		d.t.Fatalf("create a container of %s: %v: %s", GlibcImage, err, out)
	}
	src := strings.TrimSpace(string(out))
	defer func() { _ = exec.Command("docker", "rm", "-f", src).Run() }()
	dir := d.t.TempDir()
	for i, f := range files {
		local := filepath.Join(dir, fmt.Sprint(i))
		if out, err := exec.CommandContext(ctx, "docker", "cp", "-L", src+":"+f.from, local).CombinedOutput(); err != nil {
			d.t.Fatalf("copy %s out of %s: %v: %s", f.from, GlibcImage, err, out)
		}
		d.Exec("mkdir -p " + path.Dir(f.to))
		d.copyIn(ctx, local, f.to)
		d.Exec("chown root:root " + f.to + " && chmod 0755 " + f.to)
	}
}

// loadWorkload gives the dind WorkloadImage from the host's own store, so
// the dind pulls nothing.
func (d *Dind) loadWorkload(ctx context.Context) {
	d.t.Helper()
	save := exec.CommandContext(ctx, "docker", "save", WorkloadImage)
	load := exec.CommandContext(ctx, "docker", "exec", "-i", d.id, "docker", "load", "-q")
	pipe, err := save.StdoutPipe()
	if err != nil {
		d.t.Fatal(err)
	}
	load.Stdin = pipe
	if err := save.Start(); err != nil {
		d.t.Fatalf("docker save %s: %v", WorkloadImage, err)
	}
	if out, err := load.CombinedOutput(); err != nil {
		d.t.Fatalf("load %s into the dind: %v: %s", WorkloadImage, err, out)
	}
	if err := save.Wait(); err != nil {
		d.t.Fatalf("docker save %s: %v", WorkloadImage, err)
	}
}

func (d *Dind) copyIn(ctx context.Context, local, to string) {
	d.t.Helper()
	if out, err := exec.CommandContext(ctx, "docker", "cp", local, d.id+":"+to).CombinedOutput(); err != nil {
		d.t.Fatalf("copy %s into the dind at %s: %v: %s", local, to, err, out)
	}
}

// callTimeout bounds every call into the dind, so a hung daemon fails the
// test rather than holding a privileged container until the test binary's
// own timeout.
const callTimeout = 2 * time.Minute

// Exec runs script in the dind's shell and fails the test unless it exits 0.
func (d *Dind) Exec(script string) string {
	d.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "exec", d.id, "sh", "-c", script).CombinedOutput()
	if err != nil {
		d.t.Fatalf("in the dind, %q: %v: %s", script, err, out)
	}
	return string(out)
}

// InstallHook installs the hook binary at hostPath as HookPath, root-owned
// and 0755, as NVIDIA's packages do.
func (d *Dind) InstallHook(hostPath string) {
	d.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	d.copyIn(ctx, hostPath, HookPath)
	d.Exec("chown root:root " + HookPath + " && chmod 0755 " + HookPath)
}

// MakeNodes makes each name a character node 1:3 under /dev, the inert stand
// in for NVIDIA's nodes.
func (d *Dind) MakeNodes(names ...string) {
	d.t.Helper()
	for _, n := range names {
		if strings.ContainsAny(n, "/ ") || n == "" {
			d.t.Fatalf("cdidind: %q is not a node name", n)
		}
		d.Exec(fmt.Sprintf("rm -f /dev/%s && mknod /dev/%s c 1 3", n, n))
	}
}

// WriteSpec writes spec as /etc/cdi/<name>, root-owned and 0644, in a
// root-owned /etc/cdi.
func (d *Dind) WriteSpec(name, spec string) {
	d.t.Helper()
	if strings.ContainsAny(name, "/ ") || !strings.HasSuffix(name, ".yaml") {
		d.t.Fatalf("cdidind: %q is not a spec file name", name)
	}
	local := filepath.Join(d.t.TempDir(), name)
	if err := os.WriteFile(local, []byte(spec), 0o644); err != nil {
		d.t.Fatal(err)
	}
	d.Exec("mkdir -p /etc/cdi && chown root:root /etc/cdi && chmod 0755 /etc/cdi")
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	d.copyIn(ctx, local, "/etc/cdi/"+name)
	d.Exec("chown root:root /etc/cdi/" + name + " && chmod 0644 /etc/cdi/" + name)
}

// Run runs WorkloadImage in the dind's dockerd with the CDI devices named
// and the command given, and returns its combined output and how it ended.
func (d *Dind) Run(devices []string, command string) (string, error) {
	d.t.Helper()
	args := []string{"exec", d.id, "docker", "run", "--rm"}
	for _, dev := range devices {
		args = append(args, "--device", dev)
	}
	args = append(args, WorkloadImage, "sh", "-c", command)
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return string(out), err
}
