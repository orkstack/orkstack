//go:build integration && gpuhost

package orker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// lockHolderProcess is another process holding the GPU host lock dir: flock(1)
// takes it on the directory, as an Orker does, and holds it while its child
// reads stdin. Closing stdin ends it as an exit would; Process.Kill is a
// SIGKILL.
type lockHolderProcess struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func holdGPULock(ctx context.Context, t *testing.T, dir string) *lockHolderProcess {
	t.Helper()
	cmd := exec.CommandContext(ctx, "flock", "--exclusive", "--timeout", "30", dir, "cat")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("flock(1) is needed to hold the GPU host lock from another process: %v", err)
	}
	h := &lockHolderProcess{cmd: cmd, stdin: stdin}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	// Held once a lock of our own on the directory is refused; a flock that
	// could not take it has exited.
	for !lockRefused(t, dir) {
		if h.exited() {
			t.Fatalf("flock(1) could not take %s within 30s: another process holds the GPU host lock", dir)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("flock(1) never took %s", dir)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return h
}

// lockRefused reports whether another process holds the GPU host lock on
// dir: a lock of this process's own is refused. One it took is released.
func lockRefused(t *testing.T, dir string) bool {
	t.Helper()
	l, err := openGPULock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	held, err := l.tryLock()
	if err != nil {
		t.Fatal(err)
	}
	return !held
}

func (h *lockHolderProcess) exited() bool {
	return h.cmd.Process.Signal(syscall.Signal(0)) != nil
}

// lockIdentityOf is the identity a lock taken on dir has: the same for every
// opener, so a container labelled with it is bound under that lock.
func lockIdentityOf(t *testing.T, dir string) string {
	t.Helper()
	l, err := openGPULock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	return l.identity()
}

// removeFailing is the real runtime with the removal of one container made
// to fail, a fault the daemon does not produce on demand.
type removeFailing struct {
	*DockerRuntime
	fail string // a container id; matched by prefix, as the reap lists short ids
}

func (r *removeFailing) Remove(ctx context.Context, id ContainerID) error {
	if r.fail != "" && strings.HasPrefix(r.fail, string(id)) {
		return errors.New("injected: the daemon refused to remove the container")
	}
	return r.DockerRuntime.Remove(ctx, id)
}

// TestIntegrationGPU_ReapAndSeedUnderLock: an Orker SIGKILLed while its unit
// containers run leaves them, and the next Orker of the host, once it takes
// the same GPU host lock, removes every container bound under it. A removal
// that fails keeps its container and seeds exactly its unit, so the next
// task is bound the other unit; a container bound under another lock is
// left alone. The killed Orker is flock(1) holding the real lock directory;
// the failing removal is injected.
func TestIntegrationGPU_ReapAndSeedUnderLock(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const lockDir = "/run/orkstack/gpu-orker"

	holder := holdGPULock(ctx, t, lockDir)
	identity := lockIdentityOf(t, lockDir)
	dead := proof.OrkerID("gpuhost-killed")
	stuck := unitContainer(ctx, t, spec, dead, identity, "amd", 0)
	removable := unitContainer(ctx, t, spec, dead, identity, "intel", 1)
	other := unitContainer(ctx, t, spec, dead, "another-boot:1:2", "intel", 1)
	if err := holder.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.cmd.Wait()

	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-reap",
		runtime: func(d *DockerRuntime) ContainerRuntime { return &removeFailing{DockerRuntime: d, fail: stuck} }})
	if got := g.lockIdentity(t); got != identity {
		t.Fatalf("the new Orker holds lock %s; the killed one held %s", got, identity)
	}
	if state := containerState(ctx, t, removable); state != "" {
		t.Fatalf("the container bound under the held lock is %s; the reap removes it", state)
	}
	if state := containerState(ctx, t, stuck); state != "running" {
		t.Fatalf("the container whose removal failed is %q; it is left running", state)
	}
	if state := containerState(ctx, t, other); state != "running" {
		t.Fatalf("the container bound under another lock is %q; the reap never touches it", state)
	}
	g.c.mu.Lock()
	var seeded []int
	for id, hold := range g.c.unremoved {
		if strings.HasPrefix(stuck, string(id)) {
			seeded = hold.units
		}
	}
	g.c.mu.Unlock()
	if !slices.Equal(seeded, []int{0}) {
		t.Fatalf("the container whose removal failed seeds units %v; want exactly its unit 0", seeded)
	}

	next, done := g.poll(t, g.task(t, "next", 1))
	defer done()
	if err := g.c.bind(next); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if got := next.Units(); !slices.Equal(got, []int{1}) {
		t.Fatalf("the next task was bound %v; want unit 1, unit 0 being held by the container not removed", got)
	}
}

// TestIntegrationGPU_HostLockSecondOrkerWaits: while another process of the
// host holds the GPU host lock, an Orker boots with its gpu profile
// unavailable (GPU_HOST_LOCK_HELD) naming that process, offers no unit and
// waits; once the holder exits it takes the lock and offers its units. The
// first Orker is flock(1) holding the real lock directory, because an Orker
// process cannot boot without Temporal; the waiting Orker is the real gpu
// boot and its wait. The progress control is the holder alive, and the lock
// refused to a fresh opener, through every check of the wait.
func TestIntegrationGPU_HostLockSecondOrkerWaits(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const lockDir = "/run/orkstack/gpu-orker"
	const held = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_GPU_HOST_LOCK_HELD

	holder := holdGPULock(ctx, t, lockDir)
	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-wait", waitForLock: true})
	if g.d.gpu.Load() != nil {
		t.Fatal("the Orker offered its units while another process held the GPU host lock")
	}
	if !g.s.unavailableFor(gpuHostProfile, held) {
		t.Fatal("the gpu profile is not unavailable GPU_HOST_LOCK_HELD while another process holds the lock")
	}
	if who := g.lockHolder(); !strings.Contains(who, fmt.Sprint(holder.cmd.Process.Pid)) {
		t.Fatalf("the lock is said to be %q; want it to name the holder, pid %d", who, holder.cmd.Process.Pid)
	}

	waitCtx, stopWait := context.WithCancel(ctx)
	defer stopWait()
	waited := make(chan struct{})
	go func() {
		defer close(waited)
		g.a.awaitGPULock(waitCtx, 50*time.Millisecond)
	}()
	for range 10 {
		select {
		case <-waited:
			t.Fatal("the wait ended while the holder held the lock")
		case <-time.After(100 * time.Millisecond):
		}
		if holder.exited() {
			t.Fatal("the holder exited during the window; nothing shows the wait held back")
		}
		if !lockRefused(t, lockDir) {
			t.Fatal("the lock was free to a fresh opener during the window; nothing shows it was held")
		}
		if g.d.gpu.Load() != nil || !g.s.unavailableFor(gpuHostProfile, held) {
			t.Fatal("the waiting Orker offered its units while the holder held the lock")
		}
	}

	_ = holder.stdin.Close()
	_ = holder.cmd.Wait()
	select {
	case <-waited:
	case <-ctx.Done():
		t.Fatal("the Orker never took the GPU host lock after its holder exited")
	}
	if g.d.gpu.Load() == nil {
		t.Fatal("the Orker took the lock and offers no units")
	}
	if g.s.unavailableFor(gpuHostProfile, held) {
		t.Fatal("GPU_HOST_LOCK_HELD still stands once the Orker holds the lock")
	}
}

// TestIntegrationGPU_LockRecreatedWhileForeignUnitsRun: an Orker exits while
// a container bound to its unit runs, and the lock directory is then made
// anew (a reboot's tmpfiles, a hand recreating /run), so the next Orker
// holds a lock of another identity. That container's units may still be in
// use, so the profile is held FOREIGN_UNITS_HELD, the container untouched,
// until it is gone; then the hold ends. Recreating /run/orkstack/gpu-orker
// needs root, so this test's lock directory is one of its own
// (gpuLockDirPath), made anew with a rename so it cannot reuse the old
// inode; the container is bound to the real unit through the real spec.
func TestIntegrationGPU_LockRecreatedWhileForeignUnitsRun(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const foreign = orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_FOREIGN_UNITS_HELD
	dir := filepath.Join(t.TempDir(), "gpu-orker")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	first := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-recreate-a", lockDir: dir})
	running := unitContainer(ctx, t, spec, first.orkerID, first.lockIdentity(t), "amd", 0)
	before := first.lockIdentity(t)
	first.close()

	// The old directory moves aside (rename(2) will not replace a directory
	// that is not empty), and a new one takes its name.
	if err := os.Rename(dir, dir+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	second := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-recreate-b", lockDir: dir})
	if second.lockIdentity(t) == before {
		t.Fatal("the recreated lock directory has the old identity; the test needs a new one")
	}
	offer := second.d.gpu.Load()
	for i := range 3 {
		if state := containerState(ctx, t, running); state != "running" {
			t.Fatalf("check %d: the container bound under the old lock is %q; it is never touched", i, state)
		}
		if !second.s.unavailableFor(gpuHostProfile, foreign) {
			t.Fatalf("check %d: the profile is not held FOREIGN_UNITS_HELD while a container of the old lock runs", i)
		}
		second.a.recheckForeignUnits(ctx, offer)
	}

	gpuHostDocker(ctx, t, "rm", "--force", running)
	second.a.recheckForeignUnits(ctx, offer)
	if second.s.unavailableFor(gpuHostProfile, foreign) {
		t.Fatal("FOREIGN_UNITS_HELD still stands once the container of the old lock is gone")
	}
}
