//go:build linux

package orker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openTestGPULock(t *testing.T, dir string) *gpuHostLock {
	t.Helper()
	l, err := openGPULock(dir)
	if err != nil {
		t.Fatalf("openGPULock: %v", err)
	}
	t.Cleanup(l.close)
	return l
}

// TestGPULock_ExclusiveAcrossOpenings: one holder per lock directory; a
// second opening (a second Orker) is refused until the first closes.
func TestGPULock_ExclusiveAcrossOpenings(t *testing.T) {
	dir := t.TempDir()
	first := openTestGPULock(t, dir)
	second := openTestGPULock(t, dir)
	if ok, err := first.tryLock(); !ok || err != nil {
		t.Fatalf("first tryLock = %v, %v", ok, err)
	}
	if ok, err := second.tryLock(); ok || err != nil {
		t.Fatalf("second tryLock while held = %v, %v; want false", ok, err)
	}
	first.close()
	if ok, err := second.tryLock(); !ok || err != nil {
		t.Fatalf("second tryLock after release = %v, %v", ok, err)
	}
}

// TestGPULock_WaitNamesTheHolderThenTakesIt: a waiter is told who holds the
// lock, once, and takes it when the holder lets go.
func TestGPULock_WaitNamesTheHolderThenTakesIt(t *testing.T) {
	dir := t.TempDir()
	holder := openTestGPULock(t, dir)
	if ok, _ := holder.tryLock(); !ok {
		t.Fatal("holder did not take the lock")
	}
	waiter := openTestGPULock(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	told := make(chan lockHolder, 4)
	done := make(chan error, 1)
	go func() { done <- waiter.wait(ctx, 10*time.Millisecond, func(h lockHolder) { told <- h }) }()
	h := <-told
	if len(h.pids) != 1 || h.pids[0] != os.Getpid() {
		t.Fatalf("holder pids = %v, want [%d]", h.pids, os.Getpid())
	}
	comm, _ := os.ReadFile("/proc/self/comm")
	if h.comms[0] != strings.TrimSpace(string(comm)) {
		t.Fatalf("holder comm = %q, want %q", h.comms[0], strings.TrimSpace(string(comm)))
	}
	if !strings.Contains(h.String(), "pid") {
		t.Fatalf("holder detail %q names no pid", h.String())
	}
	holder.close()
	if err := <-done; err != nil {
		t.Fatalf("wait = %v", err)
	}
	if len(told) != 0 {
		t.Fatal("the waiter was told who holds the lock more than once")
	}
}

// TestGPULock_WaitNamesTheHolderOnTheTreesFilesystem: the holder is named
// on the filesystem the tree lies on too, not only the temp directory's. On
// btrfs stat's device is not the one /proc/locks prints, and a lookup by it
// found no line and named nobody. The directory is under the repository's
// .data/, which git ignores, so one a killed test leaves never dirties the
// tree.
func TestGPULock_WaitNamesTheHolderOnTheTreesFilesystem(t *testing.T) {
	root := filepath.Join("..", "..", ".data")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(root, "gpu-lock-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	holder := openTestGPULock(t, dir)
	if ok, _ := holder.tryLock(); !ok {
		t.Fatal("holder did not take the lock")
	}
	waiter := openTestGPULock(t, dir)
	if h := waiter.holder(); len(h.pids) != 1 || h.pids[0] != os.Getpid() {
		t.Fatalf("holder = %v (%v), want [%d]", h.pids, h.err, os.Getpid())
	}
}

// TestHolderFrom_NoLineNamesAHolder: asked while the lock is held, a body
// that lists no holder of its file says the holder cannot be named, never
// an empty answer that reads as nobody.
func TestHolderFrom_NoLineNamesAHolder(t *testing.T) {
	for _, body := range []string{"", "1: FLOCK  ADVISORY  WRITE 4121772 00:59:48444562 0 EOF\n",
		"2: -> FLOCK  ADVISORY  WRITE 77 00:1a:870304 0 EOF\n"} {
		h := holderFrom(strings.NewReader(body), 26, 870304)
		if len(h.pids) != 0 || !errors.Is(h.err, errHolderNotListed) {
			t.Fatalf("holderFrom(%q) = %v, %v; want no pids and errHolderNotListed", body, h.pids, h.err)
		}
		if !strings.Contains(h.String(), "cannot name") {
			t.Fatalf("holderFrom(%q) says %q", body, h.String())
		}
	}
}

// TestGPULock_WaitEndsWithItsContext: a stopping Orker stops waiting.
func TestGPULock_WaitEndsWithItsContext(t *testing.T) {
	dir := t.TempDir()
	holder := openTestGPULock(t, dir)
	if ok, _ := holder.tryLock(); !ok {
		t.Fatal("holder did not take the lock")
	}
	waiter := openTestGPULock(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := waiter.wait(ctx, 5*time.Millisecond, func(lockHolder) {}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait = %v, want the context's error", err)
	}
}

// TestGPULock_CheckFailsClosed: the held fd is the lock; a directory removed
// or recreated under the same path is a mismatch, never a new lock.
func TestGPULock_CheckFailsClosed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gpu-orker")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	l := openTestGPULock(t, dir)
	if ok, _ := l.tryLock(); !ok {
		t.Fatal("not locked")
	}
	if err := l.check(); err != nil {
		t.Fatalf("check on the held directory = %v", err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := l.check(); !errors.Is(err, errHostLockMismatch) {
		t.Fatalf("check after removal = %v, want errHostLockMismatch", err)
	}
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := l.check(); !errors.Is(err, errHostLockMismatch) {
		t.Fatalf("check after recreation = %v, want errHostLockMismatch", err)
	}
}

// TestGPULock_IdentityAndProbeView: the identity is boot-id:dev:inode, and a
// probe's view of the directory matches only the held one.
func TestGPULock_IdentityAndProbeView(t *testing.T) {
	dir := t.TempDir()
	l := openTestGPULock(t, dir)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(l.identity(), ":")
	if len(parts) != 3 || parts[0] != strings.TrimSpace(string(boot)) {
		t.Fatalf("identity %q, want <boot-id>:<dev>:<inode>", l.identity())
	}
	if err := l.checkSeen(l.dev, l.ino); err != nil {
		t.Fatalf("checkSeen of the held directory = %v", err)
	}
	if err := l.checkSeen(l.dev, l.ino+1); !errors.Is(err, errHostLockMismatch) {
		t.Fatalf("checkSeen of another inode = %v, want errHostLockMismatch", err)
	}
}

// TestGPULock_OpenRefusesAFile: the lock is a directory.
func TestGPULock_OpenRefusesAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openGPULock(path); err == nil {
		t.Fatal("openGPULock accepted a regular file")
	}
}
