package orker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// identityProbeRuntime is a runtime whose probe container reads what the
// bind at /orkstack/identity holds on the host, as a daemon that sees the
// host's directory does; blind makes it a remote runtime that sees none.
// A fake, because a daemon that binds another host's directory cannot be
// produced on demand.
type identityProbeRuntime struct {
	layoutProbeRuntime
	blind bool
	specs []ContainerSpec
}

func (r *identityProbeRuntime) RunBootProbe(_ context.Context, spec ContainerSpec) (BootProbeRun, error) {
	r.specs = append(r.specs, spec)
	if r.blind {
		return BootProbeRun{Container: "p1", Stdout: "cat: can't open '/orkstack/identity/probe': No such file or directory\n"}, nil
	}
	for _, m := range spec.Mounts {
		if m.Destination == identityMountPath && m.ReadOnly {
			b, err := os.ReadFile(filepath.Join(m.Source, identityProbeFile))
			if err != nil {
				return BootProbeRun{}, err
			}
			return BootProbeRun{Container: "p1", Stdout: string(b) + "\n"}, nil
		}
	}
	return BootProbeRun{}, errors.New("no identity bind")
}
func (r *identityProbeRuntime) StartBootProbe(context.Context, ContainerSpec) (BootProbeRun, error) {
	return BootProbeRun{}, errors.New("not used")
}
func (*identityProbeRuntime) bindProbeTimeout() time.Duration { return time.Second }
func (r *identityProbeRuntime) LocalDaemon() error            { return nil }

// TestProbeIdentityDir: a container that reads back the file the Orker put
// in the directory passes it; one that sees something else fails it; the
// probe leaves nothing behind in the directory.
func TestProbeIdentityDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ids")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	d := &OCIDispatcher{OrkerID: "orker-a", Policy: DefaultSecurityPolicy()}
	rt := &identityProbeRuntime{}
	d.Runtime = rt
	if err := d.ProbeIdentityDir(context.Background(), t.TempDir(), dir); err != nil {
		t.Fatalf("a daemon that sees the directory: %v", err)
	}
	if len(rt.specs) != 1 || rt.specs[0].Labels["ork.orker"] != "orker-a" {
		t.Fatalf("specs %+v", rt.specs)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("the probe left %v", entries)
	}
	rt.blind = true
	if err := d.ProbeIdentityDir(context.Background(), t.TempDir(), dir); err == nil || !strings.Contains(err.Error(), "did not read") {
		t.Fatalf("a daemon that does not see it: %v", err)
	}
}

// TestBootIdentityStorage: an Orker that serves identity chooses its storage
// at boot, removes what an earlier process under its identity left there,
// and reports the kind; one that reaches no engine chooses none.
func TestBootIdentityStorage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ids")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	left := filepath.Join(dir, "orker-a", "attempt-1")
	if err := os.MkdirAll(left, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orker-a", identityDirMarker), nil, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(left, "token"), []byte("old"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(left, 0o555); err != nil {
		t.Fatal(err)
	}
	crashed := filepath.Join(dir, ".ork-identity-new-123")
	if err := os.Mkdir(crashed, 0o700); err != nil {
		t.Fatal(err)
	}
	d := &OCIDispatcher{OrkerID: "orker-a", Policy: DefaultSecurityPolicy(), Runtime: &identityProbeRuntime{}}
	cfg := DefaultIdentityConfig()
	cfg.Dir = dir
	a := &Activities{dispatcher: d, identityConfig: &cfg}
	if err := a.bootIdentityStorage(context.Background(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	choice := d.identityStorage.Load()
	if choice == nil || choice.Dir != dir {
		t.Fatalf("choice %+v", choice)
	}
	if _, err := os.Lstat(left); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the earlier process's attempt directory survived the boot: %v", err)
	}
	assertMode(t, filepath.Join(dir, "orker-a"), 0o700)
	if _, err := os.Lstat(crashed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a crash's temporary directory survived the boot: %v", err)
	}
	memory, _ := identityMemoryBacked(dir)
	want := orkstackv1.IdentityStorage_IDENTITY_STORAGE_DISK
	if memory {
		want = orkstackv1.IdentityStorage_IDENTITY_STORAGE_MEMORY
	}
	if got := identityStorageReport(choice); got != want {
		t.Fatalf("reported %v, want %v", got, want)
	}

	none := &OCIDispatcher{OrkerID: "orker-b", Runtime: &identityProbeRuntime{}}
	if err := (&Activities{dispatcher: none}).bootIdentityStorage(context.Background(), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if got := identityStorageReport(none.identityStorage.Load()); got != orkstackv1.IdentityStorage_IDENTITY_STORAGE_UNSPECIFIED {
		t.Fatalf("an Orker that serves no identity reported %v", got)
	}

	for _, id := range []string{"", ".", "..", "a/b"} {
		keep := filepath.Join(dir, "keep")
		if err := os.WriteFile(keep, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		odd := &OCIDispatcher{OrkerID: id, Policy: DefaultSecurityPolicy(), Runtime: &identityProbeRuntime{}}
		if err := (&Activities{dispatcher: odd, identityConfig: &cfg}).bootIdentityStorage(context.Background(), t.TempDir()); err == nil {
			t.Errorf("Orker id %q was used to name its directory", id)
		}
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("Orker id %q removed the identity directory's contents: %v", id, err)
		}
	}

	bad := DefaultIdentityConfig()
	bad.Dir = filepath.Join(t.TempDir(), "absent")
	if err := (&Activities{dispatcher: d, identityConfig: &bad}).bootIdentityStorage(context.Background(), t.TempDir()); err == nil {
		t.Fatal("a dir that does not exist was accepted")
	}
}

// TestBootIdentityStorage_AnUnmarkedDirectoryIsLeftAlone: a directory under
// the identity storage named as the Orker but not made by an Orker is never
// removed: the boot is refused instead.
func TestBootIdentityStorage_AnUnmarkedDirectoryIsLeftAlone(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ids")
	if err := os.MkdirAll(filepath.Join(dir, "orker-a"), 0o700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dir, "orker-a", "notes.txt")
	if err := os.WriteFile(keep, []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := &OCIDispatcher{OrkerID: "orker-a", Policy: DefaultSecurityPolicy(), Runtime: &identityProbeRuntime{}}
	cfg := DefaultIdentityConfig()
	cfg.Dir = dir
	if err := (&Activities{dispatcher: d, identityConfig: &cfg}).bootIdentityStorage(context.Background(), t.TempDir()); err == nil ||
		!strings.Contains(err.Error(), identityDirMarker) {
		t.Fatalf("an unmarked directory: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("the unmarked directory's contents were removed: %v", err)
	}
}

// TestProbeIdentityDir_NotRun: a probe the runtime could not run is not a
// container that could not read: it says so, so the boot does not fall
// back to disk for it.
func TestProbeIdentityDir_NotRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ids")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	d := &OCIDispatcher{OrkerID: "orker-a", Policy: DefaultSecurityPolicy(), Runtime: &failingProbeRuntime{}}
	if err := d.ProbeIdentityDir(context.Background(), t.TempDir(), dir); !errors.Is(err, errIdentityProbeNotRun) {
		t.Fatalf("= %v", err)
	}
}

type failingProbeRuntime struct{ identityProbeRuntime }

func (*failingProbeRuntime) RunBootProbe(context.Context, ContainerSpec) (BootProbeRun, error) {
	return BootProbeRun{}, errors.New("the daemon did not answer")
}

// TestEnsureOwnIdentityDir_AnEmptyUnmarkedDirectoryIsAdopted: an empty
// directory under the Orker's id holds nothing to protect and is made the
// Orker's own, with its marker; it is made at most once by racing callers.
func TestEnsureOwnIdentityDir_AnEmptyUnmarkedDirectoryIsAdopted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ids")
	if err := os.MkdirAll(filepath.Join(dir, "orker-a"), 0o700); err != nil {
		t.Fatal(err)
	}
	own, made, err := ensureOwnIdentityDir(dir, "orker-a", false)
	if err != nil || !made || own != filepath.Join(dir, "orker-a") {
		t.Fatalf("= %q, %v, %v", own, made, err)
	}
	if _, err := os.Stat(filepath.Join(own, identityDirMarker)); err != nil {
		t.Fatalf("no marker: %v", err)
	}
	if _, made, err := ensureOwnIdentityDir(dir, "orker-a", false); err != nil || made {
		t.Fatalf("again: made %v, %v", made, err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("a temporary directory was left: %v", entries)
	}
}

// TestErrorKind: a failure names what went wrong on the host, never where.
func TestErrorKind(t *testing.T) {
	_, err := os.Open("/no/such/orkstack/identity/path")
	if got := errorKind(err); got != "no such file or directory" || strings.Contains(got, "/no/such") {
		t.Fatalf("= %q", got)
	}
	if got := errorKind(errors.New("/secret/path: boom")); strings.Contains(got, "/secret") {
		t.Fatalf("= %q", got)
	}
}
