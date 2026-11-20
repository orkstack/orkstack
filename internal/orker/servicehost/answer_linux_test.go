//go:build linux

package servicehost

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// hostileAnswer is an answer mount a hostile container planted: every kind
// of thing that is not a regular file with one name, at the names a reader
// asks for.
func hostileAnswer(t *testing.T) (string, *AnswerDir) {
	t.Helper()
	root := t.TempDir()
	answer := filepath.Join(root, "answer")
	outside := filepath.Join(root, "outside")
	for _, dir := range []string{answer, outside, filepath.Join(answer, "lanes", "0"), filepath.Join(answer, "subdir")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, body string) {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(outside, "secret"), "outside the mount")
	write(filepath.Join(answer, "status.json"), `{"schemaVersion":1}`)
	write(filepath.Join(answer, "lanes", "0", "message.json"), `{"seq":1}`)
	write(filepath.Join(answer, "big"), strings.Repeat("x", 2048))
	links := map[string]string{
		"abs":           filepath.Join(outside, "secret"),
		"rel":           "../outside/secret",
		"magic":         "/proc/self/root/etc/hostname",
		"lanes/1":       filepath.Join(outside),
		"inner-to-good": "status.json",
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(answer, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mkfifo(filepath.Join(answer, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(answer, "status.json"), filepath.Join(answer, "hard")); err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(answer, "lanes", "0", "lonely.json"), "{}")
	if err := os.Chmod(filepath.Join(answer, "lanes", "0", "lonely.json"), 0o000); err != nil {
		t.Fatal(err)
	}
	d, err := OpenAnswerDir(answer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return answer, d
}

func TestRead_NeverFollowsALink(t *testing.T) {
	_, d := hostileAnswer(t)
	for _, tc := range []struct {
		name    string
		bound   int64
		want    string // "" = reads; "not yet"; else a violation containing it
		content string
	}{
		{name: "lanes/0/message.json", bound: 1024, content: `{"seq":1}`},
		{name: "abs", bound: 1024, want: "a link"},
		{name: "rel", bound: 1024, want: "a link"},
		{name: "magic", bound: 1024, want: "a link"},
		{name: "inner-to-good", bound: 1024, want: "a link"},
		{name: "lanes/1/secret", bound: 1024, want: "a link"},
		{name: "fifo", bound: 1024, want: "not a regular file"},
		{name: "subdir", bound: 1024, want: "not a regular file"},
		{name: "hard", bound: 1024, want: "2 names"},
		{name: "status.json", bound: 1024, want: "2 names"},
		{name: "big", bound: 1024, want: "2048 bytes; at most 1024"},
		{name: "lanes/0/absent.json", bound: 1024, want: "not yet"},
		{name: "lanes/9/message.json", bound: 1024, want: "not yet"},
		{name: "big/x", bound: 1024, want: "not a directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := d.ReadStep(tc.name, tc.bound)
			var v *ViolationError
			switch {
			case tc.want == "":
				if err != nil || string(got) != tc.content {
					t.Fatalf("ReadStep = %q, %v", got, err)
				}
			case tc.want == "not yet":
				if !errors.Is(err, ErrNotYet) {
					t.Fatalf("ReadStep = %q, %v; want ErrNotYet", got, err)
				}
			default:
				if !errors.As(err, &v) || !strings.Contains(v.Reason, tc.want) {
					t.Fatalf("ReadStep = %q, %v; want a violation containing %q", got, err, tc.want)
				}
			}
		})
	}
	if os.Getuid() != 0 {
		var v *ViolationError
		if _, err := d.ReadStep("lanes/0/lonely.json", 64); !errors.As(err, &v) {
			t.Fatalf("a mode-000 step: %v", err)
		}
	}
}

func TestRead_ASocketIsAViolation(t *testing.T) {
	dir, err := os.MkdirTemp("", "sock")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ln, err := net.Listen("unix", filepath.Join(dir, "status.json"))
	if err != nil {
		t.Skip("no unix sockets here:", err)
	}
	defer ln.Close()
	d, err := OpenAnswerDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var v *ViolationError
	if _, err := d.ReadStep("status.json", 1024); !errors.As(err, &v) {
		t.Fatalf("a socket at a step's name: %v", err)
	}
}

func TestRead_RefusesNamesOutsideTheProfile(t *testing.T) {
	_, d := hostileAnswer(t)
	for _, name := range []string{"", "/etc/passwd", "../outside/secret", "lanes/../status.json", "lanes//0", "./status.json", "a\x00b"} {
		if _, err := d.ReadStep(name, 1024); err == nil || errors.Is(err, ErrNotYet) {
			t.Errorf("ReadStep(%q) = %v", name, err)
		}
	}
}

// TestRead_CopiesOnce: the bytes returned are the Orker's own; the
// container rewriting the file afterwards changes nothing already read.
func TestRead_CopiesOnce(t *testing.T) {
	answer, d := hostileAnswer(t)
	path := filepath.Join(answer, "lanes", "0", "message.json")
	got, err := d.ReadStep("lanes/0/message.json", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"seq":999}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"seq":1}` {
		t.Fatalf("the copy changed with the file: %q", got)
	}
}

// TestRead_KeepsItsHandle: the handle reads the directory it was opened on
// even after the path to it is replaced by a link elsewhere.
func TestRead_KeepsItsHandle(t *testing.T) {
	answer, d := hostileAnswer(t)
	moved := answer + ".moved"
	if err := os.Rename(answer, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(filepath.Dir(answer), "outside"), answer); err != nil {
		t.Fatal(err)
	}
	if got, err := d.ReadStep("lanes/0/message.json", 1024); err != nil || string(got) != `{"seq":1}` {
		t.Fatalf("ReadStep after the path moved = %q, %v", got, err)
	}
	if _, err := d.ReadStep("secret", 1024); !errors.Is(err, ErrNotYet) {
		t.Fatalf("the handle followed the path to another directory: %v", err)
	}
}

func TestNewAnswerDir_RefusesAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAnswerDir(path); err == nil {
		t.Fatal("a file opened as an answer handle")
	}
	var nilDir *AnswerDir
	if err := nilDir.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestStatus_RewriteIsANewFile: a status rewritten by rename with the same
// bytes is a new file, so a container that keeps refreshing is told from
// one that stopped.
func TestStatus_RewriteIsANewFile(t *testing.T) {
	dir := t.TempDir()
	write := func() {
		tmp := filepath.Join(dir, ".s")
		if err := os.WriteFile(tmp, []byte(`{"schemaVersion":1}`), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, filepath.Join(dir, "status.json")); err != nil {
			t.Fatal(err)
		}
	}
	write()
	d, err := OpenAnswerDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_, a, err := d.ReadStepID("status.json", 1024)
	if err != nil {
		t.Fatal(err)
	}
	_, same, _ := d.ReadStepID("status.json", 1024)
	write()
	_, b, _ := d.ReadStepID("status.json", 1024)
	if a != same || a == b {
		t.Fatalf("ids %+v %+v %+v: one write reads one id, a rewrite another", a, same, b)
	}
}

func TestOpenContainerAnswer(t *testing.T) {
	// /dev/shm is a memory filesystem on Linux: a directory there stands for
	// a container's answer mount, opened through this process's own root.
	dir, err := os.MkdirTemp("/dev/shm", "answer")
	if err != nil {
		t.Skip("no /dev/shm:", err)
	}
	defer os.RemoveAll(dir)
	if err := os.WriteFile(filepath.Join(dir, "status.json"), []byte(`{"schemaVersion":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := OpenContainerAnswer(os.Getpid(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if got, err := d.ReadStep("status.json", 1024); err != nil || string(got) != `{"schemaVersion":1}` {
		t.Fatalf("%q, %v", got, err)
	}
	// An answer path that is a symbolic link, here to the memory directory
	// above, is never followed: a container's link to a host directory would
	// otherwise be read.
	link := filepath.Join(t.TempDir(), "answer")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if d, err := OpenContainerAnswer(os.Getpid(), link); err == nil {
		d.Close()
		t.Fatal("an answer path that is a symbolic link was followed")
	}
	disk := t.TempDir()
	if _, err := OpenContainerAnswer(os.Getpid(), disk); err == nil {
		var st syscall.Statfs_t
		if syscall.Statfs(disk, &st) == nil && st.Type != 0x01021994 {
			t.Fatal("an answer mount that is not a memory filesystem opened")
		}
	}
	for _, bad := range []struct {
		pid  int
		path string
	}{{0, dir}, {os.Getpid(), "relative"}, {os.Getpid(), dir + "/../x"}, {1 << 30, dir}} {
		if _, err := OpenContainerAnswer(bad.pid, bad.path); err == nil {
			t.Errorf("OpenContainerAnswer(%d, %q) opened", bad.pid, bad.path)
		}
	}
}

// A step renamed over while the Orker reads it leaves the Orker holding a
// file with no name: the container replaced it, as the protocol has it do,
// and the Orker reads it again; a step with a second name is a hard link a
// container made, which breaks the contract.
func TestStepLinks(t *testing.T) {
	if err := stepLinks("s", 1); err != nil {
		t.Fatalf("one name: %v", err)
	}
	if err := stepLinks("s", 0); !errors.Is(err, ErrNotYet) {
		t.Fatalf("a replaced step: %v; want ErrNotYet", err)
	}
	var v *ViolationError
	if err := stepLinks("s", 2); !errors.As(err, &v) {
		t.Fatalf("a hard link: %v; want a violation", err)
	}
}
