//go:build linux

package procguard

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

// helperVar asks the test binary, run again as a child, to be one of the
// processes below instead of running its tests: what Harden and Settle do
// is the whole process's, so the test process itself is left as it was.
const helperVar = "PROCGUARD_TEST_HELPER"

// TestMain runs the child when asked, and the tests otherwise.
func TestMain(m *testing.M) {
	if mode := os.Getenv(helperVar); mode != "" {
		os.Exit(helper(mode))
	}
	os.Exit(m.Run())
}

func getDumpable() (bool, error) {
	r, _, errno := syscall.RawSyscall6(syscall.SYS_PRCTL, 3 /* PR_GET_DUMPABLE */, 0, 0, 0, 0, 0)
	if errno != 0 {
		return false, fmt.Errorf("prctl(PR_GET_DUMPABLE): %w", errno)
	}
	return r != 0, nil
}

// helper is the child. It prints what it found and returns its exit status.
func helper(mode string) int {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	report := func() {
		d, err := getDumpable()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("dumpable=%t\n", d)
	}
	switch mode {
	case "report":
		report()
		return 0
	case "harden", "allow", "exec":
	default:
		fmt.Printf("unknown mode %q\n", mode)
		return 2
	}
	hardenErr := Harden()
	allow := ""
	if mode == "allow" {
		allow = "true"
	}
	if err := Settle(func(string) string { return allow }, logger, hardenErr); err != nil {
		fmt.Printf("settle=%v\n", err)
	}
	report()
	if fi, err := os.Stat("/proc/self/mem"); err == nil {
		fmt.Printf("mem_uid=%d\n", fi.Sys().(*syscall.Stat_t).Uid)
	}
	if mode == "exec" {
		out, err := childOutput("report")
		if err != nil {
			fmt.Printf("exec=%v\n", err)
		}
		fmt.Printf("child:%s", out)
	}
	fmt.Printf("logs:%s\n", strings.ReplaceAll(logs.String(), "\n", " | "))
	return 0
}

func childOutput(mode string) (string, error) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), helperVar+"="+mode)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestHarden: a hardened process is not dumpable, and its /proc entries are
// root's; with process.allow_debug it is dumpable again and says so at
// warning; and a program it executes starts dumpable, so a container CLI an
// Orker runs is untouched.
func TestHarden(t *testing.T) {
	tests := []struct {
		mode    string
		want    []string
		wantNot []string
		memRoot bool
	}{
		{mode: "harden", want: []string{"dumpable=false"}, wantNot: []string{"level=WARN", "level=ERROR"}, memRoot: true},
		{mode: "allow", want: []string{"dumpable=true", "level=WARN", AllowDebugVar}},
		{mode: "exec", want: []string{"dumpable=false", "child:dumpable=true"}},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			out, err := childOutput(tt.mode)
			if err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			for _, w := range tt.want {
				if !strings.Contains(out, w) {
					t.Errorf("child said %q; want %q", out, w)
				}
			}
			for _, w := range tt.wantNot {
				if strings.Contains(out, w) {
					t.Errorf("child said %q; want no %q", out, w)
				}
			}
			if tt.memRoot && os.Geteuid() != 0 && !strings.Contains(out, "mem_uid=0\n") {
				t.Errorf("child said %q; want /proc/self/mem owned by root", out)
			}
		})
	}
}
