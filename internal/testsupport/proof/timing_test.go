package proof

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeTB stands in for a test: it records what the helper reports.
type fakeTB struct {
	testing.TB
	mu    sync.Mutex
	logs  []string
	errs  []string
	fatal []string
}

func (f *fakeTB) Helper() {}
func (f *fakeTB) Logf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

func (f *fakeTB) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, fmt.Sprintf(format, args...))
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fatal = append(f.fatal, fmt.Sprintf(format, args...))
}

func timingOn(f *fakeTB, cpus int) *Timing { return &Timing{t: f, cpus: cpus} }

func TestBoundScale(t *testing.T) {
	for _, c := range []struct {
		cpus int
		want float64
	}{
		{16, 1}, {32, 1}, {8, 2}, {4, 4}, {2, 8}, {1, 16},
		// An unknown count is held to the reference bound, never widened.
		{0, 1}, {-1, 1},
	} {
		if got := BoundScale(c.cpus); got != c.want {
			t.Errorf("BoundScale(%d) = %v, want %v", c.cpus, got, c.want)
		}
	}
}

// The reference host enforces the reference bound exactly; a smaller host
// is held to that bound scaled by 16 over its CPUs, and fails beyond it.
func TestTiming_TheBoundIsTheReferenceScaledToTheHost(t *testing.T) {
	for _, c := range []struct {
		name      string
		cpus      int
		measured  float64
		reference float64
		held      bool
		line      string
	}{
		{"the reference host within the bound", 16, 1.9, 2, true,
			"timing held: id=getrun-live-p95 measured=1.900s bound=2s reference=2s cpus=16 label=reduced"},
		{"the reference host past the bound", 16, 2.014, 2, false,
			"timing miss: id=getrun-live-p95 measured=2.014s bound=2s reference=2s cpus=16 label=reduced"},
		{"a 4-CPU runner within the scaled bound", 4, 7.9, 2, true,
			"timing held: id=getrun-live-p95 measured=7.900s bound=8s reference=2s cpus=4 label=reduced"},
		{"a 4-CPU runner past the scaled bound", 4, 8.2, 2, false,
			"timing miss: id=getrun-live-p95 measured=8.200s bound=8s reference=2s cpus=4 label=reduced"},
		{"a host larger than the reference is held to the reference", 64, 2.5, 2, false,
			"timing miss: id=getrun-live-p95 measured=2.500s bound=2s reference=2s cpus=64 label=reduced"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeTB{}
			if got := timingOn(f, c.cpus).Check("getrun-live-p95", c.measured, c.reference, "reduced"); got != c.held {
				t.Errorf("Check = %v, want %v", got, c.held)
			}
			out := f.logs
			if !c.held {
				out = f.errs
				if len(f.logs) != 0 {
					t.Errorf("a miss was also logged: %v", f.logs)
				}
			} else if len(f.errs) != 0 {
				t.Errorf("a held check failed the test: %v", f.errs)
			}
			if len(out) != 1 || out[0] != c.line {
				t.Errorf("printed %q, want %q", out, c.line)
			}
		})
	}
}

// CheckReference holds every host to the reference bound, a small one too.
func TestTiming_CheckReferenceIsNeverScaled(t *testing.T) {
	f := &fakeTB{}
	if timingOn(f, 4).CheckReference("events0-read", 2.5, 2, "") {
		t.Error("a 4-CPU host was given more than the reference bound")
	}
	want := "timing miss: id=events0-read measured=2.500s bound=2s reference=2s cpus=4"
	if len(f.errs) != 1 || f.errs[0] != want {
		t.Errorf("errors %q, want %q", f.errs, want)
	}
}

func TestTiming_RecordOnlyPrintsAMissAndFailsNothing(t *testing.T) {
	f := &fakeTB{}
	m := timingOn(f, 16)
	m.RecordOnly()
	if m.Check("replay-slowest", 31, 30, "") {
		t.Error("a miss held")
	}
	if len(f.errs) != 0 || len(f.logs) != 1 || !strings.HasPrefix(f.logs[0], "timing miss: id=replay-slowest ") {
		t.Errorf("errors %v logs %v", f.errs, f.logs)
	}
}

func TestTiming_ANaNOrInfiniteMeasurementIsAMiss(t *testing.T) {
	f := &fakeTB{}
	m := timingOn(f, 16)
	if m.Check("replay-slowest", math.NaN(), 30, "") || m.Check("replay-slowest", math.Inf(1), 30, "") {
		t.Error("a NaN or an infinity held a bound")
	}
	if len(f.errs) != 2 {
		t.Errorf("errors = %v, want two misses", f.errs)
	}
}

func TestTiming_AnInvalidIDFailsTheTestAtOnce(t *testing.T) {
	for _, id := range []string{"", "Upper", "1st", "with space", "under_score"} {
		f := &fakeTB{}
		if timingOn(f, 16).Check(id, 1, 2, "") || len(f.fatal) != 1 {
			t.Errorf("id %q: fatal %v", id, f.fatal)
		}
	}
}

func TestTiming_ChecksFromManyGoroutinesAreAllPrinted(t *testing.T) {
	f := &fakeTB{}
	m := timingOn(f, 16)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Check("replay-slowest", 1, 30, "")
		}()
	}
	wg.Wait()
	if len(f.logs) != 32 {
		t.Errorf("%d checks printed of 32", len(f.logs))
	}
}

func TestParseCPUList(t *testing.T) {
	for _, c := range []struct {
		list string
		want int
	}{
		{"0-15", 16}, {"0", 1}, {"0-3,8-11", 8}, {"0,2,4", 3},
		{"", 0}, {"x", 0}, {"3-1", 0}, {"0-", 0}, {"-1", 0},
	} {
		if got := parseCPUList(c.list); got != c.want {
			t.Errorf("parseCPUList(%q) = %d, want %d", c.list, got, c.want)
		}
	}
}

// Restricting the process to fewer CPUs never widens a bound: the host's
// online CPUs count when they are more.
func TestHostCPUs_IsNeverBelowTheOnlineCPUs(t *testing.T) {
	file := filepath.Join(t.TempDir(), "online")
	if err := os.WriteFile(file, []byte("0-63\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := onlineCPUs(file); got != 64 {
		t.Errorf("onlineCPUs = %d, want 64", got)
	}
	if got := onlineCPUs(filepath.Join(t.TempDir(), "absent")); got != 0 {
		t.Errorf("an absent file counts %d CPUs", got)
	}
	if HostCPUs() < onlineCPUs("/sys/devices/system/cpu/online") {
		t.Error("HostCPUs is below the host's online CPUs")
	}
}

func TestValidTimingID(t *testing.T) {
	for id, want := range map[string]bool{"getrun-live-p95": true, "a": true, "events0-read": true, "": false, "A": false, "-x": false} {
		if got := ValidTimingID(id); got != want {
			t.Errorf("ValidTimingID(%q) = %v, want %v", id, got, want)
		}
	}
}
