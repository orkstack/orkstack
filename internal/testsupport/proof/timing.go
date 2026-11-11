package proof

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A test whose pass depends on how fast the host is checks that budget
// through Timing, not through a bare comparison. A wall-clock budget is
// calibrated on the reference host of ReferenceCPUs CPUs (docs/PROOFS.md §12).
// Check holds a measurement on a host with fewer CPUs to the reference bound
// scaled by ReferenceCPUs over the host's CPUs, and on the reference class or
// larger to the reference bound itself; CheckReference holds it to the
// reference bound on every host. No table excuses a miss and nothing reads a
// record: a miss beyond the bound the host is held to fails the test where it
// is checked, and every check, held or missed, prints one line of the form
// TimingLine writes, so the test's own output names the measurement and the
// bound.
//
// Counts, bytes, history shares, deadlock-detector trips and every
// correctness assertion never go through here: they are never scaled, and
// keep failing the test where they are made.

// ReferenceCPUs is the CPU count of the reference host every wall-clock budget
// was calibrated on (docs/PROOFS.md §12).
const ReferenceCPUs = 16

// BoundScale is how much a reference budget widens on a host of cpus CPUs:
// ReferenceCPUs over cpus on a smaller host, and 1 on the reference class or
// larger, or when the count is unknown (cpus < 1), so an unknown host is held
// to the reference bound.
func BoundScale(cpus int) float64 {
	if cpus < 1 || cpus >= ReferenceCPUs {
		return 1
	}
	return float64(ReferenceCPUs) / float64(cpus)
}

// HostCPUs is the CPU count a budget is scaled by: the larger of the CPUs this
// process may run on and the host's online CPUs, so restricting a proof to
// fewer cores (taskset, a cgroup's cpuset) never widens a bound.
func HostCPUs() int {
	return max(runtime.NumCPU(), onlineCPUs("/sys/devices/system/cpu/online"))
}

// onlineCPUs counts the CPUs a Linux cpu list names ("0-15", "0-3,8-11"),
// read from file; 0 when there is no such file or it does not parse.
func onlineCPUs(file string) int {
	b, err := os.ReadFile(file)
	if err != nil {
		return 0
	}
	return parseCPUList(strings.TrimSpace(string(b)))
}

// parseCPUList counts the CPUs of a Linux cpu list; 0 for anything else.
func parseCPUList(list string) int {
	if list == "" {
		return 0
	}
	n := 0
	for _, part := range strings.Split(list, ",") {
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil || a < 0 {
			return 0
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil || b < a {
				return 0
			}
		}
		n += b - a + 1
	}
	return n
}

// timingID is the shape of a check's id.
var timingID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// ValidTimingID reports whether id may name a timing check.
func ValidTimingID(id string) bool { return timingID.MatchString(id) }

// TimingCheck is one wall-clock check, as TimingLine prints it.
type TimingCheck struct {
	ID    string
	Label string
	// Measured, Reference and Bound are seconds: Bound is Reference scaled to
	// a host of CPUs CPUs (BoundScale).
	Measured  float64
	Reference float64
	Bound     float64
	CPUs      int
}

// Held reports whether the measurement is within the bound; a NaN or an
// infinity is a miss.
func (c TimingCheck) Held() bool {
	return !math.IsNaN(c.Measured) && !math.IsInf(c.Measured, 0) && c.Measured <= c.Bound
}

// TimingLine is the one line a check prints, without the "file.go:N: " the
// testing package puts in front of it:
//
//	timing held: id=getrun-live-p95 measured=2.014s bound=8s reference=2s cpus=4 label=reduced-fan-out
//
// The verdict is "held" or "miss"; the label is free text without a newline,
// and is last.
func TimingLine(c TimingCheck) string {
	verdict := "held"
	if !c.Held() {
		verdict = "miss"
	}
	line := fmt.Sprintf("timing %s: id=%s measured=%ss bound=%ss reference=%ss cpus=%d", verdict, c.ID,
		strconv.FormatFloat(c.Measured, 'f', 3, 64), strconv.FormatFloat(c.Bound, 'f', -1, 64),
		strconv.FormatFloat(c.Reference, 'f', -1, 64), c.CPUs)
	if label := strings.Join(strings.Fields(c.Label), " "); label != "" {
		line += " label=" + label
	}
	return line
}

// Timing checks one test's wall-clock budgets. Its methods are safe for
// concurrent use.
type Timing struct {
	t      testing.TB
	cpus   int
	mu     sync.Mutex
	record bool // RecordOnly
}

// TimingBounds returns the handle a test checks its wall-clock budgets
// through, scaled to this host (HostCPUs).
func TimingBounds(t testing.TB) *Timing {
	return &Timing{t: t, cpus: HostCPUs()}
}

// RecordOnly makes the test print its checks and fail nothing for a miss: the
// measurement is evidence, not a claim of that test.
func (m *Timing) RecordOnly() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record = true
}

// Check holds one measurement to its reference bound scaled to this host, and
// reports whether it held. id names the check (a lowercase word with
// hyphens), measured and reference are seconds, and label says which sample
// of the test it was when the test takes more than one. A miss fails the test
// here, unless it is RecordOnly.
func (m *Timing) Check(id string, measured, reference float64, label string) bool {
	m.t.Helper()
	return m.check(id, measured, reference, BoundScale(m.cpus), label)
}

// CheckReference is Check held to the reference bound on every host, however
// small: a budget that holds on every host the proofs run on.
func (m *Timing) CheckReference(id string, measured, reference float64, label string) bool {
	m.t.Helper()
	return m.check(id, measured, reference, 1, label)
}

func (m *Timing) check(id string, measured, reference, scale float64, label string) bool {
	m.t.Helper()
	if !ValidTimingID(id) {
		m.t.Fatalf("proof: timing id %q is not a lowercase word with hyphens", id)
		return false
	}
	c := TimingCheck{ID: id, Label: label, Measured: measured, Reference: reference,
		Bound: reference * scale, CPUs: m.cpus}
	m.mu.Lock()
	record := m.record
	m.mu.Unlock()
	if c.Held() || record {
		m.t.Logf("%s", TimingLine(c))
	} else {
		m.t.Errorf("%s", TimingLine(c))
	}
	return c.Held()
}
