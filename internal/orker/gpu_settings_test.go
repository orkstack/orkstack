package orker

import (
	"bytes"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestSecurityPolicy_MaxGPUAndCDISpecDirsFromEnv(t *testing.T) {
	p := DefaultSecurityPolicy()
	if p.MaxGPU != DefaultMaxGPU || !slices.Equal(p.CDISpecDirs, DefaultCDISpecDirs) {
		t.Fatalf("defaults: max_gpu %d, spec dirs %v", p.MaxGPU, p.CDISpecDirs)
	}
	t.Setenv(MaxGPUVar, "4")
	t.Setenv(CDISpecDirsVar, "/etc/cdi, /run/cdi")
	p = SecurityPolicyFromEnv()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.MaxGPU != 4 || !slices.Equal(p.CDISpecDirs, []string{"/etc/cdi", "/run/cdi"}) {
		t.Fatalf("read: max_gpu %d, spec dirs %v", p.MaxGPU, p.CDISpecDirs)
	}
	for name, env := range map[string][2]string{
		"max_gpu not a number": {MaxGPUVar, "two"},
		"max_gpu negative":     {MaxGPUVar, "-1"},
		"max_gpu above 64":     {MaxGPUVar, "65"},
		"a relative spec dir":  {CDISpecDirsVar, "etc/cdi"},
		"an unclean spec dir":  {CDISpecDirsVar, "/etc/../etc/cdi"},
		"no spec dir at all":   {CDISpecDirsVar, " , "},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(MaxGPUVar, "")
			t.Setenv(CDISpecDirsVar, "")
			t.Setenv(env[0], env[1])
			err := SecurityPolicyFromEnv().Validate()
			if err == nil || !strings.Contains(err.Error(), env[0]) {
				t.Fatalf("Validate = %v, want a refusal naming %s", err, env[0])
			}
		})
	}
}

// TestResolveResources_GPUAboveMaxGPURefused is step 3a for gpu: a count
// above the cap is refused, never reduced.
func TestResolveResources_GPUAboveMaxGPURefused(t *testing.T) {
	p := DefaultSecurityPolicy()
	p.MaxGPU = 2
	if _, _, f := p.ResolveResources(&orkstackv1.ResourceRequest{Gpu: 2}); f != nil {
		t.Fatalf("gpu 2 under max_gpu 2 refused: %v", f)
	}
	_, _, f := p.ResolveResources(&orkstackv1.ResourceRequest{Gpu: 3})
	if f == nil || f.Reason != ReasonResourceRefused || !strings.Contains(f.Message, MaxGPUVar) {
		t.Fatalf("gpu 3 under max_gpu 2 = %v, want %s naming %s", f, ReasonResourceRefused, MaxGPUVar)
	}
}

// TestCheckGPUSettings_ZeroMaxGPURefusesAGPUProfile: a gpu profile under
// max_gpu 0 could run no gpu task, so boot refuses it, naming the setting.
func TestCheckGPUSettings_ZeroMaxGPURefusesAGPUProfile(t *testing.T) {
	gpu := Profiles{List: []Profile{{Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
		GPU: &GPUProfile{CDIKind: "amd.com/gpu", Units: []string{"0", "1"}}}}}
	p := DefaultSecurityPolicy()
	p.MaxGPU = 0
	if err := checkGPUSettings(p, gpu); err == nil || !strings.Contains(err.Error(), MaxGPUVar) {
		t.Fatalf("checkGPUSettings = %v, want a refusal naming %s", err, MaxGPUVar)
	}
	if err := checkGPUSettings(p, Profiles{}); err != nil {
		t.Fatalf("max_gpu 0 with no gpu profile = %v", err)
	}
	p.MaxGPU = 2
	if err := checkGPUSettings(p, gpu); err != nil {
		t.Fatalf("max_gpu 2 with two units = %v", err)
	}
}

// TestNewCapacityFor_GPUProfileBuildsALedger: a gpu profile builds the
// admission ledger even with no orker.capacity, unbounded in cpu and memory,
// so its units are counted.
func TestNewCapacityFor_GPUProfileBuildsALedger(t *testing.T) {
	gpu := Profiles{List: []Profile{{Name: "amd", Kind: orkstackv1.ProfileKind_PROFILE_KIND_GPU,
		GPU: &GPUProfile{CDIKind: "amd.com/gpu", Units: []string{"0", "1"}}}}}
	c, err := newCapacityFor(DefaultSecurityPolicy(), Options{Profiles: gpu, TaskQueue: "q"}, 4)
	if err != nil || c == nil {
		t.Fatalf("newCapacityFor = %v, %v; want a ledger", c, err)
	}
	if c.limit.milli != 0 || c.limit.bytes != 0 {
		t.Fatalf("limit = %v, want unbounded cpu and memory", c.limit)
	}
	if c, err := newCapacityFor(DefaultSecurityPolicy(), Options{TaskQueue: "q"}, 4); c != nil || err != nil {
		t.Fatalf("no capacity, no gpu profile: %v, %v; want no ledger", c, err)
	}
}

// TestMaxRequest_CarriesTheGPUCeiling: DescribeOrker's max_request names the
// fixed gpu ceiling of an Orker with units, which submit's fit check reads,
// and no gpu for an Orker without.
func TestMaxRequest_CarriesTheGPUCeiling(t *testing.T) {
	a := &Activities{dispatcher: &OCIDispatcher{Policy: DefaultSecurityPolicy()}}
	if got := a.maxRequest(); got == nil || got.Gpu != nil {
		t.Fatalf("no units: max_request %v, want no gpu", got)
	}
	a.capacity = newCapacity(amount{}, amount{milli: 100, bytes: 1 << 20}, 2)
	a.capacity.useUnits("amd", 4, 2, nil)
	if got := a.maxRequest(); got.GetGpu() != 2 || got.Gpu == nil {
		t.Fatalf("units: max_request %v, want gpu 2", got)
	}
}

// TestWarnGPUCeiling_AboveHalfTheUnits: an Orker whose max_gpu is more than
// half its gpu profile's units logs one WARN at boot naming both numbers,
// since one unhealthy unit then leaves fewer usable units than a task may
// hold; at half or below, or on a host of one unit, it logs nothing.
func TestWarnGPUCeiling_AboveHalfTheUnits(t *testing.T) {
	for name, tc := range map[string]struct {
		maxGPU, units int
		warns         bool
	}{
		"2 of 2": {maxGPU: 2, units: 2, warns: true},
		"2 of 3": {maxGPU: 2, units: 3, warns: true},
		"1 of 1": {maxGPU: 1, units: 1},
		"2 of 1": {maxGPU: 2, units: 1},
		"1 of 2": {maxGPU: 1, units: 2},
		"2 of 4": {maxGPU: 2, units: 4},
		"0 of 2": {maxGPU: 0, units: 2},
		"3 of 0": {maxGPU: 3, units: 0},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			warnGPUCeiling(slog.New(slog.NewTextHandler(&buf, nil)), "amd", tc.maxGPU, tc.units)
			out := buf.String()
			if !tc.warns {
				if out != "" {
					t.Fatalf("logged %q; want nothing", out)
				}
				return
			}
			if strings.Count(out, "\n") != 1 || !strings.Contains(out, "level=WARN") {
				t.Fatalf("logged %q; want one WARN line", out)
			}
			for _, want := range []string{fmt.Sprintf("max_gpu=%d", tc.maxGPU), fmt.Sprintf("units=%d", tc.units), "profile=amd"} {
				if !strings.Contains(out, want) {
					t.Fatalf("logged %q, which does not name %s", out, want)
				}
			}
		})
	}
}
