package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestLiveUsageCells: a live reading prints cores and memory against its
// limit, stale ones say so, no rate yet is the dash, and an unavailable
// reading names why, never a 0.
func TestLiveUsageCells(t *testing.T) {
	cores := 1.5
	reading := func(u *orkstackv1.LiveUsage) *orkstackv1.LiveAttempt { return &orkstackv1.LiveAttempt{Usage: u} }
	for _, tc := range []struct {
		name        string
		live        *orkstackv1.LiveAttempt
		cpu, memory string
	}{
		{"no usage at all", &orkstackv1.LiveAttempt{}, dash, dash},
		{"a reading", reading(&orkstackv1.LiveUsage{Available: true, CpuCores: &cores, MemoryBytes: 64 << 20, MemoryLimitBytes: 1 << 30,
			SampledAt: timestamppb.Now()}), "1.50 cores", "64.0 MiB of 1.0 GiB"},
		{"no rate yet, no limit", reading(&orkstackv1.LiveUsage{Available: true, MemoryBytes: 512}), dash, "512 B"},
		{"stale", reading(&orkstackv1.LiveUsage{Available: true, CpuCores: &cores, MemoryBytes: 1024, Stale: true}), "1.50 cores (stale)", "1.0 KiB (stale)"},
		{"not visible", reading(&orkstackv1.LiveUsage{UnavailableReason: orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_CGROUP_NOT_VISIBLE}), "not visible", dash},
		{"a reason this build does not know", reading(&orkstackv1.LiveUsage{UnavailableReason: 42}), "unknown (42)", dash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cpu, memory := liveUsageCells(tc.live)
			if cpu != tc.cpu || memory != tc.memory {
				t.Fatalf("cells = %q, %q; want %q, %q", cpu, memory, tc.cpu, tc.memory)
			}
		})
	}
	for reason := range orkstackv1.LiveUsageUnavailableReason_name {
		if label := liveUsageReasonLabel(orkstackv1.LiveUsageUnavailableReason(reason)); reason != 0 && strings.HasPrefix(label, "unknown") {
			t.Errorf("reason %d has no label", reason)
		}
	}
	for reason := range orkstackv1.UnmeasuredReason_name {
		if label := unmeasuredReasonLabel(orkstackv1.UnmeasuredReason(reason)); strings.HasPrefix(label, "for reason") {
			t.Errorf("unmeasured reason %d has no label", reason)
		}
	}
}

// TestUsageMeasuredNumbers: a row with nothing measured is dashes; an unset
// efficiency or percentile is the dash even when something was measured.
func TestUsageMeasuredNumbers(t *testing.T) {
	efficiency, p95 := 0.25, 0.905
	for _, tc := range []struct {
		name string
		row  *orkstackv1.UsageRow
		want []string
	}{
		{"nothing measured", &orkstackv1.UsageRow{CpuSeconds: 100}, []string{dash, dash, dash, dash}},
		{"measured, nothing to compare", &orkstackv1.UsageRow{AttemptsMeasured: 2, MeasuredCpuSeconds: 7, MemoryPeakBytesMax: 2048},
			[]string{"7", dash, "2.0 KiB", dash}},
		{"measured, compared", &orkstackv1.UsageRow{AttemptsMeasured: 2, MeasuredCpuSeconds: 7, CpuEfficiency: &efficiency,
			MemoryPeakBytesMax: 2048, MemoryUtilizationP95: &p95}, []string{"7", "25.0%", "2.0 KiB", "90.5%"}},
	} {
		got := usageMeasuredNumbers(tc.row)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}
