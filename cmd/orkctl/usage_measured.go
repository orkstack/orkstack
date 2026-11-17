package main

import (
	"fmt"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Measured usage (E94): what a container's cgroup reported, live on `run
// inspect` and folded on `stats usage`. A figure that is unset or was not
// measured prints the dash, never 0, and an unavailable live reading names
// why.

// liveUsageCells are the CPU and MEMORY cells of one live attempt.
func liveUsageCells(live *orkstackv1.LiveAttempt) (cpu, memory string) {
	usage := live.GetUsage()
	if usage == nil {
		return dash, dash
	}
	if !usage.GetAvailable() {
		return liveUsageReasonLabel(usage.GetUnavailableReason()), dash
	}
	cpu = dash
	if usage.CpuCores != nil {
		cpu = fmt.Sprintf("%.2f cores", usage.GetCpuCores())
	}
	memory = formatBytes(usage.GetMemoryBytes())
	if limit := usage.GetMemoryLimitBytes(); limit > 0 {
		memory += " of " + formatBytes(limit)
	}
	if usage.GetStale() {
		cpu, memory = cpu+" (stale)", memory+" (stale)"
	}
	return cpu, memory
}

// liveUsageReasonLabel is a few words for why there is no live reading.
func liveUsageReasonLabel(reason orkstackv1.LiveUsageUnavailableReason) string {
	switch reason {
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NOT_STARTED:
		return "not started"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_PREPARING:
		return "preparing"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_CGROUP_NOT_VISIBLE:
		return "not visible"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NO_CGROUP:
		return "no cgroup"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_DISABLED:
		return "off"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NO_SAMPLE_YET:
		return "not read yet"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_ORKER_TOO_OLD:
		return "orker too old"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE:
		return "unreadable"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_READ_FAILED:
		return "read failed"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_START_FAILED:
		return "start failed"
	case orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_CGROUP_UNVERIFIED:
		return "unverified"
	}
	return fmt.Sprintf("unknown (%d)", int32(reason))
}

// usageMeasuredColumns follow the reserved ones on `stats usage`.
var usageMeasuredColumns = []string{"MEASURED-CPU-S", "CPU-EFFICIENCY", "MEM-PEAK", "MEM-P95"}

// usageMeasuredNumbers are one row's measured cells.
func usageMeasuredNumbers(row *orkstackv1.UsageRow) []string {
	if row.GetAttemptsMeasured() == 0 {
		return []string{dash, dash, dash, dash}
	}
	efficiency, p95 := dash, dash
	if row.CpuEfficiency != nil {
		efficiency = percent(row.GetCpuEfficiency())
	}
	if row.MemoryUtilizationP95 != nil {
		p95 = percent(row.GetMemoryUtilizationP95())
	}
	peak := dash
	if row.GetMemoryPeakBytesMax() > 0 {
		peak = formatBytes(row.GetMemoryPeakBytesMax())
	}
	return []string{unitSeconds(row.GetMeasuredCpuSeconds()), efficiency, peak, p95}
}

func percent(ratio float64) string { return fmt.Sprintf("%.1f%%", ratio*100) }

// usageMeasuredFooter says what the measured columns cover and, by reason,
// what they do not.
func usageMeasuredFooter(total *orkstackv1.UsageRow) []string {
	var lines []string
	if total.GetAttemptsMeasured() > 0 {
		lines = append(lines, fmt.Sprintf(
			"MEASURED columns are what %d attempts' containers used, as their cgroups reported it; "+
				"CPU-EFFICIENCY compares the measured attempts that carry a reservation; MEM-P95 covers %d attempts",
			total.GetAttemptsMeasured(), total.GetMemoryUtilizationAttempts()))
	}
	var reasons []string
	for _, count := range total.GetAttemptsUnmeasuredByReason() {
		reasons = append(reasons, fmt.Sprintf("%d %s", count.GetAttempts(), unmeasuredReasonLabel(count.GetReason())))
	}
	if len(reasons) > 0 {
		lines = append(lines, "not measured: "+strings.Join(reasons, ", "))
	}
	return lines
}

// unmeasuredReasonLabel is a few words for why an attempt was not measured.
func unmeasuredReasonLabel(reason orkstackv1.UnmeasuredReason) string {
	switch reason {
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_UNSPECIFIED:
		return "by an orker that does not report why (older than v0.4.0, or newer than this engine)"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_CGROUP_NOT_VISIBLE:
		return "where the orker could not see the container's cgroup"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NO_CGROUP:
		return "without a cgroup (rootless on cgroup v1)"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_ENDED_BEFORE_FIRST_SAMPLE:
		return "that ended before a reading"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_READ_FAILED:
		return "whose cgroup could not be read"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NOT_RUN:
		return "that ran no container"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_DISABLED:
		return "with measuring off"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_START_FAILED:
		return "whose start failed"
	case orkstackv1.UnmeasuredReason_UNMEASURED_REASON_CGROUP_UNVERIFIED:
		return "whose cgroup could not be verified (not on a cgroup filesystem)"
	}
	return fmt.Sprintf("for reason %d", int32(reason))
}
