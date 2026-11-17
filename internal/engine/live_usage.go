package engine

import (
	"encoding/json/v2"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// liveUsage is a running attempt's live CPU and memory from its last beat
// (E94, prod-kit design §3.6). The beat's usage is decoded apart from the
// rest of it and leniently: a usage this engine cannot read is UNDECODABLE on
// the usage alone, never on the beat. beat is when the cluster recorded the
// beat; the reading's own time is that less the age the Orker reported, so
// staleness is measured on the cluster's clock and not the Orker's.
func liveUsage(progress task.HeartbeatProgress, beat, now time.Time) *orkstackv1.LiveUsage {
	unavailable := func(reason orkstackv1.LiveUsageUnavailableReason) *orkstackv1.LiveUsage {
		return &orkstackv1.LiveUsage{UnavailableReason: reason}
	}
	if len(progress.Usage) == 0 {
		// A beat after its container started that carries no usage is from
		// an Orker that does not measure; before the start there is nothing
		// to measure yet.
		if progress.Started {
			return unavailable(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_ORKER_TOO_OLD)
		}
		return unavailable(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_PREPARING)
	}
	var u task.HeartbeatUsage
	if err := json.Unmarshal(progress.Usage, &u); err != nil || !usableUsage(u) {
		return unavailable(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)
	}
	if u.Unavailable != "" {
		// A reason and numbers together contradict each other.
		if hasFigures(u) {
			return unavailable(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)
		}
		return unavailable(liveUnavailableReason(u.Unavailable))
	}
	// Every reading an Orker sends carries its sampling interval: without
	// one, the usage carries nothing to show, and staleness has no bound.
	if u.Interval <= 0 {
		return unavailable(orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE)
	}
	sampled := beat.Add(-time.Duration(u.AgeMillis) * time.Millisecond)
	window := 2*time.Duration(u.Interval)*time.Millisecond + time.Duration(u.Tick)*time.Millisecond
	out := &orkstackv1.LiveUsage{
		Available:        true,
		CpuSeconds:       u.CPUSeconds,
		MemoryBytes:      u.Memory,
		MemoryPeakBytes:  u.MemoryPeak,
		MemoryPeakExact:  u.PeakExact,
		MemoryLimitBytes: u.Limit,
		SampledAt:        timestamppb.New(sampled),
		Stale:            now.Sub(sampled) > window,
	}
	if u.CPUCores != nil {
		cores := *u.CPUCores
		out.CpuCores = &cores
	}
	return out
}

// usableUsage refuses a usage whose numbers no sampler sends: negative, or
// not finite. Such a usage is UNDECODABLE, never shown as a measurement.
func usableUsage(u task.HeartbeatUsage) bool {
	finite := func(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) && x >= 0 }
	if !finite(u.CPUSeconds) || u.CPUCores != nil && !finite(*u.CPUCores) {
		return false
	}
	for _, n := range []int64{u.Memory, u.MemoryPeak, u.Limit, u.AgeMillis, u.Interval, u.Tick} {
		if n < 0 {
			return false
		}
	}
	// An interval, tick or age no Orker sends would make staleness
	// meaningless, and a larger age would overflow the reading's time.
	const day = int64(24 * time.Hour / time.Millisecond)
	return u.Interval <= day && u.Tick <= int64(time.Hour/time.Millisecond) && u.AgeMillis <= day
}

// hasFigures reports whether u carries any reading.
func hasFigures(u task.HeartbeatUsage) bool {
	return u.CPUCores != nil || u.CPUSeconds != 0 || u.Memory != 0 || u.MemoryPeak != 0 || u.PeakExact ||
		u.Limit != 0 || u.AgeMillis != 0
}

// liveUnavailableReason maps a beat's Unavailable to the live reason; one
// this engine does not know is UNDECODABLE.
func liveUnavailableReason(name string) orkstackv1.LiveUsageUnavailableReason {
	switch name {
	case "CGROUP_NOT_VISIBLE":
		return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_CGROUP_NOT_VISIBLE
	case "NO_CGROUP":
		return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NO_CGROUP
	case "DISABLED":
		return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_DISABLED
	case "NO_SAMPLE_YET", "ENDED_BEFORE_FIRST_SAMPLE":
		return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_NO_SAMPLE_YET
	case "READ_FAILED":
		return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_READ_FAILED
	case "START_FAILED":
		return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_START_FAILED
	case "CGROUP_UNVERIFIED":
		return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_CGROUP_UNVERIFIED
	}
	return orkstackv1.LiveUsageUnavailableReason_LIVE_USAGE_UNAVAILABLE_REASON_UNDECODABLE
}
