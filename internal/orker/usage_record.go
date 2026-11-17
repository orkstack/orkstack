package orker

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// recordMeasured puts an attempt's measured usage on its record
// (TaskAttempt.measured). An unmeasured attempt carries its method and reason
// and no numbers, so nothing reads a zero as a measurement.
func recordMeasured(record *orkstackv1.TaskAttempt, u measuredUsage) {
	m := &orkstackv1.MeasuredUsage{
		Method:             measurementMethodProto(u.method),
		ContainerStartedAt: instant(u.started),
		ContainerExitedAt:  instant(u.exited),
		OomKilled:          u.oomKilled,
	}
	if u.method == usageUnmeasured {
		m.UnmeasuredReason = unmeasuredReasonProto(u.reason)
		record.Measured = m
		return
	}
	m.CpuSeconds = u.cpuSeconds
	m.MemoryPeakBytes = u.memoryPeak
	m.MemoryPeakExact = u.peakExact
	m.MemoryLimitBytes = u.limit
	m.Samples = u.samples
	m.FirstSampledAt = instant(u.firstSample)
	m.LastSampledAt = instant(u.lastSample)
	record.Measured = m
}

// measuredProto is the attempt's measured usage as its record carries it.
func measuredProto(beat *Heartbeater) *orkstackv1.MeasuredUsage {
	record := &orkstackv1.TaskAttempt{}
	recordMeasured(record, measuredOf(beat))
	return record.GetMeasured()
}

func instant(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}

func measurementMethodProto(m usageMethod) orkstackv1.MeasurementMethod {
	switch m {
	case usageCgroupV2:
		return orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2
	case usageCgroupV1:
		return orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V1
	}
	return orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_UNMEASURED
}

func unmeasuredReasonProto(r usageReason) orkstackv1.UnmeasuredReason {
	switch r {
	case reasonCgroupNotVisible:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_CGROUP_NOT_VISIBLE
	case reasonNoCgroup:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NO_CGROUP
	case reasonEndedBeforeFirstSample:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_ENDED_BEFORE_FIRST_SAMPLE
	case reasonReadFailed:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_READ_FAILED
	case reasonNotRun:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_NOT_RUN
	case reasonDisabled:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_DISABLED
	case reasonStartFailed:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_START_FAILED
	case reasonCgroupUnverified:
		return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_CGROUP_UNVERIFIED
	}
	return orkstackv1.UnmeasuredReason_UNMEASURED_REASON_UNSPECIFIED
}
