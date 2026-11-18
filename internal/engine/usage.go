package engine

import (
	"math"
	"math/bits"
	"sort"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Usage is how much of the fleet one namespace RESERVED over one window,
// derived on read from records that already exist: the archive index (which
// runs), each archived body (which tasks were dispatched, and where) and the
// Orker's per-attempt TaskAttempt records (how long each attempt held its
// Orker, and what that Orker reserved for it).
//
// The units are task-seconds (held time), cpu-seconds (reserved cores × held
// time) and GiB-seconds (reserved memory in GiB × held time). They are
// reservations, never measured utilization, and every surface that renders
// them says so.
//
// Everything here is pure: the facts are read elsewhere (internal/api's
// UsageMeter), and these functions only decide what one record contributes
// and how runs fold into a total and rows. It reuses the flake index's
// TaskSample, SampleTask and Dispatched rather than redeclaring them, so the
// two reads agree on what a dispatched task and a missing record are.

// gibibyte is 2^30 bytes, the GiB of a GiB-second.
const gibibyte = 1 << 30

// AttemptUsage is one attempt record's contribution to usage. timed is false
// when started_at or finished_at is unset or the attempt finished before it
// started; then every value is 0 and the record is unmeasured. priced is
// true when timed and the record carries a reservation.
func AttemptUsage(r *orkstackv1.TaskAttempt) (seconds, cpuSeconds, gibSeconds float64, timed, priced bool) {
	started, finished := r.GetStartedAt(), r.GetFinishedAt()
	if started == nil || finished == nil {
		return 0, 0, 0, false, false
	}
	held := finished.AsTime().Sub(started.AsTime())
	if held < 0 {
		return 0, 0, 0, false, false
	}
	seconds = float64(held) / float64(time.Second)
	res := r.GetResources()
	if res == nil {
		return seconds, 0, 0, true, false
	}
	return seconds, res.GetCpu() * seconds, float64(res.GetMemoryBytes()) / gibibyte * seconds, true, true
}

// UsageTask is one dispatched task's consumption in one finished run.
type UsageTask struct {
	TaskID           string
	Pool             string // TaskRun.pool — the sample's
	Observed         bool   // at least one record: TaskSample.Observed
	Recorded         int64  // len(TaskSample.Records)
	Unreported       int64  // TaskSample.Unreported()
	WithoutResources int64  // timed records with no reservation
	Unmeasured       int64  // untimed records
	TaskSeconds      float64
	CPUSeconds       float64
	GiBSeconds       float64
	// GPUSeconds is gpu units × held time (gpu_seconds), reported only.
	GPUSeconds float64

	// Measured usage (E94), from the records whose container's cgroup was
	// read: what it used, and what was reserved for those same attempts so
	// the two compare like for like.
	Measured           int64
	MeasuredCPUSeconds float64
	// MeasuredCPUPriced and ReservedCPUMeasured are the measured and the
	// reserved cpu-seconds of the measured attempts that carry a
	// reservation: the efficiency's two sides, one population.
	MeasuredCPUPriced   float64
	ReservedCPUMeasured float64
	MemoryPeakMax       int64
	// MemoryBuckets is each measured attempt's memory utilization as its
	// 0.5 % bucket (utilizationBucket): those whose peak is the kernel's own
	// against a limit, and those stopped at their limit, which count as at
	// it. A sampled peak would understate it and is left out. One byte an
	// attempt, since the per-run memo holds it.
	MemoryBuckets []uint8
	// UnmeasuredBy counts the records that measured nothing, by why, indexed
	// by UnmeasuredReason; a record an Orker older than E94 wrote counts
	// under UNSPECIFIED, and a reason this engine does not know under
	// UnmeasuredOther. Fixed-size: the per-run memo holds it.
	UnmeasuredBy [unmeasuredReasons]int64
}

// unmeasuredReasons is the UnmeasuredReason values this engine knows, and
// one more slot for any it does not.
const (
	unmeasuredReasons = int(orkstackv1.UnmeasuredReason_UNMEASURED_REASON_CGROUP_UNVERIFIED) + 2
	unmeasuredOther   = unmeasuredReasons - 1
)

// UsageOfTask folds one TaskSample's records with AttemptUsage. Every record
// counts whatever its outcome — succeeded, failed, canceled or an adopted
// commit all held the Orker's slot. Attempts that left no record are
// Unreported and priced at nothing: their consumption is unknown.
func UsageOfTask(s TaskSample) UsageTask {
	usage := UsageTask{
		TaskID:     s.TaskID,
		Pool:       s.Pool,
		Observed:   s.Observed,
		Recorded:   int64(len(s.Records)),
		Unreported: int64(s.Unreported()),
	}
	for _, record := range s.Records {
		seconds, cpu, gib, timed, priced := AttemptUsage(record)
		switch {
		case !timed:
			usage.Unmeasured++
			continue
		case !priced:
			usage.WithoutResources++
		}
		usage.TaskSeconds += seconds
		usage.CPUSeconds += cpu
		usage.GiBSeconds += gib
		usage.GPUSeconds += float64(record.GetResources().GetGpu()) * seconds
	}
	for _, record := range s.Records {
		usage.addMeasured(record)
	}
	return usage
}

// addMeasured folds one record's measured usage.
func (u *UsageTask) addMeasured(r *orkstackv1.TaskAttempt) {
	m := r.GetMeasured()
	switch m.GetMethod() {
	case orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V2, orkstackv1.MeasurementMethod_MEASUREMENT_METHOD_CGROUP_V1:
	default:
		reason := int(m.GetUnmeasuredReason())
		if reason < 0 || reason >= unmeasuredOther {
			reason = unmeasuredOther
		}
		u.UnmeasuredBy[reason]++
		return
	}
	cpu, peak, limit := m.GetCpuSeconds(), m.GetMemoryPeakBytes(), m.GetMemoryLimitBytes()
	// A record is read back from storage: a figure no sampler writes counts
	// the attempt as measured and adds nothing.
	finite := !math.IsNaN(cpu) && !math.IsInf(cpu, 0) && cpu >= 0
	u.Measured++
	if finite {
		u.MeasuredCPUSeconds += cpu
	}
	if _, reserved, _, _, priced := AttemptUsage(r); priced && finite {
		u.MeasuredCPUPriced += cpu
		u.ReservedCPUMeasured += reserved
	}
	if peak > 0 {
		u.MemoryPeakMax = max(u.MemoryPeakMax, peak)
	}
	switch {
	case limit <= 0 || peak < 0:
	case m.GetOomKilled():
		// Stopped at its limit: at it, whether or not its peak was read.
		u.MemoryBuckets = append(u.MemoryBuckets, utilizationBuckets-1)
	case m.GetMemoryPeakExact():
		u.MemoryBuckets = append(u.MemoryBuckets, utilizationBucket(peak, limit))
	}
}

// utilizationBucket is peak ÷ limit as its 0.5 % bucket, in integers so a
// value on a bucket's edge lands in that bucket on every replica: bucket b
// holds [b × 0.5 %, (b+1) × 0.5 %), and the last holds everything at or
// above 100 %. peak ≥ 0 and limit > 0.
func utilizationBucket(peak, limit int64) uint8 {
	if peak >= limit {
		return utilizationBuckets - 1
	}
	hi, lo := bits.Mul64(uint64(peak), utilizationBuckets-1)
	b, _ := bits.Div64(hi, lo, uint64(limit))
	return uint8(b)
}

// UsageRun is one finished run's contribution: its index coordinates and
// its dispatched tasks. A run that dispatched nothing has no Tasks.
type UsageRun struct {
	RunID     string
	CreatedAt time.Time // artifacts.RunIndexEntry.Time
	Orkflow   string
	Tasks     []UsageTask
}

// UsageEvidence is the population and evidence half of a response.
type UsageEvidence struct {
	RunsDispatched, RunsEvidenceComplete, RunsEvidenceMissing int64
	CoverageSince                                             time.Time // zero when no run has a record
}

// usageAccumulator is one row being folded, with the distinct runs it has
// seen.
type usageAccumulator struct {
	row            *orkstackv1.UsageRow
	runs           map[string]bool
	memory         utilizationSketch
	measuredPriced float64
	unmeasuredBy   [unmeasuredReasons]int64
}

func (a *usageAccumulator) add(runID string, task UsageTask) {
	if !a.runs[runID] {
		a.runs[runID] = true
		a.row.Runs++
	}
	a.row.Tasks++
	a.row.Attempts += task.Recorded
	a.row.TaskSeconds += task.TaskSeconds
	a.row.CpuSeconds += task.CPUSeconds
	a.row.GibSeconds += task.GiBSeconds
	a.row.GpuSeconds += task.GPUSeconds
	a.row.AttemptsWithoutResources += task.WithoutResources
	a.row.AttemptsUnmeasured += task.Unmeasured
	a.row.UnreportedAttempts += task.Unreported

	a.row.AttemptsMeasured += task.Measured
	a.row.MeasuredCpuSeconds += task.MeasuredCPUSeconds
	a.measuredPriced += task.MeasuredCPUPriced
	a.row.ReservedCpuSecondsMeasured += task.ReservedCPUMeasured
	a.row.MemoryPeakBytesMax = max(a.row.MemoryPeakBytesMax, task.MemoryPeakMax)
	for _, b := range task.MemoryBuckets {
		a.memory.add(b)
	}
	for reason, n := range task.UnmeasuredBy {
		a.unmeasuredBy[reason] += n
	}
}

// finish fills the row's derived measured fields: efficiency, the memory
// percentiles and the unmeasured counts in reason order.
func (a *usageAccumulator) finish() *orkstackv1.UsageRow {
	// An unset figure is "nothing to compare", never 0 %.
	if a.row.ReservedCpuSecondsMeasured > 0 {
		efficiency := a.measuredPriced / a.row.ReservedCpuSecondsMeasured
		a.row.CpuEfficiency = &efficiency
	}
	a.row.MemoryUtilizationAttempts = a.memory.n
	if a.memory.n > 0 {
		p50, p95 := a.memory.quantile(1, 2), a.memory.quantile(19, 20)
		a.row.MemoryUtilizationP50, a.row.MemoryUtilizationP95 = &p50, &p95
	}
	for reason, n := range a.unmeasuredBy {
		if n == 0 || reason == unmeasuredOther {
			continue
		}
		a.row.AttemptsUnmeasuredByReason = append(a.row.AttemptsUnmeasuredByReason,
			&orkstackv1.UnmeasuredReasonCount{Reason: orkstackv1.UnmeasuredReason(reason), Attempts: n})
	}
	// A reason this engine does not know is still counted: as UNSPECIFIED,
	// beside an older Orker's records.
	if other := a.unmeasuredBy[unmeasuredOther]; other > 0 {
		rows := a.row.AttemptsUnmeasuredByReason
		if len(rows) > 0 && rows[0].GetReason() == orkstackv1.UnmeasuredReason_UNMEASURED_REASON_UNSPECIFIED {
			rows[0].Attempts += other
		} else {
			a.row.AttemptsUnmeasuredByReason = append([]*orkstackv1.UnmeasuredReasonCount{{Attempts: other}}, rows...)
		}
	}
	return a.row
}

// utilizationSketch is a fixed-size histogram of memory utilization in
// 0.5 % buckets (utilizationBucket), the last holding everything at or
// above 100 %: a row's percentiles in constant space and the same on every
// replica, whatever order the attempts were folded in.
type utilizationSketch struct {
	n       int64
	buckets [utilizationBuckets]int64
}

// utilizationBuckets is [0, 0.5 %), …, [99.5 %, 100 %) and [100 %, ∞).
const utilizationBuckets = 201

func (s *utilizationSketch) add(b uint8) {
	s.buckets[min(int(b), utilizationBuckets-1)]++
	s.n++
}

// quantile is the num/den-quantile (the attempt at rank ⌈n × num/den⌉, in
// integers) as its bucket's upper edge, so it never understates a bucket;
// the last bucket, at or above the limit, reads 1. It is 0 with nothing
// added.
func (s *utilizationSketch) quantile(num, den int64) float64 {
	if s.n == 0 {
		return 0
	}
	rank := max((s.n*num+den-1)/den, 1)
	var seen int64
	for b, n := range s.buckets {
		seen += n
		if seen >= rank {
			return min(float64(b+1)/float64(utilizationBuckets-1), 1)
		}
	}
	return 1
}

// FoldUsage groups runs by the resolved dimensions (ORKFLOW, TASK, POOL; an
// empty list folds the total alone), orders rows totally, and returns the
// total, the rows and the evidence. A dimension outside those three is
// ignored: the handler validates group_by before calling.
//
// A task contributes to exactly one row: its run's Orkflow, its id, its pool.
// The total is the fold with no grouping, so every additive field of the rows
// sums to it. A partially observed run still contributes its observed tasks:
// real evidence is never discarded because a sibling's is gone.
func FoldUsage(runs []UsageRun, groupBy []orkstackv1.FailureGroupBy) (total *orkstackv1.UsageRow, rows []*orkstackv1.UsageRow, evidence UsageEvidence) {
	grouped := map[orkstackv1.FailureGroupBy]bool{}
	for _, dim := range groupBy {
		switch dim {
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL:
			grouped[dim] = true
		}
	}
	all := &usageAccumulator{row: &orkstackv1.UsageRow{}, runs: map[string]bool{}}
	groups := map[string]*usageAccumulator{}
	for _, run := range runs {
		if len(run.Tasks) == 0 {
			continue
		}
		evidence.RunsDispatched++
		complete, observed := true, false
		for _, task := range run.Tasks {
			if task.Observed {
				observed = true
			} else {
				complete = false
			}
			all.add(run.RunID, task)
			if len(grouped) == 0 {
				continue
			}
			row := &orkstackv1.UsageRow{}
			if grouped[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW] {
				row.OrkflowNamespace, row.OrkflowName = SplitOrkflowKey(run.Orkflow)
			}
			if grouped[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK] {
				row.TaskId = task.TaskID
			}
			if grouped[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL] {
				row.Pool = task.Pool
			}
			key := strings.Join([]string{row.OrkflowNamespace, row.OrkflowName, row.TaskId, row.Pool}, "\x00")
			acc, ok := groups[key]
			if !ok {
				acc = &usageAccumulator{row: row, runs: map[string]bool{}}
				groups[key] = acc
			}
			acc.add(run.RunID, task)
		}
		if complete {
			evidence.RunsEvidenceComplete++
		} else {
			evidence.RunsEvidenceMissing++
		}
		if observed && (evidence.CoverageSince.IsZero() || run.CreatedAt.Before(evidence.CoverageSince)) {
			evidence.CoverageSince = run.CreatedAt
		}
	}
	rows = make([]*orkstackv1.UsageRow, 0, len(groups))
	for _, acc := range groups {
		rows = append(rows, acc.finish())
	}
	sort.Slice(rows, func(i, j int) bool { return usageRowLess(rows[i], rows[j]) })
	return all.finish(), rows, evidence
}

// usageRowLess is the one row order: cpu_seconds, gib_seconds and
// task_seconds descending, then orkflow, task and pool ascending — total and
// clock-free, so two replicas produce the same table from the same bytes.
func usageRowLess(a, b *orkstackv1.UsageRow) bool {
	switch {
	case a.GetCpuSeconds() != b.GetCpuSeconds():
		return a.GetCpuSeconds() > b.GetCpuSeconds()
	case a.GetGibSeconds() != b.GetGibSeconds():
		return a.GetGibSeconds() > b.GetGibSeconds()
	case a.GetTaskSeconds() != b.GetTaskSeconds():
		return a.GetTaskSeconds() > b.GetTaskSeconds()
	case a.GetOrkflowName() != b.GetOrkflowName():
		return a.GetOrkflowName() < b.GetOrkflowName()
	case a.GetOrkflowNamespace() != b.GetOrkflowNamespace():
		return a.GetOrkflowNamespace() < b.GetOrkflowNamespace()
	case a.GetTaskId() != b.GetTaskId():
		return a.GetTaskId() < b.GetTaskId()
	default:
		return a.GetPool() < b.GetPool()
	}
}
