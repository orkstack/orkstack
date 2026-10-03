package engine

import (
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
}

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
	}
	return usage
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
	row  *orkstackv1.UsageRow
	runs map[string]bool
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
	a.row.AttemptsWithoutResources += task.WithoutResources
	a.row.AttemptsUnmeasured += task.Unmeasured
	a.row.UnreportedAttempts += task.Unreported
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
				row.OrkflowName = run.Orkflow
			}
			if grouped[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK] {
				row.TaskId = task.TaskID
			}
			if grouped[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL] {
				row.Pool = task.Pool
			}
			key := strings.Join([]string{row.OrkflowName, row.TaskId, row.Pool}, "\x00")
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
		rows = append(rows, acc.row)
	}
	sort.Slice(rows, func(i, j int) bool { return usageRowLess(rows[i], rows[j]) })
	return all.row, rows, evidence
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
	case a.GetTaskId() != b.GetTaskId():
		return a.GetTaskId() < b.GetTaskId()
	default:
		return a.GetPool() < b.GetPool()
	}
}
