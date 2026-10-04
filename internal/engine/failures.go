package engine

import (
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A failure fingerprint is the tuple of recorded facts that make one failed
// run the same failure as another: the orkflow, the task that failed first,
// its machine-readable code and AGENTS.md §4 category, and — when asked for —
// the image the final attempt actually ran, the pool, the Orker and the phase
// that attempt ended in.
//
// Everything here is pure. The facts are read elsewhere (the archive index,
// the archived run body, the attempt records); these functions only decide
// which fact is the fingerprint's and how fingerprints fold into groups, so
// the whole contract is testable without a store, a clock or Temporal.
//
// A fingerprint is EXACT equality on recorded fields. Nothing is inferred,
// normalised or clustered: an empty field stays empty, and a message is never
// a dimension.

// FailureExampleLimit is how many example runs a group carries.
const FailureExampleLimit = 3

// DefaultFailureGroupBy is the fingerprint a request that names no dimension
// asks for: which orkflow, which task, which code, which category. It returns
// a fresh slice, so no caller can edit another's default.
func DefaultFailureGroupBy() []orkstackv1.FailureGroupBy {
	return []orkstackv1.FailureGroupBy{
		orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW,
		orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
		orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE,
		orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CATEGORY,
	}
}

// NeedsAttemptEvidence reports whether a grouping names a dimension only the
// attempt records hold (IMAGE or PHASE).
func NeedsAttemptEvidence(groupBy []orkstackv1.FailureGroupBy) bool {
	for _, dim := range groupBy {
		if dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE ||
			dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE {
			return true
		}
	}
	return false
}

// FailureRecord is one failed run's fingerprint, every dimension filled
// whether or not the caller will group by it.
type FailureRecord struct {
	RunID     string
	CreatedAt time.Time // artifacts.RunIndexEntry.Time — the index stamp
	Orkflow   string
	TaskID    string // "" for a run-level failure
	Code      string
	Category  orkstackv1.FailureCategory
	Pool      string
	OrkerID   string
	Image     string // TaskAttempt.image.pinned
	Phase     string // TaskAttempt.phase
	// AttemptRead is whether an attempt record was looked for; AttemptMissing
	// is whether it was looked for and not found. Image and Phase are
	// meaningless unless AttemptRead && !AttemptMissing.
	AttemptRead    bool
	AttemptMissing bool
}

// FailureTotals are the numbers a breakdown carries over EVERY group, before
// any limit truncates the list.
type FailureTotals struct {
	// Grouped is sum(run_count) over every group: the records left after the
	// category filter.
	Grouped int64
	// GroupsTotal is how many groups there were.
	GroupsTotal int64
	// AttemptEvidenceMissing counts grouped records whose attempt record was
	// looked for and not found.
	AttemptEvidenceMissing int64
}

// PrimaryFailedTask is the task a failed run is attributed to: among the
// run's FAILED tasks, the one that finished first. A task with no finished_at
// sorts after every task that has one, and ties go to declaration order.
//
// Only FAILED counts. The tasks a failing run cancels around its failure are
// consequences, never the cause, whatever their clocks say. A run with no
// FAILED task has no primary task (nil): its failure is the run's own.
func PrimaryFailedTask(run *orkstackv1.Run) *orkstackv1.TaskRun {
	var primary *orkstackv1.TaskRun
	for _, task := range run.GetTaskRuns() {
		if task.GetStatus() != orkstackv1.Status_STATUS_FAILED {
			continue
		}
		// A FAILED fan-out is decided by the item that failed it: the
		// earliest-finished FAILED item stands for it. Its task_id is the
		// node's, so a fan-out's failures group under its task id, while its
		// code, category, pool and orker are the item's (spec each-fan-out
		// §14, failure-fingerprints).
		candidate := task
		if item := earliestFailedItem(task); item != nil {
			candidate = item
		}
		if primary == nil || finishedBefore(candidate, primary) {
			primary = candidate
		}
	}
	return primary
}

// earliestFailedItem is a fan-out row's earliest-finished FAILED item, or nil.
func earliestFailedItem(task *orkstackv1.TaskRun) *orkstackv1.TaskRun {
	var found *orkstackv1.TaskRun
	for _, item := range task.GetItems() {
		if item.GetStatus() != orkstackv1.Status_STATUS_FAILED {
			continue
		}
		if found == nil || finishedBefore(item, found) {
			found = item
		}
	}
	return found
}

// finishedBefore reports whether a finished strictly before b. Equal times
// are not "before", so the scan's declaration order breaks ties.
func finishedBefore(a, b *orkstackv1.TaskRun) bool {
	aTime, bTime := a.GetFinishedAt(), b.GetFinishedAt()
	switch {
	case aTime == nil:
		return false
	case bTime == nil:
		return true
	default:
		return aTime.AsTime().Before(bTime.AsTime())
	}
}

// FinalAttemptRecord picks the record that describes the task's final
// attempt out of an ascending ListAttempts result, or nil when there is none.
//
// The task run's own attempt number is preferred. It is 0 for a task Temporal
// timed out (the error carries no attempt), and it can name an attempt whose
// Orker died before filing a record; either way the highest-numbered record
// is the latest attempt anything wrote down, and it is used.
func FinalAttemptRecord(task *orkstackv1.TaskRun, records []*orkstackv1.TaskAttempt) *orkstackv1.TaskAttempt {
	if len(records) == 0 {
		return nil
	}
	if want := task.GetAttempt(); want > 0 {
		for _, record := range records {
			if record.GetAttempt() == want {
				return record
			}
		}
	}
	highest := records[0]
	for _, record := range records[1:] {
		if record.GetAttempt() > highest.GetAttempt() {
			highest = record
		}
	}
	return highest
}

// FingerprintOf builds one record. attempt is the record chosen by
// FinalAttemptRecord, or nil; attemptRead says whether one was looked for.
//
// A run with no FAILED task is a run-level failure: its code and category are
// the run's own, and it has no task, pool, Orker, image or phase. It is never
// counted as missing attempt evidence — there is no attempt to be missing.
func FingerprintOf(
	entry artifacts.RunIndexEntry,
	run *orkstackv1.Run,
	attempt *orkstackv1.TaskAttempt,
	attemptRead bool,
) FailureRecord {
	record := FailureRecord{
		RunID:     entry.RunID,
		CreatedAt: entry.Time,
		Orkflow:   run.GetOrkflowName(),
	}
	if record.RunID == "" {
		record.RunID = run.GetId()
	}
	if record.Orkflow == "" {
		// The key names the orkflow the run was indexed under; a body that
		// somehow lacks one is still that orkflow's run.
		record.Orkflow = entry.Orkflow
	}
	task := PrimaryFailedTask(run)
	if task == nil {
		record.Code = run.GetError().GetCode()
		record.Category = run.GetError().GetCategory()
		return record
	}
	record.TaskID = task.GetTaskId()
	record.Code = task.GetError().GetCode()
	record.Category = task.GetError().GetCategory()
	record.Pool = task.GetPool()
	record.OrkerID = task.GetOrkerId()
	if !attemptRead {
		return record
	}
	record.AttemptRead = true
	if attempt == nil {
		record.AttemptMissing = true
		return record
	}
	record.Image = attempt.GetImage().GetPinned()
	record.Phase = attempt.GetPhase()
	return record
}

// failureKey is a group's identity: the requested dimensions' values, every
// other field left zero. It is comparable, so it keys a map directly.
type failureKey struct {
	orkflow  string
	task     string
	code     string
	category orkstackv1.FailureCategory
	image    string
	pool     string
	phase    string
	orker    string
}

func keyOf(record FailureRecord, dims map[orkstackv1.FailureGroupBy]bool) failureKey {
	var key failureKey
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW] {
		key.orkflow = record.Orkflow
	}
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK] {
		key.task = record.TaskID
	}
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE] {
		key.code = record.Code
	}
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CATEGORY] {
		key.category = record.Category
	}
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE] {
		key.image = record.Image
	}
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL] {
		key.pool = record.Pool
	}
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE] {
		key.phase = record.Phase
	}
	if dims[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER] {
		key.orker = record.OrkerID
	}
	return key
}

// less orders two keys ascending, dimension by dimension in the enum's order.
// Unrequested dimensions are zero on both sides and never decide anything.
func (k failureKey) less(o failureKey) bool {
	switch {
	case k.orkflow != o.orkflow:
		return k.orkflow < o.orkflow
	case k.task != o.task:
		return k.task < o.task
	case k.code != o.code:
		return k.code < o.code
	case k.category != o.category:
		return k.category < o.category
	case k.image != o.image:
		return k.image < o.image
	case k.pool != o.pool:
		return k.pool < o.pool
	case k.phase != o.phase:
		return k.phase < o.phase
	default:
		return k.orker < o.orker
	}
}

// failureBucket accumulates one group. members are indices into the scan, in
// scan order, which is what breaks a tie between two runs created at the same
// instant without ever comparing their ids.
type failureBucket struct {
	key     failureKey
	members []int
	first   time.Time
	last    time.Time
	missing int64
}

// GroupFailures folds records into groups by the named dimensions, applies
// the category filter, orders totally, and truncates to limit. It returns the
// groups and the totals the response carries, which are over EVERY group.
//
// An empty groupBy is DefaultFailureGroupBy. A limit of zero or less keeps
// every group. The order is run_count descending, then last_seen descending,
// then each grouped dimension ascending in the enum's order — category by its
// numeric value — so two replicas produce the same table from the same bytes
// and nothing depends on a clock or on map iteration.
func GroupFailures(
	records []FailureRecord,
	groupBy []orkstackv1.FailureGroupBy,
	category orkstackv1.FailureCategory,
	limit int,
) ([]*orkstackv1.FailureGroup, FailureTotals) {
	if len(groupBy) == 0 {
		groupBy = DefaultFailureGroupBy()
	}
	dims := make(map[orkstackv1.FailureGroupBy]bool, len(groupBy))
	for _, dim := range groupBy {
		dims[dim] = true
	}

	var totals FailureTotals
	buckets := map[failureKey]*failureBucket{}
	for i, record := range records {
		if category != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED && record.Category != category {
			continue
		}
		totals.Grouped++
		key := keyOf(record, dims)
		bucket, ok := buckets[key]
		if !ok {
			bucket = &failureBucket{key: key, first: record.CreatedAt, last: record.CreatedAt}
			buckets[key] = bucket
		}
		bucket.members = append(bucket.members, i)
		if record.CreatedAt.Before(bucket.first) {
			bucket.first = record.CreatedAt
		}
		if record.CreatedAt.After(bucket.last) {
			bucket.last = record.CreatedAt
		}
		if record.AttemptRead && record.AttemptMissing {
			bucket.missing++
			totals.AttemptEvidenceMissing++
		}
	}

	ordered := make([]*failureBucket, 0, len(buckets))
	for _, bucket := range buckets {
		ordered = append(ordered, bucket)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if len(a.members) != len(b.members) {
			return len(a.members) > len(b.members)
		}
		if !a.last.Equal(b.last) {
			return a.last.After(b.last)
		}
		return a.key.less(b.key)
	})
	totals.GroupsTotal = int64(len(ordered))
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}

	groups := make([]*orkstackv1.FailureGroup, 0, len(ordered))
	for _, bucket := range ordered {
		groups = append(groups, &orkstackv1.FailureGroup{
			OrkflowName:            bucket.key.orkflow,
			TaskId:                 bucket.key.task,
			Code:                   bucket.key.code,
			Category:               bucket.key.category,
			ImagePinned:            bucket.key.image,
			Pool:                   bucket.key.pool,
			Phase:                  bucket.key.phase,
			OrkerId:                bucket.key.orker,
			RunCount:               int64(len(bucket.members)),
			FirstSeen:              timestamppb.New(bucket.first),
			LastSeen:               timestamppb.New(bucket.last),
			ExampleRunIds:          examples(records, bucket.members),
			AttemptEvidenceMissing: bucket.missing,
		})
	}
	return groups, totals
}

// examples is a group's newest runs by creation time, at most
// FailureExampleLimit, ties broken by scan position. Ids are carried, never
// compared (AGENTS.md §7).
func examples(records []FailureRecord, members []int) []string {
	newest := append([]int(nil), members...)
	sort.SliceStable(newest, func(i, j int) bool {
		return records[newest[i]].CreatedAt.After(records[newest[j]].CreatedAt)
	})
	if len(newest) > FailureExampleLimit {
		newest = newest[:FailureExampleLimit]
	}
	out := make([]string, len(newest))
	for i, index := range newest {
		out[i] = records[index].RunID
	}
	return out
}
