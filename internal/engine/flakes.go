package engine

import (
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The flake index is how often a dispatched task needed more than one
// attempt, across the finished runs of one window. A SUCCEEDED run that
// retried is invisible to every other stats number by construction; this is
// the read that makes it visible, and it is derived entirely from records
// that already exist: the archive index key (which runs), the archived body
// (which tasks were dispatched, and to which pool) and the Orker's per-attempt
// TaskAttempt records (how many attempts, where, and why each non-final one
// ended).
//
// Everything here is pure. The facts are read elsewhere; these functions only
// decide what one task of one run says (TaskSample) and how samples fold into
// groups (GroupFlakes), so the whole contract is testable without a store, a
// clock or Temporal.
//
// The one rule that matters most is the honest denominator: a dispatched task
// with no attempt record is EVIDENCE MISSING — reclaimed on the run's
// retention schedule, or a best-effort write that never landed — and is never
// counted as a task that ran once cleanly. Counting it as clean would make
// every rate trend toward health as a window ages, for purely mechanical
// reasons.

// FlakeExampleLimit is how many example runs a flake group carries.
const FlakeExampleLimit = 3

// TaskSample is one dispatched task of one finished run, with every fact the
// grouping needs read once.
type TaskSample struct {
	RunID     string
	CreatedAt time.Time // artifacts.RunIndexEntry.Time — the index stamp
	Orkflow   string    // Run.orkflow_name
	TaskID    string    // TaskRun.task_id
	Pool      string    // TaskRun.pool — non-empty by construction (dispatched)
	// Attempts is the highest attempt number anything recorded: the highest
	// stored record, or TaskRun.attempt, whichever is higher. It is 0 only
	// when Observed is false.
	Attempts int32
	// Records holds the stored records, ascending, deduplicated by attempt,
	// attempt >= 1 — the same filter taskAttemptTimeline applies.
	Records []*orkstackv1.TaskAttempt
	// Observed is len(Records) > 0. A dispatched task with no record is
	// evidence missing, never a task that ran once cleanly.
	Observed bool
}

// Retried reports whether the task needed more than one attempt.
func (s TaskSample) Retried() bool { return s.Attempts >= 2 }

// NonFinal returns the records below Attempts: the ones that did not
// complete the task and caused the next attempt to be scheduled.
func (s TaskSample) NonFinal() []*orkstackv1.TaskAttempt {
	out := make([]*orkstackv1.TaskAttempt, 0, len(s.Records))
	for _, record := range s.Records {
		if record.GetAttempt() < s.Attempts {
			out = append(out, record)
		}
	}
	return out
}

// Unreported is Attempts minus len(Records): attempts that left no record.
func (s TaskSample) Unreported() int32 {
	if missing := s.Attempts - int32(len(s.Records)); missing > 0 {
		return missing
	}
	return 0
}

// Dispatched reports whether a TaskRun reached an Orker: its pool is set
// (run.proto, TaskRun.pool: "Empty means the task never reached an Orker").
// A skipped task, a task canceled before dispatch and a task of a run whose
// start failed have no pool, no attempt and nothing to index.
func Dispatched(task *orkstackv1.TaskRun) bool {
	return task.GetPool() != ""
}

// SampleTask builds one sample. records is AttemptStore.ListAttempts' result
// for (run.id, task.task_id), or nil.
//
// TaskRun.attempt is a floor under the maximum and never trusted alone: it is
// 0 for a task Temporal timed out and the COMMITTING attempt for an adopted
// commit. The log store is deliberately not consulted — a scan over thousands
// of runs does not widen each task's timeline — so a task whose final
// attempts left neither a record nor a snapshot attempt number is undercounted,
// in the direction of fewer retries, never more.
func SampleTask(
	entry artifacts.RunIndexEntry,
	run *orkstackv1.Run,
	task *orkstackv1.TaskRun,
	records []*orkstackv1.TaskAttempt,
) TaskSample {
	sample := TaskSample{
		RunID:     entry.RunID,
		CreatedAt: entry.Time,
		Orkflow:   run.GetOrkflowName(),
		TaskID:    task.GetTaskId(),
		Pool:      task.GetPool(),
	}
	seen := make(map[int32]bool, len(records))
	for _, record := range records {
		if record.GetAttempt() < 1 || seen[record.GetAttempt()] {
			// The timeline's filter: not addressable, or a duplicate — the
			// first one wins.
			continue
		}
		seen[record.GetAttempt()] = true
		sample.Records = append(sample.Records, record)
	}
	sort.SliceStable(sample.Records, func(i, j int) bool {
		return sample.Records[i].GetAttempt() < sample.Records[j].GetAttempt()
	})
	sample.Observed = len(sample.Records) > 0
	if !sample.Observed {
		return sample
	}
	sample.Attempts = task.GetAttempt()
	if highest := sample.Records[len(sample.Records)-1].GetAttempt(); highest > sample.Attempts {
		sample.Attempts = highest
	}
	return sample
}

// NearestRank is the 1-based rank of the p-th percentile over n ascending
// samples: ceil(p*n/100), clamped to [1, n]. It is 0 when there are no
// samples. PercentileDuration and the flake index's attempt percentiles share
// it, so a p95 means one thing everywhere.
func NearestRank(n, p int) int {
	if n <= 0 {
		return 0
	}
	if p <= 0 {
		return 1
	}
	if p >= 100 {
		return n
	}
	rank := (p*n + 99) / 100 // ceil(p*n/100)
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return rank
}

// ResolveFlakeGroupBy validates a flake index's requested dimensions and
// resolves them: an empty list means TASK, and ORKFLOW is prepended when the
// caller did not name it, because a task id names a task only within its
// orkflow. CODE and CATEGORY are refused — they are what a group REPORTS as
// its dominant cause, not what it is keyed by — as are UNSPECIFIED, an
// unknown value and a duplicate.
func ResolveFlakeGroupBy(requested []orkstackv1.FailureGroupBy) ([]orkstackv1.FailureGroupBy, error) {
	if len(requested) == 0 {
		requested = []orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK}
	}
	seen := make(map[orkstackv1.FailureGroupBy]bool, len(requested))
	out := make([]orkstackv1.FailureGroupBy, 0, len(requested)+1)
	for _, dim := range requested {
		if _, known := orkstackv1.FailureGroupBy_name[int32(dim)]; !known {
			return nil, fmt.Errorf("group_by value %d is not a dimension", int32(dim))
		}
		switch dim {
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED:
			return nil, fmt.Errorf("group_by names %s, which is no dimension", dim)
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CATEGORY:
			return nil, fmt.Errorf("group_by names %s: a flake group reports its dominant "+
				"code and category; it cannot be keyed by them", dim)
		}
		if seen[dim] {
			return nil, fmt.Errorf("group_by names %s twice", dim)
		}
		seen[dim] = true
		out = append(out, dim)
	}
	if !seen[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW] {
		out = append([]orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW}, out...)
	}
	return out, nil
}

// normalizeFlakeGroupBy is ResolveFlakeGroupBy for an input the caller did not
// validate: the refused and invalid values are dropped rather than reported,
// so GroupFlakes never keys by CODE or CATEGORY whatever it is handed.
func normalizeFlakeGroupBy(requested []orkstackv1.FailureGroupBy) []orkstackv1.FailureGroupBy {
	kept := make([]orkstackv1.FailureGroupBy, 0, len(requested))
	seen := map[orkstackv1.FailureGroupBy]bool{}
	for _, dim := range requested {
		switch dim {
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE:
			if !seen[dim] {
				seen[dim] = true
				kept = append(kept, dim)
			}
		}
	}
	resolved, err := ResolveFlakeGroupBy(kept)
	if err != nil {
		// Unreachable: every kept value is one ResolveFlakeGroupBy accepts.
		return []orkstackv1.FailureGroupBy{
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
		}
	}
	return resolved
}

// FlakeBodyKeyed reports whether a grouping names no record-level dimension
// (ORKER, PHASE, IMAGE): then every sample belongs to exactly one group and a
// group's evidence-missing and unreported counts are exact.
func FlakeBodyKeyed(groupBy []orkstackv1.FailureGroupBy) bool {
	for _, dim := range groupBy {
		switch dim {
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE:
			return false
		}
	}
	return true
}

// FlakeTotals are the numbers a flake index carries over EVERY group, before
// any limit truncates the list.
type FlakeTotals struct {
	// RunsDispatched is how many distinct runs contributed a sample.
	RunsDispatched int64
	// RunsEvidenceComplete is how many of them hold a record for every task
	// they dispatched; RunsEvidenceMissing how many hold none for at least
	// one. The two sum to RunsDispatched.
	RunsEvidenceComplete int64
	RunsEvidenceMissing  int64
	// GroupsTotal is how many groups there were before limit.
	GroupsTotal int64
	// BodyKeyed is FlakeBodyKeyed(GroupBy).
	BodyKeyed bool
	// GroupBy is the dimensions actually grouped by: ORKFLOW explicit, an
	// empty request resolved to TASK.
	GroupBy []orkstackv1.FailureGroupBy
}

// flakeKey is a group's identity: the requested dimensions' values, every
// other field left zero. It is comparable, so it keys a map directly.
type flakeKey struct {
	orkflow string
	task    string
	image   string
	pool    string
	phase   string
	orker   string
}

// less orders two keys ascending, dimension by dimension in the enum's order
// (ORKFLOW, TASK, IMAGE, POOL, PHASE, ORKER). Unrequested dimensions are zero
// on both sides and never decide anything.
func (k flakeKey) less(o flakeKey) bool {
	switch {
	case k.orkflow != o.orkflow:
		return k.orkflow < o.orkflow
	case k.task != o.task:
		return k.task < o.task
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

type flakeDims struct {
	orkflow, task, pool, orker, phase, image bool
}

func (d flakeDims) bodyKey(s TaskSample) flakeKey {
	var key flakeKey
	if d.orkflow {
		key.orkflow = s.Orkflow
	}
	if d.task {
		key.task = s.TaskID
	}
	if d.pool {
		key.pool = s.Pool
	}
	return key
}

func (d flakeDims) recordKey(s TaskSample, record *orkstackv1.TaskAttempt) flakeKey {
	key := d.bodyKey(s)
	if d.orker {
		key.orker = record.GetOrkerId()
	}
	if d.phase {
		key.phase = record.GetPhase()
	}
	if d.image {
		key.image = record.GetImage().GetPinned()
	}
	return key
}

// flakeCause is the one triple a retry is attributed to.
type flakeCause struct {
	code     string
	category orkstackv1.FailureCategory
	phase    string
}

func (c flakeCause) less(o flakeCause) bool {
	switch {
	case c.code != o.code:
		return c.code < o.code
	case c.category != o.category:
		return c.category < o.category
	default:
		return c.phase < o.phase
	}
}

// flakeRun is one run's contribution to one group.
type flakeRun struct {
	position int // the run's first position in the scan
	created  time.Time
	attempts int32 // the maximum over member tasks
	retried  bool
}

type flakeBucket struct {
	key        flakeKey
	runs       map[string]*flakeRun
	missing    map[string]bool
	retries    int64
	unreported int64
	causes     map[flakeCause]int64
}

func (b *flakeBucket) run(s TaskSample, position int) *flakeRun {
	entry, ok := b.runs[s.RunID]
	if !ok {
		entry = &flakeRun{position: position, created: s.CreatedAt}
		b.runs[s.RunID] = entry
	}
	return entry
}

func (b *flakeBucket) retry(record *orkstackv1.TaskAttempt) {
	b.retries++
	b.causes[flakeCause{
		code:     record.GetError().GetCode(),
		category: record.GetError().GetCategory(),
		phase:    record.GetPhase(),
	}]++
}

func (b *flakeBucket) retried() int64 {
	var n int64
	for _, run := range b.runs {
		if run.retried {
			n++
		}
	}
	return n
}

// GroupFlakes folds samples into groups by the named dimensions, orders
// totally, and truncates to limit. It returns the groups and the totals the
// response carries, which are over EVERY group.
//
// groupBy is resolved as ResolveFlakeGroupBy resolves it (CODE, CATEGORY and
// invalid values are dropped rather than keyed by). A limit of zero or less
// keeps every group. The order is rate-defined first, retry_rate descending,
// runs_retried descending, runs_observed descending, then each keyed
// dimension ascending in the enum's order — so two replicas produce the same
// table from the same bytes and nothing depends on a clock or on map
// iteration.
func GroupFlakes(
	samples []TaskSample,
	groupBy []orkstackv1.FailureGroupBy,
	limit int,
) ([]*orkstackv1.FlakeGroup, FlakeTotals) {
	resolved := normalizeFlakeGroupBy(groupBy)
	var dims flakeDims
	for _, dim := range resolved {
		switch dim {
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW:
			dims.orkflow = true
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK:
			dims.task = true
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL:
			dims.pool = true
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER:
			dims.orker = true
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE:
			dims.phase = true
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE:
			dims.image = true
		}
	}
	totals := FlakeTotals{BodyKeyed: FlakeBodyKeyed(resolved), GroupBy: resolved}

	// Run-level evidence, for the response totals.
	positions := map[string]int{}
	runMissing := map[string]bool{}
	buckets := map[flakeKey]*flakeBucket{}
	bucketOf := func(key flakeKey) *flakeBucket {
		bucket, ok := buckets[key]
		if !ok {
			bucket = &flakeBucket{
				key:     key,
				runs:    map[string]*flakeRun{},
				missing: map[string]bool{},
				causes:  map[flakeCause]int64{},
			}
			buckets[key] = bucket
		}
		return bucket
	}

	for _, sample := range samples {
		position, known := positions[sample.RunID]
		if !known {
			position = len(positions)
			positions[sample.RunID] = position
			runMissing[sample.RunID] = false
		}
		if !sample.Observed {
			runMissing[sample.RunID] = true
			if totals.BodyKeyed {
				// The unobservable row: created for every dispatched task,
				// so a task whose evidence is all gone still appears — with
				// an undefined rate, never 0%.
				bucketOf(dims.bodyKey(sample)).missing[sample.RunID] = true
			}
			continue
		}
		if totals.BodyKeyed {
			bucket := bucketOf(dims.bodyKey(sample))
			run := bucket.run(sample, position)
			if sample.Attempts > run.attempts {
				run.attempts = sample.Attempts
			}
			if sample.Retried() {
				run.retried = true
			}
			for _, record := range sample.NonFinal() {
				bucket.retry(record)
			}
			bucket.unreported += int64(sample.Unreported())
			continue
		}
		// Record-keyed: each record carries a key, and the sample is a member
		// of every group some record of it carries. A retry is attributed to
		// the key of the NON-FINAL attempt — the one that failed and caused
		// the next — never to the attempt that then completed the task.
		carried := map[flakeKey]int32{}
		order := []flakeKey{}
		for _, record := range sample.Records {
			key := dims.recordKey(sample, record)
			if _, ok := carried[key]; !ok {
				order = append(order, key)
			}
			carried[key]++
			bucket := bucketOf(key)
			run := bucket.run(sample, position)
			if record.GetAttempt() < sample.Attempts {
				run.retried = true
				bucket.retry(record)
			}
		}
		for _, key := range order {
			run := buckets[key].runs[sample.RunID]
			if carried[key] > run.attempts {
				run.attempts = carried[key]
			}
		}
	}

	totals.RunsDispatched = int64(len(positions))
	for _, missing := range runMissing {
		if missing {
			totals.RunsEvidenceMissing++
		}
	}
	totals.RunsEvidenceComplete = totals.RunsDispatched - totals.RunsEvidenceMissing

	ordered := make([]*flakeBucket, 0, len(buckets))
	for _, bucket := range buckets {
		ordered = append(ordered, bucket)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		aObserved, bObserved := int64(len(a.runs)), int64(len(b.runs))
		if (aObserved > 0) != (bObserved > 0) {
			return aObserved > 0
		}
		aRetried, bRetried := a.retried(), b.retried()
		// Rates compared exactly, by cross-multiplication, so no float
		// rounding can decide an order.
		if left, right := aRetried*bObserved, bRetried*aObserved; left != right {
			return left > right
		}
		if aRetried != bRetried {
			return aRetried > bRetried
		}
		if aObserved != bObserved {
			return aObserved > bObserved
		}
		return a.key.less(b.key)
	})
	totals.GroupsTotal = int64(len(ordered))
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}

	groups := make([]*orkstackv1.FlakeGroup, 0, len(ordered))
	for _, bucket := range ordered {
		groups = append(groups, bucket.group(totals.BodyKeyed))
	}
	return groups, totals
}

// group renders one bucket.
func (b *flakeBucket) group(bodyKeyed bool) *orkstackv1.FlakeGroup {
	group := &orkstackv1.FlakeGroup{
		OrkflowName:   b.key.orkflow,
		TaskId:        b.key.task,
		Pool:          b.key.pool,
		OrkerId:       b.key.orker,
		Phase:         b.key.phase,
		ImagePinned:   b.key.image,
		RunsObserved:  int64(len(b.runs)),
		RetryAttempts: b.retries,
	}
	if bodyKeyed {
		group.RunsEvidenceMissing = int64(len(b.missing))
		group.UnreportedAttempts = b.unreported
	}

	attempts := make([]int32, 0, len(b.runs))
	retried := make([]*flakeRun, 0, len(b.runs))
	for _, run := range b.runs {
		attempts = append(attempts, run.attempts)
		if run.retried {
			retried = append(retried, run)
		}
	}
	group.RunsRetried = int64(len(retried))
	if n := len(attempts); n > 0 {
		sort.Slice(attempts, func(i, j int) bool { return attempts[i] < attempts[j] })
		group.RetryRateDefined = true
		group.RetryRate = float64(group.RunsRetried) / float64(n)
		group.P50Attempts = attempts[NearestRank(n, 50)-1]
		group.P95Attempts = attempts[NearestRank(n, 95)-1]
	}

	if len(b.causes) > 0 {
		var best flakeCause
		var bestCount int64
		for cause, count := range b.causes {
			if count > bestCount || (count == bestCount && cause.less(best)) {
				best, bestCount = cause, count
			}
		}
		group.DominantCode = best.code
		group.DominantCategory = best.category
		group.DominantPhase = best.phase
		group.DominantCount = bestCount
	}

	if len(retried) > 0 {
		// Newest created first; a tie keeps the scan's own (newest-first
		// index) order. Ids are carried, never compared (AGENTS.md §7).
		sort.Slice(retried, func(i, j int) bool {
			if !retried[i].created.Equal(retried[j].created) {
				return retried[i].created.After(retried[j].created)
			}
			return retried[i].position < retried[j].position
		})
		group.LastRetriedAt = timestamppb.New(retried[0].created)
		ids := make(map[*flakeRun]string, len(b.runs))
		for id, run := range b.runs {
			ids[run] = id
		}
		for i := 0; i < len(retried) && i < FlakeExampleLimit; i++ {
			group.ExampleRunIds = append(group.ExampleRunIds, ids[retried[i]])
		}
	}
	return group
}
