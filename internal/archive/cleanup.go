// Package archive retires a finished run at its own OrkStack namespace's run
// retention (or the deployment's maximum, for a namespace that sets none),
// deleting the Temporal execution explicitly rather than waiting for it to
// disappear on its own, then reclaims that run's OrkStack-owned storage.
//
// Cleanup deliberately removes only metadata/logs/{run-id}/,
// metadata/attempts/{run-id}/, and {namespace}/{run-id}/ under
// metadata/commits/, metadata/attested/, metadata/snapshots/ and
// metadata/run-context/. The snapshot prefix matters as
// much as the others: every lifecycle emit externalizes one run snapshot,
// and once the execution's history is gone, nothing else can ever reach
// them — reads for a retired run fall through to the archive body, never to
// snapshots. The attempt prefix holds the per-attempt records the Orker wrote
// (pkg/artifacts/attempts.go); they are diagnosis for the logs they sit beside,
// so they are reclaimed with them and never outlive them.
// Workload artifact bytes are not deleted here. On the local backend
// (objects/) a key is name-and-digest with no run id, so one object can be
// referenced by another live or retained run, and deleting it by a run
// prefix could destroy that run's committed result. On s3 (orkflows/) the key
// embeds the producing run, but a memo hit's commit in a later run still
// references it. The archive keeps reference markers for every object, so a
// shared object can be reclaimed once no archived run claims it — that is
// artifacts.DeleteUnreferencedObjects, a scan of the whole reference index
// rather than a delete under one run's prefixes, so it is not part of the
// per-run pass; a bucket lifecycle rule is the documented reclaimer
// (docs/STORAGE.md, "Reclaiming space").
// Archive bodies and indexes are never deleted.
//
// A finished run this worker's index walk cannot see, because it never archived
// itself, gets its archive from Sweeper (sweep.go), which runs whether or not
// this worker does.
//
// The cluster's own retention TTL on the single Temporal namespace behind the
// deployment is a BACKSTOP, not the mechanism: RetentionController keeps it
// pinned to the deployment maximum plus an enforcement margin
// (ORK_CLEANUP_RETENTION_BUFFER), strictly above every namespace's own
// retention, so it can never fire before this worker's own active retirement
// would. See retention.go.
package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	DefaultCleanupInterval = time.Hour
	DefaultRetentionBuffer = 24 * time.Hour
	DefaultBatchSize       = 100
)

// CleanupConfig controls the background cleanup worker. TemporalNamespace is
// the deployment's Temporal partition, not an OrkStack namespace from an
// archived run. Enabled defaults false because cleanup deletes durable data
// and must be explicitly opted into — and it is the only thing that makes a
// namespace's own run_retention take effect at all; a deployment that never
// enables it keeps every run for the enforced backstop
// (RetentionController.Enforced) regardless of what any namespace asks for.
type CleanupConfig struct {
	Enabled           bool
	TemporalNamespace string
	Interval          time.Duration
	// RetentionBuffer is the enforcement margin: how far past the deployment
	// maximum run retention the backstop TTL sits (see RetentionController).
	RetentionBuffer time.Duration
	BatchSize       int
	// ArtifactGC is cleanup.artifact_gc. With dry_run or delete,
	// retirement keeps a run's reference markers and run-objects list for
	// the GC to release (rule 1); off releases them at retirement.
	ArtifactGC ReclaimMode
	// ArtifactGCGrace is G, at least MinReclaimGrace.
	ArtifactGCGrace time.Duration
	// ArtifactGCBatch is how many objects one GC pass examines.
	ArtifactGCBatch int
}

// CleanupConfigFromEnv reads cleanup's ORK_* configuration. Invalid values use
// their safe defaults, matching the engine's existing duration configuration.
func CleanupConfigFromEnv() CleanupConfig {
	return CleanupConfig{
		Enabled:         envBool("ORK_CLEANUP_ENABLED", false),
		Interval:        envDuration("ORK_CLEANUP_INTERVAL", DefaultCleanupInterval),
		RetentionBuffer: envDuration("ORK_CLEANUP_RETENTION_BUFFER", DefaultRetentionBuffer),
		BatchSize:       envPositiveInt("ORK_CLEANUP_BATCH_SIZE", DefaultBatchSize),
		ArtifactGC:      envReclaimMode("ORK_CLEANUP_ARTIFACT_GC"),
		ArtifactGCGrace: envDuration("ORK_CLEANUP_ARTIFACT_GC_GRACE", DefaultReclaimGrace),
		ArtifactGCBatch: envPositiveInt("ORK_CLEANUP_ARTIFACT_GC_BATCH", DefaultReclaimBatch),
	}
}

// envReclaimMode reads the GC mode. An unknown value is dry_run, the mode
// that deletes nothing; the configuration loader refuses it before here.
func envReclaimMode(key string) ReclaimMode {
	mode, err := ParseReclaimMode(os.Getenv(key))
	if err != nil {
		return ReclaimDryRun
	}
	return mode
}

func envBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envPositiveInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

// doubleSaturating returns 2*n, or the largest int when that would wrap.
//
// BatchSize comes from ORK_CLEANUP_BATCH_SIZE and envPositiveInt only checks
// that it parses positive, so a value above MaxInt/2 doubles into a NEGATIVE
// page limit. Every limit check downstream reads a non-positive limit as "no
// limit", so the wrap would silently turn a bounded page into an unbounded
// one. Saturating keeps an absurd batch size absurd instead of inverting its
// meaning.
func doubleSaturating(n int) int {
	if n > math.MaxInt/2 {
		return math.MaxInt
	}
	return n * 2
}

// RunRetirer is the fail-closed cluster boundary cleanup needs: the same
// admin seam RetentionController uses (NamespaceRetention,
// SetNamespaceRetention — for the per-iteration backstop reconcile), plus the
// active lookup/delete pair.
type RunRetirer interface {
	RetentionAdmin
	// LookupWorkflow reports whether the cluster still holds workflowID and,
	// when it does, when the execution closed. A found execution with a zero
	// closeTime is still running.
	LookupWorkflow(ctx context.Context, namespace, workflowID string) (found bool, closeTime time.Time, err error)
	// DeleteWorkflow removes one finished execution now. An execution the
	// cluster no longer holds is not an error.
	DeleteWorkflow(ctx context.Context, namespace, workflowID string) error
}

// NamespaceLister is the registry seam cleanup needs to resolve each run's
// own retention. It is the same method set engine.NamespaceRegistry
// declares, restated here so this package never imports internal/engine.
type NamespaceLister interface {
	List(ctx context.Context) ([]*orkstackv1.Namespace, error)
}

// RunFinishReader dates a run from its archive body: when it finished, as the
// run itself recorded it. With no execution left in the cluster, the body is
// the only record of that, so cleanup reads it before it retires a run whose
// execution is missing. A body that is not there, cannot be read or records
// no finish is an error, never a zero time.
type RunFinishReader interface {
	RunFinishedAt(ctx context.Context, runID string) (time.Time, error)
}

// CleanupObserver receives each pass as it ends: its tally, and what each
// namespace's runs came to. It is how a pass reaches a metric.
type CleanupObserver interface {
	ObserveCleanup(ctx context.Context, pass CleanupPass)
}

// MissingHoldShare is the share of a pass's lookups that may come back missing
// before their run retention without holding the pass: above it, the pass
// retires no missing execution at all. More than a tenth missing early is a
// restored or damaged cluster, or a replica pointed at the wrong one, not the
// backstop TTL at work, which only ever removes runs already past retention.
const MissingHoldShare = 0.10

// CleanupStats describes one complete scan.
type CleanupStats struct {
	RunsScanned    int
	RunsCleaned    int
	ObjectsDeleted int
	BytesReclaimed int64
	Errors         int
	// RunsFailed counts the runs whose examination or retirement failed; they
	// are left as they were and tried again by a later pass. Errors also
	// counts failures that are no run's (the backstop reconcile).
	RunsFailed int
	// Processed counts the runs this pass examined against the cluster: the
	// runs that count toward BatchSize.
	Processed int
	// LookedUp counts the runs the cluster answered a lookup for.
	LookedUp int
	// MissingNotDue counts runs whose execution the cluster does not hold
	// although their own retention, counted from when they finished, has not
	// passed. They are kept: nothing removes an execution early but a damaged
	// cluster or the wrong one.
	MissingNotDue int
	// MissingHeld counts missing executions past their retention that this
	// pass did not retire, because more than MissingHoldShare of its lookups
	// were missing early.
	MissingHeld int
}

// The outcome a run a pass decided on ends in (ork.cleanup.runs). A run the
// pass left for a later one, finished but not yet due or held with every
// missing execution, has none.
const (
	// CleanupRunRetired: its execution and run-scoped storage were removed.
	CleanupRunRetired = "retired"
	// CleanupRunFailed: a step of its examination or retirement failed.
	CleanupRunFailed = "failed"
	// CleanupRunNotArchived: due, but its archive body is not written yet.
	CleanupRunNotArchived = "not_archived"
	// CleanupRunStillRunning: archived, yet the cluster says it is executing.
	CleanupRunStillRunning = "still_running"
	// CleanupRunNotFoundNotDue: its execution is missing before its retention.
	CleanupRunNotFoundNotDue = "not_found_not_due"
)

// The step a cleanup failure is counted under (ork.cleanup.errors).
const (
	CleanupStepPolicy       = "policy"
	CleanupStepNamespaces   = "namespaces"
	CleanupStepBackstop     = "backstop"
	CleanupStepList         = "list"
	CleanupStepState        = "state"
	CleanupStepLookup       = "lookup"
	CleanupStepArchiveCheck = "archive_check"
	CleanupStepRetire       = "retire"
	CleanupStepDelete       = "delete"
	CleanupStepMark         = "mark"
)

// How a cleanup pass ended (ork.cleanup.passes).
const (
	// CleanupPassCompleted: it walked to the end of the index.
	CleanupPassCompleted = "completed"
	// CleanupPassDrain: it stopped at a full batch, and the next pass
	// continues where it stopped.
	CleanupPassDrain = "drain"
	// CleanupPassFailed: it could not walk the index at all, or stopped
	// partway because the walk itself failed.
	CleanupPassFailed = "failed"
)

var (
	cleanupRunOutcomes = []string{CleanupRunRetired, CleanupRunFailed, CleanupRunNotArchived,
		CleanupRunStillRunning, CleanupRunNotFoundNotDue}
	cleanupSteps = []string{CleanupStepPolicy, CleanupStepNamespaces, CleanupStepBackstop, CleanupStepList,
		CleanupStepState, CleanupStepLookup, CleanupStepArchiveCheck, CleanupStepRetire, CleanupStepDelete, CleanupStepMark}
	cleanupPassOutcomes = []string{CleanupPassCompleted, CleanupPassDrain, CleanupPassFailed}
)

// CleanupRunOutcomes, CleanupSteps and CleanupPassOutcomes are the closed
// sets of those values, each a fresh copy.
func CleanupRunOutcomes() []string  { return slices.Clone(cleanupRunOutcomes) }
func CleanupSteps() []string        { return slices.Clone(cleanupSteps) }
func CleanupPassOutcomes() []string { return slices.Clone(cleanupPassOutcomes) }

// ValidCleanupRunOutcome and ValidCleanupStep report whether a value is in
// its closed set.
func ValidCleanupRunOutcome(s string) bool { return slices.Contains(cleanupRunOutcomes, s) }
func ValidCleanupStep(s string) bool       { return slices.Contains(cleanupSteps, s) }

// CleanupNamespaceTally is what one namespace's runs came to in a pass.
type CleanupNamespaceTally struct {
	// Runs counts the runs by outcome.
	Runs map[string]int
	// Objects and Bytes are the run-scoped storage the pass removed.
	Objects int
	Bytes   int64
}

// CleanupPass is one pass as its observer is handed it.
type CleanupPass struct {
	CleanupStats
	// Outcome is CleanupPassCompleted, CleanupPassDrain or CleanupPassFailed.
	Outcome  string
	Duration time.Duration
	// Namespaces holds each producer namespace's tally, keyed by its
	// resolved name.
	Namespaces map[string]CleanupNamespaceTally
	// StepErrors counts each failure Errors counts under the step that
	// failed; they sum to Errors.
	StepErrors map[string]int
	// WalkComplete says this pass ended a walk of the whole index, drained
	// across passes or not. Only such a pass sets RetentionLag.
	WalkComplete bool
	// LagUnknown says the walk could not tell whether some run was due (its
	// cleanup state or its execution could not be read), so it sets no
	// RetentionLag: an outage is never reported as no lag.
	LagUnknown bool
	// RetentionLag holds, per namespace, how far past its own due time the
	// most overdue run the walk left unretired is. A namespace whose due runs
	// were all retired is absent.
	RetentionLag map[string]time.Duration
}

// recordRun counts one decided run under its namespace.
func (p *CleanupPass) recordRun(namespace, outcome string) {
	if p.Namespaces == nil {
		p.Namespaces = map[string]CleanupNamespaceTally{}
	}
	ns := artifacts.ResolveNamespace(namespace)
	tally := p.Namespaces[ns]
	if tally.Runs == nil {
		tally.Runs = map[string]int{}
	}
	tally.Runs[outcome]++
	p.Namespaces[ns] = tally
}

// recordReclaimed counts storage a retirement removed under its namespace.
func (p *CleanupPass) recordReclaimed(namespace string, objects int, bytes int64) {
	if objects == 0 && bytes == 0 {
		return
	}
	if p.Namespaces == nil {
		p.Namespaces = map[string]CleanupNamespaceTally{}
	}
	ns := artifacts.ResolveNamespace(namespace)
	tally := p.Namespaces[ns]
	tally.Objects += objects
	tally.Bytes += bytes
	p.Namespaces[ns] = tally
}

// stepFailed counts one failure under its step.
func (p *CleanupPass) stepFailed(step string) {
	p.Errors++
	if p.StepErrors == nil {
		p.StepErrors = map[string]int{}
	}
	p.StepErrors[step]++
}

// missingHeld reports whether a pass's missing-but-due runs are held. The
// share is the pass's own, over at most BatchSize lookups: a small batch holds
// sooner, which only delays a retirement. A run it cannot date is not in the
// share; it is never retired either way.
func (s CleanupStats) missingHeld() bool {
	return s.MissingNotDue > 0 && float64(s.MissingNotDue) > MissingHoldShare*float64(s.LookedUp)
}

// Cleaner periodically retires finished runs at their own namespace's run
// retention and reclaims their run-scoped storage.
type Cleaner struct {
	config     CleanupConfig
	archive    artifacts.RunArchive
	bodies     RunBodyChecker
	cluster    RunRetirer
	namespaces NamespaceLister
	retention  *RetentionController
	finish     RunFinishReader
	observer   CleanupObserver
	// walkLag is the walk in progress's RetentionLag, reset when a pass
	// starts from the start of the index.
	walkLag map[string]time.Duration
	// walkLagUnknown says the walk in progress could not tell whether some
	// run was due: its lag is then unknown, never reported as none.
	walkLagUnknown bool
	logger         *slog.Logger
	now            func() time.Time
	// holdMissing holds every missing execution's retirement, in every pass,
	// until one whole walk of the index has had no pass over
	// MissingHoldShare; walkOverShare is whether the current walk has had
	// one. A pass sees at most BatchSize runs, so without this a batch of
	// only due runs, read from the same wrong or damaged cluster as the batch
	// before it, would not be held. Both are process-local, as a cordon is:
	// a restart forgets them, and each run is still checked against its own
	// retention.
	holdMissing   atomic.Bool
	walkOverShare bool
	// ticker paces Run; a test replaces it to drive passes by hand.
	ticker func(time.Duration) (<-chan time.Time, func())
}

// NewCleaner constructs a cleanup worker.
func NewCleaner(
	config CleanupConfig,
	archive artifacts.RunArchive,
	cluster RunRetirer,
	namespaces NamespaceLister,
	retention *RetentionController,
	logger *slog.Logger,
) *Cleaner {
	if config.Interval <= 0 {
		config.Interval = DefaultCleanupInterval
	}
	if config.RetentionBuffer <= 0 {
		config.RetentionBuffer = DefaultRetentionBuffer
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultBatchSize
	}
	if config.ArtifactGC == "" {
		config.ArtifactGC = DefaultReclaimMode
	}
	if logger == nil {
		logger = slog.Default()
	}
	if config.RetentionBuffer < 2*config.Interval {
		// The margin's only job is to absorb worker downtime and backlog
		// between passes; one shorter than two passes cannot do that.
		logger.Warn("cleanup retention buffer is shorter than two cleanup intervals; the backstop may fire before this worker catches up",
			"retention_buffer", config.RetentionBuffer, "interval", config.Interval)
	}
	c := &Cleaner{
		config: config, archive: archive, bodies: archive, cluster: cluster,
		namespaces: namespaces, retention: retention, logger: logger, now: time.Now,
		ticker: func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		},
	}
	// An archive that can date its own runs needs no separate reader.
	if r, ok := archive.(RunFinishReader); ok {
		c.finish = r
	}
	return c
}

// WithFinishReader sets how a run whose execution is missing is dated. Without
// one, no missing execution is ever retired.
func (c *Cleaner) WithFinishReader(r RunFinishReader) *Cleaner {
	if r != nil {
		c.finish = r
	}
	return c
}

// WithBodies sets how the cleaner asks whether a run's archive is whole before
// its execution is deleted (NewestEndings, for runs that may be resumed).
func (c *Cleaner) WithBodies(b RunBodyChecker) *Cleaner {
	if b != nil {
		c.bodies = b
	}
	return c
}

// resumed reports whether a resume marked the run.
func (c *Cleaner) resumed(ctx context.Context, runID string) (bool, error) {
	asker, ok := c.bodies.(interface {
		Resumed(ctx context.Context, runID string) (bool, error)
	})
	if !ok {
		return false, nil // an archive with one ending a run resumes none
	}
	return asker.Resumed(ctx, runID)
}

// WithObserver sets where each pass's tally goes. Nil keeps the logs only.
func (c *Cleaner) WithObserver(o CleanupObserver) *Cleaner {
	c.observer = o
	return c
}

// WithClock replaces the wall clock used to decide whether an archived run's
// close time is past its namespace's own retention. It keeps integration
// tests deterministic without waiting for a production-length retention
// window.
func (c *Cleaner) WithClock(now func() time.Time) *Cleaner {
	if now != nil {
		c.now = now
	}
	return c
}

// Run scans immediately, then periodically, until ctx is canceled.
//
// A pass that stops because it processed a full BatchSize has left a backlog
// behind it, so the next pass starts at once, resuming the index walk after
// the last run the previous pass examined, instead of waiting a whole
// Interval: a backlog drains at the store's speed, BatchSize runs at a time.
// The first pass that reaches the end of the index returns Run to the ticker,
// and the next ticked pass walks from the start again, so a run that was not
// yet due when a drain passed it is examined once it is.
//
// A pass that mostly failed does not drain at once: during an outage every
// lookup fails, each failed run still counts toward the batch, and draining
// would walk the whole index against a cluster or store that is not
// answering. It waits for the next tick instead and resumes where it stopped.
// A pass in which one run fails among others that did not still drains, so
// one run that keeps failing does not slow the walk behind it.
func (c *Cleaner) Run(ctx context.Context) {
	tick, stop := c.ticker(c.config.Interval)
	defer stop()
	resume, failed := c.runAndLog(ctx, "")
	for {
		if resume != "" && !failed {
			if ctx.Err() != nil {
				return
			}
			resume, failed = c.runAndLog(ctx, resume)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-tick:
			resume, failed = c.runAndLog(ctx, resume)
		}
	}
}

// runAndLog runs one pass after startAfter and logs it. It returns where a
// drain continues — the index key after which the next pass starts, or ""
// when this pass reached the end of the index — and whether the pass failed.
func (c *Cleaner) runAndLog(ctx context.Context, startAfter string) (string, bool) {
	c.logger.Info("artifact cleanup iteration started", "drain", startAfter != "")
	detail, resume, err := c.passDetailed(ctx, startAfter)
	stats := detail.CleanupStats
	backOff := err != nil && 2*stats.RunsFailed >= stats.Processed
	// A pass the engine's shutdown stopped is no failure of cleanup's, so
	// it is logged and never observed.
	if c.observer != nil && ctx.Err() == nil {
		c.observer.ObserveCleanup(ctx, detail)
	}
	args := []any{
		"runs_scanned", stats.RunsScanned,
		"runs_cleaned", stats.RunsCleaned,
		"objects_deleted", stats.ObjectsDeleted,
		"bytes_reclaimed", stats.BytesReclaimed,
		"errors", stats.Errors,
		"runs_missing_not_due", stats.MissingNotDue,
		"runs_missing_held", stats.MissingHeld,
	}
	if err != nil {
		args = append(args, "error", err)
		c.logger.Error("artifact cleanup iteration completed", args...)
		return resume, backOff
	}
	c.logger.Info("artifact cleanup iteration completed", args...)
	return resume, false
}

// RunOnce performs one fail-closed scan from the start of the index. Per-run
// failures are joined and do not prevent later eligible runs from being
// processed.
func (c *Cleaner) RunOnce(ctx context.Context) (CleanupStats, error) {
	stats, _, err := c.pass(ctx, "")
	return stats, err
}

// pass is RunOnce starting after the index key startAfter ("" is the start).
// resume is the index key of the last run it examined when it stopped
// because it had processed a full BatchSize — where a drain continues — and
// "" when it reached the end of the index or stopped for any other reason.
func (c *Cleaner) pass(ctx context.Context, startAfter string) (CleanupStats, string, error) {
	p, resume, err := c.passDetailed(ctx, startAfter)
	return p.CleanupStats, resume, err
}

// passDetailed is pass, returning the pass as its observer is handed it.
func (c *Cleaner) passDetailed(ctx context.Context, startAfter string) (p CleanupPass, resume string, err error) {
	start := c.now()
	defer func() {
		p.Duration = c.now().Sub(start)
		switch {
		case p.Outcome != "":
		case resume != "":
			p.Outcome = CleanupPassDrain
		default:
			p.Outcome = CleanupPassCompleted
		}
	}()
	stats := &p.CleanupStats
	// failed ends the pass before it walked: the walk itself failed.
	failed := func(e error) (CleanupPass, string, error) {
		p.Outcome = CleanupPassFailed
		return p, "", e
	}
	if c.archive == nil || c.cluster == nil || c.namespaces == nil || c.retention == nil {
		return failed(fmt.Errorf("archive cleanup: worker is not fully configured"))
	}
	if c.config.TemporalNamespace == "" {
		return failed(fmt.Errorf("archive cleanup: Temporal namespace is empty"))
	}
	policy, err := c.retention.Policy(ctx)
	if err != nil {
		p.stepFailed(CleanupStepPolicy)
		c.logger.Error("artifact cleanup retention policy read failed", "error", err)
		return failed(fmt.Errorf("archive cleanup: read retention policy: %w", err))
	}
	max := policy.GetMaxRunRetention().AsDuration()
	if err := policy.GetMaxRunRetention().CheckValid(); err != nil || max <= 0 {
		// Only a write checks the policy. One read back with no positive
		// maximum would make every finished run due at once, and set the
		// backstop to the margin alone, so it decides nothing.
		p.stepFailed(CleanupStepPolicy)
		c.logger.Error("artifact cleanup read a retention policy with no positive maximum; deleting nothing",
			"max_run_retention", policy.GetMaxRunRetention())
		return failed(fmt.Errorf("archive cleanup: retention policy has no positive maximum run retention (%v)", policy.GetMaxRunRetention()))
	}
	docs, err := c.namespaces.List(ctx)
	if err != nil {
		p.stepFailed(CleanupStepNamespaces)
		c.logger.Error("artifact cleanup namespace list failed", "error", err)
		return failed(fmt.Errorf("archive cleanup: list namespaces: %w", err))
	}
	byName := make(map[string]time.Duration, len(docs))
	minRetention := max
	for _, ns := range docs {
		r := EffectiveRunRetention(ns, max)
		byName[artifacts.ResolveNamespace(ns.GetName())] = r
		if r < minRetention {
			minRetention = r
		}
	}
	retentionFor := func(namespace string) time.Duration {
		if r, ok := byName[artifacts.ResolveNamespace(namespace)]; ok {
			return r
		}
		return max
	}

	// The backstop reconcile is best-effort and never blocks retirement: if
	// it fails, active deletion below is still safe (it never depends on the
	// cluster's own TTL), and the failure is the alarm an operator needs.
	if err := c.retention.Reconcile(ctx, max); err != nil {
		p.stepFailed(CleanupStepBackstop)
		c.logger.Error("artifact cleanup backstop reconcile failed", "error", err)
	}

	now := c.now().UTC()
	// The prefilter bounds CREATION time and creation never exceeds close
	// time, so every run whose CLOSE time is past any namespace's cutoff was
	// created before this prefilter — it is a superset, never a gate. The
	// per-namespace, close-time cutoff is applied per entry below.
	prefilter := now.Add(-minRetention)
	pageLimit := doubleSaturating(c.config.BatchSize)
	cursor := startAfter
	// Pages are walked in index-key order, so the backend stops at the page
	// boundary and one whole iteration enumerates each index key once no matter
	// how many pages it takes. Ordering by key rather than by time is what makes
	// that possible: the stamp is the fourth key segment, so a newest-first page
	// cannot be produced without reading every key first.
	var errs []error
	fail := func(entry artifacts.RunIndexEntry, step string, err error, msg string) {
		errs = append(errs, fmt.Errorf("run %q: %w", entry.RunID, err))
		p.stepFailed(step)
		stats.RunsFailed++
		p.recordRun(entry.Namespace, CleanupRunFailed)
		c.logger.Error(msg, "run_id", entry.RunID, "namespace", entry.Namespace, "error", err)
	}
	// missingDue are the runs past their retention whose execution is
	// missing, with when each fell due. They are retired at the end of the
	// pass, unless it holds them.
	var missingDue []dueRun
	if startAfter == "" {
		c.walkOverShare = false
		c.walkLag = nil
		c.walkLagUnknown = false
	}
	// overdue records a due run this pass leaves unretired.
	overdue := func(entry artifacts.RunIndexEntry, dueAt time.Time) {
		late := now.Sub(dueAt)
		if late <= 0 {
			return
		}
		if c.walkLag == nil {
			c.walkLag = map[string]time.Duration{}
		}
		ns := artifacts.ResolveNamespace(entry.Namespace)
		if late > c.walkLag[ns] {
			c.walkLag[ns] = late
		}
	}
	// reachedEnd is whether this pass walked to the end of the index.
	reachedEnd := false
	processed := 0
	for processed < c.config.BatchSize {
		entries, err := c.archive.ListRuns(ctx, artifacts.RunQuery{
			Until: prefilter, Limit: pageLimit, KeyOrder: true, StartAfterKey: cursor,
		})
		if err != nil {
			p.stepFailed(CleanupStepList)
			errs = append(errs, fmt.Errorf("archive cleanup: list runs: %w", err))
			return failed(errors.Join(errs...))
		}
		if len(entries) == 0 {
			reachedEnd = true
			break
		}

		// The cursor is only needed to ask for the NEXT page, so a page whose
		// last entry cannot be rendered back into a key stops the walk after
		// this page rather than discarding work already scanned.
		next, cursorErr := artifacts.RunIndexKeyForEntry(entries[len(entries)-1])
		// A conforming key-order page ends strictly after the cursor it was
		// given. Stop before processing if a backend violates that contract so
		// the worker cannot spin or repeat deletion attempts.
		if cursorErr == nil && next <= cursor {
			break
		}
		for _, entry := range entries {
			// Cleanup is a background worker, so a cancelled context means the
			// engine is shutting down. Stop between runs rather than grinding
			// through the page: each run is deleted then marked, so stopping
			// here leaves every run either fully cleaned or wholly untouched.
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("archive cleanup: canceled: %w", err))
				return failed(errors.Join(errs...))
			}
			stats.RunsScanned++
			state, stateErr := c.archive.CleanupState(ctx, entry.RunID)
			if stateErr == nil && state.ArtifactsCleaned {
				continue
			}
			if stateErr != nil && !errors.Is(stateErr, artifacts.ErrNotFound) {
				wrapped := fmt.Errorf("run %q: read cleanup state: %w", entry.RunID, stateErr)
				errs = append(errs, wrapped)
				p.stepFailed(CleanupStepState)
				c.walkLagUnknown = true
				continue
			}
			// The query's Until already excludes anything created after the
			// prefilter, but that bound is a superset (§ above), not this
			// entry's actual eligibility — and deletion is irreversible, so
			// eligibility is decided per-entry below, against its own
			// namespace's retention and the execution's real close time.
			if !entry.Time.Before(prefilter) {
				continue
			}
			processed++
			stats.Processed++
			retention := retentionFor(entry.Namespace)
			cutoff := now.Add(-retention)
			found, closedAt, err := c.cluster.LookupWorkflow(ctx, c.config.TemporalNamespace, entry.RunID)
			if err == nil {
				stats.LookedUp++
			}
			// dueAt is when the run fell due, once known; retired says it was
			// retired. A due run left unretired is the walk's lag.
			var dueAt time.Time
			retired := false
			switch {
			case err != nil:
				fail(entry, CleanupStepLookup, fmt.Errorf("look up execution: %w", err), "artifact cleanup workflow lookup failed")
				c.walkLagUnknown = true
			case !found:
				// The cluster does not hold it. The backstop TTL removes an
				// execution only after every namespace's retention, and this
				// worker deletes one only after its own, so a missing execution
				// is retired only once its own retention, counted from the
				// finish its archive body records, has passed. One missing
				// earlier is not this worker's to act on: it is kept, logged and
				// counted, and examined again by every pass until it is due.
				finishedAt, err := c.finishedAt(ctx, entry.RunID)
				if err != nil {
					fail(entry, CleanupStepLookup, fmt.Errorf("date a run whose execution is missing: %w", err), "artifact cleanup kept a run whose execution is missing and that it cannot date")
					c.walkLagUnknown = true
					break
				}
				if !finishedAt.Before(cutoff) {
					stats.MissingNotDue++
					p.recordRun(entry.Namespace, CleanupRunNotFoundNotDue)
					c.logger.Warn("artifact cleanup kept a run whose execution is missing before its run retention",
						"run_id", entry.RunID, "namespace", entry.Namespace, "finished_at", finishedAt,
						"run_retention", retention, "temporal_namespace", c.config.TemporalNamespace)
					break
				}
				// Due, but retired only after the pass has seen how many
				// executions are missing early (missingHeld).
				missingDue = append(missingDue, dueRun{entry: entry, dueAt: finishedAt.Add(retention)})
			case closedAt.IsZero():
				resumed, err := c.resumed(ctx, entry.RunID)
				if err != nil {
					fail(entry, CleanupStepArchiveCheck, fmt.Errorf("it is running, and whether it was resumed cannot be told: %w", err),
						"artifact cleanup kept a running run it could not tell was resumed")
					break
				}
				if resumed {
					// It ended, and a resume started it again: its row is on
					// its way out, and it is retired once it ends for good.
					c.logger.Info("artifact cleanup left a resumed run that is running", "run_id", entry.RunID, "namespace", entry.Namespace)
					break
				}
				// Impossible by construction — the archive is written by the
				// terminal PublishEvent, or by the sweep once the run has
				// closed — and the one case where deletion would TERMINATE a
				// live execution, so it is refused rather than risked.
				errs = append(errs, fmt.Errorf("run %q: archived run still executing; refusing to retire it", entry.RunID))
				p.stepFailed(CleanupStepRetire)
				stats.RunsFailed++
				p.recordRun(entry.Namespace, CleanupRunStillRunning)
				c.logger.Error("artifact cleanup found a still-running archived run", "run_id", entry.RunID, "namespace", entry.Namespace)
			case !closedAt.Before(cutoff):
				// Finished, but not yet past ITS namespace's own retention.
			default:
				// The history is about to go, so what stands in for it must
				// be there. An index row with no body is an archive write that
				// stopped short, which Sweeper finishes.
				dueAt = closedAt.Add(retention)
				has, err := c.bodies.HasRun(ctx, entry.RunID)
				if err != nil {
					fail(entry, CleanupStepArchiveCheck, fmt.Errorf("check its archive body: %w", err), "artifact cleanup archive check failed")
					break
				}
				if !has {
					p.recordRun(entry.Namespace, CleanupRunNotArchived)
					c.logger.Warn("artifact cleanup left a run its archive body is not written for yet", "run_id", entry.RunID)
					break
				}
				if err := c.cluster.DeleteWorkflow(ctx, c.config.TemporalNamespace, entry.RunID); err != nil {
					fail(entry, CleanupStepRetire, fmt.Errorf("retire execution: %w", err), "artifact cleanup retire failed")
					break
				}
				retired = c.reclaim(ctx, entry, now, &p, fail)
			}
			if !dueAt.IsZero() && !retired {
				overdue(entry, dueAt)
			}
			if processed >= c.config.BatchSize {
				// A full batch: a drain resumes after this run. A key that
				// cannot be rendered, or that does not advance, ends the
				// drain rather than repeat work.
				if key, keyErr := artifacts.RunIndexKeyForEntry(entry); keyErr == nil && key > startAfter {
					resume = key
				}
				break
			}
		}
		if processed >= c.config.BatchSize || len(entries) < pageLimit {
			reachedEnd = processed < c.config.BatchSize
			break
		}
		if cursorErr != nil {
			errs = append(errs, fmt.Errorf("archive cleanup: resume cursor: %w", cursorErr))
			p.stepFailed(CleanupStepList)
			p.Outcome = CleanupPassFailed
			break
		}
		// A short page means the walk reached the end of the index, so the
		// cursor only has to advance while full pages keep coming back.
		cursor = next
	}
	if stats.MissingNotDue > 0 {
		c.logger.Error("artifact cleanup kept runs whose execution is missing before their run retention",
			"runs_missing_not_due", stats.MissingNotDue, "runs_looked_up", stats.LookedUp,
			"temporal_namespace", c.config.TemporalNamespace)
	}
	if stats.missingHeld() {
		c.walkOverShare = true
		c.holdMissing.Store(true)
	}
	switch {
	case len(missingDue) == 0:
	case c.holdMissing.Load():
		stats.MissingHeld = len(missingDue)
		for _, due := range missingDue {
			overdue(due.entry, due.dueAt)
		}
		c.logger.Error("artifact cleanup held every run whose execution is missing: too many are missing before their run retention",
			"runs_missing_not_due", stats.MissingNotDue, "runs_looked_up", stats.LookedUp,
			"runs_held", stats.MissingHeld, "held_since_earlier_pass", !stats.missingHeld(),
			"temporal_namespace", c.config.TemporalNamespace)
	default:
		for _, due := range missingDue {
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("archive cleanup: canceled: %w", err))
				return failed(errors.Join(errs...))
			}
			if !c.reclaim(ctx, due.entry, now, &p, fail) {
				overdue(due.entry, due.dueAt)
			}
		}
	}
	if reachedEnd {
		// A whole walk with no pass over the share lifts the hold.
		c.holdMissing.Store(c.walkOverShare)
		p.WalkComplete = true
		p.LagUnknown = c.walkLagUnknown
		if !p.LagUnknown {
			p.RetentionLag = maps.Clone(c.walkLag)
		}
	}
	return p, resume, errors.Join(errs...)
}

// dueRun is a run past its retention, and when it fell due.
type dueRun struct {
	entry artifacts.RunIndexEntry
	dueAt time.Time
}

// finishedAt dates a run whose execution is missing from its archive body.
func (c *Cleaner) finishedAt(ctx context.Context, runID string) (time.Time, error) {
	if c.finish == nil {
		return time.Time{}, errors.New("no reader of archive bodies is configured")
	}
	at, err := c.finish.RunFinishedAt(ctx, runID)
	if err != nil {
		return time.Time{}, err
	}
	if at.IsZero() {
		return time.Time{}, errors.New("its archive body records no finish time")
	}
	return at, nil
}

// MissingHeld reports whether this worker holds every missing execution's
// retirement. The artifact GC holds its deletes while it does.
func (c *Cleaner) MissingHeld() bool { return c.holdMissing.Load() }

// referenceKeepingRetirer is an archive that can retire a run and keep its
// reference markers and run-objects list (rule 1).
type referenceKeepingRetirer interface {
	RetireRunArtifacts(ctx context.Context, namespace, runID string, keepReferences bool) (artifacts.CleanupResult, error)
}

// reclaim removes a retired run's run-scoped storage, then marks it cleaned,
// and reports whether it did both. The mark is last, so a failure leaves the
// run for a later pass.
func (c *Cleaner) reclaim(ctx context.Context, entry artifacts.RunIndexEntry, now time.Time, p *CleanupPass,
	fail func(artifacts.RunIndexEntry, string, error, string),
) bool {
	stats := &p.CleanupStats
	var (
		result artifacts.CleanupResult
		err    error
	)
	if retirer, ok := c.archive.(referenceKeepingRetirer); ok {
		// The GC reclaims nothing on a local directory, so nothing would
		// ever release what retirement kept there.
		keep := c.config.ArtifactGC.KeepsReferences()
		if local, ok := c.archive.(interface{ Local() bool }); ok && local.Local() {
			keep = false
		}
		result, err = retirer.RetireRunArtifacts(ctx, entry.Namespace, entry.RunID, keep)
	} else {
		result, err = c.archive.DeleteRunArtifacts(ctx, entry.Namespace, entry.RunID)
	}
	stats.ObjectsDeleted += result.ObjectsDeleted
	stats.BytesReclaimed += result.BytesReclaimed
	p.recordReclaimed(entry.Namespace, result.ObjectsDeleted, result.BytesReclaimed)
	if err != nil {
		fail(entry, CleanupStepDelete, fmt.Errorf("delete artifacts: %w", err), "artifact cleanup delete failed")
		return false
	}
	if err := c.archive.MarkArtifactsCleaned(ctx, entry.RunID, now); err != nil {
		fail(entry, CleanupStepMark, fmt.Errorf("mark artifacts cleaned: %w", err), "artifact cleanup mark failed")
		return false
	}
	stats.RunsCleaned++
	p.recordRun(entry.Namespace, CleanupRunRetired)
	c.logger.Info("run retired", "run_id", entry.RunID, "namespace", entry.Namespace,
		"objects_deleted", result.ObjectsDeleted, "bytes_reclaimed", result.BytesReclaimed)
	return true
}
