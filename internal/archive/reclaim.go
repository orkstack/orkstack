package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The artifact GC (docs/STORAGE.md, "Reclaiming space").
//
// Retention cleanup retires a run: it deletes the run's logs, attempt records,
// commits and snapshots, and writes the run's sidecar. With the GC on
// (dry_run or delete) it keeps the run's reference markers and its
// run-objects list. The Reclaimer then decides, object by object, whether the
// bytes a retired run produced or referenced can go.
//
// An object O, produced by run P (the run segment of O's key), is deleted
// only when
//
//	(i)   P is verified retired, more than the grace G ago (store clock);
//	(ii)  every other run holding a marker on O, listed after this pass's walk
//	      of the cluster's open runs, is verified retired more than G ago; and
//	(iii) the low-water mark LW is known and L(O) + M < LW, where L(O) is the
//	      latest of those retirements and M = G.
//
// LW is the earliest creation of any run that is open, or closed without an
// archive body, so (iii) says every run created before O's last supplier
// retired has finished and been archived, and therefore holds a marker if it
// references O. "Verified retired" is six checks, never the sidecar alone
// (verify). Everything that cannot be read keeps the object. The Reclaimer
// keeps between passes only a process-local low-water mark, a cursor and a
// done-set; its only durable writes are the clock probe and, in delete mode,
// the deletes and the create-only GC records.

// ReclaimMode is cleanup.artifact_gc.
type ReclaimMode string

const (
	// ReclaimOff releases a retired run's markers at retirement, as before
	// the GC existed, and reclaims nothing.
	ReclaimOff ReclaimMode = "off"
	// ReclaimDryRun runs every step, reports what a pass would delete, and
	// deletes nothing. It is the default.
	ReclaimDryRun ReclaimMode = "dry_run"
	// ReclaimDelete deletes what the rule allows.
	ReclaimDelete ReclaimMode = "delete"
)

const (
	// DefaultReclaimMode is the GC's mode when none is set.
	DefaultReclaimMode = ReclaimDryRun
	// DefaultReclaimGrace is G: how long a retirement must have stood
	// before it counts, and the margin M over the low-water mark.
	DefaultReclaimGrace = time.Hour
	// MinReclaimGrace is the shortest grace accepted.
	MinReclaimGrace = 10 * time.Minute
	// DefaultReclaimBatch is how many objects one pass examines.
	DefaultReclaimBatch = 1000
	// lowWaterPrevWindow is how many cleanup intervals a previous pass's
	// low-water mark stays usable for.
	lowWaterPrevWindow = 3
	// markerRelists is how many times a delete lists an object's markers
	// that changed under it before leaving the object for a later pass.
	markerRelists = 2
)

// ParseReclaimMode reads a mode. An unknown value is an error, never a guess.
func ParseReclaimMode(raw string) (ReclaimMode, error) {
	switch mode := ReclaimMode(raw); mode {
	case ReclaimOff, ReclaimDryRun, ReclaimDelete:
		return mode, nil
	case "":
		return DefaultReclaimMode, nil
	}
	return "", fmt.Errorf("artifact GC mode %q is not one of off, dry_run, delete", raw)
}

// KeepsReferences reports whether retirement keeps a run's markers and list
// for the GC to release (rule 1).
func (m ReclaimMode) KeepsReferences() bool { return m == ReclaimDryRun || m == ReclaimDelete }

// Object classes (ork.gc.object_class).
const (
	ClassArtifact       = "artifact"
	ClassValues         = "values"
	ClassSystemArtifact = "system_artifact"
	ClassUnlisted       = "unlisted"
)

// Outcomes (outcome).
const (
	OutcomeDeleted     = "deleted"
	OutcomeAlreadyGone = "already_gone"
	OutcomeKept        = "kept"
	OutcomeFailed      = "failed"
	// OutcomeWouldDelete is an unreferenced object a pass that does not
	// delete (a dry run, a held or warming pass) counted.
	OutcomeWouldDelete = "would_delete"
)

// Reasons (ork.gc.reason).
const (
	ReasonRunLive           = "run_live"
	ReasonRunRetained       = "run_retained"
	ReasonRunInGrace        = "run_in_grace"
	ReasonRunUnverified     = "run_unverified"
	ReasonLowWater          = "lowwater"
	ReasonClaimLive         = "claim_live"
	ReasonClaimInGrace      = "claim_in_grace"
	ReasonClaimUnlisted     = "claim_unlisted"
	ReasonClaimOrphaned     = "claim_orphaned"
	ReasonTooManyReferences = "too_many_references"
	ReasonUnparseableKey    = "unparseable_key"
	ReasonLateCommit        = "late_commit"
	ReasonUnreferenced      = "unreferenced"
	ReasonStoreError        = "store_error"
	ReasonStoreRefused      = "store_refused"
	ReasonListFailed        = "list_failed"
	ReasonListingTooOld     = "listing_too_old"
	ReasonDeadline          = "deadline"
)

// Hold reasons (ork.gc.hold_reason).
const (
	HoldComposedArtifacts      = "engines_lack_composed_artifacts"
	HoldCapabilitiesUnreadable = "capabilities_unreadable"
	HoldLowWaterUnknown        = "lowwater_unknown"
	HoldClockSkew              = "clock_skew"
	HoldExecutionsMissing      = "executions_missing"
	HoldLocalBackend           = "local_backend"
)

// Low-water causes (ork.gc.lowwater_cause).
const (
	LowWaterSweepUnclean = "sweep_unclean"
	LowWaterNoSweep      = "no_sweep"
	LowWaterWalkFailed   = "walk_failed"
	LowWaterMidSwitch    = "mid_switch"
	LowWaterNoCreation   = "no_creation"
	LowWaterIndexStale   = "index_stale"
	// LowWaterNotComputed is a preview's answer on a replica that has no
	// pass and could not compute a mark within its budget.
	LowWaterNotComputed = "not_computed"
)

// keepClass is what a kept object means for the run being examined (M5).
// The order is the precedence: a run takes the largest class among its
// objects, and only waiting and transient keep it due.
type keepClass int

const (
	// classDone: deleted, already gone, or counted as a would-delete.
	classDone keepClass = iota
	// classDelegated: another run's GC examines the object again.
	classDelegated
	// classFinal: a counted leak no pass revisits.
	classFinal
	// classWaiting: the run stays due until a claimant retires or the mark
	// passes.
	classWaiting
	// classTransient: something could not be read or was not reached.
	classTransient
)

// ReclaimStore is the object store as the GC needs it.
// *artifacts.BlobRunArchive implements it.
type ReclaimStore interface {
	Local() bool
	StoreNow(ctx context.Context) (time.Time, error)
	ListRuns(ctx context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error)
	HasRun(ctx context.Context, runID string) (bool, error)
	CleanupSidecar(ctx context.Context, runID string) (artifacts.RunCleanupState, time.Time, error)
	RunObjectList(ctx context.Context, runID string) ([]string, error)
	ListUnder(ctx context.Context, prefix string, limit int) ([]artifacts.StoredObject, bool, error)
	StatObject(ctx context.Context, key string) (artifacts.StoredObject, error)
	ListObjectReferences(ctx context.Context, objectID string) ([]string, error)
	RemoveReclaimedObject(ctx context.Context, objectID string) error
	RemoveObjectReferences(ctx context.Context, objectID string, runIDs []string) error
	RemoveRunObjectList(ctx context.Context, runID string) error
	CreateReclaimRecord(ctx context.Context, rec artifacts.ReclaimRecord) error
	EachReclaimRecord(ctx context.Context, visit func(runID string) error) error
}

// BodyObject is one object a run's archive body records, with its class.
type BodyObject struct {
	ID    string
	Class string
}

// RunBody is what the GC reads from a run's current archive body.
type RunBody struct {
	RunID      string
	Namespace  string
	Terminal   bool
	FinishedAt time.Time
	Objects    []BodyObject
}

// RunBodyReader reads a run's current-epoch archive body. A run with no body
// is an error matching artifacts.ErrNotFound.
type RunBodyReader interface {
	ReclaimBody(ctx context.Context, runID string) (RunBody, error)
}

// ClosedRun is one recently closed run the low-water walk listed.
type ClosedRun struct {
	RunID   string
	Created time.Time
}

// LowWaterWalk is steps 2 and 3 of the low-water computation, as the cluster
// answered them.
type LowWaterWalk struct {
	// Chains is how many live chains the walk settled.
	Chains int
	// Earliest is the earliest creation over every record of every live
	// chain (zero when there is none), and EarliestRunID its run.
	Earliest      time.Time
	EarliestRunID string
	// Cause is a low-water cause the walk itself found (mid_switch,
	// no_creation); "" when it found none.
	Cause string
	// Closed are the runs closed since the asked bound that are not live.
	Closed []ClosedRun
	// Seen holds every run id the walk listed, live or closed.
	Seen map[string]struct{}
}

// ReclaimCluster is the cluster as the GC needs it.
type ReclaimCluster interface {
	LookupWorkflow(ctx context.Context, namespace, workflowID string) (found bool, closeTime time.Time, err error)
	// WalkLowWater lists every live chain, and every run closed at or after
	// closedSince that is not ContinuedAsNew. A read error or a scan bound
	// is an error.
	WalkLowWater(ctx context.Context, closedSince time.Time) (LowWaterWalk, error)
}

// SweepPass is one completed archive-sweep pass.
type SweepPass struct {
	Started  time.Time
	Finished time.Time
	// Clean says it confirmed every finished run it listed has an archive:
	// no list failure, no unconfirmed run, interval not too long.
	Clean bool
}

// SweepRecord reports this replica's latest completed sweep pass.
type SweepRecord interface {
	LastCompletedSweep() (SweepPass, bool)
}

// EngineFleet says whether the deployment's engines disagree on the
// composed-artifacts capability (E87's rollout).
type EngineFleet interface {
	ComposedArtifactsMixed(ctx context.Context) (bool, error)
}

// ResumeRecords says whether a run has a resume record newer than its
// current archive body (E85). A Reclaimer given none verifies no run retired
// and so deletes nothing: the check cannot be skipped by forgetting it.
type ResumeRecords interface {
	ResumedAfter(ctx context.Context, runID string, finishedAt time.Time) (bool, error)
}

// NoResumes is the resume check of a release in which no run can be resumed
// (before E85): a run's current archive body is always its only one. E85
// replaces it with its resume records before any resume ships.
type NoResumes struct{}

// ResumedAfter answers that no run was resumed.
func (NoResumes) ResumedAfter(context.Context, string, time.Time) (bool, error) { return false, nil }

// AcceptedRuns names this replica's newest accepted run, for the index_stale
// canary.
type AcceptedRuns interface {
	NewestAccepted() (runID string, at time.Time, ok bool)
}

// MissingHold says whether retention cleanup holds missing executions.
type MissingHold interface {
	MissingHeld() bool
}

// ReclaimObserver sees each pass. It must not block.
type ReclaimObserver interface {
	ObserveReclaim(ctx context.Context, report ReclaimReport)
}

// TallyKey is one label set of ork.gc.objects.
type TallyKey struct {
	Namespace string
	Class     string
	Outcome   string
	Reason    string
}

// Tally counts objects and bytes.
type Tally struct {
	Objects int64
	Bytes   int64
}

// LowWater is one pass's low-water mark.
type LowWater struct {
	Known bool
	// Infinite says no run is open or unarchived: nothing holds the mark.
	Infinite bool
	At       time.Time
	RunID    string
	// Cause is why the mark is unknown.
	Cause    string
	Chains   int
	Warming  bool
	Computed time.Time
}

// admits reports whether t + margin is strictly before the mark.
func (lw LowWater) admits(t time.Time, margin time.Duration) bool {
	if !lw.Known {
		return false
	}
	if lw.Infinite {
		return true
	}
	return t.Add(margin).Before(lw.At)
}

// ReclaimReport is one pass.
type ReclaimReport struct {
	Mode     ReclaimMode
	Started  time.Time
	Duration time.Duration
	// Outcome is completed, partial, held, warming or failed.
	Outcome  string
	Held     []string
	LowWater LowWater
	StoreNow time.Time
	// StoreNowAt is this engine's clock when StoreNow was read.
	StoreNowAt time.Time
	Skew       time.Duration
	// StoreVersioned says the bucket keeps versions: a delete there only
	// adds a delete marker, and the bytes stay until a lifecycle rule
	// expires the noncurrent version.
	StoreVersioned bool
	Objects        map[TallyKey]Tally
	// Runs by what became of them.
	RunsFinished   int
	RunsCarried    int
	RunsWaiting    int
	RunsLateCommit int
	// CycleComplete says this pass reached the end of the run index.
	CycleComplete bool
	// Cycle is the totals of the cycle this pass completed, every pass of
	// it summed; nil when this pass did not complete one.
	Cycle map[TallyKey]Tally
	// OldestDue is the oldest retirement among the runs still due, zero
	// when there is none.
	OldestDue time.Time
	Err       error
}

// ReclaimConfig is the GC's settings.
type ReclaimConfig struct {
	Mode     ReclaimMode
	Grace    time.Duration
	Batch    int
	Interval time.Duration
}

// Reclaimer is the artifact GC.
type Reclaimer struct {
	config     ReclaimConfig
	store      ReclaimStore
	bodies     RunBodyReader
	cluster    ReclaimCluster
	sweep      SweepRecord
	namespaces NamespaceLister
	retention  *RetentionController
	temporalNS string
	fleet      EngineFleet
	resumes    ResumeRecords
	accepted   AcceptedRuns
	missing    MissingHold
	observer   ReclaimObserver
	logger     *slog.Logger
	now        func() time.Time

	// mu guards the state below, which passes and previews share.
	mu sync.Mutex
	// prev is the previous pass's LW_pass, process-local.
	prev     LowWater
	prevAt   time.Time
	last     ReclaimReport
	hasLast  bool
	cursor   string
	done     map[uint64]struct{}
	dryCycle map[TallyKey]Tally
	// cycle sums the tallies of the passes of the cycle in progress.
	cycle map[TallyKey]Tally
	// previewing admits one preview at a time.
	previewing sync.Mutex
}

// NewReclaimer builds the GC.
func NewReclaimer(config ReclaimConfig, store ReclaimStore, bodies RunBodyReader, cluster ReclaimCluster,
	sweep SweepRecord, namespaces NamespaceLister, retention *RetentionController, temporalNamespace string,
	logger *slog.Logger,
) *Reclaimer {
	if logger == nil {
		logger = slog.Default()
	}
	if config.Grace < MinReclaimGrace {
		// A shorter grace would let a margin smaller than the clocks' skew
		// decide a delete; the default is longer, never shorter.
		logger.Warn("artifact gc grace is below the minimum; using the default",
			"grace", config.Grace, "minimum", MinReclaimGrace, "default", DefaultReclaimGrace)
		config.Grace = DefaultReclaimGrace
	}
	if config.Batch <= 0 {
		config.Batch = DefaultReclaimBatch
	}
	if config.Interval <= 0 {
		config.Interval = DefaultCleanupInterval
	}
	if config.Mode == "" {
		config.Mode = DefaultReclaimMode
	}
	return &Reclaimer{
		config: config, store: store, bodies: bodies, cluster: cluster, sweep: sweep,
		namespaces: namespaces, retention: retention, temporalNS: temporalNamespace,
		logger: logger, now: time.Now,
	}
}

// WithFleet sets the composed-artifacts capability check.
func (r *Reclaimer) WithFleet(f EngineFleet) *Reclaimer { r.fleet = f; return r }

// WithResumes sets the resume-record check (E85).
func (r *Reclaimer) WithResumes(s ResumeRecords) *Reclaimer { r.resumes = s; return r }

// WithAcceptedRuns sets the index_stale canary's source.
func (r *Reclaimer) WithAcceptedRuns(a AcceptedRuns) *Reclaimer { r.accepted = a; return r }

// WithMissingHold shares retention cleanup's missing-executions hold.
func (r *Reclaimer) WithMissingHold(m MissingHold) *Reclaimer { r.missing = m; return r }

// WithObserver sets where each pass's report goes.
func (r *Reclaimer) WithObserver(o ReclaimObserver) *Reclaimer { r.observer = o; return r }

// WithClock replaces the local clock (the store's clock is always read).
func (r *Reclaimer) WithClock(now func() time.Time) *Reclaimer {
	if now != nil {
		r.now = now
	}
	return r
}

// WithGrace sets G without the minimum, for a test that cannot wait out ten
// minutes of store clock. A deployment sets G only through
// cleanup.artifact_gc_grace, which keeps the minimum.
func (r *Reclaimer) WithGrace(grace time.Duration) *Reclaimer {
	if grace > 0 {
		r.config.Grace = grace
	}
	return r
}

// Mode is the mode in force.
func (r *Reclaimer) Mode() ReclaimMode { return r.config.Mode }

// Grace is G.
func (r *Reclaimer) Grace() time.Duration { return r.config.Grace }

// Run passes once per interval until ctx is canceled. Off runs nothing.
func (r *Reclaimer) Run(ctx context.Context) {
	if r.config.Mode == ReclaimOff {
		return
	}
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()
	for {
		report := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		r.log(report)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Reclaimer) log(report ReclaimReport) {
	var wouldDelete, deleted, kept, failed int64
	for key, tally := range report.Objects {
		switch key.Outcome {
		case OutcomeWouldDelete:
			wouldDelete += tally.Objects
		case OutcomeDeleted:
			deleted += tally.Objects
		case OutcomeKept:
			kept += tally.Objects
		case OutcomeFailed:
			failed += tally.Objects
		}
	}
	args := []any{"mode", report.Mode, "outcome", report.Outcome, "held", report.Held,
		"objects_deleted", deleted, "objects_would_delete", wouldDelete, "objects_kept", kept,
		"objects_failed", failed, "runs_finished", report.RunsFinished, "runs_waiting", report.RunsWaiting,
		"runs_carried_over", report.RunsCarried, "runs_late_commit", report.RunsLateCommit,
		"lowwater_known", report.LowWater.Known, "lowwater_cause", report.LowWater.Cause}
	if report.Err != nil {
		r.logger.Error("artifact gc pass completed", append(args, "error", report.Err)...)
		return
	}
	r.logger.Info("artifact gc pass completed", args...)
}

// LastReport returns the latest pass's report.
func (r *Reclaimer) LastReport() (ReclaimReport, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last, r.hasLast
}

// passState is one pass's (or one preview's) working set.
type passState struct {
	// restarted says the walk abandoned its cycle: nothing of this pass
	// counts toward the next one.
	restarted    bool
	storeNow     time.Time
	lw           LowWater
	deleting     bool
	retention    func(namespace string) time.Duration
	verified     map[string]retirement
	lists        map[string]listRead
	tallies      map[TallyKey]Tally
	lookedUp     int
	missingEarly int
	deadline     time.Time
	samples      *sampler
	// run is what the run being examined has come to so far.
	run runResult
}

// runResult is one run's deletes, and whether a late commit of its own was
// found while its objects were being deleted.
type runResult struct {
	deleted    int64
	bytes      int64
	lateCommit bool
}

type listRead struct {
	ids map[string]struct{}
	err error
}

func (p *passState) count(namespace, class, outcome, reason string, bytes int64) {
	key := TallyKey{Namespace: artifacts.ResolveNamespace(namespace), Class: class, Outcome: outcome, Reason: reason}
	t := p.tallies[key]
	t.Objects++
	t.Bytes += bytes
	p.tallies[key] = t
}

// RunOnce makes one pass.
func (r *Reclaimer) RunOnce(ctx context.Context) ReclaimReport {
	started := r.now()
	report := ReclaimReport{Mode: r.config.Mode, Started: started, Objects: map[TallyKey]Tally{}}
	defer func() {
		report.Duration = r.now().Sub(started)
		r.mu.Lock()
		r.last, r.hasLast = report, true
		r.mu.Unlock()
		if r.observer != nil {
			r.observer.ObserveReclaim(ctx, report)
		}
	}()
	if r.config.Mode == ReclaimOff {
		report.Outcome = "held"
		return report
	}
	if r.store.Local() {
		report.Held = []string{HoldLocalBackend}
		report.Outcome = "held"
		return report
	}
	storeNow, err := r.store.StoreNow(ctx)
	if err != nil {
		report.Outcome, report.Err = "failed", fmt.Errorf("artifact gc: read the store's clock: %w", err)
		return report
	}
	// The store's now is a lower bound as of the moment the probe returned:
	// everything that reads time from it counts from then, never from the
	// pass's start, so a slow probe is never counted twice.
	report.StoreNow, report.StoreNowAt = storeNow, r.now()
	report.Skew = report.StoreNowAt.Sub(storeNow)
	if v, ok := r.store.(versionedStore); ok && r.config.Mode == ReclaimDelete {
		versioned, err := v.StoreVersioned(ctx)
		if err != nil {
			r.logger.Warn("artifact gc could not read the bucket's versioning", "error", err)
		}
		report.StoreVersioned = versioned
		if versioned {
			r.logger.Warn("artifact gc deletes on a versioned bucket only add delete markers; the bytes stay until a NoncurrentVersionExpiration rule removes them")
		}
	}
	retention, err := r.retentionFor(ctx)
	if err != nil {
		report.Outcome, report.Err = "failed", err
		return report
	}
	report.Held = r.holds(ctx, report.Skew)
	lw := r.lowWater(ctx, true)
	report.LowWater = lw
	if !lw.Known {
		report.Held = append(report.Held, HoldLowWaterUnknown)
	}
	pass := &passState{
		storeNow: storeNow, lw: lw, retention: retention,
		deleting: r.config.Mode == ReclaimDelete && len(report.Held) == 0 && lw.Known && !lw.Warming,
		verified: map[string]retirement{}, lists: map[string]listRead{}, tallies: report.Objects,
		deadline: started.Add(r.config.Interval / 2),
	}
	err = r.walk(ctx, pass, &report)
	if !report.CycleComplete && !pass.restarted {
		r.carryCycle(pass)
	}
	if pass.missingEarly > 0 && float64(pass.missingEarly) > MissingHoldShare*float64(pass.lookedUp) &&
		!slices.Contains(report.Held, HoldExecutionsMissing) {
		// Found during this pass, after its deletes were decided: the
		// deletes of this pass already required each producer's and
		// claimant's own retention to have elapsed, so the hold only
		// stops the next pass.
		report.Held = append(report.Held, HoldExecutionsMissing)
	}
	switch {
	case err != nil:
		report.Outcome, report.Err = "failed", err
	case len(report.Held) > 0:
		report.Outcome = "held"
	case lw.Warming:
		report.Outcome = "warming"
	case !report.CycleComplete:
		report.Outcome = "partial"
	default:
		report.Outcome = "completed"
	}
	return report
}

// retentionFor resolves every namespace's current run retention.
func (r *Reclaimer) retentionFor(ctx context.Context) (func(string) time.Duration, error) {
	if r.retention == nil || r.namespaces == nil {
		return nil, errors.New("artifact gc: retention is not configured")
	}
	policy, err := r.retention.Policy(ctx)
	if err != nil {
		return nil, fmt.Errorf("artifact gc: read retention policy: %w", err)
	}
	max := policy.GetMaxRunRetention().AsDuration()
	if err := policy.GetMaxRunRetention().CheckValid(); err != nil || max <= 0 {
		return nil, fmt.Errorf("artifact gc: retention policy has no positive maximum run retention (%v)", policy.GetMaxRunRetention())
	}
	docs, err := r.namespaces.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("artifact gc: list namespaces: %w", err)
	}
	byName := make(map[string]time.Duration, len(docs))
	for _, ns := range docs {
		byName[artifacts.ResolveNamespace(ns.GetName())] = EffectiveRunRetention(ns, max)
	}
	return func(namespace string) time.Duration {
		if d, ok := byName[artifacts.ResolveNamespace(namespace)]; ok {
			return d
		}
		return max
	}, nil
}

// holds returns every hold but the low-water one.
func (r *Reclaimer) holds(ctx context.Context, skew time.Duration) []string {
	var held []string
	if r.fleet != nil {
		mixed, err := r.fleet.ComposedArtifactsMixed(ctx)
		switch {
		case err != nil:
			held = append(held, HoldCapabilitiesUnreadable)
		case mixed:
			held = append(held, HoldComposedArtifacts)
		}
	}
	if skew < 0 {
		skew = -skew
	}
	if skew > r.config.Grace/4 {
		held = append(held, HoldClockSkew)
	}
	if r.missing != nil && r.missing.MissingHeld() {
		held = append(held, HoldExecutionsMissing)
	}
	return held
}

// lowWater computes this pass's mark (steps 1 to 4). record says whether it
// becomes the previous mark of the next pass (a preview's does not).
func (r *Reclaimer) lowWater(ctx context.Context, record bool) LowWater {
	computed := r.now()
	unknown := func(cause string) LowWater { return LowWater{Cause: cause, Computed: computed} }
	if r.sweep == nil || r.cluster == nil {
		return unknown(LowWaterNoSweep)
	}
	sweep, ok := r.sweep.LastCompletedSweep()
	if !ok {
		return unknown(LowWaterNoSweep)
	}
	if !sweep.Clean {
		return unknown(LowWaterSweepUnclean)
	}
	iv, g := r.config.Interval, r.config.Grace
	since := sweep.Started.Add(-iv - g)
	walk, err := r.cluster.WalkLowWater(ctx, since)
	if err != nil {
		r.logger.Error("artifact gc could not walk the cluster's open runs", "error", err)
		return unknown(LowWaterWalkFailed)
	}
	if walk.Cause != "" {
		return unknown(walk.Cause)
	}
	if r.accepted != nil {
		// This replica's newest accepted run, older than G/4 and accepted
		// after the walk's lower bound, is either open or closed since that
		// bound, so it is in the walk unless the cluster's index is stale.
		if id, at, ok := r.accepted.NewestAccepted(); ok && computed.Sub(at) > g/4 && at.After(since) {
			if _, seen := walk.Seen[id]; !seen {
				return unknown(LowWaterIndexStale)
			}
		}
	}
	lw := LowWater{Known: true, Infinite: true, Chains: walk.Chains, Computed: computed}
	if !walk.Earliest.IsZero() {
		lw.Infinite, lw.At, lw.RunID = false, walk.Earliest, walk.EarliestRunID
	}
	// Step 3: the first recently closed run, in ascending creation and
	// created before the live minimum, with no archive body. A budget that
	// runs out stops at the earliest run it did not check.
	closed := slices.Clone(walk.Closed)
	sort.SliceStable(closed, func(i, j int) bool { return closed[i].Created.Before(closed[j].Created) })
	checked := 0
	for _, run := range closed {
		if !lw.Infinite && !run.Created.Before(lw.At) {
			break
		}
		if checked == r.config.Batch {
			lw.Infinite, lw.At, lw.RunID = false, run.Created, run.RunID
			break
		}
		checked++
		has, err := r.store.HasRun(ctx, run.RunID)
		if err != nil {
			r.logger.Error("artifact gc could not check a closed run's archive body", "run_id", run.RunID, "error", err)
			return unknown(LowWaterWalkFailed)
		}
		if !has {
			lw.Infinite, lw.At, lw.RunID = false, run.Created, run.RunID
			break
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pass := lw
	prevUsable := r.prev.Known && !r.prevAt.IsZero() && computed.Sub(r.prevAt) <= lowWaterPrevWindow*iv
	switch {
	case !prevUsable:
		lw.Warming = true
	case r.prev.Infinite:
	case lw.Infinite || r.prev.At.Before(lw.At):
		lw.Infinite, lw.At, lw.RunID = false, r.prev.At, r.prev.RunID
	}
	if record {
		r.prev, r.prevAt = pass, computed
	}
	return lw
}

// walk examines due runs from the cursor until the batch or the deadline.
func (r *Reclaimer) walk(ctx context.Context, pass *passState, report *ReclaimReport) error {
	r.mu.Lock()
	cursor := r.cursor
	needDone := r.done == nil
	r.mu.Unlock()
	if needDone && pass.deleting {
		if err := r.loadDone(ctx); err != nil {
			return err
		}
	}
	until := pass.storeNow.Add(-r.config.Grace)
	examined := 0
	pageLimit := doubleSaturating(r.config.Batch)
	for {
		entries, err := r.store.ListRuns(ctx, artifacts.RunQuery{Until: until, Limit: pageLimit, KeyOrder: true, StartAfterKey: cursor})
		if err != nil {
			r.saveCursor(cursor)
			return fmt.Errorf("artifact gc: list runs: %w", err)
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				r.saveCursor(cursor)
				return err
			}
			if examined >= r.config.Batch || !r.now().Before(pass.deadline) {
				r.saveCursor(cursor)
				return nil
			}
			key, keyErr := artifacts.RunIndexKeyForEntry(entry)
			if keyErr != nil || key <= cursor {
				r.saveCursor("")
				r.restartCycle()
				pass.restarted = true
				return fmt.Errorf("artifact gc: the run index cursor did not advance at run %q", entry.RunID)
			}
			cursor = key
			if r.isDone(entry.RunID) {
				continue
			}
			n, outcome := r.examineRun(ctx, pass, entry.RunID)
			examined += n
			switch outcome {
			case runNotDue:
			case runFinished:
				report.RunsFinished++
			case runLateCommit:
				report.RunsLateCommit++
			case runWaiting:
				report.RunsWaiting++
				r.noteDue(pass, report, entry.RunID)
			default:
				report.RunsCarried++
				r.noteDue(pass, report, entry.RunID)
			}
		}
		if len(entries) < pageLimit {
			report.CycleComplete = true
			r.saveCursor("")
			report.Cycle = r.finishCycle(pass)
			return nil
		}
	}
}

func (r *Reclaimer) noteDue(pass *passState, report *ReclaimReport, runID string) {
	if v, ok := pass.verified[runID]; ok && v.state == retRetired {
		if report.OldestDue.IsZero() || v.retiredAt.Before(report.OldestDue) {
			report.OldestDue = v.retiredAt
		}
	}
}

func (r *Reclaimer) saveCursor(cursor string) {
	r.mu.Lock()
	r.cursor = cursor
	r.mu.Unlock()
}

// finishCycle keeps a complete cycle's totals, every pass of it summed, and
// drops the done-set, so the next cycle reloads it from the GC records. It
// returns a copy of the totals.
func (r *Reclaimer) finishCycle(pass *passState) map[TallyKey]Tally {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = nil
	totals := addTallies(r.cycle, pass.tallies)
	r.cycle = nil
	r.dryCycle = totals
	return maps.Clone(totals)
}

// carryCycle adds a pass that did not complete its cycle to the cycle's sums.
func (r *Reclaimer) carryCycle(pass *passState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cycle = addTallies(r.cycle, pass.tallies)
}

// restartCycle drops the sums of a cycle the walk abandoned: the next pass
// starts the run index over.
func (r *Reclaimer) restartCycle() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cycle = nil
}

// addTallies returns into with every tally of from added.
func addTallies(into, from map[TallyKey]Tally) map[TallyKey]Tally {
	if into == nil {
		into = make(map[TallyKey]Tally, len(from))
	}
	for k, v := range from {
		t := into[k]
		t.Objects += v.Objects
		t.Bytes += v.Bytes
		into[k] = t
	}
	return into
}

// CycleTotals returns the totals of the last pass that completed a cycle:
// what the would-delete and kept gauges show.
func (r *Reclaimer) CycleTotals() (map[TallyKey]Tally, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dryCycle == nil {
		return nil, false
	}
	out := make(map[TallyKey]Tally, len(r.dryCycle))
	for k, v := range r.dryCycle {
		out[k] = v
	}
	return out, true
}

// runHash is FNV-1a: a collision marks a run done, which only leaks.
func runHash(runID string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(runID); i++ {
		h ^= uint64(runID[i])
		h *= 1099511628211
	}
	return h
}

func (r *Reclaimer) loadDone(ctx context.Context) error {
	done := map[uint64]struct{}{}
	if err := r.store.EachReclaimRecord(ctx, func(runID string) error {
		done[runHash(runID)] = struct{}{}
		return nil
	}); err != nil {
		return fmt.Errorf("artifact gc: list GC records: %w", err)
	}
	r.mu.Lock()
	r.done = done
	r.mu.Unlock()
	return nil
}

func (r *Reclaimer) isDone(runID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.done[runHash(runID)]
	return ok
}

func (r *Reclaimer) markDone(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done != nil {
		r.done[runHash(runID)] = struct{}{}
	}
}

type runOutcome int

const (
	runNotDue runOutcome = iota
	runFinished
	runWaiting
	runCarried
	runLateCommit
)

// retState is what verify found.
type retState int

const (
	retUnverified retState = iota
	retRetired
	retLive
	retRetained
	retNeverSeen
	retLateCommit
)

type retirement struct {
	state     retState
	retiredAt time.Time
	namespace string
	archived  bool
}

// verifyRetired applies the six checks to one run, once per pass.
func (r *Reclaimer) verifyRetired(ctx context.Context, pass *passState, runID string) retirement {
	if v, ok := pass.verified[runID]; ok {
		return v
	}
	v := r.verify(ctx, pass, runID)
	pass.verified[runID] = v
	return v
}

// verify is a run "verified retired" only if all six hold:
//
//  1. its sidecar exists;
//  2. its current archive body exists, names it, is terminal and finished;
//  3. its namespace's current retention has elapsed since that finish;
//  4. the cluster holds no execution of it;
//  5. no resume record of it is newer than that body;
//  6. its commit prefix lists empty.
//
// Anything unreadable is retUnverified. A run with an execution is retLive;
// one with a body that is not yet past its retention, or not yet retired, is
// retRetained; one with neither a body, a sidecar nor an execution is
// retNeverSeen. A commit under a retired run's prefix is retLateCommit:
// nothing it produced is ever deleted.
func (r *Reclaimer) verify(ctx context.Context, pass *passState, runID string) retirement {
	unverified := func(check string, err error) retirement {
		r.logger.Warn("artifact gc could not verify a run's retirement; keeping its objects",
			"run_id", runID, "check", check, "error", err)
		return retirement{state: retUnverified}
	}
	_, retiredAt, sidecarErr := r.store.CleanupSidecar(ctx, runID)
	hasSidecar := sidecarErr == nil
	if sidecarErr != nil && !errors.Is(sidecarErr, artifacts.ErrNotFound) {
		return unverified("sidecar", sidecarErr)
	}
	body, bodyErr := r.bodies.ReclaimBody(ctx, runID)
	hasBody := bodyErr == nil
	if bodyErr != nil && !errors.Is(bodyErr, artifacts.ErrNotFound) {
		return unverified("archive body", bodyErr)
	}
	if hasBody && body.RunID != runID {
		return unverified("archive body", fmt.Errorf("the body names run %q", body.RunID))
	}
	found, _, err := r.cluster.LookupWorkflow(ctx, r.temporalNS, runID)
	if err != nil {
		return unverified("cluster", err)
	}
	pass.lookedUp++
	if found {
		return retirement{state: retLive, namespace: body.Namespace, archived: hasBody}
	}
	switch {
	case !hasBody && !hasSidecar:
		return retirement{state: retNeverSeen}
	case !hasBody:
		return unverified("archive body", errors.New("a sidecar with no archive body"))
	case !body.Terminal || body.FinishedAt.IsZero():
		return unverified("archive body", errors.New("the body records no terminal status and finish"))
	}
	retained := retirement{state: retRetained, namespace: body.Namespace, archived: true}
	if pass.storeNow.Sub(body.FinishedAt) <= pass.retention(body.Namespace) {
		// The cluster holds no execution of a run its own retention still
		// keeps: missing early, which only a damaged or wrong cluster does.
		pass.missingEarly++
		return retained
	}
	if !hasSidecar {
		return retained
	}
	if r.resumes == nil {
		return unverified("resume record", errors.New("no resume check is configured"))
	}
	resumed, err := r.resumes.ResumedAfter(ctx, runID, body.FinishedAt)
	if err != nil {
		return unverified("resume record", err)
	}
	if resumed {
		return retirement{state: retLive, namespace: body.Namespace, archived: true}
	}
	empty, err := r.commitsEmpty(ctx, body.Namespace, runID)
	if err != nil {
		return unverified("commit prefix", err)
	}
	if !empty {
		r.logger.Warn("artifact gc found a commit under a retired run's prefix; keeping everything it produced for good",
			"run_id", runID)
		return retirement{state: retLateCommit, retiredAt: retiredAt, namespace: body.Namespace, archived: true}
	}
	return retirement{state: retRetired, retiredAt: retiredAt, namespace: body.Namespace, archived: true}
}

// commitsEmpty lists a run's commit prefix (check 6).
func (r *Reclaimer) commitsEmpty(ctx context.Context, namespace, runID string) (bool, error) {
	prefix, err := artifacts.RunCommitPrefix(namespace, runID)
	if err != nil {
		return false, err
	}
	commits, _, err := r.store.ListUnder(ctx, prefix, 1)
	if err != nil {
		return false, err
	}
	return len(commits) == 0, nil
}

// candidate is one object a due run's GC examines.
type candidate struct {
	id    string
	class string
	size  int64
}

// examineRun examines one run of the index: nothing when it is not due, and
// otherwise every object it produced or references. It returns how many
// objects it examined.
func (r *Reclaimer) examineRun(ctx context.Context, pass *passState, runID string) (int, runOutcome) {
	_, retiredAt, err := r.store.CleanupSidecar(ctx, runID)
	if errors.Is(err, artifacts.ErrNotFound) {
		return 0, runNotDue
	}
	if err != nil {
		r.logger.Warn("artifact gc could not read a run's sidecar", "run_id", runID, "error", err)
		return 0, runCarried
	}
	if pass.storeNow.Sub(retiredAt) < r.config.Grace {
		return 0, runNotDue
	}
	self := r.verifyRetired(ctx, pass, runID)
	switch self.state {
	case retRetired:
	case retLateCommit:
		return r.keepLateCommit(ctx, pass, runID, self)
	default:
		return 0, runCarried
	}
	body, err := r.bodies.ReclaimBody(ctx, runID)
	if err != nil {
		r.logger.Warn("artifact gc could not read a retired run's archive body", "run_id", runID, "error", err)
		return 0, runCarried
	}
	candidates, complete, err := r.candidates(ctx, runID, body)
	if err != nil {
		r.logger.Warn("artifact gc could not list a retired run's objects", "run_id", runID, "error", err)
		return 0, runCarried
	}
	worst := classDone
	var final []string
	pass.run = runResult{}
	for i, c := range candidates {
		if !r.now().Before(pass.deadline) {
			for _, rest := range candidates[i:] {
				pass.count(body.Namespace, rest.class, OutcomeFailed, ReasonDeadline, rest.size)
			}
			complete = false
			break
		}
		cls := r.examineObject(ctx, pass, runID, body.Namespace, c)
		if cls == classFinal {
			final = append(final, c.id)
		}
		worst = max(worst, cls)
	}
	if !complete {
		worst = max(worst, classTransient)
	}
	switch worst {
	case classWaiting:
		return len(candidates), runWaiting
	case classTransient:
		return len(candidates), runCarried
	}
	if !pass.deleting {
		return len(candidates), runFinished
	}
	if err := r.finishRun(ctx, pass, runID, candidates, final); err != nil {
		r.logger.Error("artifact gc could not finish a run", "run_id", runID, "error", err)
		return len(candidates), runCarried
	}
	return len(candidates), runFinished
}

// candidates are the run's objects from its body, its run-objects list, and
// a listing of each of its producer prefixes; complete is false when a
// listing was cut short.
func (r *Reclaimer) candidates(ctx context.Context, runID string, body RunBody) ([]candidate, bool, error) {
	seen := map[string]int{}
	var out []candidate
	add := func(id, class string) int {
		if at, ok := seen[id]; ok {
			return at
		}
		seen[id] = len(out)
		out = append(out, candidate{id: id, class: class})
		return len(out) - 1
	}
	prefixes := map[string]struct{}{}
	for _, o := range body.Objects {
		add(o.ID, o.Class)
		if producer, ok := artifacts.ProducingRunFromObjectKey(o.ID); ok && producer == runID {
			if prefix, ok := artifacts.ObjectProducerPrefix(o.ID); ok {
				prefixes[prefix] = struct{}{}
			}
		}
	}
	list, err := r.store.RunObjectList(ctx, runID)
	if err != nil && !errors.Is(err, artifacts.ErrNotFound) {
		return nil, false, err
	}
	for _, id := range list {
		add(id, ClassUnlisted)
	}
	complete := true
	for _, prefix := range sortedKeys(prefixes) {
		objects, more, err := r.store.ListUnder(ctx, prefix, r.config.Batch)
		if err != nil {
			return nil, false, err
		}
		if more {
			complete = false
		}
		for _, o := range objects {
			out[add(o.Key, ClassUnlisted)].size = o.Size
		}
	}
	return out, complete, nil
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// examineObject decides one object of due run runID and, in a deleting
// pass, deletes it when the rule allows.
func (r *Reclaimer) examineObject(ctx context.Context, pass *passState, runID, runNamespace string, c candidate) keepClass {
	keep := func(namespace, reason string, cls keepClass, waitsFor time.Time) keepClass {
		pass.count(namespace, c.class, OutcomeKept, reason, c.size)
		pass.samples.add(runID, c, OutcomeKept, reason, waitsFor)
		return cls
	}
	failed := func(namespace, reason string, err error) keepClass {
		r.logger.Warn("artifact gc failed on an object; keeping it", "run_id", runID, "object", c.id, "reason", reason, "error", err)
		pass.count(namespace, c.class, OutcomeFailed, reason, c.size)
		return classTransient
	}
	producer, ok := artifacts.ProducingRunFromObjectKey(c.id)
	if !ok {
		return keep(runNamespace, ReasonUnparseableKey, classFinal, time.Time{})
	}
	p := r.verifyRetired(ctx, pass, producer)
	ns := p.namespace
	if ns == "" {
		ns = runNamespace
	}
	// An object another run produced is that run's GC's to examine.
	notYet := classWaiting
	if producer != runID {
		notYet = classDelegated
	}
	switch p.state {
	case retRetired:
	case retLive:
		return keep(ns, ReasonRunLive, notYet, time.Time{})
	case retRetained:
		return keep(ns, ReasonRunRetained, notYet, time.Time{})
	case retLateCommit:
		return keep(ns, ReasonLateCommit, classFinal, time.Time{})
	default:
		return keep(ns, ReasonRunUnverified, classTransient, time.Time{})
	}
	if pass.storeNow.Sub(p.retiredAt) <= r.config.Grace {
		return keep(ns, ReasonRunInGrace, notYet, time.Time{})
	}
	for attempt := 1; ; attempt++ {
		// (c) after the walk: every other run holding a marker.
		markers, err := r.store.ListObjectReferences(ctx, c.id)
		if errors.Is(err, artifacts.ErrTooManyReferences) {
			return keep(ns, ReasonTooManyReferences, classFinal, time.Time{})
		}
		if errors.Is(err, artifacts.ErrInvalidURI) {
			return keep(ns, ReasonUnparseableKey, classFinal, time.Time{})
		}
		if err != nil {
			return failed(ns, ReasonListFailed, err)
		}
		latest := p.retiredAt
		worst, reason := classDone, ""
		for _, x := range markers {
			if x == producer {
				continue
			}
			cls, why, at := r.claimant(ctx, pass, x, c.id)
			if cls > worst {
				worst, reason = cls, why
			}
			if at.After(latest) {
				latest = at
			}
		}
		if worst != classDone {
			return keep(ns, reason, worst, time.Time{})
		}
		// (d) the low-water rule.
		if !pass.lw.admits(latest, r.config.Grace) {
			return keep(ns, ReasonLowWater, classWaiting, latest.Add(r.config.Grace))
		}
		if !pass.deleting {
			pass.count(ns, c.class, OutcomeWouldDelete, ReasonUnreferenced, c.size)
			pass.samples.add(runID, c, OutcomeWouldDelete, ReasonUnreferenced, time.Time{})
			return classDone
		}
		// (e) the markers again, then the producer's commit prefix again,
		// then the delete.
		again, err := r.store.ListObjectReferences(ctx, c.id)
		if errors.Is(err, artifacts.ErrTooManyReferences) {
			return keep(ns, ReasonTooManyReferences, classFinal, time.Time{})
		}
		if err != nil {
			return failed(ns, ReasonListFailed, err)
		}
		if !sameRuns(markers, again) {
			if attempt >= markerRelists {
				return failed(ns, ReasonListingTooOld, errors.New("the object's markers kept changing"))
			}
			continue
		}
		empty, err := r.commitsEmpty(ctx, p.namespace, producer)
		if err != nil {
			return failed(ns, ReasonListFailed, err)
		}
		if !empty {
			p.state = retLateCommit
			pass.verified[producer] = p
			if producer == runID {
				pass.run.lateCommit = true
			}
			r.logger.Warn("artifact gc found a commit under a retired run's prefix before a delete; keeping everything it produced for good",
				"run_id", producer)
			return keep(ns, ReasonLateCommit, classFinal, time.Time{})
		}
		return r.deleteObject(ctx, pass, ns, c, markers)
	}
}

// claimant decides one other run holding a marker on objectID, and returns
// its retirement when it is retired past the grace.
func (r *Reclaimer) claimant(ctx context.Context, pass *passState, runID, objectID string) (keepClass, string, time.Time) {
	x := r.verifyRetired(ctx, pass, runID)
	switch x.state {
	case retRetired:
		if pass.storeNow.Sub(x.retiredAt) > r.config.Grace {
			return classDone, "", x.retiredAt
		}
		return r.claimClass(ctx, pass, runID, objectID, x, ReasonClaimInGrace)
	case retNeverSeen:
		return classFinal, ReasonClaimOrphaned, time.Time{}
	default:
		return r.claimClass(ctx, pass, runID, objectID, x, ReasonClaimLive)
	}
}

// claimClass is delegated when the claimant is archived and its list holds
// the object, claim_unlisted (waiting) when it is archived and its list does
// not, and waiting otherwise.
func (r *Reclaimer) claimClass(ctx context.Context, pass *passState, runID, objectID string, x retirement, reason string) (keepClass, string, time.Time) {
	if !x.archived {
		return classWaiting, reason, time.Time{}
	}
	list, ok := pass.lists[runID]
	if !ok {
		list = listRead{ids: map[string]struct{}{}}
		ids, err := r.store.RunObjectList(ctx, runID)
		switch {
		case errors.Is(err, artifacts.ErrNotFound):
		case err != nil:
			list.err = err
		default:
			for _, id := range ids {
				list.ids[id] = struct{}{}
			}
		}
		pass.lists[runID] = list
	}
	if list.err != nil {
		return classTransient, ReasonListFailed, time.Time{}
	}
	if _, holds := list.ids[objectID]; holds {
		return classDelegated, reason, time.Time{}
	}
	return classWaiting, ReasonClaimUnlisted, time.Time{}
}

func sameRuns(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// deleteObject removes one unreferenced object, then every marker on it.
func (r *Reclaimer) deleteObject(ctx context.Context, pass *passState, namespace string, c candidate, markers []string) keepClass {
	info, err := r.store.StatObject(ctx, c.id)
	gone := errors.Is(err, artifacts.ErrNotFound)
	if err != nil && !gone {
		r.logger.Warn("artifact gc could not stat an object; keeping it", "object", c.id, "error", err)
		pass.count(namespace, c.class, OutcomeFailed, ReasonStoreError, c.size)
		return classTransient
	}
	// Removed even when already gone: a companion a killed delete left
	// behind goes with it (RemoveReclaimedObject is idempotent).
	{
		if err := r.store.RemoveReclaimedObject(ctx, c.id); err != nil {
			reason := ReasonStoreError
			if errors.Is(err, artifacts.ErrStoreRefused) {
				reason = ReasonStoreRefused
			}
			r.logger.Error("artifact gc could not delete an object", "object", c.id, "reason", reason, "error", err)
			pass.count(namespace, c.class, OutcomeFailed, reason, info.Size)
			return classTransient
		}
	}
	if gone {
		pass.count(namespace, c.class, OutcomeAlreadyGone, ReasonUnreferenced, 0)
	} else {
		pass.count(namespace, c.class, OutcomeDeleted, ReasonUnreferenced, info.Size)
		pass.run.deleted++
		pass.run.bytes += info.Size
	}
	if len(markers) > 0 {
		if err := r.store.RemoveObjectReferences(ctx, c.id, markers); err != nil {
			// The object is gone; its markers wait for the next pass,
			// which finds it already gone and releases them.
			r.logger.Error("artifact gc could not release a deleted object's markers", "object", c.id, "error", err)
			return classTransient
		}
	}
	return classDone
}

// finishRun releases a finished run's own markers and list, then writes its
// GC record. final lists the objects kept for good.
func (r *Reclaimer) finishRun(ctx context.Context, pass *passState, runID string, candidates []candidate, final []string) error {
	for _, c := range candidates {
		if err := r.store.RemoveObjectReferences(ctx, c.id, []string{runID}); err != nil {
			if errors.Is(err, artifacts.ErrInvalidURI) {
				continue
			}
			return err
		}
	}
	if err := r.store.RemoveRunObjectList(ctx, runID); err != nil {
		return err
	}
	if err := r.store.CreateReclaimRecord(ctx, artifacts.ReclaimRecord{
		RunID: runID, ReclaimedAt: pass.storeNow, Kept: final, LateCommit: pass.run.lateCommit,
		ObjectsDeleted: pass.run.deleted, BytesDeleted: pass.run.bytes,
	}); err != nil {
		return err
	}
	r.markDone(runID)
	return nil
}

// keepLateCommit keeps everything a run with a late commit produced, for
// good, keeps its markers and list, and in delete mode records that.
func (r *Reclaimer) keepLateCommit(ctx context.Context, pass *passState, runID string, self retirement) (int, runOutcome) {
	body, err := r.bodies.ReclaimBody(ctx, runID)
	if err != nil {
		return 0, runCarried
	}
	candidates, _, err := r.candidates(ctx, runID, body)
	if err != nil {
		return 0, runCarried
	}
	var kept []string
	for _, c := range candidates {
		if producer, ok := artifacts.ProducingRunFromObjectKey(c.id); ok && producer == runID {
			pass.count(self.namespace, c.class, OutcomeKept, ReasonLateCommit, c.size)
			kept = append(kept, c.id)
		}
	}
	if pass.deleting {
		if err := r.store.CreateReclaimRecord(ctx, artifacts.ReclaimRecord{
			RunID: runID, ReclaimedAt: pass.storeNow, LateCommit: true, Kept: kept,
		}); err != nil {
			r.logger.Error("artifact gc could not record a late commit", "run_id", runID, "error", err)
			return len(candidates), runCarried
		}
		r.markDone(runID)
	}
	return len(candidates), runLateCommit
}

// sampler collects a preview's samples; a nil sampler collects nothing.
type sampler struct {
	limit int
	out   []Sample
}

// Sample is one object a preview reports.
type Sample struct {
	RunID    string
	ObjectID string
	Bytes    int64
	Outcome  string
	Reason   string
	// WaitsFor is L(O) + M for a lowwater object: the mark has to pass it.
	WaitsFor time.Time
}

func (s *sampler) add(runID string, c candidate, outcome, reason string, waitsFor time.Time) {
	if s == nil || len(s.out) >= s.limit {
		return
	}
	s.out = append(s.out, Sample{RunID: runID, ObjectID: c.id, Bytes: c.size, Outcome: outcome, Reason: reason, WaitsFor: waitsFor})
}
