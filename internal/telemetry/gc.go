package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/archive"
)

// The artifact GC's instruments (docs/DEPLOY.md, "The artifact GC").
const (
	GCObjectsMetric          = "ork.gc.objects"
	GCBytesMetric            = "ork.gc.bytes"
	GCWouldDeleteObjects     = "ork.gc.would_delete.objects"
	GCWouldDeleteBytes       = "ork.gc.would_delete.bytes"
	GCKeptObjects            = "ork.gc.kept.objects"
	GCRunsMetric             = "ork.gc.runs"
	GCPassesMetric           = "ork.gc.passes"
	GCPassDurationMetric     = "ork.gc.pass.duration"
	GCLagMetric              = "ork.gc.lag"
	GCDueRunsMetric          = "ork.gc.due_runs"
	GCWaitingRunsMetric      = "ork.gc.waiting_runs"
	GCHeldMetric             = "ork.gc.held"
	GCModeMetric             = "ork.gc.mode"
	GCGraceMetric            = "ork.gc.grace"
	GCLastPassMetric         = "ork.gc.last_pass"
	GCLastProgressMetric     = "ork.gc.last_progress"
	GCStoreVersionedMetric   = "ork.gc.store_versioned"
	GCLowWaterAgeMetric      = "ork.gc.lowwater.age"
	GCLowWaterUnknownMetric  = "ork.gc.lowwater.unknown"
	GCLowWaterChainsMetric   = "ork.gc.lowwater.chains"
	DeploymentGCObjects      = "ork.deployment.gc.objects"
	DeploymentGCBytes        = "ork.deployment.gc.bytes"
	DeploymentGCWouldDelete  = "ork.deployment.gc.would_delete.bytes"
	AttrGCObjectClass        = "ork.gc.object_class"
	AttrGCReason             = "ork.gc.reason"
	AttrGCHoldReason         = "ork.gc.hold_reason"
	AttrGCLowWaterCause      = "ork.gc.lowwater_cause"
	AttrGCMode               = "mode"
	gcRunOutcomeFinished     = "finished"
	gcRunOutcomeCarriedOver  = "carried_over"
	gcRunOutcomeWaiting      = "waiting"
	gcRunOutcomeHeld         = "held"
	gcPassDurationUnitSecond = "s"
)

// The label values the GC's instruments may carry (docs/DEPLOY.md). A value
// outside its set is never recorded: the record is dropped and logged once.
var (
	gcClasses = []string{archive.ClassArtifact, archive.ClassValues, archive.ClassSystemArtifact,
		"name_manifest", "marker", "run_objects", archive.ClassUnlisted}
	gcOutcomes   = []string{archive.OutcomeDeleted, archive.OutcomeAlreadyGone, archive.OutcomeKept, archive.OutcomeFailed}
	gcKeptReason = []string{archive.ReasonRunLive, archive.ReasonRunRetained, archive.ReasonRunInGrace,
		archive.ReasonRunUnverified, archive.ReasonLowWater, archive.ReasonClaimLive, archive.ReasonClaimInGrace,
		archive.ReasonClaimUnlisted, archive.ReasonClaimOrphaned, archive.ReasonTooManyReferences,
		archive.ReasonUnparseableKey, archive.ReasonLateCommit}
	gcFailedReason = []string{archive.ReasonStoreError, archive.ReasonStoreRefused, archive.ReasonListFailed,
		archive.ReasonListingTooOld, archive.ReasonDeadline}
	gcReasons = slices.Concat(gcKeptReason, []string{archive.ReasonUnreferenced}, gcFailedReason)
	// gcOutcomeReasons are the reasons each outcome may carry.
	gcOutcomeReasons = map[string][]string{
		archive.OutcomeDeleted:     {archive.ReasonUnreferenced},
		archive.OutcomeAlreadyGone: {archive.ReasonUnreferenced},
		archive.OutcomeKept:        gcKeptReason,
		archive.OutcomeFailed:      gcFailedReason,
	}
	gcHoldReasons = []string{archive.HoldComposedArtifacts, archive.HoldCapabilitiesUnreadable,
		archive.HoldLowWaterUnknown, archive.HoldClockSkew, archive.HoldExecutionsMissing, archive.HoldLocalBackend}
	gcLowWaterCauses = []string{archive.LowWaterSweepUnclean, archive.LowWaterNoSweep, archive.LowWaterWalkFailed,
		archive.LowWaterMidSwitch, archive.LowWaterNoCreation, archive.LowWaterIndexStale}
	gcModes         = []string{string(archive.ReclaimOff), string(archive.ReclaimDryRun), string(archive.ReclaimDelete)}
	gcPassOutcomes  = []string{"completed", "partial", "held", "warming", "failed"}
	gcPassDurations = []float64{0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 900, 1800}
)

// GCRouter is the sink a namespaced GC record goes to, or (nil, false) when
// it is dropped, which the router counts itself. A nil GCRouter sends every
// record to the default sink, which is right only for a deployment with one.
type GCRouter func(ctx context.Context, namespace string) (*Providers, bool)

// gcNamespaced are one sink's namespaced GC instruments.
type gcNamespaced struct {
	objects     metric.Int64Counter
	bytes       metric.Int64Counter
	wouldObjs   metric.Int64Gauge
	wouldBytes  metric.Int64Gauge
	keptObjects metric.Int64Gauge
}

// gcState is what the GC's observable gauges report: the last pass's view.
type gcState struct {
	// passed says a pass happened (last_pass, last_progress); observed says
	// one got far enough to see holds, a mark and due runs. A gauge whose
	// value no pass observed reports nothing, never a healthy-looking 0.
	passed         bool
	observed       bool
	lag            float64
	due, waiting   int64
	held           map[string]bool
	lastPass       time.Time
	lastProgress   time.Time
	versioned      bool
	lowWaterKnown  bool
	lowWaterAge    float64
	lowWaterCause  string
	lowWaterChains int64
}

// gcCycleKey is one series of the last complete dry-run cycle's gauges.
type gcCycleKey struct {
	sink       *Providers
	namespace  string
	class      string
	reason     string
	keptSeries bool
}

// GCObserver turns the artifact GC's passes into the metrics docs/DEPLOY.md
// lists. It implements archive.ReclaimObserver. Recording never blocks a pass
// and never fails it: a record that cannot be routed or built is dropped.
type GCObserver struct {
	defaults *Providers
	route    GCRouter
	logger   *slog.Logger

	passes      metric.Int64Counter
	duration    metric.Float64Histogram
	runs        metric.Int64Counter
	twinObjects metric.Int64Counter
	twinBytes   metric.Int64Counter
	twinWould   metric.Int64Gauge

	namespaced providerCache[*gcNamespaced]
	warned     sync.Map

	mu    sync.Mutex
	state gcState
	// cycle holds every series the last dry-run cycle recorded, so the next
	// one sets a series it no longer has back to zero.
	cycle map[gcCycleKey]struct{}
}

// NewGCObserver registers the GC's instruments on p, the default sink. mode
// and grace are the settings in force: the mode gauge is exported from boot,
// whether or not the GC runs, so an operator can tell off from silent. It
// works, as a no-op, on nil or disabled Providers; an instrument that cannot
// be built is logged and the rest still record.
func NewGCObserver(p *Providers, route GCRouter, mode archive.ReclaimMode, grace time.Duration, logger *slog.Logger) *GCObserver {
	if logger == nil {
		logger = slog.Default()
	}
	o := &GCObserver{defaults: p, route: route, logger: logger,
		state: gcState{lastProgress: time.Now(), held: map[string]bool{}}}
	meter := meterFor(p)
	var errs []error
	keep := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error
	o.passes, err = meter.Int64Counter(GCPassesMetric,
		metric.WithDescription("Artifact GC passes, by outcome."), metric.WithUnit("{pass}"))
	keep(err)
	o.duration, err = meter.Float64Histogram(GCPassDurationMetric,
		metric.WithDescription("How long one artifact GC pass took."), metric.WithUnit(gcPassDurationUnitSecond),
		metric.WithExplicitBucketBoundaries(gcPassDurations...))
	keep(err)
	o.runs, err = meter.Int64Counter(GCRunsMetric,
		metric.WithDescription("Retired runs the artifact GC examined, by what became of them: finished, carried over to the next pass, waiting on another run, or held for good by a late commit."),
		metric.WithUnit("{run}"))
	keep(err)
	o.twinObjects, err = meter.Int64Counter(DeploymentGCObjects,
		metric.WithDescription("Objects the artifact GC deleted, found gone, kept or failed on in every namespace, by class, outcome and reason (delete mode)."),
		metric.WithUnit("{object}"))
	keep(err)
	o.twinBytes, err = meter.Int64Counter(DeploymentGCBytes,
		metric.WithDescription("Bytes of the objects the artifact GC deleted, found gone, kept or failed on in every namespace, by class and outcome (delete mode)."),
		metric.WithUnit("By"))
	keep(err)
	o.twinWould, err = meter.Int64Gauge(DeploymentGCWouldDelete,
		metric.WithDescription("Bytes the last complete dry-run cycle of the artifact GC would have deleted, in every namespace."),
		metric.WithUnit("By"))
	keep(err)

	modeName := string(mode)
	_, err = meter.Int64ObservableGauge(GCModeMetric,
		metric.WithDescription("The artifact GC's mode in force on this engine: 1 for its mode, 0 for the others."),
		metric.WithInt64Callback(func(_ context.Context, ob metric.Int64Observer) error {
			for _, m := range gcModes {
				ob.Observe(boolGauge(m == modeName), metric.WithAttributes(attribute.String(AttrGCMode, m)))
			}
			return nil
		}))
	keep(err)
	_, err = meter.Float64ObservableGauge(GCGraceMetric,
		metric.WithDescription("The artifact GC's grace in force."), metric.WithUnit("s"),
		metric.WithFloat64Callback(func(_ context.Context, ob metric.Float64Observer) error {
			ob.Observe(grace.Seconds())
			return nil
		}))
	keep(err)
	keep(o.registerStateGauges(meter))
	if err := errors.Join(errs...); err != nil {
		logger.Warn("telemetry: some artifact GC instruments could not be built; they are not exported", "error", err)
	}
	return o
}

// registerStateGauges registers the gauges that report the last pass. None
// reports anything before the first pass.
func (o *GCObserver) registerStateGauges(meter metric.Meter) error {
	var errs []error
	floatGauge := func(name, desc, unit string, read func(s gcState) (float64, bool)) {
		_, err := meter.Float64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit),
			metric.WithFloat64Callback(func(_ context.Context, ob metric.Float64Observer) error {
				o.mu.Lock()
				s := o.state
				o.mu.Unlock()
				if v, ok := read(s); ok {
					ob.Observe(v)
				}
				return nil
			}))
		if err != nil {
			errs = append(errs, err)
		}
	}
	// seen reports v once a pass observed the state; passed, once any
	// pass happened.
	seen := func(s gcState, v float64) (float64, bool) { return v, s.observed }
	passed := func(s gcState, v float64) (float64, bool) { return v, s.passed }
	floatGauge(GCLagMetric, "How long the oldest run still due has waited since it retired, at the last pass.", "s",
		func(s gcState) (float64, bool) { return seen(s, s.lag) })
	floatGauge(GCDueRunsMetric, "Retired runs the last pass carried over to the next: due, and not yet finished.", "{run}",
		func(s gcState) (float64, bool) { return seen(s, float64(s.due)) })
	floatGauge(GCWaitingRunsMetric, "Retired runs the last pass found waiting on another run, or on the low-water mark; not due.", "{run}",
		func(s gcState) (float64, bool) { return seen(s, float64(s.waiting)) })
	floatGauge(GCLastPassMetric, "When the artifact GC last made a pass.", "s",
		func(s gcState) (float64, bool) { return passed(s, float64(s.lastPass.Unix())) })
	floatGauge(GCLastProgressMetric, "When the artifact GC last finished a run or deleted an object, or this engine started.", "s",
		func(s gcState) (float64, bool) { return passed(s, float64(s.lastProgress.Unix())) })
	floatGauge(GCStoreVersionedMetric, "1 when the bucket keeps versions, so a delete only adds a delete marker.", "1",
		func(s gcState) (float64, bool) { return seen(s, float64(boolGauge(s.versioned))) })
	floatGauge(GCLowWaterAgeMetric, "The age of the oldest open or unarchived run that blocks reclaim, at the last pass; 0 when none does.", "s",
		func(s gcState) (float64, bool) { return s.lowWaterAge, s.observed && s.lowWaterKnown })
	floatGauge(GCLowWaterChainsMetric, "Open runs the last pass's low-water walk read.", "{run}",
		func(s gcState) (float64, bool) { return float64(s.lowWaterChains), s.observed && s.lowWaterKnown })
	_, err := meter.Int64ObservableGauge(GCHeldMetric,
		metric.WithDescription("1 for each reason the last artifact GC pass was held, 0 for the others."),
		metric.WithInt64Callback(func(_ context.Context, ob metric.Int64Observer) error {
			o.mu.Lock()
			defer o.mu.Unlock()
			if !o.state.observed {
				return nil
			}
			for _, reason := range gcHoldReasons {
				ob.Observe(boolGauge(o.state.held[reason]), metric.WithAttributes(attribute.String(AttrGCHoldReason, reason)))
			}
			return nil
		}))
	if err != nil {
		errs = append(errs, err)
	}
	_, err = meter.Int64ObservableGauge(GCLowWaterUnknownMetric,
		metric.WithDescription("1 for the reason the last pass could not compute its low-water mark, 0 for the others."),
		metric.WithInt64Callback(func(_ context.Context, ob metric.Int64Observer) error {
			o.mu.Lock()
			defer o.mu.Unlock()
			if !o.state.observed {
				return nil
			}
			for _, cause := range gcLowWaterCauses {
				ob.Observe(boolGauge(o.state.lowWaterCause == cause), metric.WithAttributes(attribute.String(AttrGCLowWaterCause, cause)))
			}
			return nil
		}))
	if err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// ObserveReclaim records one pass. It never blocks on the network: every
// instrument records into the SDK's in-process aggregation.
func (o *GCObserver) ObserveReclaim(ctx context.Context, report archive.ReclaimReport) {
	if o == nil {
		return
	}
	outcome := report.Outcome
	if !slices.Contains(gcPassOutcomes, outcome) {
		o.warnOnce("pass_outcome:"+outcome, "an artifact GC pass outcome outside the metric contract is not recorded", "outcome", outcome)
	} else if o.passes != nil {
		o.passes.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrOutcome, outcome)))
	}
	if o.duration != nil {
		o.duration.Record(ctx, report.Duration.Seconds())
	}
	if o.runs != nil {
		for name, n := range map[string]int{gcRunOutcomeFinished: report.RunsFinished,
			gcRunOutcomeCarriedOver: report.RunsCarried, gcRunOutcomeWaiting: report.RunsWaiting,
			gcRunOutcomeHeld: report.RunsLateCommit} {
			if n > 0 {
				o.runs.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrOutcome, name)))
			}
		}
	}
	// The router is asked once per namespace per pass: its answer holds
	// for the whole pass, and a router that counts its drops counts one.
	routes := gcRoutes{}
	var deleted int64
	if report.Mode == archive.ReclaimDelete {
		deleted = o.recordObjects(ctx, routes, report.Objects)
	}
	if report.Mode == archive.ReclaimDryRun && report.Cycle != nil {
		o.recordCycle(ctx, routes, report.Cycle)
	}
	o.updateState(report, deleted)
}

// gcWarning is a warning updateState logs once it has released the lock.
type gcWarning struct {
	key, msg string
	args     []any
}

// gcRoutes are one pass's router answers by namespace: nil when dropped.
type gcRoutes map[string]*Providers

// recordObjects counts a delete-mode pass's objects in their namespaces and
// in the twins, and returns how many it deleted.
func (o *GCObserver) recordObjects(ctx context.Context, routes gcRoutes, objects map[archive.TallyKey]archive.Tally) int64 {
	var deleted int64
	for key, t := range objects {
		if key.Outcome == archive.OutcomeDeleted {
			deleted += t.Objects // progress, whatever its labels
		}
		if key.Outcome == archive.OutcomeWouldDelete {
			continue // a held or warming pass's count: no outcome of the contract
		}
		if !o.validObject(key) {
			continue
		}
		classOutcome := []attribute.KeyValue{attribute.String(AttrGCObjectClass, key.Class), attribute.String(AttrOutcome, key.Outcome)}
		withReason := append(slices.Clone(classOutcome), attribute.String(AttrGCReason, key.Reason))
		if o.twinObjects != nil {
			o.twinObjects.Add(ctx, t.Objects, metric.WithAttributes(withReason...))
		}
		if o.twinBytes != nil && t.Bytes > 0 {
			o.twinBytes.Add(ctx, t.Bytes, metric.WithAttributes(classOutcome...))
		}
		inst, _ := o.namespacedFor(ctx, routes, key.Namespace)
		if inst == nil {
			continue
		}
		ns := attribute.String(AttrNamespace, key.Namespace)
		inst.objects.Add(ctx, t.Objects, metric.WithAttributes(append(withReason, ns)...))
		if t.Bytes > 0 {
			inst.bytes.Add(ctx, t.Bytes, metric.WithAttributes(append(classOutcome, ns)...))
		}
	}
	return deleted
}

// recordCycle sets the dry-run gauges to a complete cycle's totals, and every
// series the previous cycle set that this one lacks back to zero.
func (o *GCObserver) recordCycle(ctx context.Context, routes gcRoutes, cycle map[archive.TallyKey]archive.Tally) {
	type sums struct{ objects, bytes int64 }
	values := map[gcCycleKey]sums{}
	var total int64
	for key, t := range cycle {
		switch key.Outcome {
		case archive.OutcomeWouldDelete:
			if !o.validClass(key.Class) {
				continue
			}
			total += t.Bytes
			inst, sink := o.namespacedFor(ctx, routes, key.Namespace)
			if inst == nil {
				continue
			}
			k := gcCycleKey{sink: sink, namespace: key.Namespace, class: key.Class}
			s := values[k]
			s.objects += t.Objects
			s.bytes += t.Bytes
			values[k] = s
		case archive.OutcomeKept:
			if !o.validObject(key) {
				continue
			}
			inst, sink := o.namespacedFor(ctx, routes, key.Namespace)
			if inst == nil {
				continue
			}
			k := gcCycleKey{sink: sink, namespace: key.Namespace, class: key.Class, reason: key.Reason, keptSeries: true}
			s := values[k]
			s.objects += t.Objects
			values[k] = s
		}
	}
	o.mu.Lock()
	previous := o.cycle
	o.cycle = make(map[gcCycleKey]struct{}, len(values))
	for k := range values {
		o.cycle[k] = struct{}{}
	}
	o.mu.Unlock()
	for k := range previous {
		if _, still := values[k]; !still {
			values[k] = sums{}
		}
	}
	for k, s := range values {
		inst, err := o.instrumentsFor(k.sink)
		if err != nil {
			continue
		}
		ns := attribute.String(AttrNamespace, k.namespace)
		class := attribute.String(AttrGCObjectClass, k.class)
		if k.keptSeries {
			inst.keptObjects.Record(ctx, s.objects, metric.WithAttributes(ns, class, attribute.String(AttrGCReason, k.reason)))
			continue
		}
		inst.wouldObjs.Record(ctx, s.objects, metric.WithAttributes(ns, class))
		inst.wouldBytes.Record(ctx, s.bytes, metric.WithAttributes(ns, class))
	}
	if o.twinWould != nil {
		o.twinWould.Record(ctx, total)
	}
}

// updateState keeps what the gauges report. A failed pass changes only when
// the GC last made a pass, and its progress if it made any: what it held,
// its mark and its lag are left as the last pass that got that far saw
// them, and stay unreported while none has. Its warnings are logged after
// the lock is released, so a slow log handler never stalls a collection.
func (o *GCObserver) updateState(report archive.ReclaimReport, deleted int64) {
	var warnings []gcWarning
	defer func() {
		for _, w := range warnings {
			o.warnOnce(w.key, w.msg, w.args...)
		}
	}()
	o.mu.Lock()
	defer o.mu.Unlock()
	s := &o.state
	s.passed = true
	s.lastPass = report.Started.Add(report.Duration)
	if report.RunsFinished > 0 || deleted > 0 {
		s.lastProgress = s.lastPass
	}
	if report.Outcome == "failed" {
		return
	}
	s.observed = true
	s.held = map[string]bool{}
	for _, reason := range report.Held {
		if slices.Contains(gcHoldReasons, reason) {
			s.held[reason] = true
		} else {
			warnings = append(warnings, gcWarning{"hold:" + reason, "an artifact GC hold reason outside the metric contract is not recorded", []any{"reason", reason}})
		}
	}
	s.versioned = report.StoreVersioned
	s.due, s.waiting = int64(report.RunsCarried), int64(report.RunsWaiting)
	s.lag = 0
	if !report.OldestDue.IsZero() && !report.StoreNow.IsZero() {
		s.lag = max(report.StoreNow.Sub(report.OldestDue).Seconds(), 0)
	}
	lw := report.LowWater
	s.lowWaterKnown, s.lowWaterCause, s.lowWaterAge, s.lowWaterChains = lw.Known, "", 0, int64(lw.Chains)
	switch {
	case !lw.Known:
		if slices.Contains(gcLowWaterCauses, lw.Cause) {
			s.lowWaterCause = lw.Cause
		} else if lw.Cause != "" {
			warnings = append(warnings, gcWarning{"lowwater:" + lw.Cause, "an artifact GC low-water cause outside the metric contract is not recorded", []any{"cause", lw.Cause}})
		}
	case !lw.Infinite && !report.StoreNow.IsZero():
		s.lowWaterAge = max(report.StoreNow.Sub(lw.At).Seconds(), 0)
	}
}

// namespacedFor is the instruments of the sink a namespace's records go to,
// and that sink; (nil, nil) when the record is dropped.
func (o *GCObserver) namespacedFor(ctx context.Context, routes gcRoutes, namespace string) (*gcNamespaced, *Providers) {
	sink := o.defaults
	if o.route != nil {
		p, asked := routes[namespace]
		if !asked {
			if routed, ok := o.route(ctx, namespace); ok {
				p = routed
			}
			routes[namespace] = p
		}
		if p == nil {
			return nil, nil
		}
		sink = p
	}
	inst, err := o.instrumentsFor(sink)
	if err != nil {
		return nil, nil
	}
	return inst, sink
}

// instrumentsFor is p's namespaced GC instruments, built once per sink. A
// sink whose instruments cannot be built is logged once and its records
// dropped.
func (o *GCObserver) instrumentsFor(p *Providers) (*gcNamespaced, error) {
	inst, err := o.namespaced.getOrBuild(p, func() (*gcNamespaced, error) { return newGCNamespaced(p) })
	if err != nil {
		if _, warned := o.warned.LoadOrStore(p, struct{}{}); !warned {
			o.logger.Warn("telemetry: a sink's artifact GC instruments could not be built; its records are dropped", "error", err)
		}
	}
	return inst, err
}

func newGCNamespaced(p *Providers) (*gcNamespaced, error) {
	meter := meterFor(p)
	var inst gcNamespaced
	var errs []error
	var err error
	inst.objects, err = meter.Int64Counter(GCObjectsMetric,
		metric.WithDescription("Objects the artifact GC deleted, found gone, kept or failed on, by namespace, class, outcome and reason (delete mode)."),
		metric.WithUnit("{object}"))
	errs = append(errs, err)
	inst.bytes, err = meter.Int64Counter(GCBytesMetric,
		metric.WithDescription("Bytes of the objects the artifact GC deleted, found gone, kept or failed on, by namespace, class and outcome (delete mode)."),
		metric.WithUnit("By"))
	errs = append(errs, err)
	inst.wouldObjs, err = meter.Int64Gauge(GCWouldDeleteObjects,
		metric.WithDescription("Objects the last complete dry-run cycle of the artifact GC would have deleted, by namespace and class."),
		metric.WithUnit("{object}"))
	errs = append(errs, err)
	inst.wouldBytes, err = meter.Int64Gauge(GCWouldDeleteBytes,
		metric.WithDescription("Bytes the last complete dry-run cycle of the artifact GC would have deleted, by namespace and class."),
		metric.WithUnit("By"))
	errs = append(errs, err)
	inst.keptObjects, err = meter.Int64Gauge(GCKeptObjects,
		metric.WithDescription("Objects the last complete dry-run cycle of the artifact GC kept, by namespace, class and reason."),
		metric.WithUnit("{object}"))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &inst, nil
}

func (o *GCObserver) validClass(class string) bool {
	if slices.Contains(gcClasses, class) {
		return true
	}
	o.warnOnce("class:"+class, "an artifact GC object class outside the metric contract is not recorded", "class", class)
	return false
}

// validObject reports whether a tally's labels are all in the contract's
// sets: its class, its outcome, and a reason that belongs to that outcome.
func (o *GCObserver) validObject(key archive.TallyKey) bool {
	if !o.validClass(key.Class) {
		return false
	}
	ok := slices.Contains(gcOutcomeReasons[key.Outcome], key.Reason)
	if !ok {
		o.warnOnce("object:"+key.Outcome+"/"+key.Reason, "an artifact GC outcome or reason outside the metric contract is not recorded",
			"outcome", key.Outcome, "reason", key.Reason)
	}
	return ok
}

func (o *GCObserver) warnOnce(key, msg string, args ...any) {
	if _, warned := o.warned.LoadOrStore(key, struct{}{}); !warned {
		o.logger.Warn("telemetry: "+msg, args...)
	}
}
