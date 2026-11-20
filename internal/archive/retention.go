package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// namespacePropagationWindow bounds how long Reconcile and Ensure tolerate a
// NamespaceNotFound from the cluster before giving up. It exists because
// Ensure runs immediately after task.EnsureNamespace in the same boot
// sequence (cmd/ork/main.go), and a namespace Temporal's dev-server just
// accepted a RegisterNamespace for is not always immediately visible to the
// very next admin RPC against it — a propagation delay internal to the
// cluster, observed in practice, not an operator misconfiguration. Any error
// OTHER than NamespaceNotFound (including one that persists past this
// window) is returned immediately, so a genuine failure still fails fast and
// Ensure's fail-closed boot guarantee holds for everything except this one
// transient, self-inflicted race.
const namespacePropagationWindow = 10 * time.Second

// retryOnNamespaceNotFound retries fn while it fails with NamespaceNotFound,
// up to namespacePropagationWindow. See that constant's doc for why.
func retryOnNamespaceNotFound(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(namespacePropagationWindow)
	for {
		err := fn()
		var notFound *serviceerror.NamespaceNotFound
		if err == nil || !errors.As(err, &notFound) || !time.Now().Before(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// RetentionAdmin is the narrowed cluster seam RetentionController needs: the
// same two admin-retention methods engine.TemporalRunReader already exposes
// (NamespaceRetention, SetNamespaceRetention), restated here rather than
// imported so this package never depends on internal/engine (see the package
// doc) — a plain structural match is enough for TemporalRunReader to satisfy
// it.
type RetentionAdmin interface {
	NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error)
	SetNamespaceRetention(ctx context.Context, namespace string, d time.Duration) (time.Duration, error)
}

// DefaultMaxRunRetention is what a deployment that never chose a maximum
// gets. It equals task.namespaceRetention (internal/task/namespace.go), which
// is what a freshly registered Temporal namespace's own retention is set to
// at boot — the two are pinned together by
// TestDefaultMaxRunRetention_MatchesNamespaceProvisioning, so a fresh
// cluster's backstop and its ceiling start from the same number.
const DefaultMaxRunRetention = 72 * time.Hour

// RetentionController owns the one invariant this feature depends on:
// backstop TTL (the single Temporal namespace's own
// WorkflowExecutionRetentionTtl) is always at least the deployment maximum
// run retention plus an enforcement margin, so Temporal's own passive
// deletion can never fire before OrkStack's own active retirement would.
//
// The Temporal TTL itself is never a user-facing setting — see AGENTS.md §7
// — and this type is the only thing in the engine that ever writes it.
type RetentionController struct {
	policies artifacts.RetentionPolicyStore
	admin    RetentionAdmin
	// temporalNamespace is the deployment's Temporal partition, never an
	// OrkStack namespace.
	temporalNamespace string
	// margin is the enforcement margin: how far past the maximum run
	// retention the backstop TTL sits.
	margin time.Duration
	// keyLife is how long an idempotency key's claim lives (its TTL and its
	// grace). The backstop is never below it: a retry under a key tells a run
	// that never started from one that finished only by finding it in the
	// cluster or the archive, so the cluster may not delete a finished run
	// before its key has expired. Zero when nothing is wired.
	keyLife time.Duration
	logger  *slog.Logger
	// observer is told each time the cluster refuses to change the
	// backstop; nil reports nothing.
	observer RetentionObserver
	// warnedMu guards warned, the last covered refusal logged at WARN, so a
	// refusal that repeats every cleanup pass is logged once at WARN and
	// then at DEBUG until what it reports changes.
	warnedMu sync.Mutex
	warned   [2]time.Duration
}

// RetentionObserver is told each time the cluster refuses the engine the
// right to change its namespace's retention: covered when the retention the
// cluster keeps is already at least the backstop, so nothing is at risk, and
// not covered when it is shorter, which the caller then fails on.
type RetentionObserver interface {
	ObserveBackstopRefused(ctx context.Context, covered bool)
}

// WithObserver sets where refusals are reported.
func (c *RetentionController) WithObserver(o RetentionObserver) *RetentionController {
	c.observer = o
	return c
}

// BackstopRefusedError is a cluster that refused to set its namespace's
// retention to the backstop, and that keeps closed runs for less than it: the
// cluster could delete a run before OrkStack retires it into the archive, so
// the caller must not go on as if the backstop held. It names the retention a
// cluster admin must set, and the command that sets it.
type BackstopRefusedError struct {
	Namespace string
	// Current is the retention the cluster keeps; Want is the backstop.
	Current, Want time.Duration
	Err           error
}

func (e *BackstopRefusedError) Error() string {
	return fmt.Sprintf("archive: the cluster refused to set temporal namespace %q's retention to %s, and the %s it keeps is shorter, "+
		"so it could delete a closed run before OrkStack archives it; have a cluster admin set it: "+
		"temporal operator namespace update --namespace %s --retention %s: %v",
		e.Namespace, retentionFlag(e.Want), retentionFlag(e.Current), e.Namespace, retentionFlag(e.Want), e.Err)
}

func (e *BackstopRefusedError) Unwrap() error { return e.Err }

// retentionFlag writes d as the temporal CLI's --retention takes it: whole
// hours as "96h", anything else in Go's form.
func retentionFlag(d time.Duration) string {
	if d > 0 && d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	}
	return d.String()
}

// updateRefused reports whether err is the cluster refusing the engine the
// right to update its namespace: PermissionDenied, from an authorizer, or
// Unimplemented, from a cluster that takes no namespace updates from clients.
// Any other failure is not a refusal of rights and is returned as it was.
func updateRefused(err error) bool {
	var denied *serviceerror.PermissionDenied
	var unimplemented *serviceerror.Unimplemented
	if errors.As(err, &denied) || errors.As(err, &unimplemented) {
		return true
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unimplemented:
		return true
	}
	return false
}

// refused decides a refused update: covered when the cluster already keeps
// closed runs at least as long as want, which boots with a warning, and a
// *BackstopRefusedError otherwise. Each is reported to the observer.
func (c *RetentionController) refused(ctx context.Context, current, want time.Duration, err error) error {
	covered := current >= want
	if c.observer != nil {
		c.observer.ObserveBackstopRefused(ctx, covered)
	}
	if !covered {
		return &BackstopRefusedError{Namespace: c.temporalNamespace, Current: current, Want: want, Err: err}
	}
	c.warnedMu.Lock()
	repeat := c.warned == [2]time.Duration{current, want}
	c.warned = [2]time.Duration{current, want}
	c.warnedMu.Unlock()
	level := slog.LevelWarn
	if repeat {
		level = slog.LevelDebug
	}
	c.logger.Log(ctx, level, "the cluster refused to set the retention backstop; the retention it keeps covers it",
		"temporal_namespace", c.temporalNamespace, "cluster_retention", current, "backstop", want, "error", err)
	return nil
}

// NewRetentionController builds a controller over the given policy store and
// cluster admin seam, both bound to the same deployment partition.
func NewRetentionController(
	policies artifacts.RetentionPolicyStore,
	admin RetentionAdmin,
	temporalNamespace string,
	margin time.Duration,
	logger *slog.Logger,
) *RetentionController {
	if logger == nil {
		logger = slog.Default()
	}
	return &RetentionController{
		policies:          policies,
		admin:             admin,
		temporalNamespace: temporalNamespace,
		margin:            margin,
		logger:            logger,
	}
}

// Policy reads the stored policy. An error matching artifacts.ErrNotFound
// means Ensure has not run yet on this deployment.
func (c *RetentionController) Policy(ctx context.Context) (*orkstackv1.RetentionPolicy, error) {
	return c.policies.Get(ctx)
}

// WithKeyLife sets the idempotency key's life the backstop is kept above:
// engine.idempotency_ttl plus the claim's grace. It moves the cluster's own
// retention only, never a namespace's, and runs are retired exactly when they
// were.
func (c *RetentionController) WithKeyLife(life time.Duration) *RetentionController {
	c.keyLife = life
	return c
}

// Enforced is the backstop TTL for a given maximum: the larger of max and a
// key's life, plus the margin.
func (c *RetentionController) Enforced(max time.Duration) time.Duration {
	return maxDuration(max, c.keyLife) + c.margin
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// ClusterRetention is the retention the cluster itself reports now, which is
// what decides when it deletes a closed run: another replica, or an older
// release, may have set it since this one did, so a caller that must know
// what the cluster keeps reads this, never Enforced.
func (c *RetentionController) ClusterRetention(ctx context.Context) (time.Duration, error) {
	d, err := c.admin.NamespaceRetention(ctx, c.temporalNamespace)
	if err != nil {
		return 0, fmt.Errorf("archive: read cluster retention for %q: %w", c.temporalNamespace, err)
	}
	return d, nil
}

// Reconcile makes the cluster's real retention TTL equal Enforced(max),
// raising or lowering it as needed. It is a no-op when the cluster already
// reports that value.
func (c *RetentionController) Reconcile(ctx context.Context, max time.Duration) error {
	want := c.Enforced(max)
	var current time.Duration
	if err := retryOnNamespaceNotFound(ctx, func() error {
		var err error
		current, err = c.admin.NamespaceRetention(ctx, c.temporalNamespace)
		return err
	}); err != nil {
		return fmt.Errorf("archive: read cluster retention for %q: %w", c.temporalNamespace, err)
	}
	if current == want {
		return nil
	}
	if err := retryOnNamespaceNotFound(ctx, func() error {
		_, err := c.admin.SetNamespaceRetention(ctx, c.temporalNamespace, want)
		return err
	}); err != nil {
		if updateRefused(err) {
			return c.refused(ctx, current, want, err)
		}
		return fmt.Errorf("archive: set cluster retention for %q to %s: %w", c.temporalNamespace, want, err)
	}
	c.logger.Info("retention backstop reconciled",
		"temporal_namespace", c.temporalNamespace, "max_run_retention", max, "backstop", want)
	return nil
}

// Ensure runs once at boot. When no policy document exists yet, it observes
// the cluster's current retention TTL, writes it as the deployment maximum
// (conditionally: IfAbsent, so two replicas booting together converge on one
// value rather than each raising the ceiling in turn), then reconciles the
// backstop to max+margin. This is the whole upgrade migration: an existing
// deployment's current retention becomes the new ceiling unchanged, and
// nothing is tightened.
//
// Ensure MUST materialize the document rather than merely derive a ceiling
// from the cluster's current TTL on every boot: reading the TTL, treating it
// as the maximum and reconciling would raise the TTL by another margin every
// restart, growing without bound.
func (c *RetentionController) Ensure(ctx context.Context) (*orkstackv1.RetentionPolicy, error) {
	policy, err := c.policies.Get(ctx)
	switch {
	case err == nil:
		// Already materialized by an earlier boot (this replica's or another
		// one's). Still reconcile: an operator may have set a new maximum
		// while the cluster was unreachable, or the margin config may have
		// changed.
		if err := c.Reconcile(ctx, policy.GetMaxRunRetention().AsDuration()); err != nil {
			return nil, err
		}
		return policy, nil
	case errors.Is(err, artifacts.ErrNotFound):
		// First boot on this deployment: fall through to materialize below.
	default:
		return nil, fmt.Errorf("archive: read retention policy: %w", err)
	}
	// The maximum is derived from the retention the cluster keeps, and the
	// probe that asks whether this engine may change it writes that same
	// value back. A writer that changes the retention around the probe (most
	// likely another replica materializing first) is seen by the re-read
	// after it, and the derivation starts again from what that writer left,
	// a bounded number of times. A change made between the read and the probe
	// is overwritten with the value read; the Reconcile below sets the
	// backstop the stored maximum implies either way.
	var max time.Duration
	for attempt := 1; ; attempt++ {
		var observed time.Duration
		if err := retryOnNamespaceNotFound(ctx, func() error {
			var err error
			observed, err = c.admin.NamespaceRetention(ctx, c.temporalNamespace)
			return err
		}); err != nil {
			return nil, fmt.Errorf("archive: observe cluster retention for %q: %w", c.temporalNamespace, err)
		}
		max, err = c.firstMaximum(ctx, observed)
		if !errors.Is(err, errRetentionMoved) {
			if err != nil {
				return nil, err
			}
			break
		}
		if attempt == firstBootAttempts {
			return nil, fmt.Errorf("archive: the retention of temporal namespace %q kept changing while this engine derived its maximum, %d times: %w",
				c.temporalNamespace, attempt, err)
		}
		switch winner, gerr := c.policies.Get(ctx); {
		case gerr == nil:
			// Another replica materialized the policy meanwhile: adopt it.
			if err := c.Reconcile(ctx, winner.GetMaxRunRetention().AsDuration()); err != nil {
				return nil, err
			}
			return winner, nil
		case !errors.Is(gerr, artifacts.ErrNotFound):
			return nil, fmt.Errorf("archive: read retention policy: %w", gerr)
		}
	}
	written, err := c.policies.Put(ctx, &orkstackv1.RetentionPolicy{
		MaxRunRetention: durationpb.New(max),
	}, artifacts.PutConditions{IfAbsent: true})
	switch {
	case err == nil:
		policy = written
	case errors.Is(err, artifacts.ErrPreconditionFailed):
		// Another replica won the race to materialize the document; read
		// back what it wrote rather than trusting our own observation.
		policy, err = c.policies.Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("archive: read retention policy after losing the materialization race: %w", err)
		}
	default:
		return nil, fmt.Errorf("archive: materialize retention policy: %w", err)
	}
	c.logger.Info("retention policy materialized",
		"temporal_namespace", c.temporalNamespace, "max_run_retention", policy.GetMaxRunRetention().AsDuration())
	if err := c.Reconcile(ctx, policy.GetMaxRunRetention().AsDuration()); err != nil {
		return nil, err
	}
	return policy, nil
}

// firstBootAttempts bounds how many times a first boot derives its maximum
// again because the cluster's retention changed while it did.
const firstBootAttempts = 3

// errRetentionMoved is the cluster's retention changing around the probe
// that asks whether this engine may set it.
var errRetentionMoved = errors.New("the cluster's retention changed while it was read")

// firstMaximum is the maximum a first boot materializes from the retention
// the cluster keeps, observed: observed itself where this engine may change
// the namespace's retention, so the backstop then rises a margin above it.
// Whether it may is asked by setting the retention to the value it already
// has, which changes nothing. Where the cluster refuses, the backstop must be
// what the cluster already keeps, so the maximum is observed less the margin,
// logged as derived from the cluster; a cluster that keeps runs too briefly
// for that, or for an idempotency key's life, is a *BackstopRefusedError
// naming the retention to set. An observed retention of zero (none reported)
// takes DefaultMaxRunRetention and asks nothing: Reconcile then decides.
func (c *RetentionController) firstMaximum(ctx context.Context, observed time.Duration) (time.Duration, error) {
	if observed <= 0 {
		return DefaultMaxRunRetention, nil
	}
	// A policy another replica wrote since this boot found none is adopted
	// rather than probed past: its backstop may already be above observed,
	// which the probe would lower. The window between this read and the
	// probe remains; there the probe can set the retention back to observed,
	// below that replica's backstop, until this engine's Reconcile, which
	// follows at once, or the next cleanup pass raises it again.
	switch _, gerr := c.policies.Get(ctx); {
	case gerr == nil:
		return 0, fmt.Errorf("%w: another engine stored the retention policy meanwhile", errRetentionMoved)
	case !errors.Is(gerr, artifacts.ErrNotFound):
		return 0, fmt.Errorf("archive: read retention policy: %w", gerr)
	}
	err := retryOnNamespaceNotFound(ctx, func() error {
		_, err := c.admin.SetNamespaceRetention(ctx, c.temporalNamespace, observed)
		return err
	})
	switch {
	case err == nil:
		after, rerr := c.admin.NamespaceRetention(ctx, c.temporalNamespace)
		if rerr != nil {
			return 0, fmt.Errorf("archive: read cluster retention for %q after confirming the right to set it: %w", c.temporalNamespace, rerr)
		}
		if after != observed {
			return 0, fmt.Errorf("%w: read %s, then %s", errRetentionMoved, observed, after)
		}
		return observed, nil
	case !updateRefused(err):
		return 0, fmt.Errorf("archive: confirm the right to set cluster retention for %q: %w", c.temporalNamespace, err)
	}
	derived := observed - c.margin
	if derived <= 0 {
		return 0, c.refused(ctx, observed, c.Enforced(DefaultMaxRunRetention), err)
	}
	if want := c.Enforced(derived); want > observed {
		return 0, c.refused(ctx, observed, want, err)
	}
	if c.observer != nil {
		c.observer.ObserveBackstopRefused(ctx, true)
	}
	c.logger.Info("retention maximum derived from the cluster: this engine may not change the namespace's retention",
		"temporal_namespace", c.temporalNamespace, "cluster_retention", observed, "margin", c.margin, "max_run_retention", derived,
		"error", err)
	return derived, nil
}

// SetMax is the admin write: it changes the deployment maximum and keeps the
// backstop invariant intact throughout, ordering the two writes so the
// backstop is never below the ceiling even if the process is interrupted
// between them:
//
//   - raising the maximum reconciles the (now higher) backstop FIRST, then
//     stores the new ceiling — so the backstop is always >= any ceiling a
//     reader could observe;
//   - lowering the maximum stores the (now lower) ceiling FIRST, then
//     reconciles the backstop down — so the backstop never drops below the
//     OLD ceiling before every namespace bound by it has had a chance to be
//     validated against the new, lower one.
//
// A Reconcile failure after a successful Put is returned to the caller AND
// left for the next Ensure or cleanup iteration to repair: the stored policy
// is authoritative regardless of whether the backstop has caught up yet.
func (c *RetentionController) SetMax(ctx context.Context, max time.Duration) (*orkstackv1.RetentionPolicy, error) {
	if max <= 0 {
		return nil, fmt.Errorf("archive: maximum run retention must be positive, got %s", max)
	}
	current, err := c.policies.Get(ctx)
	raising := errors.Is(err, artifacts.ErrNotFound) || (err == nil && max > current.GetMaxRunRetention().AsDuration())
	if err != nil && !errors.Is(err, artifacts.ErrNotFound) {
		return nil, fmt.Errorf("archive: read current retention policy: %w", err)
	}
	policy := &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(max)}
	if raising {
		if err := c.Reconcile(ctx, max); err != nil {
			return nil, err
		}
	}
	written, err := c.policies.Put(ctx, policy, artifacts.PutConditions{})
	if err != nil {
		return nil, fmt.Errorf("archive: write retention policy: %w", err)
	}
	if !raising {
		if err := c.Reconcile(ctx, max); err != nil {
			return written, err
		}
	}
	return written, nil
}

// EffectiveRunRetention is the retention a run of ns is actually kept for:
// ns.run_retention when set and positive, else max — and never more than
// max, so a stored value a later, lower maximum left above it is bounded at
// enforcement rather than trusted. A nil ns (a namespace whose registry
// document is gone while archived runs of it remain) resolves to max.
func EffectiveRunRetention(ns *orkstackv1.Namespace, max time.Duration) time.Duration {
	d := ns.GetRunRetention()
	if d == nil {
		return max
	}
	if err := d.CheckValid(); err != nil {
		return max
	}
	retention := d.AsDuration()
	if retention <= 0 || retention > max {
		return max
	}
	return retention
}
