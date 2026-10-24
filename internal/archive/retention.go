package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/api/serviceerror"
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
	logger *slog.Logger
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

// Enforced is the backstop TTL for a given maximum: max + margin.
func (c *RetentionController) Enforced(max time.Duration) time.Duration {
	return max + c.margin
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
	var observed time.Duration
	if err := retryOnNamespaceNotFound(ctx, func() error {
		var err error
		observed, err = c.admin.NamespaceRetention(ctx, c.temporalNamespace)
		return err
	}); err != nil {
		return nil, fmt.Errorf("archive: observe cluster retention for %q: %w", c.temporalNamespace, err)
	}
	if observed <= 0 {
		observed = DefaultMaxRunRetention
	}
	written, err := c.policies.Put(ctx, &orkstackv1.RetentionPolicy{
		MaxRunRetention: durationpb.New(observed),
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
