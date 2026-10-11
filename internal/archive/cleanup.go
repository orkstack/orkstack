// Package archive retires a finished run at its own OrkStack namespace's run
// retention (or the deployment's maximum, for a namespace that sets none),
// deleting the Temporal execution explicitly rather than waiting for it to
// disappear on its own, then reclaims that run's OrkStack-owned storage.
//
// Cleanup deliberately removes only metadata/logs/{run-id}/,
// metadata/attempts/{run-id}/, metadata/commits/{namespace}/{run-id}/ and
// metadata/snapshots/{namespace}/{run-id}/. The snapshot prefix matters as
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
	"math"
	"os"
	"strconv"
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
}

// CleanupConfigFromEnv reads cleanup's ORK_* configuration. Invalid values use
// their safe defaults, matching the engine's existing duration configuration.
func CleanupConfigFromEnv() CleanupConfig {
	return CleanupConfig{
		Enabled:         envBool("ORK_CLEANUP_ENABLED", false),
		Interval:        envDuration("ORK_CLEANUP_INTERVAL", DefaultCleanupInterval),
		RetentionBuffer: envDuration("ORK_CLEANUP_RETENTION_BUFFER", DefaultRetentionBuffer),
		BatchSize:       envPositiveInt("ORK_CLEANUP_BATCH_SIZE", DefaultBatchSize),
	}
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

// CleanupStats describes one complete scan.
type CleanupStats struct {
	RunsScanned    int
	RunsCleaned    int
	ObjectsDeleted int
	BytesReclaimed int64
	Errors         int
}

// Cleaner periodically retires finished runs at their own namespace's run
// retention and reclaims their run-scoped storage.
type Cleaner struct {
	config     CleanupConfig
	archive    artifacts.RunArchive
	cluster    RunRetirer
	namespaces NamespaceLister
	retention  *RetentionController
	logger     *slog.Logger
	now        func() time.Time
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
	if logger == nil {
		logger = slog.Default()
	}
	if config.RetentionBuffer < 2*config.Interval {
		// The margin's only job is to absorb worker downtime and backlog
		// between passes; one shorter than two passes cannot do that.
		logger.Warn("cleanup retention buffer is shorter than two cleanup intervals; the backstop may fire before this worker catches up",
			"retention_buffer", config.RetentionBuffer, "interval", config.Interval)
	}
	return &Cleaner{
		config: config, archive: archive, cluster: cluster,
		namespaces: namespaces, retention: retention, logger: logger, now: time.Now,
	}
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
func (c *Cleaner) Run(ctx context.Context) {
	resume := c.runAndLog(ctx, "")
	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()
	for {
		if resume != "" {
			if ctx.Err() != nil {
				return
			}
			resume = c.runAndLog(ctx, resume)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resume = c.runAndLog(ctx, "")
		}
	}
}

// runAndLog runs one pass after startAfter and logs it. It returns where a
// drain continues: the index key after which the next pass starts, or "" when
// this pass reached the end of the index.
func (c *Cleaner) runAndLog(ctx context.Context, startAfter string) string {
	c.logger.Info("artifact cleanup iteration started", "drain", startAfter != "")
	stats, resume, err := c.pass(ctx, startAfter)
	args := []any{
		"runs_scanned", stats.RunsScanned,
		"runs_cleaned", stats.RunsCleaned,
		"objects_deleted", stats.ObjectsDeleted,
		"bytes_reclaimed", stats.BytesReclaimed,
		"errors", stats.Errors,
	}
	if err != nil {
		args = append(args, "error", err)
		c.logger.Error("artifact cleanup iteration completed", args...)
		return resume
	}
	c.logger.Info("artifact cleanup iteration completed", args...)
	return resume
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
func (c *Cleaner) pass(ctx context.Context, startAfter string) (stats CleanupStats, resume string, err error) {
	if c.archive == nil || c.cluster == nil || c.namespaces == nil || c.retention == nil {
		return stats, "", fmt.Errorf("archive cleanup: worker is not fully configured")
	}
	if c.config.TemporalNamespace == "" {
		return stats, "", fmt.Errorf("archive cleanup: Temporal namespace is empty")
	}
	policy, err := c.retention.Policy(ctx)
	if err != nil {
		stats.Errors = 1
		c.logger.Error("artifact cleanup retention policy read failed", "error", err)
		return stats, "", fmt.Errorf("archive cleanup: read retention policy: %w", err)
	}
	max := policy.GetMaxRunRetention().AsDuration()
	docs, err := c.namespaces.List(ctx)
	if err != nil {
		stats.Errors = 1
		c.logger.Error("artifact cleanup namespace list failed", "error", err)
		return stats, "", fmt.Errorf("archive cleanup: list namespaces: %w", err)
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
		stats.Errors++
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
	processed := 0
	for processed < c.config.BatchSize {
		entries, err := c.archive.ListRuns(ctx, artifacts.RunQuery{
			Until: prefilter, Limit: pageLimit, KeyOrder: true, StartAfterKey: cursor,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("archive cleanup: list runs: %w", err))
			return stats, "", errors.Join(errs...)
		}
		if len(entries) == 0 {
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
				return stats, "", errors.Join(errs...)
			}
			stats.RunsScanned++
			state, stateErr := c.archive.CleanupState(ctx, entry.RunID)
			if stateErr == nil && state.ArtifactsCleaned {
				continue
			}
			if stateErr != nil && !errors.Is(stateErr, artifacts.ErrNotFound) {
				wrapped := fmt.Errorf("run %q: read cleanup state: %w", entry.RunID, stateErr)
				errs = append(errs, wrapped)
				stats.Errors++
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
			cutoff := now.Add(-retentionFor(entry.Namespace))
			found, closedAt, err := c.cluster.LookupWorkflow(ctx, c.config.TemporalNamespace, entry.RunID)
			switch {
			case err != nil:
				wrapped := fmt.Errorf("run %q: look up execution: %w", entry.RunID, err)
				errs = append(errs, wrapped)
				stats.Errors++
				c.logger.Error("artifact cleanup workflow lookup failed", "run_id", entry.RunID, "error", err)
			case found && closedAt.IsZero():
				// Impossible by construction — the archive is written by the
				// terminal PublishEvent — and the one case where deletion
				// would TERMINATE a live execution, so it is refused rather
				// than risked.
				wrapped := fmt.Errorf("run %q: archived run still executing; refusing to retire it", entry.RunID)
				errs = append(errs, wrapped)
				stats.Errors++
				c.logger.Error("artifact cleanup found a still-running archived run", "run_id", entry.RunID)
			case found && !closedAt.Before(cutoff):
				// Finished, but not yet past ITS namespace's own retention.
			default:
				if found {
					if err := c.cluster.DeleteWorkflow(ctx, c.config.TemporalNamespace, entry.RunID); err != nil {
						wrapped := fmt.Errorf("run %q: retire execution: %w", entry.RunID, err)
						errs = append(errs, wrapped)
						stats.Errors++
						c.logger.Error("artifact cleanup retire failed", "run_id", entry.RunID, "error", err)
						break
					}
				}
				result, err := c.archive.DeleteRunArtifacts(ctx, entry.Namespace, entry.RunID)
				stats.ObjectsDeleted += result.ObjectsDeleted
				stats.BytesReclaimed += result.BytesReclaimed
				if err != nil {
					wrapped := fmt.Errorf("run %q: delete artifacts: %w", entry.RunID, err)
					errs = append(errs, wrapped)
					stats.Errors++
					c.logger.Error("artifact cleanup delete failed", "run_id", entry.RunID, "error", err)
				} else if err := c.archive.MarkArtifactsCleaned(ctx, entry.RunID, now); err != nil {
					wrapped := fmt.Errorf("run %q: mark artifacts cleaned: %w", entry.RunID, err)
					errs = append(errs, wrapped)
					stats.Errors++
					c.logger.Error("artifact cleanup mark failed", "run_id", entry.RunID, "error", err)
				} else {
					stats.RunsCleaned++
					c.logger.Info("run retired", "run_id", entry.RunID, "namespace", entry.Namespace,
						"objects_deleted", result.ObjectsDeleted, "bytes_reclaimed", result.BytesReclaimed)
				}
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
			break
		}
		if cursorErr != nil {
			errs = append(errs, fmt.Errorf("archive cleanup: resume cursor: %w", cursorErr))
			stats.Errors++
			break
		}
		// A short page means the walk reached the end of the index, so the
		// cursor only has to advance while full pages keep coming back.
		cursor = next
	}
	return stats, resume, errors.Join(errs...)
}
