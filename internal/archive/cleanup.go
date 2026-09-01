// Package archive reclaims storage that is scoped to one run after Temporal
// retention has removed that run's workflow execution.
//
// Cleanup deliberately removes only metadata/logs/{run-id}/ and
// metadata/commits/{namespace}/{run-id}/. Workload artifacts under objects/ are
// content-addressed by logical name and digest, with no run id in the key. The
// same object can therefore be referenced by another live or retained run;
// deleting it by a run prefix could destroy that run's committed result.
// The archive does now keep those reference counts, so a shared object can be
// reclaimed once no archived run claims it — that is what
// artifacts.DeleteUnreferencedObjects does. What remains deferred is this
// worker calling it: reference accounting only exists for runs archived by a
// build that has it, so an object referenced solely by an older run would look
// unreferenced. Archive bodies and indexes are never deleted.
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
// and must be explicitly opted into.
type CleanupConfig struct {
	Enabled           bool
	TemporalNamespace string
	Interval          time.Duration
	RetentionBuffer   time.Duration
	BatchSize         int
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

// WorkflowReader is the fail-closed Temporal boundary cleanup needs.
type WorkflowReader interface {
	WorkflowExists(ctx context.Context, namespace, workflowID string) (bool, error)
	NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error)
}

// CleanupStats describes one complete scan.
type CleanupStats struct {
	RunsScanned    int
	RunsCleaned    int
	ObjectsDeleted int
	BytesReclaimed int64
	Errors         int
}

// Cleaner periodically reclaims expired run-scoped storage.
type Cleaner struct {
	config   CleanupConfig
	archive  artifacts.RunArchive
	temporal WorkflowReader
	logger   *slog.Logger
	now      func() time.Time
}

// NewCleaner constructs a cleanup worker.
func NewCleaner(config CleanupConfig, archive artifacts.RunArchive, temporal WorkflowReader, logger *slog.Logger) *Cleaner {
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
	return &Cleaner{config: config, archive: archive, temporal: temporal, logger: logger, now: time.Now}
}

// WithClock replaces the wall clock used to decide whether an archived run is
// beyond Temporal retention plus the configured buffer. It keeps integration
// tests deterministic without waiting for a production-length retention
// window.
func (c *Cleaner) WithClock(now func() time.Time) *Cleaner {
	if now != nil {
		c.now = now
	}
	return c
}

// Run scans immediately, then periodically, until ctx is canceled.
func (c *Cleaner) Run(ctx context.Context) {
	c.runAndLog(ctx)
	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runAndLog(ctx)
		}
	}
}

func (c *Cleaner) runAndLog(ctx context.Context) {
	c.logger.Info("artifact cleanup iteration started")
	stats, err := c.RunOnce(ctx)
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
		return
	}
	c.logger.Info("artifact cleanup iteration completed", args...)
}

// RunOnce performs one fail-closed scan. Per-run failures are joined and do not
// prevent later eligible runs from being processed.
func (c *Cleaner) RunOnce(ctx context.Context) (CleanupStats, error) {
	var stats CleanupStats
	if c.archive == nil || c.temporal == nil {
		return stats, fmt.Errorf("archive cleanup: worker is not fully configured")
	}
	if c.config.TemporalNamespace == "" {
		return stats, fmt.Errorf("archive cleanup: Temporal namespace is empty")
	}
	// Retention is resolved before the first page so every query can exclude
	// recent runs rather than truncating the newest, ineligible end of the
	// archive.
	// One Temporal namespace partitions the whole deployment. Resolve its
	// retention once per iteration; archive entry namespaces are OrkStack
	// coordinates and must never be used for Temporal calls. A transient
	// lookup failure fails this iteration closed and the next tick retries.
	retention, err := c.temporal.NamespaceRetention(ctx, c.config.TemporalNamespace)
	if err != nil {
		stats.Errors = 1
		c.logger.Error("artifact cleanup retention lookup failed", "namespace", c.config.TemporalNamespace, "error", err)
		return stats, fmt.Errorf("archive cleanup: resolve Temporal namespace %q retention: %w", c.config.TemporalNamespace, err)
	}
	now := c.now().UTC()
	cutoff := now.Add(-(retention + c.config.RetentionBuffer))
	pageLimit := doubleSaturating(c.config.BatchSize)
	cursor := ""
	// Pages are walked in index-key order, so the backend stops at the page
	// boundary and one whole iteration enumerates each index key once no matter
	// how many pages it takes. Ordering by key rather than by time is what makes
	// that possible: the stamp is the fourth key segment, so a newest-first page
	// cannot be produced without reading every key first.
	var errs []error
	processed := 0
	for processed < c.config.BatchSize {
		entries, err := c.archive.ListRuns(ctx, artifacts.RunQuery{
			Until: cutoff, Limit: pageLimit, KeyOrder: true, StartAfterKey: cursor,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("archive cleanup: list runs: %w", err))
			return stats, errors.Join(errs...)
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
				return stats, errors.Join(errs...)
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
			// The query's Until already excludes anything inside the retention
			// window, but deletion is irreversible and RunArchive is an
			// exported interface, so eligibility is re-checked here against the
			// entry the backend actually returned rather than trusted from the
			// filter that was requested.
			if !entry.Time.Before(cutoff) {
				continue
			}
			processed++
			exists, err := c.temporal.WorkflowExists(ctx, c.config.TemporalNamespace, entry.RunID)
			if err != nil {
				wrapped := fmt.Errorf("run %q: check Temporal workflow: %w", entry.RunID, err)
				errs = append(errs, wrapped)
				stats.Errors++
				c.logger.Error("artifact cleanup workflow check failed", "run_id", entry.RunID, "error", err)
				if processed >= c.config.BatchSize {
					break
				}
				continue
			}
			if !exists {
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
					c.logger.Info("run artifacts cleaned", "run_id", entry.RunID,
						"objects_deleted", result.ObjectsDeleted, "bytes_reclaimed", result.BytesReclaimed)
				}
			}
			if processed >= c.config.BatchSize {
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
	return stats, errors.Join(errs...)
}
