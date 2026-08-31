// Package archive reclaims storage that is scoped to one run after Temporal
// retention has removed that run's workflow execution.
//
// Cleanup deliberately removes only metadata/logs/{run-id}/ and
// metadata/commits/{namespace}/{run-id}/. Workload artifacts under objects/ are
// content-addressed by logical name and digest, with no run id in the key. The
// same object can therefore be referenced by another live or retained run;
// deleting it by a run prefix could destroy that run's committed result.
// Reclaiming those shared objects requires reference counts the archive does
// not have today. Archive bodies and indexes are never deleted.
package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// CleanupConfig controls the background cleanup worker. Enabled defaults false
// because cleanup deletes durable data and must be explicitly opted into.
type CleanupConfig struct {
	Enabled         bool
	Interval        time.Duration
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
	entries, err := c.archive.ListRuns(ctx, artifacts.RunQuery{})
	if err != nil {
		return stats, fmt.Errorf("archive cleanup: list runs: %w", err)
	}
	now := c.now().UTC()
	retentions := make(map[string]time.Duration)
	for _, entry := range entries {
		if _, known := retentions[entry.Namespace]; known {
			continue
		}
		retention, err := c.temporal.NamespaceRetention(ctx, entry.Namespace)
		if err != nil {
			stats.Errors = 1
			c.logger.Error("artifact cleanup retention lookup failed", "namespace", entry.Namespace, "error", err)
			return stats, fmt.Errorf("archive cleanup: resolve namespace %q retention: %w", entry.Namespace, err)
		}
		retentions[entry.Namespace] = retention
	}
	var errs []error
	processed := 0
	for _, entry := range entries {
		if processed >= c.config.BatchSize {
			break
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
		retention := retentions[entry.Namespace]
		if !entry.Time.Before(now.Add(-(retention + c.config.RetentionBuffer))) {
			continue
		}
		processed++
		exists, err := c.temporal.WorkflowExists(ctx, entry.Namespace, entry.RunID)
		if err != nil {
			wrapped := fmt.Errorf("run %q: check Temporal workflow: %w", entry.RunID, err)
			errs = append(errs, wrapped)
			stats.Errors++
			c.logger.Error("artifact cleanup workflow check failed", "run_id", entry.RunID, "error", err)
			continue
		}
		if exists {
			continue
		}
		result, err := c.archive.DeleteRunArtifacts(ctx, entry.Namespace, entry.RunID)
		stats.ObjectsDeleted += result.ObjectsDeleted
		stats.BytesReclaimed += result.BytesReclaimed
		if err != nil {
			wrapped := fmt.Errorf("run %q: delete artifacts: %w", entry.RunID, err)
			errs = append(errs, wrapped)
			stats.Errors++
			c.logger.Error("artifact cleanup delete failed", "run_id", entry.RunID, "error", err)
			continue
		}
		if err := c.archive.MarkArtifactsCleaned(ctx, entry.RunID, now); err != nil {
			wrapped := fmt.Errorf("run %q: mark artifacts cleaned: %w", entry.RunID, err)
			errs = append(errs, wrapped)
			stats.Errors++
			c.logger.Error("artifact cleanup mark failed", "run_id", entry.RunID, "error", err)
			continue
		}
		stats.RunsCleaned++
		c.logger.Info("run artifacts cleaned", "run_id", entry.RunID,
			"objects_deleted", result.ObjectsDeleted, "bytes_reclaimed", result.BytesReclaimed)
	}
	return stats, errors.Join(errs...)
}
