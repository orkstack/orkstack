package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Sweeper archives every finished run that did not archive itself: a run's own
// terminal publish writes its archive, so a run terminated from outside runs
// nothing that could, and a run whose ending object storage refused has its
// write refused too. Cleaner cannot reach such a run, because it walks the
// archive's index, and the cluster deletes it at its own retention without
// asking anyone. Temporal's record of it is complete, so the sweep builds the
// archive from that, and from then on it is an ordinary archived run: listed,
// retired at its namespace's run retention, readable from any replica.
//
// It asks the cluster which runs have finished and the archive which of them
// has a body, on every pass. It stores nothing, so any replica's pass is
// complete on its own, and two replicas converge as any two writers of one
// archive do. It only writes, so it runs whether or not cleanup is enabled:
// the cluster's retention fires either way.
//
// It starts a cleanup interval after a run closes rather than at the run's
// retention, so that an outage has the run's whole time in the cluster to end
// in. The cluster fixes a run's deletion when it closes, and nothing a client
// does can postpone that afterwards (Temporal server 1.32.0, read in its
// source: GenerateDeleteHistoryEventTask, executeDeleteHistoryEventTask). A run
// with no archive whose time runs out while object storage is down is lost,
// with nothing left to say it existed; a pass says which runs are at risk and
// when the first of them expires, for as long as they are, and that is the
// most it can do.
type Sweeper struct {
	config   CleanupConfig
	runs     FinishedRunLister
	bodies   RunBodyChecker
	history  HistoryArchiver
	cluster  ClusterRetention
	observer SweepObserver
	logger   *slog.Logger
	now      func() time.Time
	// questionBudget and runBudget are sweepQuestionBudget and sweepRunBudget;
	// a test shortens them.
	questionBudget time.Duration
	runBudget      time.Duration
}

// FinishedRunLister is the cluster's index of finished runs
// (engine.TemporalRunReader), restated here so this package never imports
// internal/engine.
type FinishedRunLister interface {
	EachFinishedRun(ctx context.Context, closedBefore time.Time, pageSize int,
		visit func(runID string, closedAt time.Time) error) error
}

// RunBodyChecker reports whether a run has an archive body. A body is the last
// thing an archive write leaves, so it means the archive is whole.
type RunBodyChecker interface {
	HasRun(ctx context.Context, runID string) (bool, error)
}

// HistoryArchiver writes a finished run's archive from its history
// (engine.HistoryArchiver). A run the cluster no longer holds is an error
// matching artifacts.ErrNotFound.
type HistoryArchiver interface {
	ArchiveFromHistory(ctx context.Context, runID string) error
}

// ClusterRetention reads the cluster's own retention, which dates the risk.
type ClusterRetention interface {
	NamespaceRetention(ctx context.Context, namespace string) (time.Duration, error)
}

// SweepObserver sees each pass's result. It must not block.
type SweepObserver interface {
	ObserveSweep(ctx context.Context, result SweepResult)
}

// SweepResult is one pass.
type SweepResult struct {
	// Examined is how many finished runs the cluster listed.
	Examined int
	// Archived is how many this pass archived.
	Archived int
	// Failed is how many it tried to archive and could not.
	Failed int
	// Expired is how many the cluster no longer held when this pass came to
	// archive them: they were lost.
	Expired int
	// Unconfirmed is how many it could not confirm have an archive: Failed,
	// and every run it did not reach once object storage stopped answering,
	// whether or not those have one.
	Unconfirmed int
	// EarliestExpiry is about when the first unconfirmed run expires, zero
	// when there is none or the cluster's retention could not be read. It is
	// the run's close plus the cluster's retention now, and the cluster fixed
	// the run's own at its close.
	EarliestExpiry time.Time
	// ListFailed says the cluster's index could not be read to its end, so the
	// numbers above are only what was reached.
	ListFailed bool
	// StoreFailed says object storage stopped answering during the pass, so
	// every run after that was counted unconfirmed without being asked about.
	StoreFailed bool
	// IntervalTooLong says two cleanup intervals reach the cluster's retention:
	// a run can close, wait out the interval a pass leaves it alone for and the
	// pass after that, and be gone before the sweep ever looks at it.
	IntervalTooLong bool
}

const (
	// sweepRunBudget bounds one run's archive: a read of its history and its
	// objects, and the write. A run that overruns it is tried on the next pass.
	sweepRunBudget = 5 * time.Minute
	// sweepQuestionBudget bounds one question to the store or the cluster that
	// the pass's report waits on, so a store that stalls rather than refuses
	// still ends the pass, and the pass says so.
	sweepQuestionBudget = 30 * time.Second
	// sweepRunsNamed is how many at-risk runs one pass's log line names, the
	// nearest to expiry first; the rest are counted.
	sweepRunsNamed = 10
)

// NewSweeper constructs the worker. config.Interval and config.BatchSize are
// the cleanup worker's settings: how often it passes, how recently closed a run
// it leaves alone, and how many runs it lists a page.
func NewSweeper(
	config CleanupConfig,
	runs FinishedRunLister,
	bodies RunBodyChecker,
	history HistoryArchiver,
	cluster ClusterRetention,
	observer SweepObserver,
	logger *slog.Logger,
) *Sweeper {
	if config.Interval <= 0 {
		config.Interval = DefaultCleanupInterval
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultBatchSize
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Sweeper{
		config: config, runs: runs, bodies: bodies, history: history, cluster: cluster,
		observer: observer, logger: logger, now: time.Now,
		questionBudget: sweepQuestionBudget, runBudget: sweepRunBudget,
	}
}

// WithClock replaces the wall clock, which decides how recently closed a run
// is. It keeps a test from waiting out an interval.
func (s *Sweeper) WithClock(now func() time.Time) *Sweeper {
	if now != nil {
		s.now = now
	}
	return s
}

// Run passes immediately, then every interval, until ctx is canceled.
func (s *Sweeper) Run(ctx context.Context) {
	s.runAndLog(ctx)
	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runAndLog(ctx)
		}
	}
}

func (s *Sweeper) runAndLog(ctx context.Context) {
	result, err := s.RunOnce(ctx)
	args := []any{"examined", result.Examined, "archived", result.Archived, "failed", result.Failed,
		"expired", result.Expired, "unconfirmed", result.Unconfirmed}
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.logger.Error("archive sweep completed", append(args, "error", err)...)
		return
	}
	s.logger.Info("archive sweep completed", args...)
}

// atRiskRun is one run a pass could not confirm has an archive.
type atRiskRun struct {
	id       string
	closedAt time.Time
}

// RunOnce makes one pass. A run that cannot be archived stops no other; once
// object storage stops answering whether a run has a body, the pass asks it
// nothing more and only counts what is left, so that outage costs one failed
// request a pass. A store that answers that and refuses the rest costs each run
// with no archive its own try, each within sweepRunBudget.
func (s *Sweeper) RunOnce(ctx context.Context) (SweepResult, error) {
	var (
		result   SweepResult
		firstErr error
		storeErr error
		atRisk   []atRiskRun
	)
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	risk := func(runID string, closedAt time.Time) {
		result.Unconfirmed++
		// Kept sorted and short: the walk may list every run the cluster
		// holds, and the log line names only the nearest to expiry.
		at := sort.Search(len(atRisk), func(i int) bool { return closedAt.Before(atRisk[i].closedAt) })
		if at < sweepRunsNamed {
			atRisk = append(atRisk, atRiskRun{})
			copy(atRisk[at+1:], atRisk[at:])
			atRisk[at] = atRiskRun{id: runID, closedAt: closedAt}
			if len(atRisk) > sweepRunsNamed {
				atRisk = atRisk[:sweepRunsNamed]
			}
		}
	}
	retention := s.clusterRetention(ctx)
	if retention > 0 && 2*s.config.Interval >= retention {
		result.IntervalTooLong = true
		s.logger.Error("the cleanup interval leaves the archive sweep no time: a run can be deleted before a pass looks at it",
			"interval", s.config.Interval, "cluster_retention", retention)
	}
	walkErr := s.runs.EachFinishedRun(ctx, s.now().Add(-s.config.Interval), s.config.BatchSize,
		func(runID string, closedAt time.Time) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			result.Examined++
			if storeErr != nil {
				risk(runID, closedAt)
				return nil
			}
			askCtx, cancel := context.WithTimeout(ctx, s.questionBudget)
			has, err := s.bodies.HasRun(askCtx, runID)
			cancel()
			if err != nil {
				storeErr = fmt.Errorf("archive sweep: object storage did not answer for run %q: %w", runID, err)
				result.StoreFailed = true
				note(storeErr)
				s.logger.Error("archive sweep: object storage did not answer; counting the remaining runs without asking it",
					"run_id", runID, "error", err)
				risk(runID, closedAt)
				return nil
			}
			if has {
				return nil
			}
			s.archive(ctx, runID, closedAt, &result, note, risk)
			return nil
		})
	if walkErr != nil {
		result.ListFailed = true
		note(fmt.Errorf("archive sweep: list finished runs: %w", walkErr))
	}
	s.reportRisk(&result, atRisk, retention)
	if s.observer != nil {
		s.observer.ObserveSweep(ctx, result)
	}
	return result, firstErr
}

// archive writes one run's archive and counts what became of it.
func (s *Sweeper) archive(
	ctx context.Context,
	runID string,
	closedAt time.Time,
	result *SweepResult,
	note func(error),
	risk func(string, time.Time),
) {
	runCtx, cancel := context.WithTimeout(ctx, s.runBudget)
	defer cancel()
	err := s.history.ArchiveFromHistory(runCtx, runID)
	switch {
	case err == nil:
		result.Archived++
		s.logger.Info("run archived from its history", "run_id", runID, "closed_at", closedAt)
	case errors.Is(err, artifacts.ErrNotFound):
		// It had no archive a moment ago and the cluster has let it go.
		result.Expired++
		note(fmt.Errorf("archive sweep: run %q was gone from the cluster before it could be archived: %w", runID, err))
		s.logger.Error("run lost: it was gone from the cluster before it could be archived",
			"run_id", runID, "closed_at", closedAt, "error", err)
	default:
		result.Failed++
		note(fmt.Errorf("archive sweep: run %q: %w", runID, err))
		s.logger.Error("archive sweep could not archive a run", "run_id", runID, "closed_at", closedAt, "error", err)
		risk(runID, closedAt)
	}
}

// clusterRetention reads the cluster's retention now, zero when it cannot. It
// only dates the risk and checks the interval against it, so a failure is
// logged and the pass goes on.
func (s *Sweeper) clusterRetention(ctx context.Context) time.Duration {
	askCtx, cancel := context.WithTimeout(ctx, s.questionBudget)
	defer cancel()
	retention, err := s.cluster.NamespaceRetention(askCtx, s.config.TemporalNamespace)
	if err != nil {
		s.logger.Warn("archive sweep could not read the cluster's retention, so it cannot date the risk or check its interval", "error", err)
		return 0
	}
	return retention
}

// reportRisk dates the pass's unconfirmed runs and says so: the error line an
// operator's alert reads, every pass for as long as it holds.
func (s *Sweeper) reportRisk(result *SweepResult, atRisk []atRiskRun, retention time.Duration) {
	if result.Unconfirmed == 0 {
		return
	}
	named := make([]string, 0, len(atRisk))
	for _, run := range atRisk {
		named = append(named, run.id)
	}
	args := []any{"unconfirmed", result.Unconfirmed, "nearest_to_expiry", named}
	if retention > 0 {
		result.EarliestExpiry = atRisk[0].closedAt.Add(retention)
		args = append(args, "earliest_expiry", result.EarliestExpiry)
	}
	s.logger.Error("finished runs are not confirmed archived: each is lost if it is still unarchived when the "+
		"cluster's retention expires it", args...)
}
