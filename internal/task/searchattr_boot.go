package task

import (
	"context"
	"errors"
	"log/slog"

	"go.temporal.io/sdk/client"
)

// IndexAnswers is what the cluster indexes of a run, one answer for each index
// a deployment uses: a cluster that refuses one keeps the others.
type IndexAnswers struct {
	// Namespace is a run's OrkStack namespace, Origin what started it, Root
	// its composed chain, Paused whether it is paused, Concurrency a
	// limit's four attributes, and ResultsRecorded whether its results are
	// recorded by the engine.
	Namespace, Origin, Root, Paused, Concurrency, ResultsRecorded bool
}

// Limits reports that a concurrency limit can be held: it is kept by its own
// attributes and its namespace's.
func (a IndexAnswers) Limits() bool { return a.Namespace && a.Concurrency }

// EnsureSearchAttributes registers every index a deployment uses, as each
// engine's boot does, and warns of each the cluster refuses. Its error is
// ErrNamespaceNotVisible alone.
func EnsureSearchAttributes(ctx context.Context, temporalClient client.Client, namespace string, logger *slog.Logger) (IndexAnswers, error) {
	var (
		answers IndexAnswers
		err     error
	)
	// The run index, provisioned beside the namespace: it is what makes "list
	// this namespace's runs" a question the cluster answers, instead of a
	// filter over whatever page walk another tenant's traffic left room for.
	// A cluster that will not register it is not a reason to refuse to boot —
	// everything else works, and the reader falls back to reading the whole
	// index — but it IS worth a line, because that fallback is slow and its
	// limit is an error a client would otherwise have to explain.
	answers.Namespace, err = EnsureRunSearchAttributes(ctx, temporalClient, namespace)
	if errors.Is(err, ErrNamespaceNotVisible) {
		return answers, err
	}
	if err != nil {
		logger.Warn("visibility does not index a run's orkflow namespace",
			"search_attribute", SearchAttrOrkflowNamespace,
			"consequence", "listing a namespace's live runs scans the whole index",
			"error", err)
	}
	// The origin index is a separate answer on purpose: a cluster that
	// accepted OrkflowNamespace in an earlier release and refuses the two
	// origin attributes now keeps its namespace predicate, and only an
	// origin-filtered list falls back to the bounded walk.
	answers.Origin, err = EnsureRunOriginSearchAttributes(ctx, temporalClient, namespace)
	if errors.Is(err, ErrNamespaceNotVisible) {
		return answers, err
	}
	if err != nil {
		logger.Warn("visibility does not index a run's origin",
			"search_attribute", SearchAttrOrkflowOriginKind+","+SearchAttrOrkflowOriginId,
			"consequence", "filtering runs by origin walks the whole index",
			"error", err)
	}
	// The root index is a third answer, for the same reason: a cluster at its
	// limit of custom attributes keeps the two it already has.
	answers.Root, err = EnsureRunRootSearchAttribute(ctx, temporalClient, namespace)
	if errors.Is(err, ErrNamespaceNotVisible) {
		return answers, err
	}
	if err != nil {
		logger.Warn("visibility does not index a run's composed chain",
			"search_attribute", SearchAttrOrkflowOriginRootRunId,
			"consequence", "filtering runs by root run walks the whole index",
			"error", err)
	}
	// The paused attribute is a fourth answer, for the same reason, and the
	// one a run itself writes: submit pins it Indexed only when this boot
	// found it usable, so the cluster never refuses a run's write. Every
	// boot registers it again, which is also how an operator restores it if
	// it was removed while runs were open (docs/LIMITATIONS.md).
	answers.Paused, err = EnsureRunPausedSearchAttribute(ctx, temporalClient, namespace)
	if errors.Is(err, ErrNamespaceNotVisible) {
		return answers, err
	}
	if err != nil {
		logger.Warn("visibility does not index a paused run",
			"search_attribute", SearchAttrOrkflowRunPaused,
			"consequence", "a paused run is listed as running",
			"error", err)
	}
	// Whether a run's results are recorded by the engine is a sixth answer,
	// on the same terms (decision RR-1): unusable, the engine warns, naming
	// it, and starts; its runs carry no mark, and a list by results_recorded
	// is refused by name.
	answers.ResultsRecorded, err = EnsureRunResultsRecordedSearchAttribute(ctx, temporalClient, namespace)
	if errors.Is(err, ErrNamespaceNotVisible) {
		return answers, err
	}
	if err != nil {
		logger.Warn("visibility does not index runs whose results the engine records",
			"search_attribute", SearchAttrOrkflowResultsRecorded,
			"consequence", "orkctl run records --not-applied and the results_recorded run filter are refused; add the attribute by hand (docs/DEPLOY.md)",
			"error", err)
	}
	// The concurrency limit's attributes are a fifth answer, all four as one:
	// a limit cannot hold, queue or rebuild with any of them missing. Unusable
	// is not fatal: the engine boots, and submit refuses concurrency limits
	// with cause=index once it reads this answer (E88 §9.7, P3).
	answers.Concurrency, err = EnsureConcurrencySearchAttributes(ctx, temporalClient, namespace)
	if errors.Is(err, ErrNamespaceNotVisible) {
		return answers, err
	}
	if err != nil {
		logger.Warn("visibility does not index concurrency limits",
			"consequence", "concurrency limits cannot be held on this cluster",
			"error", err)
	}
	return answers, nil
}
