package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/identity"
)

// IdentityMetrics is ork.identity.keys_copy_stale, the one implementation of
// identity.CopyObserver (docs/TASK_IDENTITY.md, "The copy on disk"): each
// refresh, about once a minute, after which this engine's copy of the
// published signing keys was older than identity.keys_copy_max_age, so it
// signed nothing and verified nothing. Recording can fail nothing.
type IdentityMetrics struct {
	stale         metric.Int64Counter
	mints         metric.Int64Counter
	commits       metric.Int64Counter
	engineRefused metric.Int64Counter
}

var (
	_ identity.CopyObserver   = (*IdentityMetrics)(nil)
	_ identity.MintObserver   = (*IdentityMetrics)(nil)
	_ identity.CommitObserver = (*IdentityMetrics)(nil)
)

// mintReasons is the closed set of ork.identity.mints' reason label: a mint
// granted, or why it issued nothing. Anything else is counted as "other".
var mintReasons = map[string]bool{
	identity.OutcomeGranted: true, identity.OutcomePermissionDenied: true, identity.OutcomeInvalid: true,
	identity.OutcomeResourceExhausted: true, identity.ReasonUnavailable: true, identity.ReasonEngineRefused: true,
	identity.ReasonNotAdmitted: true, identity.ReasonAttemptNotHeld: true, identity.ReasonTaskNotAsAdmitted: true,
	identity.ReasonAttemptsExhausted: true, identity.ReasonAttemptExpired: true, identity.ReasonRevoked: true,
	identity.ReasonRequiredResultNotRecorded: true,
}

// commitReasons is the closed set of ork.identity.commits' reason label:
// the mint's, whose checks a commit shares, and the commit's own.
var commitReasons = func() map[string]bool {
	out := map[string]bool{identity.ReasonCommitIdentityMismatch: true}
	for r := range mintReasons {
		out[r] = true
	}
	return out
}()

// NewIdentityMetrics builds the counter from p's meter provider. It works,
// as a no-op, on nil or disabled Providers.
func NewIdentityMetrics(p *Providers) (*IdentityMetrics, error) {
	stale, err := meterFor(p).Int64Counter("ork.identity.keys_copy_stale",
		metric.WithDescription("Refreshes after which this engine's copy of the published signing keys was "+
			"older than identity.keys_copy_max_age: it signed nothing and verified nothing."),
		metric.WithUnit("{refresh}"))
	if err != nil {
		return nil, err
	}
	mints, err := meterFor(p).Int64Counter("ork.identity.mints",
		metric.WithDescription("Identity token mints this engine answered, by outcome: granted, or the reason it issued nothing."),
		metric.WithUnit("{mint}"))
	if err != nil {
		return nil, err
	}
	commits, err := meterFor(p).Int64Counter("ork.identity.commits",
		metric.WithDescription("Task results this engine was asked to record (CommitResult), by outcome: granted, or the reason it recorded nothing."),
		metric.WithUnit("{commit}"))
	if err != nil {
		return nil, err
	}
	refused, err := meterFor(p).Int64Counter("ork.identity.engine_refused",
		metric.WithDescription("Mints and commits this engine refused IDENTITY_ENGINE_REFUSED: its own mode, issuer, key or wiring is wrong, never the task."),
		metric.WithUnit("{call}"))
	if err != nil {
		return nil, err
	}
	return &IdentityMetrics{stale: stale, mints: mints, commits: commits, engineRefused: refused}, nil
}

// Minted counts one mint's outcome under a reason of the closed set.
func (m *IdentityMetrics) Minted(reason string) {
	label := reason
	if !mintReasons[label] {
		label = "other"
	}
	m.mints.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", label)))
	if reason == identity.ReasonEngineRefused {
		m.engineRefused.Add(context.Background(), 1)
	}
}

// Committed counts one CommitResult's outcome under a reason of the closed
// set.
func (m *IdentityMetrics) Committed(reason string) {
	label := reason
	if !commitReasons[label] {
		label = "other"
	}
	m.commits.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", label)))
	if reason == identity.ReasonEngineRefused {
		m.engineRefused.Add(context.Background(), 1)
	}
}

// KeysCopyStale counts one refresh with a stale copy.
func (m *IdentityMetrics) KeysCopyStale() {
	m.stale.Add(context.Background(), 1)
}
