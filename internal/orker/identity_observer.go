package orker

import "time"

// How a refresh of an attempt's identity tokens ended, as IdentityObserver
// hears it: a closed set.
const (
	// IdentityRefreshed: every token was replaced.
	IdentityRefreshed = "refreshed"
	// IdentityRefreshRetried: nothing was replaced, and it is asked again.
	IdentityRefreshRetried = "retried"
	// IdentityRefreshEnded: the answer, or what came of it, ends the attempt.
	IdentityRefreshEnded = "ended"
	// IdentityRefreshStopped: the attempt's best_effort identities were
	// revoked, and nothing refreshes them any more.
	IdentityRefreshStopped = "stopped"
)

// How the engine refused this Orker itself, as IdentityObserver hears it: a
// closed set.
const (
	// EngineRefusedOrker: its credential, its role or the engine is wrong.
	EngineRefusedOrker = "refused"
	// EngineCouldNotRead: the engine could not read what it sent.
	EngineCouldNotRead = "invalid"
)

// How an attempt's wait for an engine to record its result ended, as
// IdentityObserver hears it: a closed set. An attempt that was stopped while it
// waited is not heard.
const (
	// ResultRecorded: an engine recorded it, heard at once or found there.
	ResultRecorded = "recorded"
	// ResultNotRecorded: the wait ran out, and no record was found.
	ResultNotRecorded = "not_recorded"
	// ResultRefused: an engine's answer ended the attempt.
	ResultRefused = "refused"
)

// IdentityObserver is told what an attempt of a run that holds an identity
// met, for the Orker's metrics (internal/telemetry, OrkerIdentityMetrics). It
// is told no name, run, namespace or token, and returns nothing, so telemetry
// can never change what an attempt does.
type IdentityObserver interface {
	// Refreshed is one refresh of an attempt's tokens, after its first ones.
	Refreshed(outcome string)
	// TokenMargin is how much of its life the token nearest its end had left
	// when a refresh replaced it; below zero for one that had expired.
	TokenMargin(left time.Duration)
	// ClockSkewed is one token issued more than 30 s from this Orker's clock,
	// within its bound or beyond it.
	ClockSkewed()
	// EngineRefused is one answer, to a mint or to a task's result, that
	// refused this Orker itself.
	EngineRefused(how string)
	// CommitWaited is how long an attempt waited for an engine to record its
	// result, and how that ended.
	CommitWaited(waited time.Duration, outcome string)
	// UpstreamUnrecorded is one attempt that failed because an engine
	// recorded no such result of its upstream.
	UpstreamUnrecorded()
}

// noIdentityObserver hears nothing: the observer of an Orker that exports no
// metrics.
type noIdentityObserver struct{}

func (noIdentityObserver) Refreshed(string)                   {}
func (noIdentityObserver) TokenMargin(time.Duration)          {}
func (noIdentityObserver) ClockSkewed()                       {}
func (noIdentityObserver) EngineRefused(string)               {}
func (noIdentityObserver) CommitWaited(time.Duration, string) {}
func (noIdentityObserver) UpstreamUnrecorded()                {}

func identityObserverOr(o IdentityObserver) IdentityObserver {
	if o == nil {
		return noIdentityObserver{}
	}
	return o
}
