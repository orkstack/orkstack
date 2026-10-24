// Package webhooks delivers run state transitions and task outcomes to
// external systems.
//
// A subscriber registers a URL, the transitions it cares about, and
// optionally a scope — one namespace, one orkflow, one task, each an exact
// match and each empty meaning every. When a run or one of its tasks crosses
// a transition the subscription wants, the engine renders a JSON body, signs
// it with the subscription's secret, and POSTs it. Task outcome events are
// higher-cardinality hints on this best-effort channel; a subscriber that must
// not miss one reconciles with GetRun. Delivery is at-least-once: a transport
// failure or a 5xx is retried with exponential backoff, a 4xx is not.
//
// The package is deliberately ignorant of protobuf and of OrkStack's JSON
// dialect. A Delivery carries the already-encoded run as json.RawMessage, so
// the one encoder that owns that dialect (internal/api) stays the only place
// that knows how a Run is spelled on the wire.
//
// Three pieces, each usable alone:
//
//	Registry   who is subscribed, to what, and in which scope
//	Dispatcher one POST, signed, with the retry budget
//	Notifier   fan-out: registry ∩ event → dispatcher, off the caller's goroutine
package webhooks
