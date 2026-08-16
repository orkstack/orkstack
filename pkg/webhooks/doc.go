// Package webhooks delivers run state transitions to external systems.
//
// A subscriber registers a URL and the transitions it cares about. When a run
// crosses one of them the engine renders a JSON body, signs it with the
// subscription's secret, and POSTs it. Delivery is at-least-once: a transport
// failure or a 5xx is retried with exponential backoff, a 4xx is not.
//
// The package is deliberately ignorant of protobuf and of OrkStack's JSON
// dialect. A Delivery carries the already-encoded run as json.RawMessage, so
// the one encoder that owns that dialect (internal/api) stays the only place
// that knows how a Run is spelled on the wire.
//
// Three pieces, each usable alone:
//
//	Registry   who is subscribed, and to what
//	Dispatcher one POST, signed, with the retry budget
//	Notifier   fan-out: registry ∩ event → dispatcher, off the caller's goroutine
package webhooks
