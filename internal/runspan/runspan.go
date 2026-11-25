// Package runspan names a run's span and derives a child run's trace context
// from its parent's, both as plain string work.
//
// Workflow code derives a child's context, and workflow code never imports
// OpenTelemetry (AGENTS.md §11). OpenTelemetry's own propagator was weighed
// and is not used here for that reason only; internal/telemetry uses it
// everywhere else.
package runspan

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// RunSpanID is the id of a run's span: the same in every process and release,
// so a child's span can name its parent's before either exists.
func RunSpanID(runID string) [8]byte {
	sum := sha256.Sum256([]byte("ork.run\x00" + runID))
	var id [8]byte
	copy(id[:], sum[:8])
	if id == ([8]byte{}) {
		// W3C forbids the zero id.
		id[7] = 1
	}
	return id
}

// Context is a W3C traceparent of version 00, each part in lowercase hex.
type Context struct {
	TraceID, SpanID, Flags string
}

// String renders c as a traceparent.
func (c Context) String() string { return "00-" + c.TraceID + "-" + c.SpanID + "-" + c.Flags }

// Parse reads a version 00 traceparent, strictly: a header of any other
// version or shape is not one. ok is false for it, and for a zero id.
func Parse(traceparent string) (c Context, ok bool) {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return Context{}, false
	}
	c = Context{TraceID: parts[1], SpanID: parts[2], Flags: parts[3]}
	if !lowerHex(c.TraceID, 32) || !lowerHex(c.SpanID, 16) || !lowerHex(c.Flags, 2) ||
		strings.Trim(c.TraceID, "0") == "" || strings.Trim(c.SpanID, "0") == "" {
		return Context{}, false
	}
	return c, true
}

// ChildTraceparent is the context a child of run parentRunID is started with:
// its parent's trace and flags, under its parent's run span. Empty when the
// parent has none. Pure.
func ChildTraceparent(parent, parentRunID string) string {
	c, ok := Parse(parent)
	if !ok {
		return ""
	}
	id := RunSpanID(parentRunID)
	c.SpanID = hex.EncodeToString(id[:])
	return c.String()
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
