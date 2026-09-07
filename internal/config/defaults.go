package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// EmptyDefault and DerivedDefault are the two standardized placeholder
// values Var.Default carries for a leaf with no fixed literal fallback:
// EmptyDefault when the real default is the empty string (or an empty
// list/map), DerivedDefault for anything else — a value computed at
// runtime (§ engine.limits.transfer_budget_seconds), or one an underlying
// SDK/library owns and this package does not second-guess (the OTel SDK
// defaults, the container runtime's pull timeout). Var.DerivedNote carries
// the human sentence a placeholder Default alone cannot: what "derived"
// actually means for that one key.
//
// Every OTHER Schema entry's Default is bound directly to the same Go
// constant the binary itself falls back to (formatted through formatBytes
// or formatDuration where the constant is a byte count or a
// time.Duration), so a constant's value can never drift silently out of
// sync with what docs/DEPLOY.md and deploy/orkstack.defaults.yaml claim —
// see render.go and TestSchemaCompleteness_DefaultsMatchDocs.
const (
	EmptyDefault   = "(empty)"
	DerivedDefault = "(derived)"
)

// formatBytes renders a byte-count constant the same human-readable way
// docs/DEPLOY.md's configuration table already does — "256 KiB", "5 GiB" —
// falling back to a bare decimal integer for a value that is not an exact
// multiple of one of the three units this codebase actually uses.
func formatBytes(n int64) string {
	switch {
	case n != 0 && n%(1<<30) == 0:
		return fmt.Sprintf("%d GiB", n/(1<<30))
	case n != 0 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n/(1<<20))
	case n != 0 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n/(1<<10))
	default:
		return strconv.FormatInt(n, 10)
	}
}

// formatDuration renders a time.Duration constant the same short way
// docs/DEPLOY.md writes it — "24h", "500ms" — rather than
// time.Duration.String()'s "24h0m0s": it picks the coarsest unit the value
// divides evenly into, falling back to String() for anything that does not
// divide evenly into whole milliseconds.
func formatDuration(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	case d%time.Millisecond == 0:
		return fmt.Sprintf("%dms", d/time.Millisecond)
	default:
		return d.String()
	}
}

// formatInt renders a plain integer constant (a count, not a byte size) as
// its decimal string.
func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

// formatBool renders a bool constant the way config.Load's KindBool leaves
// parse it back ("true"/"false").
func formatBool(b bool) string {
	return strconv.FormatBool(b)
}

// formatList renders a []string constant the way config.Load's KindList
// leaves parse it back — comma-separated, no spaces.
func formatList(items []string) string {
	return strings.Join(items, ",")
}
