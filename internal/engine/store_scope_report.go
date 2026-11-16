package engine

import (
	"context"
	"log/slog"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The engine's half of the storage scope check (docs/DEPLOY.md, "An Orker's
// own storage credential"): each Orker tests its own credential and reports
// its verdict on DescribeOrker; the engine can only read those reports. Once
// a minute it reads the inventory and logs at error every live Orker whose
// verdict is not scoped, and every Orker that reports none (an older build):
// such a credential may reach the access document, so `enforce` does not
// hold against whoever holds it. It decides nothing.

// StoreScopeReportInterval is how often the engine reports.
const StoreScopeReportInterval = time.Minute

// UnscopedOrker is one live Orker whose storage credential is not confirmed
// scoped.
type UnscopedOrker struct {
	OrkerID string
	// Verdict is "unscoped", "no verdict" (no round has ended either way, or
	// a scoped verdict lapsed) or "not reported" (an older build).
	Verdict string
	Detail  string
}

// UnscopedOrkers lists the Orkers of fleet whose verdict is not scoped, in
// fleet order. An Orker that did not answer is not judged.
func UnscopedOrkers(fleet []FleetOrker) []UnscopedOrker {
	var out []UnscopedOrker
	for _, o := range fleet {
		d := o.Described
		if o.Err != nil || d == nil {
			continue
		}
		u := UnscopedOrker{OrkerID: d.GetOrkerId(), Detail: d.GetStoreScopeDetail()}
		if u.OrkerID == "" {
			u.OrkerID = o.Presence.Identity
		}
		switch d.GetStoreScope() {
		case orkstackv1.StoreScope_STORE_SCOPE_SCOPED:
			continue
		case orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED:
			u.Verdict = "unscoped"
		case orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN:
			u.Verdict = "no verdict"
		default:
			u.Verdict = "not reported"
		}
		out = append(out, u)
	}
	return out
}

// ReportStoreScope reads the inventory once and logs each Orker
// UnscopedOrkers lists at error. A fleet that cannot be read is a warning.
func ReportStoreScope(ctx context.Context, fleet FleetReader, logger *slog.Logger) {
	orkers, err := ReadFleet(ctx, fleet)
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn("the storage scope report could not read the Orker inventory", "error", err)
		}
		return
	}
	// An Orker that is listed but did not answer cannot be judged: it is
	// named, so a fleet that cannot be asked is never a quiet one.
	for _, o := range orkers {
		if o.Err != nil || o.Described == nil {
			logger.Warn("an Orker's storage scope could not be read: it did not answer the inventory",
				"orker", o.Presence.Identity, "error", o.Err)
		}
	}
	for _, u := range UnscopedOrkers(orkers) {
		attrs := []any{"orker", u.OrkerID, "verdict", u.Verdict}
		if u.Detail != "" {
			attrs = append(attrs, "detail", u.Detail)
		}
		logger.Error("an Orker's storage credential is not confirmed scoped to the Orker's own prefixes: "+
			"it may reach the access document, so roles do not hold against whoever holds it "+
			"(docs/DEPLOY.md, \"An Orker's own storage credential\")", attrs...)
	}
}

// RunStoreScopeReport reports every StoreScopeReportInterval until ctx is
// done.
func RunStoreScopeReport(ctx context.Context, fleet FleetReader, logger *slog.Logger) {
	ticker := time.NewTicker(StoreScopeReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ReportStoreScope(ctx, fleet, logger)
		}
	}
}
