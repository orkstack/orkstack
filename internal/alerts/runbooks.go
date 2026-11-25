package alerts

import (
	"errors"
	"fmt"
	"strings"
)

// RunbooksFile is the runbooks' document. Its alert entries, between
// RunbooksBegin and RunbooksEnd, are generated; the rest is written by hand.
const (
	RunbooksFile  = "docs/RUNBOOKS.md"
	RunbooksBegin = "<!-- BEGIN GENERATED ALERTS: go run ./internal/alerts/cmd/genalerts -->"
	RunbooksEnd   = "<!-- END GENERATED ALERTS -->"
)

// groupTitles are the runbook sections, one per rule group, in the order
// the groups first appear.
var groupTitles = map[string]string{
	"pipeline":    "Telemetry pipeline and liveness",
	"pools":       "Pools, Orkers and usage",
	"runs":        "Runs, admission and integrity",
	"storage":     "Archive, object store, API and identity",
	"temporal":    "Temporal",
	"schedules":   "Schedules, webhooks and triggers",
	"concurrency": "Concurrency limits",
	"cleanup":     "Retention cleanup and artifact GC",
}

var severityWords = map[Severity]string{Critical: "critical", Warning: "warning", Info: "info", None: "none (always firing)"}

// runbooks is the generated part of RUNBOOKS.md: one section per group, one
// entry per alert, each with its anchor's heading.
func runbooks(rules []Rule) string {
	var b strings.Builder
	group := ""
	for _, r := range rules {
		if r.Group != group {
			group = r.Group
			fmt.Fprintf(&b, "\n## %s\n", groupTitles[group])
		}
		forText := r.For
		if forText == "" || forText == "0m" {
			forText = "at once"
		} else {
			forText = "after " + forText
		}
		fmt.Fprintf(&b, "\n### %s%s\n\n", Prefix, r.Name)
		fmt.Fprintf(&b, "Severity %s; fires %s. %s\n\n", severityWords[r.Severity], forText, plain(r.Summary))
		fmt.Fprintf(&b, "- **What it means:** %s\n", r.Meaning)
		fmt.Fprintf(&b, "- **Impact:** %s\n", r.Impact)
		fmt.Fprintf(&b, "- **Confirm:** %s\n", r.Confirm)
		fmt.Fprintf(&b, "- **Fix:** %s\n", r.Fix)
		if len(r.Related) > 0 {
			var links []string
			for _, rel := range r.Related {
				if strings.HasPrefix(rel, "#") {
					links = append(links, fmt.Sprintf("[%s](%s)", strings.TrimPrefix(rel, "#"), rel))
					continue
				}
				links = append(links, fmt.Sprintf("[%s%s](#%s)", Prefix, rel, Anchor(rel)))
			}
			fmt.Fprintf(&b, "- **Related:** %s\n", strings.Join(links, ", "))
		}
	}
	return b.String()
}

// plain is an annotation's text with its label references written as the
// label's name, for a document that has no alert to fill them from.
func plain(s string) string {
	return labelRef.ReplaceAllString(s, "`$1`")
}

// Splice puts the generated entries between the markers of doc.
func Splice(doc, generated string) (string, error) {
	if strings.Count(doc, RunbooksBegin) != 1 || strings.Count(doc, RunbooksEnd) != 1 {
		return "", errors.New(RunbooksFile + " must hold each generated-alerts marker exactly once")
	}
	i := strings.Index(doc, RunbooksBegin)
	j := strings.Index(doc, RunbooksEnd)
	if j < i {
		return "", errors.New(RunbooksFile + "'s generated-alerts markers are out of order")
	}
	return doc[:i+len(RunbooksBegin)] + "\n" + generated + "\n" + doc[j:], nil
}
