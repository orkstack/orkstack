package automation

import "testing"

// TestParse_AScheduleOrTriggerTargetInAnotherNamespaceIsRefused: schedules and
// triggers run only their own namespace's Orkflows (docs/AUTOMATION.md); a
// "<namespace>/<name>" target is refused by name, not as a malformed slug.
func TestParse_AScheduleOrTriggerTargetInAnotherNamespaceIsRefused(t *testing.T) {
	_, issues := Parse([]byte(header + `
schedules:
  nightly:
    orkflow: platform/deploy
    cron: "0 2 * * *"
triggers:
  hook:
    orkflow: platform/deploy
`))
	requireIssue(t, issues, "schedules.nightly.orkflow", "a schedule runs only its own namespace's Orkflows", "uses: orkflow:platform/deploy@<version>")
	requireIssue(t, issues, "triggers.hook.orkflow", "a trigger runs only its own namespace's Orkflows")
}
