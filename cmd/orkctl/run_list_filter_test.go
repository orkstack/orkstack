package main

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// seedFilterRuns saves runs of two Orkflows in three statuses, a minute
// apart, so filtered listings and their pages have something to choose from.
func seedFilterRuns(f *fixture) {
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	for i, run := range []*orkstackv1.Run{
		{Id: "run-a", OrkflowName: "deploy-api", Status: orkstackv1.Status_STATUS_SUCCEEDED},
		{Id: "run-b", OrkflowName: "deploy-api", Status: orkstackv1.Status_STATUS_FAILED},
		{Id: "run-c", OrkflowName: "build-web", Status: orkstackv1.Status_STATUS_FAILED},
		{Id: "run-d", OrkflowName: "deploy-api", Status: orkstackv1.Status_STATUS_CANCELED},
		{Id: "run-e", OrkflowName: "deploy-api", Status: orkstackv1.Status_STATUS_FAILED},
	} {
		at := timestamppb.New(base.Add(time.Duration(i) * time.Minute))
		run.Namespace, run.OrkflowVersion = "default", "1.0.0"
		run.CreatedAt, run.StartedAt, run.FinishedAt = at, at, at
		run.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}
		f.runs.Save(run)
	}
}

func TestRunList_FilteredGolden(t *testing.T) {
	f := newFixture(t)
	seedFilterRuns(f)
	out := f.run("run", "list",
		"--filter", "orkflow_name^deploy",
		"--filter", "status=FAILED,CANCELED",
		"--filter", "created_at>=2026-10-01T09:01:00Z").requireExit(t, exitOK)
	requireGolden(t, "run-list-filtered.golden", out.stdout)
	none := f.run("run", "list", "--filter", "orkflow_name~zz").requireExit(t, exitOK)
	if none.stdout != "No runs match --filter 'orkflow_name~zz'.\n" {
		t.Fatalf("no match = %q", none.stdout)
	}
	// What the server refuses is printed verbatim, with a non-zero exit.
	bad := f.run("run", "list", "--filter", "created_at>=2026-10-01")
	if bad.code == exitOK || !strings.Contains(bad.stderr, `"2026-10-01" is not an RFC 3339 instant with a zone (2026-10-09T00:00:00Z)`) {
		t.Fatalf("a bare date = exit %d, stderr %q", bad.code, bad.stderr)
	}
}

// TestRunList_PrintsANextPageLineWithItsFilters: the printed next-page
// command repeats every filter, the limit and the token, and running it
// continues the same listing to its end.
func TestRunList_PrintsANextPageLineWithItsFilters(t *testing.T) {
	f := newFixture(t)
	seedFilterRuns(f)
	first := f.run("run", "list", "--filter", "orkflow_name=deploy-api", "--filter", "status!=SUCCEEDED", "--limit", "2").requireExit(t, exitOK)
	requireGolden(t, "run-list-next-page.golden", first.stdout)
	idx := strings.Index(first.stdout, "Next page: ")
	if idx < 0 {
		t.Fatalf("no next-page line:\n%s", first.stdout)
	}
	line := strings.TrimSpace(first.stdout[idx+len("Next page: "):])
	const prefix = "orkctl run list --filter 'orkflow_name=deploy-api' --filter 'status!=SUCCEEDED' --limit 2 --page-token "
	if !strings.HasPrefix(line, prefix) {
		t.Fatalf("next-page line = %q, want it to start %q", line, prefix)
	}
	token := strings.TrimPrefix(line, prefix)
	second := f.run("run", "list", "--filter", "orkflow_name=deploy-api", "--filter", "status!=SUCCEEDED", "--limit", "2", "--page-token", token).requireExit(t, exitOK)
	second.requireContains(t, "run-b")
	if strings.Contains(second.stdout, "Next page") || strings.Contains(second.stdout, "run-e") || strings.Contains(second.stdout, "run-a") {
		t.Fatalf("second page:\n%s", second.stdout)
	}
	// The token is bound to its filters: another set is refused, verbatim.
	other := f.run("run", "list", "--filter", "orkflow_name=deploy-api", "--limit", "2", "--page-token", token)
	if other.code == exitOK || !strings.Contains(other.stderr, "page_token was issued for other filters; drop it and list from the first page") {
		t.Fatalf("a token under other filters = exit %d, stderr %q", other.code, other.stderr)
	}
}
