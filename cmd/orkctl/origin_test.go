package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestOriginKindSlug_MatchesTheEnum(t *testing.T) {
	values := orkstackv1.File_orkstack_v1_run_proto.Enums().ByName("RunOriginKind").Values()
	for i := 0; i < values.Len(); i++ {
		value := values.Get(i)
		kind := orkstackv1.RunOriginKind(value.Number())
		slug, ok := originKindSlug(kind)
		if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
			if ok {
				t.Fatalf("UNSPECIFIED has slug %q", slug)
			}
			continue
		}
		want := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(string(value.Name()), "RUN_ORIGIN_KIND_")), "_", "-")
		if !ok || slug != want {
			t.Fatalf("%s slug = %q (%v), want %q", value.Name(), slug, ok, want)
		}
		if back, ok := parseOriginKindSlug(slug); !ok || back != kind {
			t.Fatalf("parseOriginKindSlug(%q) = %v, %v", slug, back, ok)
		}
	}
	if _, ok := parseOriginKindSlug("unspecified"); ok {
		t.Fatal("unspecified parsed as a kind")
	}
}

func TestParseOriginFilter(t *testing.T) {
	for _, tc := range []struct {
		in   string
		kind orkstackv1.RunOriginKind
		id   string
	}{
		{"", orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED, ""},
		{"trigger", orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, ""},
		{"trigger:trg-x", orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, "trg-x"},
		{"parent-run:ork-y", orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, "ork-y"},
		{"schedule:sch-a:b", orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE, "sch-a:b"},
	} {
		kind, id, err := parseOriginFilter(tc.in)
		if err != nil || kind != tc.kind || id != tc.id {
			t.Fatalf("parseOriginFilter(%q) = %v, %q, %v; want %v, %q", tc.in, kind, id, err, tc.kind, tc.id)
		}
	}
	_, _, err := parseOriginFilter("bogus")
	if err == nil {
		t.Fatal("bogus parsed")
	}
	for _, slug := range []string{"submit", "trigger", "schedule", "parent-run", "run-again"} {
		if !strings.Contains(err.Error(), slug) {
			t.Fatalf("error %q does not name %s", err, slug)
		}
	}
	if _, _, err := parseOriginFilter("trigger:"); err == nil {
		t.Fatal("an empty id after a colon parsed")
	}
}

func TestRunSubmit_SendsOriginRef(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	out := f.run("--json", "run", "submit", "hello-ork", "--origin-ref", "https://ci.example/jobs/7").requireExit(t, exitOK)
	var response struct {
		Run struct {
			Origin map[string]string `json:"origin"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(out.stdout), &response); err != nil {
		t.Fatalf("decode: %v (%s)", err, out.stdout)
	}
	if response.Run.Origin["kind"] != "SUBMIT" || response.Run.Origin["reference"] != "https://ci.example/jobs/7" {
		t.Fatalf("submitted origin = %v", response.Run.Origin)
	}

	refused := f.run("run", "submit", "hello-ork", "--origin-ref", strings.Repeat("x", 257))
	if refused.code == exitOK || !strings.Contains(refused.stderr, "256") {
		t.Fatalf("an over-long reference = exit %d, stderr %q; want the server's refusal naming 256", refused.code, refused.stderr)
	}
}

func TestRunAgain_SendsSourceRunIdAndOriginRef(t *testing.T) {
	f := newFixture(t)
	f.applyExample().requireExit(t, exitOK)
	f.run("run", "submit", "hello-ork", "--idempotency-key", "k").requireExit(t, exitOK)

	dry := f.run("run", "again", "run-001", "--origin-ref", "INC-4412", "--dry-run").requireExit(t, exitOK)
	var request map[string]any
	if err := json.Unmarshal([]byte(dry.stdout), &request); err != nil {
		t.Fatalf("decode dry run: %v (%s)", err, dry.stdout)
	}
	if request["sourceRunId"] != "run-001" || request["originReference"] != "INC-4412" {
		t.Fatalf("dry-run submission = %v, want sourceRunId run-001 and originReference INC-4412", request)
	}
	if _, reused := request["idempotencyKey"]; reused {
		t.Fatalf("dry-run submission reuses the source's key: %v", request)
	}

	again := f.run("--json", "run", "again", "run-001", "--origin-ref", "INC-4412").requireExit(t, exitOK)
	var response struct {
		Run struct {
			ID     string            `json:"id"`
			Origin map[string]string `json:"origin"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(again.stdout), &response); err != nil {
		t.Fatalf("decode: %v (%s)", err, again.stdout)
	}
	if response.Run.ID == "run-001" || response.Run.Origin["kind"] != "RUN_AGAIN" ||
		response.Run.Origin["id"] != "run-001" || response.Run.Origin["reference"] != "INC-4412" {
		t.Fatalf("run again = %+v", response.Run)
	}
}

// seedOriginRuns stores three finished runs with different origins and one
// with none, as the read model would hold them.
func seedOriginRuns(f *fixture) {
	at := func(minute int) *timestamppb.Timestamp {
		return timestamppb.New(fixedNow.Add(time.Duration(minute) * time.Minute))
	}
	for i, run := range []*orkstackv1.Run{
		{Id: "run-legacy", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0"},
		{Id: "run-submit", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0",
			Origin: &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}},
		{Id: "run-trigger", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0",
			Origin: &orkstackv1.RunOrigin{
				Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-4a1c0e2b7d3f8156",
				Reference: "https://github.com/acme/svc/actions/runs/123", Description: "GitHub deploy hook",
				Principal: "ci-github",
			}},
		{Id: "run-again", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0",
			Origin: &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN, Id: "run-submit"}},
	} {
		run.Namespace = "default"
		run.Status = orkstackv1.Status_STATUS_SUCCEEDED
		run.CreatedAt = at(i)
		run.StartedAt = at(i)
		run.FinishedAt = at(i)
		f.runs.Save(run)
	}
}

func TestRunList_SendsOriginFilter(t *testing.T) {
	f := newFixture(t)
	seedOriginRuns(f)
	all := f.run("run", "list").requireExit(t, exitOK)
	all.requireContains(t, "ORIGIN", "trigger trg-4a1c0e2b7d3f8156", "run-again run-submit", "submit")
	for _, line := range strings.Split(all.stdout, "\n") {
		if strings.HasPrefix(line, "run-legacy") && !strings.Contains(line, " - ") {
			t.Fatalf("an unrecorded origin renders %q, want a dash in its column", line)
		}
	}
	filtered := f.run("run", "list", "--origin", "trigger:trg-4a1c0e2b7d3f8156").requireExit(t, exitOK)
	requireGolden(t, "run-list-origin.golden", filtered.stdout)
	if strings.Contains(filtered.stdout, "run-submit") || strings.Contains(filtered.stdout, "run-legacy") {
		t.Fatalf("the filtered list shows other runs:\n%s", filtered.stdout)
	}
	submit := f.run("run", "list", "--origin", "submit").requireExit(t, exitOK)
	if !strings.Contains(submit.stdout, "run-submit") || strings.Contains(submit.stdout, "run-legacy") ||
		strings.Contains(submit.stdout, "run-trigger") {
		t.Fatalf("--origin submit =\n%s", submit.stdout)
	}
	bad := f.run("run", "list", "--origin", "bogus")
	if bad.code == exitOK || !strings.Contains(bad.stderr, "parent-run") {
		t.Fatalf("--origin bogus = exit %d, stderr %q", bad.code, bad.stderr)
	}
	paged := f.run("run", "list", "--origin", "trigger", "--limit", "1")
	paged.requireExit(t, exitOK)
	if strings.Contains(paged.stdout, "Next page") && !strings.Contains(paged.stdout, "--origin trigger") {
		t.Fatalf("the next-page command drops the origin filter its token is bound to:\n%s", paged.stdout)
	}
}

func TestRunGet_OriginTrigger_Golden(t *testing.T) {
	f := newFixture(t)
	seedOriginRuns(f)
	got := f.run("run", "get", "run-trigger").requireExit(t, exitOK)
	requireGolden(t, "run-get-origin-trigger.golden", got.stdout)
	got.requireContains(t, "not authenticated")
}

func TestRunGet_OriginUnrecorded_Golden(t *testing.T) {
	f := newFixture(t)
	seedOriginRuns(f)
	got := f.run("run", "get", "run-legacy").requireExit(t, exitOK)
	requireGolden(t, "run-get-origin-unrecorded.golden", got.stdout)
	if strings.Contains(got.stdout, "Origin:    submit") {
		t.Fatalf("a run with no origin rendered as submit:\n%s", got.stdout)
	}
}

func TestRunGet_JSONCarriesOrigin(t *testing.T) {
	f := newFixture(t)
	seedOriginRuns(f)
	got := f.run("--json", "run", "get", "run-trigger").requireExit(t, exitOK)
	var response struct {
		Run struct {
			Origin map[string]string `json:"origin"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := map[string]string{
		"kind": "TRIGGER", "id": "trg-4a1c0e2b7d3f8156",
		"reference": "https://github.com/acme/svc/actions/runs/123", "description": "GitHub deploy hook",
		"principal": "ci-github",
	}
	for key, value := range want {
		if response.Run.Origin[key] != value {
			t.Fatalf("origin[%s] = %q, want %q (origin %v)", key, response.Run.Origin[key], value, response.Run.Origin)
		}
	}
	legacy := f.run("--json", "run", "get", "run-legacy").requireExit(t, exitOK)
	if strings.Contains(legacy.stdout, `"origin"`) {
		t.Fatalf("a run with no origin carries an origin object:\n%s", legacy.stdout)
	}
}
