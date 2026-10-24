//go:build integration

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
)

// The shipped-examples scenarios (examples/README.md): every example this repository ships runs end to end, as a reader
// would run it — the shipped file, applied under its own name, submitted with
// the inputs its header names — against a real engine, Orker, Temporal and
// MinIO. Each scenario writes its transcript to
// <ORK_PROOF_DIR>/orkctl/examples-<E>.txt.

// scenario runs one named scenario as a subtest on the shared stack: the
// stack's helpers report to the subtest, and the transcript is written when
// it ends, pass or fail.
func (x *examplesStack) scenario(t *testing.T, name string, body func(t *testing.T)) {
	t.Run(name, func(t *testing.T) {
		parent := x.t
		x.t = t
		defer func() { x.t = parent }()
		defer x.finish(t, name)
		body(t)
	})
}

func asJSON(t *testing.T, v any) string {
	t.Helper()
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func mustJSON(t *testing.T, text string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(text), &v); err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return v
}

func requireStatuses(t *testing.T, run *orkstackv1.Run, want orkstackv1.Status, ids ...string) {
	t.Helper()
	for _, id := range ids {
		requireTaskStatus(t, run, id, want)
	}
}

// requireRunFailedAt holds a FAILED run to the task whose failure it carries
// (Run.error.details is `task "<id>"`), and returns that task's own error.
func requireRunFailedAt(t *testing.T, run *orkstackv1.Run, id string) *orkstackv1.Error {
	t.Helper()
	requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
	if got, want := run.GetError().GetDetails(), fmt.Sprintf("task %q", id); got != want {
		t.Fatalf("run %s failed at %s (%v), want at %q", run.GetId(), got, run.GetError(), id)
	}
	return taskRunOf(t, run, id).GetError()
}

func TestIntegration_EveryExampleValidatesClean(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	x := newExamplesStack(ctx, t, examplesOptions{})
	defer x.finish(t, "E1")
	ensureExampleImages(ctx, t, x.orkctlPath, x.root)
	x.apply("examples/release-block.ork.yaml")
	x.apply("examples/notify-block.ork.yaml")

	// The whole of the documented exceptions: cached-pipeline's train sets
	// no TTL on purpose (TestCompile_CacheCompiles asserts it).
	expectedWarnings := map[string]string{"examples/cached-pipeline.ork.yaml": "tasks[2].cache.ttl_seconds"}
	for _, file := range exampleFiles(t, x.root) {
		wf, err := dsl.CompileFile(filepath.Join(x.root, file))
		if err != nil {
			t.Fatalf("compile %s: %v", file, err)
		}
		path := file
		if file == "examples/custom-image.ork.yaml" {
			// The shipped document names the `:dev` tags; this process
			// pushed its own (exampleImageTag).
			path = x.variantFile("custom-image.ork.yaml", customImageDocument(t, x.root))
		}
		out := x.in("orkflow", "validate", path)
		valid := fmt.Sprintf("Valid orkflow %s (version %s)", wf.GetName(), wf.GetVersion())
		if field, ok := expectedWarnings[file]; ok {
			if !strings.Contains(out, valid+", 1 warning\n") || !strings.Contains(out, field) {
				t.Errorf("%s: validate printed\n%s\nwant exactly one warning, at %s", file, out, field)
			}
			continue
		}
		if out != valid+"\n" {
			t.Errorf("%s: validate printed\n%s\nwant exactly %q (no warning)", file, out, valid)
		}
	}

	// The automation document, planned with the two Orkflows it names.
	shipped, err := os.ReadFile(filepath.Join(x.root, "examples", "automation", "examples.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	variant := x.variantFile("examples.ork.yaml", automationVariant(t, shipped, x.ns, x.services.base))
	out := x.in("--json", "apply", "examples/poll-until-ready.ork.yaml", "examples/approval-gate.ork.yaml", variant, "--dry-run")
	var report orkstackv1.ApplyReport
	if err := api.UnmarshalJSON([]byte(out), &report); err != nil {
		t.Fatalf("decode ApplyReport: %v\n%s", err, out)
	}
	for _, doc := range report.GetOrkflows() {
		if issues := doc.GetValidated().GetIssues(); len(issues) != 0 {
			t.Errorf("%s: issues %v", doc.GetFile(), issues)
		}
	}
	if len(report.GetAutomation()) != 1 {
		t.Fatalf("automation reports = %d, want 1", len(report.GetAutomation()))
	}
	plan := report.GetAutomation()[0].GetPlan()
	if len(plan.GetIssues()) != 0 {
		t.Fatalf("automation plan issues: %v", plan.GetIssues())
	}
	planned := map[string]orkstackv1.AutomationAction{}
	for _, step := range plan.GetSteps() {
		planned[step.GetKind().String()+"/"+step.GetName()] = step.GetAction()
	}
	want := map[string]orkstackv1.AutomationAction{
		"AUTOMATION_OBJECT_KIND_NAMESPACE/" + x.ns:      orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE,
		"AUTOMATION_OBJECT_KIND_SCHEDULE/poll-exports":  orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE,
		"AUTOMATION_OBJECT_KIND_TRIGGER/change-from-ci": orkstackv1.AutomationAction_AUTOMATION_ACTION_CREATE,
	}
	if !reflect.DeepEqual(planned, want) {
		t.Fatalf("automation plan = %v, want %v", planned, want)
	}
	x.note("every example validated with no warning but cached-pipeline's documented one; the automation plan is %v", planned)
}

func TestIntegration_ExamplesRunAsShipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	x := newExamplesStack(ctx, t, examplesOptions{})
	base := x.services.base

	x.scenario(t, "E2", func(t *testing.T) {
		x.apply("examples/decide-and-route.ork.yaml")
		cases := []struct {
			name    string
			flags   []string
			decide  string
			ran     []string
			skipped []string
			plan    string
		}{
			{"default input", nil, `{"result":{"route":"direct","notify":false}}`,
				[]string{"direct"}, []string{"canary", "hold", "notify-owner"}, "direct: all traffic at once"},
			{"a risky change", []string{"--input-json", `{"change":{"service":"payments","risk":"high","files_changed":40}}`},
				`{"result":{"route":"canary","notify":true}}`,
				[]string{"canary", "notify-owner"}, []string{"direct", "hold"}, "canary: 5%, then 50%, then 100% of traffic"},
			{"a freeze", []string{"--input", "freeze=true"}, `{"result":{"route":"hold","notify":false}}`,
				[]string{"hold"}, []string{"canary", "direct", "notify-owner"}, "hold: a change freeze is on, nothing rolls out"},
		}
		for _, c := range cases {
			run := x.runToEnd("decide-and-route", c.flags...)
			requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
			requireStatuses(t, run, orkstackv1.Status_STATUS_SUCCEEDED, append([]string{"decide", "report"}, c.ran...)...)
			requireStatuses(t, run, orkstackv1.Status_STATUS_SKIPPED, c.skipped...)
			if got := taskOutput(t, run, "decide"); !reflect.DeepEqual(got, mustJSON(t, c.decide)) {
				t.Fatalf("%s: decide = %v, want %s", c.name, got, c.decide)
			}
			if got, want := taskOutput(t, run, "report"), map[string]any{"result": map[string]any{"plan": c.plan}}; !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: report = %v, want %v", c.name, got, want)
			}
			// The plan, given the decision the run made, plans exactly what ran.
			plan := x.plan(append(append([]string{"decide-and-route"}, c.flags...), "--output", "decide="+c.decide)...)
			requirePlanMatchesRun(t, plan, run)
		}
		stdout, stderr, code := x.taskRun("--json", "--file", "examples/decide-and-route.ork.yaml", "--task", "decide",
			"--input-json", `{"change":{"risk":"high","files_changed":3}}`)
		if code != 0 {
			t.Fatalf("task run decide exit %d\n%s\n%s", code, stdout, stderr)
		}
		if got := taskRunValues(t, stdout); !reflect.DeepEqual(got, mustJSON(t, `{"result":{"route":"canary","notify":true}}`)) {
			t.Fatalf("task run decide values = %v", got)
		}
	})

	x.scenario(t, "E3", func(t *testing.T) {
		x.apply("examples/each-fan-out.ork.yaml")
		run := x.runToEnd("each-fan-out")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		if got, want := taskOutput(t, run, "list"), mustJSON(t, `{"result":[{"name":"a"},{"name":"b"},{"name":"c"},{"name":"d"}]}`); !reflect.DeepEqual(got, want) {
			t.Fatalf("list = %v, want %v", got, want)
		}
		items := taskRunOf(t, run, "convert").GetItems()
		if len(items) != 4 {
			t.Fatalf("convert has %d items, want 4", len(items))
		}
		for _, item := range items {
			if item.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("convert item %d = %s", item.GetItem(), item.GetStatus())
			}
		}
		if got := string(x.download(run.GetId(), "merge", "merged.txt", -1)); got != "a\nb\nc\nd\n" {
			t.Fatalf("merged.txt = %q", got)
		}
	})

	x.scenario(t, "E4", func(t *testing.T) {
		x.apply("examples/inputs-and-escaping.ork.yaml")
		run := x.runToEnd("inputs-and-escaping", "--input", "release=1.4.2")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		const note = "it's a test; echo unsafe"
		if got := string(x.download(run.GetId(), "positional", "note.txt", -1)); got != "note="+note+"\n" {
			t.Fatalf("note.txt = %q", got)
		}
		logs := x.in("run", "logs", run.GetId(), "--task", "quoted")
		if !strings.Contains(logs, "note="+note) {
			t.Fatalf("quoted's log lacks the quoted note:\n%s", logs)
		}
		for _, line := range strings.Split(logs, "\n") {
			if strings.TrimSpace(line) == "unsafe" || strings.HasSuffix(line, "] unsafe") {
				t.Fatalf("quoted ran `echo unsafe`: the sh helper did not quote the note:\n%s", logs)
			}
		}
		var inputs any
		if err := json.Unmarshal(x.download(run.GetId(), "as-json", "inputs.json", -1), &inputs); err != nil {
			t.Fatalf("inputs.json: %v", err)
		}
		if want := mustJSON(t, `{"note":"it's a test; echo unsafe","release":"1.4.2","retries":3}`); !reflect.DeepEqual(inputs, want) {
			t.Fatalf("inputs.json = %v, want %v", inputs, want)
		}
		if got := string(x.download(run.GetId(), "stamp", "stamp.txt", -1)); got != "release: 1.4.2\nstage: build of 1.4.2\nstage: test of 1.4.2\nstage: package of 1.4.2\n" {
			t.Fatalf("stamp.txt = %q", got)
		}
		requireTaskStatus(t, run, "regional", orkstackv1.Status_STATUS_SUCCEEDED)
		requireTaskStatus(t, run, "publish", orkstackv1.Status_STATUS_SKIPPED)

		// Without the required input, the first task that renders it fails
		// RENDER_FAILED, a ContractViolation, and nothing that renders it ran.
		run = x.runToEnd("inputs-and-escaping")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_FAILED)
		failedAt := strings.Trim(strings.TrimPrefix(run.GetError().GetDetails(), "task "), `"`)
		taskErr := taskRunOf(t, run, failedAt).GetError()
		if taskErr.GetCode() != "RENDER_FAILED" || taskErr.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
			t.Fatalf("the run failed at %s with %v, want RENDER_FAILED (ContractViolation)", failedAt, taskErr)
		}
		for _, id := range []string{"as-json", "publish", "stamp", "archive"} {
			if got := taskRunOf(t, run, id).GetStatus(); got == orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("%s, which renders .inputs.release, succeeded without it", id)
			}
		}
	})

	x.scenario(t, "E5", func(t *testing.T) {
		x.apply("examples/notify-block.ork.yaml")
		x.apply("examples/order-saga.ork.yaml")
		saga := func(order string) *orkstackv1.Run {
			return x.runToEnd("order-saga", "--input", "api="+base, "--input", "notify_url="+base+"/notify",
				"--input-json", `{"order":`+order+`}`)
		}

		// (a) every step succeeds.
		mark := len(x.services.Notifications(t, 0))
		run := saga(`{"id":"o-1001","sku":"kb-01","quantity":2,"amount":49.9,"address":"1 Main St"}`)
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		requireStatuses(t, run, orkstackv1.Status_STATUS_SKIPPED, "release", "refund")
		prepared := taskOutput(t, run, "prepare")["result"].(map[string]any)
		requests := x.services.Requests("/orders/o-1001/")
		var seen []string
		for _, r := range requests {
			seen = append(seen, r.Method+" "+r.Path)
		}
		wantSeen := []string{"PUT /orders/o-1001/reservation", "PUT /orders/o-1001/payment", "PUT /orders/o-1001/shipment"}
		if !reflect.DeepEqual(seen, wantSeen) {
			t.Fatalf("the order service saw %v, want %v", seen, wantSeen)
		}
		// The http image sends a string `body:` verbatim, with no newline
		// added (images/activity-http/entry.sh writes it with `jq -j`): the
		// bytes are exactly the text prepare built.
		for i, key := range []string{"reservation", "payment", "shipment"} {
			if string(requests[i].Body) != fmt.Sprint(prepared[key]) {
				t.Fatalf("%s body = %q, want what prepare built: %q", key, requests[i].Body, prepared[key])
			}
		}
		notes := x.services.Notifications(t, mark)
		if len(notes) != 1 || notes[0] != (notification{Subject: "order saga", Text: "order o-1001: reserved, charged and shipped"}) {
			t.Fatalf("notifications = %+v", notes)
		}
		if got := taskOutput(t, run, "notify"); !reflect.DeepEqual(got, map[string]any{"status": "200"}) {
			t.Fatalf("notify = %v, want {status: \"200\"}", got)
		}
		x.note("(a) notification: %s", notes[0].Text)

		// (b) the shipment is refused: compensate, report, and still fail.
		x.services.SetStatus("/orders/o-1002/shipment", http.StatusServiceUnavailable)
		mark = len(x.services.Notifications(t, 0))
		run = saga(`{"id":"o-1002","sku":"kb-01","quantity":1,"amount":19.9,"address":"2 Main St"}`)
		requireRunFailedAt(t, run, "shipped")
		requireStatuses(t, run, orkstackv1.Status_STATUS_SUCCEEDED, "release", "refund", "report", "notify")
		var deletes []string
		for _, r := range x.services.Requests("/orders/o-1002/") {
			if r.Method == http.MethodDelete {
				deletes = append(deletes, r.Path)
			}
		}
		if !reflect.DeepEqual(deletes, []string{"/orders/o-1002/reservation", "/orders/o-1002/payment"}) &&
			!reflect.DeepEqual(deletes, []string{"/orders/o-1002/payment", "/orders/o-1002/reservation"}) {
			t.Fatalf("the compensations sent DELETE %v, want the reservation and the payment once each", deletes)
		}
		notes = x.services.Notifications(t, mark)
		if len(notes) != 1 || !strings.HasPrefix(notes[0].Text, "order o-1002 failed at shipped (") ||
			!strings.HasSuffix(notes[0].Text, "release succeeded, refund succeeded") {
			t.Fatalf("notifications = %+v", notes)
		}
		x.note("(b) notification: %s", notes[0].Text)

		// (c) the order is refused before any request.
		before := len(x.services.Requests("/orders/"))
		mark = len(x.services.Notifications(t, 0))
		run = saga(`{"id":"Bad Id","sku":"kb-01","quantity":1,"amount":1,"address":"3 Main St"}`)
		requireRunFailedAt(t, run, "prepare")
		if after := len(x.services.Requests("/orders/")); after != before {
			t.Fatalf("a refused order reached the order service (%d requests)", after-before)
		}
		requireStatuses(t, run, orkstackv1.Status_STATUS_SKIPPED, "release", "refund")
		requireStatuses(t, run, orkstackv1.Status_STATUS_SUCCEEDED, "report", "notify")
		notes = x.services.Notifications(t, mark)
		if len(notes) != 1 || !strings.HasPrefix(notes[0].Text, "order Bad Id failed at prepare (") {
			t.Fatalf("notifications = %+v", notes)
		}
		x.note("(c) notification: %s", notes[0].Text)
	})

	x.scenario(t, "E6", func(t *testing.T) {
		x.apply("examples/notify-block.ork.yaml")
		x.apply("examples/approval-gate.ork.yaml")
		const payments = `{"changes":[{"service":"payments","setting":"limit","value":"10"}]}`
		awaitApproval := func(extra ...string) string {
			runID := x.submit("approval-gate", append([]string{"--input", "notify_url=" + base + "/notify"}, extra...)...)
			x.await(runID, func(run *orkstackv1.Run) bool {
				for _, tr := range run.GetTaskRuns() {
					if tr.GetTaskId() == "approval" && tr.GetStatus() == orkstackv1.Status_STATUS_AWAITING {
						return true
					}
				}
				return terminal(run)
			})
			return runID
		}
		finish := func(runID string) *orkstackv1.Run {
			run := x.await(runID, terminal)
			x.note("run %s: %s", runID, summarizeRun(run))
			return run
		}

		// (a) a low-risk change needs no approval.
		mark := len(x.services.Notifications(t, 0))
		run := x.runToEnd("approval-gate", "--input", "notify_url="+base+"/notify")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		requireTaskStatus(t, run, "approval", orkstackv1.Status_STATUS_SKIPPED)
		requireTaskStatus(t, run, "apply", orkstackv1.Status_STATUS_SUCCEEDED)
		if notes := x.services.Notifications(t, mark); len(notes) != 1 || notes[0].Text != "applied: web: replicas = 3" {
			t.Fatalf("(a) notifications = %+v", notes)
		}

		// (b) a risky change, declined: a completion, so the run succeeds.
		mark = len(x.services.Notifications(t, 0))
		runID := awaitApproval("--input-json", payments)
		x.in("run", "resolve", runID, "--task", "approval", "--values", `{"approved":false,"approver":"dana"}`)
		run = finish(runID)
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		requireTaskStatus(t, run, "apply", orkstackv1.Status_STATUS_SKIPPED)
		if notes := x.services.Notifications(t, mark); len(notes) != 1 || notes[0].Text != "declined: payments: limit = 10 (approver: dana)" {
			t.Fatalf("(b) notifications = %+v", notes)
		}

		// (c) approved.
		mark = len(x.services.Notifications(t, 0))
		runID = awaitApproval("--input-json", payments)
		x.in("run", "resolve", runID, "--task", "approval", "--values", `{"approved":true,"approver":"dana"}`)
		run = finish(runID)
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		requireTaskStatus(t, run, "apply", orkstackv1.Status_STATUS_SUCCEEDED)
		if notes := x.services.Notifications(t, mark); len(notes) != 1 || notes[0].Text != "applied: payments: limit = 10 (approver: dana)" {
			t.Fatalf("(c) notifications = %+v", notes)
		}

		// (d) rejected: a failure, which the failure handler reports.
		mark = len(x.services.Notifications(t, 0))
		runID = awaitApproval("--input-json", payments)
		x.in("run", "resolve", runID, "--task", "approval", "--reject", "--reason", "not during the freeze")
		run = finish(runID)
		if code := requireRunFailedAt(t, run, "approval").GetCode(); code != "AWAIT_REJECTED" {
			t.Fatalf("approval failed with %s, want AWAIT_REJECTED", code)
		}
		requireTaskStatus(t, run, "tell-requester", orkstackv1.Status_STATUS_SUCCEEDED)
		if got := taskRunOf(t, run, "announce").GetStatus(); got == orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatal("announce succeeded in a rejected run")
		}
		if notes := x.services.Notifications(t, mark); len(notes) != 1 || notes[0].Text != "not applied: the approval was rejected or expired (payments: limit = 10)" {
			t.Fatalf("(d) notifications = %+v", notes)
		}
		x.note("(a) applied, (b) declined, (c) applied by dana, (d) rejected and the requester told")
	})

	x.scenario(t, "E7", func(t *testing.T) {
		x.services.SetNotReady(2)
		x.apply("examples/poll-until-ready.ork.yaml")
		run := x.runToEnd("poll-until-ready", "--input", "status_url="+base+"/exports/status",
			"--input", "export_url="+base+"/exports/latest.json")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		var resp orkstackv1.ListTaskAttemptsResponse
		if err := api.UnmarshalJSON([]byte(x.in("--json", "run", "attempts", run.GetId(), "--task", "wait-ready")), &resp); err != nil {
			t.Fatalf("decode run attempts: %v", err)
		}
		var outcomes []orkstackv1.AttemptOutcome
		for _, a := range resp.GetAttempts() {
			outcomes = append(outcomes, a.GetOutcome())
		}
		wantOutcomes := []orkstackv1.AttemptOutcome{
			orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
			orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
			orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		}
		if !reflect.DeepEqual(outcomes, wantOutcomes) {
			t.Fatalf("wait-ready attempts = %v, want two failed polls and one that saw ready", outcomes)
		}
		// 04:04 is Berlin time in January: the task's env: {TZ: Europe/Berlin}
		// reached jq.
		if got, want := taskOutput(t, run, "summarize"), mustJSON(t, `{"result":{"rows":2,"total":12.5,"generated_local":"2026-01-02 04:04"}}`); !reflect.DeepEqual(got, want) {
			t.Fatalf("summarize = %v, want %v", got, want)
		}
		x.note("wait-ready: %v; summarize: %s", outcomes, asJSON(t, taskOutput(t, run, "summarize")))
	})

	x.scenario(t, "E12", func(t *testing.T) {
		for _, file := range []string{"file-read", "shell-eval", "http-post", "pinned-build"} {
			x.apply("examples/" + file + ".ork.yaml")
		}
		run := x.runToEnd("file-read")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)

		run = x.runToEnd("shell-eval")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		if got := string(x.download(run.GetId(), "report", "report.txt", -1)); got != "text=orkstack bytes=8 probe_exit=1\n" {
			t.Fatalf("shell-eval report.txt = %q", got)
		}

		run = x.runToEnd("http-post", "--input", "url="+base+"/post")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		requireTaskStatus(t, run, "summarize", orkstackv1.Status_STATUS_SUCCEEDED)

		// The body is a YAML mapping, so a quote, a backslash and a newline
		// in the message stay inside its string: the bytes sent are exactly
		// the compact JSON of the rendered mapping.
		posted := len(x.services.Requests("/post"))
		run = x.runToEnd("http-post", "--input-json", `{"url":"`+base+`/post","message":"say \"hi\" \\ bye\nnext"}`)
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		requireTaskStatus(t, run, "summarize", orkstackv1.Status_STATUS_SUCCEEDED)
		requests := x.services.Requests("/post")
		if len(requests) != posted+1 {
			t.Fatalf("POST /post requests = %d, want %d", len(requests), posted+1)
		}
		received := requests[len(requests)-1]
		if want := `{"message":"say \"hi\" \\ bye\nnext"}`; string(received.Body) != want || received.ContentType != "application/json" {
			t.Fatalf("http-post sent %q with Content-Type %q, want %q as application/json", received.Body, received.ContentType, want)
		}
		x.note("http-post with a quote, a backslash and a newline sent %s (Content-Type %s)", received.Body, received.ContentType)

		run = x.runToEnd("pinned-build")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		if got := string(x.download(run.GetId(), "read", "version.txt", -1)); got != "release: 1.4.2\nhost-local handoff: this file never leaves the pin\n" {
			t.Fatalf("pinned-build version.txt = %q", got)
		}
	})
}

func TestIntegration_GitExamplesRunAsShipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	x := newExamplesStack(ctx, t, examplesOptions{git: true})
	repo := x.git.URL("private/examples.git")
	first := x.git.AddRepository(t, "private/examples.git", examplesRepoFiles)

	variant := func(file string) string {
		shipped, err := os.ReadFile(filepath.Join(x.root, "examples", file))
		if err != nil {
			t.Fatal(err)
		}
		withCA := withExampleGitCA(t, shipped)
		requireOnlyGitCAAdded(t, shipped, withCA)
		return x.variantFile(file, withCA)
	}

	// E9 runs first: E8's third run pushes a commit, and E9 reads VERSION at
	// the seeded one.
	x.scenario(t, "E9", func(t *testing.T) {
		x.apply(variant("pinned-checkout.ork.yaml"))
		run := x.runToEnd("pinned-checkout", "--input", "repo="+repo, "--input", "environment=prod")
		requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
		checkout := taskRunOf(t, run, "checkout")
		if checkout.GetPin() == "" || checkout.GetOrkerId() == "" {
			t.Fatalf("checkout carries pin %q and orker %q", checkout.GetPin(), checkout.GetOrkerId())
		}
		for _, id := range []string{"validate", "merge", "package"} {
			member := taskRunOf(t, run, id)
			if member.GetPin() != checkout.GetPin() || member.GetOrkerId() != checkout.GetOrkerId() {
				t.Fatalf("%s ran in pin %q on %q, want checkout's %q on %q", id, member.GetPin(), member.GetOrkerId(), checkout.GetPin(), checkout.GetOrkerId())
			}
		}
		if got := taskOutput(t, run, "checkout")["path"]; got != "/orkstack/shared/src" {
			t.Fatalf("checkout path = %v", got)
		}
		bundle := x.download(run.GetId(), "package", "bundle.tar.gz", -1)
		members := untarGz(t, bundle)
		if len(members) != 3 || members["COMMIT"] != first+"\n" || members["VERSION"] != "1.4.2\n" {
			t.Fatalf("bundle.tar.gz members = %v, want COMMIT %s, VERSION 1.4.2 and config.json", members, first)
		}
		if got, want := mustJSON(t, members["config.json"]), mustJSON(t, `{"log":{"level":"info","format":"text"},"replicas":3}`); !reflect.DeepEqual(got, want) {
			t.Fatalf("config.json = %v, want %v", got, want)
		}
		if got := taskOutput(t, run, "checksum")["sha256"]; got != sha256Hex(bundle) {
			t.Fatalf("checksum sha256 = %v, want the downloaded bundle's %s", got, sha256Hex(bundle))
		}
		x.note("pin %s on %s; bundle %s", checkout.GetPin(), checkout.GetOrkerId(), sha256Hex(bundle))
	})

	x.scenario(t, "E8", func(t *testing.T) {
		x.apply(variant("config-from-git.ork.yaml"))
		input := []string{"--input", "repo=" + repo, "--input", "environment=prod"}
		cached := []string{"tmpl", "services", "prepare", "manifest"}
		outcomes := func(label string, run *orkstackv1.Run) {
			var parts []string
			for _, tr := range run.GetTaskRuns() {
				outcome := "uncached"
				if c := tr.GetCache(); c != nil {
					outcome = strings.TrimPrefix(c.GetOutcome().String(), "CACHE_OUTCOME_")
				}
				parts = append(parts, tr.GetTaskId()+"="+outcome)
				for _, item := range tr.GetItems() {
					parts = append(parts, fmt.Sprintf("%s[%d]=%s", tr.GetTaskId(), item.GetItem(),
						strings.TrimPrefix(item.GetCache().GetOutcome().String(), "CACHE_OUTCOME_")))
				}
			}
			x.note("%s cache outcomes: %s", label, strings.Join(parts, " "))
		}
		apiConf := func(commit string) string {
			return "# api, rendered from commit " + commit[:12] + "\nservice api {\n  listen   8080;\n  replicas 4;\n  env      prod;\n}\n"
		}

		// Run 1.
		run1 := x.runToEnd("config-from-git", input...)
		requireRunStatus(t, run1, orkstackv1.Status_STATUS_SUCCEEDED)
		if got := taskOutput(t, run1, "resolve"); got["commit"] != first || got["ref"] != "refs/heads/main" {
			t.Fatalf("resolve = %v, want commit %s ref refs/heads/main", got, first)
		}
		if items := taskRunOf(t, run1, "render").GetItems(); len(items) != 2 {
			t.Fatalf("render has %d items, want 2", len(items))
		}
		manifest, _ := taskOutput(t, run1, "manifest")["result"].([]any)
		var files []string
		for _, entry := range manifest {
			m, _ := entry.(map[string]any)
			if sha, _ := m["sha256"].(string); len(sha) != 64 {
				t.Fatalf("manifest entry %v has no sha256", m)
			}
			files = append(files, fmt.Sprint(m["file"]))
		}
		if !reflect.DeepEqual(files, []string{"api.conf", "web.conf"}) {
			t.Fatalf("manifest files = %v, want api.conf and web.conf", files)
		}
		if got := string(x.download(run1.GetId(), "render", "api.conf", 0)); got != apiConf(first) {
			t.Fatalf("api.conf = %q, want %q", got, apiConf(first))
		}
		outcomes("run 1", run1)

		// Run 2, same commit: the fetches run again, everything after them hits.
		before := x.git.AuthenticatedRequests("private/examples.git")
		run2 := x.runToEnd("config-from-git", input...)
		requireRunStatus(t, run2, orkstackv1.Status_STATUS_SUCCEEDED)
		for _, id := range []string{"resolve", "clone"} {
			requireTaskStatus(t, run2, id, orkstackv1.Status_STATUS_SUCCEEDED)
			if c := taskRunOf(t, run2, id).GetCache(); c != nil {
				t.Fatalf("run 2's %s carries a cache result %v: a task with a secret is never cached", id, c)
			}
		}
		if x.git.AuthenticatedRequests("private/examples.git") <= before {
			t.Fatal("run 2 made no authenticated request: the token was not exercised")
		}
		for _, id := range cached {
			requireCacheOutcome(t, run2, id, orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT)
			if src := taskRunOf(t, run2, id).GetCache().GetSource().GetRunId(); src != run1.GetId() {
				t.Fatalf("run 2's %s hit a memo of %s, want run 1 (%s)", id, src, run1.GetId())
			}
		}
		for _, item := range taskRunOf(t, run2, "render").GetItems() {
			if c := item.GetCache(); c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT || c.GetSource().GetRunId() != run1.GetId() {
				t.Fatalf("run 2's render item %d cache = %v, want a hit of run 1", item.GetItem(), c)
			}
		}
		outcomes("run 2", run2)

		// Run 3, after a push changing only VERSION: a new commit, so the
		// derived files miss and name it.
		second := x.git.PushCommit(t, "private/examples.git", map[string]string{"VERSION": "1.4.3\n"})
		run3 := x.runToEnd("config-from-git", input...)
		requireRunStatus(t, run3, orkstackv1.Status_STATUS_SUCCEEDED)
		if got := taskOutput(t, run3, "resolve")["commit"]; got != second {
			t.Fatalf("run 3 resolved %v, want the pushed commit %s", got, second)
		}
		requireCacheOutcome(t, run3, "prepare", orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS)
		for _, item := range taskRunOf(t, run3, "render").GetItems() {
			if got := item.GetCache().GetOutcome(); got != orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS {
				t.Fatalf("run 3's render item %d cache = %s, want MISS", item.GetItem(), got)
			}
		}
		if got := string(x.download(run3.GetId(), "render", "api.conf", 0)); got != apiConf(second) {
			t.Fatalf("run 3 api.conf = %q, want %q", got, apiConf(second))
		}
		outcomes("run 3", run3)
	})
}

func untarGz(t *testing.T, body []byte) map[string]string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	reader := tar.NewReader(gz)
	members := map[string]string{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return members
		}
		if err != nil {
			t.Fatalf("untar: %v", err)
		}
		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read %s: %v", header.Name, err)
		}
		members[strings.TrimPrefix(header.Name, "./")] = string(content)
	}
}

func TestIntegration_CustomImageExampleRunsAsShipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	x := newExamplesStack(ctx, t, examplesOptions{})
	defer x.finish(t, "E10")
	refs := ensureExampleImages(ctx, t, x.orkctlPath, x.root)
	x.note("orkctl activity build pinned %v", refs)
	if len(refs) != 2 {
		t.Fatalf("pinned references = %v, want both images", refs)
	}
	for _, ref := range refs {
		out := x.in("activity", "inspect", ref)
		if !strings.Contains(out, "schema 1 · 3 keys") {
			t.Fatalf("activity inspect %s does not head its interface `schema 1 · 3 keys`:\n%s", ref, out)
		}
		for _, field := range []string{"label", "src", "as"} {
			if !strings.Contains(out, field) {
				t.Fatalf("activity inspect %s does not list %s:\n%s", ref, field, out)
			}
		}
	}

	document := x.variantFile("custom-image.ork.yaml", customImageDocument(t, x.root))
	x.apply(document)
	run := x.runToEnd("custom-image")
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	report := taskOutput(t, run, "report")
	if report["label"] != "no sdk required" {
		t.Fatalf("report label = %v", report["label"])
	}
	if got := asJSON(t, report["argv"]); got != `["{{ .inputs.label }}","is-not-rendered"]` {
		t.Fatalf("report argv = %s: command: must reach the image unrendered", got)
	}
	if want := x.ns + "/" + run.GetId() + "/report"; report["idempotencyKey"] != want {
		t.Fatalf("report idempotencyKey = %v, want %s", report["idempotencyKey"], want)
	}
	if _, ok := report["stats"].(map[string]any); !ok {
		t.Fatalf("report has no stats: %v", report)
	}
	goReport := x.download(run.GetId(), "report", "report.txt", -1)
	shReport := x.download(run.GetId(), "report-shell", "report-shell.txt", -1)
	if !bytes.Equal(goReport, shReport) {
		t.Fatalf("the two images' reports differ:\n--- go\n%s--- shell\n%s", goReport, shReport)
	}
	x.note("report.txt and report-shell.txt are byte-identical, sha256 %s", sha256Hex(goReport))
	if items := taskRunOf(t, run, "per-label").GetItems(); len(items) != 3 {
		t.Fatalf("per-label has %d items, want 3", len(items))
	}
	var want []any
	for i, label := range []string{"alpha", "beta", "gamma"} {
		want = append(want, map[string]any{"index": float64(i), "label": label,
			"idempotencyKey": fmt.Sprintf("%s/%s/per-label/%d", x.ns, run.GetId(), i)})
	}
	if got := taskOutput(t, run, "summary")["result"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("summary = %v, want %v", got, want)
	}
	requireTaskStatus(t, run, "collate", orkstackv1.Status_STATUS_SUCCEEDED)

	stdout, stderr, code := x.taskRun("--json", "--file", document, "--task", "report",
		"--output", `probe={"idempotencyKey":"x","attempt":1,"taskId":"probe","argv":[]}`)
	if code != 0 {
		t.Fatalf("task run report exit %d\n%s\n%s", code, stdout, stderr)
	}
	if got := taskRunValues(t, stdout)["label"]; got != "no sdk required" {
		t.Fatalf("task run report label = %v", got)
	}
}

func TestIntegration_AutomationExampleAppliesAndFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	x := newExamplesStack(ctx, t, examplesOptions{})
	defer x.finish(t, "E11")
	base := x.services.base
	x.apply("examples/notify-block.ork.yaml")

	shipped, err := os.ReadFile(filepath.Join(x.root, "examples", "automation", "examples.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	variant := x.variantFile("examples.ork.yaml", automationVariant(t, shipped, x.ns, base))
	out := x.in("--json", "apply", "examples/poll-until-ready.ork.yaml", "examples/approval-gate.ork.yaml", variant)
	var report orkstackv1.ApplyReport
	if err := api.UnmarshalJSON([]byte(out), &report); err != nil {
		t.Fatalf("decode ApplyReport: %v\n%s", err, out)
	}
	triggerID, token := disclosed(t, &report, "change-from-ci")
	scheduleID := ""
	for _, doc := range report.GetAutomation() {
		for _, result := range doc.GetResults() {
			if step := doc.GetPlan().GetSteps()[result.GetStep()]; step.GetName() == "poll-exports" {
				scheduleID = result.GetId()
			}
		}
	}
	if scheduleID == "" {
		t.Fatalf("the apply created no schedule poll-exports:\n%s", out)
	}
	x.mustRun(0, "apply", variant, "--check")

	// One fire of the schedule, as if its time had come.
	x.in("schedule", "fire", "poll-exports")
	var scheduled string
	deadline := time.Now().Add(2 * time.Minute)
	for scheduled == "" && time.Now().Before(deadline) {
		if ids := listedIDs(t, x.in("--json", "run", "list", "--filter", "origin.kind=SCHEDULE", "--filter", "origin.id="+scheduleID)); len(ids) == 1 {
			scheduled = ids[0]
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if scheduled == "" {
		t.Fatalf("no run listed under --filter origin.kind=SCHEDULE --filter origin.id=%s", scheduleID)
	}
	run := x.await(scheduled, terminal)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	if got := run.GetOrigin().GetKind(); got != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE {
		t.Fatalf("scheduled run origin = %s", got)
	}
	x.note("scheduled run %s: origin %s, %s", scheduled, run.GetOrigin().GetKind(), summarizeRun(run))

	// One invoke of the trigger, as CI would.
	mark := len(x.services.Notifications(t, 0))
	url := "http://" + x.replicas[0].addr + strings.Replace(api.TriggerInvokePath, "{id}", triggerID, 1)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(`{"notify_url":"`+base+`/notify"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	fmt.Fprintf(&x.transcript, "$ curl -X POST %s -H 'Authorization: Bearer <token>' -d '{\"notify_url\":\"%s/notify\"}'\n%s(HTTP %d)\n\n",
		url, base, body, response.StatusCode)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("trigger invoke answered %d: %s", response.StatusCode, body)
	}
	var submitted orkstackv1.SubmitRunResponse
	if err := api.UnmarshalJSON(body, &submitted); err != nil {
		t.Fatalf("decode the trigger's response: %v\n%s", err, body)
	}
	run = x.await(submitted.GetRun().GetId(), terminal)
	requireRunStatus(t, run, orkstackv1.Status_STATUS_SUCCEEDED)
	if got := run.GetOrigin().GetKind(); got != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER {
		t.Fatalf("triggered run origin = %s", got)
	}
	if notes := x.services.Notifications(t, mark); len(notes) != 1 || notes[0].Text != "applied: web: replicas = 3" {
		t.Fatalf("notifications = %+v", notes)
	}
	x.note("triggered run %s: origin %s, notification applied: web: replicas = 3", run.GetId(), run.GetOrigin().GetKind())
}
