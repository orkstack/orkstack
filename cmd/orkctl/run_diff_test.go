package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The run-diff goldens pin what both clients print: the UI reproduces these
// strings from the same response (ui/src/lib/runDiff.ts), so a change here is
// a change to the parity contract, not to one client's taste.

const (
	cliDigestMon   = "sha256:9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c"
	cliDigestTue   = "sha256:aa10aa10aa10aa10aa10aa10aa10aa10aa10aa10aa10aa10aa10aa10aa10aa10"
	cliImageOld    = "ghcr.io/acme/transform@sha256:ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12ab12"
	cliImageNew    = "ghcr.io/acme/transform@sha256:ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90ff90"
	cliImageShell  = "registry.local/orkstack/shell@sha256:5e115e115e115e115e115e115e115e115e115e115e115e115e115e115e115e11"
	cliPlatformDig = "sha256:c0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff"
)

type diffCLIFixture struct {
	*fixture
	orkflows *engine.MemoryOrkflowStore
	attempts *cliAttemptStore
}

func newDiffCLIFixture(t *testing.T) *diffCLIFixture {
	t.Helper()
	orkflows := engine.NewMemoryOrkflowStore()
	attempts := newCLIAttemptStore()
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Orkflows = orkflows
		cfg.Attempts = attempts
	})
	return &diffCLIFixture{fixture: f, orkflows: orkflows, attempts: attempts}
}

func (f *diffCLIFixture) define(t *testing.T, namespace, name, version, digest string, images map[string]string) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Namespace: namespace, Name: name, Version: version, DefinitionDigest: digest, SourceYaml: name + "@" + version}
	for _, id := range []string{"extract", "transform", "load", "publish", "a", "b", "c", "d"} {
		if image, ok := images[id]; ok {
			wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: id, Image: &orkstackv1.ImageRef{Pinned: image}})
		}
	}
	if _, _, err := f.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("store definition: %v", err)
	}
}

// seed puts the run in the read model and files an outcome record for every
// task that reached an Orker, except those named in missing.
func (f *diffCLIFixture) seed(t *testing.T, run *orkstackv1.Run, missing ...string) {
	t.Helper()
	f.runs.Apply(run)
	for _, task := range run.GetTaskRuns() {
		if task.GetOrkerId() == "" || task.GetAttempt() == 0 || contains(missing, task.GetTaskId()) {
			continue
		}
		if err := f.attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
			SchemaVersion: 1, RunId: run.GetId(), TaskId: task.GetTaskId(), Attempt: task.GetAttempt(),
			Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, OrkerId: task.GetOrkerId(),
			Image: &orkstackv1.ImageRef{PlatformDigest: cliPlatformDig},
		}); err != nil {
			t.Fatalf("put attempt: %v", err)
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func cliDiffTask(id string, edit ...func(*orkstackv1.TaskRun)) *orkstackv1.TaskRun {
	task := &orkstackv1.TaskRun{
		TaskId: id, Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, Pool: "main", OrkerId: "orker-a1",
		StartedAt:  timestamppb.New(fixedNow),
		FinishedAt: timestamppb.New(fixedNow.Add(63 * time.Second)),
	}
	for _, e := range edit {
		e(task)
	}
	return task
}

func cliDiffRun(t *testing.T, id, namespace, name, version, digest, date string, day int, tasks ...*orkstackv1.TaskRun) *orkstackv1.Run {
	t.Helper()
	input, err := structpb.NewStruct(map[string]any{"date": date, "limit": 500})
	if err != nil {
		t.Fatal(err)
	}
	at := fixedNow.Add(time.Duration(day) * 24 * time.Hour)
	return &orkstackv1.Run{
		Id: id, Namespace: namespace, OrkflowName: name, OrkflowVersion: version, OrkflowDefinitionDigest: digest,
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		Input:     &orkstackv1.Payload{Data: input},
		CreatedAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(8 * time.Minute)),
		TaskRuns: tasks,
	}
}

// seedChangedPair is C1's fixture: nightly-etl@1.2.0 on Monday succeeded;
// 1.2.1 on Tuesday, with a moved transform tag, a new date and a gpu pool,
// failed. Monday's load record was reclaimed.
func seedChangedPair(t *testing.T, f *diffCLIFixture) {
	t.Helper()
	f.define(t, "", "nightly-etl", "1.2.0", cliDigestMon, map[string]string{
		"extract": cliImageShell, "transform": cliImageOld, "load": cliImageShell, "publish": cliImageShell,
	})
	f.define(t, "", "nightly-etl", "1.2.1", cliDigestTue, map[string]string{
		"extract": cliImageShell, "transform": cliImageNew, "load": cliImageShell, "publish": cliImageShell,
	})
	f.seed(t, cliDiffRun(t, "ork-mon", "", "nightly-etl", "1.2.0", cliDigestMon, "2026-09-21", 0,
		cliDiffTask("extract"), cliDiffTask("transform"), cliDiffTask("load"), cliDiffTask("publish"),
	), "load")
	tue := cliDiffRun(t, "ork-tue", "", "nightly-etl", "1.2.1", cliDigestTue, "2026-09-22", 1,
		cliDiffTask("extract", func(task *orkstackv1.TaskRun) {
			task.Pool = "gpu"
			task.FinishedAt = timestamppb.New(fixedNow.Add(4*time.Minute + 12*time.Second))
		}),
		cliDiffTask("transform", func(task *orkstackv1.TaskRun) {
			task.Status = orkstackv1.Status_STATUS_FAILED
			task.Attempt = 3
			task.ExitCode = 3
			task.Error = &orkstackv1.Error{
				Code: "CONTAINER_EXIT_NONZERO", Message: "container exited 3",
				Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
			}
		}),
		cliDiffTask("load"),
		cliDiffTask("publish"),
	)
	tue.Status = orkstackv1.Status_STATUS_FAILED
	f.seed(t, tue)
}

// C1
func TestGoldenRunDiff_HumanOutput(t *testing.T) {
	f := newDiffCLIFixture(t)
	seedChangedPair(t, f)
	out := f.run("run", "diff", "ork-mon", "ork-tue").requireExit(t, exitOK)
	requireGolden(t, "run-diff.golden", out.stdout)
	for _, want := range []string{
		"Definition:  differs: nightly-etl@1.2.0 " + cliDigestMon + " -> nightly-etl@1.2.1 " + cliDigestTue,
		"Input:       differs (2 lines)",
		"load       platform_digest  unavailable",
		"load platform_digest in ork-mon: attempt_record_missing (no record of the outcome attempt or of a retry that adopted its commit: reclaimed, or the Orker never filed one)",
	} {
		if !strings.Contains(out.stdout, want) {
			t.Errorf("output lacks %q:\n%s", want, out.stdout)
		}
	}
}

// C2
func TestGoldenRunDiff_IdenticalRuns(t *testing.T) {
	f := newDiffCLIFixture(t)
	f.define(t, "", "nightly-etl", "1.2.0", cliDigestMon, map[string]string{
		"extract": cliImageShell, "transform": cliImageOld, "load": cliImageShell, "publish": cliImageShell,
	})
	for i, id := range []string{"ork-mon", "ork-tue"} {
		f.seed(t, cliDiffRun(t, id, "", "nightly-etl", "1.2.0", cliDigestMon, "2026-09-21", i,
			cliDiffTask("extract"), cliDiffTask("transform"), cliDiffTask("load"), cliDiffTask("publish")))
	}
	out := f.run("run", "diff", "ork-mon", "ork-tue").requireExit(t, exitOK)
	requireGolden(t, "run-diff-identical.golden", out.stdout)
	for _, want := range []string{
		"Definition:  identical (" + cliDigestMon + ")",
		"Input:       identical",
		"No task differences.",
		"0 of 4 tasks differ; 4 identical.",
	} {
		if !strings.Contains(out.stdout, want) {
			t.Errorf("output lacks %q:\n%s", want, out.stdout)
		}
	}
}

// C3
func TestGoldenRunDiff_JSON(t *testing.T) {
	f := newDiffCLIFixture(t)
	seedChangedPair(t, f)
	out := f.run("run", "diff", "ork-mon", "ork-tue", "--json").requireExit(t, exitOK)
	requireGolden(t, "run-diff.json.golden", out.stdout)

	var envelope struct {
		From struct {
			Status string `json:"status"`
		} `json:"from"`
		Differences []map[string]any `json:"differences"`
	}
	if err := json.Unmarshal([]byte(out.stdout), &envelope); err != nil {
		t.Fatalf("--json is not one JSON document: %v\n%s", err, out.stdout)
	}
	if envelope.From.Status != "SUCCEEDED" {
		t.Errorf("from.status = %q, want the short dialect SUCCEEDED", envelope.From.Status)
	}
	for _, row := range envelope.Differences {
		_, hasReason := row["fromReason"]
		available, _ := row["fromAvailable"].(bool)
		if hasReason == available {
			t.Errorf("row %v: fromReason present=%v with fromAvailable=%v; a reason belongs to an unavailable side only", row, hasReason, available)
		}
	}
}

// C4
func TestRunDiff_OnlyNarrowsAndUnknownFieldExitsNonZero(t *testing.T) {
	f := newDiffCLIFixture(t)
	seedChangedPair(t, f)
	joined := f.run("run", "diff", "ork-mon", "ork-tue", "--only", "image,pool").requireExit(t, exitOK)
	requireGolden(t, "run-diff-only.golden", joined.stdout)
	if !strings.Contains(joined.stdout, "Compared:    presence, pool, image\n") {
		t.Errorf("a narrowed diff does not say it is one:\n%s", joined.stdout)
	}
	repeated := f.run("run", "diff", "ork-mon", "ork-tue", "--only", "image", "--only", "pool").requireExit(t, exitOK)
	if repeated.stdout != joined.stdout {
		t.Errorf("--only image --only pool and --only image,pool differ:\n%s\n---\n%s", repeated.stdout, joined.stdout)
	}
	unknown := f.run("run", "diff", "ork-mon", "ork-tue", "--only", "images")
	if unknown.code == exitOK {
		t.Fatalf("--only images exited 0:\n%s", unknown.stdout)
	}
	if !strings.Contains(unknown.stderr, "valid fields:") {
		t.Errorf("the refusal does not list the valid fields:\n%s", unknown.stderr)
	}
}

// C5
func TestRunDiff_SameRunIsRefused(t *testing.T) {
	f := newDiffCLIFixture(t)
	seedChangedPair(t, f)
	same := f.run("run", "diff", "ork-mon", "ork-mon")
	if same.code == exitOK {
		t.Fatalf("a run diffed against itself exited 0:\n%s", same.stdout)
	}
	if !strings.Contains(same.stderr, `from_run_id and to_run_id name the same run "ork-mon"`) {
		t.Errorf("stderr does not carry the server's message:\n%s", same.stderr)
	}
}

// C6
func TestRunDiff_AnotherNamespaceExitsNonZero(t *testing.T) {
	f := newDiffCLIFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.seed(t, cliDiffRun(t, "ork-pay", "payments", "nightly-etl", "1.2.0", cliDigestMon, "2026-09-21", 0, cliDiffTask("extract")))
	f.seed(t, cliDiffRun(t, "ork-default", "", "nightly-etl", "1.2.0", cliDigestMon, "2026-09-21", 0, cliDiffTask("extract")))
	res := f.run("-n", "payments", "run", "diff", "ork-pay", "ork-default")
	if res.code == exitOK {
		t.Fatalf("a run of another namespace was diffed:\n%s", res.stdout)
	}
	for _, want := range []string{"not_found", `"ork-default"`, `"payments"`} {
		if !strings.Contains(res.stderr, want) {
			t.Errorf("stderr lacks %q:\n%s", want, res.stderr)
		}
	}
}

// C7
func TestRunDiff_DifferentOrkflowsRenderThePresenceRows(t *testing.T) {
	f := newDiffCLIFixture(t)
	f.define(t, "", "etl", "1.0.0", cliDigestMon, map[string]string{"a": cliImageShell, "b": cliImageShell, "c": cliImageShell})
	f.define(t, "", "report", "1.0.0", cliDigestTue, map[string]string{"b": cliImageShell, "c": cliImageShell, "d": cliImageShell})
	f.seed(t, cliDiffRun(t, "ork-etl", "", "etl", "1.0.0", cliDigestMon, "2026-09-21", 0,
		cliDiffTask("a"), cliDiffTask("b"), cliDiffTask("c")))
	f.seed(t, cliDiffRun(t, "ork-report", "", "report", "1.0.0", cliDigestTue, "2026-09-21", 0,
		cliDiffTask("b", func(task *orkstackv1.TaskRun) { task.Attempt = 2 }), cliDiffTask("c"), cliDiffTask("d")))

	full := f.run("run", "diff", "ork-etl", "ork-report").requireExit(t, exitOK)
	requireGolden(t, "run-diff-orkflows.golden", full.stdout)
	if !strings.Contains(full.stdout, "Orkflow:     etl -> report (DIFFERENT ORKFLOWS)") {
		t.Errorf("the header does not flag the different orkflows:\n%s", full.stdout)
	}

	narrowed := f.run("run", "diff", "ork-etl", "ork-report", "--only", "image").requireExit(t, exitOK)
	requireGolden(t, "run-diff-orkflows-only.golden", narrowed.stdout)
	for _, want := range []string{
		"Compared:    presence, image\n",
		"a     presence  present  absent",
		"d     presence  absent   present",
		"2 of 4 tasks differ; 2 identical.",
	} {
		if !strings.Contains(narrowed.stdout, want) {
			t.Errorf("--only image output lacks %q:\n%s", want, narrowed.stdout)
		}
	}
}
