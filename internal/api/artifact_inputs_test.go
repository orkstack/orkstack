package api_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// requireArtifactInputRefusal: a run of an Orkflow with an artifact input is
// started only by a composing task, so every other start is refused by
// name, as a failed precondition.
func requireArtifactInputRefusal(t *testing.T, what string, err error) {
	t.Helper()
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "ARTIFACT_INPUT_UNSUPPORTED: orkflow train declares artifact input dataset") ||
		!strings.Contains(err.Error(), "only a composed task (uses: orkflow:) passes artifacts") {
		t.Fatalf("%s = %v; want failed_precondition ARTIFACT_INPUT_UNSUPPORTED", what, err)
	}
}

func TestSubmit_ArtifactInputRefusedBeforeMint(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, artifactChildYAML)
	_, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		Namespace: "team-a", OrkflowName: "train"}))
	requireArtifactInputRefusal(t, "SubmitRun", err)
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a refused submission started %v", started)
	}
}

func TestCreateSchedule_ArtifactInputRefused(t *testing.T) {
	f := newComposeFixture(t, func(cfg *api.Config) { cfg.Schedules = newFakeSchedules() })
	f.mustApply(t, artifactChildYAML)
	_, err := f.svc.CreateSchedule(context.Background(), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Namespace: "team-a", Name: "nightly", OrkflowName: "train",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}},
	}))
	requireArtifactInputRefusal(t, "CreateSchedule", err)
}

func TestCreateTrigger_ArtifactInputRefused(t *testing.T) {
	f := newComposeFixture(t)
	f.mustApply(t, artifactChildYAML)
	_, err := f.triggerClient().CreateTrigger(context.Background(), connect.NewRequest(&orkstackv1.CreateTriggerRequest{
		Namespace: "team-a", OrkflowName: "train", Name: "hook",
	}))
	requireArtifactInputRefusal(t, "CreateTrigger", err)
}

// A version with an artifact input never becomes what a schedule or a
// trigger that follows current would start: neither by being created while
// no version is promoted, nor by being promoted.
func TestPromote_RefusedWhileScheduleFollowsCurrent(t *testing.T) {
	plain := strings.Replace(artifactChildYAML, "  dataset:\n    type: artifact\n    required: true\n", "", 1)
	plain = strings.Replace(plain, `        - '{{ artifact "_inputs/dataset" }}'`+"\n", "", 1)
	plain = strings.Replace(plain, `version: "1.0.0"`, `version: "0.9.0"`, 1)

	t.Run("created while a schedule follows current", func(t *testing.T) {
		f := newComposeFixture(t, func(cfg *api.Config) { cfg.Schedules = newFakeSchedules() })
		f.mustApply(t, plain)
		_, err := f.svc.CreateSchedule(context.Background(), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
			Namespace: "team-a", Name: "nightly", OrkflowName: "train",
			Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}},
		}))
		if err != nil {
			t.Fatalf("CreateSchedule: %v", err)
		}
		_, err = f.apply(t, artifactChildYAML)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
			!strings.Contains(err.Error(), "ARTIFACT_INPUT_UNSUPPORTED: version 1.0.0 of orkflow train declares artifact input dataset") ||
			!strings.Contains(err.Error(), "schedule nightly follow its current version") {
			t.Fatalf("apply = %v", err)
		}
	})

	t.Run("promoted while a trigger follows current", func(t *testing.T) {
		f := newComposeFixture(t)
		f.mustApply(t, plain)
		_, err := f.orkflowClient().SetCurrentOrkflowVersion(context.Background(), connect.NewRequest(
			&orkstackv1.SetCurrentOrkflowVersionRequest{Namespace: "team-a", Name: "train", Version: "0.9.0"}))
		if err != nil {
			t.Fatalf("promote 0.9.0: %v", err)
		}
		f.mustApply(t, artifactChildYAML) // current is promoted: 1.0.0 is not what "train" resolves to
		if _, err := f.triggerClient().CreateTrigger(context.Background(), connect.NewRequest(&orkstackv1.CreateTriggerRequest{
			Namespace: "team-a", OrkflowName: "train", Name: "hook"})); err != nil {
			t.Fatalf("CreateTrigger: %v", err)
		}
		_, err = f.orkflowClient().SetCurrentOrkflowVersion(context.Background(), connect.NewRequest(
			&orkstackv1.SetCurrentOrkflowVersionRequest{Namespace: "team-a", Name: "train", Version: "1.0.0",
				AcknowledgeBreaking: true}))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
			!strings.Contains(err.Error(), "trigger hook follow its current version") {
			t.Fatalf("promote 1.0.0 = %v", err)
		}
	})
}

// Every artifacts: binding of a tree reserves room in its plan for the
// reference it passes, so a start never outgrows the bound the submit held:
// the smallest child that no longer fits is refused naming the reserve.
func TestSubmit_ChildStartHeadroomReserved(t *testing.T) {
	submit := func(padBytes int) error {
		f := newComposeFixture(t)
		f.mustApply(t, strings.Replace(artifactChildYAML, "        - fit\n",
			"        - fit\n        - "+strings.Repeat("x", padBytes)+"\n", 1))
		f.mustApply(t, artifactParentYAML("    artifacts:\n      dataset: build/dataset.tar\n", ""))
		_, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
			Namespace: "team-a", OrkflowName: "release-model"}))
		return err
	}
	// The largest pad that submits, by bisection: fits submits, refused does not.
	fits, refused := engine.MaxPlanBytes-32*1024, engine.MaxPlanBytes
	if err := submit(fits); err != nil {
		t.Fatalf("a plan well inside the bound was refused: %v", err)
	}
	for refused-fits > 1 {
		mid := (fits + refused) / 2
		if submit(mid) == nil {
			fits = mid
		} else {
			refused = mid
		}
	}
	err := submit(refused)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "its 1 artifact bindings reserve 1 KiB for the references they pass") {
		t.Fatalf("SubmitRun = %v; want the reserve named", err)
	}
	// The control: the same tree with no binding, a few bytes smaller and
	// reserving nothing, still submits at that size, so the reserve is what
	// refused it.
	f := newComposeFixture(t)
	plain := strings.Replace(artifactChildYAML, "  dataset:\n    type: artifact\n    required: true\n", "", 1)
	plain = strings.Replace(plain, `        - '{{ artifact "_inputs/dataset" }}'`+"\n", "", 1)
	f.mustApply(t, strings.Replace(plain, "        - fit\n",
		"        - fit\n        - "+strings.Repeat("x", refused)+"\n", 1))
	f.mustApply(t, artifactParentYAML("", ""))
	if _, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		Namespace: "team-a", OrkflowName: "release-model"})); err != nil {
		t.Fatalf("the same tree without a binding was refused too, so the reserve decided nothing: %v", err)
	}
}

// An update re-points a schedule or a trigger no more than a create may: not
// at a version with an artifact input, pinned or followed as current.
func TestUpdate_ArtifactInputRefused(t *testing.T) {
	plain := strings.Replace(artifactChildYAML, "  dataset:\n    type: artifact\n    required: true\n", "", 1)
	plain = strings.Replace(plain, `        - '{{ artifact "_inputs/dataset" }}'`+"\n", "", 1)
	plain = strings.Replace(plain, `version: "1.0.0"`, `version: "0.9.0"`, 1)
	f := newComposeFixture(t, func(cfg *api.Config) { cfg.Schedules = newFakeSchedules() })
	f.mustApply(t, plain)
	if _, err := f.orkflowClient().SetCurrentOrkflowVersion(context.Background(), connect.NewRequest(
		&orkstackv1.SetCurrentOrkflowVersionRequest{Namespace: "team-a", Name: "train", Version: "0.9.0"})); err != nil {
		t.Fatalf("promote 0.9.0: %v", err)
	}
	f.mustApply(t, artifactChildYAML)
	if _, err := f.svc.CreateSchedule(context.Background(), connect.NewRequest(&orkstackv1.CreateScheduleRequest{
		Namespace: "team-a", Name: "nightly", OrkflowName: "train", OrkflowVersion: "0.9.0",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}},
	})); err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}
	_, err := f.svc.UpdateSchedule(context.Background(), connect.NewRequest(&orkstackv1.UpdateScheduleRequest{
		Namespace: "team-a", Name: "nightly", OrkflowVersion: "1.0.0",
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}},
	}))
	requireArtifactInputRefusal(t, "UpdateSchedule", err)

	created, err := f.triggerClient().CreateTrigger(context.Background(), connect.NewRequest(&orkstackv1.CreateTriggerRequest{
		Namespace: "team-a", OrkflowName: "train", OrkflowVersion: "0.9.0", Name: "hook"}))
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	_, err = f.triggerClient().UpdateTrigger(context.Background(), connect.NewRequest(&orkstackv1.UpdateTriggerRequest{
		Id: created.Msg.GetTrigger().GetId(), Namespace: "team-a", Name: "hook", OrkflowVersion: "1.0.0"}))
	requireArtifactInputRefusal(t, "UpdateTrigger", err)
}
