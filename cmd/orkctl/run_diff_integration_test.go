//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The run-diff integration scenarios (docs/UI_CLI_PARITY.md "Run diff"): two
// real `ork` replicas sharing
// one private Temporal namespace, one Orker and the MinIO bucket; every
// DiffRuns is asked of replica B, which started neither run, so no answer can
// have come from the process that watched the runs.

var sixtyFourHex = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// runDiffOrkflow's task `work` exits 3 when the run's input `fail` is true and
// 0 otherwise, writing values.json either way, and sleeps `sleep` seconds
// first. One attempt only: the diff is about the outcome, not the retries.
func runDiffOrkflow(name string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: run-diff probe.
inputs:
  fail:
    type: boolean
    default: false
  sleep:
    type: number
    default: 0
tasks:
  - id: work
    uses: shell
    timeout_seconds: 120
    retry:
      maximum_attempts: 1
    with:
      op: script
      interpreter: sh
      script: |
        sleep {{ .inputs.sleep }}
        printf '{"fail": "{{ .inputs.fail }}"}' > /orkstack/output/values.json
        if [ "{{ .inputs.fail }}" = "true" ]; then exit 3; fi
`, name)
}

// diffStack is a timing stack with two replicas and an Orker, the probe
// orkflow applied on A and visible on B.
func newDiffStack(ctx context.Context, t *testing.T) (*timingStack, string) {
	t.Helper()
	s := newTimingStack(ctx, t, 2)
	s.startOrker()
	name := fmt.Sprintf("run-diff-%d-%d", os.Getpid(), time.Now().UnixNano())
	s.apply(runDiffOrkflow(name))
	awaitOrkflowVisible(ctx, t, s.replicas[1].flows, name)
	return s, name
}

// submitOnA submits the probe with an input on replica A only.
func submitOnA(t *testing.T, s *timingStack, name string, input map[string]any) string {
	t.Helper()
	data, err := structpb.NewStruct(input)
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := s.replicas[0].runs.SubmitRun(s.ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: name, Input: &orkstackv1.Payload{Data: data},
	}))
	if err != nil {
		t.Fatalf("SubmitRun on replica A: %v", err)
	}
	return submitted.Msg.GetRun().GetId()
}

// finishedPair runs the probe twice on A — once succeeding, once failing — and
// waits for both to be terminal as replica B sees them.
func finishedPair(t *testing.T, s *timingStack, name string) (string, string) {
	t.Helper()
	ok := submitOnA(t, s, name, map[string]any{"fail": false})
	failed := submitOnA(t, s, name, map[string]any{"fail": true})
	if run := awaitTerminalOnB(s.ctx, t, s.replicas[1], ok); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s: %v", ok, run.GetStatus(), run.GetError())
	}
	if run := awaitTerminalOnB(s.ctx, t, s.replicas[1], failed); run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run %s finished %s, want FAILED: %v", failed, run.GetStatus(), run.GetError())
	}
	return ok, failed
}

// diffOnB asks replica B, which started neither run.
func diffOnB(t *testing.T, s *timingStack, from, to string, only ...string) *orkstackv1.DiffRunsResponse {
	t.Helper()
	res, err := s.replicas[1].runs.DiffRuns(s.ctx, connect.NewRequest(&orkstackv1.DiffRunsRequest{
		FromRunId: from, ToRunId: to, Only: only,
	}))
	if err != nil {
		t.Fatalf("DiffRuns(%s, %s, only=%v) on replica B: %v", from, to, only, err)
	}
	return res.Msg
}

// rowText is one row as the transcript and assertions read it.
func rowText(row *orkstackv1.RunFieldDiff) string {
	render := func(value string, available bool, reason string) string {
		if !available {
			return "unavailable(" + reason + ")"
		}
		return orDash(value)
	}
	return fmt.Sprintf("%s %s %s / %s", row.GetTaskId(), row.GetField(),
		render(row.GetFromValue(), row.GetFromAvailable(), row.GetFromReason()),
		render(row.GetToValue(), row.GetToAvailable(), row.GetToReason()))
}

func recordDiff(s *timingStack, label string, res *orkstackv1.DiffRunsResponse) {
	fmt.Fprintf(&s.transcript, "## DiffRuns on replica B: %s\nfields_compared=%v same_orkflow=%t same_definition_digest=%t tasks_compared=%d tasks_identical=%d evidence_unavailable=%d\n",
		label, res.GetFieldsCompared(), res.GetSameOrkflow(), res.GetSameDefinitionDigest(),
		res.GetTasksCompared(), res.GetTasksIdentical(), res.GetEvidenceUnavailable())
	for _, row := range res.GetDifferences() {
		fmt.Fprintf(&s.transcript, "  row %s\n", rowText(row))
	}
	if diff := res.GetInputUnifiedDiff(); diff != "" {
		fmt.Fprintf(&s.transcript, "input_unified_diff:\n%s", diff)
	}
	fmt.Fprintln(&s.transcript)
}

func requireRow(t *testing.T, res *orkstackv1.DiffRunsResponse, want string) {
	t.Helper()
	for _, row := range res.GetDifferences() {
		if rowText(row) == want {
			t.Logf("EVIDENCE row %s", want)
			return
		}
	}
	rows := make([]string, 0, len(res.GetDifferences()))
	for _, row := range res.GetDifferences() {
		rows = append(rows, rowText(row))
	}
	t.Fatalf("no row %q in:\n  %s", want, strings.Join(rows, "\n  "))
}

// outcomeRecords reads one task's records straight from the real store.
func outcomeRecords(t *testing.T, s *timingStack, runID, taskID string) []*orkstackv1.TaskAttempt {
	t.Helper()
	store, err := artifacts.NewAttemptStore(s.artifactCfg)
	if err != nil {
		t.Fatalf("open attempt store: %v", err)
	}
	records, err := store.ListAttempts(s.ctx, runID, taskID, artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts(%s, %s): %v", runID, taskID, err)
	}
	return records
}

// deleteAttemptRecord removes one record object from the bucket — the absence
// the cleanup worker and an Orker crash both leave (docs/STORAGE.md).
func deleteAttemptRecord(t *testing.T, s *timingStack, runID, taskID string, attempt int32) {
	t.Helper()
	key, err := artifacts.AttemptKey(artifacts.AttemptRef{RunID: runID, TaskID: taskID, Attempt: attempt})
	if err != nil {
		t.Fatalf("AttemptKey: %v", err)
	}
	cfg := s.artifactCfg
	awsCfg, err := awsconfig.LoadDefaultConfig(s.ctx,
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
	if _, err := client.DeleteObject(s.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
	}); err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
	fmt.Fprintf(&s.transcript, "deleted s3://%s/%s\n\n", cfg.S3Bucket, key)
}

// TestIntegration_RunDiffIsAnsweredByTheReplicaThatStartedNeitherRun.
func TestIntegration_RunDiffIsAnsweredByTheReplicaThatStartedNeitherRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s, name := newDiffStack(ctx, t)
	defer s.writeTranscript("run-diff-two-replicas.txt")
	run1, run2 := finishedPair(t, s, name)
	fmt.Fprintf(&s.transcript, "# run diff across two replicas; runs submitted on A (%s), diffed on B (%s)\nrun1=%s (fail=false) run2=%s (fail=true)\n\n",
		s.replicas[0].addr, s.replicas[1].addr, run1, run2)

	full := diffOnB(t, s, run1, run2)
	recordDiff(s, "full", full)
	if !full.GetSameOrkflow() || !full.GetSameDefinitionDigest() {
		t.Fatalf("same_orkflow=%t same_definition_digest=%t for two runs of one version", full.GetSameOrkflow(), full.GetSameDefinitionDigest())
	}
	diff := full.GetInputUnifiedDiff()
	if strings.Count(diff, "\n-  \"fail\": false") != 1 || strings.Count(diff, "\n+  \"fail\": true") != 1 {
		t.Fatalf("input diff does not carry one -/+ pair on fail:\n%s", diff)
	}
	requireRow(t, full, "work status SUCCEEDED / FAILED")
	requireRow(t, full, "work exit_code 0 / 3")
	requireRow(t, full, "work error - / CONTAINER_EXIT_NONZERO (ACTIVITY_FAILED)")
	for _, row := range full.GetDifferences() {
		if row.GetField() == "image" || row.GetField() == "platform_digest" {
			t.Fatalf("an %s row for one image run by one Orker: %s", row.GetField(), rowText(row))
		}
	}

	image := diffOnB(t, s, run1, run2, "image")
	recordDiff(s, "only image", image)
	if image.GetTasksIdentical() != 1 || len(image.GetDifferences()) != 0 {
		t.Fatalf("only=[image]: tasks_identical=%d rows=%d, want 1 and 0", image.GetTasksIdentical(), len(image.GetDifferences()))
	}
	platform := diffOnB(t, s, run1, run2, "platform_digest")
	recordDiff(s, "only platform_digest", platform)
	if len(platform.GetDifferences()) != 0 || platform.GetEvidenceUnavailable() != 0 {
		t.Fatalf("only=[platform_digest]: rows=%d evidence_unavailable=%d, want 0 and 0",
			len(platform.GetDifferences()), platform.GetEvidenceUnavailable())
	}
	records := outcomeRecords(t, s, run1, "work")
	if len(records) != 1 || !sixtyFourHex.MatchString(records[0].GetImage().GetPlatformDigest()) {
		t.Fatalf("run %s's outcome record carries no 64-hex platform_digest: %v", run1, records)
	}
	t.Logf("EVIDENCE I1 replica B answered: same_definition_digest=%t platform_digest=%s (equal on both sides, available)",
		full.GetSameDefinitionDigest(), records[0].GetImage().GetPlatformDigest())

	human := s.mustCLI(1, "run", "diff", run1, run2)
	if !strings.Contains(human, "work       status") && !strings.Contains(human, "work  status") {
		t.Errorf("orkctl run diff against B does not print the status row:\n%s", human)
	}

	// Another namespace: the ids exist, but not in it.
	other := fmt.Sprintf("diff-other-%d", time.Now().UnixNano()%1_000_000_000)
	s.mustCLI(1, "namespace", "create", other)
	// A defer, not t.Cleanup: it must run while the stack's context is live.
	defer func() { _, _, _ = s.cli(1, "namespace", "delete", other, "--yes") }()
	stdout, stderr, code := s.cli(1, "-n", other, "run", "diff", run1, run2)
	if code == 0 || !strings.Contains(stderr, "not_found") || !strings.Contains(stderr, other) {
		t.Fatalf("-n %s run diff exit=%d, want non-zero NotFound naming the namespace\nstdout:\n%s\nstderr:\n%s", other, code, stdout, stderr)
	}
	t.Logf("EVIDENCE I1 other namespace: %s", strings.TrimSpace(stderr))
}

// TestIntegration_RunDiffSurvivesReclaimedAttemptRecords.
//
// The definition-unavailable path stays a unit scenario: DeleteOrkflow
// refuses to delete a version a run used within its 24 h window, so a real
// stack cannot reach it here without waiting a day.
func TestIntegration_RunDiffSurvivesReclaimedAttemptRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s, name := newDiffStack(ctx, t)
	defer s.writeTranscript("run-diff-reclaimed.txt")
	run1, run2 := finishedPair(t, s, name)

	for _, record := range outcomeRecords(t, s, run1, "work") {
		deleteAttemptRecord(t, s, run1, "work", record.GetAttempt())
	}
	res := diffOnB(t, s, run1, run2)
	recordDiff(s, "run1's attempt records reclaimed", res)
	var digestRow *orkstackv1.RunFieldDiff
	for _, row := range res.GetDifferences() {
		switch row.GetField() {
		case "platform_digest":
			digestRow = row
		case "image":
			t.Fatalf("image went missing with the attempt records; it answers from the definition: %s", rowText(row))
		}
	}
	if digestRow == nil || digestRow.GetFromAvailable() || digestRow.GetFromReason() != "attempt_record_missing" || !digestRow.GetToAvailable() {
		t.Fatalf("platform_digest row = %v, want from unavailable (attempt_record_missing) and to available", digestRow)
	}
	if res.GetEvidenceUnavailable() != 1 {
		t.Fatalf("evidence_unavailable = %d, want 1", res.GetEvidenceUnavailable())
	}
	t.Logf("EVIDENCE I2 row %s; image equal and available from the definition; evidence_unavailable=%d",
		rowText(digestRow), res.GetEvidenceUnavailable())
	s.mustCLI(1, "run", "diff", run1, run2)
}

// TestIntegration_RunDiffOfALiveRun.
func TestIntegration_RunDiffOfALiveRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s, name := newDiffStack(ctx, t)
	defer s.writeTranscript("run-diff-live.txt")
	done := submitOnA(t, s, name, map[string]any{})
	if run := awaitTerminalOnB(ctx, t, s.replicas[1], done); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s finished %s", done, run.GetStatus())
	}
	live := submitOnA(t, s, name, map[string]any{"sleep": 20})

	// The task is running when the snapshot says so; the cluster's own status
	// confirms the run is live when the diff is asked.
	deadline := time.Now().Add(90 * time.Second)
	for {
		got, err := s.replicas[1].runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: live}))
		if err == nil && len(got.Msg.GetRun().GetTaskRuns()) > 0 &&
			got.Msg.GetRun().GetTaskRuns()[0].GetStatus() == orkstackv1.Status_STATUS_RUNNING {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s's task never reported RUNNING: %v", live, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	res := diffOnB(t, s, live, done)
	if status := executionStatus(ctx, t, s.temporal, live); status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the cluster reports %s as %s; the diff was not of a live run", live, status)
	}
	recordDiff(s, "live vs finished", res)
	requireRow(t, res, "work status RUNNING / SUCCEEDED")
	var duration *orkstackv1.RunFieldDiff
	for _, row := range res.GetDifferences() {
		if row.GetField() == "duration" {
			duration = row
		}
	}
	if duration == nil || !duration.GetFromAvailable() || duration.GetFromValue() != "" || !duration.GetToAvailable() || duration.GetToValue() == "" {
		t.Fatalf("duration row = %v, want - / a span", duration)
	}
	t.Logf("EVIDENCE I3 %s", rowText(duration))
}

// adoptionProbeWorkflow gives its one activity a real attempt 2: attempt 1
// stands in for the committing attempt that died after commit.json (it
// returns without touching anything), and Temporal's test environment
// retries it — the same mechanism internal/orker/attempts_test.go's
// attemptProbeWorkflow uses to make activity.GetInfo(ctx).Attempt count.
func adoptionProbeWorkflow(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2, InitialInterval: time.Millisecond},
	})
	var result *orkstackv1.ExecuteTaskResult
	err := workflow.ExecuteActivity(ctx, task.ActivityExecuteTask, in).Get(ctx, &result)
	return result, err
}

// scheduledInput reads the ExecuteTaskInput the workflow sent for one task out
// of the run's own history: the replacement attempt must be the SAME input.
func scheduledInput(t *testing.T, s *timingStack, runID, taskID string) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	iter := s.temporal.GetWorkflowHistory(s.ctx, runID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history of %s: %v", runID, err)
		}
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		var in orkstackv1.ExecuteTaskInput
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &in); err != nil {
			t.Fatalf("decode ExecuteTaskInput: %v", err)
		}
		if in.GetTaskId() == taskID {
			return &in
		}
	}
	t.Fatalf("no ExecuteTask for %q in %s's history", taskID, runID)
	return nil
}

// containersOf counts the containers — running or exited — that the Orker
// orkerID created, by its ork.orker label, never host-wide: other tests and
// other proofs start and stop containers on the same daemon concurrently
// (AGENTS.md §11.1), so a host-wide count moves under a test that starts none.
func containersOf(t *testing.T, orkerID string) int {
	t.Helper()
	out, err := exec.Command("docker", "ps", "--all", "--quiet", "--filter", "label=ork.orker="+orkerID).Output()
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	return len(strings.Fields(string(out)))
}

// TestIntegration_RunDiffReadsTheAdopterWhenTheCommittingRecordIsGone:
// the store state the AGENTS.md §11 crash window leaves — commit.json
// durable, the committing attempt's record gone, the adopting retry's record
// filed under its own number — produced with production code only.
func TestIntegration_RunDiffReadsTheAdopterWhenTheCommittingRecordIsGone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s, name := newDiffStack(ctx, t)
	defer s.writeTranscript("run-diff-adopter.txt")
	run1, run2 := finishedPair(t, s, name)

	before := outcomeRecords(t, s, run1, "work")
	if len(before) != 1 || before[0].GetAttempt() != 1 || !sixtyFourHex.MatchString(before[0].GetImage().GetPlatformDigest()) {
		t.Fatalf("run %s's work records = %v; want one attempt-1 record with a 64-hex platform_digest", run1, before)
	}
	kept := before[0].GetImage()
	deleteAttemptRecord(t, s, run1, "work", 1)

	// The real Orker's dispatcher against the real bucket.
	cfg := s.artifactCfg
	store, err := artifacts.New(cfg)
	if err != nil {
		t.Fatalf("open artifact store: %v", err)
	}
	commits, err := artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("open commit store: %v", err)
	}
	logs, err := artifacts.NewLogStore(cfg)
	if err != nil {
		t.Fatalf("open log store: %v", err)
	}
	attemptStore, err := artifacts.NewAttemptStore(cfg)
	if err != nil {
		t.Fatalf("open attempt store: %v", err)
	}
	refs, ok := store.(artifacts.RefPredictor)
	if !ok {
		t.Fatalf("artifact backend %T cannot predict references", store)
	}
	adopterID := proof.OrkerID("run-diff-adopter")
	activities := orker.NewActivitiesWithOptions(store, orker.Options{
		Logs: logs, Commits: commits, Attempts: attemptStore, Refs: refs,
		Pool: task.DefaultPool, OrkerID: adopterID, WorkRoot: bindableOrkerWorkRoot(t),
	})
	in := scheduledInput(t, s, run1, "work")

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(2 * time.Minute)
	env.RegisterWorkflow(adoptionProbeWorkflow)
	env.RegisterActivityWithOptions(func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if activity.GetInfo(ctx).Attempt < 2 {
			return nil, temporal.NewApplicationError("stand-in for the committing attempt that died after commit.json", "PROBE")
		}
		return activities.ExecuteTask(ctx, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	containersBefore := containersOf(t, adopterID)
	env.ExecuteWorkflow(adoptionProbeWorkflow, in)
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("replacement attempt: completed=%t err=%v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	var adopted *orkstackv1.ExecuteTaskResult
	if err := env.GetWorkflowResult(&adopted); err != nil {
		t.Fatalf("result: %v", err)
	}
	containersAfter := containersOf(t, adopterID)
	if containersAfter != containersBefore {
		t.Fatalf("the adopter's containers %d -> %d across the replacement attempt; adoption starts none", containersBefore, containersAfter)
	}
	if !adopted.GetCommitAdopted() || adopted.GetAttempt() != 1 {
		t.Fatalf("the replacement attempt did not adopt attempt 1's commit: adopted=%t attempt=%d", adopted.GetCommitAdopted(), adopted.GetAttempt())
	}

	after := outcomeRecords(t, s, run1, "work")
	if len(after) != 1 {
		t.Fatalf("work records after the replacement = %v; want exactly one", after)
	}
	record := after[0]
	if record.GetAttempt() != 2 || !record.GetCommitAdopted() ||
		record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED || !proto.Equal(record.GetImage(), kept) {
		t.Fatalf("the adopter's record = attempt %d adopted=%t outcome=%s image=%v; want 2, true, SUCCEEDED and record 1's image %v",
			record.GetAttempt(), record.GetCommitAdopted(), record.GetOutcome(), record.GetImage(), kept)
	}
	fmt.Fprintf(&s.transcript, "replacement attempt through the real OCIDispatcher: store holds one record, attempt=%d commit_adopted=%t outcome=%s platform_digest=%s (record 1's was %s); the adopter's containers %d -> %d\n\n",
		record.GetAttempt(), record.GetCommitAdopted(), record.GetOutcome(), record.GetImage().GetPlatformDigest(), kept.GetPlatformDigest(), containersBefore, containersAfter)
	t.Logf("EVIDENCE I4 single record attempt=%d commit_adopted=%t outcome=%s", record.GetAttempt(), record.GetCommitAdopted(), record.GetOutcome())

	res := diffOnB(t, s, run1, run2, "platform_digest", "image")
	recordDiff(s, "record 1 gone, adopter present", res)
	if len(res.GetDifferences()) != 0 || res.GetTasksIdentical() != 1 || res.GetEvidenceUnavailable() != 0 {
		t.Fatalf("rows=%d tasks_identical=%d evidence_unavailable=%d; want 0, 1, 0 (the adopter answers)",
			len(res.GetDifferences()), res.GetTasksIdentical(), res.GetEvidenceUnavailable())
	}
	t.Logf("EVIDENCE I4 record 1 absent: tasks_identical=%d evidence_unavailable=%d", res.GetTasksIdentical(), res.GetEvidenceUnavailable())
	human := s.mustCLI(1, "run", "diff", run1, run2, "--only", "platform_digest")
	if !strings.Contains(human, "No task differences.") || strings.Contains(human, "attempt_record_missing") {
		t.Fatalf("orkctl run diff --only platform_digest:\n%s", human)
	}

	deleteAttemptRecord(t, s, run1, "work", 2)
	gone := diffOnB(t, s, run1, run2, "platform_digest")
	recordDiff(s, "records 1 and 2 gone", gone)
	if len(gone.GetDifferences()) != 1 || gone.GetDifferences()[0].GetFromReason() != "attempt_record_missing" {
		t.Fatalf("with the adopter's record gone too, rows = %v; want one attempt_record_missing", gone.GetDifferences())
	}
	t.Logf("EVIDENCE I4 record 2 gone too: %s", rowText(gone.GetDifferences()[0]))
	proof.Write(t, "orkctl", "run-diff-adopter-rows.txt", []byte(rowText(gone.GetDifferences()[0])+"\n"))
}
