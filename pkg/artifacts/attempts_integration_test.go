//go:build integration

// Integration coverage for the S3 task attempt store.
//
// Run against the repo's local infrastructure (`make infra-up`):
//
//	go test -tags=integration ./pkg/artifacts/... -count=1
//
// The tests skip (never fail) when MinIO is unreachable, unless
// ORK_REQUIRE_INFRA is set — see infraAbsent in infra_required_test.go.
package artifacts_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The local store and the S3 store must answer identically, because a developer
// proves a behavior against the local one and a deployment runs the other. This
// is the S3 half of TestAttemptStores_AnswerIdentically against real MinIO,
// over a listing genuinely produced by the server rather than by a sorted map.
func TestIntegration_S3AttemptStore_OrdersARealListingByAttempt(t *testing.T) {
	store, cfg := newS3AttemptStore(t)
	ctx := context.Background()
	runID := fmt.Sprintf("ork-attempt-%s", uniqueToken(t))
	cleanupAttemptRecords(t, cfg, runID, "flaky")

	// A task with no records reads EMPTY, never an error — the ordinary state
	// of every run that finished before this key family existed.
	before, err := store.ListAttempts(ctx, runID, "flaky", artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts on an unknown task: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("ListAttempts on an unknown task = %d records, want 0", len(before))
	}

	// Crossing a digit boundary, written out of order: unpadded keys would put
	// 10 before 2, and this is where that would show.
	written := []int32{2, 11, 1, 10, 3}
	for _, attempt := range written {
		record := &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion,
			RunId:         runID,
			TaskId:        "flaky",
			Attempt:       attempt,
			Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
			Error: &orkstackv1.Error{
				Code:      "CONTAINER_EXIT_NONZERO",
				Message:   fmt.Sprintf("attempt %d exited 1", attempt),
				Retryable: true,
				Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
			},
			ExitCode: 1,
			Phase:    "exit",
			OrkerId:  "orker-integration",
			Pool:     "main",
		}
		if err := store.PutAttempt(ctx, record); err != nil {
			t.Fatalf("PutAttempt(%d): %v", attempt, err)
		}
	}

	got, err := store.ListAttempts(ctx, runID, "flaky", artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(got) != len(written) {
		t.Fatalf("read %d records from MinIO, want %d", len(got), len(written))
	}
	for i, want := range []int32{1, 2, 3, 10, 11} {
		if got[i].GetAttempt() != want {
			t.Fatalf("record %d has attempt %d, want %d: the bucket listing is not in attempt order",
				i, got[i].GetAttempt(), want)
		}
		if msg := got[i].GetError().GetMessage(); msg != fmt.Sprintf("attempt %d exited 1", want) {
			t.Errorf("record %d carries %q", i, msg)
		}
	}

	// A sibling task of the same run must not leak into this task's timeline:
	// the task id is a key segment, not a filter applied after the fact.
	cleanupAttemptRecords(t, cfg, runID, "other")
	if err := store.PutAttempt(ctx, &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion,
		RunId:         runID,
		TaskId:        "other",
		Attempt:       1,
		Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
	}); err != nil {
		t.Fatalf("PutAttempt for a sibling task: %v", err)
	}
	after, err := store.ListAttempts(ctx, runID, "flaky", artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts after the sibling write: %v", err)
	}
	if len(after) != len(written) {
		t.Fatalf("a sibling task's record leaked in: %d records, want %d", len(after), len(written))
	}
}

// The bound is enforced against a real PUT, and the truncation marker survives
// the round trip: an oversized workload-controlled message is stored short
// rather than refused, because a shortened record of a failed attempt is worth
// more than no record at all.
func TestIntegration_S3AttemptStore_TruncatesAnOversizedErrorAndStillStoresIt(t *testing.T) {
	store, cfg := newS3AttemptStore(t)
	ctx := context.Background()
	runID := fmt.Sprintf("ork-attempt-big-%s", uniqueToken(t))
	cleanupAttemptRecords(t, cfg, runID, "loud")

	if err := store.PutAttempt(ctx, &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion,
		RunId:         runID,
		TaskId:        "loud",
		Attempt:       1,
		Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
		Phase:         "run",
		Error: &orkstackv1.Error{
			Code:    "CONTAINER_EXIT_NONZERO",
			Message: strings.Repeat("x", artifacts.AttemptMaxBytes*2),
			Details: strings.Repeat("y", artifacts.AttemptMaxBytes*2),
		},
	}); err != nil {
		t.Fatalf("PutAttempt with an oversized error: %v", err)
	}

	got, err := store.ListAttempts(ctx, runID, "loud", artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d records, want 1", len(got))
	}
	record := got[0]
	if len(record.GetError().GetMessage()) > artifacts.AttemptErrorMessageMaxBytes {
		t.Errorf("stored message = %d bytes, limit is %d",
			len(record.GetError().GetMessage()), artifacts.AttemptErrorMessageMaxBytes)
	}
	if !strings.HasSuffix(record.GetError().GetMessage(), artifacts.AttemptTruncationMarker) {
		t.Error("the stored message carries no truncation marker")
	}
	// Whatever was cut, the fields that say WHICH attempt failed and why are
	// what the record exists for and must always survive.
	if record.GetError().GetCode() != "CONTAINER_EXIT_NONZERO" || record.GetPhase() != "run" {
		t.Errorf("bounding lost the reason or the phase: %v", record)
	}
}

// TestIntegration_S3AttemptStore_PhasesRoundTrip is S1's S3 half: the phase
// spans an Orker writes come back from a real MinIO listing unchanged.
func TestIntegration_S3AttemptStore_PhasesRoundTrip(t *testing.T) {
	store, cfg := newS3AttemptStore(t)
	ctx := context.Background()
	runID := fmt.Sprintf("ork-attempt-%s", uniqueToken(t))
	cleanupAttemptRecords(t, cfg, runID, "spans")

	base := time.Date(2026, 9, 22, 9, 14, 4, 0, time.UTC)
	record := &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion,
		RunId:         runID,
		TaskId:        "spans",
		Attempt:       1,
		Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		Phase:         "run",
		Phases: []*orkstackv1.PhaseSpan{
			{Phase: "start", StartedAt: timestamppb.New(base), FinishedAt: timestamppb.New(base.Add(3 * time.Millisecond))},
			{Phase: "image", StartedAt: timestamppb.New(base.Add(3 * time.Millisecond)), FinishedAt: timestamppb.New(base.Add(time.Second))},
			{Phase: "run", StartedAt: timestamppb.New(base.Add(time.Second)), FinishedAt: timestamppb.New(base.Add(61 * time.Second))},
		},
	}
	if err := store.PutAttempt(ctx, record); err != nil {
		t.Fatalf("PutAttempt: %v", err)
	}
	got, err := store.ListAttempts(ctx, runID, "spans", artifacts.NoItem)
	if err != nil || len(got) != 1 {
		t.Fatalf("ListAttempts = %d records, %v", len(got), err)
	}
	if !proto.Equal(got[0], record) {
		t.Fatalf("S3 round trip changed the record:\n got %v\nwant %v", got[0], record)
	}
}

func newS3AttemptStore(t *testing.T) (*artifacts.S3AttemptStore, artifacts.Config) {
	t.Helper()
	cfg := s3TestConfig()
	requireObjectStore(t, cfg)

	store, err := artifacts.NewS3AttemptStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3AttemptStore: %v", err)
	}
	return store, cfg
}

// cleanupAttemptRecords removes a task's records so repeated runs leave the
// shared dev bucket as they found it.
func cleanupAttemptRecords(t *testing.T, cfg artifacts.Config, runID, taskID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		client := newRawS3Client(t, cfg)
		prefix, err := artifacts.AttemptTaskPrefix(runID, taskID)
		if err != nil {
			t.Fatalf("AttemptTaskPrefix: %v", err)
		}
		for _, key := range listKeys(t, ctx, client, cfg.S3Bucket, prefix) {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(cfg.S3Bucket),
				Key:    aws.String(key),
			}); err != nil {
				t.Logf("cleanup %s: %v", key, err)
			}
		}
	})
}

// cleanupRunAttempts removes every record of the given runs.
func cleanupRunAttempts(t *testing.T, cfg artifacts.Config, runIDs ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		client := newRawS3Client(t, cfg)
		for _, runID := range runIDs {
			prefix, err := artifacts.RunAttemptPrefix(runID)
			if err != nil {
				t.Fatalf("RunAttemptPrefix: %v", err)
			}
			for _, key := range listKeys(t, ctx, client, cfg.S3Bucket, prefix) {
				if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(cfg.S3Bucket),
					Key:    aws.String(key),
				}); err != nil {
					t.Logf("cleanup %s: %v", key, err)
				}
			}
		}
	})
}

// TestS3AttemptStore_RunAttemptReaderAgreesWithListAttempts: the run-wide
// listing names exactly the union of the per-task listings, in key order,
// across S3's 1,000-key page, and never a record of a run whose id merely
// extends this one's.
func TestS3AttemptStore_RunAttemptReaderAgreesWithListAttempts(t *testing.T) {
	store, cfg := newS3AttemptStore(t)
	ctx := context.Background()
	runID := fmt.Sprintf("ork-usage-%s", uniqueToken(t))
	extended := runID + "0"
	cleanupRunAttempts(t, cfg, runID, extended)

	counts := map[string]int32{"alpha": 500, "beta": 1, "gamma": 500} // 1,001 records
	for task, n := range counts {
		for attempt := int32(1); attempt <= n; attempt++ {
			if err := store.PutAttempt(ctx, &orkstackv1.TaskAttempt{
				SchemaVersion: artifacts.AttemptSchemaVersion, RunId: runID, TaskId: task, Attempt: attempt,
				Outcome:   orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
				Resources: &orkstackv1.ReservedResources{Cpu: 1, MemoryBytes: 1 << 30},
			}); err != nil {
				t.Fatalf("PutAttempt(%s/%d): %v", task, attempt, err)
			}
		}
	}
	if err := store.PutAttempt(ctx, &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion, RunId: extended, TaskId: "alpha", Attempt: 1,
		Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
	}); err != nil {
		t.Fatalf("PutAttempt(extended run): %v", err)
	}

	refs, err := store.ListRunAttemptRefs(ctx, runID)
	if err != nil {
		t.Fatalf("ListRunAttemptRefs: %v", err)
	}
	var want []artifacts.AttemptRef
	for _, task := range []string{"alpha", "beta", "gamma"} {
		records, err := store.ListAttempts(ctx, runID, task, artifacts.NoItem)
		if err != nil {
			t.Fatalf("ListAttempts(%s): %v", task, err)
		}
		for _, record := range records {
			want = append(want, artifacts.AttemptRef{RunID: runID, TaskID: task, Attempt: record.GetAttempt()})
		}
	}
	if len(refs) != 1001 || len(want) != 1001 {
		t.Fatalf("run-wide listing has %d refs, per-task listings %d; want 1001 each", len(refs), len(want))
	}
	for i := range want {
		if refs[i] != want[i] {
			t.Fatalf("ref %d = %v, want %v (key order: task, then attempt)", i, refs[i], want[i])
		}
	}
	for _, ref := range []artifacts.AttemptRef{refs[0], refs[500], refs[1000]} {
		record, err := store.GetAttempt(ctx, ref)
		if err != nil {
			t.Fatalf("GetAttempt(%v): %v", ref, err)
		}
		if record.GetTaskId() != ref.TaskID || record.GetAttempt() != ref.Attempt || record.GetResources().GetCpu() != 1 {
			t.Fatalf("GetAttempt(%v) = %v", ref, record)
		}
	}
	t.Logf("run-wide listing of %s: %d refs across S3's 1,000-key page, first %v, last %v",
		runID, len(refs), refs[0], refs[len(refs)-1])

	// Deleted after the listing: ErrNotFound, the rule a reader drops it by.
	client := newRawS3Client(t, cfg)
	key, err := artifacts.AttemptKey(refs[0])
	if err != nil {
		t.Fatalf("AttemptKey: %v", err)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := store.GetAttempt(ctx, refs[0]); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("GetAttempt of a deleted record = %v, want ErrNotFound", err)
	}
}
