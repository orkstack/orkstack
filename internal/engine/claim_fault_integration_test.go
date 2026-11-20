//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// refuseAttemptWrites makes object storage refuse every write under the
// attempt prefix of one task of one run, and nothing else: a bucket policy
// that denies s3:PutObject there, on the test binary's own bucket and
// removed when the test ends. The policy is proven before the scenario
// relies on it, by a write the store must refuse and a write beside it that it
// must accept. The development SeaweedFS (4.53, deploy/docker-compose.yml)
// enforces such a Deny for the identity the tests use, measured 2026-11-20,
// so the fault is the real store's and nothing stands in for it; a store that
// does not enforce it fails the test, never quietly runs another scenario.
func refuseAttemptWrites(t *testing.T, ctx context.Context, cfg artifacts.Config, stores *realOrkerStores, runID, taskID, twinID string) (*realOrkerStores, string) {
	t.Helper()
	denied, err := artifacts.AttemptTaskPrefix(runID, taskID)
	must.OK(t, err)
	beside, err := artifacts.AttemptTaskPrefix(runID, twinID)
	must.OK(t, err)
	raw := newRawS3ClientForCommitE2E(t, cfg)
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Sid":"RefuseOneTasksAttemptRecords",`+
		`"Effect":"Deny","Principal":"*","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::%s/%s*"]}]}`,
		cfg.S3Bucket, denied)
	if _, err := raw.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: aws.String(cfg.S3Bucket), Policy: aws.String(policy)}); err != nil {
		t.Fatalf("the object store refused the bucket policy that makes the fault: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := raw.DeleteBucketPolicy(cleanup, &s3.DeleteBucketPolicyInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
			t.Errorf("remove the bucket policy the test set on %s: %v", cfg.S3Bucket, err)
		}
	})
	put := func(key string) error {
		_, err := raw.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("{}")),
		})
		return err
	}
	if err := put(beside + "policy-probe"); err != nil {
		t.Fatalf("a write beside the refused prefix, %s, failed under the policy: %v", beside, err)
	}
	err = put(denied + "policy-probe")
	var apiErr smithy.APIError
	switch {
	case err == nil:
		t.Fatalf("the object store accepted a write its bucket policy denies under %s: it cannot make this fault", denied)
		return nil, ""
	case errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied":
		return stores, "a bucket policy denying s3:PutObject under " + denied
	default:
		t.Fatalf("the probe write under the refused prefix failed with %v, not AccessDenied", err)
		return nil, ""
	}
}

// claimFaultDocument is two independent tasks of the real pool that write a
// value and exit: "denied" runs where the attempt store refuses its claims,
// and "twin" beside it where it does not. Neither has a retry: so each makes
// one attempt of its own (M = 1), and only an interruption runs it again.
func claimFaultDocument(t *testing.T, image string) *orkstackv1.Orkflow {
	t.Helper()
	doc := "apiVersion: orkstack/v1\nkind: Orkflow\nname: claim-fault\nversion: \"1.0.0\"\ntasks:\n"
	for _, id := range []string{"denied", "twin"} {
		doc += fmt.Sprintf("  - id: %s\n    uses: shell\n    needs: []\n    pool: real\n    with: {op: exec}\n", id)
	}
	wf := compile(t, doc)
	for _, tk := range wf.GetTasks() {
		tk.Uses = image
		tk.Image = &orkstackv1.ImageRef{Reference: image, Pinned: image}
		tk.Command = []string{"/bin/sh", "-c", `printf '{"ok":true}' > /orkstack/output/values.json`}
		tk.TimeoutSeconds = 120
		tk.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
	}
	return wf
}

// TestIntegration_PersistentClaimFailureWithoutStoreCordonEndsExplicitly: an
// Orker whose object store refuses the attempt records of one task while every
// read and every other write passes is never cordoned for a store outage, so
// nothing keeps the task from being handed to it again. The fault is
// before the container starts, so each hand is an interruption the engine runs
// again; once the run's budget of such faults in a row is spent they spend the
// task's own attempts, and the task ends FAILED ATTEMPT_STORE_UNAVAILABLE,
// naming what the store answered, within M × 5 re-dispatches (M = 1: its one
// attempt) and the back-off. Its twin, whose attempt prefix is writable, runs
// once in the same window and succeeds.
//
// The Orker runs its real store watch (cmd/orker's, over the stack's commit
// store), so "the cordon never trips" is a watch that checked and found the
// store answering, not one that was not there.
func TestIntegration_PersistentClaimFailureWithoutStoreCordonEndsExplicitly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "interruption/claim-faults")
	stores := s.realStores()
	realQueue := mustQueueForPool(t, "real")
	s.startReplica("replica-a", replicaOptions{})
	s.startProjection(nil)

	runID := s.newRunID()
	s.transcript.run(runID)
	orkerStores, mechanism := refuseAttemptWrites(t, ctx, s.cfg, stores, runID, "denied", "twin")
	s.transcript.step("the fault: %s", mechanism)
	t.Logf("observed: the attempt-store fault is %s", mechanism)

	activities, orkerID := s.realOrkerActivities("orker-a", "real", orkerStores, nil)
	s.startOrkerWith("orker-a", realQueue, s.control, func(actx context.Context, _ *chaosStack, _ string,
		in *orkstackv1.ExecuteTaskInput,
	) (*orkstackv1.ExecuteTaskResult, error) {
		return activities.ExecuteTask(actx, in)
	})
	var checks atomic.Int32
	watchCtx, stopWatch := context.WithCancel(ctx)
	t.Cleanup(stopWatch)
	go activities.NewStoreWatch(func(c context.Context) error {
		checks.Add(1)
		return artifacts.CheckCommitStore(c, stores.commits)
	}, orker.StoreWatchConfig{
		Enabled: true, Interval: time.Second, Timeout: 5 * time.Second,
		FailureThreshold: orker.DefaultStoreCheckFailures, RecoveryThreshold: orker.DefaultStoreCheckRecoveries,
	}, nil).Run(watchCtx)

	wf := claimFaultDocument(t, pinnedAlpineForCommitE2E(t))
	plan := realTaskPlan(t, s, realQueue)
	// The plan's budgets are the defaults, with the back-off the test would
	// otherwise wait out (45 s floored, then 60 s a time) cut to a fifth of a
	// second: it decides how long the run takes, not how many times it tries.
	budgets := engine.DefaultInterruptionBudgets
	budgets.NeverStartedBackoffMillis = 200
	budgets.NeverStartedFaultFloorMillis = 0
	plan.Interruptions = &budgets
	s.start(runID, wf, nil, plan)
	// Opened once the run exists: a stream opened before it is answered
	// "run not found" and ends.
	stream := s.openStream(s.reader, runID, 0)

	if _, err := s.wait(runID, 8*time.Minute); err != nil && !strings.Contains(err.Error(), "ATTEMPT_STORE_UNAVAILABLE") {
		t.Logf("observed: the run's workflow ended with %v", err)
	}
	s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, "")
	run, err := s.reader.GetRun(ctx, runID)
	must.OK(t, err)

	denied, twin := taskRunOfRun(run, "denied"), taskRunOfRun(run, "twin")
	if denied.GetStatus() != orkstackv1.Status_STATUS_FAILED || denied.GetError().GetCode() != "ATTEMPT_STORE_UNAVAILABLE" {
		t.Fatalf("the denied task ended %v %v, want FAILED ATTEMPT_STORE_UNAVAILABLE", denied.GetStatus(), denied.GetError())
	}
	if !strings.Contains(denied.GetError().GetMessage(), "object storage answered:") {
		t.Errorf("the failure does not name what the store answered: %q", denied.GetError().GetMessage())
	}
	st := denied.GetInterruptions()
	if st.GetLast().GetReason() != "ATTEMPT_STORE_UNAVAILABLE" || st.GetLast().GetStoreError() == "" ||
		st.GetOwnAttemptsUsed() != 1 || st.GetLast().GetClosedBy() != "never_started" {
		t.Errorf("the denied task's interruptions = %v, want ATTEMPT_STORE_UNAVAILABLE naming the store's answer, its one own attempt spent by the faults", st)
	}
	// M × 5 re-dispatches, and the attempt that spends the own attempt.
	const m, faultBudget = 1, 5
	got, _ := s.attemptsOf("denied")
	if got < 2 || got > m*faultBudget+m {
		t.Errorf("the denied task was handed to the Orker %d times, want from 2 (it was run again) to %d", got, m*faultBudget+m)
	}
	t.Logf("observed: the denied task was handed to the Orker %d times; last store answer: %s", got, st.GetLast().GetStoreError())

	// The twin, in the same window, on a writable prefix: once, and it succeeded.
	if twin.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || twin.GetInterruptions().GetLast() != nil {
		t.Errorf("the twin ended %v with interruptions %v, want SUCCEEDED, never interrupted", twin.GetStatus(), twin.GetInterruptions())
	}
	if n, _ := s.attemptsOf("twin"); n != 1 {
		t.Errorf("the twin was handed to the Orker %d times, want once", n)
	}
	twinRecords, err := stores.attempts.ListAttempts(ctx, runID, "twin", artifacts.NoItem)
	must.OK(t, err)
	deniedRecords, err := stores.attempts.ListAttempts(ctx, runID, "denied", artifacts.NoItem)
	must.OK(t, err)
	if len(twinRecords) != 1 || len(deniedRecords) != 0 {
		t.Errorf("attempt records: %d for the twin and %d for the denied task, want 1 and 0 (the store refused them)", len(twinRecords), len(deniedRecords))
	}

	// The cordon never tripped: the watch checked, in the window the faults
	// lasted, and the Orker reports no outage and no cordon.
	if n := checks.Load(); n < orker.DefaultStoreCheckFailures {
		t.Errorf("the store watch checked %d times in the window, want at least %d: it would not have tripped either way", n, orker.DefaultStoreCheckFailures)
	}
	described, err := activities.DescribeOrker(ctx)
	must.OK(t, err)
	if described.GetCordoned() || described.GetStoreOutage() != nil {
		t.Errorf("the Orker %s reports cordoned=%v outage=%v; the fault was meant to leave it taking work", orkerID, described.GetCordoned(), described.GetStoreOutage())
	}
}
