//go:build integration

package api

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// CommitResult's integration suite (docs/TASK_IDENTITY.md, "Results the
// engine records"), on the mint suite's rig: one real execution on the
// proof's own Temporal whose attempts stand-in Orkers hold, and the
// records in the process's own bucket of the real store. The recorder is
// the mint's checks over the same cluster and store; what it writes is
// read back with the store's own client.

// recording gives the rig's Service a recorder over the rig's cluster and
// store.
func (r *mintRig) recording() {
	r.t.Helper()
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	r.svc.artifacts = cfg
	r.svc.recorder = &identity.Recorder{Stores: r.records, Starts: r.starts, Describe: r.minter.Describe,
		PinnedImage: func(context.Context, artifacts.Admission, string) (string, error) { return "", nil },
		EngineBuild: "commit-test", Now: time.Now}
}

func (r *mintRig) commitRequest(attempt int32, orker string) *orkstackv1.CommitResultRequest {
	key, err := artifacts.CommitKey(artifacts.DefaultNamespace, r.runID, mintTask)
	if err != nil {
		r.t.Fatal(err)
	}
	return &orkstackv1.CommitResultRequest{Namespace: artifacts.DefaultNamespace, RunId: r.runID, TaskId: mintTask, Attempt: attempt,
		TaskSpecSha256: strings.Repeat(mintSpec, 64), AdmissionSha256: r.sha, CommitKey: key,
		Commit: &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.DefaultNamespace,
			RunId: r.runID, TaskId: mintTask, OrkflowName: "deploy", OrkflowVersion: "1",
			OrkflowDefinitionDigest: "sha256:" + strings.Repeat("d", 64),
			IdempotencyKey:          task.IdempotencyKeyFor(artifacts.DefaultNamespace, r.runID, mintTask),
			Image:                   &orkstackv1.ImageRef{IndexDigest: "sha256:" + strings.Repeat("e", 64)}, Attempt: attempt, OrkerId: orker}}
}

func (r *mintRig) commit(orker string, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
	p := &authn.Principal{Kind: authn.KindService, ID: "serviceaccount:" + orker, Issuer: "serviceaccount",
		Method: authn.MethodServiceAccount, OrkerID: orker}
	resp, err := r.svc.CommitResult(authn.WithPrincipal(authn.WithMode(r.ctx, authn.ModeEnforce), p), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// recorded reads the task's attested record from the bucket with the
// store's own client: nil when there is none.
func (r *mintRig) recorded() *orkstackv1.ActivityCommit {
	r.t.Helper()
	key, _ := artifacts.CommitKey(artifacts.DefaultNamespace, r.runID, mintTask)
	attested, err := artifacts.AttestedCommitKey(key)
	if err != nil {
		r.t.Fatal(err)
	}
	out, err := r.raw.GetObject(r.ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(attested)})
	var missing interface{ ErrorCode() string }
	if errors.As(err, &missing) && missing.ErrorCode() == "NoSuchKey" {
		return nil
	}
	if err != nil {
		r.t.Fatalf("read %s: %v", attested, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, artifacts.CommitMaxBytes+1))
	if err != nil {
		r.t.Fatal(err)
	}
	c, err := artifacts.UnmarshalCommit(body)
	if err != nil {
		r.t.Fatalf("decode %s: %v", attested, err)
	}
	return c
}

// commitRefusals is the task.result.refused records, by reason.
func (r *mintRig) commitRefusals() map[string]int {
	r.t.Helper()
	page, err := r.audits.ListAuditRecords(r.ctx, artifacts.AuditQuery{Limit: 100, Until: time.Now().Add(time.Hour)})
	if err != nil {
		r.t.Fatal(err)
	}
	out := map[string]int{}
	for _, rec := range page.Records {
		if rec.GetAction() == "task.result.refused" {
			out[rec.GetErrorCode()]++
		}
	}
	return out
}

// TestIntegration_Commit_TheHolderIsRecorded: the Orker the cluster says
// holds the attempt has its preflight answered and its result written
// under the attested prefix, with the engine's recording naming the pool
// and that Orker; a repeat is answered with that record and writes nothing.
func TestIntegration_Commit_TheHolderIsRecorded(t *testing.T) {
	r := newMintRig(t, mintRigOptions{slots: 1, maxAttempts: 1})
	r.recording()
	r.orker(honest)
	r.held(1)

	pre := r.commitRequest(1, honest)
	pre.Preflight, pre.Commit = true, nil
	if resp, err := r.commit(honest, pre); err != nil || resp.GetCommit() != nil {
		t.Fatalf("preflight = %v, %v", resp, err)
	}
	if r.recorded() != nil {
		t.Fatal("a preflight wrote a record")
	}
	resp, err := r.commit(honest, r.commitRequest(1, honest))
	if err != nil || !resp.GetCreated() || !strings.HasSuffix(resp.GetCommit().GetUri(), "/metadata/attested/default/"+r.runID+"/"+mintTask+"/commit.json") {
		t.Fatalf("commit = %v, %v", resp, err)
	}
	got := r.recorded()
	if got == nil || got.GetRecorded().GetPool() != "main" || got.GetRecorded().GetOrkerId() != honest || got.GetRecorded().GetAdmissionSha256() != r.sha {
		t.Fatalf("the stored record = %v", got)
	}
	again, err := r.commit(honest, r.commitRequest(1, honest))
	if err != nil || again.GetCreated() || again.GetCommit().GetSha256() != resp.GetCommit().GetSha256() {
		t.Fatalf("a repeat = %v, %v", again, err)
	}
	if n := r.commitRefusals(); len(n) != 0 {
		t.Fatalf("refusal records = %v", n)
	}
}

// TestIntegration_Commit_AStolenOrkerIsRefused: a credential that may
// commit at the task's pool but does not hold the attempt is
// ATTEMPT_NOT_HELD, unaudited; one of another pool is denied, and the
// denial is recorded; neither writes a record, and the holder still
// commits afterwards.
func TestIntegration_Commit_AStolenOrkerIsRefused(t *testing.T) {
	r := newMintRig(t, mintRigOptions{slots: 1, maxAttempts: 1})
	r.recording()
	r.orker(honest)
	r.held(1)

	if _, err := r.commit(thief, r.commitRequest(1, thief)); connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(errString(err), identity.ReasonAttemptNotHeld) {
		t.Fatalf("a thief's commit: %v", err)
	}
	if _, err := r.commit("orker-gpu", r.commitRequest(1, "orker-gpu")); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("another pool's commit: %v", err)
	}
	if r.recorded() != nil {
		t.Fatal("a refused commit wrote a record")
	}
	if got := r.commitRefusals(); got["permission_denied"] != 1 || got[identity.ReasonAttemptNotHeld] != 0 {
		t.Fatalf("refusal records = %v", got)
	}
	if _, err := r.commit(honest, r.commitRequest(1, honest)); err != nil {
		t.Fatalf("the holder's commit: %v", err)
	}
}

// TestIntegration_Commit_ConcurrentCommitsMakeOneRecord: the holder's
// commits sent at once, as a lost answer's repeats arrive, make one record
// on the real store; every one is answered with it.
func TestIntegration_Commit_ConcurrentCommitsMakeOneRecord(t *testing.T) {
	r := newMintRig(t, mintRigOptions{slots: 1, maxAttempts: 1})
	r.recording()
	r.orker(honest)
	r.held(1)

	const n = 4 // an attempt's granted calls in a minute
	var wg sync.WaitGroup
	resps := make([]*orkstackv1.CommitResultResponse, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { resps[i], errs[i] = r.commit(honest, r.commitRequest(1, honest)) })
	}
	wg.Wait()
	created, digest := 0, ""
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("commit %d: %v", i, errs[i])
		}
		if resps[i].GetCreated() {
			created++
		}
		if digest == "" {
			digest = resps[i].GetCommit().GetSha256()
		}
		if resps[i].GetCommit().GetSha256() != digest {
			t.Fatalf("commit %d names another record: %s, %s", i, resps[i].GetCommit().GetSha256(), digest)
		}
	}
	if created != 1 {
		t.Fatalf("%d of %d commits created the record, want 1", created, n)
	}
}
