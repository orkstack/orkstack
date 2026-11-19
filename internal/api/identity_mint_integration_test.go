//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/typeid"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The mint's integration suite (docs/TASK_IDENTITY.md, "The mint"). Each
// test runs one real execution on the proof's own Temporal, in a namespace
// of its own, whose ExecuteTask attempts are held by stand-in Orkers: Go
// workers whose identity is an Orker id and whose activity blocks until
// the test releases it. The records live in the process's own bucket of
// the proof's real store. What the cluster says (who holds which attempt,
// the run's first execution) and what the store holds (the admission, the
// chain, the slots, what was issued) are therefore the real ones; the
// stand-in replaces only the Orker binary, which is not under test here.

const (
	mintIssuer = "https://ork.mint.test"
	mintTask   = "build"
	mintSpec   = "a" // repeated to 64: the spec digest the admission holds
	honest     = "orker-1"
	thief      = "orker-2"
)

// mintRig is one run: its execution on the cluster, its admission in the
// store, and a Service whose mint reads both.
type mintRig struct {
	t        *testing.T
	ctx      context.Context
	temporal client.Client
	records  *artifacts.RecordStore
	starts   identity.ClusterStarts
	raw      *s3.Client
	bucket   string
	svc      *Service
	audits   *artifacts.MemoryAuditStore
	minter   *identity.Minter
	runID    string
	sha      string
	wfQueue  string
	actQueue string

	started chan startedAttempt
	mu      sync.Mutex
	release map[int32]chan error
}

// startedAttempt is an attempt a stand-in Orker began.
type startedAttempt struct {
	attempt int32
	orker   string
}

type mintRigOptions struct {
	slots       int32 // the admission's identity slots for the task
	maxAttempts int32 // the activity's retry policy
}

func newMintRig(t *testing.T, o mintRigOptions) *mintRig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	raw, err := s3bucket.Client(ctx, cfg)
	if err != nil {
		mintInfraAbsent(t, "object store client: %v", err)
	}
	if _, err := raw.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		mintInfraAbsent(t, "object store bucket %q at %s: %v; run `make infra-up`", cfg.S3Bucket, cfg.S3Endpoint, err)
	}
	records, err := artifacts.NewRecordStore(ctx, cfg)
	if err != nil {
		t.Fatalf("record store: %v", err)
	}
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	r := &mintRig{t: t, ctx: ctx, records: records, raw: raw, bucket: cfg.S3Bucket,
		temporal: mintTemporal(ctx, t, mintNamespace(t, stamp)),
		runID:    engine.NewRunID(), wfQueue: "mint-wf-" + stamp, actQueue: "mint-act-" + stamp,
		started: make(chan startedAttempt, 16), release: map[int32]chan error{}}
	r.starts = identity.ClusterStarts{RecordStore: records, Temporal: r.temporal}
	r.sha = r.admit(o.slots)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	r.minter = &identity.Minter{
		Stores: records,
		Starts: r.starts,
		Describe: func(ctx context.Context, runID string) (identity.ExecutionFacts, error) {
			return r.starts.Describe(ctx, runID, "")
		},
		Audiences: func(context.Context, string, string) ([]string, error) { return []string{"sts"}, nil },
		Signer:    func() (*jwt.Signer, error) { return signer, nil },
		Issuer:    mintIssuer, Deployment: "mint-test",
		TTL:            identity.TTLBounds{Default: time.Hour, Min: 10 * time.Minute, Max: 12 * time.Hour},
		Skew:           time.Minute,
		RecordIssuance: true,
		Now:            time.Now,
		NewJTI:         typeid.New,
	}
	r.audits = artifacts.NewMemoryAuditStore()
	r.svc = NewService(Config{AuditStore: r.audits, Minter: r.minter})
	both := []*orkstackv1.Role{{Name: "mints", Permissions: []string{"identity.token.mint", "task.result.commit"}}}
	access, _ := accessFor(t, authn.ModeEnforce, both,
		&orkstackv1.Binding{Name: "orkers", Role: "mints", Scope: "pool:main", Subjects: []string{"serviceaccount:" + honest, "serviceaccount:" + thief}},
		&orkstackv1.Binding{Name: "gpu", Role: "mints", Scope: "pool:gpu", Subjects: []string{"serviceaccount:orker-gpu"}})
	r.svc.access = access

	r.engineWorker()
	run, err := r.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: r.runID, TaskQueue: r.wfQueue}, mintRigWorkflow, r.actQueue, o.maxAttempts)
	if err != nil {
		t.Fatalf("start run %s: %v", r.runID, err)
	}
	t.Cleanup(func() {
		_ = r.temporal.TerminateWorkflow(context.Background(), r.runID, "", "the test is done")
	})
	// The engine completes the chain record from the run's first start; the
	// rig writes what it would: the first execution and the admission.
	if err := records.CompleteChain(ctx, artifacts.DefaultNamespace, r.runID,
		artifacts.Chain{FirstExecutionID: run.GetRunID(), AdmissionSHA256: r.sha}); err != nil {
		t.Fatalf("chain record: %v", err)
	}
	return r
}

// admit writes the run's admission create-only at its key, as the engine
// writes it at submit, and answers its digest.
func (r *mintRig) admit(slots int32) string {
	r.t.Helper()
	adm := artifacts.Admission{RunID: r.runID, Namespace: artifacts.DefaultNamespace, Orkflow: "deploy", OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: artifacts.Starter{Kind: artifacts.StarterEngine},
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(),
		EngineBuild: "mint-test", SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, Issuer: mintIssuer,
		NamespaceID: "ns_default", OrkflowID: "of_1", TokenTTL: 3600,
		Sets: []artifacts.IdentitySet{{Identities: []artifacts.AdmittedIdentity{{Name: "aws", Audience: "sts", TTLSeconds: 900, Refresh: "required"}}}},
		Tasks: map[string]artifacts.AdmittedTask{mintTask: {Set: 1, Pool: "main", SpecSHA256: strings.Repeat(mintSpec, 64),
			IdentitySlots: slots, AttemptDeadline: 3600}}}
	body, sha, err := artifacts.EncodeAdmission(adm)
	if err != nil {
		r.t.Fatal(err)
	}
	key, err := artifacts.AdmissionKey(adm.Namespace, adm.RunID, sha)
	if err != nil {
		r.t.Fatal(err)
	}
	if _, err := r.raw.PutObject(r.ctx, &s3.PutObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), IfNoneMatch: aws.String("*")}); err != nil {
		r.t.Fatalf("write admission: %v", err)
	}
	return sha
}

// mintRigWorkflow dispatches the task's attempts under the activity id and
// type a run's ExecuteTask has, on the stand-in Orkers' queue.
func mintRigWorkflow(ctx workflow.Context, queue string, maxAttempts int32) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID: identity.ActivityID(mintTask, artifacts.Item{}), TaskQueue: queue,
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, BackoffCoefficient: 1, MaximumAttempts: maxAttempts},
	})
	return workflow.ExecuteActivity(ctx, task.ActivityExecuteTask).Get(ctx, nil)
}

func (r *mintRig) engineWorker() {
	w := worker.New(r.temporal, r.wfQueue, worker.Options{Identity: "engine"})
	w.RegisterWorkflow(mintRigWorkflow)
	if err := w.Start(); err != nil {
		r.t.Fatalf("engine worker: %v", err)
	}
	r.t.Cleanup(w.Stop)
}

// orker starts a stand-in Orker: a worker of id whose ExecuteTask blocks
// until the test releases the attempt it holds.
func (r *mintRig) orker(id string) worker.Worker {
	w := worker.New(r.temporal, r.actQueue, worker.Options{Identity: id})
	w.RegisterActivityWithOptions(func(ctx context.Context) error {
		attempt := activity.GetInfo(ctx).Attempt
		r.started <- startedAttempt{attempt: attempt, orker: id}
		select {
		case err := <-r.releaseOf(attempt):
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := w.Start(); err != nil {
		r.t.Fatalf("Orker %s: %v", id, err)
	}
	r.t.Cleanup(w.Stop)
	return w
}

func (r *mintRig) releaseOf(attempt int32) chan error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.release[attempt]
	if !ok {
		c = make(chan error, 1)
		r.release[attempt] = c
	}
	return c
}

// fail ends attempt with a retryable failure, so the cluster dispatches the next.
func (r *mintRig) fail(attempt int32) {
	r.releaseOf(attempt) <- fmt.Errorf("attempt %d fails", attempt)
}

// held waits until a stand-in Orker begins attempt and the cluster reports
// it started and last held by that Orker, what the mint's step 4 reads,
// and answers the Orker.
func (r *mintRig) held(attempt int32) string {
	r.t.Helper()
	var orker string
	select {
	case got := <-r.started:
		if got.attempt != attempt {
			r.t.Fatalf("attempt %d started, want %d", got.attempt, attempt)
		}
		orker = got.orker
	case <-r.ctx.Done():
		r.t.Fatalf("attempt %d never started: %v", attempt, r.ctx.Err())
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		facts, err := r.starts.Describe(r.ctx, r.runID, "")
		if err == nil {
			if _, v := identity.HeldAttempt(facts, r.runID, identity.AttemptAsk{TaskID: mintTask, Attempt: attempt, OrkerID: orker}); v == identity.AttemptHeld {
				return orker
			}
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("the cluster never reported attempt %d held by %s: %v %+v", attempt, orker, err, facts.Pending)
		}
		select {
		case <-r.ctx.Done():
			r.t.Fatal(r.ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *mintRig) request(attempt int32) *orkstackv1.MintAttemptTokensRequest {
	return &orkstackv1.MintAttemptTokensRequest{Namespace: artifacts.DefaultNamespace, RunId: r.runID, TaskId: mintTask, Attempt: attempt,
		TaskSpecSha256: strings.Repeat(mintSpec, 64), AdmissionSha256: r.sha, Tokens: []*orkstackv1.TokenRequest{{Name: "aws", Audience: "sts"}}}
}

func (r *mintRig) mint(orker string, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
	p := &authn.Principal{Kind: authn.KindService, ID: "serviceaccount:" + orker, Issuer: "serviceaccount",
		Method: authn.MethodServiceAccount, OrkerID: orker}
	resp, err := r.svc.MintAttemptTokens(authn.WithPrincipal(authn.WithMode(r.ctx, authn.ModeEnforce), p), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (r *mintRig) granted(orker string, req *orkstackv1.MintAttemptTokensRequest) *orkstackv1.MintAttemptTokensResponse {
	r.t.Helper()
	resp, err := r.mint(orker, req)
	if err != nil {
		r.t.Fatalf("%s's mint of attempt %d: %v", orker, req.GetAttempt(), err)
	}
	if len(resp.GetTokens()) != 1 {
		r.t.Fatalf("%s's mint of attempt %d: %d tokens", orker, req.GetAttempt(), len(resp.GetTokens()))
	}
	return resp
}

func (r *mintRig) refused(orker string, req *orkstackv1.MintAttemptTokensRequest, code connect.Code, reason string) {
	r.t.Helper()
	_, err := r.mint(orker, req)
	if connect.CodeOf(err) != code || !strings.Contains(errString(err), reason) {
		r.t.Fatalf("%s's mint of attempt %d: %v, want %s %s", orker, req.GetAttempt(), err, code, reason)
	}
}

// refusals is the identity.token.refused records, by reason.
func (r *mintRig) refusals() map[string][]*orkstackv1.AuditRecord {
	r.t.Helper()
	page, err := r.audits.ListAuditRecords(r.ctx, artifacts.AuditQuery{Limit: 100, Until: time.Now().Add(time.Hour)})
	if err != nil {
		r.t.Fatal(err)
	}
	out := map[string][]*orkstackv1.AuditRecord{}
	for _, rec := range page.Records {
		if rec.GetAction() == "identity.token.refused" {
			out[rec.GetErrorCode()] = append(out[rec.GetErrorCode()], rec)
		}
	}
	return out
}

func (r *mintRig) issued() []artifacts.IssuedRecord {
	r.t.Helper()
	page, _, err := r.records.ListIssued(r.ctx, artifacts.IssuedQuery{Namespace: artifacts.DefaultNamespace, RunID: r.runID, TaskID: mintTask})
	if err != nil {
		r.t.Fatalf("list issued: %v", err)
	}
	out := make([]artifacts.IssuedRecord, 0, len(page))
	for _, rr := range page {
		if rr.Answer != artifacts.RecordFound {
			r.t.Fatalf("an issued record is %v: %s %s", rr.Answer, rr.Object, rr.Reason)
		}
		out = append(out, rr.Value)
	}
	return out
}

// claims reads a token's payload; its signature is the unit suite's.
func claims(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var c map[string]any
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestIntegration_Mint_TheHolderIsGranted: the Orker the cluster says holds
// the attempt is issued its token, whose subject names the task and whose
// audience is the admitted one; the grant writes one record of what was
// issued, naming the caller, and never the token. A repeated request of the
// same attempt is granted again under the same slot.
func TestIntegration_Mint_TheHolderIsGranted(t *testing.T) {
	r := newMintRig(t, mintRigOptions{slots: 1, maxAttempts: 1})
	r.orker(honest)
	r.held(1)

	resp := r.granted(honest, r.request(1))
	tok := resp.GetTokens()[0]
	c := claims(t, tok.GetToken())
	if c["aud"] != "sts" || !strings.Contains(fmt.Sprint(c["sub"]), ":task:"+mintTask) || c["iss"] != mintIssuer {
		t.Fatalf("claims = %v", c)
	}
	recs := r.issued()
	if len(recs) != 1 || recs[0].OrkerID != honest || recs[0].Attempt != 1 || len(recs[0].Tokens) != 1 || recs[0].Tokens[0].JTI != tok.GetJti() {
		t.Fatalf("issued records = %+v", recs)
	}
	// A refresh: the slot the attempt holds is its own, and a cap of one
	// still grants it.
	r.granted(honest, r.request(1))
	if got := len(r.issued()); got != 2 {
		t.Fatalf("issued records after a refresh = %d, want 2", got)
	}
	if got := r.refusals(); len(got) != 0 {
		t.Fatalf("refusal records = %v", got)
	}
}

// TestIntegration_Mint_AStolenOrkerIsRefused: a credential that may mint at
// the task's pool, but is not the one the cluster says holds the attempt,
// is ATTEMPT_NOT_HELD; so is the holder asking for an attempt it does not
// hold. A credential of another pool is denied, and an audience the task
// was not admitted is refused before anything of the run is read. Only the
// denial is recorded: ATTEMPT_NOT_HELD is counted and logged, never
// recorded (it comes in bulk at a hand-over). None takes a slot: the
// holder is granted afterwards under a cap of one.
func TestIntegration_Mint_AStolenOrkerIsRefused(t *testing.T) {
	r := newMintRig(t, mintRigOptions{slots: 1, maxAttempts: 1})
	r.orker(honest)
	r.held(1)

	r.refused(thief, r.request(1), connect.CodeFailedPrecondition, identity.ReasonAttemptNotHeld)
	r.refused(honest, r.request(2), connect.CodeFailedPrecondition, identity.ReasonAttemptNotHeld)
	r.refused("orker-gpu", r.request(1), connect.CodePermissionDenied, "may not mint")
	other := r.request(1)
	other.Tokens = []*orkstackv1.TokenRequest{{Name: "aws", Audience: "vault"}}
	r.refused(honest, other, connect.CodeFailedPrecondition, identity.ReasonNotAdmitted)

	got := r.refusals()
	if len(got[identity.ReasonAttemptNotHeld]) != 0 || len(got[identity.ReasonNotAdmitted]) != 0 || len(got["permission_denied"]) != 1 {
		t.Fatalf("refusal records = %v", got)
	}
	if p := got["permission_denied"][0].GetPrincipal(); !strings.Contains(p, "orker-gpu") {
		t.Fatalf("the denial names %q", p)
	}
	// Control: the holder, in the same window, under a cap of one.
	r.granted(honest, r.request(1))
	if recs := r.issued(); len(recs) != 1 || recs[0].OrkerID != honest {
		t.Fatalf("issued records = %+v", recs)
	}
}

// TestIntegration_Cap_TheCallerThatTookTheSlotIsNamed: a task admitted one
// attempt that may hold an identity. An Orker the cluster handed attempt 1
// (a second Orker, standing for a holder of the cluster's Write that took
// a dispatch) spends the slot. Attempt 2, whichever Orker the cluster hands
// it (the honest one polls too), is then IDENTITY_ATTEMPTS_EXHAUSTED and
// recorded, and the record of what was issued names who took the slot.
func TestIntegration_Cap_TheCallerThatTookTheSlotIsNamed(t *testing.T) {
	r := newMintRig(t, mintRigOptions{slots: 1, maxAttempts: 2})
	r.orker(thief)
	if got := r.held(1); got != thief {
		t.Fatalf("attempt 1 is held by %s", got)
	}
	r.granted(thief, r.request(1))
	r.orker(honest)
	r.fail(1)
	second := r.held(2)

	r.refused(second, r.request(2), connect.CodeFailedPrecondition, identity.ReasonAttemptsExhausted)
	if got := r.refusals()[identity.ReasonAttemptsExhausted]; len(got) != 1 || !strings.Contains(got[0].GetPrincipal(), second) {
		t.Fatalf("exhausted records = %v", got)
	}
	recs := r.issued()
	if len(recs) != 1 || recs[0].OrkerID != thief || recs[0].Attempt != 1 || recs[0].Principal != "serviceaccount:"+thief {
		t.Fatalf("issued records = %+v", recs)
	}
}

// TestIntegration_Cap_ConcurrentFirstMintsTakeOneSlot: 200 first mints of
// one attempt at once, through the mint's own steps 4 to 8 (the handler's
// per-attempt rate limit would refuse most of them before they reach the
// cap), take one slot: under a cap of two, attempt 2 is still granted, and
// attempt 3 is then exhausted.
func TestIntegration_Cap_ConcurrentFirstMintsTakeOneSlot(t *testing.T) {
	r := newMintRig(t, mintRigOptions{slots: 2, maxAttempts: 3})
	r.orker(honest)
	r.held(1)

	mreq := mintRequestOf(r.request(1))
	admitted, err := r.minter.Admit(r.ctx, mreq)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	const n = 200
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			_, err := r.minter.Issue(r.ctx, admitted, mreq, identity.Caller{Principal: "serviceaccount:" + honest, OrkerID: honest})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	var failed []error
	for err := range errs {
		if err != nil {
			failed = append(failed, err)
		}
	}
	if len(failed) != 0 {
		t.Fatalf("%d of %d concurrent first mints failed, the first: %v", len(failed), n, errors.Join(failed[:1]...))
	}

	r.fail(1)
	r.held(2)
	r.granted(honest, r.request(2))
	r.fail(2)
	r.held(3)
	r.refused(honest, r.request(3), connect.CodeFailedPrecondition, identity.ReasonAttemptsExhausted)
}

// mintNamespace is this test's own Temporal namespace: the proof's prefix
// extended with the process, the test and a stamp (AGENTS.md §11.1).
func mintNamespace(t *testing.T, stamp string) string {
	prefix := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if prefix == "" {
		prefix = "ork-main"
	}
	name := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if len(name) > 60 {
		name = name[:60]
	}
	return prefix + "-" + name + "-" + stamp
}

func mintTemporal(ctx context.Context, t *testing.T, namespace string) client.Client {
	t.Helper()
	address := os.Getenv("ORK_TEMPORAL_ADDRESS")
	if address == "" {
		address = client.DefaultHostPort
	}
	bootstrap, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: client.DefaultNamespace})
	if err != nil {
		mintInfraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	if err := task.EnsureNamespace(ctx, bootstrap, namespace); err != nil {
		bootstrap.Close()
		t.Fatalf("provision namespace %q: %v", namespace, err)
	}
	bootstrap.Close()
	// A dial succeeds before the cluster's own services know a namespace
	// just registered, and a start then fails NamespaceNotFound. The
	// namespace is usable once describing an execution in it reaches
	// history and answers that the execution, not the namespace, is not
	// found.
	deadline := time.Now().Add(60 * time.Second)
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
		if err == nil {
			_, err = c.DescribeWorkflow(ctx, "mint-namespace-probe", "")
			var missing *serviceerror.NotFound
			if errors.As(err, &missing) {
				t.Cleanup(c.Close)
				return c
			}
			c.Close()
			if err == nil {
				err = errors.New("an execution no test started was described")
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %q did not become usable: %v", namespace, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// mintInfraAbsent is infraAbsent (infra_required_test.go) for this
// package's internal tests: a skip, or a failure under ORK_REQUIRE_INFRA.
func mintInfraAbsent(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
		t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: "+format, args...)
	}
	t.Skipf(format, args...)
}
