//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/typeid"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The identity train's end-to-end proof (docs/TASK_IDENTITY.md, "On the
// Orker" and "The mint"): one attempt of a task that declares an identity,
// run by a real Orker's dispatcher on the host's real container runtime,
// inside a real ExecuteTask activity on the proof's own Temporal, held by a
// worker whose identity is the Orker's. The dispatcher mints through the
// real IdentityService handler, whose mint reads the real cluster (who holds
// the attempt) and the real store (the admission, its spec digest, the
// slots, what was issued). The container reads its token where a workload
// does. What stands in: the mint is called in-process with the Orker's
// verified principal, as the authentication interceptor would set it, since
// that interceptor is not under test here; and the storage scope verdict is
// given as scoped, since proving an Orker's credential scoped needs a store
// that scopes by prefix (AGENTS.md §2.2), which the dev store is not.

// e2eImageTag is the reference image the dispatcher suite measures against.
const e2eImageTag = "docker.io/library/alpine:3.24"

// TestIntegration_IdentityEndToEnd_ARealMintThroughTheDispatcher: the
// container reads a JWT at /orkstack/identity/aws/token whose audience is
// the admitted one and whose subject names the task; task.json names
// paths.identity; the token is masked in the task's stdout; and nothing of
// the identity's files is left once the attempt is done.
func TestIntegration_IdentityEndToEnd_ARealMintThroughTheDispatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	cli := e2eContainerCLI(t)
	pinned := e2ePinnedReference(t, ctx, cli)
	_, indexDigest, _ := strings.Cut(pinned, "@")
	orkerID := proof.OrkerID("orker-identity-e2e")

	// The store, the cluster and the run, as the B1 rig builds them.
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
	temporal := mintTemporal(ctx, t, mintNamespace(t, stamp))
	runID := engine.NewRunID()
	wfQueue, actQueue := "e2e-wf-"+stamp, "e2e-act-"+stamp
	starts := identity.ClusterStarts{RecordStore: records, Temporal: temporal}

	// The task, from which both the admission's digest and the dispatched
	// input are made: what the Orker hashes is what the engine admitted.
	tk := &orkstackv1.Task{Id: mintTask, Pool: "main",
		Image: &orkstackv1.ImageRef{Reference: e2eImageTag, Pinned: pinned, IndexDigest: indexDigest},
		Command: []string{"/bin/sh", "-c",
			`tok="$(cat /orkstack/identity/aws/token)"; ` +
				`echo "the token is $tok"; ` +
				`printf '%s' "$tok" > /orkstack/output/artifacts/token.txt; ` +
				`cp /orkstack/input/task.json /orkstack/output/artifacts/task.json; ` +
				`printf '{}' > /orkstack/output/values.json`},
		Network:    orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts", TtlSeconds: 900}},
	}
	spec, _, err := task.IdentitySpecDigest(task.IdentitySpecVersion, task.IdentitySpecOfTask(tk))
	if err != nil {
		t.Fatal(err)
	}
	adm := artifacts.Admission{RunID: runID, Namespace: artifacts.DefaultNamespace, Orkflow: "deploy", OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: artifacts.Starter{Kind: artifacts.StarterEngine},
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(),
		EngineBuild: "e2e-test", SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: int(task.IdentitySpecVersion), Issuer: mintIssuer,
		NamespaceID: "ns_default", OrkflowID: "of_1", TokenTTL: 3600,
		Sets:  []artifacts.IdentitySet{{Identities: []artifacts.AdmittedIdentity{{Name: "aws", Audience: "sts", TTLSeconds: 900, Refresh: "required"}}}},
		Tasks: map[string]artifacts.AdmittedTask{mintTask: {Set: 1, Pool: "main", SpecSHA256: spec, IdentitySlots: 1, AttemptDeadline: 3600}}}
	body, sha, err := artifacts.EncodeAdmission(adm)
	if err != nil {
		t.Fatal(err)
	}
	key, err := artifacts.AdmissionKey(adm.Namespace, adm.RunID, sha)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), IfNoneMatch: aws.String("*")}); err != nil {
		t.Fatalf("write admission: %v", err)
	}

	// The engine's mint and its access, enforcing: the Orker holds the mint
	// and the commit at the task's pool.
	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewSigner(signingKey)
	if err != nil {
		t.Fatal(err)
	}
	minter := &identity.Minter{
		Stores: records,
		Starts: starts,
		Describe: func(ctx context.Context, runID string) (identity.ExecutionFacts, error) {
			return starts.Describe(ctx, runID, "")
		},
		Audiences: func(context.Context, string, string) ([]string, error) { return []string{"sts"}, nil },
		Signer:    func() (*jwt.Signer, error) { return signer, nil },
		Issuer:    mintIssuer, Deployment: "e2e-test",
		TTL:            identity.TTLBounds{Default: time.Hour, Min: 10 * time.Minute, Max: 12 * time.Hour},
		Skew:           time.Minute,
		RecordIssuance: true,
		Now:            time.Now,
		NewJTI:         typeid.New,
	}
	// The engine records the run's results (docs/TASK_IDENTITY.md, "Results
	// the engine records"): its real CommitResult, writing to the same store.
	recorder := &identity.Recorder{Stores: records, Starts: starts, Describe: minter.Describe,
		PinnedImage: func(context.Context, artifacts.Admission, string) (string, error) { return indexDigest, nil },
		EngineBuild: "e2e-test", Now: time.Now}
	svc := NewService(Config{AuditStore: artifacts.NewMemoryAuditStore(), Minter: minter, Recorder: recorder, Artifacts: cfg})
	access, _ := accessFor(t, authn.ModeEnforce,
		[]*orkstackv1.Role{{Name: "mints", Permissions: []string{"identity.token.mint", "task.result.commit"}}},
		&orkstackv1.Binding{Name: "orker", Role: "mints", Scope: "pool:main", Subjects: []string{"serviceaccount:" + orkerID}})
	svc.access = access
	asOrker := func(ctx context.Context) context.Context {
		return authn.WithPrincipal(authn.WithMode(ctx, authn.ModeEnforce), &authn.Principal{Kind: authn.KindService,
			ID: "serviceaccount:" + orkerID, Issuer: "serviceaccount", Method: authn.MethodServiceAccount, OrkerID: orkerID})
	}

	// The real Orker: its dispatcher on the host's runtime, its identity
	// settings, its self-check asked of the engine's real GetSignin, and its
	// mints answered by the engine's real MintAttemptTokens.
	workRoot := e2eWorkRoot(t, ctx)
	idCfg := orker.DefaultIdentityConfig()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	policy := orker.DefaultSecurityPolicy()
	commits, err := artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("commit store: %v", err)
	}
	commitRoot, err := artifacts.CommitRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	commitKey, err := artifacts.CommitKey(artifacts.DefaultNamespace, runID, mintTask)
	if err != nil {
		t.Fatal(err)
	}
	acts := orker.NewActivitiesWithOptions(store, orker.Options{
		WorkRoot: workRoot, Commits: commits, Refs: artifacts.MemoryRefPredictor{},
		Runtime: orker.NewDockerRuntime(), Policy: &policy, OrkerID: orkerID, BuildID: "e2e-test",
		Pool: "main", Identity: &idCfg,
	})
	support := orker.NewIdentitySupport(signinAs{svc: svc, ctx: asOrker}, orkerID, "main",
		func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	support.Step(ctx)
	if ok, why := support.Report(); !ok {
		t.Fatalf("the Orker's self-check, asked of the real GetSignin, did not grant support: %s", why)
	}
	acts.WatchIdentitySupport(support)
	dispatcher := acts.Dispatcher()
	dispatcher.SetIdentityMint(func(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		resp, err := svc.MintAttemptTokens(asOrker(ctx), connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	})
	dispatcher.SetResultCommit(func(ctx context.Context, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
		resp, err := svc.CommitResult(asOrker(ctx), connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	})

	in := &orkstackv1.ExecuteTaskInput{RunId: runID, OrkflowName: "deploy", OrkflowVersion: "1", Namespace: artifacts.DefaultNamespace,
		Pool: "main", TaskId: mintTask, IdempotencyKey: artifacts.DefaultNamespace + "/" + runID + "/" + mintTask,
		OrkflowDefinitionDigest: "sha256:" + strings.Repeat("d", 64),
		CommitUri:               strings.TrimSuffix(commitRoot, "/") + "/" + commitKey,
		Image:                   tk.GetImage(), Command: tk.GetCommand(), Network: tk.GetNetwork(),
		TimeoutSeconds: 120, CancellationGracePeriodSeconds: 2,
		Limits: &orkstackv1.WorkspaceLimits{ValuesMaxBytes: 262144, ArtifactMaxCount: 256, ArtifactMaxBytes: 1 << 30,
			LogMaxBytes: 16 << 20, CommitMaxBytes: 4 << 20},
		Identity: &orkstackv1.IdentityContext{Issuer: mintIssuer, Identities: tk.GetIdentities(), AdmissionSha256: sha,
			DigestVersion: task.IdentitySpecVersion},
	}

	// The cluster's side: the run's ExecuteTask, held by the Orker's worker,
	// whose activity is the dispatcher's real attempt.
	type outcome struct {
		result *orkstackv1.ExecuteTaskResult
		err    error
	}
	done := make(chan outcome, 1)
	ew := worker.New(temporal, wfQueue, worker.Options{Identity: "engine"})
	ew.RegisterWorkflow(mintRigWorkflow)
	if err := ew.Start(); err != nil {
		t.Fatalf("engine worker: %v", err)
	}
	t.Cleanup(ew.Stop)
	ow := worker.New(temporal, actQueue, worker.Options{Identity: orkerID})
	ow.RegisterActivityWithOptions(func(ctx context.Context) error {
		result, err := dispatcher.Execute(ctx, in)
		done <- outcome{result: result, err: err}
		return err
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := ow.Start(); err != nil {
		t.Fatalf("Orker worker: %v", err)
	}
	t.Cleanup(ow.Stop)
	run, err := temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: runID, TaskQueue: wfQueue}, mintRigWorkflow, actQueue, int32(1))
	if err != nil {
		t.Fatalf("start run %s: %v", runID, err)
	}
	t.Cleanup(func() { _ = temporal.TerminateWorkflow(context.Background(), runID, "", "the test is done") })
	if err := records.CompleteChain(ctx, artifacts.DefaultNamespace, runID,
		artifacts.Chain{FirstExecutionID: run.GetRunID(), AdmissionSHA256: sha}); err != nil {
		t.Fatalf("chain record: %v", err)
	}

	var got outcome
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatalf("the attempt never ended: %v", ctx.Err())
	}
	if got.err != nil {
		t.Fatalf("the attempt failed: %v", got.err)
	}
	refs := got.result.GetArtifacts()
	token := e2eArtifact(t, ctx, store, refs, "token.txt")
	c := claims(t, token)
	if c["aud"] != "sts" || !strings.Contains(fmt.Sprint(c["sub"]), ":task:"+mintTask) || c["iss"] != mintIssuer {
		t.Fatalf("the container's token's claims = %v", c)
	}
	var doc struct {
		Paths struct {
			Identity string `json:"identity"`
		} `json:"paths"`
		Identities []struct {
			Name     string `json:"name"`
			Audience string `json:"audience"`
			Path     string `json:"path"`
		} `json:"identities"`
	}
	if err := json.Unmarshal([]byte(e2eArtifact(t, ctx, store, refs, "task.json")), &doc); err != nil {
		t.Fatalf("task.json: %v", err)
	}
	if doc.Paths.Identity != "/orkstack/identity" || len(doc.Identities) != 1 || doc.Identities[0].Path != "/orkstack/identity/aws/token" {
		t.Fatalf("task.json names %+v", doc)
	}
	stdout := e2eArtifact(t, ctx, store, got.result.GetSystemArtifacts(), orker.StdoutLogName)
	if !strings.Contains(stdout, "the token is ") || strings.Contains(stdout, token) {
		t.Fatalf("the token is not masked in the task's stdout: %q", stdout)
	}
	// Nothing of the identity is left under the work root.
	_ = filepath.WalkDir(workRoot, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.Name() == "identity" && d.IsDir() {
			t.Errorf("an identity directory survived the attempt: %s", path)
		}
		return nil
	})
	// The result is the engine's record, under the attested prefix and
	// naming the Orker; nothing was written under metadata/commits/.
	attested, err := artifacts.AttestedCommitURI(in.GetCommitUri())
	if err != nil || got.result.GetCommit().GetUri() != attested {
		t.Fatalf("the result's commit = %v, want the engine's record at %s (%v)", got.result.GetCommit(), attested, err)
	}
	if rec, err := commits.Get(ctx, attested); err != nil || rec.Commit.GetRecorded().GetOrkerId() != orkerID {
		t.Fatalf("the engine's record = %v, %v", rec.Commit, err)
	}
	if _, err := commits.Get(ctx, in.GetCommitUri()); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a commit under metadata/commits/: %v", err)
	}
	// The engine's own record of what it issued names the Orker.
	page, _, err := records.ListIssued(ctx, artifacts.IssuedQuery{Namespace: artifacts.DefaultNamespace, RunID: runID, TaskID: mintTask})
	if err != nil || len(page) != 1 || page[0].Value.OrkerID != orkerID {
		t.Fatalf("issued records = %+v, %v", page, err)
	}
}

// signinAs is the engine's real GetSignin, asked as the Orker.
type signinAs struct {
	svc *Service
	ctx func(context.Context) context.Context
}

func (s signinAs) Whoami(ctx context.Context) (*orkstackv1.GetSigninResponse, error) {
	resp, err := authHandler{svc: s.svc}.GetSignin(s.ctx(ctx), connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func e2eArtifact(t *testing.T, ctx context.Context, store artifacts.Store, refs []*orkstackv1.ArtifactRef, name string) string {
	t.Helper()
	for _, ref := range refs {
		if ref.GetName() != name {
			continue
		}
		r, err := store.Get(ctx, ref.GetUri())
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		defer func() { _ = r.Close() }()
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return string(b)
	}
	t.Fatalf("no artifact %q among %v", name, refs)
	return ""
}

// e2eContainerCLI is the container CLI the Orker's runtime drives, as the
// dispatcher suite finds it.
func e2eContainerCLI(t *testing.T) string {
	t.Helper()
	name := strings.TrimSpace(os.Getenv(orker.DockerBinVar))
	if name == "" {
		name = orker.DefaultDockerBin
		if strings.EqualFold(strings.TrimSpace(os.Getenv(orker.CLIVar)), orker.CLIPodman) {
			name = orker.CLIPodman
		}
	}
	path, err := exec.LookPath(name)
	if err != nil {
		mintInfraAbsent(t, "the container CLI %q is not on PATH: %v", name, err)
	}
	return path
}

// e2ePinnedReference is the reference image by digest, pulled if absent.
func e2ePinnedReference(t *testing.T, ctx context.Context, cli string) string {
	t.Helper()
	if _, err := exec.CommandContext(ctx, cli, "image", "inspect", e2eImageTag).CombinedOutput(); err != nil {
		if out, err := exec.CommandContext(ctx, cli, "pull", "--quiet", e2eImageTag).CombinedOutput(); err != nil {
			mintInfraAbsent(t, "cannot pull %s: %v: %s", e2eImageTag, err, out)
		}
	}
	out, err := exec.CommandContext(ctx, cli, "image", "inspect", "--format", "{{index .RepoDigests 0}}", e2eImageTag).Output()
	if err != nil {
		id, idErr := exec.CommandContext(ctx, cli, "image", "inspect", "--format", "{{.Id}}", e2eImageTag).Output()
		if idErr != nil {
			mintInfraAbsent(t, "cannot resolve a digest for %s: %v", e2eImageTag, idErr)
		}
		return e2eImageTag + "@" + strings.TrimSpace(string(id))
	}
	return strings.TrimSpace(string(out))
}

// e2eWorkRoot is a work root of this test's own the daemon can bind, under
// the repository's .data/work as the dispatcher suite's first candidate.
func e2eWorkRoot(t *testing.T, ctx context.Context) string {
	t.Helper()
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	candidates := []string{filepath.Join(repoRoot, ".data", "work"), t.TempDir()}
	prober := &orker.OCIDispatcher{Runtime: orker.NewDockerRuntime(), Policy: orker.DefaultSecurityPolicy()}
	var reasons []string
	for _, candidate := range candidates {
		if err := os.MkdirAll(candidate, 0o755); err != nil {
			reasons = append(reasons, candidate+": "+err.Error())
			continue
		}
		if err := prober.ProbeRuntime(ctx, candidate); err != nil {
			if errors.Is(err, orker.ErrPostureRefused) {
				t.Fatalf("the boot probe refused this host's container runtime under %s: %v", candidate, err)
			}
			reasons = append(reasons, candidate+": "+err.Error())
			continue
		}
		root, err := os.MkdirTemp(candidate, proof.WorkRootPattern("ork-identity-e2e"))
		if err != nil {
			reasons = append(reasons, candidate+": "+err.Error())
			continue
		}
		t.Cleanup(func() { _ = os.RemoveAll(root) })
		return root
	}
	mintInfraAbsent(t, "no bind-mountable work root:\n  %s", strings.Join(reasons, "\n  "))
	return ""
}
