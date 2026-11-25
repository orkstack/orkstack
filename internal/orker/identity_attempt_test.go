package orker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func identityInput() *orkstackv1.ExecuteTaskInput {
	return &orkstackv1.ExecuteTaskInput{
		Namespace: "ns", RunId: "ork-1", TaskId: "deploy",
		Image:   &orkstackv1.ImageRef{Pinned: "ghcr.io/acme/deploy@sha256:" + strings.Repeat("a", 64)},
		Command: []string{"deploy"},
		Each:    &orkstackv1.EachContext{Index: 4, Count: 9},
		Identity: &orkstackv1.IdentityContext{Issuer: "https://ork.example", DigestVersion: 1, AdmissionSha256: strings.Repeat("b", 64),
			RootScope: 3, ResumeScope: 2,
			Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}, {Name: "gcp", Audience: "gcp", Refresh: orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT}}},
	}
}

// TestIdentityRequest: the mint request names the attempt, its item, the
// task's spec digest and its members, the admission, both scopes, and each
// identity's name and audience; an Orker that does not compute the digest
// version fails IDENTITY_UNAVAILABLE.
func TestIdentityRequest(t *testing.T) {
	in := identityInput()
	req, f := identityRequest(in, 2)
	if f != nil {
		t.Fatal(f)
	}
	digest, members, _ := task.IdentitySpecDigest(1, task.IdentitySpecOfInput(in))
	if req.GetRunId() != "ork-1" || req.GetTaskId() != "deploy" || req.GetNamespace() != "ns" || req.GetAttempt() != 2 ||
		req.Item == nil || req.GetItem() != 4 || req.GetTaskSpecSha256() != digest || len(req.GetSpecMembers()) != len(members) ||
		req.GetAdmissionSha256() != strings.Repeat("b", 64) || req.GetRootScope() != 3 || req.GetResumeScope() != 2 ||
		len(req.GetTokens()) != 2 || req.GetTokens()[1].GetName() != "gcp" || req.GetTokens()[1].GetAudience() != "gcp" {
		t.Fatalf("request %v", req)
	}
	in.Identity.DigestVersion = 9
	if _, f := identityRequest(in, 2); f == nil || f.Reason != "IDENTITY_UNAVAILABLE" {
		t.Fatalf("an unknown version: %v", f)
	}
}

// TestSetupIdentities: an attempt's identities are made under the chosen
// storage, written before the container exists, and removed with the
// attempt; a failed setup leaves nothing.
func TestSetupIdentities(t *testing.T) {
	store := filepath.Join(t.TempDir(), "ids")
	if _, _, err := ensureOwnIdentityDir(mkdirAll(t, store), "orker-a", false); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultIdentityConfig()
	var asked *orkstackv1.MintAttemptTokensRequest
	d := &OCIDispatcher{OrkerID: "orker-a", BuildID: "b1", identityConfig: &cfg,
		identityMint: func(_ context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
			asked = req
			resp := &orkstackv1.MintAttemptTokensResponse{}
			for _, tok := range req.GetTokens() {
				resp.Tokens = append(resp.Tokens, &orkstackv1.IssuedToken{Name: tok.GetName(), Audience: tok.GetAudience(),
					Token: "eyJhbGciOiJFUzI1NiJ9.payload-of-" + tok.GetName() + ".signature-bytes", ExpiresInSeconds: 600, IssuedAt: timestamppb.Now()})
			}
			return resp, nil
		}}
	d.identityStorage.Store(&identityStorageChoice{Dir: store, Kind: identityStorageMemory})
	wsRoot := filepath.Join(t.TempDir(), "ork-ork-1-deploy-2-abc")
	if err := os.Mkdir(wsRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	mask := d.newAttemptMask(nil)
	ids, f := d.setupIdentities(context.Background(), identityInput(), 2, wsRoot, mask)
	if f != nil {
		t.Fatal(f)
	}
	if asked == nil || asked.GetAttempt() != 2 {
		t.Fatalf("asked %v", asked)
	}
	if !strings.HasPrefix(ids.dir, filepath.Join(store, "orker-a")+string(filepath.Separator)) {
		t.Fatalf("identity dir %s is not under the chosen storage", ids.dir)
	}
	t.Cleanup(func() { _ = ids.remove() })
	if got := readThrough(t, filepath.Join(ids.dir, "aws"), "token"); got != "eyJhbGciOiJFUzI1NiJ9.payload-of-aws.signature-bytes" {
		t.Fatalf("token %q", got)
	}
	assertMode(t, ids.dir, 0o555)
	if !ids.refresher.isRequired("aws") || ids.refresher.isRequired("gcp") {
		t.Fatal("the refresh policies were not carried")
	}
	if got := mask.text("printed eyJhbGciOiJFUzI1NiJ9.payload-of-aws.signature-bytes"); strings.Contains(got, "payload-of-aws") {
		t.Fatalf("the token is not masked in the task's logs: %q", got)
	}
	if err := ids.remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(ids.base); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the attempt's identity directory survived: %v", err)
	}

	d.identityMint = func(context.Context, *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("IDENTITY_ATTEMPTS_EXHAUSTED: x"))
	}
	if _, f := d.setupIdentities(context.Background(), identityInput(), 2, wsRoot, d.newAttemptMask(nil)); f == nil || f.Reason != "IDENTITY_ATTEMPTS_EXHAUSTED" {
		t.Fatalf("a refused setup: %v", f)
	}
	if entries, _ := os.ReadDir(filepath.Join(store, "orker-a")); len(entries) != 1 || entries[0].Name() != identityDirMarker {
		t.Fatalf("a failed setup left %v", entries)
	}
}

// identityReadingRuntime is the phase runtime whose container, at create,
// reads its token through the identity bind and its task.json, as a
// workload would.
type identityReadingRuntime struct {
	phaseCapturingRuntime
	token    string
	taskJSON string
	mounts   []Mount
}

func (r *identityReadingRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mounts = spec.Mounts
	for _, m := range spec.Mounts {
		switch m.Destination {
		case identityMountPath:
			b, err := os.ReadFile(filepath.Join(m.Source, "aws", "token"))
			if err != nil {
				return "", err
			}
			r.token = string(b)
		case ContainerInput:
			b, err := os.ReadFile(filepath.Join(m.Source, "task.json"))
			if err != nil {
				return "", err
			}
			r.taskJSON = string(b)
		}
	}
	return r.phaseCapturingRuntime.Create(ctx, spec)
}

// TestExecute_AnIdentityTask: an attempt of a task that declares an
// identity mints before its container exists, binds the files read-only at
// /orkstack/identity, names them in task.json, and removes them with the
// attempt; a task with none mints nothing and binds nothing.
func TestExecute_AnIdentityTask(t *testing.T) {
	rt := &identityReadingRuntime{}
	d := newAttemptDispatcher(t, rt, artifacts.NewMemoryAttemptStore())
	cfg := DefaultIdentityConfig()
	d.identityConfig = &cfg
	d.identity.Store(identitySupportOf(true))
	var mints int
	d.identityMint = func(_ context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
		mints++
		return &orkstackv1.MintAttemptTokensResponse{Tokens: []*orkstackv1.IssuedToken{{Name: "aws", Audience: "sts",
			Token: "eyJhbGciOiJFUzI1NiJ9.an-identity-token.sig", ExpiresInSeconds: 3600, IssuedAt: timestamppb.Now()}}}, nil
	}
	in := baseExportInput("deploy")
	in.Identity = &orkstackv1.IdentityContext{DigestVersion: 1, Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}}}
	// A run that holds an identity has its result recorded by the engine.
	commits := artifacts.NewMemoryCommitStore()
	d.Commits = commits
	d.SetResultCommit((&recordingEngine{store: commits, root: "mem://store/"}).commit)
	key, err := artifacts.CommitKey(in.GetNamespace(), in.GetRunId(), in.GetTaskId())
	if err != nil {
		t.Fatal(err)
	}
	in.CommitUri = "mem://store/" + key
	if _, err := d.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute = %v", err)
	}
	if mints != 1 || rt.token != "eyJhbGciOiJFUzI1NiJ9.an-identity-token.sig" {
		t.Fatalf("mints %d, the container read %q", mints, rt.token)
	}
	var bound *Mount
	for i := range rt.mounts {
		if rt.mounts[i].Destination == identityMountPath {
			bound = &rt.mounts[i]
		}
	}
	if bound == nil || !bound.ReadOnly {
		t.Fatalf("identity mount %+v", bound)
	}
	if _, err := os.Lstat(bound.Source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the identity files survived the attempt: %v", err)
	}
	for _, want := range []string{`"identity":"/orkstack/identity"`, `"path":"/orkstack/identity/aws/token"`, `"audience":"sts"`} {
		if !strings.Contains(strings.ReplaceAll(rt.taskJSON, " ", ""), want) {
			t.Errorf("task.json lacks %s:\n%s", want, rt.taskJSON)
		}
	}
	if strings.Contains(rt.taskJSON, "an-identity-token") {
		t.Fatal("task.json holds a token")
	}

	rt2 := &identityReadingRuntime{}
	d2 := newAttemptDispatcher(t, rt2, artifacts.NewMemoryAttemptStore())
	d2.identityMint = d.identityMint
	if _, err := d2.Execute(context.Background(), baseExportInput("plain")); err != nil {
		t.Fatalf("Execute = %v", err)
	}
	if mints != 1 || strings.Contains(rt2.taskJSON, "identity") {
		t.Fatalf("a task with no identity: mints %d, task.json %s", mints, rt2.taskJSON)
	}
	for _, m := range rt2.mounts {
		if m.Destination == identityMountPath {
			t.Fatal("a task with no identity was given the identity mount")
		}
	}
}

func mkdirAll(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}
