package api

import (
	"context"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// errPulled is where pullStops ends an attempt: everything before the image
// step, staging included, has passed.
var errPulled = errors.New("the attempt reached its image")

// pullStops is a runtime that only says the image step was reached.
type pullStops struct{ orker.ContainerRuntime }

func (pullStops) Pull(context.Context, string) (orker.PullInfo, error) {
	return orker.PullInfo{}, errPulled
}

func (pullStops) LoadLayout(context.Context, string, string) (orker.PullInfo, error) {
	return orker.PullInfo{}, errPulled
}

// orkerSignin asks the service who an Orker is, as the Orker's self-check.
type orkerSignin struct{ svc *Service }

func (s orkerSignin) Whoami(context.Context) (*orkstackv1.GetSigninResponse, error) {
	resp, err := authHandler{svc: s.svc}.GetSignin(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// TestCommitResult_ADependentStagesFromItsAnswer: the commit CommitResult
// answers is what the producer's result reports and the interpreter hands
// the dependent, so the dependent's real Orker stages from exactly it; the
// key an Orker would have committed at, in the same input, is refused before
// any image is pulled.
func TestCommitResult_ADependentStagesFromItsAnswer(t *testing.T) {
	ctx := context.Background()
	s, req, _ := commitService(t, []*orkstackv1.Role{{Name: "commits", Permissions: []string{"identity.token.mint", "task.result.commit"}}},
		commitBinding("pool:main"))
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	values, err := store.Put(ctx, "values.json", strings.NewReader(`{"ok":true}`), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	req.Commit.Values = &orkstackv1.ArtifactRef{Name: "values.json", Uri: values.URI, Sha256: values.SHA256, SizeBytes: values.SizeBytes}
	answer, err := s.CommitResult(asOrker(authn.ModeEnforce, anOrker), connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}

	commits, err := artifacts.NewCommitStore(ctx, s.artifacts)
	if err != nil {
		t.Fatal(err)
	}
	root, err := artifacts.CommitRoot(s.artifacts)
	if err != nil {
		t.Fatal(err)
	}
	root = strings.TrimSuffix(root, "/")
	policy, idCfg := orker.DefaultSecurityPolicy(), orker.DefaultIdentityConfig()
	acts := orker.NewActivitiesWithOptions(store, orker.Options{WorkRoot: t.TempDir(), Commits: commits, Refs: artifacts.MemoryRefPredictor{},
		Runtime: pullStops{}, Policy: &policy, OrkerID: "orker-1", BuildID: "b7", Pool: "main", Identity: &idCfg})
	support := orker.NewIdentitySupport(orkerSignin{s}, "orker-1", "main",
		func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	support.Step(ctx)
	if ok, why := support.Report(); !ok {
		t.Fatalf("the Orker's self-check did not grant support: %s", why)
	}
	acts.WatchIdentitySupport(support)
	// The dependent's own preflight is not under test: the admission holds
	// its producer alone.
	acts.Dispatcher().SetResultCommit(func(context.Context, *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
		return &orkstackv1.CommitResultResponse{}, nil
	})

	key, err := artifacts.CommitKey("default", "ork-1", "report")
	if err != nil {
		t.Fatal(err)
	}
	dispatched := func(upstreamURI string) *orkstackv1.ExecuteTaskInput {
		return &orkstackv1.ExecuteTaskInput{RunId: "ork-1", OrkflowName: "deploy", OrkflowVersion: "1", Namespace: "default", Pool: "main",
			TaskId: "report", IdempotencyKey: task.IdempotencyKeyFor("default", "ork-1", "report"), CommitUri: root + "/" + key,
			OrkflowDefinitionDigest: req.GetCommit().GetOrkflowDefinitionDigest(), TimeoutSeconds: 30, CancellationGracePeriodSeconds: 1,
			Image: &orkstackv1.ImageRef{Reference: "example/image:1", Pinned: "example/image@sha256:" + strings.Repeat("a", 64),
				IndexDigest: "sha256:" + strings.Repeat("a", 64)},
			Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
			Limits: &orkstackv1.WorkspaceLimits{ValuesMaxBytes: 262144, ArtifactMaxCount: 256, ArtifactMaxBytes: 1 << 30, LogMaxBytes: 16 << 20,
				CommitMaxBytes: 4 << 20},
			Identity: &orkstackv1.IdentityContext{AdmissionSha256: req.GetAdmissionSha256(), DigestVersion: task.IdentitySpecVersion, CommitWaitSeconds: 60},
			Upstream: []*orkstackv1.UpstreamRef{{TaskId: "build", CommitUri: upstreamURI, Values: req.GetCommit().GetValues()}}}
	}
	if _, err := acts.Dispatcher().Execute(ctx, dispatched(answer.Msg.GetCommit().GetUri())); err == nil || !strings.Contains(err.Error(), errPulled.Error()) {
		t.Fatalf("the dependent of %s = %v; want it staged and at its image", answer.Msg.GetCommit().GetUri(), err)
	}
	if _, err := acts.Dispatcher().Execute(ctx, dispatched(root+"/"+req.GetCommitKey())); err == nil ||
		!strings.Contains(err.Error(), orker.ReasonUpstreamNotRecorded) || strings.Contains(err.Error(), errPulled.Error()) {
		t.Fatalf("the dependent of the key an Orker commits at = %v; want %s", err, orker.ReasonUpstreamNotRecorded)
	}
}
