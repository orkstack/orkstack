package engine

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIdentitySpecDigest_FromTheInterpreterInput is the second of the spec
// digest's three ways (docs/TASK_IDENTITY.md, "Which run a token is for"):
// each task's digest computed from the input this interpreter builds for it
// is the one the engine computes from its definition at submit, so an Orker
// that hashes its input agrees with the admission. The identities, the
// credentials and the grants reach it in its identity context.
func TestIdentitySpecDigest_FromTheInterpreterInput(t *testing.T) {
	with, err := structpb.NewStruct(map[string]any{"count": 3, "ratio": 0.5, "dry": false, "targets": []any{"a", "b"},
		"nested": map[string]any{"k": "v", "empty": map[string]any{}}, "none": nil})
	if err != nil {
		t.Fatal(err)
	}
	pinned := &orkstackv1.ImageRef{Reference: "ghcr.io/acme/migrate:v3", Pinned: "ghcr.io/acme/migrate@sha256:" + strings.Repeat("a", 64)}
	tasks := []*orkstackv1.Task{
		{Id: "minimal", Image: pinned},
		{Id: "network-default", Image: pinned, Network: orkstackv1.NetworkMode_NETWORK_MODE_DEFAULT},
		{Id: "full", Image: pinned, Command: []string{"sh", "-c", "migrate"}, Env: map[string]string{"MODE": "apply", "REGION": "eu-west-1"},
			With: with, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE, RunAsOrkerUser: true, Pool: "payments", TimeoutSeconds: 600,
			Secrets: []*orkstackv1.SecretRef{{Name: "token", Source: "env:DEPLOY_TOKEN", Unmasked: true},
				{Name: "db-url", Source: "vault:secret/data/payments/db#url", Grant: "db", Ciphertext: "SGVsbG8="}},
			Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts.amazonaws.com", TtlSeconds: 3600},
				{Name: "vault", Audience: "vault.internal", TtlSeconds: 900, Refresh: orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT}},
			Credentials: []*orkstackv1.CredentialRef{{Name: "cert", Grant: "pki", Path: "pki/issue/payments",
				Params: map[string]string{"common_name": "payments.internal", "ttl": "12h"}}},
			Devices: []string{"gpu-a", "fpga"}, DeviceContracts: map[string]string{"gpu-a": "v2"}},
	}
	wf := &orkstackv1.Orkflow{Name: "spec", Version: "1", Namespace: "default", Tasks: tasks}
	s := &runState{runID: "ork-spec", wf: wf, tasks: map[string]*orkstackv1.Task{}, plan: RunPlan{Namespace: "default",
		ProfileIdentities: map[string]map[string]string{"payments": {"gpu-a": strings.Repeat("e", 64), "fpga": strings.Repeat("f", 64)}},
		ProfileContracts: map[string]map[string]ProfileContracts{"payments": {
			"gpu-a": {Declared: map[string][]string{"v2": {strings.Repeat("c", 64)}}},
			"fpga":  {Set: []ContractPin{{Label: "v1"}}}}}}}
	s.plan.Identity = &IdentityPlan{AdmissionSHA256: strings.Repeat("c", 64), Issuer: "https://ork.example", SpecVersion: 1,
		Grants: map[string]artifacts.GrantSelection{
			"db":    {Provider: "vault", Principal: "payments-app", Audience: "vault.internal"},
			"pki":   {Provider: "vault", Principal: "pki", Audience: "vault.internal"},
			"other": {Provider: "aws", Principal: "role/x", Audience: "sts"}}}
	for _, tk := range tasks {
		s.tasks[tk.GetId()] = tk
	}
	for _, tk := range tasks {
		want, _, err := task.IdentitySpecDigest(1, task.IdentitySpecOfTask(tk, IdentityGrants(tk, s.plan.Identity.Grants)))
		if err != nil {
			t.Fatal(err)
		}
		in := s.executeTaskInput(tk, tk.GetId(), -1, tk.GetPool(), nil)
		// The profiles reach the input at dispatch, as taskDispatch sets them.
		if in.Devices, err = s.deviceRefs(tk.GetPool(), tk); err != nil {
			t.Fatal(err)
		}
		got, gotMembers, err := task.IdentitySpecDigest(1, task.IdentitySpecOfInput(in))
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			_, wantMembers, _ := task.IdentitySpecDigest(1, task.IdentitySpecOfTask(tk, IdentityGrants(tk, s.plan.Identity.Grants)))
			var differs []string
			for _, m := range task.IdentitySpecMembers {
				if gotMembers[m] != wantMembers[m] {
					differs = append(differs, m)
				}
			}
			t.Errorf("task %q: the interpreter's input hashes to %s, the definition to %s; differs in %v", tk.GetId(), got, want, differs)
		}
	}
}

// TestExecuteTaskInput_IdentityContext: every task of a run its plan admitted
// is dispatched with what its attempt needs of the engine: the admission, the
// spec's version and the wait at its commit, which a task that holds no
// identity needs too, since the engine records its result; and its own
// identities and credentials, copied verbatim. A task of a run with no
// admission is dispatched with none.
func TestExecuteTaskInput_IdentityContext(t *testing.T) {
	ids := []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts"}}
	creds := []*orkstackv1.CredentialRef{{Name: "cert", Grant: "pki", Path: "pki/issue/x"}}
	tasks := []*orkstackv1.Task{
		{Id: "holds", Identities: ids},
		{Id: "creds", Credentials: creds},
		{Id: "plain"},
		{Id: "reads", Secrets: []*orkstackv1.SecretRef{{Name: "own", Source: "env:OWN"}, {Name: "db", Source: "vault:secret/db#url", Grant: "db"}}},
	}
	plan := RunPlan{Namespace: "default", Identity: &IdentityPlan{AdmissionSHA256: strings.Repeat("c", 64), Issuer: "https://ork.example",
		TokenTTLSeconds: 900, SpecVersion: 1, CommitWaitSeconds: 60, Grants: map[string]artifacts.GrantSelection{
			"db":  {Provider: "vault", Principal: "payments-app", Audience: "vault.internal"},
			"pki": {Provider: "vault", Principal: "pki", Audience: "vault.internal"}}}}
	wantGrants := map[string]*orkstackv1.IdentityGrant{
		"creds": {Name: "pki", Provider: "vault", Principal: "pki", Audience: "vault.internal"},
		"reads": {Name: "db", Provider: "vault", Principal: "payments-app", Audience: "vault.internal"},
	}
	s := &runState{runID: "ork-id", wf: &orkstackv1.Orkflow{Name: "w", Version: "1", Tasks: tasks}, tasks: map[string]*orkstackv1.Task{}, plan: plan}
	for _, tk := range tasks {
		got := s.executeTaskInput(tk, tk.GetId(), -1, "", nil).GetIdentity()
		if got.GetIssuer() != "https://ork.example" || got.GetAdmissionSha256() != strings.Repeat("c", 64) || got.GetDigestVersion() != 1 ||
			got.GetCommitWaitSeconds() != 60 || got.GetRootScope() != 0 || got.GetResumeScope() != 0 ||
			len(got.GetIdentities()) != len(tk.GetIdentities()) || len(got.GetCredentials()) != len(tk.GetCredentials()) {
			t.Errorf("task %q: %v", tk.GetId(), got)
		}
		switch want := wantGrants[tk.GetId()]; {
		case want == nil && len(got.GetGrants()) != 0:
			t.Errorf("task %q uses no grant and has %v", tk.GetId(), got.GetGrants())
		case want != nil && (len(got.GetGrants()) != 1 || !proto.Equal(got.GetGrants()[0], want)):
			t.Errorf("task %q's grants = %v; want only %v", tk.GetId(), got.GetGrants(), want)
		}
	}
	s.plan.Identity = nil
	if got := s.executeTaskInput(tasks[0], "holds", -1, "", nil).GetIdentity(); got != nil {
		t.Fatalf("a run with no admission: %v", got)
	}
}

// TestExecuteTaskInput_AnUpstreamsCommitUriIsWhatItsResultReported: a
// dependent is dispatched with the commit uri its producer's result named,
// copied as it is. For a run whose results the engine records that is the
// record's, under the attested prefix, and the Orker's staging holds it to
// exactly that (orker.TestRecordedRun_ADependentStagesWhatItsProducerReported).
func TestExecuteTaskInput_AnUpstreamsCommitUriIsWhatItsResultReported(t *testing.T) {
	const reported = "s3://bucket/metadata/attested/default/ork-up/apply/commit.json"
	tasks := []*orkstackv1.Task{{Id: "apply"}, {Id: "report", Needs: []string{"apply"}}}
	s := &runState{runID: "ork-up", wf: &orkstackv1.Orkflow{Name: "w", Version: "1", Tasks: tasks},
		tasks:   map[string]*orkstackv1.Task{"apply": tasks[0], "report": tasks[1]},
		plan:    RunPlan{Namespace: "default", CommitRoot: "s3://bucket"},
		results: map[string]*orkstackv1.ExecuteTaskResult{"apply": {TaskId: "apply", Commit: &orkstackv1.CommitRef{Uri: reported}}}}
	up := s.executeTaskInput(tasks[1], "report", -1, "", nil).GetUpstream()
	if len(up) != 1 || up[0].GetTaskId() != "apply" || up[0].GetCommitUri() != reported {
		t.Fatalf("upstream = %v; want apply at %s", up, reported)
	}
}
