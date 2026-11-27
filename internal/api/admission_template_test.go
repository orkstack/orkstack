package api_test

import (
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestAdmissionTemplate_IsTheDefinitionsOwn: what an admission says of a
// definition is the definition's own, as the commit check compares it: its
// name, version and digest, a row for each task that runs a container, with
// its pool and the digest of its spec, and what each task follows; and the
// record Admitted makes of it names the run and draws a nonce of its own.
func TestAdmissionTemplate_IsTheDefinitionsOwn(t *testing.T) {
	wf, err := dsl.Compile([]byte(`apiVersion: orkstack/v1
kind: Orkflow
name: admitted
version: "1.0.0"
tasks:
  - id: approval
    await:
      timeout_seconds: 60
  - id: apply
    needs:
      - approval
    uses: shell
    pool: real
    with:
      op: exec
`))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		t.Fatal(err)
	}
	wf.DefinitionDigest = digest
	common := artifacts.Admission{Version: 1, Namespace: "default", CommitWait: 30, SpecVersion: int(task.IdentitySpecVersion)}
	a, err := api.AdmissionTemplate(common, wf, &engine.RunPlan{}, identity.DefaultAttemptsMax)
	if err != nil {
		t.Fatal(err)
	}
	var apply *orkstackv1.Task
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == "apply" {
			apply = tk
		}
	}
	spec, _, err := task.IdentitySpecDigest(task.IdentitySpecVersion, task.IdentitySpecOfTask(apply, nil))
	if err != nil {
		t.Fatal(err)
	}
	if a.DefinitionDigest != strings.TrimPrefix(digest, "sha256:") || a.Orkflow != "admitted" || a.OrkflowVersion != "1.0.0" || a.CommitWait != 30 {
		t.Fatalf("admission = %+v", a)
	}
	if len(a.Tasks) != 1 || a.Tasks["apply"].SpecSHA256 != spec || a.Tasks["apply"].Pool != "real" {
		t.Fatalf("tasks = %+v; want apply's row alone", a.Tasks)
	}
	if f := a.Follows["apply"]; len(f) != 1 || f[0].TaskID != "approval" || f[0].Kind != artifacts.FollowAwaited {
		t.Fatalf("follows = %+v", a.Follows)
	}
	wf.DefinitionDigest = "md5:x"
	if _, err := api.AdmissionTemplate(common, wf, &engine.RunPlan{}, identity.DefaultAttemptsMax); connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("a definition with no SHA-256 digest: %v, want an internal error", err)
	}
	wf.DefinitionDigest = digest
	// A task holding an identity whose attempts would pass identity.attempts.max.
	apply.Identities = []*orkstackv1.IdentityRef{{Name: "deploy", Audience: "aud"}}
	apply.Retry = &orkstackv1.RetryPolicy{MaximumAttempts: 3}
	if _, err := api.AdmissionTemplate(common, wf, &engine.RunPlan{}, 1); connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "identity.attempts.max") {
		t.Fatalf("over identity.attempts.max: %v, want a refusal", err)
	}
	apply.Identities, apply.Retry = nil, nil
	at := time.Now()
	one, err := api.Admitted(a, "ork-1", "", artifacts.Starter{Kind: artifacts.StarterEngine}, at)
	if err != nil {
		t.Fatal(err)
	}
	two, _ := api.Admitted(a, "ork-1", "", artifacts.Starter{Kind: artifacts.StarterEngine}, at)
	if one.RunID != "ork-1" || one.Starter.Kind != artifacts.StarterEngine || !one.AdmittedAt.Equal(at) || len(one.Nonce) != 32 || one.Nonce == two.Nonce {
		t.Fatalf("admitted = %+v, %+v", one, two)
	}
	if a.RunID != "" || a.Nonce != "" || a.Children != nil {
		t.Fatalf("Admitted changed its template: %+v", a)
	}
	if p := api.IdentityPlanOf(one, strings.Repeat("a", 64), false); p.AdmissionSHA256 != strings.Repeat("a", 64) || p.CommitWaitSeconds != 30 {
		t.Fatalf("plan = %+v", p)
	}
}
