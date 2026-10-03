//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// automationStack is the origin stack — two real `ork` replicas and one
// Orker on the Bash fixture, a private Temporal namespace, the shared MinIO
// bucket, an OrkStack namespace of the test's own holding the orkflow
// deploy-service — plus a directory for documents and a webhook receiver.
type automationStack struct {
	*originStack
	dir      string
	receiver *webhookReceiver
}

func newAutomationStack(ctx context.Context, t *testing.T, label string) *automationStack {
	t.Helper()
	s := &automationStack{originStack: newOriginStack(ctx, t, "auto-"+label), dir: t.TempDir(), receiver: newWebhookReceiver(t)}
	fmt.Fprintf(&s.transcript, "# declarative automation: %s\n\n", label)
	return s
}

// webhookReceiver records every POST the engine delivers.
type webhookReceiver struct {
	server *httptest.Server
	mu     sync.Mutex
	posts  []receivedPost
}

type receivedPost struct {
	path    string
	body    []byte
	headers http.Header
}

func newWebhookReceiver(t *testing.T) *webhookReceiver {
	t.Helper()
	r := &webhookReceiver{}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.posts = append(r.posts, receivedPost{path: req.URL.Path, body: body, headers: req.Header.Clone()})
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(r.server.Close)
	return r
}

// await returns the first POST to path whose body names runID.
func (r *webhookReceiver) await(t *testing.T, path, runID string) receivedPost {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for _, post := range r.posts {
			if post.path == path && bytes.Contains(post.body, []byte(runID)) {
				r.mu.Unlock()
				return post
			}
		}
		r.mu.Unlock()
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("no delivery to %s for run %s arrived", path, runID)
	return receivedPost{}
}

// write puts a document in the stack's directory and returns its path.
func (s *automationStack) write(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(s.dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	fmt.Fprintf(&s.transcript, "$ cat %s\n%s\n", name, content)
	return path
}

// doc is an automation document for the stack's namespace.
func (s *automationStack) doc(body string) string {
	return fmt.Sprintf("apiVersion: orkstack/v1\nkind: Automation\nnamespace: %s\n%s", s.namespace, body)
}

// applyReport runs `apply --json ARGS` on a replica and decodes the report.
func (s *automationStack) applyReport(t *testing.T, replica *engineReplica, want int, args ...string) *orkstackv1.ApplyReport {
	t.Helper()
	out := s.cli(t, replica, want, append([]string{"--json", "apply"}, args...)...)
	var report orkstackv1.ApplyReport
	if err := api.UnmarshalJSON([]byte(out), &report); err != nil {
		t.Fatalf("decode ApplyReport: %v\n%s", err, out)
	}
	return &report
}

// invoke posts to a trigger's URL with a token and returns the status and body.
func (s *automationStack) invoke(ctx context.Context, t *testing.T, replica *engineReplica, id, token string) (int, string) {
	t.Helper()
	url := "http://" + replica.addr + strings.Replace(api.TriggerInvokePath, "{id}", id, 1)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	fmt.Fprintf(&s.transcript, "$ curl -X POST %s -H 'Authorization: Bearer <token>' -d '{}'\n%s(HTTP %d)\n\n", url, body, response.StatusCode)
	return response.StatusCode, string(body)
}

// triggerByName lists the namespace's triggers and returns the named one.
func (s *automationStack) triggerByName(t *testing.T, replica *engineReplica, name string) *orkstackv1.Trigger {
	t.Helper()
	var listed orkstackv1.ListTriggersResponse
	if err := api.UnmarshalJSON([]byte(s.cli(t, replica, exitOK, "--json", "trigger", "list")), &listed); err != nil {
		t.Fatalf("decode trigger list: %v", err)
	}
	for _, trigger := range listed.GetTriggers() {
		if trigger.GetName() == name {
			return trigger
		}
	}
	return nil
}

// disclosed returns the one-time secret the report recorded for name.
func disclosed(t *testing.T, report *orkstackv1.ApplyReport, name string) (id, secret string) {
	t.Helper()
	for _, doc := range report.GetAutomation() {
		for _, result := range doc.GetResults() {
			step := doc.GetPlan().GetSteps()[result.GetStep()]
			if step.GetName() == name && result.GetSecret() != "" {
				return result.GetId(), result.GetSecret()
			}
		}
	}
	t.Fatalf("the apply disclosed no secret for %s", name)
	return "", ""
}

// metadataSnapshot lists the registry keys an apply could touch, with their
// ETags, and the cluster's schedules: what a dry run must leave identical.
func (s *automationStack) metadataSnapshot(ctx context.Context, t *testing.T) []string {
	t.Helper()
	raw := auditRawS3Client(t, s.artifactCfg)
	var out []string
	for _, prefix := range []string{"metadata/namespaces/", "metadata/triggers/", "metadata/webhooks/"} {
		pager := s3.NewListObjectsV2Paginator(raw, &s3.ListObjectsV2Input{Bucket: aws.String(s.artifactCfg.S3Bucket), Prefix: aws.String(prefix)})
		for pager.HasMorePages() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				t.Fatalf("list %s: %v", prefix, err)
			}
			for _, object := range page.Contents {
				out = append(out, aws.ToString(object.Key)+" "+aws.ToString(object.ETag))
			}
		}
	}
	iter, err := s.temporal.ScheduleClient().List(ctx, client.ScheduleListOptions{})
	if err != nil {
		t.Fatalf("list schedules: %v", err)
	}
	for iter.HasNext() {
		entry, err := iter.Next()
		if err != nil {
			t.Fatalf("list schedules: %v", err)
		}
		out = append(out, "schedule "+entry.ID)
	}
	sort.Strings(out)
	return out
}

// auditActions lists the audit actions recorded for this namespace since
// since: records in the namespace, and the deployment-wide webhook records
// whose scope names it (a webhook.register carries no namespace of its own).
func (s *automationStack) auditActions(t *testing.T, replica *engineReplica, since time.Time) []string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	args := []string{"--json", "admin", "audit", "--since", since.UTC().Format(time.RFC3339Nano), "--limit", "200",
		"--server", "http://" + replica.addr}
	var stdout, stderr bytes.Buffer
	if code := Execute(args, &stdout, &stderr); code != exitOK {
		t.Fatalf("admin audit exited %d: %s", code, stderr.String())
	}
	fmt.Fprintf(&s.transcript, "$ orkctl %s\n%s\n", strings.Join(args, " "), stdout.String())
	var page struct {
		Records []struct {
			Action    string `json:"action"`
			Namespace string `json:"namespace"`
			Detail    string `json:"detail"`
		} `json:"records"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &page); err != nil {
		t.Fatalf("decode audit: %v (%s)", err, stdout.String())
	}
	var actions []string
	for _, rec := range page.Records {
		if rec.Namespace == s.namespace || strings.Contains(rec.Detail, "scope="+s.namespace+"/") {
			actions = append(actions, rec.Action)
		}
	}
	sort.Strings(actions)
	return actions
}

func (s *automationStack) summaryDoc() string {
	return s.doc(fmt.Sprintf(`settings:
  run_retention: 24h
schedules:
  nightly-reconcile:
    orkflow: %[1]s
    cron: "0 2 * * *"
    timezone: Europe/Lisbon
    overlap: skip
    input: '{"mode": "full"}'
triggers:
  github-deploy:
    orkflow: %[1]s
    description: GitHub Actions deploy hook
webhooks:
  etl-loader:
    url: %[2]s/etl
    orkflow: %[1]s
    events: [RUN_SUCCEEDED, RUN_FAILED]
`, s.orkflow, s.receiver.server.URL))
}

func TestIntegration_ApplyAutomationCreatesAndReapplyIsANoOp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "noop")
	doc := s.write(t, "automation.ork.yaml", s.summaryDoc())

	s.cli(t, s.a, exitOK, "apply", doc, "--dry-run")
	first := s.applyReport(t, s.a, exitOK, doc)
	end := time.Now()
	for _, result := range first.GetAutomation()[0].GetResults() {
		if result.GetOutcome() != orkstackv1.AutomationStepOutcome_AUTOMATION_STEP_OUTCOME_APPLIED {
			t.Fatalf("first apply step %d = %v %s", result.GetStep(), result.GetOutcome(), result.GetError())
		}
	}
	if trigger := s.triggerByName(t, s.b, "github-deploy"); trigger == nil {
		t.Fatal("the applied trigger does not list by name on replica B")
	}
	s.cli(t, s.b, exitOK, "schedule", "get", "nightly-reconcile")

	for round := 1; round <= 2; round++ {
		again := s.applyReport(t, s.b, exitOK, doc)
		if len(again.GetAutomation()[0].GetResults()) != 0 {
			t.Fatalf("re-apply %d has results: %v", round, again.GetAutomation()[0].GetResults())
		}
		for _, step := range again.GetAutomation()[0].GetPlan().GetSteps() {
			if step.GetAction() != orkstackv1.AutomationAction_AUTOMATION_ACTION_UNCHANGED {
				t.Fatalf("re-apply %d plans %v %s", round, step.GetAction(), step.GetName())
			}
		}
	}
	if actions := s.auditActions(t, s.a, end); len(actions) != 0 {
		t.Fatalf("re-applies recorded audit actions %v; want zero writes", actions)
	}
	t.Logf("EVIDENCE reapply_writes=0 audit_records_since_first_apply=0")
	proof.Write(t, "orkctl", "automation-reapply-noop.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyDryRunWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "dryrun")
	doc := s.write(t, "automation.ork.yaml", s.summaryDoc())
	before := s.metadataSnapshot(ctx, t)
	s.cli(t, s.a, exitOK, "apply", doc, "--dry-run", "--prune")
	after := s.metadataSnapshot(ctx, t)
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("a dry run changed the store or the cluster:\nbefore:\n%s\nafter:\n%s", strings.Join(before, "\n"), strings.Join(after, "\n"))
	}
	fmt.Fprintf(&s.transcript, "# metadata/ and schedules before and after the dry run (%d entries, identical)\n%s\n", len(before), strings.Join(before, "\n"))
	proof.Write(t, "orkctl", "automation-dry-run.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyDirectoryAppliesOrkflowsBeforeAutomation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "dir")
	s.write(t, "rollback.ork.yaml", originOrkflowYAML("rollback"))
	s.write(t, "automation.ork.yaml", s.doc("triggers:\n  undo:\n    orkflow: rollback\n    version: 0.1.0\n"))
	out := s.cli(t, s.a, exitOK, "apply", s.dir, "--dry-run")
	if strings.Contains(out, "ERROR") {
		t.Fatalf("a pending orkflow did not satisfy the trigger:\n%s", out)
	}
	report := s.applyReport(t, s.a, exitOK, s.dir)
	if report.GetOrkflows()[0].GetApplied() == nil {
		t.Fatal("the orkflow was not applied")
	}
	id, token := disclosed(t, report, "undo")
	if status, body := s.invoke(ctx, t, s.b, id, token); status != http.StatusCreated {
		t.Fatalf("invoke = %d: %s", status, body)
	}
	proof.Write(t, "orkctl", "automation-directory.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyTriggerTokenIsShownOnceAndInvokes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "token")
	doc := s.write(t, "automation.ork.yaml", s.doc(fmt.Sprintf("triggers:\n  github-deploy:\n    orkflow: %s\n", s.orkflow)))
	report := s.applyReport(t, s.a, exitOK, doc)
	id, token := disclosed(t, report, "github-deploy")
	status, _ := s.invoke(ctx, t, s.b, id, token)
	if status != http.StatusCreated {
		t.Fatalf("invoke with the apply's token = %d, want 201", status)
	}
	t.Logf("EVIDENCE apply_token_invoke=%d", status)
	if out := s.cli(t, s.a, exitOK, "apply", doc); strings.Contains(out, token) || strings.Contains(out, "shown once") {
		t.Fatalf("a re-apply printed a token:\n%s", out)
	}
	proof.Write(t, "orkctl", "automation-token.txt", []byte(s.transcript.String()))
}

func TestIntegration_TriggerTokenSurvivesInPlaceUpdateByApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "inplace")
	report := s.applyReport(t, s.a, exitOK, s.write(t, "v1.ork.yaml", s.doc(fmt.Sprintf("triggers:\n  github-deploy:\n    orkflow: %s\n", s.orkflow))))
	id, token := disclosed(t, report, "github-deploy")
	second := strings.Replace(originOrkflowYAML(s.orkflow), `version: "0.1.0"`, `version: "0.2.0"`, 1)
	s.cli(t, s.a, exitOK, "apply", s.write(t, "deploy.ork.yaml", second))
	updated := s.applyReport(t, s.b, exitOK, s.write(t, "v2.ork.yaml",
		s.doc(fmt.Sprintf("triggers:\n  github-deploy:\n    orkflow: %s\n    version: 0.2.0\n    description: pinned\n", s.orkflow))))
	step := updated.GetAutomation()[0].GetPlan().GetSteps()[0]
	if step.GetAction() != orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE || step.GetId() != id {
		t.Fatalf("the version change planned %v on %s, want an in-place UPDATE of %s", step.GetAction(), step.GetId(), id)
	}
	status, body := s.invoke(ctx, t, s.a, id, token)
	if status != http.StatusCreated {
		t.Fatalf("the original token after an in-place update = %d: %s", status, body)
	}
	runID := decodeSubmittedID(t, body)
	run := s.awaitTerminal(ctx, t, s.b, runID)
	if run.GetOrkflowVersion() != "0.2.0" {
		t.Fatalf("the run pins %q, want the new 0.2.0", run.GetOrkflowVersion())
	}
	t.Logf("EVIDENCE original_token_after_update=%d run_version=%s", status, run.GetOrkflowVersion())
	proof.Write(t, "orkctl", "automation-token-survives-update.txt", []byte(s.transcript.String()))
}

func TestIntegration_WebhookUpdateKeepsItsSecret(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "whsecret")
	const secret = "pre-shared-etl-secret"
	t.Setenv("ETL_WEBHOOK_SECRET", secret)
	entry := func(orkflow string) string {
		return s.doc(fmt.Sprintf("webhooks:\n  etl-loader:\n    url: %s/etl\n    orkflow: %s\n    events: [RUN_SUCCEEDED]\n    secret_env: ETL_WEBHOOK_SECRET\n",
			s.receiver.server.URL, orkflow))
	}
	s.cli(t, s.a, exitOK, "apply", s.write(t, "v1.ork.yaml", entry("other-orkflow")))
	report := s.applyReport(t, s.b, exitOK, s.write(t, "v2.ork.yaml", entry(s.orkflow)))
	if step := report.GetAutomation()[0].GetPlan().GetSteps()[0]; step.GetAction() != orkstackv1.AutomationAction_AUTOMATION_ACTION_UPDATE {
		t.Fatalf("the scope change planned %v, want UPDATE", step.GetAction())
	}
	runID := decodeSubmittedID(t, s.cli(t, s.a, exitOK, "--json", "run", "submit", s.orkflow))
	post := s.receiver.await(t, "/etl", runID)
	signature := post.headers.Get(webhooks.SignatureHeader)
	if !webhooks.Verify(secret, post.body, signature) {
		t.Fatalf("the delivery after the update does not verify with the env secret (%s)", signature)
	}
	t.Logf("EVIDENCE webhooks.Verify(env secret, body, %s) = true after an in-place scope update", signature)
	proof.Write(t, "orkctl", "automation-webhook-secret.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyPruneDeletesOnlyNamedUndeclaredObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "prune")
	s.cli(t, s.a, exitOK, "trigger", "create", "--orkflow", s.orkflow, "--name", "manual-hook")
	var unnamed orkstackv1.CreateTriggerResponse
	if err := api.UnmarshalJSON([]byte(s.cli(t, s.a, exitOK, "--json", "trigger", "create", "--orkflow", s.orkflow)), &unnamed); err != nil {
		t.Fatal(err)
	}
	s.cli(t, s.a, exitOK, "webhook", "register", s.receiver.server.URL+"/named", "--name", "old-hook")
	s.cli(t, s.a, exitOK, "schedule", "create", "old-schedule", "--orkflow", s.orkflow, "--cron", "0 4 * * *")
	var wide struct {
		Webhook struct {
			ID string `json:"id"`
		} `json:"webhook"`
	}
	t.Setenv(serverEnvVar, "")
	var stdout, stderr bytes.Buffer
	if code := Execute([]string{"--json", "webhook", "register", s.receiver.server.URL + "/everyone", "--server", "http://" + s.a.addr}, &stdout, &stderr); code != exitOK {
		t.Fatalf("deployment-wide register: %s", stderr.String())
	}
	if err := json.Unmarshal(stdout.Bytes(), &wide); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = Execute([]string{"webhook", "delete", wide.Webhook.ID, "--server", "http://" + s.a.addr}, io.Discard, io.Discard)
	})

	doc := s.write(t, "automation.ork.yaml", s.doc("schedules: {}\ntriggers: {}\nwebhooks: {}\n"))
	out := s.cli(t, s.a, exitOK, "apply", doc, "--prune")
	for _, fragment := range []string{"- schedule   old-schedule", "- trigger    manual-hook", "- webhook    old-hook"} {
		if !strings.Contains(out, fragment) {
			t.Fatalf("the --prune output does not name %q:\n%s", fragment, out)
		}
	}
	if s.triggerByName(t, s.b, "manual-hook") != nil {
		t.Fatal("the named undeclared trigger survived --prune")
	}
	status, _ := s.invoke(ctx, t, s.b, unnamed.GetTrigger().GetId(), unnamed.GetSecret())
	if status != http.StatusCreated {
		t.Fatalf("the unnamed trigger no longer invokes after --prune: %d", status)
	}
	if listed := s.cli(t, s.b, exitOK, "webhook", "list"); !strings.Contains(listed, wide.Webhook.ID) {
		t.Fatalf("the deployment-wide webhook was pruned:\n%s", listed)
	}
	t.Logf("EVIDENCE unnamed_trigger_after_prune=%d deployment_wide_webhook_kept=%s", status, wide.Webhook.ID)
	proof.Write(t, "orkctl", "automation-prune.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyOmittedSectionIsUntouchedByPrune(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "omitted")
	s.cli(t, s.a, exitOK, "trigger", "create", "--orkflow", s.orkflow, "--name", "kept-hook")
	s.cli(t, s.a, exitOK, "apply", s.write(t, "automation.ork.yaml", s.doc("schedules: {}\n")), "--prune")
	if s.triggerByName(t, s.b, "kept-hook") == nil {
		t.Fatal("--prune of a document without a triggers section deleted a trigger")
	}
	proof.Write(t, "orkctl", "automation-omitted-section.txt", []byte(s.transcript.String()))
}

func TestIntegration_ExportThenCheckIsClean(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "export")
	s.cli(t, s.a, exitOK, "trigger", "create", "--orkflow", s.orkflow, "--name", "hand-made", "--description", "by hand")
	s.cli(t, s.a, exitOK, "schedule", "create", "hand-schedule", "--orkflow", s.orkflow, "--interval", "1h", "--paused")
	s.cli(t, s.a, exitOK, "webhook", "register", s.receiver.server.URL+"/x", "--name", "hand-hook", "--event", "RUN_FAILED")
	exported := s.cli(t, s.b, exitOK, "export")
	s.cli(t, s.a, exitOK, "apply", s.write(t, "exported.ork.yaml", exported), "--check")
	t.Logf("EVIDENCE export_then_check_exit=0")
	proof.Write(t, "orkctl", "automation-export-check.txt", []byte(s.transcript.String()))
}

func TestIntegration_ExportAdoptsHandMadeObjectsWithoutReissuingSecrets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "adopt")
	var created orkstackv1.CreateTriggerResponse
	if err := api.UnmarshalJSON([]byte(s.cli(t, s.a, exitOK, "--json", "trigger", "create", "--orkflow", s.orkflow)), &created); err != nil {
		t.Fatal(err)
	}
	var hook struct {
		Webhook struct {
			ID     string `json:"id"`
			Secret string `json:"secret"`
		} `json:"webhook"`
	}
	if err := json.Unmarshal([]byte(s.cli(t, s.a, exitOK, "--json", "webhook", "register", s.receiver.server.URL+"/adopt", "--event", "RUN_SUCCEEDED")), &hook); err != nil {
		t.Fatal(err)
	}
	first := s.cli(t, s.a, exitOK, "export")
	for _, fragment := range []string{"orkctl trigger update " + created.GetTrigger().GetId(), "orkctl webhook update " + hook.Webhook.ID} {
		if !strings.Contains(first, fragment) {
			t.Fatalf("the export does not name %q:\n%s", fragment, first)
		}
	}
	s.cli(t, s.a, exitOK, "trigger", "update", created.GetTrigger().GetId(), "--name", "adopted-hook")
	s.cli(t, s.a, exitOK, "webhook", "update", hook.Webhook.ID, "--name", "adopted-webhook")
	second := s.cli(t, s.b, exitOK, "export")
	if !strings.Contains(second, "adopted-hook:") || !strings.Contains(second, "adopted-webhook:") {
		t.Fatalf("the adopted objects are not entries:\n%s", second)
	}
	s.cli(t, s.a, exitOK, "apply", s.write(t, "adopted.ork.yaml", second), "--check")
	status, body := s.invoke(ctx, t, s.b, created.GetTrigger().GetId(), created.GetSecret())
	if status != http.StatusCreated {
		t.Fatalf("the adopted trigger's original token = %d", status)
	}
	runID := decodeSubmittedID(t, body)
	post := s.receiver.await(t, "/adopt", runID)
	if !webhooks.Verify(hook.Webhook.Secret, post.body, post.headers.Get(webhooks.SignatureHeader)) {
		t.Fatal("a delivery to the adopted webhook does not verify with its original secret")
	}
	t.Logf("EVIDENCE adopted_token_invoke=%d adopted_webhook_verify=true", status)
	proof.Write(t, "orkctl", "automation-adopt.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyExpectPlanRefusesAfterDrift(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "expect")
	doc := s.write(t, "automation.ork.yaml", s.doc(fmt.Sprintf("triggers:\n  github-deploy:\n    orkflow: %s\n", s.orkflow)))
	reviewed := s.applyReport(t, s.a, exitOK, doc, "--dry-run", "--prune").GetAutomation()[0].GetPlan().GetPlanDigest()
	s.cli(t, s.b, exitOK, "trigger", "create", "--orkflow", s.orkflow, "--name", "between-review-and-merge")
	out := s.cli(t, s.a, exitError, "apply", doc, "--prune", "--expect-plan", reviewed)
	if !strings.Contains(out, "the plan differs from the one reviewed") || !strings.Contains(out, "between-review-and-merge") {
		t.Fatalf("the refusal does not print the new plan:\n%s", out)
	}
	if s.triggerByName(t, s.b, "between-review-and-merge") == nil {
		t.Fatal("the hand-made trigger was deleted by a refused apply")
	}
	t.Logf("EVIDENCE expect_plan_refused=true hand_made_trigger_kept=true")
	proof.Write(t, "orkctl", "automation-expect-plan.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyPartialFailureReportsAndReapplyConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "partial")
	body := func(cron string) string {
		return s.doc(fmt.Sprintf("schedules:\n  a-first:\n    orkflow: %[1]s\n    cron: \"0 1 * * *\"\n  b-second:\n    orkflow: %[1]s\n    cron: %q\n"+
			"triggers:\n  later:\n    orkflow: %[1]s\n", s.orkflow, cron))
	}
	report := s.applyReport(t, s.a, exitError, s.write(t, "broken.ork.yaml", body("61 25 * * *")))
	var outcomes []string
	for _, result := range report.GetAutomation()[0].GetResults() {
		outcomes = append(outcomes, strings.TrimPrefix(result.GetOutcome().String(), "AUTOMATION_STEP_OUTCOME_"))
	}
	if strings.Join(outcomes, ",") != "APPLIED,FAILED,NOT_RUN" {
		t.Fatalf("outcomes = %v, want APPLIED,FAILED,NOT_RUN", outcomes)
	}
	t.Logf("EVIDENCE cluster_refusal=%s", report.GetAutomation()[0].GetResults()[1].GetError())
	fixed := s.applyReport(t, s.b, exitOK, s.write(t, "fixed.ork.yaml", body("0 2 * * *")))
	var created []string
	for _, result := range fixed.GetAutomation()[0].GetResults() {
		created = append(created, fixed.GetAutomation()[0].GetPlan().GetSteps()[result.GetStep()].GetName())
	}
	if strings.Join(created, ",") != "b-second,later" {
		t.Fatalf("the re-apply created %v, want only b-second and later", created)
	}
	proof.Write(t, "orkctl", "automation-partial-failure.txt", []byte(s.transcript.String()))
}

func TestIntegration_ApplyNeverReleasesAHold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "hold")
	s.cli(t, s.a, exitOK, "namespace", "update", s.namespace, "--hold", "Q3 close")
	readHold := func() (string, string) {
		var got orkstackv1.GetNamespaceResponse
		if err := api.UnmarshalJSON([]byte(s.cli(t, s.b, exitOK, "--json", "namespace", "get", s.namespace)), &got); err != nil {
			t.Fatal(err)
		}
		return got.GetNamespace().GetHold().GetReason(), got.GetNamespace().GetHold().GetSince().AsTime().String()
	}
	reason, since := readHold()
	out := s.cli(t, s.a, exitOK, "apply", s.write(t, "automation.ork.yaml", s.doc("settings:\n  run_retention: 24h\n")))
	if !strings.Contains(out, "on hold since") {
		t.Fatalf("the plan does not note the hold:\n%s", out)
	}
	if r, s2 := readHold(); r != reason || s2 != since || r != "Q3 close" {
		t.Fatalf("the hold changed: %q %s, was %q %s", r, s2, reason, since)
	}
	proof.Write(t, "orkctl", "automation-hold.txt", []byte(s.transcript.String()))
}

func TestIntegration_CheckDetectsDrift(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "drift")
	doc := s.write(t, "automation.ork.yaml", s.doc(fmt.Sprintf("triggers:\n  github-deploy:\n    orkflow: %s\n    description: from git\n", s.orkflow)))
	s.cli(t, s.a, exitOK, "apply", doc)
	s.cli(t, s.a, exitOK, "apply", doc, "--check")
	trigger := s.triggerByName(t, s.a, "github-deploy")
	s.cli(t, s.b, exitOK, "trigger", "update", trigger.GetId(), "--description", "edited by hand")
	out := s.cli(t, s.a, exitApplyCheckChanges, "apply", doc, "--check")
	if !strings.Contains(out, "~ trigger    github-deploy") || !strings.Contains(out, `description: "edited by hand" → "from git"`) {
		t.Fatalf("--check does not name the drift:\n%s", out)
	}
	t.Logf("EVIDENCE check_exit_clean=0 check_exit_drift=%d", exitApplyCheckChanges)
	proof.Write(t, "orkctl", "automation-check.txt", []byte(s.transcript.String()))
}

func TestIntegration_AutomationTwoReplicasAgree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "replicas")
	doc := s.write(t, "automation.ork.yaml", s.summaryDoc())
	planned := s.applyReport(t, s.a, exitOK, doc, "--dry-run").GetAutomation()[0].GetPlan().GetPlanDigest()
	s.cli(t, s.b, exitOK, "apply", doc, "--expect-plan", planned)
	onA := s.cli(t, s.a, exitOK, "export")
	onB := s.cli(t, s.b, exitOK, "export")
	if onA != onB {
		t.Fatalf("the replicas export different documents:\nA:\n%s\nB:\n%s", onA, onB)
	}
	proof.Write(t, "orkctl", "automation-two-replicas.txt", []byte(s.transcript.String()))
}

func TestIntegration_ScheduleFromApplyFiresWithScheduleOrigin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "fire")
	report := s.applyReport(t, s.a, exitOK, s.write(t, "automation.ork.yaml",
		s.doc(fmt.Sprintf("schedules:\n  nightly:\n    orkflow: %s\n    cron: \"0 2 * * *\"\n", s.orkflow))))
	scheduleID := report.GetAutomation()[0].GetResults()[0].GetId()
	s.cli(t, s.b, exitOK, "schedule", "fire", "nightly")
	ids := s.awaitListed(t, s.b, func(ids []string) bool { return len(ids) == 1 }, "--origin", "schedule:"+scheduleID)
	origin := originOf(t, s.cli(t, s.a, exitOK, "--json", "run", "get", ids[0]))
	if origin["kind"] != "SCHEDULE" || origin["id"] != scheduleID {
		t.Fatalf("origin = %v, want SCHEDULE %s", origin, scheduleID)
	}
	proof.Write(t, "orkctl", "automation-schedule-origin.txt", []byte(s.transcript.String()))
}

func TestIntegration_EachAppliedStepIsAudited(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newAutomationStack(ctx, t, "audit")
	start := time.Now()
	s.cli(t, s.a, exitOK, "apply", s.write(t, "automation.ork.yaml", s.summaryDoc()))
	want := []string{"namespace.update", "schedule.create", "trigger.create", "webhook.register"}
	var got []string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		got = s.auditActions(t, s.b, start)
		if len(got) >= len(want) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("audit actions = %v, want one per executed step %v and none for the plan", got, want)
	}
	proof.Write(t, "orkctl", "automation-audit.txt", []byte(s.transcript.String()))
}
