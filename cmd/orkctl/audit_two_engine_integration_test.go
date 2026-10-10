//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestAudit_TwoReplicas_WriteOnOneListedOnOther drives the audit trail against
// the production topology: two real `ork` processes sharing one Temporal
// namespace and one MinIO bucket.
//
//  1. `orkctl namespace create` with ORK_PRINCIPAL=proof-a against replica A.
//  2. `orkctl admin audit --json` against replica B — which recorded nothing —
//     shows the record, attributed, succeeded.
//  3. The object exists under metadata/audit/ in MinIO with the audit content
//     type, and a create-only put of the same key is refused (the ETag does
//     not move).
//  4. `orkctl namespace delete default` against replica B is refused, and the
//     refusal is itself a record, read back from replica A
//     (TestAudit_RefusedWriteIsRecorded's claim, on the same topology).
func TestAudit_TwoReplicas_WriteOnOneListedOnOther(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)

	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	partition := isolatedNamespace()
	t.Logf("temporal namespace %s", partition)
	dialIsolatedNamespace(ctx, t, address, partition)

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	queue := idempotencyTaskQueue()
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, partition, queue)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, partition, queue)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	name := fmt.Sprintf("audit-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	var transcript strings.Builder
	fmt.Fprintf(&transcript, "# control-plane audit trail across two engine replicas (%s)\n",
		time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&transcript, "bucket %s, backend s3\n\n", artifactCfg.S3Bucket)

	// ---- 1. a write on A, attributed ---------------------------------------
	created := auditCLI(t, replicaA, "proof-a", "namespace", "create", name)
	fmt.Fprintf(&transcript, "$ ORK_PRINCIPAL=proof-a orkctl --server <replica A> namespace create %s\n%s\n", name, created)

	// ---- 2. listed on B ------------------------------------------------------
	listed := auditCLI(t, replicaB, "", "--json", "admin", "audit", "--filter", "namespace="+name, "--filter", "at>="+since)
	fmt.Fprintf(&transcript, "$ orkctl --server <replica B> admin audit --filter namespace=%s --filter 'at>=%s' --json\n%s\n", name, since, listed)
	resp := &orkstackv1.ListAuditRecordsResponse{}
	if err := protojson.Unmarshal([]byte(listed), resp); err != nil {
		t.Fatalf("admin audit --json is not a ListAuditRecordsResponse: %v\n%s", err, listed)
	}
	var rec *orkstackv1.AuditRecord
	for _, candidate := range resp.GetRecords() {
		if candidate.GetAction() == "namespace.create" && candidate.GetTarget() == name {
			rec = candidate
		}
	}
	if rec == nil || rec.GetPrincipal() != "proof-a" || !rec.GetSucceeded() || rec.GetNamespace() != name ||
		rec.GetProcedure() != "/orkstack.v1.NamespaceService/CreateNamespace" || rec.GetSourceAddress() == "" {
		t.Fatalf("replica B's listing = %v, want the namespace.create of %s by proof-a", resp.GetRecords(), name)
	}
	human := auditCLI(t, replicaB, "", "admin", "audit", "--filter", "namespace="+name, "--filter", "at>="+since)
	if !strings.Contains(human, "proof-a") || strings.Contains(human, "Every namespace unless") ||
		!strings.Contains(human, "does not authenticate it") {
		t.Fatalf("human listing on B:\n%s", human)
	}
	fmt.Fprintf(&transcript, "$ orkctl --server <replica B> admin audit --filter namespace=%s --filter 'at>=%s'\n%s\n", name, since, human)

	// ---- 3. the object in MinIO: content type, create-only -----------------
	key, err := artifacts.AuditKey(rec)
	if err != nil {
		t.Fatalf("AuditKey: %v", err)
	}
	raw := auditRawS3Client(t, artifactCfg)
	head, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(artifactCfg.S3Bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("HeadObject(%s): %v", key, err)
	}
	if got := aws.ToString(head.ContentType); got != artifacts.AuditContentType {
		t.Errorf("content type = %q, want %q", got, artifacts.AuditContentType)
	}
	store, err := artifacts.NewS3AuditStore(ctx, artifactCfg)
	if err != nil {
		t.Fatalf("NewS3AuditStore: %v", err)
	}
	forged := &orkstackv1.AuditRecord{
		SchemaVersion: artifacts.AuditSchemaVersion, Id: rec.GetId(), At: rec.GetAt(),
		Principal: "forger", Action: "namespace.delete", Target: name,
	}
	if err := store.PutAuditRecord(ctx, forged); err == nil || !strings.Contains(err.Error(), artifacts.ErrAuditRecordExists.Error()) {
		t.Fatalf("overwrite of %s = %v, want ErrAuditRecordExists", key, err)
	}
	again, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(artifactCfg.S3Bucket), Key: aws.String(key)})
	if err != nil || aws.ToString(again.ETag) != aws.ToString(head.ETag) {
		t.Fatalf("ETag moved from %s to %s (%v): the record was overwritten",
			aws.ToString(head.ETag), aws.ToString(again.ETag), err)
	}
	fmt.Fprintf(&transcript, "s3://%s/%s content-type=%s etag=%s; overwrite refused (etag unchanged)\n\n",
		artifactCfg.S3Bucket, key, aws.ToString(head.ContentType), aws.ToString(head.ETag))

	t.Logf("EVIDENCE audit_two_replica written_on=A listed_on=B principal=%s action=%s namespace=%s succeeded=%t key=%s content_type=%s etag=%s",
		rec.GetPrincipal(), rec.GetAction(), rec.GetNamespace(), rec.GetSucceeded(), key,
		aws.ToString(head.ContentType), aws.ToString(head.ETag))

	// ---- 4. a refused write is a record too ---------------------------------
	refusal := auditRefusal(t, replicaB, replicaA, since)
	fmt.Fprintf(&transcript, "%s\n", refusal)

	// Leave nothing behind in the shared namespace registry.
	auditCLI(t, replicaA, "proof-a", "namespace", "delete", name)
	proof.Write(t, "orkctl", "audit-two-replicas.txt", []byte(transcript.String()))
}

// TestAudit_RefusedWriteIsRecorded is the refusal claim on its own: against
// real Temporal and MinIO, `orkctl namespace delete default` is refused and
// the trail shows `refused: failed_precondition`.
func TestAudit_RefusedWriteIsRecorded(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	partition := isolatedNamespace()
	dialIsolatedNamespace(ctx, t, address, partition)
	enginePath := buildBinary(t, repoRoot(t), t.TempDir(), "ork")
	replica := startReplica(ctx, t, "ork-refusal", enginePath, address, partition, idempotencyTaskQueue())
	replica.awaitReady(ctx, t)

	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	transcript := auditRefusal(t, replica, replica, since)
	proof.Write(t, "orkctl", "audit-refused-write.txt", []byte(transcript))
}

// TestAudit_TwoEngines_DeploymentWideRecordsByEmptyNamespace: a
// deployment-wide write (a webhook registration, which carries no namespace of
// its own) made through replica A is selected on replica B by the empty
// namespace filter `--filter namespace=` — which no request could select
// before filters — and a namespaced write by the same principal is not.
func TestAudit_TwoEngines_DeploymentWideRecordsByEmptyNamespace(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	partition := isolatedNamespace()
	dialIsolatedNamespace(ctx, t, address, partition)
	enginePath := buildBinary(t, repoRoot(t), t.TempDir(), "ork")
	queue := idempotencyTaskQueue()
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, partition, queue)
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, partition, queue)
	replicaA.awaitReady(ctx, t)
	replicaB.awaitReady(ctx, t)

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	principal := "proof-wide-" + suffix
	name := "audit-wide-" + suffix
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)

	// A namespaced write and a deployment-wide one, both through A by the
	// same principal. The webhook is scoped to the namespace this test owns,
	// which runs nothing, so it is never delivered to.
	auditCLI(t, replicaA, principal, "namespace", "create", name)
	registered := auditCLI(t, replicaA, principal, "--json", "webhook", "register", "http://127.0.0.1:1/never", "-n", name)
	var hook struct {
		Webhook struct {
			ID string `json:"id"`
		} `json:"webhook"`
	}
	if err := json.Unmarshal([]byte(registered), &hook); err != nil || hook.Webhook.ID == "" {
		t.Fatalf("webhook register --json = %v:\n%s", err, registered)
	}
	// A deferred call, not t.Cleanup: the replicas run under ctx, which the
	// earlier `defer cancel()` ends as this function returns — before any
	// t.Cleanup would run — so the deletes must reach A while it still serves.
	defer func() {
		auditCLI(t, replicaA, principal, "webhook", "delete", hook.Webhook.ID)
		auditCLI(t, replicaA, principal, "namespace", "delete", name)
	}()

	list := func(filters ...string) []*orkstackv1.AuditRecord {
		args := []string{"--json", "admin", "audit", "--filter", "principal=" + principal, "--filter", "at>=" + since}
		for _, f := range filters {
			args = append(args, "--filter", f)
		}
		out := auditCLI(t, replicaB, "", args...)
		resp := &orkstackv1.ListAuditRecordsResponse{}
		if err := protojson.Unmarshal([]byte(out), resp); err != nil {
			t.Fatalf("admin audit --json is not a ListAuditRecordsResponse: %v\n%s", err, out)
		}
		return resp.GetRecords()
	}
	actions := func(records []*orkstackv1.AuditRecord) string {
		var out []string
		for _, rec := range records {
			out = append(out, rec.GetAction()+"@"+rec.GetNamespace())
		}
		return strings.Join(out, ",")
	}

	every := list()
	if got := actions(every); got != "webhook.register@,namespace.create@"+name {
		t.Fatalf("replica B, every namespace = %s; want the webhook.register (no namespace) and the namespace.create", got)
	}
	wide := list("namespace=")
	if got := actions(wide); got != "webhook.register@" {
		t.Fatalf("replica B, namespace= = %s; want only the deployment-wide webhook.register", got)
	}
	scoped := list("namespace=" + name)
	if got := actions(scoped); got != "namespace.create@"+name {
		t.Fatalf("replica B, namespace=%s = %s; want only the namespace.create", name, got)
	}
	human := auditCLI(t, replicaB, "", "admin", "audit", "--filter", "principal="+principal, "--filter", "at>="+since, "--filter", "namespace=")
	transcript := fmt.Sprintf("$ orkctl --server <replica B> admin audit --filter principal=%s --filter 'at>=%s' --filter 'namespace='\n%s",
		principal, since, human)
	t.Logf("EVIDENCE audit_deployment_wide written_on=A listed_on=B every=%s namespace_empty=%s namespace_named=%s",
		actions(every), actions(wide), actions(scoped))
	proof.Write(t, "orkctl", "audit-deployment-wide.txt", []byte(transcript))
}

// auditRefusal deletes `default` through writer (refused) and reads the
// refusal back through reader, with a principal unique to this call so a
// concurrent suite's refusal cannot be mistaken for this one's.
func auditRefusal(t *testing.T, writer, reader *engineReplica, since string) string {
	t.Helper()
	principal := fmt.Sprintf("proof-refusal-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	var stdout, stderr bytes.Buffer
	t.Setenv(principalEnvVar, principal)
	code := Execute([]string{"namespace", "delete", "default", "--server", "http://" + writer.addr}, &stdout, &stderr)
	t.Setenv(principalEnvVar, "")
	if code == exitOK || !strings.Contains(stderr.String(), "failed_precondition") {
		t.Fatalf("namespace delete default exited %d (%s), want a failed_precondition refusal", code, stderr.String())
	}
	listed := auditCLI(t, reader, "", "admin", "audit", "--filter", "principal="+principal, "--filter", "at>="+since)
	var row string
	for _, line := range strings.Split(listed, "\n") {
		if strings.Contains(line, "namespace.delete") && strings.Contains(line, principal) {
			row = line
		}
	}
	if !strings.Contains(row, "refused: failed_precondition") || !strings.Contains(row, "default") {
		t.Fatalf("the refusal was not recorded:\n%s", listed)
	}
	t.Logf("EVIDENCE audit_refused_write principal=%s row=%q", principal, strings.Join(strings.Fields(row), " "))
	return fmt.Sprintf("$ ORK_PRINCIPAL=%s orkctl --server <%s> namespace delete default\nError: %s\n"+
		"$ orkctl --server <%s> admin audit --filter principal=%s --filter 'at>=%s'\n%s",
		principal, writer.name, strings.TrimSpace(stderr.String()), reader.name, principal, since, listed)
}

// auditCLI runs the real orkctl against one replica, asserting principal when
// it is not empty, and returns stdout.
func auditCLI(t *testing.T, replica *engineReplica, principal string, args ...string) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	t.Setenv(principalEnvVar, principal)
	var stdout, stderr bytes.Buffer
	code := Execute(append(args, "--server", "http://"+replica.addr), &stdout, &stderr)
	t.Setenv(principalEnvVar, "")
	if code != exitOK {
		t.Fatalf("orkctl %s against %s exited %d: %s", strings.Join(args, " "), replica.name, code, stderr.String())
	}
	if json.Valid(stdout.Bytes()) {
		return strings.TrimSpace(stdout.String())
	}
	return stdout.String()
}

func auditRawS3Client(t *testing.T, cfg artifacts.Config) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
}
