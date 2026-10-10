//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// policyStack is two real `ork` replicas and one real Orker, sharing one
// private Temporal namespace, one PRIVATE bucket and one PRIVATE registry:
//
//   - the bucket is private because the image policy is ONE deployment-wide
//     document at a fixed key (metadata/settings/image-policy.json); written
//     into the shared bucket it would refuse images in every other suite;
//   - the registry is private (registry:3 with deletion enabled) because a
//     scenario removes a signature and another stops the registry, neither
//     of which may touch the registry every other suite on this host reads.
//
// The first-party images are copied into the private registry preserving
// their digests, and the engines' catalog points there (ORK_TEST_REGISTRY,
// localcatalog). The shell image is signed with key "release" by
// test/tools/cosign-sign; key "other" signs nothing. The Orker runs with
// ORK_ORKER_REGISTRY_AUTH_FILE unset: the private registry is anonymous.
type policyStack struct {
	cfg       artifacts.Config
	temporal  client.Client
	partition string
	registry  *cosigntest.PrivateRegistry
	// shared is the host's shared registry (captured before this test points
	// ORK_TEST_REGISTRY at its private one): an image there is real and
	// resolvable but outside a floor naming only the private registry.
	shared     string
	a, b       *engineReplica
	signTool   string
	keys       string // dir holding release/ and other/ cosign.key + cosign.pub
	transcript strings.Builder
}

func newPolicyStack(ctx context.Context, t *testing.T, label string) *policyStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, cfg)
	shared := localcatalog.Registry()

	s := &policyStack{
		partition: fmt.Sprintf("ork-imgpol-%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()%1_000_000_000),
		shared:    shared,
	}
	s.registry = cosigntest.StartRegistry(t, func(format string, args ...any) { infraAbsent(t, format, args...) })
	copier := &cosigntest.Client{}
	for _, name := range localcatalog.Names {
		if _, err := copier.CopyImage(ctx, shared, s.registry.Host, "orkstack/activity-"+name, "dev"); err != nil {
			infraAbsent(t, "copy %s/orkstack/activity-%s:dev into the private registry: %v (run `make -C images push`)",
				shared, name, err)
		}
	}
	// Every process this test starts inherits these: the engines' catalog
	// points at the private registry, and every store at the private bucket.
	t.Setenv(localcatalog.EnvVar, s.registry.Host)
	bucket := "orkstack-imgpol-" + strings.ReplaceAll(s.partition[len("ork-imgpol-"):], "_", "-")
	raw := auditRawS3Client(t, cfg)
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create private bucket %s: %v", bucket, err)
	}
	t.Cleanup(func() { emptyAndRemoveBucket(raw, bucket) })
	t.Setenv("ORK_S3_BUCKET", bucket)
	cfg.S3Bucket = bucket
	s.cfg = cfg

	s.temporal = dialIsolatedNamespace(ctx, t, address, s.partition)
	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")
	s.signTool = filepath.Join(binDir, "cosign-sign")
	build := exec.Command("go", "build", "-o", s.signTool, "./test/tools/cosign-sign")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build test/tools/cosign-sign: %v\n%s", err, out)
	}
	s.keys = t.TempDir()
	if dir := os.Getenv(proof.EnvDir); dir != "" {
		s.keys = filepath.Join(dir, "orkctl", "image-policy-keys-"+label)
	}
	s.tool(t, "keygen", filepath.Join(s.keys, "release"))
	s.tool(t, "keygen", filepath.Join(s.keys, "other"))
	s.tool(t, "sign", "--key", filepath.Join(s.keys, "release", "cosign.key"), s.image("shell")+":dev")

	channel := idempotencyTaskQueue()
	startOrkerWithEnv(ctx, t, s.temporal, orkerPath, address, s.partition, channel)
	s.a = startReplica(ctx, t, "ork-A", enginePath, address, s.partition, channel)
	s.b = startReplica(ctx, t, "ork-B", enginePath, address, s.partition, channel)
	s.a.awaitReady(ctx, t)
	s.b.awaitReady(ctx, t)

	fmt.Fprintf(&s.transcript, "# image supply-chain policy, two engine replicas (%s)\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&s.transcript, "partition  %s\nbucket     %s (this test's own)\nregistry   %s (this test's own, deletion enabled)\n",
		s.partition, bucket, s.registry.Host)
	fmt.Fprintf(&s.transcript, "replica A  %s\nreplica B  %s\norker      ORK_ORKER_REGISTRY_AUTH_FILE unset (anonymous registry)\n\n",
		s.a.addr, s.b.addr)
	return s
}

// image names one first-party image in the private registry, untagged.
func (s *policyStack) image(name string) string {
	return s.registry.Host + "/orkstack/activity-" + name
}

// digest resolves one private image's tag through the engine's resolver.
func (s *policyStack) digest(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	ref, err := dsl.NewResolver("").Resolve(ctx, s.image(name)+":dev", s.image(name)+":dev")
	if err != nil {
		t.Fatalf("resolve %s: %v", s.image(name), err)
	}
	return ref.GetIndexDigest()
}

func (s *policyStack) tool(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(s.signTool, args...).CombinedOutput()
	fmt.Fprintf(&s.transcript, "$ cosign-sign %s\n%s\n", strings.Join(args, " "), out)
	if err != nil {
		t.Fatalf("cosign-sign %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func (s *policyStack) pub(key string) string {
	return filepath.Join(s.keys, key, "cosign.pub")
}

func (s *policyStack) cli(t *testing.T, replica *engineReplica, want int, args ...string) (string, string) {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	var stdout, stderr bytes.Buffer
	code := Execute(append(append([]string{}, args...), "--server", "http://"+replica.addr), &stdout, &stderr)
	fmt.Fprintf(&s.transcript, "$ orkctl %s    # via %s\n", strings.Join(quoteArgs(args), " "), replica.name)
	s.transcript.WriteString(stdout.String())
	s.transcript.WriteString(stderr.String())
	fmt.Fprintf(&s.transcript, "(exit %d)\n\n", code)
	if want >= 0 && code != want {
		t.Fatalf("orkctl %v via %s exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			args, replica.name, code, want, stdout.String(), stderr.String())
	}
	if want < 0 && code == exitOK {
		t.Fatalf("orkctl %v via %s exited 0, want a refusal\nstdout:\n%s", args, replica.name, stdout.String())
	}
	return stdout.String(), stderr.String()
}

func (s *policyStack) note(format string, args ...any) {
	fmt.Fprintf(&s.transcript, format+"\n\n", args...)
}

func (s *policyStack) writeTranscript(t *testing.T, name string) {
	t.Helper()
	proof.Write(t, "orkctl", name, []byte(s.transcript.String()))
}

func (s *policyStack) document(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orkflow.ork.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write orkflow: %v", err)
	}
	return path
}

func (s *policyStack) submitID(t *testing.T, replica *engineReplica, orkflow, namespace string) string {
	t.Helper()
	args := []string{"--json", "run", "submit", orkflow}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	stdout, _ := s.cli(t, replica, exitOK, args...)
	var response struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(stdout), &response); err != nil || response.Run.ID == "" {
		t.Fatalf("decode run submit --json: %v\n%s", err, stdout)
	}
	return response.Run.ID
}

func (s *policyStack) getRun(ctx context.Context, replica *engineReplica, runID, namespace string) (*orkstackv1.Run, error) {
	got, err := replica.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID, Namespace: namespace}))
	if err != nil {
		return nil, err
	}
	return got.Msg.GetRun(), nil
}

func (s *policyStack) awaitStatus(ctx context.Context, t *testing.T, replica *engineReplica, runID, namespace string, want ...orkstackv1.Status) *orkstackv1.Run {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	var last orkstackv1.Status
	for time.Now().Before(deadline) {
		run, err := s.getRun(ctx, replica, runID, namespace)
		if err == nil {
			last = run.GetStatus()
			for _, status := range want {
				if last == status {
					s.note("run %s reached %s (read via %s)", runID, last, replica.name)
					return run
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach %v (last %s)", runID, want, last)
	return nil
}

func (s *policyStack) requireNoRuns(t *testing.T, namespace string) {
	t.Helper()
	for _, replica := range []*engineReplica{s.a, s.b} {
		args := []string{"run", "list"}
		if namespace != "" {
			args = append(args, "-n", namespace)
		}
		stdout, _ := s.cli(t, replica, exitOK, args...)
		if !strings.Contains(stdout, "No runs") {
			t.Fatalf("run list via %s shows runs after a refused submit:\n%s", replica.name, stdout)
		}
	}
}

// objects reads every object under prefix in the private bucket.
func (s *policyStack) objects(ctx context.Context, t *testing.T, prefix string) map[string][]byte {
	t.Helper()
	raw := auditRawS3Client(t, s.cfg)
	out := map[string][]byte{}
	pager := s3.NewListObjectsV2Paginator(raw, &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.S3Bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("list %s: %v", prefix, err)
		}
		for _, object := range page.Contents {
			got, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.cfg.S3Bucket), Key: object.Key})
			if err != nil {
				t.Fatalf("get %s: %v", aws.ToString(object.Key), err)
			}
			body, _ := io.ReadAll(got.Body)
			_ = got.Body.Close()
			out[aws.ToString(object.Key)] = body
		}
	}
	return out
}

func emptyAndRemoveBucket(raw *s3.Client, bucket string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pager := s3.NewListObjectsV2Paginator(raw, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return
		}
		for _, object := range page.Contents {
			_, _ = raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key})
		}
	}
	_, _ = raw.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}

// policyOrkflowYAML is one task running `command` on `uses`, not retried.
func policyOrkflowYAML(name, uses, command string) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: image policy probe.
tasks:
  - id: work
    uses: %s
    timeout_seconds: 120
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", %q]
`, name, uses, command)
}

// twoStepYAML is two sequential tasks on `uses` (second needs first: two
// tasks with no needs are independent and start together): the first sleeps long enough
// for the test to act while it runs; the second allows three attempts, so
// "exactly one attempt" is a statement about retryability.
func twoStepYAML(name, uses string, sleep int) string {
	return fmt.Sprintf(`name: %s
version: "0.1.0"
description: image policy probe, two steps.
tasks:
  - id: first
    uses: %s
    timeout_seconds: 240
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "sleep %d"]
  - id: second
    uses: %s
    needs: [first]
    timeout_seconds: 120
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 1
    with:
      op: exec
      command: ["sh", "-c", "echo second"]
`, name, uses, sleep, uses)
}

func uniqueNamespace(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

func TestIntegration_AnEmptyPolicyReproducesTodayByteForByte(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "empty")
	defer s.writeTranscript(t, "image-policy-empty-policy.txt")

	stdout, _ := s.cli(t, s.a, exitOK, "admin", "image-policy", "get")
	for _, want := range []string{"require digest:      no", "allowed registries:  (unrestricted)",
		"signing keys:        (none)", "updated:             never (no policy stored; every image is permitted)"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("empty rendering lacks %q:\n%s", want, stdout)
		}
	}
	s.cli(t, s.a, exitOK, "apply", s.document(t, statsOrkflowYAML("plain", "echo plain")))
	runID := s.submitID(t, s.a, "plain", "")
	s.awaitStatus(ctx, t, s.b, runID, "", orkstackv1.Status_STATUS_SUCCEEDED)
	s.cli(t, s.b, exitOK, "--json", "run", "get", runID)

	// The archived run body, read back from the bucket.
	deadline := time.Now().Add(2 * time.Minute)
	var archived map[string][]byte
	for {
		archived = s.objects(ctx, t, "metadata/runs/"+runID)
		if len(archived) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(archived) == 0 {
		t.Fatalf("run %s was not archived", runID)
	}
	for key, body := range archived {
		if bytes.Contains(body, []byte("imagePolicy")) || bytes.Contains(body, []byte("image_policy")) {
			t.Fatalf("archived %s carries an image policy key", key)
		}
		s.note("grep -c imagePolicy s3://%s/%s: 0 (%d bytes)", s.cfg.S3Bucket, key, len(body))
	}
	// And the recorded ExecuteTask payloads in the run's Temporal history.
	iter := s.temporal.GetWorkflowHistory(ctx, runID, "", false, 0)
	var scheduled, carrying int
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == "ExecuteTask" {
			scheduled++
			for _, p := range attrs.GetInput().GetPayloads() {
				if bytes.Contains(p.GetData(), []byte("imagePolicy")) {
					carrying++
				}
			}
		}
	}
	if scheduled == 0 || carrying != 0 {
		t.Fatalf("history: %d ExecuteTask inputs, %d carrying imagePolicy; want >0 and 0", scheduled, carrying)
	}
	s.note("Temporal history of %s: %d ExecuteTask input(s), %d carrying imagePolicy", runID, scheduled, carrying)
}

func TestIntegration_ThePolicyRefusesValidateAndApplyWithTheFieldPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "validate")
	defer s.writeTranscript(t, "image-policy-validate-apply.txt")
	private := s.registry.Host + "/orkstack/"
	// The image outside the floor is the SHARED registry's copy of the same
	// shell image: a real, resolvable reference that is simply not under the
	// allowed prefix (no public-registry round trip in the proof).
	outside := s.shared + "/orkstack/activity-shell:dev"

	s.cli(t, s.a, exitOK, "admin", "image-policy", "set", "--allowed-registry", private)
	path := s.document(t, policyOrkflowYAML("deploy", outside, "echo outside"))
	stdout, _ := s.cli(t, s.b, -1, "orkflow", "validate", path)
	want := fmt.Sprintf(`tasks[0].uses: task "work": image %q resolves to %s/orkstack/activity-shell, which is outside the registries this deployment allows: %s (orkctl admin image-policy get)`,
		outside, s.shared, private)
	if !strings.Contains(stdout, want) {
		t.Fatalf("validate did not print the field-anchored issue\nwant: %s\ngot:\n%s", want, stdout)
	}
	_, stderr := s.cli(t, s.b, -1, "apply", path)
	if !strings.Contains(stderr, "invalid_argument") {
		t.Fatalf("apply refusal: %s", stderr)
	}
	for _, replica := range []*engineReplica{s.a, s.b} {
		out, _ := s.cli(t, replica, exitOK, "orkflow", "list")
		if !strings.Contains(out, "No orkflows") {
			t.Fatalf("a refused apply stored something (via %s):\n%s", replica.name, out)
		}
	}

	s.cli(t, s.a, exitOK, "admin", "image-policy", "set", "--require-digest")
	tagged := s.image("shell") + ":dev"
	_, stderr = s.cli(t, s.b, -1, "apply", s.document(t, policyOrkflowYAML("tagged", tagged, "echo tagged")))
	if !strings.Contains(stderr, "is a mutable tag, and this deployment requires a digest") {
		t.Fatalf("tag refusal: %s", stderr)
	}
	s.cli(t, s.b, exitOK, "apply", s.document(t, policyOrkflowYAML("catalog", "shell", "echo catalog")))
	s.note("uses: shell (a catalog name) is exempt from require_digest and applied")
}

func TestIntegration_AFloorTightenedOnOneReplicaRefusesASubmitOnTheOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "floor")
	defer s.writeTranscript(t, "image-policy-floor-two-replicas.txt")
	outside := s.shared + "/orkstack/activity-shell:dev"
	since := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)

	s.cli(t, s.a, exitOK, "apply", s.document(t, policyOrkflowYAML("deploy", outside, "echo deployed")))
	_, stderr := s.cli(t, s.a, -1, "admin", "image-policy", "set", "--allowed-registry", "ghcr.io/acme/")
	if !strings.Contains(stderr, "failed_precondition") || !strings.Contains(stderr, "allowed registries exclude catalog activities") {
		t.Fatalf("catalog refusal: %s", stderr)
	}
	s.cli(t, s.a, exitOK, "admin", "image-policy", "set",
		"--allowed-registry", "ghcr.io/acme/", "--allowed-registry", s.registry.Host+"/orkstack/")
	_, stderr = s.cli(t, s.b, -1, "run", "submit", "deploy")
	if !strings.Contains(stderr, "failed_precondition") || !strings.Contains(stderr, "outside the registries this deployment allows") {
		t.Fatalf("submit via B: %s", stderr)
	}
	s.requireNoRuns(t, "")
	s.cli(t, s.b, exitOK, "admin", "image-policy", "set", "--clear-allowed-registries")
	runID := s.submitID(t, s.a, "deploy", "")
	s.awaitStatus(ctx, t, s.a, runID, "", orkstackv1.Status_STATUS_SUCCEEDED)

	stdout, _ := s.cli(t, s.b, exitOK, "--json", "admin", "audit", "--filter", "action^image_policy.", "--filter", "at>="+since)
	var page struct {
		Records []struct {
			Action    string `json:"action"`
			Detail    string `json:"detail"`
			Succeeded bool   `json:"succeeded"`
		} `json:"records"`
	}
	if err := json.Unmarshal([]byte(stdout), &page); err != nil {
		t.Fatalf("decode audit: %v\n%s", err, stdout)
	}
	if len(page.Records) < 3 {
		t.Fatalf("audit via B shows %d image_policy.update records, want 3 (refused, set, cleared)", len(page.Records))
	}
	for _, rec := range page.Records {
		if rec.Action != "image_policy.update" || strings.Contains(rec.Detail, "-----BEGIN") {
			t.Fatalf("audit record %+v", rec)
		}
	}
}

func TestIntegration_ASignedImageRunsAndAnUnsignedOneIsRefusedAtApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "signed")
	defer s.writeTranscript(t, "image-policy-signed-run.txt")
	prod := uniqueNamespace("prod")

	s.cli(t, s.a, exitOK, "admin", "image-policy", "set",
		"--signing-key", "release="+s.pub("release"), "--signing-key", "other="+s.pub("other"))
	s.cli(t, s.a, exitOK, "namespace", "create", prod, "--require-signed-by", "cosign:release")
	s.cli(t, s.a, exitOK, "apply", "-n", prod, s.document(t, policyOrkflowYAML("deploy", "shell", "echo signed")))
	runID := s.submitID(t, s.b, "deploy", prod)
	s.awaitStatus(ctx, t, s.a, runID, prod, orkstackv1.Status_STATUS_SUCCEEDED)
	stdout, _ := s.cli(t, s.a, exitOK, "--json", "run", "attempts", runID, "-n", prod)
	if n := strings.Count(stdout, `"attempt":`); n != 1 {
		t.Fatalf("run attempts shows %d attempts, want 1:\n%s", n, stdout)
	}

	s.cli(t, s.a, exitOK, "namespace", "update", prod, "--require-signed-by", "cosign:other")
	_, stderr := s.cli(t, s.b, -1, "apply", "-n", prod,
		s.document(t, policyOrkflowYAML("deploy", "shell", "echo signed, again")))
	if !strings.Contains(stderr, "invalid_argument") || !strings.Contains(stderr, `none verifies with key "other"`) {
		t.Fatalf("IMAGE_SIGNATURE_INVALID refusal: %s", stderr)
	}
	s.cli(t, s.a, exitOK, "namespace", "update", prod, "--require-signed-by", "cosign:release")
	_, stderr = s.cli(t, s.b, -1, "apply", "-n", prod,
		s.document(t, policyOrkflowYAML("fetch", "http", "echo unsigned")))
	if !strings.Contains(stderr, "invalid_argument") || !strings.Contains(stderr, "carries no cosign signature") {
		t.Fatalf("IMAGE_NOT_SIGNED refusal: %s", stderr)
	}
}

func TestIntegration_ASignatureRemovedAfterSubmitFailsTheTaskAtTheOrker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "unsign")
	defer s.writeTranscript(t, "image-policy-unsigned-at-the-orker.txt")
	prod := uniqueNamespace("prod")

	s.cli(t, s.a, exitOK, "admin", "image-policy", "set", "--signing-key", "release="+s.pub("release"))
	s.cli(t, s.a, exitOK, "namespace", "create", prod, "--require-signed-by", "cosign:release")
	s.cli(t, s.a, exitOK, "apply", "-n", prod, s.document(t, twoStepYAML("slow", "shell", 20)))
	runID := s.submitID(t, s.a, "slow", prod)

	// Wait for task one's CONTAINER, not its status: a task reads RUNNING from
	// the moment it is scheduled, before any Orker has picked it up and run
	// its own "image-policy" phase. Removing the signature on RUNNING alone
	// raced that check, and task one, not task two, then failed
	// IMAGE_NOT_SIGNED. The live attempt's phase "run" comes after
	// "image-policy", so task one has passed it by then.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		live, err := s.a.runs.InspectRun(ctx, connect.NewRequest(&orkstackv1.InspectRunRequest{
			RunId: runID, Namespace: prod, TaskId: "first",
		}))
		running := false
		if err == nil {
			for _, attempt := range live.Msg.GetAttempts() {
				running = running || (attempt.GetTaskId() == "first" && attempt.GetPhase() == "run")
			}
		}
		if running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task one of %s never reached its container's run phase (last error %v)", runID, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.note("task one of %s is running its container; removing the signature now", runID)
	s.tool(t, "unsign", s.image("shell")+":dev")
	t.Cleanup(func() {
		_, _ = exec.Command(s.signTool, "sign", "--key", filepath.Join(s.keys, "release", "cosign.key"), s.image("shell")+":dev").CombinedOutput()
	})

	run := s.awaitStatus(ctx, t, s.b, runID, prod, orkstackv1.Status_STATUS_FAILED)
	var second *orkstackv1.TaskRun
	for _, task := range run.GetTaskRuns() {
		if task.GetTaskId() == "second" {
			second = task
		}
	}
	if second == nil || second.GetError().GetCode() != "IMAGE_NOT_SIGNED" ||
		second.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
		t.Fatalf("task second = %v, want IMAGE_NOT_SIGNED / CONTRACT_VIOLATION", second)
	}
	s.cli(t, s.b, exitOK, "--json", "run", "get", runID, "-n", prod)
	stdout, _ := s.cli(t, s.b, exitOK, "--json", "run", "attempts", runID, "-n", prod, "--task", "second")
	if n := strings.Count(stdout, `"attempt":`); n != 1 {
		t.Fatalf("task second has %d attempts, want exactly 1 (non-retryable):\n%s", n, stdout)
	}
	records := s.objects(ctx, t, "metadata/attempts/"+runID+"/")
	found := false
	for key, body := range records {
		if strings.Contains(key, "second") && bytes.Contains(body, []byte("IMAGE_NOT_SIGNED")) {
			found = true
			s.note("attempt record s3://%s/%s carries IMAGE_NOT_SIGNED", s.cfg.S3Bucket, key)
		}
	}
	if !found {
		t.Fatalf("no attempt record of task second carries IMAGE_NOT_SIGNED among %d objects", len(records))
	}
}

func TestIntegration_ATightenedPolicyNeverTouchesARunAlreadyAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "accepted")
	defer s.writeTranscript(t, "image-policy-accepted-run.txt")
	// Tag-spelled (not the catalog name), so require_digest refuses it.
	tagged := s.image("shell") + ":dev"
	s.cli(t, s.a, exitOK, "apply", s.document(t, twoStepYAML("slow", tagged, 15)))
	runID := s.submitID(t, s.a, "slow", "")
	s.cli(t, s.b, exitOK, "admin", "image-policy", "set", "--require-digest",
		"--allowed-registry", s.registry.Host+"/orkstack/")
	s.note("policy tightened while %s runs", runID)
	s.awaitStatus(ctx, t, s.a, runID, "", orkstackv1.Status_STATUS_SUCCEEDED)
	_, stderr := s.cli(t, s.b, -1, "run", "submit", "slow")
	if !strings.Contains(stderr, "failed_precondition") || !strings.Contains(stderr, "is a mutable tag") {
		t.Fatalf("submit after tightening: %s", stderr)
	}
}

func TestIntegration_RemovingAKeyANamespaceNamesIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "keys")
	defer s.writeTranscript(t, "image-policy-key-removal.txt")
	prod := uniqueNamespace("prod")
	s.cli(t, s.a, exitOK, "admin", "image-policy", "set", "--signing-key", "release="+s.pub("release"))
	s.cli(t, s.a, exitOK, "namespace", "create", prod, "--require-signed-by", "cosign:release")
	_, stderr := s.cli(t, s.b, -1, "admin", "image-policy", "set", "--remove-signing-key", "release")
	if !strings.Contains(stderr, "failed_precondition") || !strings.Contains(stderr, prod) {
		t.Fatalf("removal refusal: %s", stderr)
	}
	s.cli(t, s.b, exitOK, "namespace", "update", prod, "--clear-require-signed-by")
	s.cli(t, s.b, exitOK, "admin", "image-policy", "set", "--remove-signing-key", "release")
}

func TestIntegration_ARegistryOutageIsNeverAPass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "outage")
	defer s.writeTranscript(t, "image-policy-registry-outage.txt")
	prod := uniqueNamespace("prod")
	s.cli(t, s.a, exitOK, "admin", "image-policy", "set", "--signing-key", "release="+s.pub("release"))
	s.cli(t, s.a, exitOK, "namespace", "create", prod, "--require-signed-by", "cosign:release")
	s.cli(t, s.a, exitOK, "apply", "-n", prod, s.document(t, policyOrkflowYAML("deploy", "shell", "echo up")))

	s.registry.Stop(t)
	s.note("registry %s stopped (docker stop of this test's own container)", s.registry.Host)
	_, stderr := s.cli(t, s.a, -1, "apply", "-n", prod,
		s.document(t, policyOrkflowYAML("deploy", "shell", "echo a new version")))
	if !strings.Contains(stderr, "unavailable") || strings.Contains(stderr, "invalid_argument") {
		t.Fatalf("apply during the outage: %s", stderr)
	}
	_, stderr = s.cli(t, s.b, -1, "run", "submit", "deploy", "-n", prod)
	if !strings.Contains(stderr, "unavailable") || !strings.Contains(stderr, "IMAGE_SIGNATURE_UNAVAILABLE") {
		t.Fatalf("submit during the outage: %s", stderr)
	}
	s.requireNoRuns(t, prod)
	s.registry.Start(t)
	s.note("registry %s started again", s.registry.Host)
	runID := s.submitID(t, s.b, "deploy", prod)
	s.awaitStatus(ctx, t, s.a, runID, prod, orkstackv1.Status_STATUS_SUCCEEDED)
}

func TestIntegration_ASignatureProducedByTheCosignCLIVerifies(t *testing.T) {
	cosign, err := exec.LookPath("cosign")
	if err != nil {
		infraAbsent(t, "the cosign CLI is not on PATH (image-policy interop test): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newPolicyStack(ctx, t, "cosign")
	defer s.writeTranscript(t, "image-policy-cosign-interop.txt")
	version, _ := exec.Command(cosign, "version").CombinedOutput()
	for _, line := range strings.Split(string(version), "\n") {
		if strings.HasPrefix(line, "GitVersion:") {
			s.note("cosign %s", strings.TrimSpace(line))
		}
	}
	dir := filepath.Join(s.keys, "cosign-cli")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(cosign, args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "COSIGN_PASSWORD=")
		out, err := cmd.CombinedOutput()
		fmt.Fprintf(&s.transcript, "$ cosign %s\n%s\n", strings.Join(args, " "), out)
		if err != nil {
			t.Fatalf("cosign %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	pinned := s.image("shell") + "@" + s.digest(ctx, t, "shell")
	run("generate-key-pair")
	run("sign", "--key", "cosign.key", "--tlog-upload=false", "--yes", "--allow-insecure-registry", pinned)

	prod := uniqueNamespace("prod")
	s.cli(t, s.a, exitOK, "admin", "image-policy", "set", "--signing-key", "cosign-cli="+filepath.Join(dir, "cosign.pub"))
	s.cli(t, s.a, exitOK, "namespace", "create", prod, "--require-signed-by", "cosign:cosign-cli")
	s.cli(t, s.a, exitOK, "apply", "-n", prod, s.document(t, policyOrkflowYAML("deploy", "shell", "echo cosign")))
	runID := s.submitID(t, s.b, "deploy", prod)
	s.awaitStatus(ctx, t, s.a, runID, prod, orkstackv1.Status_STATUS_SUCCEEDED)

	// The converse: the real cosign verifies the object cosign-sign wrote.
	out := run("verify", "--key", s.pub("release"), "--insecure-ignore-tlog", "--allow-insecure-registry", pinned)
	if !strings.Contains(out, "The signatures were verified against the specified public key") {
		t.Fatalf("cosign verify of the cosign-sign signature:\n%s", out)
	}
}
