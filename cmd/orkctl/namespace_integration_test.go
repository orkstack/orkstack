//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_TwoNamespacesStayDisjointThroughTheCLI is the end-to-end
// namespace acceptance, over a real Temporal cluster and a real bucket, driven by the
// real orkctl binary against the real ork and orker binaries.
//
// What it proves, in the order a user meets it:
//
//  1. `orkctl namespace create` registers two namespaces, and repeating one is
//     idempotent rather than an error.
//  2. The SAME orkflow name is applied into both, with different task sets —
//     which is only meaningful because a name is unique within a namespace and
//     not across the deployment.
//  3. Listing and getting in one namespace never shows the other's document,
//     and the default namespace holds neither.
//  4. A run submitted in each executes for real — both under ONE idempotency
//     key, which is a coordinate inside a namespace like every other one — and
//     listing runs in one never shows the other's.
//  5. `orkctl namespace use` persists a default so `-n` is not retyped.
//  6. Deleting a namespace that still holds an orkflow is refused, by name.
//  7. Submitting into a namespace nobody registered names it AND lists the ones
//     that exist.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace and its own pool, so a
// parallel agent's Orker can never pick up its tasks and its runs are invisible
// to anyone else's listing.
func TestIntegration_TwoNamespacesStayDisjointThroughTheCLI(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Provisions the namespace and waits until the frontend serves it; the
	// client it returns is closed by its own t.Cleanup.
	temporalNamespace := isolatedTemporalNamespace("ork-namespace-cli")
	dialIsolatedNamespace(ctx, t, address, temporalNamespace)

	pool := privateTaskPool()
	taskQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map test pool: %v", err)
	}

	repoRoot := repoRoot(t)
	binDir := t.TempDir()
	orkctlPath := buildBinary(t, repoRoot, binDir, "orkctl")
	enginePath := buildBinary(t, repoRoot, binDir, "ork")
	orkerPath := buildBinary(t, repoRoot, binDir, "orker")

	startNamespacedOrker(ctx, t, orkerPath, address, temporalNamespace, taskQueue)
	engineAddr := startNamespacedEngine(ctx, t, enginePath, address, temporalNamespace, taskQueue)

	// Every invocation gets its own config file, so `namespace use` writes
	// where the test can read it and never touches the developer's own.
	configPath := filepath.Join(t.TempDir(), "config.json")

	transcript := &sessionRecorder{}
	cli := func(args ...string) (string, string, int) {
		full := append(append([]string(nil), args...), "--server", "http://"+engineAddr)
		stdout, stderr, code := runBinaryWithEnv(ctx, t, orkctlPath, repoRoot,
			[]string{configEnvVar + "=" + configPath, namespaceEnvVar + "="}, full...)
		transcript.record(full, stdout, stderr, code)
		return stdout, stderr, code
	}
	waitForEngine(t, cli)
	t.Cleanup(func() {
		publishTranscript(t, "two-namespace-session.txt", transcript.transcript.String())
	})

	// The two namespaces are named for this run: another agent's registry
	// entries must not decide whether this test passes.
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	alpha := "alpha-" + token
	bravo := "bravo-" + token
	// The bucket is shared, so this run takes its own two namespaces away with
	// it. It cannot do that through the CLI: `namespace delete` refuses one
	// that still holds an orkflow, and nothing deletes an orkflow yet — so the
	// objects go, definitions first, exactly as an operator emptying the
	// prefix would. Leaving them costs the next run a longer "these do:" list
	// and the bucket one more tenant per execution.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		removeNamespaceObjects(t, cleanupCtx, alpha, bravo)
	})

	// ── 1. create, and creating twice is idempotent ────────────────────────
	for _, ns := range []string{alpha, bravo} {
		stdout, stderr, code := cli("namespace", "create", ns,
			"--description", "namespace acceptance", "--default-pool", pool)
		if code != 0 {
			t.Fatalf("namespace create %s exit = %d\nstdout:\n%s\nstderr:\n%s",
				ns, code, stdout, stderr)
		}
		if !strings.Contains(stdout, "Created namespace "+ns) {
			t.Fatalf("namespace create %s stdout:\n%s", ns, stdout)
		}
	}
	stdout, stderr, code := cli("namespace", "create", alpha,
		"--description", "namespace acceptance", "--default-pool", pool)
	if code != 0 {
		t.Fatalf("repeating a create exit = %d, want 0 (it must be idempotent)"+
			"\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "Unchanged namespace "+alpha) {
		t.Fatalf("repeating a create did not report it as unchanged:\n%s", stdout)
	}

	// A create that differs under the same name is a collision, not a repeat.
	if _, stderr, code = cli("namespace", "create", alpha,
		"--description", "something else", "--default-pool", pool); code == 0 {
		t.Fatal("re-creating a namespace with a different spec succeeded")
	} else if !strings.Contains(stderr, "already_exists") {
		t.Fatalf("a differing create did not report already_exists:\n%s", stderr)
	}

	// ── 2. the same orkflow name in both, with different task sets ─────────
	const orkflowName = "deploy"
	alphaDoc := writeNamespacedOrkflow(t, orkflowName, pool, "build")
	bravoDoc := writeNamespacedOrkflow(t, orkflowName, pool, "ship")
	if stdout, stderr, code = cli("apply", alphaDoc, "-n", alpha); code != 0 {
		t.Fatalf("apply into %s exit = %d\nstdout:\n%s\nstderr:\n%s", alpha, code, stdout, stderr)
	}
	if stdout, stderr, code = cli("apply", bravoDoc, "-n", bravo); code != 0 {
		t.Fatalf("apply into %s exit = %d\nstdout:\n%s\nstderr:\n%s", bravo, code, stdout, stderr)
	}

	// ── 3. lists and gets are disjoint ─────────────────────────────────────
	for _, tc := range []struct{ namespace, taskID, otherTaskID string }{
		{alpha, "build", "ship"},
		{bravo, "ship", "build"},
	} {
		listed, stderr, code := cli("orkflow", "list", "-n", tc.namespace)
		if code != 0 {
			t.Fatalf("orkflow list -n %s exit = %d\nstderr:\n%s", tc.namespace, code, stderr)
		}
		if strings.Count(listed, orkflowName) != 1 {
			t.Fatalf("orkflow list -n %s did not show exactly its own %q:\n%s",
				tc.namespace, orkflowName, listed)
		}
		got, stderr, code := cli("orkflow", "get", orkflowName, "-n", tc.namespace)
		if code != 0 {
			t.Fatalf("orkflow get -n %s exit = %d\nstderr:\n%s", tc.namespace, code, stderr)
		}
		if !strings.Contains(got, tc.taskID) || strings.Contains(got, tc.otherTaskID) {
			t.Fatalf("orkflow get %s -n %s served the wrong namespace's document:\n%s",
				orkflowName, tc.namespace, got)
		}
	}
	// By NAME column, not substring: the default namespace holds whatever
	// this process's other suites applied, and a description may say
	// "deploy" (examples/branch-and-recover.ork.yaml's does).
	if listed, _, _ := cli("orkflow", "list"); listsName(listed, orkflowName) {
		t.Fatalf("the default namespace lists another namespace's orkflow:\n%s", listed)
	}

	// ── 4. a real run in each, and run lists stay disjoint ─────────────────
	//
	// Both submissions carry the SAME idempotency key, deliberately. A key is
	// the caller's own word for one submission and two tenants pick the same
	// words, so it is a coordinate inside a namespace like every other one
	// here: the claim that arbitrates it is a Temporal workflow whose id is
	// the key's digest, one binding for the whole deployment, so an unscoped
	// key would answer bravo with alpha's run id and never start bravo's
	// work. This is the live half of
	// TestSubmitRun_OneIdempotencyKeyInTwoNamespacesStartsTwoRuns, against the
	// real claim authority rather than a double. The key needs no per-run
	// suffix: this test's Temporal namespace is its own, so no other run's
	// claim can be in it.
	const sharedKey = "nightly"
	runIn := map[string]string{}
	for _, ns := range []string{alpha, bravo} {
		waitForPoolLive(t, cli, ns, pool)
		stdout, stderr, code := cli("run", "submit", orkflowName, "-n", ns,
			"--idempotency-key", sharedKey, "--watch")
		if code != 0 {
			t.Fatalf("run submit -n %s exit = %d\nstdout:\n%s\nstderr:\n%s",
				ns, code, stdout, stderr)
		}
		runID := parseRunID(t, stdout)
		if !strings.Contains(stdout, "SUCCEEDED") {
			t.Fatalf("run %s in %s did not reach SUCCEEDED:\n%s", runID, ns, stdout)
		}
		runIn[ns] = runID
	}
	if runIn[alpha] == runIn[bravo] {
		t.Fatalf("one idempotency key in two namespaces produced one run (%s): "+
			"the second namespace was answered with the first one's run and its own "+
			"was never started", runIn[alpha])
	}
	// …and scoping the key must not stop it deduplicating: repeated inside its
	// own namespace it still names the run it already started.
	stdout, stderr, code = cli("run", "submit", orkflowName, "-n", alpha,
		"--idempotency-key", sharedKey)
	if code != 0 {
		t.Fatalf("repeating the key in %s exit = %d\nstdout:\n%s\nstderr:\n%s",
			alpha, code, stdout, stderr)
	}
	if got := parseRunID(t, stdout); got != runIn[alpha] {
		t.Fatalf("repeating the key in %s answered with run %s, want its own run %s",
			alpha, got, runIn[alpha])
	}
	for _, tc := range []struct{ namespace, want, unwanted string }{
		{alpha, runIn[alpha], runIn[bravo]},
		{bravo, runIn[bravo], runIn[alpha]},
	} {
		listed, stderr, code := cli("run", "list", "-n", tc.namespace)
		if code != 0 {
			t.Fatalf("run list -n %s exit = %d\nstderr:\n%s", tc.namespace, code, stderr)
		}
		if !strings.Contains(listed, tc.want) {
			t.Fatalf("run list -n %s does not show its own run %s:\n%s",
				tc.namespace, tc.want, listed)
		}
		if strings.Contains(listed, tc.unwanted) {
			t.Fatalf("run list -n %s leaked run %s from the other namespace:\n%s",
				tc.namespace, tc.unwanted, listed)
		}
	}

	// ── 5. the persisted default ───────────────────────────────────────────
	if stdout, stderr, code = cli("namespace", "use", alpha); code != 0 {
		t.Fatalf("namespace use %s exit = %d\nstdout:\n%s\nstderr:\n%s",
			alpha, code, stdout, stderr)
	}
	saved, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read the saved config: %v", err)
	}
	if !strings.Contains(string(saved), alpha) {
		t.Fatalf("the config file does not name %s:\n%s", alpha, saved)
	}
	listedAfterUse, stderr, code := cli("orkflow", "list")
	if code != 0 {
		t.Fatalf("orkflow list after `namespace use` exit = %d\nstderr:\n%s", code, stderr)
	}
	if !strings.Contains(listedAfterUse, orkflowName) {
		t.Fatalf("orkflow list with no -n did not act in the saved namespace:\n%s",
			listedAfterUse)
	}

	// ── 6. delete refuses while the namespace holds an orkflow ─────────────
	_, stderr, code = cli("namespace", "delete", alpha, "--yes")
	if code == 0 {
		t.Fatal("deleting a namespace that still holds an orkflow succeeded")
	}
	for _, want := range []string{alpha, orkflowName} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the delete refusal does not name %q:\n%s", want, stderr)
		}
	}

	// ── 7. a missing namespace names itself and its alternatives ───────────
	_, stderr, code = cli("run", "submit", orkflowName, "-n", alpha+"-typo")
	if code == 0 {
		t.Fatal("submitting into a namespace nobody registered succeeded")
	}
	for _, want := range []string{alpha + "-typo", alpha, bravo} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the missing-namespace error does not name %q:\n%s", want, stderr)
		}
	}
}

// isolatedTemporalNamespace names a Temporal namespace this run alone uses
// (AGENTS.md §11.1), keeping the operator's own value as the prefix so the
// isolation choice stays visible in `temporal workflow list`.
// listsName reports whether `orkflow list`'s table has a row whose NAME
// column is exactly name.
func listsName(listed, name string) bool {
	for _, line := range strings.Split(listed, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == name {
			return true
		}
	}
	return false
}

func isolatedTemporalNamespace(fallback string) string {
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = fallback
	}
	return fmt.Sprintf("%s-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
}

// startNamespacedOrker is startOrker bound to one Temporal namespace.
func startNamespacedOrker(
	ctx context.Context,
	t *testing.T,
	orkerPath, temporalAddr, temporalNamespace, queue string,
) {
	t.Helper()
	startProcess(ctx, t, "orker", orkerPath,
		"ORK_TEMPORAL_ADDRESS="+temporalAddr,
		"ORK_TEMPORAL_NAMESPACE="+temporalNamespace,
		"ORK_ORKER_QUEUE="+queue,
		"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=local",
		"ORK_ARTIFACT_DIR="+sharedLocalArtifactDir(t),
	)
}

// startNamespacedEngine is startEngine bound to one Temporal namespace.
//
// Both binaries are pinned to ONE local artifact root. The namespace registry
// and the run's commit records must be the same durable store for every process
// in the test: the engine mints the commit URI and the Orker reads it (§5.4),
// so two roots fail every activity with COMMIT_READ_FAILED.
func startNamespacedEngine(
	ctx context.Context,
	t *testing.T,
	enginePath, temporalAddr, temporalNamespace, orkerQueue string,
) string {
	t.Helper()
	addr := freeAddress(t)
	startProcess(ctx, t, "ork", enginePath,
		"ORK_HTTP_ADDR="+addr,
		"ORK_TEMPORAL_ADDRESS="+temporalAddr,
		"ORK_TEMPORAL_NAMESPACE="+temporalNamespace,
		"ORK_ORKER_QUEUE="+orkerQueue,
		"ORK_ARTIFACT_BACKEND=local",
		"ORK_ARTIFACT_DIR="+sharedLocalArtifactDir(t),
	)
	return addr
}

// writeNamespacedOrkflow writes a one-task orkflow pinned to this test's pool.
//
// The document deliberately names NO namespace: the selector is what puts it in
// one, which is how the same file is applied into two of them. The task id
// varies so the two documents are distinguishable by their contents rather than
// only by where they were stored — identical documents would pass a
// disjointness test even if both namespaces resolved to one object.
func writeNamespacedOrkflow(t *testing.T, name, pool, taskID string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+"-"+taskID+".ork.yaml")
	document := fmt.Sprintf(`name: %s
version: 1.0.0
description: namespace acceptance
tasks:
  - id: %s
    uses: shell
    pool: %s
    with:
      op: exec
      command: ["sh", "-c", "echo %s"]
`, name, taskID, pool, taskID)
	if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
		t.Fatalf("write orkflow %s: %v", name, err)
	}
	return path
}

// publishTranscript records the CLI session under ORK_PROOF_DIR when it is
// set (scripts/proof.sh).
func publishTranscript(t *testing.T, name, body string) {
	t.Helper()
	header := fmt.Sprintf(
		"# orkctl driving a real ork + orker + Temporal + MinIO (%s)\n"+
			"# Two namespaces, one orkflow name, two real runs. Every command below is\n"+
			"# the orkctl binary; nothing is simulated.\n"+
			"# generated by: cmd/orkctl/namespace_integration_test.go\n\n",
		time.Now().UTC().Format(time.RFC3339))
	proof.Write(t, "orkctl", name, []byte(header+body))
}

// runBinaryWithEnv is runBinary with extra environment, so an invocation can be
// pointed at this test's own orkctl config file.
func runBinaryWithEnv(
	ctx context.Context,
	t *testing.T,
	path, dir string,
	env []string,
	args ...string,
) (string, string, int) {
	t.Helper()
	previous := make(map[string]string, len(env))
	for _, assignment := range env {
		key, value, _ := strings.Cut(assignment, "=")
		previous[key] = os.Getenv(key)
		t.Setenv(key, value)
	}
	stdout, stderr, code := runBinary(ctx, t, path, dir, args...)
	for key, value := range previous {
		t.Setenv(key, value)
	}
	return stdout, stderr, code
}

// removeNamespaceObjects deletes a namespace's definitions and its registry
// document straight from the bucket.
//
// It talks to the store the engine talks to, not through it: the API has no
// way to delete an orkflow, and a namespace that still holds one cannot be
// deleted — correctly, because the definitions would survive where nothing
// lists them. Emptying both prefixes is what an operator decommissioning a
// tenant does, and it is what leaves the shared bucket as this run found it.
//
// It is best-effort: a cleanup that failed the test would report a fault in
// the tree that is not in the tree.
func removeNamespaceObjects(t *testing.T, ctx context.Context, namespaces ...string) {
	t.Helper()
	cfg := artifacts.ConfigFromEnv()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)),
	)
	if err != nil {
		t.Logf("cleanup: load aws config: %v", err)
		return
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3PathStyle
	})
	for _, namespace := range namespaces {
		prefixes := []string{artifacts.OrkflowPrefix + "/" + namespace + "/"}
		if key, err := artifacts.NamespaceKey(namespace); err == nil {
			prefixes = append(prefixes, key)
		}
		for _, prefix := range prefixes {
			pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
				Bucket: aws.String(cfg.S3Bucket),
				Prefix: aws.String(prefix),
			})
			for pager.HasMorePages() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					t.Logf("cleanup: list %s: %v", prefix, err)
					break
				}
				for _, object := range page.Contents {
					if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
						Bucket: aws.String(cfg.S3Bucket),
						Key:    object.Key,
					}); err != nil {
						t.Logf("cleanup: delete %s: %v", aws.ToString(object.Key), err)
					}
				}
			}
		}
	}
}
