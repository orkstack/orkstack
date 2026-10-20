//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_AVersionStoredBeforeTheHeaderReadsRunsDiffsAndReappliesAsANoOp
// is the stored-definitions promise of the document header (docs/DSL.md, "The
// document header"), against a real engine, Orker, Temporal and MinIO: a
// version written before the header was required reads with the header, runs,
// diffs, and re-applies — with the header after its comments or on line 1 —
// as a no-op that leaves its object untouched.
func TestIntegration_AVersionStoredBeforeTheHeaderReadsRunsDiffsAndReappliesAsANoOp(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	cfg := artifacts.ConfigFromEnv()
	requireObjectStoreForIdempotency(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	namespace := isolatedNamespace()
	dialIsolatedNamespace(ctx, t, address, namespace)
	pool := privateTaskPool()
	taskQueue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map test pool: %v", err)
	}
	root := repoRoot(t)
	orkctlPath := testbin.Path(t, "./cmd/orkctl")
	enginePath := testbin.Path(t, "./cmd/ork")
	orkerPath := testbin.Path(t, "./cmd/orker")
	startProcess(ctx, t, "orker", orkerPath,
		"ORK_TEMPORAL_ADDRESS="+address,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+taskQueue,
		"ORK_ORKER_WORKDIR="+bindableOrkerWorkRoot(t),
	)
	engineAddr := freeAddress(t)
	startProcess(ctx, t, "ork", enginePath,
		"ORK_HTTP_ADDR="+engineAddr,
		"ORK_TEMPORAL_ADDRESS="+address,
		"ORK_TEMPORAL_NAMESPACE="+namespace,
		"ORK_ORKER_QUEUE="+taskQueue,
	)
	configPath := filepath.Join(t.TempDir(), "orkctl.json")
	session := &sessionRecorder{}
	cli := func(args ...string) (string, string, int) {
		full := append(append([]string(nil), args...), "--server", "http://"+engineAddr)
		stdout, stderr, code := runBinaryWithEnv(ctx, t, orkctlPath, root,
			[]string{configEnvVar + "=" + configPath}, full...)
		session.record(full, stdout, stderr, code)
		return stdout, stderr, code
	}
	waitForEngine(t, cli)
	waitForPoolLive(t, cli, "default", pool)

	name := fmt.Sprintf("legacy-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	legacy := fmt.Sprintf("# OrkStack orkflow DSL\n#\n# A version applied before the header was required.\nname: %s\n"+
		"version: \"1.0.0\"\ndescription: stored before the header\ntasks:\n  - id: report\n    uses: shell\n    pool: %s\n"+
		"    with:\n      op: exec\n      command: [\"sh\", \"-c\", \"echo legacy\"]\n", name, pool)
	below, ok := dsl.AddHeader(legacy)
	if !ok {
		t.Fatal("AddHeader refused the legacy document")
	}
	write := func(file, content string) string {
		path := filepath.Join(t.TempDir(), file)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	// 1. Store the legacy version the way an apply before the header did: the
	// engine prepares (and resolves) the definition under a scratch name, and
	// it is written back under the real name with its header-less bytes.
	scratch := strings.Replace(below, "name: "+name, "name: "+name+"-scratch", 1)
	requireCLI(t, cli, "prepare the definition", "apply", write("scratch.ork.yaml", scratch))
	store, err := artifacts.NewS3OrkflowStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.Get(ctx, "", name+"-scratch", "1.0.0")
	if err != nil {
		t.Fatalf("read the prepared definition: %v", err)
	}
	prepared.Name, prepared.SourceYaml = name, legacy
	if prepared.DefinitionDigest, err = dsl.DefinitionDigest(prepared); err != nil {
		t.Fatal(err)
	}
	if _, created, err := store.Put(ctx, prepared, artifacts.NoHeadExpectation()); err != nil || !created {
		t.Fatalf("write the legacy version: created=%v, %v", created, err)
	}
	key, err := artifacts.OrkflowKey("", name, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := s3bucket.Client(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	etag := func() string {
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("head %s: %v", key, err)
		}
		return aws.ToString(head.ETag)
	}
	stored := etag()

	// 2. It reads with the header.
	got := requireCLI(t, cli, "get shows the header", "orkflow", "get", name, "--version", "1.0.0", "--json")
	var shown orkstackv1.GetOrkflowResponse
	if err := api.UnmarshalJSON([]byte(got), &shown); err != nil {
		t.Fatalf("orkflow get --json: %v\n%s", err, got)
	}
	if shown.GetOrkflow().GetSourceYaml() != below {
		t.Fatalf("orkflow get shows %q, want the header after the comments: %q", shown.GetOrkflow().GetSourceYaml(), below)
	}

	// 3. It runs.
	ran := requireCLI(t, cli, "run the legacy version", "run", "submit", name, "--version", "1.0.0", "--watch")
	if !strings.Contains(ran, "SUCCEEDED") {
		t.Fatalf("the run did not succeed:\n%s", ran)
	}

	// 4. It diffs against a version applied with its header after the
	// comments, and no header line changes.
	v2 := strings.Replace(strings.Replace(below, `"1.0.0"`, `"2.0.0"`, 1), "echo legacy", "echo current", 1)
	requireCLI(t, cli, "apply 2.0.0", "apply", write("v2.ork.yaml", v2))
	diff := requireCLI(t, cli, "diff the two versions", "orkflow", "diff", name, "1.0.0", "2.0.0")
	if !strings.Contains(diff, "+      command: [\"sh\", \"-c\", \"echo current\"]") {
		t.Fatalf("the diff does not describe the change:\n%s", diff)
	}
	for _, line := range strings.Split(diff, "\n") {
		if (strings.HasPrefix(line, "-") || strings.HasPrefix(line, "+")) && !strings.HasPrefix(line, "---") &&
			!strings.HasPrefix(line, "+++") && (strings.Contains(line, "apiVersion") || strings.Contains(line, "kind: Orkflow")) {
			t.Fatalf("the diff changes a header line %q:\n%s", line, diff)
		}
	}

	// 5–6. The author's file with the header added — after the comments or
	// on line 1 — is identical to the stored version, and applying it writes
	// nothing.
	for where, content := range map[string]string{"after the comments": below, "on line 1": dsl.OrkflowHeader + legacy} {
		file := write("reapply.ork.yaml", content)
		plan := requireCLI(t, cli, "dry-run the header "+where, "apply", file, "--dry-run")
		if !strings.Contains(plan, name) || !strings.Contains(plan, "1.0.0  unchanged") {
			t.Fatalf("header %s: the plan does not report the version unchanged:\n%s", where, plan)
		}
		applied := requireCLI(t, cli, "apply the header "+where, "apply", file, "--json")
		if strings.Contains(applied, `"created"`) || !strings.Contains(applied, `"version": "1.0.0"`) {
			t.Fatalf("header %s: the apply was not a no-op:\n%s", where, applied)
		}
		if now := etag(); now != stored {
			t.Fatalf("header %s: the version object's ETag moved from %s to %s", where, stored, now)
		}
	}
	if raw, err := store.Get(ctx, "", name, "1.0.0"); err != nil || raw.GetSourceYaml() != legacy {
		t.Fatalf("the stored source moved: %q, %v", raw.GetSourceYaml(), err)
	}

	// 7. Saving the shown document unchanged in the editor applies nothing.
	editor := writeCLIEditor(t, `true`)
	edited := requireCLI(t, cli, "edit without a change", "orkflow", "edit", name, "--version", "1.0.0", "--editor", editor)
	if !strings.Contains(edited, "No change to "+name+"@1.0.0") {
		t.Fatalf("an unchanged edit was not a no-op:\n%s", edited)
	}
	t.Logf("a legacy version read, ran, diffed and re-applied as a no-op (ETag %s)", stored)
	_ = session
}
