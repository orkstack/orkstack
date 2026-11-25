//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/dockerport"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The same policy on a real store (docs/DEPLOY.md, "An Orker's own storage
// credential"): s3scope is OrkStack's own reading of the policy, so one test
// runs it on the store the proofs use. It never touches the shared store's
// identities: it starts a SeaweedFS of its own, of the image the development
// stack pins, with an identity file of its own, and stops it when it ends.

// seaweedImage is the SeaweedFS the development stack pins, read from
// deploy/docker-compose.yml so the two cannot drift.
func seaweedImage(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy", "docker-compose.yml"))
	must.OK(t, err)
	m := regexp.MustCompile(`image: (chrislusf/seaweedfs:[^\s@]+@sha256:[0-9a-f]{64})`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("deploy/docker-compose.yml pins no SeaweedFS image by digest")
	}
	return string(m[1])
}

// seaweedIdentities is the store's identity file: an administrator, which
// the engine signs with, and the documented Orker policy (s3scope.OrkerPolicy,
// which a test holds to docs/DEPLOY.md) in SeaweedFS's actions. SeaweedFS's Write covers put and delete together, so the Orker's
// writes are granted as Write; what the check asks is outside every prefix
// granted.
func seaweedIdentities(bucket string) []byte {
	var orker []string
	for _, rule := range s3scope.OrkerPolicy() {
		for _, action := range []struct {
			name  string
			verbs []s3scope.Verb
		}{
			{"Read", []s3scope.Verb{s3scope.Get, s3scope.Head}},
			{"Write", []s3scope.Verb{s3scope.Put, s3scope.Delete}},
			{"List", []s3scope.Verb{s3scope.List}},
		} {
			if slices.ContainsFunc(action.verbs, func(v s3scope.Verb) bool { return slices.Contains(rule.Verbs, v) }) {
				orker = append(orker, fmt.Sprintf("%s:%s/%s*", action.name, bucket, rule.Prefix))
			}
		}
	}
	doc := map[string]any{"identities": []map[string]any{
		{"name": "admin", "credentials": []map[string]string{{"accessKey": "scope-admin", "secretKey": "scope-admin-secret"}},
			"actions": []string{"Admin", "Read", "Write", "List", "Tagging"}},
		{"name": "orker", "credentials": []map[string]string{{"accessKey": "scope-orker", "secretKey": "scope-orker-secret"}},
			"actions": orker},
	}}
	out, _ := json.Marshal(doc)
	return out
}

// processStartTime is this process's start time as /proc records it, the
// third field of the owner label proofrun clean reaps by.
func processStartTime(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("/proc/self/stat")
	must.OK(t, err)
	s := string(raw)
	f := strings.Fields(s[strings.LastIndex(s, ") ")+2:])
	if len(f) < 20 {
		t.Fatalf("/proc/self/stat has %d fields after the name", len(f))
	}
	return f[19]
}

// startOwnSeaweed starts the test's own store, labelled with this process as
// its owner, so proofrun clean stops it if the test is killed; --rm removes
// its anonymous volume with it. It returns the endpoint.
func startOwnSeaweed(t *testing.T, ctx context.Context, bucket string) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		infraAbsent(t, "docker is not on PATH: %v", err)
	}
	dir := t.TempDir()
	config := filepath.Join(dir, "s3.json")
	must.OK(t, os.WriteFile(config, seaweedIdentities(bucket), 0o644))
	must.OK(t, os.Chmod(dir, 0o755))
	host, err := os.Hostname()
	must.OK(t, err)
	proofID := os.Getenv(proof.EnvID)
	if proofID == "" {
		proofID = "by-hand"
	}
	name := fmt.Sprintf("ork-scope-seaweed-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"--label", "orkstack.proof="+proofID,
		"--label", fmt.Sprintf("orkstack.proof.owner=%s:%d:%s", host, os.Getpid(), processStartTime(t)),
		"-p", dockerport.Publish("8333"),
		"-v", config+":/etc/s3.json:ro",
		seaweedImage(t), "mini", "-dir=/data", "-s3.config=/etc/s3.json", "-s3.autoCreateBucket=false",
	).CombinedOutput()
	must.OKf(t, err, "start the test's own SeaweedFS: %s", out)
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if out, err := exec.CommandContext(stop, "docker", "stop", "-t", "2", name).CombinedOutput(); err != nil {
			t.Logf("stop %s: %v: %s", name, err, out)
		}
	})
	// Docker picked the port and holds it for this container alone.
	addr, err := dockerport.Addr(ctx, name, "8333")
	must.OK(t, err)
	endpoint := "http://" + addr
	// The bucket, made by the administrator once the store answers.
	deadline := time.Now().Add(90 * time.Second)
	for {
		status, err := signedPut(ctx, endpoint+"/"+bucket, "scope-admin", "scope-admin-secret")
		if err == nil && status == http.StatusOK {
			return endpoint
		}
		if time.Now().After(deadline) {
			t.Fatalf("the test's own SeaweedFS never made its bucket: status %d, %v", status, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// signedPut makes one SigV4-signed PUT with curl, which signs as the SDK does.
func signedPut(ctx context.Context, url, key, secret string) (int, error) {
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-X", "PUT",
		"--aws-sigv4", "aws:amz:us-east-1:s3", "--user", key+":"+secret, url)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return 0, err
	}
	var status int
	_, err := fmt.Sscan(out.String(), &status)
	return status, err
}

// TestIntegration_TheDocumentedOrkerPolicyHoldsOnSeaweedFS: on SeaweedFS,
// with the documented policy as its own identity, an Orker runs every kind
// of task and reports its credential scoped, while the engine works with
// the administrator's.
func TestIntegration_TheDocumentedOrkerPolicyHoldsOnSeaweedFS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	bucket := "scope-real"
	endpoint := startOwnSeaweed(t, ctx, bucket)
	store := []string{"ORK_S3_ENDPOINT=" + endpoint, "ORK_S3_BUCKET=" + bucket, "ORK_S3_PATH_STYLE=true"}
	s := startScopeStack(t,
		append(store, "ORK_S3_ACCESS_KEY=scope-admin", "ORK_S3_SECRET_KEY=scope-admin-secret"),
		append(store, "ORK_S3_ACCESS_KEY=scope-orker", "ORK_S3_SECRET_KEY=scope-orker-secret"))
	fmt.Fprintf(&s.log, "# the documented Orker policy on SeaweedFS (%s)\nimage %s, endpoint %s, bucket %s\n\n",
		time.Now().UTC().Format(time.RFC3339), seaweedImage(t), endpoint, bucket)

	scope := s.awaitScope(orkstackv1.StoreScope_STORE_SCOPE_SCOPED)
	name := "scope-seaweed-" + fmt.Sprint(time.Now().UnixNano()%1_000_000_000)
	s.create(name, fmt.Sprintf(everyKindOfTask, name))
	for i := 1; i <= 2; i++ {
		run := s.submit(name)
		if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("run %d (%s) on SeaweedFS ended %v: %v\n%s", i, run.GetId(), run.GetStatus(), run.GetError(), s.log.String())
		}
	}
	// What this store says of a run context that is not there, to a credential
	// that may get but not list: missing or refused, never invalid and never
	// an answer the Orker cannot read.
	contexts, err := artifacts.NewRunContextStore(ctx, artifacts.Config{Backend: artifacts.BackendS3,
		S3Endpoint: endpoint, S3Region: "us-east-1", S3Bucket: bucket, S3PathStyle: true,
		S3AccessKey: "scope-orker", S3SecretKey: "scope-orker-secret"})
	must.OK(t, err)
	_, missing := contexts.ReadRunContext(ctx, "default", "ork-no-such-run", strings.Repeat("1", 64))
	if !errors.Is(missing, artifacts.ErrRunContextMissing) && !errors.Is(missing, artifacts.ErrRunContextRefused) {
		t.Fatalf("a run context that is not there reads %v, want missing or refused", missing)
	}
	fmt.Fprintf(&s.log, "a run context that is not there, read with the Orker's credential: %v\n", missing)
	t.Logf("EVIDENCE documented Orker policy on SeaweedFS %s: store_scope=%v; two runs of every kind of task succeeded; a missing run context reads %q",
		seaweedImage(t), scope.GetStoreScope(), missing)
	proof.Write(t, "orkctl", "store-scope-seaweedfs", []byte(s.log.String()))
}
