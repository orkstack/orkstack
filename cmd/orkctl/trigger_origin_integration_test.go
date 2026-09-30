//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_TriggerInvokeRecordsOriginEndToEnd creates a trigger on
// replica A, invokes it over plain HTTP with Ork-Origin-Reference and
// Ork-Principal, and reads the full origin back on replica B; after the run
// finishes, its origin row is in MinIO and the origin filters list it.
func TestIntegration_TriggerInvokeRecordsOriginEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newOriginStack(ctx, t, "trigger")
	boot := s.requireBootLine(t)

	created := s.cli(t, s.a, exitOK, "--json", "trigger", "create", "--orkflow", s.orkflow, "--description", "GitHub deploy hook")
	var trigger struct {
		Trigger struct {
			ID string `json:"id"`
		} `json:"trigger"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal([]byte(created), &trigger); err != nil || trigger.Trigger.ID == "" || trigger.Secret == "" {
		t.Fatalf("decode trigger create: %v (%s)", err, created)
	}
	triggerID := trigger.Trigger.ID

	const ref = "https://github.com/acme/svc/actions/runs/123"
	url := "http://" + s.a.addr + strings.Replace(api.TriggerInvokePath, "{id}", triggerID, 1)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+trigger.Secret)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(api.OriginReferenceHeader, ref)
	request.Header.Set(api.AuditPrincipalHeader, "ci-github")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	fmt.Fprintf(&s.transcript, "$ curl -X POST %s -H 'Authorization: Bearer <secret>' -H '%s: %s' -H '%s: ci-github' -d '{}'    # via ork-A\n%s(HTTP %d)\n\n",
		url, api.OriginReferenceHeader, ref, api.AuditPrincipalHeader, body, response.StatusCode)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("invoke = %d: %s", response.StatusCode, body)
	}
	runID := decodeSubmittedID(t, string(body))

	onB := s.cli(t, s.b, exitOK, "--json", "run", "get", runID)
	want := map[string]string{
		"kind": "TRIGGER", "id": triggerID, "reference": ref, "description": "GitHub deploy hook", "principal": "ci-github",
	}
	origin := originOf(t, onB)
	for key, value := range want {
		if origin[key] != value {
			t.Fatalf("replica B reads origin %v, want %v", origin, want)
		}
	}
	t.Logf("EVIDENCE run_get_json_on_B=%s", compactJSON(t, onB))

	s.awaitTerminal(ctx, t, s.b, runID)

	// The origin row, straight from the bucket: the key the archive writes
	// beside the run-index row.
	prefix := fmt.Sprintf("%s/%s/trigger/%s/", artifacts.RunOriginIndexNamespace, s.namespace, triggerID)
	raw := auditRawS3Client(t, s.artifactCfg)
	var keys []string
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		page, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.artifactCfg.S3Bucket), Prefix: aws.String(prefix)})
		if err != nil {
			t.Fatalf("list %s: %v", prefix, err)
		}
		keys = keys[:0]
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
		if len(keys) == 1 && strings.HasSuffix(keys[0], "/"+runID) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(keys) != 1 || !strings.HasSuffix(keys[0], "/"+runID) {
		t.Fatalf("origin rows under %s = %v, want exactly the run's", prefix, keys)
	}
	fmt.Fprintf(&s.transcript, "$ aws s3 ls s3://%s/%s\n%s\n\n", s.artifactCfg.S3Bucket, prefix, keys[0])
	t.Logf("EVIDENCE minio_origin_row=%s", keys[0])

	s.awaitListed(t, s.b, func(ids []string) bool { return len(ids) == 1 && ids[0] == runID }, "--origin", "trigger:"+triggerID)
	s.awaitListed(t, s.b, func(ids []string) bool { return containsID(ids, runID) }, "--origin", "trigger")
	if ids := listedIDs(t, s.cli(t, s.b, exitOK, "--json", "run", "list", "--origin", "submit")); containsID(ids, runID) {
		t.Fatalf("--origin submit lists the trigger run %s", runID)
	}
	human := s.cli(t, s.b, exitOK, "run", "get", runID)
	for _, fragment := range []string{"trigger " + triggerID + " (GitHub deploy hook)", ref, "ci-github", "not authenticated"} {
		if !strings.Contains(human, fragment) {
			t.Fatalf("run get on B does not contain %q:\n%s", fragment, human)
		}
	}
	proof.Write(t, "orkctl", "run-origin-trigger.txt", []byte(s.transcript.String()))
	t.Logf("EVIDENCE boot_line_B=%s", strings.TrimSpace(boot))
}
