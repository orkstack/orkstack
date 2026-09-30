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
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// TestIntegration_DeliveryRecordedOnOneReplicaListedOnAnother drives delivery
// records through two real `ork` processes sharing one Temporal namespace and
// one MinIO bucket, with the production wiring of cmd/ork/main.go (durable
// registry, S3 delivery store, the recorder as the notifier's Observe).
//
// Replica A is the only engine running while the run executes, so A's
// projection worker makes — and records — every delivery. Replica B starts
// afterwards, has made no delivery at all, and still lists the record, shows
// it, and re-sends it: the read is the shared store, never process memory.
func TestIntegration_DeliveryRecordedOnOneReplicaListedOnAnother(t *testing.T) {
	address := temporalAddress()
	requireTemporal(t, address)
	artifactCfg := artifacts.ConfigFromEnv()
	artifactCfg.Backend = artifacts.BackendS3
	requireMinIOForIdempotency(t, artifactCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	namespace := isolatedNamespace()
	t.Logf("temporal namespace %s", namespace)
	temporalClient := dialIsolatedNamespace(ctx, t, address, namespace)

	root := repoRoot(t)
	binDir := t.TempDir()
	enginePath := buildBinary(t, root, binDir, "ork")
	orkerPath := buildBinary(t, root, binDir, "orker")
	queue := idempotencyTaskQueue()
	startIdempotencyOrker(ctx, t, temporalClient, orkerPath, address, namespace, queue)
	replicaA := startReplica(ctx, t, "ork-A", enginePath, address, namespace, queue)
	replicaA.awaitReady(ctx, t)

	var (
		mu       sync.Mutex
		received []http.Header
	)
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		received = append(received, r.Header.Clone())
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer subscriber.Close()
	requestsFor := func(runID string) []http.Header {
		mu.Lock()
		defer mu.Unlock()
		var out []http.Header
		for _, h := range received {
			if h.Get(webhooks.RunHeader) == runID {
				out = append(out, h)
			}
		}
		return out
	}

	orkflowName := fmt.Sprintf("records-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	var transcript strings.Builder
	fmt.Fprintf(&transcript, "# delivery records across two engine replicas (%s)\nbucket %s, backend s3, temporal namespace %s\n\n",
		time.Now().UTC().Format(time.RFC3339), artifactCfg.S3Bucket, namespace)

	registered := deliveriesCLI(t, replicaA, exitOK, "--json", "webhook", "register", subscriber.URL,
		"--event", "RUN_SUCCEEDED", "--orkflow", orkflowName)
	var envelope struct {
		Webhook webhooks.Webhook `json:"webhook"`
	}
	if err := json.Unmarshal([]byte(registered), &envelope); err != nil || envelope.Webhook.Secret == "" {
		t.Fatalf("webhook register --json = %v\n%s", err, registered)
	}
	hook := envelope.Webhook
	defer deliveriesCLI(t, replicaA, exitOK, "webhook", "delete", hook.ID)
	fmt.Fprintf(&transcript, "$ orkctl --server <replica A> webhook register <subscriber> --event RUN_SUCCEEDED --orkflow %s\nregistered %s\n\n",
		orkflowName, hook.ID)

	if _, err := replicaA.flows.ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: fmt.Sprintf("name: %s\nversion: \"0.1.0\"\ndescription: delivery record probe.\ntasks:\n"+
			"  - id: hello\n    uses: shell\n    timeout_seconds: 60\n    with:\n      op: exec\n"+
			"      command: [\"sh\", \"-c\", \"echo hello\"]\n", orkflowName),
	})); err != nil {
		t.Fatalf("ApplyOrkflow on A: %v", err)
	}
	submitted, err := replicaA.runs.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: orkflowName}))
	if err != nil {
		t.Fatalf("SubmitRun on A: %v", err)
	}
	runID := submitted.Msg.GetRun().GetId()
	if finished := awaitTerminalOnB(ctx, t, replicaA, runID); finished.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s ended %s (%v)", runID, finished.GetStatus(), finished.GetError())
	}
	deadline := time.Now().Add(time.Minute)
	for len(requestsFor(runID)) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the subscriber never received run %s's RUN_SUCCEEDED", runID)
		}
		time.Sleep(200 * time.Millisecond)
	}
	original := requestsFor(runID)[0]
	originalID := original.Get(webhooks.DeliveryHeader)
	t.Logf("replica A delivered %s for run %s", originalID, runID)

	// ---- replica B starts only now: it has delivered nothing --------------
	replicaB := startReplica(ctx, t, "ork-B", enginePath, address, namespace, queue)
	replicaB.awaitReady(ctx, t)

	var listed *orkstackv1.ListDeliveriesResponse
	for deadline := time.Now().Add(30 * time.Second); ; {
		out := deliveriesCLI(t, replicaB, exitOK, "--json", "webhook", "deliveries", "--run", runID, "--webhook", hook.ID)
		listed = &orkstackv1.ListDeliveriesResponse{}
		// --json is OrkStack's JSON dialect (short enum names), which the
		// API's own decoder reads.
		if err := api.UnmarshalJSON([]byte(out), listed); err != nil {
			t.Fatalf("webhook deliveries --json is not a ListDeliveriesResponse: %v\n%s", err, out)
		}
		if len(listed.GetDeliveries()) > 0 || time.Now().After(deadline) {
			fmt.Fprintf(&transcript, "$ orkctl --server <replica B> webhook deliveries --run %s --webhook %s --json\n%s\n\n", runID, hook.ID, out)
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(listed.GetDeliveries()) != 1 {
		t.Fatalf("replica B lists %d records for run %s, want the one replica A made", len(listed.GetDeliveries()), runID)
	}
	rec := listed.GetDeliveries()[0]
	if rec.GetId() != originalID || !rec.GetDelivered() || rec.GetWebhookId() != hook.ID ||
		rec.GetSignature() == "" || rec.GetSignature() != original.Get(webhooks.SignatureHeader) {
		t.Fatalf("replica B's record = %v, want %s delivered with the signature the subscriber saw (%s)",
			rec, originalID, original.Get(webhooks.SignatureHeader))
	}
	human := deliveriesCLI(t, replicaB, exitOK, "webhook", "deliveries", "--run", runID, "--webhook", hook.ID)
	shown := deliveriesCLI(t, replicaB, exitOK, "webhook", "delivery", originalID)
	if !strings.Contains(human, originalID) || !strings.Contains(human, "(the run's start)") ||
		!strings.Contains(shown, "SIGNATURE  "+rec.GetSignature()) || !strings.Contains(shown, "1  ") {
		t.Fatalf("human renderings on B:\n%s\n%s", human, shown)
	}
	fmt.Fprintf(&transcript, "$ orkctl --server <replica B> webhook deliveries --run %s\n%s\n$ orkctl --server <replica B> webhook delivery %s\n%s\n",
		runID, human, originalID, shown)

	// ---- B re-sends it: one POST, recorded as a new delivery --------------
	before := len(requestsFor(runID))
	redelivered := deliveriesCLI(t, replicaB, exitOK, "webhook", "redeliver", originalID)
	after := requestsFor(runID)
	if len(after) != before+1 {
		t.Fatalf("the subscriber saw %d more requests for one redelivery, want 1", len(after)-before)
	}
	again := after[len(after)-1]
	if again.Get(webhooks.RedeliveryHeader) != originalID || again.Get(webhooks.AttemptHeader) != "1" ||
		!strings.Contains(redelivered, "Redelivered "+originalID+" as "+again.Get(webhooks.DeliveryHeader)+": delivered, 200") {
		t.Fatalf("redelivery headers %v, output:\n%s", again, redelivered)
	}
	fmt.Fprintf(&transcript, "$ orkctl --server <replica B> webhook redeliver %s\n%s\n", originalID, redelivered)
	fromA := deliveriesCLI(t, replicaA, exitOK, "webhook", "deliveries", "--run", runID, "--webhook", hook.ID)
	if !strings.Contains(fromA, again.Get(webhooks.DeliveryHeader)) {
		t.Fatalf("replica A does not list the redelivery replica B recorded:\n%s", fromA)
	}
	fmt.Fprintf(&transcript, "$ orkctl --server <replica A> webhook deliveries --run %s\n%s\n", runID, fromA)

	t.Logf("EVIDENCE delivery_records_two_replica recorded_on=A listed_on=B original=%s redelivered_on=B as=%s %s=%s listed_back_on=A",
		originalID, again.Get(webhooks.DeliveryHeader), webhooks.RedeliveryHeader, again.Get(webhooks.RedeliveryHeader))
	proof.Write(t, "orkctl", "delivery-records-two-replicas.txt", []byte(transcript.String()))
}

// deliveriesCLI runs the real orkctl against one replica, requiring an exit
// code, and returns stdout.
func deliveriesCLI(t *testing.T, replica *engineReplica, want int, args ...string) string {
	t.Helper()
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	var stdout, stderr bytes.Buffer
	code := Execute(append(args, "--server", "http://"+replica.addr), &stdout, &stderr)
	if code != want {
		t.Fatalf("orkctl %s against %s exited %d, want %d: %s\n%s",
			strings.Join(args, " "), replica.name, code, want, stderr.String(), stdout.String())
	}
	return stdout.String()
}
