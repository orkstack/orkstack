//go:build integration

package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// These tests prove delivery records on the production shape: the durable
// webhook registry on MinIO (so deliveries are signed through
// Store.ListForDelivery, scoped-subscriptions §4.1), an S3DeliveryStore wired
// both as the notifier's Observe callback and as api.Config.Deliveries, a real
// run on a private Temporal namespace, and a receiver that reads the bytes.

// recordsOrkflowYAML is a one-task orkflow on a private pool with one declared
// input, so a test can look for the input's value in the stored record.
func recordsOrkflowYAML(name, pool string) string {
	return fmt.Sprintf(`name: %s
version: "1.0.0"
description: One task whose run transitions reach a recorded webhook.
inputs:
  marker:
    type: string
tasks:
  - id: only
    uses: shell
    pool: %q
    with:
      op: exec
`, name, pool)
}

// switchableSubscriber answers every request with a status the test can
// change, and keeps every request it read.
type switchableSubscriber struct {
	mu       sync.Mutex
	status   int
	requests []switchableRequest
	server   *httptest.Server
}

type switchableRequest struct {
	headers http.Header
	body    []byte
	status  int
}

func newSwitchableSubscriber(t *testing.T, status int) *switchableSubscriber {
	t.Helper()
	s := &switchableSubscriber{status: status}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		status := s.status
		s.requests = append(s.requests, switchableRequest{headers: r.Header.Clone(), body: body, status: status})
		s.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *switchableSubscriber) answer(status int) {
	s.mu.Lock()
	s.status = status
	s.mu.Unlock()
}

// forDelivery returns every request that carried this X-Ork-Delivery.
func (s *switchableSubscriber) forDelivery(id string) []switchableRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []switchableRequest
	for _, req := range s.requests {
		if req.headers.Get(webhooks.DeliveryHeader) == id {
			out = append(out, req)
		}
	}
	return out
}

func (s *switchableSubscriber) forRun(runID string) []switchableRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []switchableRequest
	for _, req := range s.requests {
		if req.headers.Get(webhooks.RunHeader) == runID {
			out = append(out, req)
		}
	}
	return out
}

// failingTaskWorker stands in for an Orker whose container broke the contract:
// every task fails, non-retryably, so the run fails at once.
func failingTaskWorker(t *testing.T, temporalClient client.Client, queue string) {
	t.Helper()
	w := worker.New(temporalClient, queue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	w.RegisterActivityWithOptions(
		func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return nil, temporal.NewNonRetryableApplicationError(
				"VALUES_MISSING: the container exited 0 but wrote no values.json",
				task.CategoryContractViolation, nil, &orkstackv1.ActivityFailureDetail{
					ReasonCode: "VALUES_MISSING",
					Category:   orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
					Attempt:    activity.GetInfo(ctx).Attempt,
				})
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	if err := w.Start(); err != nil {
		t.Fatalf("start task worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

// recordsHarness is one engine replica with durable registry and record store.
type recordsHarness struct {
	cfg        artifacts.Config
	raw        *s3.Client
	engine     *scopedEngine
	store      artifacts.DeliveryStore
	subscriber *switchableSubscriber
	hook       webhooks.Webhook
	orkflow    string
}

func startRecordsHarness(t *testing.T, ctx context.Context, fail bool, subscriberStatus int, events ...string) *recordsHarness {
	t.Helper()
	cfg := s3Config()
	requireMinIO(t, cfg)
	temporalClient := scopedCluster(t, ctx)
	pool := uniquePool("delivery-records")
	queue, err := task.QueueForPool(pool, "")
	if err != nil {
		t.Fatalf("map pool: %v", err)
	}
	if fail {
		failingTaskWorker(t, temporalClient, queue)
	} else {
		committingTaskWorker(t, temporalClient, queue)
	}
	webhookStore, err := artifacts.NewS3WebhookStore(ctx, cfg)
	if err != nil {
		infraAbsent(t, "cannot open the webhook registry: %v", err)
	}
	store, err := artifacts.NewS3DeliveryStore(ctx, cfg)
	if err != nil {
		infraAbsent(t, "cannot open the delivery record store: %v", err)
	}
	h := &recordsHarness{
		cfg: cfg, raw: newRawS3ClientForCommitE2E(t, cfg), store: store,
		subscriber: newSwitchableSubscriber(t, subscriberStatus),
		orkflow:    fmt.Sprintf("records-%d", time.Now().UnixNano()%1_000_000_000),
	}
	h.engine = startScopedEngine(t, scopedEngineConfig{
		temporal: temporalClient, runQueue: queue, registry: webhooks.NewDurableRegistry(webhookStore),
		project: true, snapshots: newTestSnapshotStore(), deliveries: store,
		config: api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Starter: engine.NewTemporalStarter(temporalClient, queue)},
	})
	// Scoped to this test's orkflow, so a run of another suite sharing the
	// bucket's registry never reaches this subscriber.
	h.hook = registerWebhookOverHTTP(t, h.engine.httpClient, h.engine.origin, map[string]any{
		"url": h.subscriber.server.URL, "events": events, "orkflowName": h.orkflow,
	})
	if h.hook.Secret == "" {
		t.Fatal("registration disclosed no signing secret")
	}
	t.Cleanup(func() {
		doWebhookRequest(t, h.engine.httpClient, http.MethodDelete,
			h.engine.origin+strings.Replace(api.WebhookItemPath, "{id}", h.hook.ID, 1), nil)
	})
	if _, err := h.engine.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: recordsOrkflowYAML(h.orkflow, pool)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return h
}

// run submits one run carrying marker as its input, drains it, and waits for
// every delivery to settle — which is also when every record has been
// written, because the recorder runs inside the delivery's goroutine.
func (h *recordsHarness) run(t *testing.T, ctx context.Context, marker string) *orkstackv1.Run {
	t.Helper()
	input, err := structpb.NewStruct(map[string]any{"marker": marker})
	if err != nil {
		t.Fatalf("input: %v", err)
	}
	submitted, err := h.engine.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: h.orkflow, Input: &orkstackv1.Payload{Data: input},
	}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	run := h.engine.drainRun(t, ctx, submitted.Msg.GetRun().GetId(), "")
	h.engine.settle(t)
	return run
}

func (h *recordsHarness) list(t *testing.T, ctx context.Context, runID string) []*orkstackv1.Delivery {
	t.Helper()
	resp, err := h.engine.webhookClient().ListDeliveries(ctx, connect.NewRequest(&orkstackv1.ListDeliveriesRequest{
		// This test's subscription only: the registry is the bucket's, and a
		// subscription another suite left behind may also match the run.
		RunId: runID, WebhookId: h.hook.ID,
	}))
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	return resp.Msg.GetDeliveries()
}

func (h *recordsHarness) object(t *testing.T, ctx context.Context, rec *orkstackv1.Delivery) (string, string, []byte) {
	t.Helper()
	key, err := artifacts.DeliveryKey(rec)
	if err != nil {
		t.Fatalf("DeliveryKey: %v", err)
	}
	object, err := h.raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(h.cfg.S3Bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject(%s): %v", key, err)
	}
	defer object.Body.Close()
	body, err := io.ReadAll(object.Body)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return key, aws.ToString(object.ContentType), body
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func TestIntegration_DeliveryRecordIsWrittenWhenADeliverySettles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	h := startRecordsHarness(t, ctx, false, http.StatusOK, "RUN_STARTED", "RUN_SUCCEEDED")
	marker := fmt.Sprintf("input-marker-%d", time.Now().UnixNano())
	run := h.run(t, ctx, marker)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s ended %v (%v)", run.GetId(), run.GetStatus(), run.GetError())
	}

	received := h.subscriber.forRun(run.GetId())
	records := h.list(t, ctx, run.GetId())
	if len(received) != 2 || len(records) != 2 {
		t.Fatalf("subscriber received %d, store lists %d; want the 2 run transitions each", len(received), len(records))
	}
	type evidence struct {
		ID             string `json:"id"`
		Event          string `json:"event"`
		Key            string `json:"key"`
		ContentType    string `json:"contentType"`
		Signature      string `json:"signature"`
		HeaderSeen     string `json:"signatureHeaderSeen"`
		Verified       bool   `json:"signatureVerifiesUnderDisclosedSecret"`
		BodySHA256     string `json:"bodySha256"`
		ReceiverSHA256 string `json:"receiverSha256"`
		BodyBytes      int64  `json:"bodyBytes"`
		BodyInRecord   bool   `json:"inputValueFoundInRecord"`
		Attempts       int    `json:"attempts"`
	}
	var proofRows []evidence
	for _, rec := range records {
		seen := h.subscriber.forDelivery(rec.GetId())
		if len(seen) != 1 {
			t.Fatalf("record %s matches %d received requests, want 1", rec.GetId(), len(seen))
		}
		got := seen[0]
		header := got.headers.Get(webhooks.SignatureHeader)
		verified := webhooks.Verify(h.hook.Secret, got.body, rec.GetSignature())
		switch {
		case !rec.GetDelivered():
			t.Errorf("record %s is not delivered", rec.GetId())
		case rec.GetSignature() == "" || rec.GetSignature() != header:
			t.Errorf("record %s signature %q, receiver saw %q", rec.GetId(), rec.GetSignature(), header)
		case !verified:
			t.Errorf("record %s signature does not verify under the disclosed secret", rec.GetId())
		case rec.GetBodySha256() != sha256Hex(got.body) || rec.GetBodyBytes() != int64(len(got.body)):
			t.Errorf("record %s digest %s/%d, receiver read %s/%d", rec.GetId(),
				rec.GetBodySha256(), rec.GetBodyBytes(), sha256Hex(got.body), len(got.body))
		}
		if !strings.Contains(string(got.body), marker) {
			t.Fatalf("the delivery body does not embed the run input %q; the absence check below would prove nothing", marker)
		}
		key, contentType, stored := h.object(t, ctx, rec)
		if !strings.HasPrefix(key, "metadata/deliveries/"+rec.GetCreatedAt().AsTime().UTC().Format("2006/01/02")+"/"+rec.GetId()+"/") {
			t.Errorf("record %s stored at %s", rec.GetId(), key)
		}
		if contentType != artifacts.DeliveryContentType {
			t.Errorf("record %s content type %q", rec.GetId(), contentType)
		}
		inRecord := strings.Contains(string(stored), marker)
		if inRecord {
			t.Errorf("record %s carries the run's input value: the body was stored", rec.GetId())
		}
		t.Logf("EVIDENCE delivery_record id=%s event=%s signature=%s header=%s equal=%t verified=%t body_sha256=%s receiver_sha256=%s key=%s",
			rec.GetId(), rec.GetEvent(), rec.GetSignature(), header, rec.GetSignature() == header, verified,
			rec.GetBodySha256(), sha256Hex(got.body), key)
		proofRows = append(proofRows, evidence{
			ID: rec.GetId(), Event: rec.GetEvent().String(), Key: key, ContentType: contentType,
			Signature: rec.GetSignature(), HeaderSeen: header, Verified: verified,
			BodySHA256: rec.GetBodySha256(), ReceiverSHA256: sha256Hex(got.body), BodyBytes: rec.GetBodyBytes(),
			BodyInRecord: inRecord, Attempts: len(rec.GetAttempts()),
		})
	}
	listing, _ := protojson.Marshal(&orkstackv1.ListDeliveriesResponse{Deliveries: records})
	proof.WriteJSON(t, "engine", "webhook-delivery-records.json", map[string]any{
		"runId": run.GetId(), "webhookId": h.hook.ID, "records": proofRows, "listing": string(listing),
	})
}

func TestIntegration_FailedDeliveryRecordNamesEveryAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	h := startRecordsHarness(t, ctx, true, http.StatusBadGateway, "RUN_FAILED")
	run := h.run(t, ctx, "failing")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run %s ended %v, want FAILED", run.GetId(), run.GetStatus())
	}
	records := h.list(t, ctx, run.GetId())
	if len(records) != 1 {
		t.Fatalf("store lists %d records for the run, want the one RUN_FAILED", len(records))
	}
	rec := records[0]
	seen := h.subscriber.forDelivery(rec.GetId())
	if rec.GetDelivered() || len(rec.GetAttempts()) != 4 || len(seen) != 4 {
		t.Fatalf("record delivered=%t attempts=%d, receiver saw %d; want false, 4, 4",
			rec.GetDelivered(), len(rec.GetAttempts()), len(seen))
	}
	for i, attempt := range rec.GetAttempts() {
		if attempt.GetNumber() != int32(i+1) || attempt.GetStatusCode() != int32(seen[i].status) ||
			seen[i].headers.Get(webhooks.AttemptHeader) != fmt.Sprint(i+1) ||
			attempt.GetError() != "receiver answered 502" {
			t.Errorf("attempt %d = %v; receiver logged status %d with %s=%s", i+1, attempt, seen[i].status,
				webhooks.AttemptHeader, seen[i].headers.Get(webhooks.AttemptHeader))
		}
	}
	got, err := h.engine.webhookClient().GetDelivery(ctx, connect.NewRequest(&orkstackv1.GetDeliveryRequest{Id: rec.GetId()}))
	if err != nil || len(got.Msg.GetDelivery().GetAttempts()) != 4 {
		t.Fatalf("GetDelivery(%s) = %v, %v", rec.GetId(), got, err)
	}
	t.Logf("EVIDENCE failed_delivery_record id=%s delivered=false attempts=4 codes=[%d %d %d %d] receiver_requests=%d",
		rec.GetId(), rec.GetAttempts()[0].GetStatusCode(), rec.GetAttempts()[1].GetStatusCode(),
		rec.GetAttempts()[2].GetStatusCode(), rec.GetAttempts()[3].GetStatusCode(), len(seen))
}

func TestIntegration_RedeliverPostsOnceSignedAndRecordsANewDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	h := startRecordsHarness(t, ctx, true, http.StatusBadGateway, "RUN_FAILED")
	run := h.run(t, ctx, "redeliver")
	records := h.list(t, ctx, run.GetId())
	if len(records) != 1 || records[0].GetDelivered() {
		t.Fatalf("records = %v, want one failed RUN_FAILED", records)
	}
	original := records[0]
	_, _, before := h.object(t, ctx, original)
	requestsBefore := len(h.subscriber.forRun(run.GetId()))

	h.subscriber.answer(http.StatusOK)
	resp, err := h.engine.webhookClient().RedeliverWebhook(ctx,
		connect.NewRequest(&orkstackv1.RedeliverWebhookRequest{Id: original.GetId()}))
	if err != nil {
		t.Fatalf("RedeliverWebhook: %v", err)
	}
	rec := resp.Msg.GetDelivery()
	all := h.subscriber.forRun(run.GetId())
	if len(all) != requestsBefore+1 {
		t.Fatalf("receiver saw %d more requests, want exactly 1", len(all)-requestsBefore)
	}
	got := all[len(all)-1]
	verified := webhooks.Verify(h.hook.Secret, got.body, got.headers.Get(webhooks.SignatureHeader))
	if got.headers.Get(webhooks.RedeliveryHeader) != original.GetId() || got.headers.Get(webhooks.DeliveryHeader) != rec.GetId() ||
		got.headers.Get(webhooks.AttemptHeader) != "1" || !verified || !rec.GetDelivered() ||
		rec.GetRedeliveryOf() != original.GetId() || rec.GetBodySha256() != sha256Hex(got.body) {
		t.Fatalf("redelivery request headers %v, record %v, verified %t", got.headers, rec, verified)
	}
	listed := h.list(t, ctx, run.GetId())
	if len(listed) != 2 || listed[0].GetId() != rec.GetId() {
		t.Errorf("listing after redelivery = %v, want the new record first", listed)
	}
	_, _, after := h.object(t, ctx, original)
	if string(before) != string(after) {
		t.Error("the original record changed after the redelivery")
	}
	t.Logf("EVIDENCE redelivery original=%s new=%s receiver_additional_requests=1 %s=%s %s=%s signature_verified=%t delivered=%t status=%d original_unchanged=%t",
		original.GetId(), rec.GetId(), webhooks.RedeliveryHeader, got.headers.Get(webhooks.RedeliveryHeader),
		webhooks.AttemptHeader, got.headers.Get(webhooks.AttemptHeader), verified, rec.GetDelivered(),
		rec.GetAttempts()[0].GetStatusCode(), string(before) == string(after))
}
