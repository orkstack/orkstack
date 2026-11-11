package engine_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"os"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// webhookDeliveryFixture is WebhookDeliveryWorkflow's recorded history: a
// real delivery a receiver answered 503 and then 200, so it holds one
// retried send and one settle. TestIntegration_RecordWebhookDeliveryHistory
// records it.
const webhookDeliveryFixture = "testdata/history/webhook-delivery/webhook-delivery-v1.json"

// requireWebhookDeliveryFixture skips, naming the recorder, when the fixture
// is absent, and fails instead under ORK_REQUIRE_WEBHOOK_DELIVERY_FIXTURE.
func requireWebhookDeliveryFixture(t *testing.T) *historypb.History {
	t.Helper()
	if _, err := os.Stat(webhookDeliveryFixture); err != nil {
		message := "no recorded delivery history at " + webhookDeliveryFixture + " (" + err.Error() + "); record it with " +
			"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ -run TestIntegration_RecordWebhookDeliveryHistory"
		if os.Getenv("ORK_REQUIRE_WEBHOOK_DELIVERY_FIXTURE") != "" {
			t.Fatal(message)
		}
		t.Skip(message)
	}
	return loadHistoryFile(t, webhookDeliveryFixture)
}

// Command half: the recorded delivery replays against today's workflow.
func TestWebhookDeliveryWorkflow_ReplaysCommittedHistory(t *testing.T) {
	recorded := requireWebhookDeliveryFixture(t)
	if err := engine.ReplayWebhookDeliveryHistory(recorded); err != nil {
		t.Fatalf("the committed delivery history no longer replays: %v", err)
	}
}

// Negative: a workflow that settles without sending first is a different
// command stream, and the fixture catches it.
func TestWebhookDeliveryHistory_CatchesASettleWithoutItsSend(t *testing.T) {
	recorded := requireWebhookDeliveryFixture(t)
	reordered := func(ctx workflow.Context, in engine.WebhookDeliveryInput) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: engine.EventQueue, StartToCloseTimeout: in.Policy.SettleTimeout})
		return workflow.ExecuteActivity(ctx, engine.ActivitySettleWebhookDelivery, in, webhooks.Result{}).Get(ctx, nil)
	}
	if err := engine.ReplayWebhookDeliveryHistoryWith(recorded, reordered); err == nil {
		t.Fatal("a workflow that skips the send replayed the recorded delivery cleanly")
	}
}

// Payload half: the replayer never looks inside a payload, so today's
// workflow is re-executed from the fixture's own input with the recorded
// results fed back, and what it passes to each activity must be what the
// recorded build passed.
func TestWebhookDeliveryHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	recorded := requireWebhookDeliveryFixture(t)
	dc := converter.GetDefaultDataConverter()
	var (
		input     engine.WebhookDeliveryInput
		scheduled = map[string][][]byte{}
		results   = map[string][]byte{}
		typeOf    = map[int64]string{}
	)
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
			if err := dc.FromPayloads(attrs.GetInput(), &input); err != nil {
				t.Fatalf("decode the recorded input: %v", err)
			}
		}
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			name := attrs.GetActivityType().GetName()
			typeOf[event.GetEventId()] = name
			for _, payload := range attrs.GetInput().GetPayloads() {
				scheduled[name] = append(scheduled[name], payload.GetData())
			}
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			if payloads := attrs.GetResult().GetPayloads(); len(payloads) > 0 {
				results[typeOf[attrs.GetScheduledEventId()]] = payloads[0].GetData()
			}
		}
	}
	if input.Delivery.ID == "" || len(scheduled[engine.ActivitySendWebhookAttempt]) == 0 || len(scheduled[engine.ActivitySettleWebhookDelivery]) == 0 {
		t.Fatalf("the fixture holds input %+v and activities %v, want a send and a settle", input, scheduled)
	}
	var sent webhooks.Result
	if err := json.Unmarshal(results[engine.ActivitySendWebhookAttempt], &sent, stablejson.DurationAsNano); err != nil {
		t.Fatalf("decode the recorded send result: %v", err)
	}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(engine.WebhookDeliveryWorkflow, workflow.RegisterOptions{Name: engine.WebhookDeliveryWorkflowType})
	sends := map[string][]any{}
	env.RegisterActivityWithOptions(func(_ context.Context, in engine.WebhookDeliveryInput) (webhooks.Result, error) {
		sends[engine.ActivitySendWebhookAttempt] = []any{in}
		return sent, nil
	}, activity.RegisterOptions{Name: engine.ActivitySendWebhookAttempt})
	env.RegisterActivityWithOptions(func(_ context.Context, in engine.WebhookDeliveryInput, result webhooks.Result) error {
		sends[engine.ActivitySettleWebhookDelivery] = []any{in, result}
		return nil
	}, activity.RegisterOptions{Name: engine.ActivitySettleWebhookDelivery})
	env.ExecuteWorkflow(engine.WebhookDeliveryWorkflowType, input)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("re-executing the recorded delivery failed: %v", err)
	}
	for name, recordedArgs := range scheduled {
		got := sends[name]
		if len(got) != len(recordedArgs) {
			t.Fatalf("%s got %d arguments, the recorded build passed %d", name, len(got), len(recordedArgs))
		}
		for i, arg := range got {
			payload, err := dc.ToPayload(arg)
			if err != nil {
				t.Fatal(err)
			}
			if !jsonEqual(t, payload.GetData(), recordedArgs[i]) {
				t.Errorf("%s argument %d = %s\nthe recorded build passed %s", name, i, payload.GetData(), recordedArgs[i])
			}
		}
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("decode %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	ex, _ := stablejson.Marshal(x)
	ey, _ := stablejson.Marshal(y)
	return bytes.Equal(ex, ey)
}
