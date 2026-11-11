package engine

import (
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
)

// TestTemporalPayloads_AreTheBytesRecordedBefore pins the payload bytes the
// Temporal SDK's own JSON converter writes for the plain-Go workflow and
// activity arguments this package defines, recorded before the module moved
// to encoding/json/v2. The SDK still encodes them with encoding/json, so a
// field type whose v1 encoding changed — a jsontext.Value where a raw value
// was, a time.Duration v1 cannot read back — would make every recorded
// history decode differently. The replay fixtures prove the decode; this
// proves the encode.
func TestTemporalPayloads_AreTheBytesRecordedBefore(t *testing.T) {
	plan := RunPlan{
		Routes:                map[string]string{"build": "ork-pool-gpu", "a": "ork-tasks"},
		CommitRoot:            "s3://bucket/<root>&",
		TransferBudgetSeconds: 30,
		Awaits:                map[string]AwaitCredential{"approve": {Salt: "s", Digest: "d"}},
		Children: map[string]*ChildRunPlan{"child": {
			RunID:   "ork-child",
			Orkflow: []byte(`{"name":"c","tasks":[{"id":"t","with":{"q":"<a>"}}]}`),
		}},
	}
	for _, c := range []struct {
		name  string
		value any
		want  string
	}{
		{"RunPlan", plan, "{\"namespace\":\"\",\"routes\":{\"a\":\"ork-tasks\",\"build\":\"ork-pool-gpu\"},\"commit_root\":\"s3://bucket/\\u003croot\\u003e\\u0026\",\"limits\":{\"values_max_bytes\":0,\"artifact_max_count\":0,\"artifact_max_bytes\":0,\"log_max_bytes\":0,\"commit_max_bytes\":0},\"transfer_budget_seconds\":30,\"awaits\":{\"approve\":{\"salt\":\"s\",\"digest\":\"d\"}},\"children\":{\"child\":{\"run_id\":\"ork-child\",\"orkflow\":{\"name\":\"c\",\"tasks\":[{\"id\":\"t\",\"with\":{\"q\":\"\\u003ca\\u003e\"}}]},\"plan\":{\"namespace\":\"\",\"routes\":null,\"commit_root\":\"\",\"limits\":{\"values_max_bytes\":0,\"artifact_max_count\":0,\"artifact_max_bytes\":0,\"log_max_bytes\":0,\"commit_max_bytes\":0},\"transfer_budget_seconds\":0}}}}"},
		{"ComposeRenderRequest", ComposeRenderRequest{
			Label: "child input", CodePrefix: renderPrefixChildInput,
			Templates: []byte(`{"x":"{{ .inputs.q }}"}`), Inputs: []byte(`{"q":"<v>"}`),
			Upstream: []ComposeUpstream{{TaskID: "up", URI: "s3://b/k", SHA256: "ab"}}, MaxBytes: 1024,
		}, "{\"label\":\"child input\",\"code_prefix\":\"CHILD_INPUT\",\"templates\":{\"x\":\"{{ .inputs.q }}\"},\"inputs\":{\"q\":\"\\u003cv\\u003e\"},\"upstream\":[{\"task_id\":\"up\",\"uri\":\"s3://b/k\",\"sha256\":\"ab\"}],\"max_bytes\":1024}"},
		{"ComposeRenderRequest zero", ComposeRenderRequest{}, "{\"label\":\"\",\"code_prefix\":\"\",\"templates\":null,\"inputs\":null,\"upstream\":null,\"max_bytes\":0}"},
		{"ComposeStoreRequest", ComposeStoreRequest{Orkflow: "o", RunID: "r", TaskID: "t", Values: []byte(`{"v":1}`)}, "{\"orkflow\":\"o\",\"run_id\":\"r\",\"task_id\":\"t\",\"values\":{\"v\":1}}"},
		{"ClaimWorkflowInput", ClaimWorkflowInput{Key: "k", TTL: 90 * time.Second}, "{\"key\":\"k\",\"ttl\":90000000000}"},
	} {
		payload, err := converter.GetDefaultDataConverter().ToPayload(c.value)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := string(payload.GetData()); got != c.want {
			t.Errorf("%s payload moved.\n got: %q\nwant: %q", c.name, got, c.want)
		}
	}
}
