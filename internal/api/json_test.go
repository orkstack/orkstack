package api_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
)

// ---------------------------------------------------------------------------
// MarshalJSON: enum values lose their type prefix
// ---------------------------------------------------------------------------

func TestMarshalJSON_WritesStatusWithoutItsEnumPrefix(t *testing.T) {
	run := &orkstackv1.Run{
		Id:           "ork-1786833695100960000-1",
		WorkflowName: "hello-ork",
		Status:       orkstackv1.Status_STATUS_SUCCEEDED,
	}

	encoded, err := api.MarshalJSON(run)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var decoded struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, encoded)
	}
	if decoded.Status != "SUCCEEDED" {
		t.Errorf("status = %q, want SUCCEEDED\n%s", decoded.Status, encoded)
	}
	if decoded.ID != run.GetId() {
		t.Errorf("id = %q, want %q", decoded.ID, run.GetId())
	}
	if strings.Contains(string(encoded), "STATUS_") {
		t.Errorf("output still carries an enum prefix:\n%s", encoded)
	}
}

func TestMarshalJSON_WritesEventTypeAndStatusWithoutPrefixes(t *testing.T) {
	event := &orkstackv1.Event{
		RunId:    "ork-1",
		Sequence: 7,
		Type:     orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		Status:   orkstackv1.Status_STATUS_RUNNING,
	}

	encoded, err := api.MarshalJSON(event)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var decoded struct {
		RunID    string `json:"runId"`
		Sequence string `json:"sequence"`
		Type     string `json:"type"`
		Status   string `json:"status"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, encoded)
	}
	if decoded.Type != "RUN_STARTED" {
		t.Errorf("type = %q, want RUN_STARTED\n%s", decoded.Type, encoded)
	}
	if decoded.Status != "RUNNING" {
		t.Errorf("status = %q, want RUNNING\n%s", decoded.Status, encoded)
	}
	// int64 must still travel as a JSON string, exactly as protojson writes it.
	if decoded.Sequence != "7" {
		t.Errorf("sequence = %q, want \"7\"\n%s", decoded.Sequence, encoded)
	}
}

func TestMarshalJSON_StripsPrefixesInNestedMessagesAndLists(t *testing.T) {
	run := &orkstackv1.Run{
		Id:     "ork-1",
		Status: orkstackv1.Status_STATUS_FAILED,
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "fetch", Status: orkstackv1.Status_STATUS_SUCCEEDED},
			{TaskId: "save", Status: orkstackv1.Status_STATUS_FAILED},
			{TaskId: "notify", Status: orkstackv1.Status_STATUS_SKIPPED},
		},
	}

	encoded, err := api.MarshalJSON(run)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var decoded struct {
		Status   string `json:"status"`
		TaskRuns []struct {
			TaskID string `json:"taskId"`
			Status string `json:"status"`
		} `json:"taskRuns"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, encoded)
	}
	want := []string{"SUCCEEDED", "FAILED", "SKIPPED"}
	if len(decoded.TaskRuns) != len(want) {
		t.Fatalf("task runs = %d, want %d\n%s", len(decoded.TaskRuns), len(want), encoded)
	}
	for i, status := range want {
		if decoded.TaskRuns[i].Status != status {
			t.Errorf("taskRuns[%d].status = %q, want %q", i, decoded.TaskRuns[i].Status, status)
		}
	}
	if decoded.Status != "FAILED" {
		t.Errorf("status = %q, want FAILED", decoded.Status)
	}
}

// A user payload is opaque data. A string that merely looks like an enum value
// must survive untouched — the rewrite is driven by the schema, not by text.
func TestMarshalJSON_LeavesUserPayloadStringsAlone(t *testing.T) {
	data, err := structpb.NewStruct(map[string]any{
		"status": "STATUS_SUCCEEDED",
		"type":   "EVENT_TYPE_RUN_FAILED",
		"note":   "a <b> & c",
	})
	if err != nil {
		t.Fatalf("build struct: %v", err)
	}
	run := &orkstackv1.Run{
		Id:     "ork-1",
		Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Output: &orkstackv1.Payload{Data: data},
	}

	encoded, err := api.MarshalJSON(run)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var decoded struct {
		Status string `json:"status"`
		Output struct {
			Data map[string]string `json:"data"`
		} `json:"output"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, encoded)
	}
	if decoded.Status != "SUCCEEDED" {
		t.Errorf("run status = %q, want SUCCEEDED", decoded.Status)
	}
	if decoded.Output.Data["status"] != "STATUS_SUCCEEDED" {
		t.Errorf("payload status = %q, want it preserved verbatim\n%s",
			decoded.Output.Data["status"], encoded)
	}
	if decoded.Output.Data["type"] != "EVENT_TYPE_RUN_FAILED" {
		t.Errorf("payload type = %q, want it preserved verbatim", decoded.Output.Data["type"])
	}
	if decoded.Output.Data["note"] != "a <b> & c" {
		t.Errorf("payload note = %q, want it preserved verbatim", decoded.Output.Data["note"])
	}
	// protojson leaves angle brackets unescaped; so must we.
	if !strings.Contains(string(encoded), "a <b> & c") {
		t.Errorf("output HTML-escaped a plain string:\n%s", encoded)
	}
}

// The only difference from protojson must be the enum values themselves:
// field order, numbers and nesting stay exactly as protojson wrote them.
func TestMarshalJSON_ChangesNothingButTheEnumValues(t *testing.T) {
	run := &orkstackv1.Run{
		Id:              "ork-1",
		WorkflowName:    "hello-ork",
		WorkflowVersion: "0.1.0",
		Status:          orkstackv1.Status_STATUS_SUCCEEDED,
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "fetch", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2},
		},
		Error: &orkstackv1.Error{Code: "NONE", Message: "ok"},
	}

	canonical, err := protojson.Marshal(run)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	clean, err := api.MarshalJSON(run)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	if got, want := jsonKeyOrder(t, clean), jsonKeyOrder(t, canonical); got != want {
		t.Errorf("field order changed:\n got %s\nwant %s", got, want)
	}
	restored := strings.ReplaceAll(compactJSON(t, clean), `"SUCCEEDED"`, `"STATUS_SUCCEEDED"`)
	if restored != compactJSON(t, canonical) {
		t.Errorf("output differs from protojson beyond enum values:\n got %s\nwant %s",
			restored, compactJSON(t, canonical))
	}
}

// ---------------------------------------------------------------------------
// UnmarshalJSON: both spellings parse
// ---------------------------------------------------------------------------

func TestUnmarshalJSON_AcceptsShortAndPrefixedEnumNames(t *testing.T) {
	cases := []struct {
		name string
		body string
		want orkstackv1.Status
	}{
		{"short form", `{"id":"ork-1","status":"SUCCEEDED"}`, orkstackv1.Status_STATUS_SUCCEEDED},
		{"proto form", `{"id":"ork-1","status":"STATUS_SUCCEEDED"}`, orkstackv1.Status_STATUS_SUCCEEDED},
		{"lower case", `{"id":"ork-1","status":"succeeded"}`, orkstackv1.Status_STATUS_SUCCEEDED},
		{"numeric", `{"id":"ork-1","status":3}`, orkstackv1.Status_STATUS_SUCCEEDED},
		{"unset", `{"id":"ork-1"}`, orkstackv1.Status_STATUS_UNSPECIFIED},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := &orkstackv1.Run{}
			if err := api.UnmarshalJSON([]byte(tc.body), run); err != nil {
				t.Fatalf("UnmarshalJSON(%s): %v", tc.body, err)
			}
			if run.GetStatus() != tc.want {
				t.Errorf("status = %v, want %v", run.GetStatus(), tc.want)
			}
			if run.GetId() != "ork-1" {
				t.Errorf("id = %q, want ork-1", run.GetId())
			}
		})
	}
}

func TestUnmarshalJSON_AcceptsShortEventTypes(t *testing.T) {
	event := &orkstackv1.Event{}
	body := `{"runId":"ork-1","sequence":"4","type":"RUN_FAILED","status":"FAILED"}`
	if err := api.UnmarshalJSON([]byte(body), event); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if event.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Errorf("type = %v, want EVENT_TYPE_RUN_FAILED", event.GetType())
	}
	if event.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Errorf("status = %v, want STATUS_FAILED", event.GetStatus())
	}
	if event.GetSequence() != 4 {
		t.Errorf("sequence = %d, want 4", event.GetSequence())
	}
}

func TestUnmarshalJSON_RejectsValuesThatAreNotEnumMembers(t *testing.T) {
	run := &orkstackv1.Run{}
	err := api.UnmarshalJSON([]byte(`{"id":"ork-1","status":"BANANA"}`), run)
	if err == nil {
		t.Fatal("UnmarshalJSON accepted a bogus status")
	}
	if !strings.Contains(err.Error(), "status") {
		t.Errorf("error = %v, want it to name the offending field", err)
	}
}

func TestUnmarshalJSON_ReportsMalformedJSON(t *testing.T) {
	run := &orkstackv1.Run{}
	if err := api.UnmarshalJSON([]byte(`{"id":`), run); err == nil {
		t.Fatal("UnmarshalJSON accepted truncated JSON")
	}
}

func TestMarshalJSON_RoundTripsThroughUnmarshalJSON(t *testing.T) {
	data, err := structpb.NewStruct(map[string]any{"url": "https://example.com", "retries": 3})
	if err != nil {
		t.Fatalf("build struct: %v", err)
	}
	run := &orkstackv1.Run{
		Id:           "ork-1",
		WorkflowName: "hello-ork",
		Status:       orkstackv1.Status_STATUS_CANCELED,
		Input:        &orkstackv1.Payload{Data: data},
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "fetch", Status: orkstackv1.Status_STATUS_SKIPPED, Attempt: 1},
		},
		Error: &orkstackv1.Error{Code: "CANCELED", Message: "user asked", Retryable: false},
	}

	encoded, err := api.MarshalJSON(run)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	decoded := &orkstackv1.Run{}
	if err := api.UnmarshalJSON(encoded, decoded); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if !proto.Equal(run, decoded) {
		t.Errorf("round trip lost data:\n got %v\nwant %v", decoded, run)
	}
}

// Map-valued fields carry enums too (none today), and map keys must not be
// touched. ArtifactRef.metadata is the map we ship.
func TestMarshalJSON_LeavesMapKeysAndValuesAlone(t *testing.T) {
	taskRun := &orkstackv1.TaskRun{
		TaskId: "fetch",
		Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Artifacts: []*orkstackv1.ArtifactRef{{
			Name:     "body",
			Uri:      "file:///tmp/body.json",
			Metadata: map[string]string{"status": "STATUS_SUCCEEDED"},
		}},
	}

	encoded, err := api.MarshalJSON(taskRun)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var decoded struct {
		Status    string `json:"status"`
		Artifacts []struct {
			Metadata map[string]string `json:"metadata"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, encoded)
	}
	if decoded.Status != "SUCCEEDED" {
		t.Errorf("status = %q, want SUCCEEDED", decoded.Status)
	}
	if got := decoded.Artifacts[0].Metadata["status"]; got != "STATUS_SUCCEEDED" {
		t.Errorf("artifact metadata = %q, want it preserved verbatim\n%s", got, encoded)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// jsonKeyOrder renders every object key, in document order, as a flat string.
func jsonKeyOrder(t *testing.T, encoded []byte) string {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	var keys []string
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return fmt.Errorf("object key %v is not a string", key)
				}
				keys = append(keys, name)
				if err := walk(); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
		}
		_, err = decoder.Token() // closing delimiter
		return err
	}
	if err := walk(); err != nil {
		t.Fatalf("scan json keys: %v", err)
	}
	return strings.Join(keys, ",")
}

func compactJSON(t *testing.T, encoded []byte) string {
	t.Helper()
	var value any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatalf("compact: %v", err)
	}
	out, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	return string(out)
}
