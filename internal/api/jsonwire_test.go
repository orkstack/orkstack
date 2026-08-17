package api_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
)

// ---------------------------------------------------------------------------
// WebSocket frames
// ---------------------------------------------------------------------------

// The realtime bridge is the JSON surface browsers read directly, so its wire
// bytes — not just the decoded message — must carry short enum names.
func TestWebSocketFramesCarryEnumValuesWithoutTheirPrefix(t *testing.T) {
	f := newFixture(t)
	f.seedRun(t, "run-ws-json")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(f.baseURL, "http") + "/ws/runs/run-ws-json"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.CloseNow()

	_, frame, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("websocket read: %v", err)
	}
	text := string(frame)

	if !strings.Contains(text, `"status":"RUNNING"`) {
		t.Errorf("frame lacks \"status\":\"RUNNING\":\n%s", text)
	}
	if !strings.Contains(text, `"type":"RUN_STARTED"`) {
		t.Errorf("frame lacks \"type\":\"RUN_STARTED\":\n%s", text)
	}
	if strings.Contains(text, "STATUS_") || strings.Contains(text, "EVENT_TYPE_") {
		t.Errorf("frame still carries an enum prefix:\n%s", text)
	}

	// The short names still decode back to the right enum members.
	event := &orkstackv1.Event{}
	if err := api.UnmarshalJSON(frame, event); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if event.GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Errorf("status = %v, want STATUS_RUNNING", event.GetStatus())
	}
	if event.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_STARTED {
		t.Errorf("type = %v, want EVENT_TYPE_RUN_STARTED", event.GetType())
	}
}

// ---------------------------------------------------------------------------
// Connect JSON input
// ---------------------------------------------------------------------------

// postJSON sends a raw Connect unary JSON request and returns status + body.
func (f *fixture) postJSON(t *testing.T, procedure, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, f.baseURL+procedure, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")

	response, err := f.client.Do(request)
	if err != nil {
		t.Fatalf("post %s: %v", procedure, err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return response.StatusCode, string(payload)
}

// Both spellings must parse on input: clients that were written against the
// old prefixed names keep working while new ones use the short form.
func TestConnectJSONInputAcceptsShortAndPrefixedEnumNames(t *testing.T) {
	f := newFixture(t)
	f.runs.Save(&orkstackv1.Run{Id: "run-a", OrkflowName: "hello-ork",
		Status: orkstackv1.Status_STATUS_RUNNING})
	f.runs.Save(&orkstackv1.Run{Id: "run-b", OrkflowName: "hello-ork",
		Status: orkstackv1.Status_STATUS_SUCCEEDED})

	cases := []struct {
		name string
		body string
	}{
		{"short form", `{"status":"RUNNING"}`},
		{"proto form", `{"status":"STATUS_RUNNING"}`},
		{"lower case", `{"status":"running"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := f.postJSON(t, orkstackv1connect.RunServiceListRunsProcedure, tc.body)
			if code != http.StatusOK {
				t.Fatalf("status = %d, want 200\nbody: %s", code, body)
			}
			listed := &orkstackv1.ListRunsResponse{}
			if err := api.UnmarshalJSON([]byte(body), listed); err != nil {
				t.Fatalf("decode response %q: %v", body, err)
			}
			if len(listed.GetRuns()) != 1 || listed.GetRuns()[0].GetId() != "run-a" {
				t.Fatalf("filtered runs = %v, want just run-a", listed.GetRuns())
			}
		})
	}
}

// Tolerating two spellings must not turn into tolerating anything: a name that
// is neither spelling must never be read as a real status. Connect's codec
// contract (DiscardUnknown, unchanged here) drops it, so the filter stays
// unset and every run comes back.
func TestConnectJSONInputNeverInventsAStatusFromAnUnknownName(t *testing.T) {
	f := newFixture(t)
	f.runs.Save(&orkstackv1.Run{Id: "run-a", Status: orkstackv1.Status_STATUS_RUNNING})
	f.runs.Save(&orkstackv1.Run{Id: "run-b", Status: orkstackv1.Status_STATUS_SUCCEEDED})

	code, body := f.postJSON(t, orkstackv1connect.RunServiceListRunsProcedure,
		`{"status":"BANANA"}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (unknown values are discarded)\nbody: %s", code, body)
	}
	listed := &orkstackv1.ListRunsResponse{}
	if err := api.UnmarshalJSON([]byte(body), listed); err != nil {
		t.Fatalf("decode response %q: %v", body, err)
	}
	if len(listed.GetRuns()) != 2 {
		t.Fatalf("runs = %v, want both (no filter applied)", listed.GetRuns())
	}
}

// Responses keep protojson's canonical names so generated browser clients
// (protobuf-es fromJson) can still decode them. This pins the deliberate
// asymmetry documented in internal/api/codec.go.
func TestConnectJSONResponsesStayCanonicalForGeneratedClients(t *testing.T) {
	f := newFixture(t)
	f.runs.Save(&orkstackv1.Run{Id: "run-a", Status: orkstackv1.Status_STATUS_RUNNING})

	code, body := f.postJSON(t, orkstackv1connect.RunServiceGetRunProcedure,
		`{"runId":"run-a"}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", code, body)
	}
	if !strings.Contains(body, `"status":"STATUS_RUNNING"`) {
		t.Errorf("response = %s, want the canonical STATUS_RUNNING", body)
	}
}
