package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
)

// TestProofSamplesForM72 writes the before/after JSON artifacts for proof/M7.2.
// It is skipped unless ORK_PROOF_DIR names a directory:
//
//	ORK_PROOF_DIR=$PWD/proof/M7.2 go test ./internal/api/ -run Proof -count=1
//
// "before" is the pre-M7.2 encoder verbatim — protojson.Marshal plus
// json.Indent for the CLI, and protojson.MarshalOptions{}.Marshal for the
// WebSocket bridge. "after" is what those two surfaces emit today.
func TestProofSamplesForM72(t *testing.T) {
	dir := os.Getenv("ORK_PROOF_DIR")
	if dir == "" {
		t.Skip("set ORK_PROOF_DIR to write proof samples")
	}

	run := proofRun(t)

	before, err := indent(mustMarshal(t, protojson.Marshal, run))
	if err != nil {
		t.Fatalf("indent before: %v", err)
	}
	after, err := indent(mustMarshal(t, api.MarshalJSON, run))
	if err != nil {
		t.Fatalf("indent after: %v", err)
	}
	writeProof(t, filepath.Join(dir, "run-get.before.json"), before)
	writeProof(t, filepath.Join(dir, "run-get.after.json"), after)

	if !bytes.Contains(before, []byte(`"status": "STATUS_SUCCEEDED"`)) {
		t.Errorf("before sample does not show the old prefixed status:\n%s", before)
	}
	if !bytes.Contains(after, []byte(`"status": "SUCCEEDED"`)) {
		t.Errorf("after sample lacks the clean status:\n%s", after)
	}
	if bytes.Contains(after, []byte(`"status": "STATUS_SUCCEEDED"`)) {
		t.Errorf("after sample still writes a prefixed status:\n%s", after)
	}
	// The user payload's look-alike string is untouched in both samples.
	if !bytes.Contains(after, []byte(`"upstream_status": "STATUS_SUCCEEDED"`)) {
		t.Errorf("after sample rewrote opaque user data:\n%s", after)
	}

	writeProof(t, filepath.Join(dir, "ws-frames.before.jsonl"), proofFrames(t, false))
	writeProof(t, filepath.Join(dir, "ws-frames.after.jsonl"), proofFrames(t, true))
}

// proofRun is a finished run with the shapes that matter: nested task runs, a
// structured error, and a user payload holding a string that looks like an
// enum value.
func proofRun(t *testing.T) *orkstackv1.Run {
	t.Helper()
	at := timestamppb.New(time.Date(2026, 8, 15, 23, 36, 40, 0, time.UTC))
	input, err := structpb.NewStruct(map[string]any{"url": "http://127.0.0.1:8081/json"})
	if err != nil {
		t.Fatalf("build struct: %v", err)
	}
	output, err := structpb.NewStruct(map[string]any{
		"saved": "result.txt",
		// Opaque user data: it must survive verbatim, prefix and all.
		"upstream_status": "STATUS_SUCCEEDED",
	})
	if err != nil {
		t.Fatalf("build struct: %v", err)
	}
	return &orkstackv1.Run{
		Id:             "ork-1786837000491917000-1",
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
		Input:          &orkstackv1.Payload{Data: input},
		Output:         &orkstackv1.Payload{Data: output},
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "fetch", Plugin: "http", Status: orkstackv1.Status_STATUS_SUCCEEDED,
				Attempt: 1, StartedAt: at, FinishedAt: at},
			{TaskId: "transform", Plugin: "shell", Status: orkstackv1.Status_STATUS_SUCCEEDED,
				Attempt: 1, StartedAt: at, FinishedAt: at},
			{TaskId: "save", Plugin: "file", Status: orkstackv1.Status_STATUS_SUCCEEDED,
				Attempt: 1, StartedAt: at, FinishedAt: at},
		},
		CreatedAt:  at,
		StartedAt:  at,
		FinishedAt: at,
	}
}

// proofFrames dials the live bridge and returns its frames, either as they go
// out today (clean) or re-encoded with the pre-M7.2 marshaler.
func proofFrames(t *testing.T, clean bool) []byte {
	t.Helper()
	f := newFixture(t)
	f.seedRun(t, "run-proof")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx,
		"ws"+strings.TrimPrefix(f.baseURL, "http")+"/ws/runs/run-proof", nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.CloseNow()

	var out bytes.Buffer
	for range 4 {
		_, frame, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("websocket read: %v", err)
		}
		if !clean {
			event := &orkstackv1.Event{}
			if err := api.UnmarshalJSON(frame, event); err != nil {
				t.Fatalf("decode frame: %v", err)
			}
			frame, err = protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(event)
			if err != nil {
				t.Fatalf("re-encode frame: %v", err)
			}
		}
		out.Write(frame)
		out.WriteByte('\n')
	}
	return out.Bytes()
}

func mustMarshal(t *testing.T, marshal func(proto.Message) ([]byte, error), message proto.Message) []byte {
	t.Helper()
	encoded, err := marshal(message)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return encoded
}

func indent(compact []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Indent(&buf, compact, "", "  "); err != nil {
		return nil, err
	}
	return append(buf.Bytes(), '\n'), nil
}

func writeProof(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote %s (%d bytes)", path, len(content))
}
