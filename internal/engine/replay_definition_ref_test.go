package engine_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The definition-by-reference replay fixture (testdata/history/README.md): generation 0 of a
// run whose definition travels by reference, recorded on a real cluster
// with its gate's marker and its definition's chunk markers, and the
// definition object the recording read back from MinIO.
const (
	definitionRefHistory    = "testdata/history/run-definition-ref-v1.json"
	definitionRefObjectPath = "testdata/history/run-definition-ref-v1.objects/definition.pb.gz"
)

// TestRunWorkflow_ReplaysCommittedDefinitionRefHistory is the command half:
// today's body asks the definition-by-reference gate first, reads the
// definition in the recorded chunks and batches with their yield timers,
// and runs the definition the object holds, command for command.
func TestRunWorkflow_ReplaysCommittedDefinitionRefHistory(t *testing.T) {
	recorded := loadHistoryFile(t, definitionRefHistory)
	runID, _, _, plan, _ := startArgs(t, recorded)
	if plan.Definition == nil || len(recordedChunks(t, recorded)) < 3 {
		t.Fatal("the fixture is not a run with its definition by reference read in several chunks")
	}
	if err := replayAs(t, recorded, runID); err != nil {
		t.Fatalf("replaying %s against current code failed: %v", definitionRefHistory, err)
	}
}

// TestDefinitionRefHistory_PayloadsMatchWhatCurrentCodeSends is the payload
// half: the companion object is exactly the bytes the recorded chunks read
// and decodes, against the recorded plan and stub, into the definition the
// run executed; and the run, re-executed from its recorded arguments with
// those chunks fed back, sends every recorded ExecuteTask input and
// publishes every recorded event.
func TestDefinitionRefHistory_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	recorded := loadHistoryFile(t, definitionRefHistory)
	id, stub, input, plan, _ := startArgs(t, recorded)
	object, err := os.ReadFile(definitionRefObjectPath)
	if err != nil {
		t.Fatalf("the companion object %s: %v", filepath.Base(definitionRefObjectPath), err)
	}
	chunks := recordedChunks(t, recorded)
	if !bytes.Equal(bytes.Join(chunks, nil), object) || artifacts.ContinuationSHA256(object) != plan.Definition.SHA256 {
		t.Fatal("the recorded chunks are not the companion object, or it does not hash to the plan's reference")
	}
	loaded, err := engine.DecodeDefinitionObject(object, plan.Definition, stub)
	if err != nil {
		t.Fatalf("the companion object does not decode against the recorded plan and stub: %v", err)
	}
	if len(stub.GetTasks()) != 0 || !proto.Equal(engine.DefinitionStub(loaded), stub) {
		t.Fatalf("the recorded start carried %d tasks, want the stub of the definition", len(stub.GetTasks()))
	}
	inputs, results := recordedItemPayloads(t, recorded)
	if len(inputs) != len(loaded.GetTasks()) {
		t.Fatalf("the recording dispatched %d tasks, the definition declares %d", len(inputs), len(loaded.GetTasks()))
	}
	got := reexecute(t, id, startedAt(recorded), []any{id, stub, input, plan, (*orkstackv1.RunContinuation)(nil)},
		results, nil, nil, chunks, 0)
	if got.runError != nil {
		t.Fatalf("the re-executed run failed: %v", got.runError)
	}
	if got.writes != 0 {
		t.Fatal("the re-executed run took a continuation path its recording never took")
	}
	compareAll(t, inputs, got.inputs, publishedEvents(t, recorded, 0), got.events)
}

// TestDefinitionRefHistory_CatchesAChangedPinnedChunkSize: the fixture with
// its plan's chunk_bytes rewritten must fail to replay — the pinned value,
// not a code constant, decides how many chunk reads a generation issues.
func TestDefinitionRefHistory_CatchesAChangedPinnedChunkSize(t *testing.T) {
	recorded := loadHistoryFile(t, definitionRefHistory)
	runID, _, _, _, _ := startArgs(t, recorded)
	attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	plan := map[string]any{}
	if err := json.Unmarshal(attrs.GetInput().GetPayloads()[3].GetData(), &plan); err != nil {
		t.Fatal(err)
	}
	continuation := plan["continuation"].(map[string]any)
	pinned := continuation["chunk_bytes"].(float64)
	definition := plan["definition"].(map[string]any)
	if definition["size_bytes"].(float64) <= 2*pinned {
		t.Fatalf("the fixture's definition is %v bytes in %v-byte chunks: too few to see a change", definition["size_bytes"], pinned)
	}
	continuation["chunk_bytes"] = 2 * pinned
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	attrs.GetInput().GetPayloads()[3] = &commonpb.Payload{Metadata: attrs.GetInput().GetPayloads()[3].GetMetadata(), Data: data}
	if err := replayAs(t, recorded, runID); err == nil {
		t.Fatalf("the fixture replayed with %v-byte chunks: it cannot see its pinned chunk size", 2*pinned)
	}
}
