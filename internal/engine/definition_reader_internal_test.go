package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// byReferenceStart is a run's start event when its definition travels by
// reference — the stub as payload 1 and the plan naming the object — the
// object's bytes, and a counting store holding them under runID.
func byReferenceStart(t *testing.T, runID string) (*historypb.HistoryEvent, *orkstackv1.Orkflow, []byte, *countingSnapshotStore) {
	t.Helper()
	planned := largeDefinition(t, 40, 2000)
	data, ref, err := EncodeDefinitionObject(planned)
	if err != nil {
		t.Fatal(err)
	}
	plan := awaitPlanForTest()
	plan.Definition = &ref
	started := &historypb.HistoryEvent{
		EventId: 1, EventTime: timestamppb.New(readerEpoch), EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: RunWorkflowType},
				Input:        payloadsOf(t, runID, DefinitionStub(planned), &orkstackv1.Payload{}, plan),
			},
		},
	}
	store := &countingSnapshotStore{objects: map[string][]byte{}}
	uri, err := store.ContinuationURI(planned.GetNamespace(), runID, 0, artifacts.ContinuationKindDefinition, ref.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	store.objects[uri] = data
	return started, planned, data, store
}

// TestRunStart_ReadsOneEventAndNoObject: for a run whose definition travels
// by reference RunStart is what it always was — one history event, no
// object read — and returns the stub, marked ByReference, so the stream's
// tenancy check, both WebSocket upgrades and the source-run check stay one
// event whatever the definition's size, and answer even when the object is
// missing or corrupt.
func TestRunStart_ReadsOneEventAndNoObject(t *testing.T) {
	const runID = "ork-1700000000-91"
	started, planned, _, store := byReferenceStart(t, runID)
	_, fake := readerOver([]*historypb.HistoryEvent{started, {EventId: 2}, {EventId: 3}}, runID)
	counting := &eventCountingClient{fakeReadClient: fake}
	reader := newRunReader(counting, converter.GetDefaultDataConverter())
	reader.WithSnapshotStore(store)
	record, err := reader.RunStart(context.Background(), runID)
	if err != nil {
		t.Fatalf("RunStart: %v", err)
	}
	if !record.ByReference || len(record.Orkflow.GetTasks()) != 0 || !proto.Equal(record.Orkflow, DefinitionStub(planned)) {
		t.Fatalf("RunStart = %v (by reference %t), want the stub, by reference", record.Orkflow, record.ByReference)
	}
	if read := counting.events; read != 1 {
		t.Fatalf("RunStart read %d history events, want 1", read)
	}
	if store.reads != 0 {
		t.Fatalf("RunStart read %d objects, want none", store.reads)
	}
}

// TestRunDefinition_ResolvesTheReference: RunDefinition derives the object's
// key from the run id it was asked about, reads it once, verifies its whole
// SHA-256 and the stub's identity, and returns the definition; an inline
// run's definition comes back without a read; a missing or altered object,
// or one named under another run's id, is an error naming the run — never
// the object.
func TestRunDefinition_ResolvesTheReference(t *testing.T) {
	const runID = "ork-1700000000-92"
	started, planned, data, store := byReferenceStart(t, runID)
	reader, _ := readerOver([]*historypb.HistoryEvent{started}, runID)
	reader.WithSnapshotStore(store)
	ctx := context.Background()
	record, err := reader.RunStart(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	wf, err := reader.RunDefinition(ctx, runID, record)
	if err != nil || !proto.Equal(wf, planned) {
		t.Fatalf("RunDefinition = %v; want the planned definition", err)
	}
	if store.reads != 1 {
		t.Fatalf("RunDefinition read %d objects, want 1", store.reads)
	}

	t.Run("an inline definition is not read", func(t *testing.T) {
		inline := &RunStartRecord{Orkflow: planned}
		before := store.reads
		if got, err := reader.RunDefinition(ctx, runID, inline); err != nil || got != planned || store.reads != before {
			t.Fatalf("RunDefinition(inline) = %v (%d reads); want the record's own, no read", err, store.reads-before)
		}
	})
	t.Run("another run's id reads another key", func(t *testing.T) {
		if _, err := reader.RunDefinition(ctx, "ork-1700000000-93", record); err == nil ||
			!strings.Contains(err.Error(), "ork-1700000000-93") || !errors.Is(err, artifacts.ErrNotFound) {
			t.Fatalf("RunDefinition under another id = %v, want NotFound naming that run", err)
		}
	})
	for name, stored := range map[string][]byte{"missing": nil, "altered": func() []byte {
		altered := bytes.Clone(data)
		altered[len(altered)/3] ^= 0x01
		return altered
	}()} {
		t.Run("a "+name+" object", func(t *testing.T) {
			bad := &countingSnapshotStore{objects: map[string][]byte{}}
			if stored != nil {
				for uri := range store.objects {
					bad.objects[uri] = stored
				}
			}
			badReader, _ := readerOver([]*historypb.HistoryEvent{started}, runID)
			badReader.WithSnapshotStore(bad)
			_, err := badReader.RunDefinition(ctx, runID, record)
			if err == nil || !strings.Contains(err.Error(), runID) || strings.Contains(err.Error(), "definition-") {
				t.Fatalf("RunDefinition = %v, want an error naming the run and not the object", err)
			}
			if name == "altered" && !errors.Is(err, ErrContinuationObjectInvalid) {
				t.Fatalf("an altered object = %v, want a verification failure", err)
			}
		})
	}
}

// TestClosedExecution_MapsTheDefinitionCodes: a run that ended
// RUN_DEFINITION_* without its terminal snapshot (object storage down) still
// reads with its own code and category, from the execution's close.
func TestClosedExecution_MapsTheDefinitionCodes(t *testing.T) {
	for code, category := range map[string]orkstackv1.FailureCategory{
		RunDefinitionUnavailableCode: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		RunDefinitionCorruptCode:     orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
	} {
		event := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
				WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					Failure: &failurepb.Failure{Message: "its definition could not be used",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: code}}}}}}
		status, failure, closed := closedExecution(event, false)
		if !closed || status != orkstackv1.Status_STATUS_FAILED || failure.GetCode() != code || failure.GetCategory() != category {
			t.Fatalf("%s = %v %v, want FAILED %s %v", code, status, failure, code, category)
		}
	}
}

// eventCountingClient counts the history events a reader takes.
type eventCountingClient struct {
	*fakeReadClient
	events int
}

func (c *eventCountingClient) GetWorkflowHistory(ctx context.Context, workflowID, execution string, isLongPoll bool,
	filter enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	return &countingIterator{HistoryEventIterator: c.fakeReadClient.GetWorkflowHistory(ctx, workflowID, execution, isLongPoll, filter), c: c}
}

type countingIterator struct {
	client.HistoryEventIterator
	c *eventCountingClient
}

func (i *countingIterator) Next() (*historypb.HistoryEvent, error) {
	i.c.events++
	return i.HistoryEventIterator.Next()
}
