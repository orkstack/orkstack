package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// keyRunClient is a cluster holding one execution: its memo and its first
// event's start arguments.
type keyRunClient struct {
	memo      *commonpb.Memo
	start     *commonpb.Payloads
	continued string
	describes int
}

func (c *keyRunClient) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.describes++
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "ork-read", RunId: "r1"}, Memo: c.memo,
	}}, nil
}

func (c *keyRunClient) GetWorkflowHistory(context.Context, string, string, bool, enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	return &oneEvent{event: &historypb.HistoryEvent{EventId: 1, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: c.start, ContinuedExecutionRunId: c.continued},
	}}}
}

type oneEvent struct {
	event *historypb.HistoryEvent
	done  bool
}

func (o *oneEvent) HasNext() bool { return !o.done }
func (o *oneEvent) Next() (*historypb.HistoryEvent, error) {
	o.done = true
	return o.event, nil
}

func readerFixture(t *testing.T) (*payloadcrypt.EngineKeys, *payloadcrypt.Scope, *commonpb.Memo, func(*payloadcrypt.Scope, ...any) *commonpb.Payloads) {
	t.Helper()
	ek, _ := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	dk, _ := payloadcrypt.NewKey(bytes.Repeat([]byte{9}, payloadcrypt.KeySize))
	keys, _ := payloadcrypt.NewEngineKeys(ek)
	tree, km, err := payloadcrypt.NewRunTree(context.Background(), payloadcrypt.NewStaticDataKeys("pk", dk), ek, "default", "ns_d", "ork-read")
	if err != nil {
		t.Fatal(err)
	}
	dc := NewDataConverter(nil)
	field := func(v any) *commonpb.Payload {
		p, err := dc.ToPayload(v)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{
		payloadcrypt.MemoField: field(km), MemoNamespace: field("default"), MemoOrkflowName: field("flow"),
	}}
	args := func(s *payloadcrypt.Scope, vs ...any) *commonpb.Payloads {
		out := &commonpb.Payloads{}
		for _, v := range vs {
			out.Payloads = append(out.Payloads, field(payloadcrypt.Seal(s, v)))
		}
		return out
	}
	return keys, payloadcrypt.NewScope(tree, payloadcrypt.RunContext("ork-read")), memo, args
}

func TestRunKeys_OpensARunsKeyFromItsMemoAndStart(t *testing.T) {
	keys, own, memo, args := readerFixture(t)
	wf := &orkstackv1.Orkflow{Name: "flow", Namespace: "default"}
	good := args(own, "ork-read", wf, &orkstackv1.Payload{}, RunPlan{})
	c := &keyRunClient{memo: memo, start: good}
	rk := NewRunKeys(c, keys)
	k, err := rk.KeyFor(context.Background(), payloadcrypt.RunContext("ork-read"))
	if err != nil || k == nil {
		t.Fatalf("KeyFor = %v, %v", k, err)
	}
	if _, err := rk.KeyFor(context.Background(), payloadcrypt.RunContext("ork-read")); err != nil || c.describes != 1 {
		t.Fatalf("a second ask described the run again (%d describes), %v", c.describes, err)
	}

	sibling := own.For(payloadcrypt.RunContext("ork-sibling"))
	for name, tc := range map[string]*keyRunClient{
		"a start sealed for another run":      {memo: memo, start: args(sibling, "ork-read", wf, &orkstackv1.Payload{}, RunPlan{})},
		"a plaintext start":                   {memo: memo, start: args(payloadcrypt.Unsealed(), "ork-read", wf, &orkstackv1.Payload{}, RunPlan{})},
		"a start that lacks its plan":         {memo: memo, start: args(own, "ork-read", wf, &orkstackv1.Payload{})},
		"a start naming another Orkflow":      {memo: memo, start: args(own, "ork-read", &orkstackv1.Orkflow{Name: "other", Namespace: "default"}, &orkstackv1.Payload{}, RunPlan{})},
		"a continued execution with no carry": {memo: memo, start: good, continued: "r0"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewRunKeys(tc, keys).KeyFor(context.Background(), payloadcrypt.RunContext("ork-read")); !errors.Is(err, payloadcrypt.ErrKeyless) {
				t.Fatalf("KeyFor = %v, want ErrKeyless", err)
			}
		})
	}
	t.Run("a run that is not sealed has no key", func(t *testing.T) {
		plain := &commonpb.Memo{Fields: map[string]*commonpb.Payload{MemoNamespace: memo.Fields[MemoNamespace]}}
		if _, err := NewRunKeys(&keyRunClient{memo: plain, start: good}, keys).KeyFor(context.Background(), payloadcrypt.RunContext("ork-read")); err == nil {
			t.Fatal("a run with no key field gave a key")
		}
	})
	t.Run("a schedule's context is not a run's", func(t *testing.T) {
		if _, err := rk.KeyFor(context.Background(), payloadcrypt.ScheduleContext("s")); err == nil {
			t.Fatal("a schedule's context gave a run's key")
		}
	})
}
