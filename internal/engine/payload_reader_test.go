package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
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

// gatedRunClient holds each describe until released, or answers with err.
type gatedRunClient struct {
	keyRunClient
	mu      sync.Mutex
	asked   int
	entered chan struct{}
	release chan struct{}
	err     error
}

func (c *gatedRunClient) DescribeWorkflowExecution(ctx context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.mu.Lock()
	c.asked++
	err := c.err
	c.mu.Unlock()
	if c.entered != nil {
		c.entered <- struct{}{}
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.keyRunClient.DescribeWorkflowExecution(ctx, id, run)
}

func (c *gatedRunClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.asked
}

// Asks for one run's key at once are one ask of the cluster, and one whose
// caller went away is still the others'.
func TestRunKeys_AsksAtOnceAreOne(t *testing.T) { synctest.Test(t, asksAtOnceAreOne) }

func asksAtOnceAreOne(t *testing.T) {
	keys, own, memo, args := readerFixture(t)
	start := args(own, "ork-read", &orkstackv1.Orkflow{Name: "flow", Namespace: "default"}, &orkstackv1.Payload{}, RunPlan{})
	c := &gatedRunClient{keyRunClient: keyRunClient{memo: memo, start: start}, entered: make(chan struct{}, 8), release: make(chan struct{})}
	rk := NewRunKeys(c, keys)
	first, leave := context.WithCancel(context.Background())
	gone := make(chan error, 1)
	go func() {
		_, err := rk.KeyFor(first, payloadcrypt.RunContext("ork-read"))
		gone <- err
	}()
	<-c.entered
	const others = 4
	got := make(chan error, others)
	for range others {
		go func() {
			k, err := rk.KeyFor(context.Background(), payloadcrypt.RunContext("ork-read"))
			if err == nil && k == nil {
				err = errors.New("no key")
			}
			got <- err
		}()
	}
	synctest.Wait() // every caller is waiting on the one ask
	leave()
	if err := <-gone; !errors.Is(err, context.Canceled) {
		t.Fatalf("the caller that left was told %v", err)
	}
	close(c.release)
	for range others {
		if err := <-got; err != nil {
			t.Fatalf("another caller was told %v", err)
		}
	}
	if c.count() != 1 {
		t.Fatalf("the cluster was asked %d times, want once", c.count())
	}
}

// A run that has no key to give, or whose key this process cannot open, is
// asked about once in a while; a cluster that did not answer, every time.
func TestRunKeys_KeepsAPermanentAnswerAWhile(t *testing.T) {
	keys, own, memo, args := readerFixture(t)
	good := args(own, "ork-read", &orkstackv1.Orkflow{Name: "flow", Namespace: "default"}, &orkstackv1.Payload{}, RunPlan{})
	k, err := payloadcrypt.NewKey(bytes.Repeat([]byte{8}, payloadcrypt.KeySize))
	must.OK(t, err)
	others, err := payloadcrypt.NewEngineKeys(k)
	must.OK(t, err)
	for name, tc := range map[string]struct {
		client *gatedRunClient
		keys   *payloadcrypt.EngineKeys
		want   error
		kept   bool
	}{
		"a keyless run":                      {&gatedRunClient{keyRunClient: keyRunClient{memo: memo, start: &commonpb.Payloads{}}}, keys, payloadcrypt.ErrKeyless, true},
		"an engines' key this process lacks": {&gatedRunClient{keyRunClient: keyRunClient{memo: memo, start: good}}, others, payloadcrypt.ErrEngineKeyMissing, true},
		"the cluster did not answer":         {&gatedRunClient{err: errors.New("down")}, keys, nil, false},
		"a run the cluster does not know":    {&gatedRunClient{err: serviceerror.NewNotFound("gone")}, keys, nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rk := NewRunKeys(tc.client, tc.keys)
				ask := func() error {
					_, err := rk.KeyFor(context.Background(), payloadcrypt.RunContext("ork-read"))
					if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
						t.Fatalf("KeyFor = %v, want %v", err, tc.want)
					}
					return err
				}
				first := ask().Error()
				if again := ask().Error(); again != first {
					t.Fatalf("asked again it says %q, not %q", again, first)
				}
				want := 2
				if tc.kept {
					want = 1
				}
				if tc.client.count() != want {
					t.Fatalf("the cluster was asked %d times, want %d", tc.client.count(), want)
				}
				time.Sleep(runKeyRefusalKept) // the bubble's clock
				ask()
				if tc.client.count() != want+1 {
					t.Fatalf("after a while the cluster was asked %d times, want %d", tc.client.count(), want+1)
				}
			})
		})
	}
}

// What is kept is bounded, and a kept refusal never outlives its key.
func TestRunKeys_KeepsNoMoreThanItsBound(t *testing.T) {
	keys, _, memo, _ := readerFixture(t)
	rk := NewRunKeys(&anyRunClient{keyRunClient{memo: memo, start: &commonpb.Payloads{}}}, keys)
	for i := range runKeysCacheSize + 10 {
		if _, err := rk.KeyFor(context.Background(), payloadcrypt.RunContext(fmt.Sprintf("ork-%d", i))); err == nil {
			t.Fatal("a run with no start gave a key")
		}
	}
	rk.mu.Lock()
	defer rk.mu.Unlock()
	if len(rk.refused) != runKeysCacheSize {
		t.Fatalf("%d refusals are kept, want %d: one past the bound drops one", len(rk.refused), runKeysCacheSize)
	}
}

// anyRunClient describes whichever run it is asked for.
type anyRunClient struct{ keyRunClient }

func (c *anyRunClient) DescribeWorkflowExecution(ctx context.Context, id, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	d, err := c.keyRunClient.DescribeWorkflowExecution(ctx, id, run)
	d.GetWorkflowExecutionInfo().Execution.WorkflowId = id
	return d, err
}
