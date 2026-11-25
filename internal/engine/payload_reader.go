package engine

import (
	"context"
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// runKeyClient is what RunKeys asks the cluster: the run's execution (its
// memo) and that execution's first event (its start).
type runKeyClient interface {
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool,
		filterType enumspb.HistoryEventFilterType) client.HistoryEventIterator
}

// runKeysCacheSize bounds how many runs' keys an engine keeps at once; past
// it the cache starts again empty.
const runKeysCacheSize = 4096

// RunKeys is an engine's way to a run's key outside workflow code
// (payload-encryption.md §4.1 rule 5): one describe of the run for its memo,
// and one read of the described execution's first event, which must hold
// the run to rule 6's test, before the key is used for anything. It needs the
// cluster and the engines' keys, and no provider. A key is kept for the run
// it was read for: every execution of a run carries its tree's one key.
type RunKeys struct {
	client runKeyClient
	keys   *payloadcrypt.EngineKeys

	mu    sync.Mutex
	cache map[string]*payloadcrypt.Key // by run id
}

// NewRunKeys reads runs' keys through c with keys.
func NewRunKeys(c runKeyClient, keys *payloadcrypt.EngineKeys) *RunKeys {
	return &RunKeys{client: c, keys: keys, cache: map[string]*payloadcrypt.Key{}}
}

// KeyFor is the key of the run c names, for its current execution.
func (r *RunKeys) KeyFor(ctx context.Context, c payloadcrypt.Context) (*payloadcrypt.Key, error) {
	if c.Kind != payloadcrypt.KindRun {
		return nil, fmt.Errorf("payload key: a %s's context names no run", c.Kind)
	}
	// Every execution of a run carries the one key of its tree, so a key
	// read for the run is kept for it, by its id.
	cacheKey := c.ID
	r.mu.Lock()
	k, ok := r.cache[cacheKey]
	r.mu.Unlock()
	if ok {
		return k, nil
	}
	described, err := r.client.DescribeWorkflowExecution(ctx, c.ID, "")
	if err != nil {
		return nil, fmt.Errorf("payload key: describe run %s: %w", c.ID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	execution := info.GetExecution()
	if execution.GetWorkflowId() != c.ID {
		return nil, fmt.Errorf("payload key: run %s was described as another", c.ID)
	}
	st := payloadcrypt.RunKeyOf(info.GetMemo(), r.keys, c.ID, "")
	if st.Err() != nil {
		return nil, fmt.Errorf("payload key: run %s: %w", c.ID, st.Err())
	}
	if !st.Sealed() {
		return nil, fmt.Errorf("payload key: run %s is not encrypted", c.ID)
	}
	if err := r.checkStart(ctx, st, info.GetMemo(), execution); err != nil {
		return nil, fmt.Errorf("payload key: run %s: %w", c.ID, err)
	}
	k = st.Key()
	r.mu.Lock()
	if len(r.cache) >= runKeysCacheSize {
		r.cache = map[string]*payloadcrypt.Key{}
	}
	r.cache[cacheKey] = k
	r.mu.Unlock()
	return k, nil
}

// checkStart is rule 6's test of the described execution's own start: its
// run id, definition, input and plan (and, continuing another, its carry)
// are each sealed and open for the run's own id, and the start names the
// namespace and Orkflow its memo does. A run that fails it is keyless to
// every reader.
func (r *RunKeys) checkStart(ctx context.Context, st payloadcrypt.KeyState, memo *commonpb.Memo, execution *commonpb.WorkflowExecution) error {
	events := r.client.GetWorkflowHistory(ctx, execution.GetWorkflowId(), execution.GetRunId(), false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !events.HasNext() {
		return fmt.Errorf("%w: the execution has no start", payloadcrypt.ErrKeyless)
	}
	first, err := events.Next()
	if err != nil {
		return fmt.Errorf("read the run's start: %w", err)
	}
	started := first.GetWorkflowExecutionStartedEventAttributes()
	if started == nil {
		return fmt.Errorf("%w: the first event is not a start", payloadcrypt.ErrKeyless)
	}
	args := started.GetInput().GetPayloads()
	need := 4
	if started.GetContinuedExecutionRunId() != "" {
		need = 5
	}
	if len(args) < need {
		return fmt.Errorf("%w: the start lacks an argument", payloadcrypt.ErrKeyless)
	}
	scope := st.Scope()
	var runID string
	var wf *orkstackv1.Orkflow
	var input *orkstackv1.Payload
	var plan RunPlan
	var carry *orkstackv1.RunContinuation
	targets := []any{&runID, &wf, &input, &plan, &carry}
	for i := 0; i < need; i++ {
		if _, err := payloadcrypt.OpenRaw(scope, DataConverter(), converter.NewRawValue(args[i]), targets[i]); err != nil {
			return fmt.Errorf("%w: start argument %d: %v", payloadcrypt.ErrKeyless, i, err)
		}
	}
	field := func(name string) string {
		var v string
		if p, ok := memo.GetFields()[name]; ok {
			if DataConverter().FromPayload(p, &v) != nil {
				return "\x00unreadable"
			}
		}
		return v
	}
	own := namespaceOrDefault(wf.GetNamespace())
	if wf == nil || own != namespaceOrDefault(st.Namespace()) || own != namespaceOrDefault(field(MemoNamespace)) ||
		wf.GetName() != field(MemoOrkflowName) {
		return fmt.Errorf("%w: the start names another namespace or Orkflow than its memo", payloadcrypt.ErrKeyless)
	}
	return nil
}
