package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"golang.org/x/sync/singleflight"

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

// runKeyRefusalKept is how long a refusal that asking again soon would only
// repeat is kept: a run that has no key to give, or whose key this process,
// whose key files were read at boot, cannot open.
const runKeyRefusalKept = 30 * time.Second

// RunKeys is an engine's way to a run's key outside workflow code
// (payload-encryption.md §4.1 rule 5): one describe of the run for its memo,
// and one read of the described execution's first event, which must hold
// the run to rule 6's test, before the key is used for anything. It needs the
// cluster and the engines' keys, and no provider. A key is kept for the run
// it was read for: every execution of a run carries its tree's one key. Asks
// for one run at once are one ask of the cluster.
type RunKeys struct {
	client runKeyClient
	keys   *payloadcrypt.EngineKeys
	asks   singleflight.Group // by run id

	mu      sync.Mutex
	cache   map[string]RunKeyHeld // by run id
	refused map[string]runKeyRefused
}

// runKeyRefused is a refusal kept until a time.
type runKeyRefused struct {
	err   error
	until time.Time
}

// NewRunKeys reads runs' keys through c with keys.
func NewRunKeys(c runKeyClient, keys *payloadcrypt.EngineKeys) *RunKeys {
	return &RunKeys{client: c, keys: keys, cache: map[string]RunKeyHeld{}, refused: map[string]runKeyRefused{}}
}

// RunKeyHeld is a run's key, and the memo field it was opened from, verified.
type RunKeyHeld struct {
	Key   *payloadcrypt.Key
	Field payloadcrypt.KeyMemo
}

// KeyFor is the key of the run c names, for a data converter.
func (r *RunKeys) KeyFor(ctx context.Context, c payloadcrypt.Context) (*payloadcrypt.Key, error) {
	held, err := r.RunKeyFor(ctx, c)
	return held.Key, err
}

// RunKeyFor is the key of the run c names, for its current execution.
func (r *RunKeys) RunKeyFor(ctx context.Context, c payloadcrypt.Context) (RunKeyHeld, error) {
	if c.Kind != payloadcrypt.KindRun {
		return RunKeyHeld{}, fmt.Errorf("payload key: a %s's context names no run", c.Kind)
	}
	// Every execution of a run carries the one key of its tree, so a key
	// read for the run is kept for it, by its id.
	if held, err, kept := r.kept(c.ID); kept {
		return held, err
	}
	// The ask is nobody's own: a caller that leaves does not end the others'.
	asked := r.asks.DoChan(c.ID, func() (any, error) {
		// Kept by an ask that ended since this caller looked.
		if held, err, kept := r.kept(c.ID); kept {
			return held, err
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runKeyWait)
		defer cancel()
		held, err := r.ask(ctx, c)
		r.keep(c.ID, held, err)
		return held, err
	})
	select {
	case res := <-asked:
		held, _ := res.Val.(RunKeyHeld)
		return held, res.Err
	case <-ctx.Done():
		return RunKeyHeld{}, fmt.Errorf("payload key: run %s: %w", c.ID, ctx.Err())
	}
}

// kept is the answer kept for a run, if one is.
func (r *RunKeys) kept(id string) (RunKeyHeld, error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if held, ok := r.cache[id]; ok {
		return held, nil, true
	}
	if refused, ok := r.refused[id]; ok {
		if time.Now().Before(refused.until) {
			return RunKeyHeld{}, refused.err, true
		}
		delete(r.refused, id)
	}
	return RunKeyHeld{}, nil, false
}

// keep keeps an answer: a key, or a refusal that does not change by asking
// again at once. What the cluster did not answer is not kept.
func (r *RunKeys) keep(id string, held RunKeyHeld, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case err == nil:
		if len(r.cache) >= runKeysCacheSize {
			r.cache = map[string]RunKeyHeld{}
		}
		r.cache[id] = held
	case errors.Is(err, payloadcrypt.ErrKeyless), errors.Is(err, payloadcrypt.ErrUnreadable), errors.Is(err, payloadcrypt.ErrEngineKeyMissing):
		r.makeRoom()
		r.refused[id] = runKeyRefused{err: err, until: time.Now().Add(runKeyRefusalKept)}
	}
}

// makeRoom keeps the refusals under their bound: what has ended goes, or else
// one, whichever. Locked.
func (r *RunKeys) makeRoom() {
	if len(r.refused) < runKeysCacheSize {
		return
	}
	now := time.Now()
	maps.DeleteFunc(r.refused, func(_ string, refused runKeyRefused) bool { return !now.Before(refused.until) })
	for id := range r.refused {
		if len(r.refused) < runKeysCacheSize {
			return
		}
		delete(r.refused, id)
	}
}

// ask reads the key of the run c names from the cluster.
func (r *RunKeys) ask(ctx context.Context, c payloadcrypt.Context) (RunKeyHeld, error) {
	described, err := r.client.DescribeWorkflowExecution(ctx, c.ID, "")
	if err != nil {
		return RunKeyHeld{}, fmt.Errorf("payload key: describe run %s: %w", c.ID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	execution := info.GetExecution()
	if execution.GetWorkflowId() != c.ID {
		return RunKeyHeld{}, fmt.Errorf("payload key: run %s was described as another", c.ID)
	}
	st := payloadcrypt.RunKeyOf(info.GetMemo(), r.keys, c.ID, "")
	if st.Err() != nil {
		return RunKeyHeld{}, fmt.Errorf("payload key: run %s: %w", c.ID, st.Err())
	}
	if !st.Sealed() {
		return RunKeyHeld{}, fmt.Errorf("%w: run %s is not encrypted", payloadcrypt.ErrKeyless, c.ID)
	}
	if err := r.checkStart(ctx, st, info.GetMemo(), execution); err != nil {
		return RunKeyHeld{}, fmt.Errorf("payload key: run %s: %w", c.ID, err)
	}
	field, err := payloadcrypt.ParseKeyMemo(info.GetMemo().GetFields()[payloadcrypt.MemoField].GetData())
	if err != nil {
		return RunKeyHeld{}, fmt.Errorf("payload key: run %s: %w", c.ID, err)
	}
	return RunKeyHeld{Key: st.Key(), Field: field}, nil
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
	return startIsTheRuns(st.Scope(), st.Namespace(), memo, started)
}

// startIsTheRuns is rule 6's test of one execution's start, for every reader
// of it: each argument is sealed and opens for the run's own id, and the
// start names the namespace its key is bound to, and the namespace and
// Orkflow of its memo.
func startIsTheRuns(scope *payloadcrypt.Scope, keyNamespace string, memo *commonpb.Memo, started *historypb.WorkflowExecutionStartedEventAttributes) error {
	args := started.GetInput().GetPayloads()
	need := 4
	if started.GetContinuedExecutionRunId() != "" {
		need = 5
	}
	if len(args) != need {
		return fmt.Errorf("%w: the start has %d arguments, want %d", payloadcrypt.ErrKeyless, len(args), need)
	}
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
	if wf == nil || own != namespaceOrDefault(keyNamespace) || own != namespaceOrDefault(field(MemoNamespace)) ||
		wf.GetName() != field(MemoOrkflowName) {
		return fmt.Errorf("%w: the start names another namespace or Orkflow than its memo", payloadcrypt.ErrKeyless)
	}
	return nil
}
