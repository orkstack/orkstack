//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// The server facts the continue-as-new design relies on (spec Stage 1, T1
// contract), proved against the real cluster with a probe workflow of its own
// type in a private namespace. Each item is logged, and the transcript is
// written under ORK_PROOF_DIR (continuation-contract/server-contract.json).

const (
	canProbeType   = "ContinueAsNewContractProbe"
	canProbeNext   = "next"
	canProbeMemo   = "orkflow_created_at"
	canProbeStamp  = "2026-10-13T12:00:00.000000001Z"
	canSwitches    = 20
	canINWorkflows = 100
	// canVisibilityWindowHalf is W/2: a visibility lag above it fails (j).
	canVisibilityWindowHalf = 30 * time.Minute
)

// canProbe is generation `generation` of a chain of `switches` switches.
// Generation 0 upserts the created_at memo before its switch. With wait, each
// generation blocks on a "next" signal — sent with an empty run id, so it
// reaches whichever generation is current — before it continues as new, and
// the last before it completes. Every successor gets an EMPTY first argument,
// as a run's switch passes an empty run id.
func canProbe(ctx workflow.Context, first string, generation, switches int, wait bool) (int, error) {
	if generation == 0 {
		if err := workflow.UpsertMemo(ctx, map[string]any{canProbeMemo: canProbeStamp}); err != nil {
			return 0, err
		}
	}
	if wait {
		workflow.GetSignalChannel(ctx, canProbeNext).Receive(ctx, nil)
	}
	if generation < switches {
		return 0, workflow.NewContinueAsNewError(ctx, canProbeType, "", generation+1, switches, wait)
	}
	return generation, nil
}

// canHistoryHog grows its history until the cluster stops it.
func canHistoryHog(ctx workflow.Context) error {
	for i := 0; i < 1000; i++ {
		if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

type canContract struct {
	t      *testing.T
	ctx    context.Context
	c      client.Client
	ns     string
	queue  string
	result map[string]any
	mu     sync.Mutex
}

func (k *canContract) record(item, line string, detail any) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.result[item] = map[string]any{"verified": line, "detail": detail}
	k.t.Logf("E1 contract (%s): %s", item, line)
}

func (k *canContract) start(id string, switches int, wait bool) client.WorkflowRun {
	k.t.Helper()
	run, err := k.c.ExecuteWorkflow(k.ctx, client.StartWorkflowOptions{ID: id, TaskQueue: k.queue},
		canProbeType, id, 0, switches, wait)
	if err != nil {
		k.t.Fatalf("start %s: %v", id, err)
	}
	return run
}

// records lists every visibility record of id.
func (k *canContract) records(query string) ([]*workflowpb.WorkflowExecutionInfo, error) {
	var out []*workflowpb.WorkflowExecutionInfo
	var token []byte
	for {
		resp, err := k.c.ListWorkflow(k.ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: k.ns, Query: query, PageSize: 1000, NextPageToken: token})
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetExecutions()...)
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return out, nil
		}
	}
}

func (k *canContract) byID(id string) string {
	return fmt.Sprintf("WorkflowType = %q AND WorkflowId = %q", canProbeType, id)
}

// eventually polls cond every 50 ms until it holds or 60 s pass.
func (k *canContract) eventually(what string, cond func() (bool, error)) {
	k.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		ok, err := cond()
		if err != nil {
			k.t.Fatalf("%s: %v", what, err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			k.t.Fatalf("%s: not within 60s", what)
		}
		select {
		case <-k.ctx.Done():
			k.t.Fatalf("%s: %v", what, k.ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (k *canContract) firstEvent(id, execution string) (*historypb.HistoryEvent, error) {
	iter := k.c.GetWorkflowHistory(k.ctx, id, execution, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !iter.HasNext() {
		return nil, fmt.Errorf("history of %s/%s is empty", id, execution)
	}
	return iter.Next()
}

func (k *canContract) current(id string) (*workflowpb.WorkflowExecutionInfo, error) {
	resp, err := k.c.DescribeWorkflowExecution(k.ctx, id, "")
	if err != nil {
		return nil, err
	}
	return resp.GetWorkflowExecutionInfo(), nil
}

func TestIntegration_ContinueAsNewServerContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	ns := routingNamespace()
	c := dialRoutingCluster(t, ctx, address, ns)
	k := &canContract{t: t, ctx: ctx, c: c, ns: ns, result: map[string]any{},
		queue: fmt.Sprintf("ork-can-contract-%d-%d", os.Getpid(), time.Now().UnixNano())}
	w := worker.New(c, k.queue, worker.Options{DisableRegistrationAliasing: true})
	w.RegisterWorkflowWithOptions(canProbe, workflow.RegisterOptions{Name: canProbeType})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	dc := converter.GetDefaultDataConverter()

	id := fmt.Sprintf("can-contract-%d-%d", os.Getpid(), time.Now().UnixNano())
	t0 := time.Now().Add(-time.Second).UTC()
	run := k.start(id, canSwitches, true)
	executions := []string{run.GetRunID()}

	// (e) a long poll on generation 0 ends at CONTINUED_AS_NEW naming its
	// successor.
	longPolled := make(chan string, 1)
	go func() {
		iter := c.GetWorkflowHistory(ctx, id, executions[0], true, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				longPolled <- "error: " + err.Error()
				return
			}
			if attrs := event.GetWorkflowExecutionContinuedAsNewEventAttributes(); attrs != nil {
				longPolled <- attrs.GetNewExecutionRunId()
				return
			}
		}
		longPolled <- "the long poll ended without CONTINUED_AS_NEW"
	}()

	// (j) and (f): 20 switches, each started by a signal with an empty run id
	// — which must reach the current generation — and watched in the index
	// every 50 ms: which of the closing and the starting record is indexed
	// first, and how far apart.
	type switchSeen struct {
		Generation   int     `json:"generation"`
		First        string  `json:"first_indexed"`
		LagMillis    float64 `json:"lag_ms"`
		SuccessorRun string  `json:"successor_run_id"`
	}
	var seen []switchSeen
	var maxLag time.Duration
	for gen := 0; gen < canSwitches; gen++ {
		if err := c.SignalWorkflow(ctx, id, "", canProbeNext, nil); err != nil {
			t.Fatalf("signal generation %d: %v", gen, err)
		}
		closing := executions[gen]
		var successor string
		k.eventually(fmt.Sprintf("generation %d's successor started", gen+1), func() (bool, error) {
			info, err := k.current(id)
			if err != nil {
				return false, err
			}
			successor = info.GetExecution().GetRunId()
			return successor != closing, nil
		})
		executions = append(executions, successor)
		var closedAt, startedAt time.Time
		k.eventually(fmt.Sprintf("generation %d's two records indexed", gen), func() (bool, error) {
			records, err := k.records(k.byID(id))
			if err != nil {
				return false, err
			}
			now := time.Now()
			for _, info := range records {
				switch info.GetExecution().GetRunId() {
				case closing:
					if closedAt.IsZero() && info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
						closedAt = now
					}
				case successor:
					if startedAt.IsZero() {
						startedAt = now
					}
				}
			}
			return !closedAt.IsZero() && !startedAt.IsZero(), nil
		})
		first, lag := "both in one poll", time.Duration(0)
		switch {
		case closedAt.Before(startedAt):
			first, lag = "closing (ContinuedAsNew) record", startedAt.Sub(closedAt)
		case startedAt.Before(closedAt):
			first, lag = "successor (Running) record", closedAt.Sub(startedAt)
		}
		maxLag = max(maxLag, lag)
		seen = append(seen, switchSeen{Generation: gen, First: first, LagMillis: float64(lag) / float64(time.Millisecond), SuccessorRun: successor})
	}
	if maxLag > canVisibilityWindowHalf {
		t.Fatalf("(j) the largest visibility lag at a switch is %s, above W/2 = %s", maxLag, canVisibilityWindowHalf)
	}
	order := map[string]int{}
	for _, s := range seen {
		order[s.First]++
	}
	k.record("j", fmt.Sprintf("over %d switches, indexed first: %v; largest delay between the two records %s (W/2 = %s)",
		canSwitches, order, maxLag, canVisibilityWindowHalf), seen)
	k.record("f", fmt.Sprintf("%d signals sent with an empty run id after each switch each reached the successor", canSwitches), nil)

	select {
	case got := <-longPolled:
		if got != executions[1] {
			t.Fatalf("(e) the long poll on generation 0 = %q, want CONTINUED_AS_NEW naming %s", got, executions[1])
		}
		k.record("e", "a long poll on generation 0 ended at CONTINUED_AS_NEW naming new_execution_run_id "+got, nil)
	case <-time.After(30 * time.Second):
		t.Fatal("(e) the long poll on generation 0 did not end")
	}

	// (a) GetWorkflowHistory(id, "") is the newest execution; (i) its first
	// argument is empty and the workflow id is kept; (k) the 10 s workflow
	// task timeout.
	newest, err := k.firstEvent(id, "")
	if err != nil {
		t.Fatal(err)
	}
	attrs := newest.GetWorkflowExecutionStartedEventAttributes()
	var first string
	var generation int
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) != 4 {
		t.Fatalf("the newest start carries %d arguments", len(payloads))
	}
	if err := dc.FromPayload(payloads[0], &first); err != nil {
		t.Fatal(err)
	}
	if err := dc.FromPayload(payloads[1], &generation); err != nil {
		t.Fatal(err)
	}
	if generation != canSwitches || attrs.GetContinuedExecutionRunId() != executions[canSwitches-1] {
		t.Fatalf("(a) GetWorkflowHistory(id, \"\") started generation %d continued from %s; want %d from %s",
			generation, attrs.GetContinuedExecutionRunId(), canSwitches, executions[canSwitches-1])
	}
	k.record("a", fmt.Sprintf("GetWorkflowHistory(id, \"\") returned generation %d, the newest", generation), nil)
	if first != "" {
		t.Fatalf("(i) the successor's first argument = %q, want empty", first)
	}
	current, err := k.current(id)
	if err != nil {
		t.Fatal(err)
	}
	if current.GetExecution().GetWorkflowId() != id {
		t.Fatalf("(i) the chain's workflow id is %q, want %q", current.GetExecution().GetWorkflowId(), id)
	}
	k.record("i", "a continue-as-new with an empty first argument started the successor with payload 0 \"\" under the same workflow id", nil)
	gen0, err := k.firstEvent(id, executions[0])
	if err != nil {
		t.Fatal(err)
	}
	for name, event := range map[string]*historypb.HistoryEvent{"generation 0": gen0, "the newest generation": newest} {
		if timeout := event.GetWorkflowExecutionStartedEventAttributes().GetWorkflowTaskTimeout().AsDuration(); timeout != 10*time.Second {
			t.Fatalf("(k) %s's workflow_task_timeout = %s, want 10s", name, timeout)
		}
	}
	k.record("k", "the started events' workflow_task_timeout is 10s, generation 0's and a successor's", nil)

	// (b) closed generations list as ContinuedAsNew with a CloseTime, and
	// the window query finds them; (c) the memo; (g) StartTimes.
	var all []*workflowpb.WorkflowExecutionInfo
	k.eventually("every generation indexed", func() (bool, error) {
		all, err = k.records(k.byID(id))
		return err == nil && len(all) == canSwitches+1, err
	})
	windowQuery := fmt.Sprintf("WorkflowType = %q AND WorkflowId = %q AND ExecutionStatus = %q AND CloseTime >= %q",
		canProbeType, id, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW.String(), t0.Format("2006-01-02T15:04:05.000000000Z"))
	var windowed []*workflowpb.WorkflowExecutionInfo
	k.eventually("the ContinuedAsNew window query", func() (bool, error) {
		windowed, err = k.records(windowQuery)
		return err == nil && len(windowed) == canSwitches, err
	})
	starts := map[string]time.Time{}
	closes := map[string]time.Time{}
	for _, info := range all {
		runID := info.GetExecution().GetRunId()
		starts[runID] = info.GetStartTime().AsTime()
		if runID != current.GetExecution().GetRunId() {
			if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW || info.GetCloseTime() == nil {
				t.Fatalf("(b) closed generation %s lists as %s close %v", runID, info.GetStatus(), info.GetCloseTime())
			}
			closes[runID] = info.GetCloseTime().AsTime()
		}
		var memo string
		if payload, ok := info.GetMemo().GetFields()[canProbeMemo]; !ok || dc.FromPayload(payload, &memo) != nil || memo != canProbeStamp {
			t.Fatalf("(c) record %s carries memo %q (present %v), want the upserted %q", runID, memo, ok, canProbeStamp)
		}
	}
	k.record("b", fmt.Sprintf("%d closed generations list as ContinuedAsNew with a CloseTime; %q found all %d",
		canSwitches, windowQuery, len(windowed)), nil)
	k.record("c", "the memo upserted in generation 0 before its switch is on generation 0's own closed record and on every successor's", nil)
	for gen := 0; gen < canSwitches; gen++ {
		next := executions[gen+1]
		if starts[next].Equal(starts[executions[0]]) || starts[next].Before(closes[executions[gen]].Add(-time.Second)) {
			t.Fatalf("(g) generation %d's StartTime %s is not its own (generation 0 started %s, generation %d closed %s)",
				gen+1, starts[next], starts[executions[0]], gen, closes[executions[gen]])
		}
	}
	k.record("g", "every successor's StartTime is its own, at or after its predecessor's close", nil)

	// (d) DeleteWorkflowExecution by explicit run id removes one generation;
	// an empty run id removes only the current one.
	admin := c.WorkflowService()
	if _, err := admin.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: executions[0]}}); err != nil {
		t.Fatalf("delete generation 0: %v", err)
	}
	k.eventually("generation 0 deleted", func() (bool, error) {
		_, err := k.firstEvent(id, executions[0])
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound), nil
	})
	if _, err := k.firstEvent(id, executions[1]); err != nil {
		t.Fatalf("(d) deleting generation 0 by run id also removed generation 1: %v", err)
	}
	if info, err := k.current(id); err != nil || info.GetCloseTime() != nil {
		t.Fatalf("(d) the current generation after deleting generation 0: %v, %v", info, err)
	}
	if err := c.SignalWorkflow(ctx, id, "", canProbeNext, nil); err != nil {
		t.Fatal(err)
	}
	var last int
	if err := c.GetWorkflow(ctx, id, "").Get(ctx, &last); err != nil || last != canSwitches {
		t.Fatalf("the chain ended with %d, %v; want generation %d", last, err, canSwitches)
	}
	if _, err := admin.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id}}); err != nil {
		t.Fatalf("delete with an empty run id: %v", err)
	}
	k.eventually("the current generation deleted", func() (bool, error) {
		_, err := k.firstEvent(id, executions[canSwitches])
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound), nil
	})
	if _, err := k.firstEvent(id, executions[canSwitches-1]); err != nil {
		t.Fatalf("(d) an empty run id also removed generation %d: %v", canSwitches-1, err)
	}
	k.record("d", "DeleteWorkflowExecution by explicit run id removed generation 0 alone; an empty run id removed only the current generation", nil)

	// (l) WorkflowId IN (…) naming 100 ids returns every record of each,
	// closed generations included.
	ids := make([]string, canINWorkflows)
	chains := make([]client.WorkflowRun, canINWorkflows)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-in-%03d", id, i)
		chains[i] = k.start(ids[i], 1, false)
	}
	for i, chain := range chains {
		if err := chain.Get(ctx, nil); err != nil {
			t.Fatalf("chain %s: %v", ids[i], err)
		}
	}
	quoted := make([]string, len(ids))
	for i, x := range ids {
		quoted[i] = strconv.Quote(x)
	}
	inQuery := fmt.Sprintf("WorkflowType = %q AND WorkflowId IN (%s)", canProbeType, strings.Join(quoted, ", "))
	var inRecords []*workflowpb.WorkflowExecutionInfo
	var inErr error
	k.eventually("the IN listing", func() (bool, error) {
		inRecords, inErr = k.records(inQuery)
		var invalid *serviceerror.InvalidArgument
		if errors.As(inErr, &invalid) {
			return true, nil
		}
		return inErr == nil && len(inRecords) == 2*canINWorkflows, inErr
	})
	if inErr != nil {
		k.record("l", "the cluster refuses WorkflowId IN (…): "+inErr.Error()+"; resolveChains' per-id fallback is the path in use", nil)
	} else {
		closed := 0
		for _, info := range inRecords {
			if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
				closed++
			}
		}
		if closed != canINWorkflows {
			t.Fatalf("(l) the IN listing returned %d closed generations of %d chains", closed, canINWorkflows)
		}
		k.record("l", fmt.Sprintf("IN supported: WorkflowId IN (…) naming %d ids returned all %d records, %d closed generations included",
			canINWorkflows, len(inRecords), closed), nil)
	}

	// (h) the cluster's termination reason at its history-count limit.
	k.historyLimitReason(t)

	proof.WriteJSON(t, "continuation-contract", "server-contract.json", k.result)
}

// historyLimitReason runs a private dev server of the image the stack runs,
// with its history-count limit lowered by dynamic config, and reads the
// reason it terminates a workflow with at that limit — the text the reader's
// backstop matches. Where no such server can be started (no docker, the image
// not present), (h) is logged as not verifiable on this cluster.
func (k *canContract) historyLimitReason(t *testing.T) {
	notVerifiable := func(why string) {
		k.record("h", "not verifiable on this cluster: "+why+"; the backstop stays documented as unverified", nil)
	}
	image, err := composeTemporalImage()
	if err != nil {
		notVerifiable(err.Error())
		return
	}
	if _, err := exec.LookPath("docker"); err != nil {
		notVerifiable("no docker on this host")
		return
	}
	if err := exec.CommandContext(k.ctx, "docker", "image", "inspect", image).Run(); err != nil {
		notVerifiable("the image " + image + " is not present")
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	// timeout(1) bounds the server's life even if this process dies before
	// its cleanup runs.
	out, err := exec.CommandContext(k.ctx, "docker", "run", "-d", "--rm",
		"--label", fmt.Sprintf("ork.test=can-contract-%d", os.Getpid()),
		"-p", fmt.Sprintf("127.0.0.1:%d:7233", port), "--entrypoint", "sh", image, "-c",
		"exec timeout 600 temporal server start-dev --ip 0.0.0.0 --headless --log-level error "+
			"--dynamic-config-value limit.historyCount.error=60 --dynamic-config-value limit.historyCount.warn=30").Output()
	if err != nil {
		notVerifiable("the private dev server did not start: " + err.Error())
		return
	}
	container := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
	var c client.Client
	deadline := time.Now().Add(90 * time.Second)
	for {
		c, err = client.DialContext(k.ctx, client.Options{HostPort: fmt.Sprintf("127.0.0.1:%d", port), Namespace: client.DefaultNamespace})
		if err == nil {
			if _, err = c.CheckHealth(k.ctx, &client.CheckHealthRequest{}); err == nil {
				break
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("(h) the private dev server did not answer: %v", err)
		}
		select {
		case <-k.ctx.Done():
			t.Fatal(k.ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	defer c.Close()
	queue := fmt.Sprintf("ork-can-hog-%d", os.Getpid())
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflowWithOptions(canHistoryHog, workflow.RegisterOptions{Name: "ContinueAsNewContractHistoryHog"})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	var run client.WorkflowRun
	for {
		run, err = c.ExecuteWorkflow(k.ctx, client.StartWorkflowOptions{TaskQueue: queue}, "ContinueAsNewContractHistoryHog")
		if err == nil || time.Now().After(deadline) {
			break
		}
		select { // the default namespace is still registering
		case <-k.ctx.Done():
			t.Fatal(k.ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	if err != nil {
		t.Fatalf("(h) start the history hog: %v", err)
	}
	_ = run.Get(k.ctx, nil)
	iter := c.GetWorkflowHistory(k.ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	var reason string
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if attrs := event.GetWorkflowExecutionTerminatedEventAttributes(); attrs != nil {
			reason = attrs.GetReason()
		}
	}
	if reason == "" {
		t.Fatalf("(h) the cluster did not terminate a workflow past its lowered history-count limit")
	}
	if !engine.IsHistoryLimitTerminationForTest(reason) {
		t.Fatalf("(h) the cluster's termination reason %q is not what the reader's backstop matches", reason)
	}
	k.record("h", fmt.Sprintf("verified on a private dev server (%s, limit.historyCount.error=60): the termination reason %q matches the backstop", image, reason), nil)
}

var composeImage = regexp.MustCompile(`image:\s*(temporalio/temporal:\S+)`)

// composeTemporalImage is the Temporal image deploy/docker-compose.yml runs.
func composeTemporalImage() (string, error) {
	data, err := os.ReadFile("../../deploy/docker-compose.yml")
	if err != nil {
		return "", fmt.Errorf("read deploy/docker-compose.yml: %w", err)
	}
	match := composeImage.FindSubmatch(data)
	if match == nil {
		return "", fmt.Errorf("deploy/docker-compose.yml names no temporalio/temporal image")
	}
	return string(match[1]), nil
}
