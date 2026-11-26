//go:build integration

package engine_test

import (
	"context"
	"encoding/json/jsontext"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// sourceNamespaces answers every namespace as the default one is.
type sourceNamespaces struct{}

func (sourceNamespaces) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	return &orkstackv1.Namespace{Name: name}, nil
}

// sourceStack is a source's workflow on a real cluster: the engine's worker,
// its stores in a directory, and a stand-in for the source hosts of one pool,
// whose instances do what a scenario tells them to.
type sourceStack struct {
	t       *testing.T
	ctx     context.Context
	c       client.Client
	docs    *artifacts.BlobEventSourceStore
	records *artifacts.EventSourceInstanceStore
	starter *engine.SourceStarter

	mu sync.Mutex
	// ends is what an instance of a source returns, by its id; none keeps it
	// running until it is cancelled.
	ends map[string]*orkstackv1.HostEventSourceResult
}

func newSourceStack(t *testing.T, ctx context.Context, pin eventsource.Pinned) *sourceStack {
	t.Helper()
	namespace := chaosNamespace("evs-record")
	c := dialRoutingCluster(t, ctx, temporalAddress(), namespace)
	dir := t.TempDir()
	docs, err := artifacts.NewLocalEventSourceStore(dir)
	must.OKf(t, err, "the documents")
	records, err := artifacts.NewLocalEventSourceInstanceStore(dir)
	must.OKf(t, err, "the records")
	logger := slog.New(slog.DiscardHandler)
	s := &sourceStack{t: t, ctx: ctx, c: c, docs: docs, records: records, ends: map[string]*orkstackv1.HostEventSourceResult{},
		starter: &engine.SourceStarter{Client: c, Tombstoned: records.Tombstoned, Namespaces: sourceNamespaces{}, Pin: pin, Versioned: true, Runs: true, Logger: logger}}
	// As cmd/ork's run worker has them, on the channel a source's workflow is
	// started on.
	w := worker.New(c, task.DefaultQueue, worker.Options{Interceptors: append([]interceptor.WorkerInterceptor{engine.PayloadInterceptor(nil)}, engine.RunWorkerInterceptors()...)})
	engine.RegisterWorkflows(w)
	engine.RegisterSourceActivities(w, &engine.SourceWorker{Documents: docs, Records: records, Namespaces: sourceNamespaces{}, Pin: pin,
		MaxEvents: engine.DefaultHistoryMaxEvents, MaxBytes: engine.DefaultHistoryMaxBytes, Now: time.Now, Logger: logger})
	hosts := worker.New(c, "ork-source-gpu", worker.Options{})
	hosts.RegisterActivityWithOptions(s.host, activity.RegisterOptions{Name: task.ActivityHostEventSource})
	for _, w := range []worker.Worker{w, hosts} {
		must.OKf(t, w.Start(), "start a worker")
		t.Cleanup(w.Stop)
	}
	return s
}

// host is one attempt of an instance: it ends as its source's scenario says,
// or beats until it is cancelled.
func (s *sourceStack) host(ctx context.Context, in *orkstackv1.HostEventSourceInput) (*orkstackv1.HostEventSourceResult, error) {
	s.mu.Lock()
	end := s.ends[in.GetEventSourceId()]
	s.mu.Unlock()
	if end != nil && in.GetIndex() == 0 {
		return end, nil
	}
	for {
		activity.RecordHeartbeat(ctx, eventsources.HeartbeatDetails{Epoch: in.GetEpoch(), State: "active", Since: time.Now()}.Encode())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// source is a source of two instances on the pool the stand-in hosts, with
// its spec stored.
func (s *sourceStack) source(t *testing.T, name string, edit func(*orkstackv1.EventSource)) *orkstackv1.EventSource {
	t.Helper()
	id, err := eventsources.NewID()
	must.OKf(t, err, "an id")
	src := &orkstackv1.EventSource{Id: id, Namespace: "default", Name: name, OrkflowName: "flow", Uses: "ghcr.io/acme/s:1", Pool: "gpu", Instances: 2,
		ServiceRevision: 1, JudgingRevision: 1, ServiceDigest: strings.Repeat("a", 64)}
	if edit != nil {
		edit(src)
	}
	must.OKf(t, s.docs.PutSpec(s.ctx, id, src.GetServiceDigest(), []byte("{}")), "its spec")
	out, err := s.docs.Create(s.ctx, src)
	must.OKf(t, err, "its document")
	return out
}

func (s *sourceStack) start(t *testing.T, src *orkstackv1.EventSource) {
	t.Helper()
	got, err := s.starter.Start(s.ctx, src)
	if err != nil || got != engine.SourceStarted {
		t.Fatalf("start %s: %s, %v", src.GetName(), got, err)
	}
}

// memo is what the latest execution's memo says now.
func (s *sourceStack) memo(t *testing.T, id string) (runID string, open bool, carry eventsource.Carry, now eventsource.Now) {
	t.Helper()
	resp, err := describeExecution(s.c, s.ctx, id, "")
	must.OKf(t, err, "describe %s", id)
	info := resp.GetWorkflowExecutionInfo()
	var raw jsontext.Value
	if p, ok := info.GetMemo().GetFields()["carry"]; ok {
		must.OKf(t, engine.DataConverter().FromPayload(p, &raw), "its carry")
		carry, err = eventsource.DecodeCarry(raw)
		must.OKf(t, err, "its carry")
	}
	if p, ok := info.GetMemo().GetFields()["now"]; ok {
		must.OKf(t, engine.DataConverter().FromPayload(p, &raw), "its now")
		now, err = eventsource.DecodeNow(raw)
		must.OKf(t, err, "its now")
	}
	return info.GetExecution().GetRunId(), info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, carry, now
}

// until waits for the memo to say what holds says.
func (s *sourceStack) until(t *testing.T, id, what string, holds func(open bool, carry eventsource.Carry, now eventsource.Now) bool) string {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		run, open, carry, now := s.memo(t, id)
		if holds(open, carry, now) {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: never %s: carry %+v, now %+v", id, what, carry, now)
		}
		select {
		case <-s.ctx.Done():
			t.Fatal(s.ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ready reports both instances as an Orker's door would.
func (s *sourceStack) ready(t *testing.T, src *orkstackv1.EventSource) {
	t.Helper()
	_, _, _, now := s.memo(t, src.GetId())
	for index, slot := range now.Slots {
		ref := held.Ref{SourceID: src.GetId(), Index: index, Epoch: slot.Current.Epoch, Attempt: 1}
		must.OKf(t, s.starter.Started(s.ctx, ref), "started")
		must.OKf(t, s.starter.Report(s.ctx, ref, orkstackv1.InstanceReport_INSTANCE_REPORT_READY, ""), "ready")
	}
}

// handOver ends the execution as a hand-over does, and waits for it to close.
func (s *sourceStack) handOver(t *testing.T, id, run string) {
	t.Helper()
	must.OKf(t, s.c.SignalWorkflow(s.ctx, id, run, engine.SignalSourceHandover, nil), "hand over")
	s.closed(t, id, run)
}

func (s *sourceStack) closed(t *testing.T, id, run string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		resp, err := describeExecution(s.c, s.ctx, id, run)
		must.OKf(t, err, "describe %s", id)
		if resp.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s/%s did not close", id, run)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

type sourceScenario struct {
	name string
	pin  func(*eventsource.Pinned)
	run  func(t *testing.T, s *sourceStack, name string)
}

func sourceScenarios() []sourceScenario {
	bothReady := func(open bool, _ eventsource.Carry, now eventsource.Now) bool {
		return open && len(now.Slots) == 2 && now.Slots[0].Current.Phase == eventsource.PhaseReady && now.Slots[1].Current.Phase == eventsource.PhaseReady
	}
	twoScheduled := func(open bool, _ eventsource.Carry, now eventsource.Now) bool { return open && len(now.Slots) == 2 }
	return []sourceScenario{
		{name: "runs", run: func(t *testing.T, s *sourceStack, name string) {
			src := s.source(t, name, nil)
			s.start(t, src)
			s.until(t, src.GetId(), "running both", twoScheduled)
			s.ready(t, src)
			run := s.until(t, src.GetId(), "ready", bothReady)
			s.handOver(t, src.GetId(), run)
			_, _, carry, _ := s.memo(t, src.GetId())
			if carry.Continued == nil || carry.Continued.How != eventsource.EndedHandover || carry.Good == nil {
				t.Fatalf("it ended as %+v, good %+v", carry.Continued, carry.Good)
			}
		}},
		{name: "pauses", run: func(t *testing.T, s *sourceStack, name string) {
			src := s.source(t, name, nil)
			s.mu.Lock()
			s.ends[src.GetId()] = &orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE,
				Class: orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE, Reason: eventsources.ReasonServiceConfigRejected}
			s.mu.Unlock()
			s.start(t, src)
			run := s.until(t, src.GetId(), "paused, and stopped", func(open bool, carry eventsource.Carry, now eventsource.Now) bool {
				return open && carry.Loaded != nil && carry.Loaded.Paused && now.Running == 0
			})
			got, err := s.docs.Get(s.ctx, "default", name)
			must.OKf(t, err, "read it")
			if got.GetPause().GetCode() != eventsources.PauseInstanceFailed || got.GetPause().GetRefusalCode() != eventsources.ReasonServiceConfigRejected {
				t.Fatalf("paused as %v", got.GetPause())
			}
			s.handOver(t, src.GetId(), run)
		}},
		{name: "purges", pin: func(p *eventsource.Pinned) { p.PurgeBatch = 2 }, run: func(t *testing.T, s *sourceStack, name string) {
			src := s.source(t, name, func(s *orkstackv1.EventSource) { s.DeletingSince = timestamppb.Now() })
			for _, digit := range []string{"b", "c"} {
				must.OKf(t, s.docs.PutSpec(s.ctx, src.GetId(), strings.Repeat(digit, 64), []byte("{}")), "a spec")
			}
			s.start(t, src)
			run, _, _, _ := s.memo(t, src.GetId())
			s.closed(t, src.GetId(), run)
			_, _, carry, _ := s.memo(t, src.GetId())
			if carry.Continued == nil || carry.Continued.How != eventsource.EndedPurged {
				t.Fatalf("it ended as %+v", carry.Continued)
			}
			if kept, err := s.records.KeepsSpecs(s.ctx, src.GetId()); kept || err != nil {
				t.Fatalf("it kept its specs: %v", err)
			}
			if gone, err := s.records.Tombstoned(s.ctx, src.GetId()); !gone || err != nil {
				t.Fatalf("no tombstone: %v", err)
			}
		}},
		{name: "carries-on", pin: func(p *eventsource.Pinned) { p.BudgetEvents = 1 }, run: func(t *testing.T, s *sourceStack, name string) {
			src := s.source(t, name, func(s *orkstackv1.EventSource) { s.Pause = &orkstackv1.EventSourcePause{Reason: "x"} })
			s.start(t, src)
			first, _, _, _ := s.memo(t, src.GetId())
			s.closed(t, src.GetId(), first)
			next := s.until(t, src.GetId(), "carried on", func(open bool, carry eventsource.Carry, _ eventsource.Now) bool {
				return open && carry.Continued != nil && carry.Continued.How == eventsource.EndedIdle
			})
			if next == first {
				t.Fatal("it did not carry on in another execution")
			}
			// Every one after it carries on at once too, its budget being one
			// event: the chain is ended here, as a person would end it.
			must.OKf(t, s.c.TerminateWorkflow(s.ctx, src.GetId(), "", "the test is done"), "end the chain")
		}},
	}
}

// TestIntegration_EventSourceWorkflow_OnARealCluster runs a source's workflow
// on a real cluster, with its stores in a directory and a stand-in for its
// pool's source hosts, along each of its main paths: two instances started,
// reported ready and handed over; an instance that cannot work, which pauses
// the source and stops the other; a marked source purged in batches; an idle
// source past its budget carried on as new. Each asserts what it comes to.
func TestIntegration_EventSourceWorkflow_OnARealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	for _, sc := range sourceScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			pin := eventsource.Pin(7*24*time.Hour, 5*time.Minute, 5*time.Minute)
			if sc.pin != nil {
				sc.pin(&pin)
			}
			s := newSourceStack(t, ctx, pin)
			sc.run(t, s, sc.name)
		})
	}
}
