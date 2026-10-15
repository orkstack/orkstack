//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// Stage 5's chaos scenarios (T5, C33–C36): a definition passed by reference
// under a corrupted object, a lost store, a lost frontend and a cancel.

// nthFirstRead wants the n-th read at offset 0 the replica makes: a run
// whose definition travels by reference reads it at offset 0 first in every
// generation, so the first is generation 0's definition and the second
// generation 1's.
func nthFirstRead(n int) func(kind string, offset int64) bool {
	var mu sync.Mutex
	seen := 0
	return func(_ string, offset int64) bool {
		if offset != 0 {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		seen++
		return seen == n
	}
}

// definitionChaosRun starts a 30-task chain under budget (which switches it
// every ~120 events) with its definition passed by reference, on a replica
// built from o with hook as its read hook, and blocks until the hook holds a
// read. It returns the run id, the stream opened at its start and the
// definition object's URI.
func (s *chaosStack) definitionChaosRun(t *testing.T, name string, budget engine.ContinuationBudget, o replicaOptions,
	hook *blockingHook,
) (string, *streamCollector, string) {
	t.Helper()
	o.beforeRead = hook.read
	s.startReplica("replica-a", o)
	s.startProjection(o.client)
	s.startOrker("orker-1", instant)
	runID := s.newRunID()
	s.transcript.run(runID)
	wf := compile(t, "name: "+name+"\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, ""))
	stub, plan, uri := storeDefinitionIn(t, s.ctx, s.store, runID, wf, s.plan(1000, engine.DefaultHistoryMaxBytes, &budget))
	if err := s.starter.StartRun(s.ctx, runID, stub, nil, plan); err != nil {
		t.Fatalf("StartRun %s: %v", runID, err)
	}
	s.transcript.step("run %s started with its %d-byte definition passed by reference", runID, plan.Definition.SizeBytes)
	stream := s.openStream(s.reader, runID, 0)
	hook.waitEntered(t, 2*time.Minute)
	execution, _ := s.current(runID)
	s.transcript.step("a definition read is held in execution %s (generation %d)", execution,
		carryOf(t, s.history(runID, execution)).GetGeneration())
	return runID, stream, uri
}

// apiEvents is the run's stream as the API's StreamRunEvents serves it.
func apiEvents(t *testing.T, ctx context.Context, a *chaosAPI, runID string) []*orkstackv1.Event {
	t.Helper()
	var events []*orkstackv1.Event
	if err := a.svc.StreamRunEvents(ctx, runID, "", 0, func(e *orkstackv1.Event) error {
		events = append(events, e)
		return nil
	}); err != nil {
		t.Fatalf("StreamRunEvents: %v", err)
	}
	return events
}

// TestIntegration_DefinitionChaosC33_CorruptAtALaterGeneration: after the
// first switch, generation 1's definition read is held while the object is
// overwritten with other bytes of its size. The run ends
// RUN_DEFINITION_CORRUPT: a terminal event built by endUnrestorable from the
// switch snapshot, which the API's StreamRunEvents serves with the object
// still corrupt (RunStart reads no object), an archive body, and Temporal
// Failed with that type.
func TestIntegration_DefinitionChaosC33_CorruptAtALaterGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C33")
	hook := newBlockingHook(nthFirstRead(2))
	t.Cleanup(func() { hook.open(nil) })
	_, budget := s.tcPlan()
	runID, stream, uri := s.definitionChaosRun(t, "c33-corrupt", budget, replicaOptions{}, hook)
	execution, _ := s.current(runID)
	carry := carryOf(t, s.history(runID, execution))
	if carry == nil {
		t.Fatal("the held definition read is generation 0's")
	}
	data, err := s.store.ReadSnapshot(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	forged := bytes.Repeat([]byte("not the run's definition "), len(data)/25+1)[:len(data)]
	raw := newRawS3ClientForCommitE2E(t, s.cfg)
	bucket, key := s3Key(t, uri)
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
		Body: bytes.NewReader(forged), ContentLength: aws.Int64(int64(len(forged)))}); err != nil {
		t.Fatalf("overwrite the definition: %v", err)
	}
	s.transcript.step("definition overwritten with %d other bytes", len(forged))
	hook.open(nil)
	status, _ := s.wait(runID, 3*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, engine.RunDefinitionCorruptCode)
	last := events[len(events)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || last.GetSequence() != carry.GetEventSequence()+1 {
		t.Fatalf("the last event is %v at %d, want RUN_FAILED right after the carried %d", last.GetType(), last.GetSequence(),
			carry.GetEventSequence())
	}
	_, histories := s.histories(runID)
	if got := failureType(t, histories[len(histories)-1]); got != engine.RunDefinitionCorruptCode {
		t.Fatalf("the execution failed with type %q, want %s", got, engine.RunDefinitionCorruptCode)
	}
	s.requireTerminalIsTheSwitchSnapshot(t, runID, carry)
	s.requireCategory(t, runID, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION)
	a := s.serveAPI(api.Config{})
	served := apiEvents(t, ctx, a, runID)
	if strings.Join(eventKeys(served), "\n") != strings.Join(eventKeys(events), "\n") {
		t.Fatal("the API's StreamRunEvents serves another stream than Events(0)")
	}
	t.Logf("C33: RUN_DEFINITION_CORRUPT at generation %d, RUN_FAILED at %d from the switch snapshot; StreamRunEvents served %d "+
		"events with the object still corrupt; archived", carry.GetGeneration(), last.GetSequence(), len(served))
}

// TestIntegration_DefinitionChaosC34_StoreCutDuringTheLoad: object storage
// is cut at generation 0's first definition read and restored 10 s later,
// inside the restore timeout: the load completes on a retry and the run
// succeeds, with no RUN_DEFINITION_* code.
func TestIntegration_DefinitionChaosC34_StoreCutDuringTheLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C34/restored")
	proxy := s.minioProxy()
	hook := newBlockingHook(nthFirstRead(1))
	hook.onEntry = proxy.Cut
	t.Cleanup(func() { hook.open(nil) })
	_, budget := s.tcPlan()
	runID, stream, _ := s.definitionChaosRun(t, "c34-restored", budget, replicaOptions{store: s.snapshotStoreVia(proxy)}, hook)
	s.transcript.step("object storage cut at generation 0's first definition read")
	hook.open(nil)
	time.Sleep(10 * time.Second) // an outage of a known length, inside the window
	proxy.Restore()
	s.transcript.step("object storage restored after 10 s (%+v)", proxy.Stats())
	status, err := s.wait(runID, 4*time.Minute)
	if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the run closed %v: %v", status, err)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
	for _, e := range events {
		if strings.Contains(e.GetMessage(), "RUN_DEFINITION_") || strings.Contains(e.GetMessage(), "its definition") {
			t.Fatalf("event %d mentions the definition: %q", e.GetSequence(), e.GetMessage())
		}
	}
	if proxy.Stats().Refused == 0 {
		t.Fatal("no read met the cut: the outage was not exercised")
	}
	t.Logf("C34: storage back within the window; the definition loaded and the run succeeded across %d generations (%d events, "+
		"%d refused connections)", len(s.chain(runID)), len(events), proxy.Stats().Refused)
}

// TestIntegration_DefinitionChaosC34_DeletedAtALaterGeneration: the
// definition object is deleted at generation 1's load and never restored:
// the run ends RUN_DEFINITION_UNAVAILABLE after its one-minute restore
// timeout, with a terminal event, and that generation's whole history stays
// within RestoreHistoryBound of its plan.
func TestIntegration_DefinitionChaosC34_DeletedAtALaterGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C34/deleted")
	hook := newBlockingHook(nthFirstRead(2))
	t.Cleanup(func() { hook.open(nil) })
	budget := engine.DefaultContinuationBudget(120, 1<<20, time.Minute)
	runID, stream, uri := s.definitionChaosRun(t, "c34-deleted", budget, replicaOptions{}, hook)
	raw := newRawS3ClientForCommitE2E(t, s.cfg)
	bucket, key := s3Key(t, uri)
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
		t.Fatalf("delete the definition: %v", err)
	}
	deletedAt := time.Now()
	s.transcript.step("definition deleted at generation 1's load")
	hook.open(nil)
	status, _ := s.wait(runID, 4*time.Minute)
	took := time.Since(deletedAt)
	if status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the run closed %v, want Failed", status)
	}
	events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_FAILED, engine.RunDefinitionUnavailableCode)
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("the last event is %v, want a published RUN_FAILED", last.GetType())
	}
	s.requireCategory(t, runID, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR)
	_, histories := s.histories(runID)
	last := histories[len(histories)-1]
	if got := failureType(t, last); got != engine.RunDefinitionUnavailableCode {
		t.Fatalf("the execution failed with type %q, want %s", got, engine.RunDefinitionUnavailableCode)
	}
	bound := engine.RestoreHistoryBound(budget)
	if n := int64(len(last.GetEvents())); n > bound {
		t.Fatalf("the failed generation's history holds %d events, over RestoreHistoryBound %d", n, bound)
	}
	s.transcript.figure("bootstrap_events", len(last.GetEvents()))
	s.transcript.figure("restore_history_bound", bound)
	s.transcript.figure("ended_after", took.String())
	t.Logf("C34 variant: RUN_DEFINITION_UNAVAILABLE %s after the delete with a terminal event; the generation's history %d "+
		"events ≤ bound %d", took, len(last.GetEvents()), bound)
}

// TestIntegration_DefinitionChaosC35_FrontendCutDuringTheLoad: the engine
// cannot reach the Temporal frontend from a definition read until 30 s
// later — at generation 0's load, and at generation 1's. The load completes,
// the run succeeds, and no RUN_DEFINITION_* code is recorded.
func TestIntegration_DefinitionChaosC35_FrontendCutDuringTheLoad(t *testing.T) {
	for _, tc := range []struct {
		name string
		nth  int
	}{{"generation 0", 1}, {"generation 1", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
			defer cancel()
			s := newChaosStack(t, ctx, fmt.Sprintf("C35/g%d", tc.nth-1))
			proxy := s.temporalProxy()
			engineClient := s.dial("replica-a", proxy.Addr())
			hook := newBlockingHook(nthFirstRead(tc.nth))
			hook.onEntry = proxy.Cut
			t.Cleanup(func() { hook.open(nil) })
			_, budget := s.tcPlan()
			runID, stream, _ := s.definitionChaosRun(t, fmt.Sprintf("c35-g%d", tc.nth-1), budget,
				replicaOptions{client: engineClient}, hook)
			s.transcript.step("the engine's connection to the Temporal frontend cut at the definition read")
			hook.open(nil)
			time.Sleep(30 * time.Second) // the outage
			proxy.Restore()
			s.transcript.step("the frontend reachable again after 30 s (%d connections refused meanwhile)", proxy.Stats().Refused)
			status, err := s.wait(runID, 3*time.Minute)
			if status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
				t.Fatalf("the run closed %v: %v", status, err)
			}
			events := s.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_SUCCEEDED, "")
			for _, e := range events {
				if strings.Contains(e.GetMessage(), "RUN_DEFINITION_") || strings.Contains(e.GetMessage(), "its definition") {
					t.Fatalf("event %d mentions the definition: %q", e.GetSequence(), e.GetMessage())
				}
			}
			if proxy.Stats().Refused == 0 {
				t.Fatal("no connection met the cut: the outage was not exercised")
			}
			t.Logf("C35 (%s): the load completed after the frontend came back; %d generations, %d events",
				tc.name, len(s.chain(runID)), len(events))
		})
	}
}

// TestIntegration_DefinitionChaosC36_CancelDuringTheLoad: an operator's
// cancel while generation 0's definition reads fail and Temporal backs off
// between them ends the run RUN_CANCELED within 30 s, through endBeforeStart
// — RUN_CREATED then RUN_CANCELED, read through the API's StreamRunEvents —
// with an archive body, never RUN_DEFINITION_*.
func TestIntegration_DefinitionChaosC36_CancelDuringTheLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C36")
	proxy := s.minioProxy()
	hook := newBlockingHook(nthFirstRead(1))
	hook.onEntry = proxy.Cut
	t.Cleanup(func() { hook.open(nil) })
	_, budget := s.tcPlan()
	runID, _, _ := s.definitionChaosRun(t, "c36-cancel", budget, replicaOptions{store: s.snapshotStoreVia(proxy)}, hook)
	hook.open(nil)
	// Long enough that Temporal is backing off between failing reads.
	time.Sleep(20 * time.Second)
	canceledAt := time.Now()
	if err := s.starter.CancelRun(ctx, runID, ""); err != nil {
		t.Fatal(err)
	}
	reached := s.waitReadCanceled(t, runID, time.Minute)
	proxy.Restore()
	status, _ := s.wait(runID, 2*time.Minute)
	latency := time.Since(canceledAt)
	if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the run closed %v, want Canceled", status)
	}
	if latency > 30*time.Second {
		t.Fatalf("the run ended %s after the cancel, over 30 s", latency)
	}
	a := s.serveAPI(api.Config{})
	served := apiEvents(t, ctx, a, runID)
	if got := definitionEventTypes(served); fmt.Sprint(got) != "[1:RUN_CREATED 2:RUN_CANCELED]" {
		t.Fatalf("StreamRunEvents served %v, want RUN_CREATED then RUN_CANCELED", got)
	}
	archived := awaitArchivedEvents(t, ctx, s.archive, runID)
	if strings.Join(eventKeys(archived), "\n") != strings.Join(eventKeys(served), "\n") {
		t.Fatal("the archive body's events differ from the stream")
	}
	run, err := s.archive.LoadRun(ctx, runID)
	if err != nil || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("the archived run = %v (%v), want RUN_CANCELED", run.GetError(), err)
	}
	s.transcript.figure("cancel_reached_the_read_after", reached.Sub(canceledAt).String())
	s.transcript.figure("cancel_latency", latency.String())
	t.Logf("C36: canceled during generation 0's definition load: RUN_CANCELED %s after the cancel, RUN_CREATED and "+
		"RUN_CANCELED through StreamRunEvents, archived", latency)
}

// requireCategory: the run's error reads with want from the terminal
// snapshot (GetRun) and from the archive body alike — the same category
// generation 0 and the history-only read give its code.
func (s *chaosStack) requireCategory(t *testing.T, runID string, want orkstackv1.FailureCategory) {
	t.Helper()
	run, err := s.reader.GetRun(s.ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got := run.GetError().GetCategory(); got != want {
		t.Fatalf("GetRun's error %v has category %v, want %v", run.GetError(), got, want)
	}
	archived, err := s.archive.LoadRun(s.ctx, runID)
	if err != nil {
		t.Fatalf("the archive body: %v", err)
	}
	if got := archived.GetError().GetCategory(); got != want {
		t.Fatalf("the archive body's error %v has category %v, want %v", archived.GetError(), got, want)
	}
	s.transcript.step("the run's %s reads %v from GetRun and the archive body", run.GetError().GetCode(), want)
}
