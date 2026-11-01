//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The definition-by-reference chaos scenarios (T5, C33–C36): a definition passed by reference
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
	wf := compile(t, "apiVersion: orkstack/v1\nkind: Orkflow\nname: "+name+"\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 30, ""))
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
// cannot reach the Temporal frontend from a definition read until 15 s
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
			// The outage. The cut is made on the hook's entry, inside the
			// workflow task that reads the definition, so counted from that
			// task's start it lasts longer than outage. It must outlast the
			// cluster's 10 s workflow-task timeout (StartRun sets none), so
			// the task that was reading always times out and is retried
			// rather than sometimes completing; 15 s leaves a 5 s margin.
			// Refused > 0 below proves the cut was met.
			const outage = 15 * time.Second
			time.Sleep(outage)
			proxy.Restore()
			s.transcript.step("the frontend reachable again after %v (%d connections refused meanwhile)", outage, proxy.Stats().Refused)
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

// cancelDuringTheLoad starts a C36 run: object storage is cut at
// generation 0's first definition read, onWrite stands in front of the
// replica's event snapshot writes, and the operator's cancel is sent once
// Temporal has retried the failing read — a second read has begun, and the
// cut has refused a connection. It returns the run id and when the cancel
// was sent.
func (s *chaosStack) cancelDuringTheLoad(t *testing.T, name string, proxy *faultproxy.Proxy,
	onWrite func(sequence int64, attempt int32),
) (string, time.Time) {
	t.Helper()
	hook := newBlockingHook(nthFirstRead(1))
	hook.onEntry = proxy.Cut
	t.Cleanup(func() { hook.open(nil) })
	var mu sync.Mutex
	reads := 0
	retried := make(chan struct{})
	store := storeHooks{
		SnapshotStore: s.snapshotStoreVia(proxy),
		onWrite: func(sequence int64, attempt int32) error {
			onWrite(sequence, attempt)
			return nil
		},
		onRead: func() {
			mu.Lock()
			defer mu.Unlock()
			if reads++; reads == 2 {
				close(retried)
			}
		},
	}
	_, budget := s.tcPlan()
	runID, _, _ := s.definitionChaosRun(t, name, budget, replicaOptions{store: store}, hook)
	hook.open(nil)
	select {
	case <-retried:
	case <-time.After(time.Minute):
		t.Fatal("Temporal never retried the failing definition read within a minute of the cut")
	}
	if proxy.Stats().Refused == 0 {
		t.Fatal("no read met the cut: the outage was not exercised")
	}
	s.transcript.step("the definition read failed and was retried under the cut (%+v)", proxy.Stats())
	canceledAt := time.Now()
	if err := s.starter.CancelRun(s.ctx, runID, ""); err != nil {
		t.Fatal(err)
	}
	return runID, canceledAt
}

// TestIntegration_DefinitionChaosC36_CancelDuringTheLoad: an operator's
// cancel while generation 0's definition reads fail and Temporal retries
// them ends the run RUN_CANCELED within 30 s, through endBeforeStart —
// RUN_CREATED then RUN_CANCELED, read through the API's StreamRunEvents —
// with an archive body, never RUN_DEFINITION_*. Object storage comes back
// when the ending writes its first snapshot: by then the cancel has reached
// the read, since nothing in generation 0 writes one before its definition
// is loaded.
func TestIntegration_DefinitionChaosC36_CancelDuringTheLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "C36")
	proxy := s.minioProxy()
	writes := &runCreatedWrites{}
	var restore sync.Once
	runID, canceledAt := s.cancelDuringTheLoad(t, "c36-cancel", proxy, func(sequence int64, attempt int32) {
		writes.note(sequence, attempt)
		restore.Do(proxy.Restore)
	})
	status, _ := s.wait(runID, 2*time.Minute)
	latency := time.Since(canceledAt)
	if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the run closed %v, want Canceled", status)
	}
	if latency > 30*time.Second {
		t.Fatalf("the run ended %s after the cancel, over 30 s", latency)
	}
	reached := s.waitReadCanceled(t, runID, time.Minute)
	if n, _, _ := writes.counts(); n != 1 {
		t.Fatalf("RUN_CREATED's writes began %d times, want once, with the store back", n)
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

// TestIntegration_DefinitionChaosC36_StoreStillCutAtTheEnding is C36 with
// object storage still cut when the canceled run writes RUN_CREATED, and
// through every attempt of that write (run-created-first,
// emitRunCreatedFirst):
//
//   - back for RUN_CREATED's second write: RUN_CREATED and RUN_CANCELED,
//     from 1, served and archived alike;
//   - back only for the terminal event: RUN_CANCELED at 2 is served, and the
//     archive holds it with 1 recorded as an explicit gap, which a read of
//     the archive over 1 names (DataLoss);
//   - cut through every write: nothing is published, the run reads canceled
//     from its close, at 1, and nothing archives it (as for any run that
//     publishes no terminal event).
//
// Before that gate the next write after a refused RUN_CREATED was
// RUN_CANCELED at 2, archived with no word of the missing 1.
func TestIntegration_DefinitionChaosC36_StoreStillCutAtTheEnding(t *testing.T) {
	for _, tc := range []struct {
		name, label string
		// back reports whether the store comes back for an attempt at
		// sequence, the n-th write of sequence 1 (0 for another sequence).
		back func(n int, sequence int64) bool
		want string
		// writes is how many writes of sequence 1 begin; terminal, whether
		// the terminal event is written; gap, whether the archive records 1.
		writes        int
		terminal, gap bool
	}{
		{"back for the second write", "second", func(n int, _ int64) bool { return n >= 2 },
			"[1:RUN_CREATED 2:RUN_CANCELED]", 2, true, false},
		{"back only for the terminal event", "terminal", func(_ int, sequence int64) bool { return sequence != 1 },
			"[2:RUN_CANCELED]", 2, true, true},
		{"cut through every write", "every", func(int, int64) bool { return false },
			"[1:RUN_CANCELED]", 2, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
			defer cancel()
			s := newChaosStack(t, ctx, "C36/cut-"+tc.label)
			proxy := s.minioProxy()
			writes := &runCreatedWrites{}
			var restore, ending sync.Once
			var endingAt time.Time
			runID, canceledAt := s.cancelDuringTheLoad(t, "c36-cut-"+tc.label, proxy,
				func(sequence int64, attempt int32) {
					ending.Do(func() { endingAt = time.Now() })
					if tc.back(writes.note(sequence, attempt), sequence) {
						restore.Do(proxy.Restore)
					}
				})
			status, _ := s.wait(runID, 2*time.Minute)
			restore.Do(proxy.Restore)
			if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
				t.Fatalf("the run closed %v, want Canceled", status)
			}
			// The cancel's own latency: how long until the run began to end.
			// What the ending takes after that is its writes' retries under
			// the cut, which C36 above bounds with the store back.
			ending.Do(func() { t.Fatal("the run ended without writing a snapshot") })
			latency := endingAt.Sub(canceledAt)
			if latency > 30*time.Second {
				t.Fatalf("the run began to end %s after the cancel, over 30 s", latency)
			}
			n, tries, others := writes.counts()
			if tries <= n {
				t.Fatalf("RUN_CREATED's writes made %d attempts over %d writes: the first never met the cut", tries, n)
			}
			if n != tc.writes || others == 0 {
				t.Fatalf("RUN_CREATED's writes began %d times and %d attempts were at other numbers, want %d writes "+
					"and the terminal event attempted", n, others, tc.writes)
			}
			a := s.serveAPI(api.Config{})
			served := apiEvents(t, ctx, a, runID)
			if got := fmt.Sprint(definitionEventTypes(served)); got != tc.want {
				t.Fatalf("StreamRunEvents served %s, want %s", got, tc.want)
			}
			run, err := s.reader.GetRun(ctx, runID)
			if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" {
				t.Fatalf("GetRun = %v (%v), want CANCELED with RUN_CANCELED", run, err)
			}
			if !tc.terminal {
				if published := publishedOf(t, s.history(runID, "")); len(published) != 0 {
					t.Fatalf("the run published %v with every write refused", definitionEventTypes(published))
				}
				if _, err := s.archive.LoadRun(ctx, runID); !errors.Is(err, artifacts.ErrNotFound) {
					t.Fatalf("the archive body = %v, want none: nothing published a terminal event", err)
				}
				s.transcript.figure("cancel_latency", latency.String())
				return
			}
			s.requireArchivedWith(t, a, runID, served, "RUN_CANCELED", tc.gap)
			s.transcript.figure("cancel_latency", latency.String())
			t.Logf("C36 (%s): StreamRunEvents served %s, %s after the cancel; RUN_CREATED's writes began %d times "+
				"in %d attempts; archive gap at 1: %v", tc.name, tc.want, latency, n, tries, tc.gap)
		})
	}
}

// requireArchivedWith: the run's archive body holds the events served and
// the run's code, and records 1 as a gap exactly when gap is set — which a
// read of the archive from the start names, DataLoss, with what lies past it
// still readable from 1.
func (s *chaosStack) requireArchivedWith(t *testing.T, a *chaosAPI, runID string, served []*orkstackv1.Event, code string, gap bool) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	var archived []*orkstackv1.Event
	var err error
	for {
		archived, err = s.archive.LoadRunEvents(s.ctx, runID)
		if !errors.Is(err, artifacts.ErrNotFound) || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	var lost *engine.ArchivedEventsLostError
	switch {
	case gap && !errors.As(err, &lost):
		t.Fatalf("LoadRunEvents = %v, want the archive to record 1 as a gap", err)
	case gap && (len(lost.Gaps) != 1 || lost.Gaps[0].First != 1 || lost.Gaps[0].Last != 1):
		t.Fatalf("the archive records gaps %+v, want 1–1 alone", lost.Gaps)
	case !gap && err != nil:
		t.Fatalf("LoadRunEvents: %v", err)
	}
	if strings.Join(eventKeys(archived), "\n") != strings.Join(eventKeys(served), "\n") {
		t.Fatalf("the archive body's events %v differ from the stream %v", definitionEventTypes(archived), definitionEventTypes(served))
	}
	run, err := s.archive.LoadRun(s.ctx, runID)
	if err != nil || run.GetError().GetCode() != code {
		t.Fatalf("the archived run = %v (%v), want %s", run.GetError(), err, code)
	}
	if !gap {
		return
	}
	if _, err := a.svc.ArchivedEvents(s.ctx, runID, 0); connect.CodeOf(err) != connect.CodeDataLoss || !strings.Contains(err.Error(), "1–1") {
		t.Fatalf("ArchivedEvents from 0 = %v, want DataLoss naming 1–1", err)
	}
	if past, err := a.svc.ArchivedEvents(s.ctx, runID, 1); err != nil || fmt.Sprint(definitionEventTypes(past)) != fmt.Sprint(definitionEventTypes(served)) {
		t.Fatalf("ArchivedEvents from 1 = %v (%v), want %v", definitionEventTypes(past), err, definitionEventTypes(served))
	}
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
