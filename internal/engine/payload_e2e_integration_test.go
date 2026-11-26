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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// sealedE2E is a deployment that seals: an engine holding the engines' key, an
// Orker given its runs' keys, and a plain client that stands for whoever can
// read the cluster and holds nothing.
type sealedE2E struct {
	*eachE2E
	plain client.Client
	ek    *payloadcrypt.Key
	data  payloadcrypt.DataKeySource
	keys  *payloadcrypt.EngineKeys
	// runKeys opens what this engine's activities are given of a sealed run.
	runKeys *engine.RunKeys
	tasks   *e2eTaskKeys
}

// e2eTaskKeys gives the Orker the keys of the runs this test started, in
// place of the engine's GetPayloadKey (P20b).
type e2eTaskKeys struct {
	mu   sync.Mutex
	byID map[string]*payloadcrypt.Key
}

func (k *e2eTaskKeys) KeyFor(_ context.Context, c payloadcrypt.Context) (*payloadcrypt.Key, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if key, ok := k.byID[c.ID]; ok && c.Kind == payloadcrypt.KindRun {
		return key, nil
	}
	return nil, errors.New("no key for " + c.String())
}

func startSealedE2E(t *testing.T) *sealedE2E {
	t.Helper()
	ek, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	must.OK(t, err)
	dk, err := payloadcrypt.NewKey(bytes.Repeat([]byte{9}, payloadcrypt.KeySize))
	must.OK(t, err)
	keys, err := payloadcrypt.NewEngineKeys(ek)
	must.OK(t, err)
	s := &sealedE2E{ek: ek, data: payloadcrypt.NewStaticDataKeys("pk_e2e", dk), keys: keys,
		tasks: &e2eTaskKeys{byID: map[string]*payloadcrypt.Key{}}}
	s.eachE2E = startEachE2EOver(t, true, eachE2ETracing{}, func(e *eachE2E) client.Client {
		s.plain = e.temporal
		// The gate's start carries the run attributes too.
		usable, err := task.EnsureRunSearchAttributes(e.ctx, s.plain, e.namespace)
		must.OKf(t, err, "register the run attributes")
		if !usable {
			t.Fatal("the run attributes are not usable")
		}
		usable, err = task.EnsureConcurrencySearchAttributes(e.ctx, s.plain, e.namespace)
		must.OKf(t, err, "register the concurrency attributes")
		if !usable {
			t.Fatal("the concurrency attributes are not usable")
		}
		e.registerRun = func(w worker.Worker) { engine.RegisterGateActivities(w, engine.NewGateActivities(s.plain)) }
		s.runKeys = engine.NewRunKeys(s.plain, keys)
		o := engine.ClientOptions(client.Options{Namespace: e.namespace}, keys)
		dc := engine.NewDataConverter(keys).WithReaderKeys(s.runKeys.KeyFor)
		o.DataConverter, o.FailureConverter = dc, payloadcrypt.NewFailureConverter(dc, true)
		sealing, err := client.NewClientFromExisting(s.plain, o)
		must.OKf(t, err, "the sealing engine's client")
		return sealing
	})
	s.startSealingOrker()
	return s
}

// startSealingOrker starts a real Orker as cmd/orker registers one, hosting
// pins, dialed with converters that seal.
func (s *sealedE2E) startSealingOrker() {
	t := s.t
	indexes, err := artifacts.NewEachIndexStore(s.ctx, s.cfg)
	must.OKf(t, err, "open line-index store")
	refs, _ := s.store.(artifacts.RefPredictor)
	activities := orker.NewActivitiesWithOptions(s.store, orker.Options{
		Logs: s.logs, Commits: s.commits, Attempts: s.attempts, Refs: refs, EachIndexes: indexes,
		Pool: task.DefaultPool, OrkerID: proof.OrkerID("sealed-" + s.runQueue), WorkRoot: s.workRoot, MaxPins: 1,
	}).WithTaskKeys(s.tasks)
	if err := activities.Boot(s.ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	dc := payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil)
	c, err := client.NewClientFromExisting(s.plain, client.Options{Namespace: s.namespace,
		DataConverter: dc, FailureConverter: payloadcrypt.NewFailureConverter(dc, true)})
	must.OKf(t, err, "the Orker's client")
	t.Cleanup(c.Close)
	w := worker.New(c, s.orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
		EnableSessionWorker: true, MaxConcurrentSessionExecutionSize: 1,
		Interceptors: []interceptor.WorkerInterceptor{activities.WorkerInterceptor()},
	})
	activities.RegisterTaskActivities(w)
	must.OKf(t, w.Start(), "start the sealing Orker")
	t.Cleanup(w.Stop)
}

// startRun starts wf as a run, sealed under a tree key of its own or, for the
// control, in plaintext, and waits for its end.
func (s *sealedE2E) startRun(wf *orkstackv1.Orkflow, plan engine.RunPlan, sealed bool) (runID string, err error) {
	t := s.t
	t.Helper()
	runID = engine.NewRunID()
	wf.Namespace = "default"
	input, err := structpbStruct(map[string]any{"who": engine.Canary})
	must.OK(t, err)
	memo := engine.RunStartMemo(wf, plan)
	scope := payloadcrypt.Unsealed()
	if sealed {
		tree, field, err := payloadcrypt.NewRunTree(s.ctx, s.data, s.ek, "default", "ns_default", runID)
		must.OK(t, err)
		memo[payloadcrypt.MemoField] = field
		scope = payloadcrypt.NewScope(tree, payloadcrypt.RunContext(runID))
		s.tasks.mu.Lock()
		s.tasks.byID[runID] = tree
		s.tasks.mu.Unlock()
	}
	terminateAll(t, s.plain, runID)
	run, err := s.temporal.ExecuteWorkflow(s.ctx, client.StartWorkflowOptions{ID: runID, TaskQueue: s.runQueue, Memo: memo},
		engine.RunWorkflowType, payloadcrypt.Seal(scope, runID), payloadcrypt.Seal(scope, wf),
		payloadcrypt.Seal(scope, &orkstackv1.Payload{Data: input}), payloadcrypt.Seal(scope, plan))
	must.OKf(t, err, "start run %s", runID)
	return runID, run.Get(s.ctx, nil)
}

// reader is a replica's reader holding the engines' key.
func (s *sealedE2E) reader() *engine.TemporalRunReader {
	return engine.NewTemporalRunReader(s.plain).WithSnapshotStore(s.snapshots).WithPayloadKeys(engine.NewRunKeys(s.plain, s.keys))
}

// describe is what the cluster says of workflowID's current execution.
func (s *sealedE2E) describe(workflowID string) *workflowservice.DescribeWorkflowExecutionResponse {
	s.t.Helper()
	described, err := s.plain.DescribeWorkflow(s.ctx, workflowID, "")
	must.OKf(s.t, err, "describe %s", workflowID)
	return described.RawResponse
}

// quotes is every place the cluster holds the canary of workflowID in
// plaintext: its recorded events, its memo and its search attributes.
func (s *sealedE2E) quotes(workflowID string) []string {
	t := s.t
	t.Helper()
	var found []string
	see := func(where string, m proto.Message) {
		raw, err := proto.Marshal(m)
		must.OK(t, err)
		if bytes.Contains(raw, []byte(engine.Canary)) {
			found = append(found, where)
		}
		// The headers a local activity's key travels in are never recorded.
		if bytes.Contains(raw, []byte("ork-payload-")) {
			found = append(found, where+" (a payload header)")
		}
	}
	events := s.plain.GetWorkflowHistory(s.ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for events.HasNext() {
		e, err := events.Next()
		must.OKf(t, err, "history of %s", workflowID)
		see(fmt.Sprintf("event %d %s", e.GetEventId(), e.GetEventType()), e)
	}
	see("the describe", s.describe(workflowID))
	return found
}

// plainLocalResults is every local activity of workflowID whose result its
// history holds unsealed, and how many results it holds.
func (s *sealedE2E) plainLocalResults(workflowID string) (plain []string, results int) {
	t := s.t
	t.Helper()
	events := s.plain.GetWorkflowHistory(s.ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for events.HasNext() {
		e, err := events.Next()
		must.OKf(t, err, "history of %s", workflowID)
		marker := e.GetMarkerRecordedEventAttributes()
		if marker.GetMarkerName() != "LocalActivity" {
			continue
		}
		for _, p := range marker.GetDetails()["result"].GetPayloads() {
			results++
			if !payloadcrypt.IsSealed(p) {
				plain = append(plain, fmt.Sprintf("event %d: %s", e.GetEventId(), marker.GetDetails()["data"].GetPayloads()[0].GetData()))
			}
		}
	}
	return plain, results
}

func (s *sealedE2E) shell(id, script string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Uses: s.alpine, Image: &orkstackv1.ImageRef{Reference: s.alpine, Pinned: s.alpine},
		Needs: needs, Command: []string{"/bin/sh", "-c", script}, TimeoutSeconds: 120, Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE}
}

const sealedValues = ` && printf '{"who":"` + engine.Canary + `"}' > /orkstack/output/values.json`

// TestIntegration_ASealedRunEndToEnd: a sealed run with a pin, a fan-out over
// an artifact's lines and a concurrency limit runs on a real Orker to success;
// of its input, its definition, its tasks' values, its artifacts' contents and
// its failures the cluster holds nothing in plaintext, and a replica that
// holds the engines' key reads all of it back.
func TestIntegration_ASealedRunEndToEnd(t *testing.T) {
	s := startSealedE2E(t)

	t.Run("a pin, a line index and a limit", func(t *testing.T) {
		rows := fmt.Sprintf(`printf '%%s\n' '{"n":"%s-1"}' '{"n":"%s-2"}' > /orkstack/output/artifacts/rows.jsonl`, engine.Canary, engine.Canary)
		with, err := structpbStruct(map[string]any{"n": "{{ .each.item.n }}"})
		must.OK(t, err)
		convert := s.shell("convert", `cat /orkstack/input/config.json`+sealedValues, "list")
		convert.With = with
		convert.Each = &orkstackv1.EachSpec{OverArtifact: "list/rows.jsonl", Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL,
			As: "item", MaxItems: 10, MaxParallel: 2,
			Source: &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: "list", Name: "rows.jsonl"}}}
		keep := s.shell("keep", `printf `+engine.Canary+` > /orkstack/shared/note`+sealedValues, "convert")
		read := s.shell("read", `grep -q `+engine.Canary+` /orkstack/shared/note`+sealedValues, "keep")
		for _, pinned := range []*orkstackv1.Task{keep, read} {
			pinned.Pin, pinned.RunAsOrkerUser = "share", true
		}
		wf := &orkstackv1.Orkflow{Name: "sealed-e2e", Version: "1", Description: engine.Canary, Pins: []*orkstackv1.Pin{{Name: "share"}},
			Tasks: []*orkstackv1.Task{s.shell("list", rows+sealedValues), convert, keep, read}}
		plan := s.plan()
		plan.EachLines = engine.DefaultEachLinesPolicy()
		// A rendered key is readable in the cluster (ENC-D31), so this one
		// holds no run data.
		key := "sealed-e2e-" + s.namespace
		run := 0
		gate := engine.GateID("default", "", key)
		plan.Concurrency = &engine.ConcurrencyPlan{
			Slots: []engine.ConcurrencySlotPlan{{GateID: gate, Key: key, Label: key, Limit: 1,
				Strategy: engine.ConcurrencyStrategyQueue, QueueTimeoutSeconds: 120}},
			Run: &run, SeqBase: 1, GateQueue: s.runQueue, Namespace: "default",
			AcquireTimeout: 30 * time.Second, AcquireBackoffMin: 200 * time.Millisecond, AcquireBackoffMax: 2 * time.Second,
			RequeueInterval: time.Minute, RequeueBackoffFactor: 2, RequeueIntervalMax: 4 * time.Minute,
			PerSlotBytes: 2048, EventHoldersShown: 5, Gate: engine.DefaultGateTunables(),
		}
		runID, err := s.startRun(wf, plan, true)
		must.OKf(t, err, "the sealed run")
		for _, where := range s.quotes(runID) {
			t.Errorf("the cluster holds the run's data in plaintext at %s", where)
		}
		if plain, results := s.plainLocalResults(runID); len(plain) > 0 || results == 0 {
			t.Errorf("of the run's %d local activities' results the cluster holds in plaintext %v", results, plain)
		}
		for _, where := range s.quotes(gate) {
			t.Errorf("the limit's gate holds the run's data in plaintext at %s", where)
		}
		got, err := s.reader().GetRun(s.ctx, runID)
		must.OKf(t, err, "read the run back")
		if got.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || got.GetInput().GetData().AsMap()["who"] != engine.Canary ||
			len(got.GetTaskRuns()) != 4 || taskRunNamed(got, "convert").GetEach().GetCount() != 2 {
			t.Fatalf("read back %v", got)
		}
		events, err := s.reader().Events(s.ctx, runID, 0)
		must.OKf(t, err, "read the run's events back")
		if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
			t.Fatalf("the last event is %v", last)
		}
		// A replica holding no key is told nothing of it.
		if blind, err := engine.NewTemporalRunReader(s.plain).Events(s.ctx, runID, 0); err == nil || strings.Contains(fmt.Sprint(blind, err), engine.Canary) {
			t.Fatalf("a reader with no key read %v, %v", blind, err)
		}
		s.awaitSlotFree(gate)
	})

	// The control: unsealed, the same data is in the cluster, so the scan sees
	// what there is to see.
	for _, sealed := range []bool{true, false} {
		t.Run(fmt.Sprintf("a failing run, sealed %v", sealed), func(t *testing.T) {
			wf := &orkstackv1.Orkflow{Name: "sealed-fails", Version: "1", Tasks: []*orkstackv1.Task{
				s.shell("fails", `echo `+engine.Canary+` >&2; exit 3`)}}
			runID, err := s.startRun(wf, s.plan(), sealed)
			if err == nil {
				t.Fatal("the run did not fail")
			}
			found := s.quotes(runID)
			if sealed && len(found) > 0 {
				t.Errorf("the cluster holds the failing run's data in plaintext at %v", found)
			}
			if !sealed && len(found) == 0 {
				t.Error("the scan found nothing in an unsealed run: it proves nothing of a sealed one")
			}
			got, err := s.reader().GetRun(s.ctx, runID)
			must.OKf(t, err, "read the failed run back")
			row := taskRunNamed(got, "fails")
			if got.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(row.GetError().GetMessage(), "3") ||
				row.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED {
				t.Fatalf("read back %v / %v", got.GetError(), row.GetError())
			}
			if msg := got.GetError().GetMessage(); msg == "" || msg == got.GetError().GetCode() || msg == payloadcrypt.Withheld {
				t.Fatalf("the failure reads back as %q, not its own sentence", msg)
			}
		})
	}
}

// awaitSlotFree waits until the gate has closed or holds nothing: the sealed
// run's release reached a gate that holds no key.
func (s *sealedE2E) awaitSlotFree(gate string) {
	t := s.t
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		described := s.describe(gate)
		if described.GetWorkflowExecutionInfo().GetCloseTime() != nil {
			return
		}
		value, err := s.plain.QueryWorkflow(s.ctx, gate, "", engine.GateQueryState)
		must.OKf(t, err, "ask the gate")
		var view engine.GateStateView
		must.OKf(t, value.Get(&view), "read the gate")
		if len(view.State.Holders) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sealed run's slot was never released: %v", view.State.Holders)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestIntegration_ASealedSchedulesFires: a sealed schedule's fires, on a real
// Schedule: one its preparation kept from starting a run completes with its
// code, and one it refuses fails. The fire opens its own preparation's
// failure, as the SDK wrote it into the history; neither its history nor its
// memo, which is plaintext, quotes the record, the reason's text or the
// refusal.
func TestIntegration_ASealedSchedulesFires(t *testing.T) {
	s := startSealedE2E(t)
	suffix := fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	scheduleID := "sch-00000000" + suffix
	key, field, err := payloadcrypt.NewSchedule(s.ctx, s.data, s.ek, "default", "ns_default", scheduleID)
	must.OK(t, err)
	scope := payloadcrypt.NewScope(key, payloadcrypt.ScheduleContext(scheduleID))

	var mu sync.Mutex
	var records []*orkstackv1.ScheduleRecord
	queue := s.runQueue + "-fires"
	w := worker.New(s.temporal, queue, worker.Options{DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true})
	engine.RegisterWorkflows(w)
	w.RegisterActivityWithOptions(func(_ context.Context, rec *orkstackv1.ScheduleRecord, _ time.Time) (*orkstackv1.ScheduledRunPreparation, error) {
		mu.Lock()
		defer mu.Unlock()
		records = append(records, rec)
		if len(records) == 1 {
			return nil, engine.NewFireNotStarted(engine.FireNotStarted{Code: engine.FireSkippedConcurrencyCode, Message: "key " + engine.Canary + " was full"})
		}
		return nil, temporal.NewNonRetryableApplicationError("no Orkflow for "+engine.Canary, engine.ScheduleFireRefusedType, nil)
	}, activity.RegisterOptions{Name: engine.ActivityPrepareScheduledRun})
	must.OKf(t, w.Start(), "start the fires' worker")
	t.Cleanup(w.Stop)

	input, err := structpbStruct(map[string]any{"who": engine.Canary})
	must.OK(t, err)
	rec := &orkstackv1.ScheduleRecord{ScheduleId: scheduleID, Name: "sealed-" + suffix, Namespace: "default",
		OrkflowName: "sealed-e2e", Input: &orkstackv1.Payload{Data: input},
		Spec: &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}}}
	handle, err := s.temporal.ScheduleClient().Create(s.ctx, client.ScheduleOptions{
		ID: scheduleID, Spec: client.ScheduleSpec{CronExpressions: []string{"0 2 * * *"}},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		Action: &client.ScheduleWorkflowAction{ID: engine.ScheduleFireWorkflowIDPrefix + scheduleID,
			Workflow: engine.ScheduleFireWorkflowType, TaskQueue: queue,
			Args: []any{payloadcrypt.Seal(scope, rec)}, Memo: map[string]any{payloadcrypt.MemoField: field}},
	})
	must.OKf(t, err, "create the sealed schedule")
	t.Cleanup(func() { _ = handle.Delete(context.Background()) })

	for n := 1; n <= 2; n++ {
		must.OKf(t, handle.Trigger(s.ctx, client.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL}), "fire %d", n)
		ids := scheduleFireIDs(t, s.ctx, s.plain, scheduleID, n)
		fire := ids[n-1]
		fireErr := s.plain.GetWorkflow(s.ctx, fire, "").Get(s.ctx, nil)
		described := s.describe(fire)
		memo := func(name string) string {
			var v string
			if p, ok := described.GetWorkflowExecutionInfo().GetMemo().GetFields()[name]; ok {
				must.OKf(t, converter.GetDefaultDataConverter().FromPayload(p, &v), "memo %s", name)
			}
			return v
		}
		switch n {
		case 1:
			must.OKf(t, fireErr, "a fire a limit kept from starting a run")
			if memo(engine.MemoScheduleFireSkipCode) != engine.FireSkippedConcurrencyCode || memo(engine.MemoScheduleFireSkipMessage) == "" {
				t.Fatalf("the skipped fire's memo says %q, %q", memo(engine.MemoScheduleFireSkipCode), memo(engine.MemoScheduleFireSkipMessage))
			}
		case 2:
			if fireErr == nil || memo(engine.MemoScheduleFireRefusal) == "" {
				t.Fatalf("a refused fire ended %v, memo %q", fireErr, memo(engine.MemoScheduleFireRefusal))
			}
		}
		for _, where := range s.quotes(fire) {
			t.Errorf("fire %d: the cluster holds its data in plaintext at %s", n, where)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(records) != 2 || records[0].GetInput().GetData().AsMap()["who"] != engine.Canary {
		t.Fatalf("the fires' preparations were given %v", records)
	}
	stored, err := handle.Describe(s.ctx)
	must.OKf(t, err, "describe the schedule")
	if strings.Contains(fmt.Sprint(stored.Schedule.Action), engine.Canary) {
		t.Error("the schedule's stored action holds its record in plaintext")
	}
}
