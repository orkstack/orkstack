package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// reconcileYAML is the orkflow every admission test knocks with. Its label is
// what the refusal counter's selection must carry stamped ork.label.tier.
const reconcileYAML = `
namespace: payments
name: reconcile
version: 1.0.0
labels:
  Tier: gold
tasks:
  - id: work
    uses: shell
    with: {command: "echo work"}
`

const (
	heldReason   = "partner CI loop, INC-4412"
	holdSentence = `namespace "payments" is on hold since `
	capSentence  = `namespace "payments" already has 2 runs in flight, which is its max_running. ` +
		"Wait for one to finish, or raise the cap with `orkctl namespace update payments --max-running N`"
)

// clusterStarter is a fakeStarter whose accepted runs show up in the reader
// the way a started RunWorkflow shows up in the cluster's index: readable by
// id, listed, and counted as in flight in its namespace.
type clusterStarter struct {
	fakeStarter
	reader *fakeReader
	// namespaces remembers which namespace each started run belongs to, so a
	// finish can take it out of the right namespace's in-flight set.
	namespaces map[string]string
}

func (c *clusterStarter) StartRun(
	ctx context.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan engine.RunPlan,
) error {
	if err := c.fakeStarter.StartRun(ctx, runID, wf, input, plan); err != nil {
		return err
	}
	ns := artifacts.ResolveNamespace(wf.GetNamespace())
	run := &orkstackv1.Run{
		Id:             runID,
		Namespace:      ns,
		OrkflowName:    wf.GetName(),
		OrkflowVersion: wf.GetVersion(),
		Status:         orkstackv1.Status_STATUS_RUNNING,
		CreatedAt:      timestamppb.Now(),
	}
	c.reader.setRun(run)
	c.reader.mu.Lock()
	defer c.reader.mu.Unlock()
	if c.reader.executions == nil {
		c.reader.executions = map[string]engine.Executions{}
	}
	current := c.reader.executions[ns]
	current.Running = append(current.Running, run)
	c.reader.executions[ns] = current
	if c.namespaces == nil {
		c.namespaces = map[string]string{}
	}
	c.namespaces[runID] = ns
	return nil
}

// finish moves a run out of flight, as the cluster does when it completes.
func (c *clusterStarter) finish(runID string) {
	c.reader.mu.Lock()
	defer c.reader.mu.Unlock()
	ns := c.namespaces[runID]
	current := c.reader.executions[ns]
	kept := current.Running[:0]
	for _, run := range current.Running {
		if run.GetId() != runID {
			kept = append(kept, run)
		}
	}
	current.Running = kept
	c.reader.executions[ns] = current
	if run, ok := c.reader.runs[runID]; ok {
		run.Status = orkstackv1.Status_STATUS_SUCCEEDED
	}
}

// countingPools records every pool the plan looked at, so a test can prove
// the gate refused before routing began.
type countingPools struct {
	mu      sync.Mutex
	queried []string
}

func (p *countingPools) ObservePool(_ context.Context, queue string) (engine.PoolLiveness, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queried = append(p.queried, queue)
	return engine.PoolLiveness{Orkers: 1, LastSeen: time.Now()}, nil
}

func (p *countingPools) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queried)
}

// syncBuffer is a log sink safe to write from the request goroutine and read
// from the test's.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// admissionFixture is one engine replica over a registry, a reader that
// behaves like the cluster's index, a shared claimer, and the refusal seam,
// served over real h2c so Connect codes and the trigger adapter are the
// production ones.
type admissionFixture struct {
	*fixture
	registry *fakeRegistry
	reader   *fakeReader
	cluster  *clusterStarter
	claimer  *sharedClaimer
	pools    *countingPools
	logs     *syncBuffer
}

// newAdmissionFixture builds the replica over two namespaces: payments, whose
// telemetry policy selects sink b with attribute team=payments, and billing.
// configure, when given, edits the Config before the Service is built.
func newAdmissionFixture(t *testing.T, configure ...func(*api.Config)) *admissionFixture {
	t.Helper()
	registry := newFakeRegistry(
		&orkstackv1.Namespace{
			Name: "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{
				Sink:       "b",
				Attributes: map[string]string{"team": "payments"},
			},
		},
		&orkstackv1.Namespace{Name: "billing"},
	)
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{}, executions: map[string]engine.Executions{}}
	cluster := &clusterStarter{reader: reader}
	claimer := newSharedClaimer()
	pools := &countingPools{}
	logs := &syncBuffer{}
	bus := engine.NewEventBus()
	runs := engine.NewRunStore()
	var counter int
	var counterMu sync.Mutex
	cfg := api.Config{
		Orkflows:       engine.NewMemoryOrkflowStore(),
		Namespaces:     registry,
		Runs:           runs,
		Bus:            bus,
		Reader:         reader,
		Starter:        cluster,
		Pools:          pools,
		Idempotency:    claimer,
		TelemetrySinks: []string{"b"},
		Logger:         slog.New(slog.NewTextHandler(logs, nil)),
		NewRunID: func() string {
			counterMu.Lock()
			defer counterMu.Unlock()
			counter++
			return fmt.Sprintf("run-%03d", counter)
		},
	}
	for _, edit := range configure {
		edit(&cfg)
	}
	svc := api.NewService(cfg)

	server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	f := &admissionFixture{
		fixture: &fixture{
			svc:     svc,
			bus:     bus,
			runs:    runs,
			starter: &cluster.fakeStarter,
			baseURL: "http://" + listener.Addr().String(),
			client:  &http.Client{Transport: transport},
		},
		registry: registry,
		reader:   reader,
		cluster:  cluster,
		claimer:  claimer,
		pools:    pools,
		logs:     logs,
	}
	if _, err := f.orkflowClient().ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: reconcileYAML})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return f
}

func (f *admissionFixture) namespaceClient() orkstackv1connect.NamespaceServiceClient {
	return orkstackv1connect.NewNamespaceServiceClient(f.client, f.baseURL)
}

// edit is the read-edit-write every client performs: GetNamespace, change the
// document, UpdateNamespace under the etag it read.
func (f *admissionFixture) edit(t *testing.T, name string, change func(*orkstackv1.Namespace)) *orkstackv1.Namespace {
	t.Helper()
	ctx := context.Background()
	got, err := f.namespaceClient().GetNamespace(ctx, connect.NewRequest(&orkstackv1.GetNamespaceRequest{Name: name}))
	if err != nil {
		t.Fatalf("GetNamespace(%s): %v", name, err)
	}
	doc := got.Msg.GetNamespace()
	change(doc)
	updated, err := f.namespaceClient().UpdateNamespace(ctx,
		connect.NewRequest(&orkstackv1.UpdateNamespaceRequest{Namespace: doc}))
	if err != nil {
		t.Fatalf("UpdateNamespace(%s): %v", name, err)
	}
	return updated.Msg.GetNamespace()
}

func (f *admissionFixture) hold(t *testing.T, reason string) *orkstackv1.Namespace {
	t.Helper()
	return f.edit(t, "payments", func(ns *orkstackv1.Namespace) {
		ns.Hold = &orkstackv1.NamespaceHold{Reason: reason}
	})
}

func (f *admissionFixture) release(t *testing.T) {
	t.Helper()
	f.edit(t, "payments", func(ns *orkstackv1.Namespace) { ns.Hold = nil })
}

func (f *admissionFixture) setCap(t *testing.T, limit int32) {
	t.Helper()
	f.edit(t, "payments", func(ns *orkstackv1.Namespace) { ns.MaxRunning = limit })
}

func (f *admissionFixture) submit(key string, allowEmptyPool bool) (*orkstackv1.Run, error) {
	resp, err := f.runClient().SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName:    "reconcile",
		Namespace:      "payments",
		IdempotencyKey: key,
		AllowEmptyPool: allowEmptyPool,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}

// assertNothingBehind is the "leaves nothing behind" property prepare
// promises for every refusal: no run started, no run recorded, no key claimed.
func (f *admissionFixture) assertNothingBehind(t *testing.T, startedBefore int) {
	t.Helper()
	if started := f.starter.startedRuns(); len(started) != startedBefore {
		t.Errorf("started runs = %v, want %d: a refusal must start nothing", started, startedBefore)
	}
	if recorded := f.runs.List(engine.ListFilter{}); len(recorded) != startedBefore {
		t.Errorf("recorded runs = %d, want %d: a refusal must record nothing", len(recorded), startedBefore)
	}
}

func requireRefusal(t *testing.T, err error, code connect.Code, contains ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("submit succeeded, want %v", code)
	}
	if got := connect.CodeOf(err); got != code {
		t.Fatalf("code = %v, want %v: %v", got, code, err)
	}
	for _, want := range contains {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not carry %q", err.Error(), want)
		}
	}
}

func TestSubmitRun_RefusedByHoldLeavesNothingBehind(t *testing.T) {
	f := newAdmissionFixture(t)
	held := f.hold(t, heldReason)
	since := held.GetHold().GetSince().AsTime().UTC().Format(time.RFC3339)

	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition,
		`namespace "payments" is on hold since `+since+`: "partner CI loop, INC-4412". `+
			"Release it with `orkctl namespace update payments --release-hold`")
	f.assertNothingBehind(t, 0)

	listed, err := f.runClient().ListRuns(context.Background(),
		connect.NewRequest(&orkstackv1.ListRunsRequest{Namespace: "payments"}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(listed.Msg.GetRuns()) != 0 {
		t.Errorf("ListRuns(payments) = %v, want none", listed.Msg.GetRuns())
	}

	_, err = f.submit("order-42", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, heldReason)
	if proposed, _ := f.claimer.counts(); proposed != 0 {
		t.Errorf("claimer proposed %d candidates, want 0: a refused key must not be claimed", proposed)
	}
	f.assertNothingBehind(t, 0)
}

func TestSubmitRun_RefusedAtMaxRunningLeavesNothingBehind(t *testing.T) {
	f := newAdmissionFixture(t)
	f.setCap(t, 2)
	for i := 0; i < 2; i++ {
		if _, err := f.submit("", false); err != nil {
			t.Fatalf("submit %d under the cap: %v", i+1, err)
		}
	}

	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, capSentence)
	f.assertNothingBehind(t, 2)

	_, err = f.submit("order-42", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, capSentence)
	if proposed, _ := f.claimer.counts(); proposed != 0 {
		t.Errorf("claimer proposed %d candidates, want 0", proposed)
	}

	// One finishes: one in flight, under a cap of 2, admitted.
	f.cluster.finish("run-001")
	if _, err := f.submit("", false); err != nil {
		t.Fatalf("submit with one run in flight under a cap of 2: %v", err)
	}
}

func TestSubmitRun_MaxRunningCountsOnlyThisNamespace(t *testing.T) {
	f := newAdmissionFixture(t)
	f.reader.executions["billing"] = engine.Executions{Running: []*orkstackv1.Run{
		{Id: "b1", Namespace: "billing"}, {Id: "b2", Namespace: "billing"}, {Id: "b3", Namespace: "billing"},
	}}
	f.setCap(t, 1)
	if _, err := f.submit("", false); err != nil {
		t.Fatalf("submit into an empty payments beside a busy billing: %v", err)
	}
	if asked := f.reader.runningCountNamespaces(); len(asked) != 1 || asked[0] != "payments" {
		t.Errorf("RunningCount asked for %v, want exactly [payments]", asked)
	}
}

func TestSubmitRun_AnUnlimitedNamespaceNeverReadsTheIndex(t *testing.T) {
	f := newAdmissionFixture(t)
	if _, err := f.submit("", false); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if asked := f.reader.runningCountNamespaces(); len(asked) != 0 {
		t.Errorf("RunningCount asked for %v, want no read for a namespace with max_running 0", asked)
	}
}

func TestSubmitRun_AnUnreadableIndexRefusesRatherThanAdmits(t *testing.T) {
	f := newAdmissionFixture(t)
	f.setCap(t, 5)
	f.reader.mu.Lock()
	f.reader.runningCountErr = errors.New("visibility store unavailable")
	f.reader.mu.Unlock()

	_, err := f.submit("", false)
	requireRefusal(t, err, connect.CodeUnavailable,
		`namespace "payments" max_running could not be checked against the execution index: `,
		"visibility store unavailable", "ask again")
	f.assertNothingBehind(t, 0)
	if lines := refusalLines(f.logs.String()); len(lines) != 0 {
		t.Errorf("an unreadable index was logged as a refusal: %v", lines)
	}
}

func TestSubmitRun_AllowEmptyPoolDoesNotBypassAdmission(t *testing.T) {
	f := newAdmissionFixture(t)
	f.hold(t, heldReason)
	_, err := f.submit("", true)
	requireRefusal(t, err, connect.CodeFailedPrecondition, heldReason)
	if calls := f.pools.calls(); calls != 0 {
		t.Errorf("the pool probe was asked %d times; the hold must refuse before any pool is looked at", calls)
	}
	f.assertNothingBehind(t, 0)
}

func TestSubmitRun_AKeyedRepeatDuringAHoldIsRefusedNotAnswered(t *testing.T) {
	f := newAdmissionFixture(t)
	first, err := f.submit("order-42", false)
	if err != nil {
		t.Fatalf("first keyed submit: %v", err)
	}
	f.hold(t, heldReason)

	_, err = f.submit("order-42", false)
	requireRefusal(t, err, connect.CodeFailedPrecondition, heldReason)

	got, err := f.runClient().GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: first.GetId(), Namespace: "payments"}))
	if err != nil {
		t.Fatalf("GetRun(%s) after a refused repeat: %v", first.GetId(), err)
	}
	if got.Msg.GetRun().GetId() != first.GetId() {
		t.Errorf("GetRun = %s, want %s", got.Msg.GetRun().GetId(), first.GetId())
	}
	listed, err := f.runClient().ListRuns(context.Background(),
		connect.NewRequest(&orkstackv1.ListRunsRequest{Namespace: "payments"}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if runs := listed.Msg.GetRuns(); len(runs) != 1 || runs[0].GetId() != first.GetId() {
		t.Errorf("ListRuns(payments) = %v, want only %s", runs, first.GetId())
	}
	if started := f.starter.startedRuns(); len(started) != 1 {
		t.Errorf("started = %v, want only the first", started)
	}
}

func TestSubmitRun_ReleasingAHoldAdmitsAgainAndStartsNothingRefusedEarlier(t *testing.T) {
	f := newAdmissionFixture(t)
	f.hold(t, heldReason)
	for i := 0; i < 2; i++ {
		if _, err := f.submit("", false); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("held submit %d = %v, want FailedPrecondition", i+1, err)
		}
	}
	f.release(t)
	if _, err := f.submit("", false); err != nil {
		t.Fatalf("submit after release: %v", err)
	}
	if started := f.starter.startedRuns(); len(started) != 1 {
		t.Errorf("started = %v, want exactly one run: a refused submit was refused, not queued", started)
	}
	if recorded := f.runs.List(engine.ListFilter{}); len(recorded) != 1 {
		t.Errorf("recorded runs = %d, want 1", len(recorded))
	}
}

func TestSubmitRun_AHoldNeverCancelsAnAcceptedRun(t *testing.T) {
	f := newAdmissionFixture(t)
	run, err := f.submit("", false)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	f.hold(t, heldReason)
	if canceled := f.starter.canceledRuns(); len(canceled) != 0 {
		t.Errorf("a hold canceled %v", canceled)
	}
	got, err := f.runClient().GetRun(context.Background(),
		connect.NewRequest(&orkstackv1.GetRunRequest{RunId: run.GetId(), Namespace: "payments"}))
	if err != nil {
		t.Fatalf("GetRun after a hold: %v", err)
	}
	if got.Msg.GetRun().GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Errorf("status = %v, want still RUNNING", got.Msg.GetRun().GetStatus())
	}
}

func TestCreateNamespace_IdempotentRepeatComparesHoldAndMaxRunning(t *testing.T) {
	f := newAdmissionFixture(t)
	ctx := context.Background()
	spec := func(reason string, limit int32) *orkstackv1.Namespace {
		ns := &orkstackv1.Namespace{Name: "ledger", MaxRunning: limit}
		if reason != "" {
			ns.Hold = &orkstackv1.NamespaceHold{Reason: reason}
		}
		return ns
	}
	create := func(ns *orkstackv1.Namespace) (*connect.Response[orkstackv1.CreateNamespaceResponse], error) {
		return f.namespaceClient().CreateNamespace(ctx, connect.NewRequest(&orkstackv1.CreateNamespaceRequest{Namespace: ns}))
	}
	if _, err := create(spec(heldReason, 20)); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	again, err := create(spec(heldReason, 20))
	if err != nil {
		t.Fatalf("identical repeat: %v", err)
	}
	if again.Msg.GetCreated() {
		t.Error("an identical repeat reported created: true, want the no-op")
	}

	_, err = create(spec(heldReason, 0))
	if connect.CodeOf(err) != connect.CodeAlreadyExists || !strings.Contains(err.Error(), "max_running is 20, not 0") {
		t.Errorf("a different cap = %v, want AlreadyExists naming max_running is 20, not 0", err)
	}
	_, err = create(spec("another reason", 20))
	if connect.CodeOf(err) != connect.CodeAlreadyExists || !strings.Contains(err.Error(), "hold") {
		t.Errorf("a different reason = %v, want AlreadyExists naming hold", err)
	}
	_, err = create(spec("", 20))
	if connect.CodeOf(err) != connect.CodeAlreadyExists ||
		!strings.Contains(err.Error(), `hold is "partner CI loop, INC-4412", not none`) {
		t.Errorf("no hold against a held namespace = %v, want AlreadyExists naming the hold", err)
	}
}

func TestUpdateNamespace_StampsSinceAndKeepsItAcrossAReasonEdit(t *testing.T) {
	f := newAdmissionFixture(t)
	forged := timestamppb.New(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	before := time.Now().Add(-time.Minute)
	held := f.edit(t, "payments", func(ns *orkstackv1.Namespace) {
		ns.Hold = &orkstackv1.NamespaceHold{Reason: heldReason, Since: forged}
	})
	since := held.GetHold().GetSince()
	if since == nil || !since.AsTime().After(before) {
		t.Fatalf("since = %v, want the registry's clock, never the client's %s", since, forged.AsTime())
	}
	edited := f.edit(t, "payments", func(ns *orkstackv1.Namespace) {
		ns.Hold.Reason = heldReason + " (vendor paged)"
		ns.Hold.Since = forged
	})
	if !edited.GetHold().GetSince().AsTime().Equal(since.AsTime()) {
		t.Errorf("since moved from %s to %s on a reason edit", since.AsTime(), edited.GetHold().GetSince().AsTime())
	}
	if edited.GetHold().GetReason() != heldReason+" (vendor paged)" {
		t.Errorf("reason = %q, want the edit", edited.GetHold().GetReason())
	}
}

func TestTriggerInvoke_IntoAHeldNamespaceIs412FailedPrecondition(t *testing.T) {
	f := newAdmissionFixture(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{
		OrkflowName: "reconcile",
		Namespace:   "payments",
	})
	held := f.hold(t, heldReason)
	since := held.GetHold().GetSince().AsTime().UTC().Format(time.RFC3339)

	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412: %s", status, body)
	}
	if code := triggerErrorCode(t, body); code != "failed_precondition" {
		t.Errorf("error.code = %q, want failed_precondition", code)
	}
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	want := holdSentence + since + `: "partner CI loop, INC-4412". ` +
		"Release it with `orkctl namespace update payments --release-hold`"
	if !strings.Contains(envelope.Error.Message, want) {
		t.Errorf("message %q does not carry the hold sentence %q", envelope.Error.Message, want)
	}
	listed, err := f.runClient().ListRuns(context.Background(),
		connect.NewRequest(&orkstackv1.ListRunsRequest{Namespace: "payments"}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(listed.Msg.GetRuns()) != 0 {
		t.Errorf("a refused invoke left runs: %v", listed.Msg.GetRuns())
	}
	f.assertNothingBehind(t, 0)
}

func refusalLines(logs string) []string {
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, `msg="run refused"`) {
			out = append(out, line)
		}
	}
	return out
}
