package api_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// countingOrkflows is the memory orkflow store with its reads counted, so a
// test can prove a refusal happened before prepare read anything.
type countingOrkflows struct {
	engine.OrkflowStore
	mu   sync.Mutex
	gets int
}

func (c *countingOrkflows) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.OrkflowStore.Get(ctx, namespace, name, version)
}

func (c *countingOrkflows) reads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets
}

// originHarness is one Service with a reader, a starter, a run store and an
// orkflow store a test can each inspect.
type originHarness struct {
	svc      *api.Service
	starter  *fakeStarter
	reader   *fakeReader
	runs     *engine.RunStore
	orkflows *countingOrkflows
	archive  *engine.RunArchiver
}

func newOriginHarness(t *testing.T) *originHarness {
	t.Helper()
	h := &originHarness{
		starter:  &fakeStarter{},
		reader:   &fakeReader{runs: map[string]*orkstackv1.Run{}},
		runs:     engine.NewRunStore(),
		orkflows: &countingOrkflows{OrkflowStore: engine.NewMemoryOrkflowStore()},
		archive:  newArchiver(t),
	}
	var counter int
	h.svc = api.NewService(api.Config{
		Orkflows: h.orkflows,
		Reader:   h.reader,
		Runs:     h.runs,
		Bus:      engine.NewEventBus(),
		Starter:  h.starter,
		Archive:  h.archive,
		NewRunID: func() string {
			counter++
			return fmt.Sprintf("ork-origin-%d", counter)
		},
	})
	if _, err := h.svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return h
}

func (h *originHarness) submit(msg *orkstackv1.SubmitRunRequest, principal string) (*orkstackv1.Run, error) {
	req := connect.NewRequest(msg)
	if principal != "" {
		req.Header().Set(api.AuditPrincipalHeader, principal)
	}
	resp, err := h.svc.SubmitRun(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRun(), nil
}

func TestSubmitRun_RecordsSubmitOriginWithReferenceAndPrincipal(t *testing.T) {
	h := newOriginHarness(t)
	const ref = "https://github.com/acme/svc/actions/runs/123"
	run, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", OriginReference: ref}, "ci-github")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	want := &orkstackv1.RunOrigin{
		Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT, Reference: ref, Principal: "ci-github",
	}
	if !proto.Equal(run.GetOrigin(), want) {
		t.Fatalf("SubmitRunResponse.run.origin = %v, want %v", run.GetOrigin(), want)
	}
	plans := h.starter.startedPlans()
	if len(plans) != 1 || !proto.Equal(plans[0].Origin.Proto(), want) {
		t.Fatalf("the plan handed to the starter carries %+v, want %v", plans, want)
	}
}

func TestSubmitRun_NoPrincipalRecordsEmptyNotUnattributed(t *testing.T) {
	h := newOriginHarness(t)
	run, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"}, "")
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	if got := run.GetOrigin().GetPrincipal(); got != "" {
		t.Fatalf("principal = %q, want empty: no header means no assertion, never a word standing in for one", got)
	}
	if run.GetOrigin().GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT {
		t.Fatalf("kind = %v, want SUBMIT", run.GetOrigin().GetKind())
	}
}

func TestSubmitRun_ReferenceIsValidatedNotSanitized(t *testing.T) {
	for name, ref := range map[string]string{
		"257 bytes":    strings.Repeat("a", 257),
		"a newline":    "INC-1\nINC-2",
		"a DEL":        "INC\x7f",
		"invalid utf8": "INC-\xff",
	} {
		t.Run(name, func(t *testing.T) {
			h := newOriginHarness(t)
			_, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", OriginReference: ref}, "")
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("SubmitRun = %v, want InvalidArgument", err)
			}
			if name == "257 bytes" && !strings.Contains(err.Error(), "256") {
				t.Fatalf("error %q does not name the 256-byte limit", err)
			}
			if got := h.starter.startedRuns(); len(got) != 0 {
				t.Fatalf("started %v for a refused reference", got)
			}
			if got := h.runs.List(engine.ListFilter{}); len(got) != 0 {
				t.Fatalf("recorded %d runs for a refused reference", len(got))
			}
		})
	}

	t.Run("256 bytes of UTF-8 verbatim", func(t *testing.T) {
		h := newOriginHarness(t)
		ref := strings.Repeat("é", 128) // 256 bytes
		run, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", OriginReference: ref}, "")
		if err != nil {
			t.Fatalf("SubmitRun: %v", err)
		}
		if run.GetOrigin().GetReference() != ref {
			t.Fatalf("reference = %q, want it verbatim", run.GetOrigin().GetReference())
		}
	})

	t.Run("an over-long reference is refused before the source run is read", func(t *testing.T) {
		h := newOriginHarness(t)
		_, err := h.submit(&orkstackv1.SubmitRunRequest{
			OrkflowName: "hello-ork", OriginReference: strings.Repeat("a", 257), SourceRunId: "ork-nowhere",
		}, "")
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "origin reference") {
			t.Fatalf("SubmitRun = %v, want InvalidArgument naming the reference, not NotFound", err)
		}
		if reads := h.reader.runReads(); reads != 0 {
			t.Fatalf("the reader served %d run reads; the reference check must come first", reads)
		}
	})
}

func TestSubmitRun_PrincipalAndDescriptionAreBoundedByTruncation(t *testing.T) {
	h := newOriginHarness(t)
	long := strings.Repeat("p", 400)
	run, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"}, long)
	if err != nil {
		t.Fatalf("SubmitRun with a 400-byte principal: %v", err)
	}
	got := run.GetOrigin().GetPrincipal()
	if len(got) > 256 || !strings.HasSuffix(got, artifacts.AttemptTruncationMarker) {
		t.Fatalf("principal = %d bytes %q, want at most 256 ending in the truncation marker", len(got), got)
	}

	// The description is the trigger's; it reaches the run through the
	// invoke door, bounded the same way.
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{
		OrkflowName: "hello-ork", Description: strings.Repeat("d", 300),
	})
	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status != http.StatusCreated {
		t.Fatalf("invoke = %d: %s", status, body)
	}
	origin := decodeInvokeOrigin(t, body)
	if len(origin.Description) > 256 || !strings.HasSuffix(origin.Description, artifacts.AttemptTruncationMarker) {
		t.Fatalf("description = %d bytes, want at most 256 ending in the truncation marker", len(origin.Description))
	}
}

func TestSubmitRun_RunAgainRecordsSourceRunAndChecksIt(t *testing.T) {
	h := newOriginHarness(t)
	h.reader.setRun(&orkstackv1.Run{Id: "ork-source", Namespace: "default", OrkflowName: "hello-ork",
		Status: orkstackv1.Status_STATUS_SUCCEEDED})
	h.reader.setRun(&orkstackv1.Run{Id: "ork-billing", Namespace: "billing", OrkflowName: "hello-ork",
		Status: orkstackv1.Status_STATUS_SUCCEEDED})

	run, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", SourceRunId: "ork-source", OriginReference: "INC-9"}, "ops")
	if err != nil {
		t.Fatalf("run again: %v", err)
	}
	want := &orkstackv1.RunOrigin{
		Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN, Id: "ork-source", Reference: "INC-9", Principal: "ops",
	}
	if !proto.Equal(run.GetOrigin(), want) {
		t.Fatalf("origin = %v, want %v", run.GetOrigin(), want)
	}
	if run.GetId() == "ork-source" {
		t.Fatal("a re-run reused its source's id")
	}

	for name, source := range map[string]string{"another namespace's run": "ork-billing", "an unknown run": "ork-nowhere"} {
		t.Run(name, func(t *testing.T) {
			startedBefore, readsBefore := len(h.starter.startedRuns()), h.orkflows.reads()
			runsBefore := len(h.runs.List(engine.ListFilter{}))
			_, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", SourceRunId: source}, "")
			if connect.CodeOf(err) != connect.CodeNotFound || !strings.Contains(err.Error(), source) {
				t.Fatalf("run again of %s = %v, want NotFound naming it", source, err)
			}
			if len(h.starter.startedRuns()) != startedBefore {
				t.Fatal("a refused run again started a run")
			}
			if h.orkflows.reads() != readsBefore {
				t.Fatal("a refused run again read the orkflow store: the refusal must precede prepare")
			}
			if len(h.runs.List(engine.ListFilter{})) != runsBefore {
				t.Fatal("a refused run again recorded a run")
			}
		})
	}
}

// TestSubmitRun_RequestCannotClaimAKind: SubmitRunRequest has no field that
// could carry a kind, and every request the public door can receive becomes
// SUBMIT or RUN_AGAIN.
func TestSubmitRun_RequestCannotClaimAKind(t *testing.T) {
	fields := (&orkstackv1.SubmitRunRequest{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Enum() != nil && field.Enum().FullName() == "orkstack.v1.RunOriginKind" {
			t.Fatalf("SubmitRunRequest.%s carries a RunOriginKind: a client could claim a kind", field.Name())
		}
		if field.Message() != nil && field.Message().FullName() == "orkstack.v1.RunOrigin" {
			t.Fatalf("SubmitRunRequest.%s carries a RunOrigin: a client could claim a kind", field.Name())
		}
	}
	h := newOriginHarness(t)
	h.reader.setRun(&orkstackv1.Run{Id: "ork-source", Namespace: "default", OrkflowName: "hello-ork"})
	for _, msg := range []*orkstackv1.SubmitRunRequest{
		{OrkflowName: "hello-ork"},
		{OrkflowName: "hello-ork", OriginReference: "trigger:trg-x"},
		{OrkflowName: "hello-ork", SourceRunId: "ork-source"},
		{OrkflowName: "hello-ork", IdempotencyKey: "k"},
	} {
		run, err := h.submit(msg, "")
		if err != nil {
			t.Fatalf("SubmitRun(%v): %v", msg, err)
		}
		switch run.GetOrigin().GetKind() {
		case orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT, orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_RUN_AGAIN:
		default:
			t.Fatalf("SubmitRun(%v) produced kind %v; the public door makes only SUBMIT and RUN_AGAIN", msg, run.GetOrigin().GetKind())
		}
	}
}

func TestSubmitRun_KeyedRepeatReturnsTheFirstOrigin(t *testing.T) {
	h := newOriginHarness(t)
	first, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", IdempotencyKey: "nightly", OriginReference: "first"}, "")
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	second, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", IdempotencyKey: "nightly", OriginReference: "second"}, "")
	if err != nil {
		t.Fatalf("keyed repeat: %v", err)
	}
	if second.GetId() != first.GetId() {
		t.Fatalf("keyed repeat answered %s, want %s", second.GetId(), first.GetId())
	}
	if got := second.GetOrigin().GetReference(); got != "first" {
		t.Fatalf("keyed repeat reports reference %q, want the first run's", got)
	}
	for _, plan := range h.starter.startedPlans() {
		if plan.Origin.Reference == "second" {
			t.Fatal("the repeat's reference reached a plan")
		}
	}
}

func TestSubmitRun_FailedStartArchivesTheOrigin(t *testing.T) {
	h := newOriginHarness(t)
	h.starter.err = errors.New("temporal is down")
	_, err := h.submit(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork", OriginReference: "INC-7"}, "ops")
	if err == nil {
		t.Fatal("SubmitRun succeeded against a failing starter")
	}
	stored := h.runs.List(engine.ListFilter{})
	if len(stored) != 1 || stored[0].GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run store = %v, want one failed run", stored)
	}
	if got := stored[0].GetOrigin(); got.GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT || got.GetReference() != "INC-7" {
		t.Fatalf("the failed start's run carries origin %v", got)
	}
	archived, err := h.archive.ListRuns(context.Background(), engine.ArchiveQuery{
		Namespace: "default", OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT,
	})
	if err != nil {
		t.Fatalf("archive list by origin: %v", err)
	}
	if len(archived) != 1 || archived[0].GetId() != stored[0].GetId() ||
		archived[0].GetOrigin().GetReference() != "INC-7" {
		t.Fatalf("archived by origin = %v, want the failed start with its origin", archived)
	}
}

type invokeOrigin struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Reference   string `json:"reference"`
	Description string `json:"description"`
	Principal   string `json:"principal"`
}

func decodeInvokeOrigin(t *testing.T, body []byte) invokeOrigin {
	t.Helper()
	var response struct {
		Run struct {
			Origin *invokeOrigin `json:"origin"`
		} `json:"run"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode invoke response: %v (%s)", err, body)
	}
	if response.Run.Origin == nil {
		t.Fatalf("the invoke response's run carries no origin: %s", body)
	}
	return *response.Run.Origin
}

func TestTriggerInvoke_RecordsTriggerOriginFromRecordAndHeader(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{
		OrkflowName: "hello-ork", Description: "GitHub deploy hook",
	})
	const ref = "https://github.com/acme/svc/actions/runs/123"
	// A body key named like the header is input data, never the reference.
	status, body := f.invoke(t, trigger.GetId(), bearer(secret),
		`{"url":"https://example.test/x","Ork-Origin-Reference":"from-the-body","origin_reference":"from-the-body"}`,
		map[string]string{api.OriginReferenceHeader: ref, api.AuditPrincipalHeader: "ci-github"})
	if status != http.StatusCreated {
		t.Fatalf("invoke = %d: %s", status, body)
	}
	got := decodeInvokeOrigin(t, body)
	want := invokeOrigin{Kind: "TRIGGER", ID: trigger.GetId(), Reference: ref, Description: "GitHub deploy hook", Principal: "ci-github"}
	if got != want {
		t.Fatalf("run.origin = %+v, want %+v", got, want)
	}
	plans := f.starter.startedPlans()
	if len(plans) != 1 || plans[0].Origin == nil || plans[0].Origin.Kind != "RUN_ORIGIN_KIND_TRIGGER" || plans[0].Origin.Reference != ref {
		t.Fatalf("the started plan carries %+v", plans)
	}
	inputs := f.starter.startedInputs()
	if len(inputs) != 1 || inputs[0]["origin_reference"] != "from-the-body" {
		t.Fatalf("the body did not reach the run as input unchanged: %v", inputs)
	}
}

func TestTriggerInvoke_MalformedReferenceIs400AndStartsNothing(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`,
		map[string]string{api.OriginReferenceHeader: strings.Repeat("r", 257)})
	if status != http.StatusBadRequest || triggerErrorCode(t, body) != "invalid_argument" {
		t.Fatalf("invoke = %d %s, want 400 invalid_argument", status, body)
	}
	if got := f.starter.startedRuns(); len(got) != 0 {
		t.Fatalf("started %v", got)
	}
	listed, err := f.runClient().ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(listed.Msg.GetRuns()) != 0 {
		t.Fatalf("ListRuns shows %d runs after a refused invoke", len(listed.Msg.GetRuns()))
	}
}

func TestTriggerInvoke_UnauthenticatedNeverReachesTheReference(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, _ := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	status, body := f.invoke(t, trigger.GetId(), bearer("wrong"), `{}`,
		map[string]string{api.OriginReferenceHeader: strings.Repeat("r", 257)})
	if status != http.StatusUnauthorized {
		t.Fatalf("invoke with a wrong token and a malformed reference = %d %s, want 401", status, body)
	}
}

func TestListRuns_OriginFilterMergesLiveAndArchive(t *testing.T) {
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	trigger := func(id string) *orkstackv1.RunOrigin {
		return &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: id}
	}
	run := func(id string, minute int, status orkstackv1.Status, origin *orkstackv1.RunOrigin) *orkstackv1.Run {
		return &orkstackv1.Run{
			Id: id, Namespace: "default", OrkflowName: "hello-ork", OrkflowVersion: "0.2.0",
			Status: status, CreatedAt: timestamppb.New(at.Add(time.Duration(minute) * time.Minute)), Origin: origin,
		}
	}
	archiver := newArchiver(t)
	for _, archived := range []*orkstackv1.Run{
		run("ork-a1", 1, orkstackv1.Status_STATUS_SUCCEEDED, trigger("trg-1")),
		run("ork-a2", 2, orkstackv1.Status_STATUS_FAILED, trigger("trg-1")),
		run("ork-a3", 3, orkstackv1.Status_STATUS_SUCCEEDED, &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}),
		run("ork-legacy", 4, orkstackv1.Status_STATUS_SUCCEEDED, nil),
	} {
		if err := archiver.Archive(context.Background(), archived, nil); err != nil {
			t.Fatalf("Archive(%s): %v", archived.GetId(), err)
		}
	}
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{}}
	// Two live runs of the trigger, and one finished run visibility still
	// lists and the archive also holds: it must appear once.
	reader.setRun(run("ork-l1", 5, orkstackv1.Status_STATUS_RUNNING, trigger("trg-1")))
	reader.setRun(run("ork-l2", 6, orkstackv1.Status_STATUS_RUNNING, trigger("trg-1")))
	reader.setRun(run("ork-a2", 2, orkstackv1.Status_STATUS_FAILED, trigger("trg-1")))
	reader.setRun(run("ork-l3", 7, orkstackv1.Status_STATUS_RUNNING, trigger("trg-2")))
	svc := api.NewService(api.Config{Reader: reader, Runs: engine.NewRunStore(), Archive: archiver})

	list := func(req *orkstackv1.ListRunsRequest) []string {
		t.Helper()
		resp, err := svc.ListRuns(context.Background(), connect.NewRequest(req))
		if err != nil {
			t.Fatalf("ListRuns(%v): %v", req, err)
		}
		ids := []string{}
		for _, r := range resp.Msg.GetRuns() {
			ids = append(ids, r.GetId())
		}
		return ids
	}
	got := list(&orkstackv1.ListRunsRequest{Filters: filters(t, "origin.kind=TRIGGER", "origin.id=trg-1")})
	if want := []string{"ork-l2", "ork-l1", "ork-a2", "ork-a1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trigger:trg-1 = %v, want %v newest first with no duplicate", got, want)
	}
	got = list(&orkstackv1.ListRunsRequest{Filters: filters(t, "origin.kind=SUBMIT")})
	if want := []string{"ork-a3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("submit = %v, want %v", got, want)
	}
	got = list(&orkstackv1.ListRunsRequest{Filters: filters(t, "origin.kind=TRIGGER")})
	if want := []string{"ork-l3", "ork-l2", "ork-l1", "ork-a2", "ork-a1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trigger = %v, want %v", got, want)
	}
	for _, q := range reader.runQueries() {
		if q.OriginKind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
			t.Fatalf("the reader was asked %+v without the origin filter", q)
		}
	}
}

func TestListRuns_CursorBindsOriginFilter(t *testing.T) {
	runs := engine.NewRunStore()
	at := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	for _, id := range []string{"ork-a", "ork-b", "ork-c"} {
		runs.Save(&orkstackv1.Run{Id: id, OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			CreatedAt: timestamppb.New(at), Origin: &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "x"}})
	}
	svc := api.NewService(api.Config{Runs: runs})
	filtered := &orkstackv1.ListRunsRequest{PageSize: 2, Filters: filters(t, "origin.kind=TRIGGER", "origin.id=x")}
	first, err := svc.ListRuns(context.Background(), connect.NewRequest(filtered))
	if err != nil || first.Msg.GetNextPageToken() == "" {
		t.Fatalf("first filtered page = %v, %v", first, err)
	}
	next := proto.Clone(filtered).(*orkstackv1.ListRunsRequest)
	next.PageToken = first.Msg.GetNextPageToken()
	second, err := svc.ListRuns(context.Background(), connect.NewRequest(next))
	if err != nil || len(second.Msg.GetRuns()) != 1 || second.Msg.GetRuns()[0].GetId() != "ork-a" {
		t.Fatalf("second filtered page = %v, %v", second, err)
	}
	for name, changed := range map[string]*orkstackv1.ListRunsRequest{
		"filter dropped": {PageSize: 2, PageToken: first.Msg.GetNextPageToken()},
		"id changed":     {PageSize: 2, PageToken: first.Msg.GetNextPageToken(), Filters: filters(t, "origin.kind=TRIGGER", "origin.id=y")},
		"kind changed":   {PageSize: 2, PageToken: first.Msg.GetNextPageToken(), Filters: filters(t, "origin.kind=SUBMIT")},
	} {
		if _, err := svc.ListRuns(context.Background(), connect.NewRequest(changed)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s: token replayed = %v, want InvalidArgument", name, err)
		}
	}

	// A version-1 token — the typed filters' shape — is refused, never read
	// as an unfiltered position.
	legacy := map[string]any{"v": 1, "n": "default", "o": "", "s": 0, "c": at.Format(time.RFC3339Nano), "i": "ork-c"}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	_, err = svc.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{PageSize: 2, PageToken: token}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "invalid page_token") {
		t.Fatalf("a version-1 token = %v, want invalid page_token", err)
	}
}

func TestWebhookPayload_EmbeddedRunCarriesOrigin(t *testing.T) {
	f := newNotifierFixture(t)
	if _, err := f.registry.Register(webhooks.Registration{
		URL: f.server.URL, Events: []webhooks.Event{webhooks.EventRunSucceeded}, Secret: "s3cr3t",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	run := &orkstackv1.Run{
		Id: "ork-1", OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Origin: &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1", Reference: "https://ci.example/1"},
	}
	f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{
		RunId: run.GetId(), Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Timestamp: timestamppb.Now(),
	}, run)
	body, _ := f.awaitDelivery(t)
	var payload struct {
		Run struct {
			Origin map[string]string `json:"origin"`
		} `json:"run"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode payload: %v (%s)", err, body)
	}
	if payload.Run.Origin["kind"] != "TRIGGER" || payload.Run.Origin["id"] != "trg-1" || payload.Run.Origin["reference"] != "https://ci.example/1" {
		t.Fatalf("delivered run.origin = %v, want kind TRIGGER, id trg-1 and the reference", payload.Run.Origin)
	}
}

func TestJSON_OriginKindRendersShort(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-1", Origin: &orkstackv1.RunOrigin{
		Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-parent",
	}}
	encoded, err := api.MarshalJSON(run)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"kind":"PARENT_RUN"`) {
		t.Fatalf("encoded %s, want the short enum", encoded)
	}
	if strings.Contains(string(encoded), `"reference"`) || strings.Contains(string(encoded), `"principal"`) {
		t.Fatalf("encoded %s, want empty strings omitted", encoded)
	}
	for _, spelling := range []string{`"PARENT_RUN"`, `"RUN_ORIGIN_KIND_PARENT_RUN"`} {
		decoded := &orkstackv1.Run{}
		if err := api.UnmarshalJSON([]byte(`{"id":"ork-1","origin":{"kind":`+spelling+`,"id":"ork-parent"}}`), decoded); err != nil {
			t.Fatalf("UnmarshalJSON(%s): %v", spelling, err)
		}
		if !proto.Equal(decoded, run) {
			t.Fatalf("UnmarshalJSON(%s) = %v, want %v", spelling, decoded, run)
		}
	}
	legacy, err := api.MarshalJSON(&orkstackv1.Run{Id: "ork-2"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(legacy), "origin") {
		t.Fatalf("a run with no origin encodes %s, want no origin object", legacy)
	}
}
