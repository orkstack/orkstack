package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// eachReads is the fan-out read fixture: the evidence fixture's production
// wiring (reader, archive, local store, attempt and commit stores) plus a log
// store, holding one finished run whose `convert` fanned out over 3 items,
// every item recording an artifact under the SAME name.
type eachReads struct {
	*evidenceFixture
	logs  *memoryLogStore
	run   *orkstackv1.Run
	parts []*orkstackv1.ArtifactRef
}

const eachRunID = "ork-each-reads"

func newEachReads(t *testing.T) *eachReads {
	t.Helper()
	logs := &memoryLogStore{}
	f := newEvidenceFixture(t, withConfig(func(cfg *api.Config) { cfg.Logs = logs }))
	e := &eachReads{evidenceFixture: f, logs: logs}
	at := evidenceNow.Add(-time.Hour)
	node := &orkstackv1.TaskRun{
		TaskId: "convert", Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main",
		Each:        &orkstackv1.TaskEach{Expanded: true, Count: 3},
		ScheduledAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
	}
	for i := int32(0); i < 3; i++ {
		part := f.put("part.txt", []byte(fmt.Sprintf("part %d\n", i)))
		e.parts = append(e.parts, part)
		values := f.put("values.json", []byte(fmt.Sprintf(`{"i":%d}`, i)))
		key, _ := artifacts.ItemCommitKey("default", eachRunID, "convert", i)
		uri := "mem://commits/" + key
		rec, err := f.commits.Create(context.Background(), uri, &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: eachRunID, TaskId: "convert",
			OrkflowDefinitionDigest: f.digest, Values: values, Artifacts: []*orkstackv1.ArtifactRef{part},
		})
		if err != nil {
			t.Fatal(err)
		}
		attempt := int32(1)
		if i == 1 {
			attempt = 2
		}
		node.Items = append(node.Items, &orkstackv1.TaskRun{
			TaskId: "convert", Item: proto.Int32(i), Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Attempt: attempt, OrkerId: "orker-1", Pool: "main", ExitCode: 0,
			Artifacts: []*orkstackv1.ArtifactRef{part}, ValuesRef: values,
			Commit:      &orkstackv1.CommitRef{Uri: uri, Sha256: rec.SHA256, Etag: rec.ETag},
			ScheduledAt: timestamppb.New(at), StartedAt: timestamppb.New(at.Add(time.Second)),
			FinishedAt: timestamppb.New(at.Add(time.Duration(10+i) * time.Second)),
		})
		// Item 1 retried: attempt 2 has a record, attempt 1 left none (the
		// UNREPORTED gap); every other item has its one record.
		record := &orkstackv1.TaskAttempt{
			SchemaVersion: artifacts.AttemptSchemaVersion, RunId: eachRunID, TaskId: "convert",
			Attempt: attempt, Item: proto.Int32(i), Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
		}
		if err := f.attempts.PutAttempt(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		logs.write(artifacts.LogRef{RunID: eachRunID, TaskID: "convert", Item: artifacts.ItemAt(i), Attempt: attempt}, 1,
			fmt.Sprintf("item %d says hello\n", i))
	}
	e.run = f.finish(&orkstackv1.Run{Id: eachRunID, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "list", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, OrkerId: "orker-1"},
		node,
	}})
	return e
}

func item(i int32) *int32 { return &i }

func TestItemSelectorRules(t *testing.T) {
	e := newEachReads(t)
	ctx := context.Background()
	type call func(task string, item *int32) error
	calls := map[string]call{
		"ListRunArtifacts": func(task string, it *int32) error {
			_, err := e.client.ListRunArtifacts(ctx, connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{RunId: eachRunID, TaskId: task, Item: it}))
			return err
		},
		"ReadArtifact": func(task string, it *int32) error {
			stream, err := e.client.ReadArtifact(ctx, connect.NewRequest(&orkstackv1.ReadArtifactRequest{
				RunId: eachRunID, TaskId: task, Item: it, Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "part.txt"}))
			if err != nil {
				return err
			}
			for stream.Receive() {
			}
			return stream.Err()
		},
		"StreamTaskLogs": func(task string, it *int32) error {
			stream, err := e.client.StreamTaskLogs(ctx, connect.NewRequest(&orkstackv1.StreamTaskLogsRequest{RunId: eachRunID, TaskId: task, Item: it}))
			if err != nil {
				return err
			}
			for stream.Receive() {
			}
			return stream.Err()
		},
		"ListTaskAttempts": func(task string, it *int32) error {
			_, err := e.client.ListTaskAttempts(ctx, connect.NewRequest(&orkstackv1.ListTaskAttemptsRequest{RunId: eachRunID, TaskId: task, Item: it}))
			return err
		},
	}
	for name, c := range calls {
		t.Run(name, func(t *testing.T) {
			// Rule 1. StreamTaskLogs and ReadArtifact require a task anyway.
			if err := c("", item(0)); connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("rule 1: %v", err)
			}
			// Rule 2.
			if err := c("convert", item(-1)); connect.CodeOf(err) != connect.CodeInvalidArgument ||
				!strings.Contains(err.Error(), "items are numbered from 0") {
				t.Fatalf("rule 2: %v", err)
			}
			// Rule 3.
			if err := c("list", item(0)); connect.CodeOf(err) != connect.CodeInvalidArgument ||
				!strings.Contains(err.Error(), `task "list" is not a fan-out`) {
				t.Fatalf("rule 3: %v", err)
			}
			// Rule 4.
			if err := c("convert", item(3)); connect.CodeOf(err) != connect.CodeNotFound ||
				!strings.Contains(err.Error(), "has 3 items (0–2)") {
				t.Fatalf("rule 4: %v", err)
			}
			// Rule 5.
			err := c("convert", nil)
			switch name {
			case "ListRunArtifacts", "ListTaskAttempts":
				if err != nil {
					t.Fatalf("rule 5 (every item): %v", err)
				}
			default:
				if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "fan-out of 3 items") {
					t.Fatalf("rule 5 (name one): %v", err)
				}
			}
			// And a real item answers.
			if err := c("convert", item(1)); err != nil {
				t.Fatalf("item 1: %v", err)
			}
		})
	}
}

func TestListRunArtifacts_EveryItemCarriesItsItem(t *testing.T) {
	e := newEachReads(t)
	resp, err := e.client.ListRunArtifacts(context.Background(), connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{RunId: eachRunID, TaskId: "convert"}))
	if err != nil {
		t.Fatal(err)
	}
	var outputs []string
	for _, a := range resp.Msg.GetArtifacts() {
		if a.Item == nil {
			t.Fatalf("an item artifact carries no item: %v", a)
		}
		if a.GetKind() == orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT {
			outputs = append(outputs, fmt.Sprintf("%d@%d", a.GetItem(), a.GetAttempt()))
		}
	}
	if fmt.Sprint(outputs) != "[0@1 1@2 2@1]" {
		t.Fatalf("outputs = %v, want items ascending with their own attempts", outputs)
	}
	one, err := e.client.ListRunArtifacts(context.Background(), connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{RunId: eachRunID, TaskId: "convert", Item: item(2)}))
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range one.Msg.GetArtifacts() {
		if a.GetItem() != 2 {
			t.Fatalf("item 2 listed %v", a)
		}
	}
}

func TestReadArtifact_FanOutNeedsAnItem(t *testing.T) {
	e := newEachReads(t)
	read := func(it *int32) (string, error) {
		stream, err := e.client.ReadArtifact(context.Background(), connect.NewRequest(&orkstackv1.ReadArtifactRequest{
			RunId: eachRunID, TaskId: "convert", Item: it, Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "part.txt"}))
		if err != nil {
			return "", err
		}
		var b strings.Builder
		for stream.Receive() {
			b.Write(stream.Msg().GetData())
		}
		return b.String(), stream.Err()
	}
	if _, err := read(nil); connect.CodeOf(err) != connect.CodeInvalidArgument ||
		!strings.Contains(err.Error(), "orkctl run artifact download … --item N") {
		t.Fatalf("no item = %v", err)
	}
	got, err := read(item(2))
	if err != nil || got != "part 2\n" {
		t.Fatalf("item 2 read %q, %v — two items record the same name, and the item decides which", got, err)
	}
}

func TestStreamTaskLogs_StreamsOneItemAndEchoesIt(t *testing.T) {
	e := newEachReads(t)
	stream, err := e.client.StreamTaskLogs(context.Background(), connect.NewRequest(&orkstackv1.StreamTaskLogsRequest{RunId: eachRunID, TaskId: "convert", Item: item(1)}))
	if err != nil {
		t.Fatal(err)
	}
	var body strings.Builder
	for stream.Receive() {
		chunk := stream.Msg()
		if chunk.Item == nil || chunk.GetItem() != 1 {
			t.Fatalf("chunk does not echo the item: %v", chunk)
		}
		if chunk.GetAttempt() != 2 && !chunk.GetLast() {
			t.Fatalf("chunk of attempt %d; item 1's latest is 2", chunk.GetAttempt())
		}
		body.Write(chunk.GetData())
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if body.String() != "item 1 says hello\n" {
		t.Fatalf("body = %q", body.String())
	}

	// The socket: item= with the attempt= parsing rules.
	server := httpServerFor(t, e.svc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := strings.Replace(server, "http://", "ws://", 1) + "/ws/runs/" + eachRunID + "/tasks/convert/logs?item=2"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	_, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	chunk := &orkstackv1.TaskLogChunk{}
	if err := protojson.Unmarshal(payload, chunk); err != nil {
		t.Fatal(err)
	}
	if chunk.GetItem() != 2 || string(chunk.GetData()) != "item 2 says hello\n" {
		t.Fatalf("socket chunk = %v", chunk)
	}
	bad := strings.Replace(server, "http://", "ws://", 1) + "/ws/runs/" + eachRunID + "/tasks/convert/logs?item=-1"
	if _, resp, err := websocket.Dial(ctx, bad, nil); err == nil || resp == nil || resp.StatusCode != 400 {
		t.Fatalf("item=-1 = %v", err)
	}
}

func httpServerFor(t *testing.T, svc *api.Service) string {
	t.Helper()
	srv := httptest.NewServer(api.NewMux(svc, api.MuxOptions{}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestListTaskAttempts_OneTimelinePerItemInIndexOrder(t *testing.T) {
	e := newEachReads(t)
	resp, err := e.client.ListTaskAttempts(context.Background(), connect.NewRequest(&orkstackv1.ListTaskAttemptsRequest{RunId: eachRunID, TaskId: "convert"}))
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, a := range resp.Msg.GetAttempts() {
		if a.Item == nil {
			t.Fatalf("an item record carries no item: %v", a)
		}
		rows = append(rows, fmt.Sprintf("%d#%d=%s", a.GetItem(), a.GetAttempt(),
			strings.TrimPrefix(a.GetOutcome().String(), "ATTEMPT_OUTCOME_")))
	}
	want := "[0#1=SUCCEEDED 1#1=UNREPORTED 1#2=SUCCEEDED 2#1=SUCCEEDED]"
	if fmt.Sprint(rows) != want {
		t.Fatalf("timeline = %v, want %s", rows, want)
	}
}

func TestGetRun_CarriesItems(t *testing.T) {
	e := newEachReads(t)
	resp, err := e.client.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: eachRunID}))
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range resp.Msg.GetRun().GetTaskRuns() {
		if task.GetTaskId() == "convert" && len(task.GetItems()) != 3 {
			t.Fatalf("GetRun carries %d items", len(task.GetItems()))
		}
	}
}

func TestListRuns_CarriesNoItems(t *testing.T) {
	e := newEachReads(t)
	resp, err := e.client.ListRuns(context.Background(), connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, run := range resp.Msg.GetRuns() {
		for _, task := range run.GetTaskRuns() {
			if task.GetTaskId() == "convert" {
				found = true
				if len(task.GetItems()) != 0 || task.GetEach().GetCount() != 3 {
					t.Fatalf("a list row = %v, want each and status without items", task)
				}
			}
		}
	}
	if !found {
		t.Fatalf("the fan-out run is not on the page: %v", resp.Msg.GetRuns())
	}
}

func TestWebhookNotifier_BodyCarriesNoItems(t *testing.T) {
	f := newNotifierFixture(t)
	if _, err := f.registry.Register(webhooks.Registration{
		URL: f.server.URL, Events: []webhooks.Event{webhooks.EventRunSucceeded}, Secret: "s",
	}); err != nil {
		t.Fatal(err)
	}
	run := &orkstackv1.Run{Id: "ork-each-hook", OrkflowName: "fan", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "convert", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Each:  &orkstackv1.TaskEach{Expanded: true, Count: 1},
			Items: []*orkstackv1.TaskRun{{TaskId: "convert", Item: proto.Int32(0)}}}}}
	f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{RunId: run.GetId(),
		Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, Status: orkstackv1.Status_STATUS_SUCCEEDED}, run)
	body, _ := f.awaitDelivery(t)
	var payload struct {
		Run json.RawMessage `json:"run"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload.Run), `"items"`) || !strings.Contains(string(payload.Run), `"each"`) {
		t.Fatalf("the delivery body = %s, want each without items", payload.Run)
	}
	if len(run.GetTaskRuns()[0].GetItems()) != 1 {
		t.Fatal("the notifier mutated the caller's run")
	}
}

func TestInspectRun_ItemAttempts(t *testing.T) {
	run := inspectRun("ork-each-live", orkstackv1.Status_STATUS_RUNNING, "list", "convert")
	run.TaskRuns[1].Each = &orkstackv1.TaskEach{Expanded: true, Count: 4}
	for i := int32(0); i < 4; i++ {
		run.TaskRuns[1].Items = append(run.TaskRuns[1].Items, &orkstackv1.TaskRun{TaskId: "convert", Item: proto.Int32(i)})
	}
	live := engine.LiveRun{Attempts: []*orkstackv1.LiveAttempt{
		{TaskId: "convert", Item: proto.Int32(3)},
		{TaskId: "convert", Item: proto.Int32(1)},
		{TaskId: "list"},
	}}
	f := newInspectFixture(t, run, live, nil)
	resp, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, a := range resp.GetAttempts() {
		order = append(order, fmt.Sprintf("%s%v", a.GetTaskId(), itemSuffix(a.Item)))
	}
	if fmt.Sprint(order) != "[list convert[1] convert[3]]" {
		t.Fatalf("order = %v", order)
	}
	narrowed, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId(), TaskId: "convert", Item: item(3)})
	if err != nil || len(narrowed.GetAttempts()) != 1 || narrowed.GetAttempts()[0].GetItem() != 3 {
		t.Fatalf("narrowed = %v, %v", narrowed, err)
	}
	if _, err := f.inspect(t, &orkstackv1.InspectRunRequest{RunId: run.GetId(), Item: item(3)}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("item without task = %v", err)
	}
}

func itemSuffix(item *int32) string {
	if item == nil {
		return ""
	}
	return fmt.Sprintf("[%d]", *item)
}

func TestDiffRuns_ComparesItemsPairwise(t *testing.T) {
	e := newEachReads(t)
	other := proto.Clone(e.run).(*orkstackv1.Run)
	other.Id = "ork-each-reads-2"
	node := other.GetTaskRuns()[1]
	node.Items[2].Status = orkstackv1.Status_STATUS_FAILED
	node.Items[2].Error = &orkstackv1.Error{Code: "CONTAINER_EXIT_NONZERO"}
	node.Items = append(node.Items, &orkstackv1.TaskRun{TaskId: "convert", Item: proto.Int32(3), Status: orkstackv1.Status_STATUS_SUCCEEDED})
	node.Each.Count = 4
	e.reader.setRun(other)
	resp, err := e.client.DiffRuns(context.Background(), connect.NewRequest(&orkstackv1.DiffRunsRequest{
		FromRunId: eachRunID, ToRunId: other.GetId(), Only: []string{"status", "error", "attempt", "orker"}}))
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, d := range resp.Msg.GetDifferences() {
		rows = append(rows, fmt.Sprintf("%s%s.%s", d.GetTaskId(), itemSuffix(d.Item), d.GetField()))
	}
	want := "[convert[2].status convert[2].error convert[3].presence]"
	if fmt.Sprint(rows) != want {
		t.Fatalf("rows = %v, want %s (the node's own attempt/orker are never compared)", rows, want)
	}
	if resp.Msg.GetTasksCompared() != 2 || resp.Msg.GetTasksIdentical() != 1 {
		t.Fatalf("compared %d, identical %d; want 2 and 1 (task ids, not items)",
			resp.Msg.GetTasksCompared(), resp.Msg.GetTasksIdentical())
	}
}

func TestGetArtifactProvenance_ItemReference(t *testing.T) {
	f := newProvenanceFixture(t)
	part0 := f.put(t, "part.txt", "zero\n")
	part1 := f.put(t, "part.txt", "one\n")
	run := f.newRun("ork-each-prov", "default", t0)
	run.TaskRuns = []*orkstackv1.TaskRun{{TaskId: "convert", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Each: &orkstackv1.TaskEach{Expanded: true, Count: 2},
		Items: []*orkstackv1.TaskRun{
			{TaskId: "convert", Item: proto.Int32(0), Attempt: 1, Artifacts: []*orkstackv1.ArtifactRef{part0}},
			{TaskId: "convert", Item: proto.Int32(1), Attempt: 1, Artifacts: []*orkstackv1.ArtifactRef{part1}},
		}}}
	f.reader.setRun(run)
	f.archive(t, run)
	if _, err := f.ask("default", run.GetId(), "convert", "part.txt"); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("no item = %v", err)
	}
	res, err := f.askReq(&orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), TaskId: "convert", Item: item(1),
		Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "part.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetArtifact().GetSha256() != part1.GetSha256() {
		t.Fatalf("selected %v, want item 1's object", res.GetArtifact())
	}
	if len(res.GetReferences()) != 1 || res.GetReferences()[0].Item == nil || res.GetReferences()[0].GetItem() != 1 {
		t.Fatalf("references = %v, want one carrying item 1", res.GetReferences())
	}
}

func TestExportRunEvidence_ItemMembers(t *testing.T) {
	e := newEachReads(t)
	result := e.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: eachRunID})
	m := result.manifest
	var entries []string
	for _, task := range m.Tasks {
		entries = append(entries, task.TaskID+itemSuffix(task.Item))
	}
	if fmt.Sprint(entries) != "[list convert convert[0] convert[1] convert[2]]" {
		t.Fatalf("tasks = %v", entries)
	}
	for _, path := range []string{
		"tasks/convert/items/00000/commit.json", "tasks/convert/items/00001/values.json",
		"tasks/convert/items/00002/artifacts/part.txt", "tasks/convert/items/00001/attempts/2.json",
		"tasks/convert/items/00001/attempts/1.json",
	} {
		member := memberAt(t, m, path)
		if member.Item == nil {
			t.Fatalf("member %s carries no item", path)
		}
	}
	if report := verifyBundle(t, result.data); !report.OK() {
		t.Fatalf("the bundle does not verify: %v", report.Failures)
	}
	narrowed := e.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: eachRunID, TaskId: "convert", Item: item(2), ManifestOnly: true})
	if narrowed.manifest.Scope.Item == nil || *narrowed.manifest.Scope.Item != 2 {
		t.Fatalf("scope = %+v", narrowed.manifest.Scope)
	}
	for _, member := range narrowed.manifest.Members {
		if member.Item != nil && *member.Item != 2 {
			t.Fatalf("narrowed to item 2 but exported %s", member.Path)
		}
	}
}

const eachValidateDoc = `
apiVersion: orkstack/v1
kind: Orkflow
name: fan-validate
inputs:
  parts: {type: array}
tasks:
  - id: convert
    uses: busybox:latest
    each: {over: "{{ .inputs.parts }}", max_items: 600}
  - id: render
    uses: busybox:latest
    each: {over: "{{ .inputs.parts }}", max_items: %d}
    with: {blob: "%s"}
`

func TestValidateOrkflow_DeploymentCapBelowTheDeclaredTotalIsAnIssue(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), EachMaxItems: 200})
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: fmt.Sprintf(eachValidateDoc, 100, "x")}))
	if err != nil {
		t.Fatal(err)
	}
	issues := resp.Msg.GetIssues()
	if len(issues) != 1 || issues[0].GetField() != "tasks[0].each.max_items" ||
		issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR ||
		!strings.Contains(issues[0].GetMessage(), "declares 700 items across its fan-outs (convert 600, render 100); this deployment allows 200 (ORK_EACH_MAX_ITEMS)") {
		t.Fatalf("issues = %v", issues)
	}
	open := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	ok, err := open.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: fmt.Sprintf(eachValidateDoc, 100, "x")}))
	if err != nil || len(ok.Msg.GetIssues()) != 0 {
		t.Fatalf("the default cap = %v, %v", ok.Msg.GetIssues(), err)
	}
}

func TestValidateOrkflow_EachWithOverTheItemBoundIsAnError(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: fmt.Sprintf(eachValidateDoc, 400, strings.Repeat("y", 20000))}))
	if err != nil {
		t.Fatal(err)
	}
	issues := resp.Msg.GetIssues()
	if len(issues) != 1 || issues[0].GetField() != "tasks[1].with" ||
		!strings.Contains(issues[0].GetMessage(), `every item of "render" would carry at least`) ||
		!strings.Contains(issues[0].GetMessage(), "declare 1000 items, which allows 16777 bytes per item") {
		t.Fatalf("issues = %v", issues)
	}
}

func TestSubmitRun_LoweredCapIsFailedPreconditionAndMintsNothing(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	starter := &fakeStarter{}
	applier := api.NewService(api.Config{Orkflows: orkflows, Starter: starter})
	if _, err := applier.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: fmt.Sprintf(eachValidateDoc, 100, "x")})); err != nil {
		t.Fatalf("Apply under the default cap: %v", err)
	}
	lowered := api.NewService(api.Config{Orkflows: orkflows, Starter: starter, EachMaxItems: 200})
	_, err := lowered.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "fan-validate"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "ORK_EACH_MAX_ITEMS was lowered after version") {
		t.Fatalf("submit = %v", err)
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatalf("a refused submission started %v", starter.startedRuns())
	}
}

func TestSubmitRun_PoolWithoutEachSupportIsFailedPrecondition(t *testing.T) {
	starter := &fakeStarter{}
	gpuQueue := poolQueue(t, "gpu")
	svc := api.NewService(api.Config{
		Starter: starter, Pools: livePools{gpuQueue: 1}, Fleet: oldGPUOrker(func(o *orkstackv1.DescribeOrkerResult) { o.EachSupported = false }),
	})
	doc := `
apiVersion: orkstack/v1
kind: Orkflow
name: fan-gpu
inputs:
  parts: {type: array}
tasks:
  - id: convert
    uses: busybox:latest
    pool: gpu
    each: {over: "{{ .inputs.parts }}", max_items: 5}
`
	if _, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: doc})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "fan-gpu", AllowEmptyPool: true}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "predates fan-out (each:) support") {
		t.Fatalf("submit = %v", err)
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatal("a refused submission started a run")
	}
}

var _ = io.EOF
