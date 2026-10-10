package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
)

func (f *fixture) triggerClient() orkstackv1connect.TriggerServiceClient {
	return orkstackv1connect.NewTriggerServiceClient(f.client, f.baseURL)
}

// applyHello stores the example orkflow every trigger below points at.
func (f *fixture) applyHello(t *testing.T) {
	t.Helper()
	if _, err := f.orkflowClient().ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// createTrigger stores one trigger and returns it with its one-time secret.
func (f *fixture) createTrigger(t *testing.T, req *orkstackv1.CreateTriggerRequest) (*orkstackv1.Trigger, string) {
	t.Helper()
	created, err := f.triggerClient().CreateTrigger(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("CreateTrigger(%v): %v", req, err)
	}
	if created.Msg.GetSecret() == "" {
		t.Fatal("CreateTrigger disclosed no secret; nothing could ever invoke the trigger")
	}
	return created.Msg.GetTrigger(), created.Msg.GetSecret()
}

// invoke performs one HTTP invocation and returns the status and body. A nil
// token sends no Authorization header at all.
func (f *fixture) invoke(t *testing.T, id string, token *string, body string, headers map[string]string) (int, []byte) {
	t.Helper()
	path := strings.Replace(api.TriggerInvokePath, "{id}", id, 1)
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, f.baseURL+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != nil {
		req.Header.Set("Authorization", *token)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, payload
}

func bearer(token string) *string {
	header := "Bearer " + token
	return &header
}

// triggerErrorCode reads the error envelope every failure answers with.
func triggerErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error envelope: %v (%s)", err, body)
	}
	if envelope.Error.Message == "" {
		t.Errorf("error envelope carries no message: %s", body)
	}
	return envelope.Error.Code
}

func TestTriggerInvoke_ValidTokenStartsTheOrkflowTheTriggerNames(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{
		OrkflowName: "hello-ork",
		Description: "nightly",
	})
	if !strings.HasPrefix(trigger.GetId(), "trg-") {
		t.Errorf("trigger id = %q, want a trg- id", trigger.GetId())
	}

	status, body := f.invoke(t, trigger.GetId(), bearer(secret),
		`{"url":"https://example.test/data.json"}`, nil)
	if status != http.StatusCreated {
		t.Fatalf("invoke status = %d, want 201: %s", status, body)
	}

	// The envelope is the one `orkctl run submit --json` and the UI read, in
	// the short-enum dialect (json.go), not a trimmed shape of its own.
	var response struct {
		Run struct {
			ID             string `json:"id"`
			OrkflowName    string `json:"orkflowName"`
			OrkflowVersion string `json:"orkflowVersion"`
			Namespace      string `json:"namespace"`
			Status         string `json:"status"`
		} `json:"run"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode response: %v (%s)", err, body)
	}
	if response.Run.ID != "run-001" {
		t.Errorf("run id = %q, want run-001", response.Run.ID)
	}
	if response.Run.Status != "PENDING" {
		t.Errorf("run status = %q, want PENDING without its proto prefix", response.Run.Status)
	}
	if response.Run.OrkflowName != "hello-ork" || response.Run.OrkflowVersion != "0.2.0" {
		t.Errorf("run pins %s@%s, want hello-ork@0.2.0",
			response.Run.OrkflowName, response.Run.OrkflowVersion)
	}
	if response.Run.Namespace != "default" {
		t.Errorf("run namespace = %q, want default", response.Run.Namespace)
	}

	if started := f.starter.startedRuns(); len(started) != 1 || started[0] != "run-001" {
		t.Fatalf("started runs = %v, want [run-001]", started)
	}
	// The body IS the input object: it reaches the run as declared inputs.
	inputs := f.starter.startedInputs()
	if len(inputs) != 1 || inputs[0]["url"] != "https://example.test/data.json" {
		t.Errorf("started input = %v, want the posted object", inputs)
	}
}

func TestTriggerInvoke_RejectsAMissingOrWrongTokenAndStartsNothing(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	_, otherSecret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})

	noHeader := ""
	cases := []struct {
		label string
		token *string
	}{
		{"no Authorization header", nil},
		{"an empty Authorization header", &noHeader},
		{"another scheme", strPtr("Basic " + secret)},
		{"a bare token", strPtr(secret)},
		{"Bearer with no token", strPtr("Bearer ")},
		{"a wrong token", bearer("nope")},
		{"another trigger's token", bearer(otherSecret)},
		{"the right token with trailing text", bearer(secret + "x")},
	}
	for _, tc := range cases {
		status, body := f.invoke(t, trigger.GetId(), tc.token, `{}`, nil)
		if status != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401: %s", tc.label, status, body)
		}
		if code := triggerErrorCode(t, body); code != "unauthenticated" {
			t.Errorf("%s: error code = %q, want unauthenticated", tc.label, code)
		}
	}
	// Proven by observation, not by the absence of an error.
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Errorf("an unauthenticated invoke started %v", started)
	}
}

func TestTriggerInvoke_UnknownOrDeletedIDIsNotFound(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})

	// An id is not confidential: an unknown id is 404 and a wrong token on a
	// real id is 401, and the two are deliberately distinguishable.
	for _, id := range []string{"trg-00000000000000ff", "hello", "trg-"} {
		status, body := f.invoke(t, id, bearer(secret), `{}`, nil)
		if status != http.StatusNotFound {
			t.Errorf("invoke %q: status = %d, want 404: %s", id, status, body)
		}
		if code := triggerErrorCode(t, body); code != "not_found" {
			t.Errorf("invoke %q: error code = %q, want not_found", id, code)
		}
	}

	if _, err := f.triggerClient().DeleteTrigger(context.Background(),
		connect.NewRequest(&orkstackv1.DeleteTriggerRequest{Id: trigger.GetId()})); err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status != http.StatusNotFound {
		t.Errorf("invoking a deleted trigger = %d, want 404: %s", status, body)
	}
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Errorf("a 404 invoke started %v", started)
	}
}

func TestTriggerInvoke_BodyMustBeAJSONObjectAndMayBeEmpty(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})

	for _, body := range []string{`[1,2]`, `"hello"`, `12`, `true`, `null`, `{`, `{}{}`} {
		status, payload := f.invoke(t, trigger.GetId(), bearer(secret), body, nil)
		if status != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400: %s", body, status, payload)
		}
		if code := triggerErrorCode(t, payload); code != "invalid_argument" {
			t.Errorf("body %s: error code = %q, want invalid_argument", body, code)
		}
	}
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Fatalf("a rejected body started %v", started)
	}

	// An empty body means no input, and the orkflow's declared defaults apply
	// exactly as they do for any other submission.
	status, payload := f.invoke(t, trigger.GetId(), bearer(secret), "", nil)
	if status != http.StatusCreated {
		t.Fatalf("empty body: status = %d, want 201: %s", status, payload)
	}
	inputs := f.starter.startedInputs()
	if len(inputs) != 1 || inputs[0]["url"] != "https://httpbin.org/json" {
		t.Errorf("started input = %v, want the orkflow's declared default", inputs)
	}
}

func TestTriggerInvoke_CannotBeRedirectedByTheCaller(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.2.0",
	})

	// Coordinates in the body are input data like any other key: the run that
	// starts is the one the record names.
	status, body := f.invoke(t, trigger.GetId(), bearer(secret),
		`{"orkflow_name":"ghost","namespace":"payments","orkflowVersion":"9.9.9"}`, nil)
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", status, body)
	}
	if len(f.starter.orkflows) != 1 {
		t.Fatalf("started %d runs, want 1", len(f.starter.orkflows))
	}
	started := f.starter.orkflows[0]
	if started.GetName() != "hello-ork" || started.GetVersion() != "0.2.0" ||
		started.GetNamespace() != "default" {
		t.Errorf("started %s@%s in %q, want the trigger's own coordinates",
			started.GetName(), started.GetVersion(), started.GetNamespace())
	}
}

func TestTriggerInvoke_IdempotencyKeyPassesStraightThrough(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})

	headers := map[string]string{"Idempotency-Key": "nightly-2026-09-08"}
	first, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, headers)
	if first != http.StatusCreated {
		t.Fatalf("first invoke = %d, want 201: %s", first, body)
	}
	second, repeat := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, headers)
	if second != http.StatusCreated {
		t.Fatalf("repeated invoke = %d, want 201: %s", second, repeat)
	}
	if !bytes.Equal(body, repeat) {
		t.Errorf("a repeated key answered\n%s\nwant the first run\n%s", repeat, body)
	}
	if started := f.starter.startedRuns(); len(started) != 1 {
		t.Errorf("a repeated idempotency key started %v, want one run", started)
	}
}

func TestTriggerInvoke_SubmitFailureIsMappedAndStartsNothing(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	f.starter.err = fmt.Errorf("temporal is unreachable")

	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status == http.StatusCreated {
		t.Fatalf("a failed start answered 201: %s", body)
	}
	if status < 400 {
		t.Fatalf("status = %d, want a failure: %s", status, body)
	}
	if code := triggerErrorCode(t, body); code == "" {
		t.Errorf("failure carried no error code: %s", body)
	}
}

// TestTriggerInvoke_AForbiddenSecretIs400InvalidArgument: a trigger invoke
// is a SubmitRun, so it passes the same Plan gate — a definition applied
// while the namespace was unrestricted is refused once the list tightens,
// with the submit's own code carried through and nothing started.
func TestTriggerInvoke_AForbiddenSecretIs400InvalidArgument(t *testing.T) {
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "sandbox"})
	f := newFixtureWith(t, func(cfg *api.Config) { cfg.Namespaces = reg })
	ctx := context.Background()
	if _, err := f.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(
		&orkstackv1.ApplyOrkflowRequest{Yaml: exfilOrkflow, Namespace: "sandbox"})); err != nil {
		t.Fatalf("Apply while unrestricted: %v", err)
	}
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{
		OrkflowName: "exfil", Namespace: "sandbox",
	})
	tighten(t, reg, "env:SANDBOX_*")

	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", status, body)
	}
	if code := triggerErrorCode(t, body); code != "invalid_argument" {
		t.Fatalf("error.code = %q, want invalid_argument: %s", code, body)
	}
	if !strings.Contains(string(body), strings.ReplaceAll(exfilRefusal, `"`, `\"`)) {
		t.Errorf("body %s does not carry %q", body, exfilRefusal)
	}
	runs, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{Namespace: "sandbox"}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Msg.GetRuns()) != 0 || len(f.starter.startedRuns()) != 0 {
		t.Fatalf("a refused invoke started %v", runs.Msg.GetRuns())
	}
}

func TestTriggerInvoke_OnlyPostIsServed(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	trigger, _ := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})

	path := strings.Replace(api.TriggerInvokePath, "{id}", trigger.GetId(), 1)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, f.baseURL+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET %s = %d, want 405", path, resp.StatusCode)
	}
}

func TestCreateTrigger_RefusesAnOrkflowOrVersionThatDoesNotExist(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	ctx := context.Background()

	cases := []struct {
		label string
		req   *orkstackv1.CreateTriggerRequest
		want  connect.Code
	}{
		{"no orkflow name", &orkstackv1.CreateTriggerRequest{}, connect.CodeInvalidArgument},
		{"an orkflow nobody applied", &orkstackv1.CreateTriggerRequest{
			OrkflowName: "ghost"}, connect.CodeNotFound},
		{"a version nobody applied", &orkstackv1.CreateTriggerRequest{
			OrkflowName: "hello-ork", OrkflowVersion: "9.9.9"}, connect.CodeNotFound},
		// The namespace is resolved through the same seam every other request
		// uses (Service.namespace). This fixture wires no namespace registry,
		// so any name but the built-in default is refused there — before the
		// orkflow is read and before anything is written, which is the part
		// this case pins.
		{"a namespace this deployment cannot resolve", &orkstackv1.CreateTriggerRequest{
			OrkflowName: "hello-ork", Namespace: "payments"}, connect.CodeFailedPrecondition},
		{"a namespace name the rule forbids", &orkstackv1.CreateTriggerRequest{
			OrkflowName: "hello-ork", Namespace: "Payments"}, connect.CodeInvalidArgument},
	}
	for _, tc := range cases {
		_, err := f.triggerClient().CreateTrigger(ctx, connect.NewRequest(tc.req))
		if err == nil {
			t.Errorf("%s: CreateTrigger succeeded, want %v", tc.label, tc.want)
			continue
		}
		if connect.CodeOf(err) != tc.want {
			t.Errorf("%s: code = %v, want %v (%v)", tc.label, connect.CodeOf(err), tc.want, err)
		}
	}

	// A refused create writes nothing: a trigger must never point at nothing.
	listed, err := f.triggerClient().ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{}))
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if len(listed.Msg.GetTriggers()) != 0 {
		t.Errorf("a refused create was stored: %+v", listed.Msg.GetTriggers())
	}
}

func TestTriggerService_ListNarrowsToOneOrkflowAndCarriesNoSecret(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	ctx := context.Background()

	pinned, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{
		OrkflowName: "hello-ork", OrkflowVersion: "0.2.0", Description: "pinned",
	})
	current, _ := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	if pinned.GetOrkflowVersion() != "0.2.0" {
		t.Errorf("pinned trigger version = %q, want 0.2.0", pinned.GetOrkflowVersion())
	}
	if current.GetOrkflowVersion() != "" {
		t.Errorf("unpinned trigger version = %q, want empty (the current pointer)",
			current.GetOrkflowVersion())
	}
	if pinned.GetNamespace() != "default" || pinned.GetCreatedAt() == nil {
		t.Errorf("created trigger = %v, want a namespace and a creation time", pinned)
	}

	listed, err := f.triggerClient().ListTriggers(ctx, connect.NewRequest(
		&orkstackv1.ListTriggersRequest{Filters: filters(t, "orkflow_name=hello-ork")}))
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if len(listed.Msg.GetTriggers()) != 2 {
		t.Fatalf("ListTriggers returned %d triggers, want 2", len(listed.Msg.GetTriggers()))
	}
	encoded, err := api.MarshalJSON(listed.Msg)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if strings.Contains(string(encoded), secret) {
		t.Error("a listing contains a trigger's secret")
	}

	narrowed, err := f.triggerClient().ListTriggers(ctx, connect.NewRequest(
		&orkstackv1.ListTriggersRequest{Filters: filters(t, "orkflow_name=other-ork")}))
	if err != nil {
		t.Fatalf("ListTriggers(other-ork): %v", err)
	}
	if len(narrowed.Msg.GetTriggers()) != 0 {
		t.Errorf("ListTriggers(other-ork) = %+v, want nothing", narrowed.Msg.GetTriggers())
	}
}

func TestDeleteTrigger_RemovesOnceAndThenReportsNotFound(t *testing.T) {
	f := newFixture(t)
	f.applyHello(t)
	ctx := context.Background()
	trigger, _ := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})

	if _, err := f.triggerClient().DeleteTrigger(ctx, connect.NewRequest(
		&orkstackv1.DeleteTriggerRequest{Id: trigger.GetId()})); err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	for _, id := range []string{trigger.GetId(), "trg-00000000000000ff", "not-an-id"} {
		_, err := f.triggerClient().DeleteTrigger(ctx, connect.NewRequest(
			&orkstackv1.DeleteTriggerRequest{Id: id}))
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("DeleteTrigger(%q) code = %v, want NotFound (%v)", id, connect.CodeOf(err), err)
		}
	}
	listed, err := f.triggerClient().ListTriggers(ctx, connect.NewRequest(&orkstackv1.ListTriggersRequest{}))
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if len(listed.Msg.GetTriggers()) != 0 {
		t.Errorf("after delete the registry still holds %+v", listed.Msg.GetTriggers())
	}
}

func strPtr(value string) *string { return &value }

// TestTriggerInvoke_APolicyRefusalIs412FailedPrecondition: a trigger invoke
// is a SubmitRun, so a definition applied under the empty image policy is
// refused once the floor tightens — 412 failed_precondition through the
// generic mapping, the policy sentence carried, and no run started.
func TestTriggerInvoke_APolicyRefusalIs412FailedPrecondition(t *testing.T) {
	policies := &memImagePolicies{}
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.ImagePolicies = policies
		cfg.Resolver = &stampResolver{}
	})
	ctx := context.Background()
	if _, err := f.orkflowClient().ApplyOrkflow(ctx, connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: orkflowUsing("deploy", "docker.io/library/alpine:3.20"),
	})); err != nil {
		t.Fatalf("Apply under the empty policy: %v", err)
	}
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "deploy"})
	policies.set(&orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/"}})

	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, want 412: %s", status, body)
	}
	if code := triggerErrorCode(t, body); code != "failed_precondition" {
		t.Fatalf("error.code = %q, want failed_precondition: %s", code, body)
	}
	if !strings.Contains(string(body), "outside the registries this deployment allows") {
		t.Errorf("body %s does not carry the policy sentence", body)
	}
	runs, err := f.runClient().ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{}))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Msg.GetRuns()) != 0 || len(f.starter.startedRuns()) != 0 {
		t.Fatalf("a refused invoke started %v", runs.Msg.GetRuns())
	}
}
