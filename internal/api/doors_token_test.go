package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// faultDetail is what a fault says of a deployment; no sender is told it.
const faultDetail = "dial tcp 10.0.0.7:7233: s3://ork-prod/x: /srv/ork/secret"

func requireFixedWords(t *testing.T, status, want int, body []byte, words string, log *bytes.Buffer) {
	t.Helper()
	if status != want || !strings.Contains(string(body), words) {
		t.Fatalf("status %d, body %s; want %d saying %q", status, body, want, words)
	}
	for _, w := range []string{"10.0.0.7", "ork-prod", "/srv/ork"} {
		if strings.Contains(string(body), w) {
			t.Fatalf("the sender is told %q: %s", w, body)
		}
	}
	if !strings.Contains(log.String(), "10.0.0.7") {
		t.Fatalf("the log has no cause: %s", log)
	}
}

// A callback's sender holds that await's token and nothing else: a fault
// after its check says what to do, and its cause is the log's.
func TestAwaitDoor_AFaultAfterTheTokenIsFixedWords(t *testing.T) {
	const run = "ork-door-fault"
	awaiting := map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}
	const again = `the resolution of run \"ork-door-fault\" could not be recorded now; send it again`
	for name, c := range map[string]struct {
		fault func(t *testing.T, f *awaitFixture)
		body  string
	}{
		"the definition's object": {func(t *testing.T, f *awaitFixture) {
			byReferenceReadStart(t, f.reader, run, f.wf, startPlan(t, f.reader, run), false)
			f.reader.definitions = func(context.Context, string, *engine.RunStartRecord) (*orkstackv1.Orkflow, error) {
				return nil, errors.New(faultDetail)
			}
		}, `{"approved":true}`},
		"the run":      {func(_ *testing.T, f *awaitFixture) { f.reader.getErr = errors.New(faultDetail) }, `{"approved":true}`},
		"the store":    {func(_ *testing.T, f *awaitFixture) { f.store.err = errors.New(faultDetail) }, `{"approved":true}`},
		"the delivery": {func(_ *testing.T, f *awaitFixture) { f.starter.signalErr = errors.New(faultDetail) }, `{"approved":true}`},
	} {
		t.Run(name, func(t *testing.T) {
			var log bytes.Buffer
			f := newAwaitFixture(t, func(cfg *api.Config) { cfg.Logger = slog.New(slog.NewTextHandler(&log, nil)) })
			f.seedRun(t, run, "team-a", awaiting)
			c.fault(t, f)
			status, body := f.door(t, awaitPath(run, "gate"), bearer(awaitToken), c.body, nil)
			requireFixedWords(t, status, http.StatusServiceUnavailable, body, again, &log)
		})
	}
	t.Run("what the sender can mend it is told", func(t *testing.T) {
		f := newAwaitFixture(t)
		f.seedRun(t, run, "team-a", awaiting)
		status, body := f.door(t, awaitPath(run, "gate"), bearer(awaitToken), `{"approved":1}`, nil)
		if status != http.StatusBadRequest || !strings.Contains(string(body), "approved") {
			t.Fatalf("status %d, body %s", status, body)
		}
	})
}

type faultyOrkflows struct {
	engine.OrkflowStore
	err error
}

func (f *faultyOrkflows) Get(ctx context.Context, namespace, name, version string) (*orkstackv1.Orkflow, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.OrkflowStore.Get(ctx, namespace, name, version)
}

// Nor is a trigger's sender, whose run may or may not have started: it is
// asked to send the request again under its key. A documented reason code
// stays, whatever its detail.
func TestTriggerInvoke_AFaultAfterTheTokenIsFixedWords(t *testing.T) {
	// A sender with no key is never asked to send again as if that were safe.
	for name, c := range map[string]struct {
		headers map[string]string
		words   string
	}{
		"no key":  {nil, "the run may have been started all the same: look for it before you send the request again, since it has no Idempotency-Key"},
		"its key": {map[string]string{api.IdempotencyKeyHeader: "k1"}, "send the request again with the same Idempotency-Key"},
	} {
		t.Run(name, func(t *testing.T) {
			var log bytes.Buffer
			var store *faultyOrkflows
			f := newFixtureWith(t, func(cfg *api.Config) {
				store = &faultyOrkflows{OrkflowStore: cfg.Orkflows}
				cfg.Orkflows, cfg.Logger = store, slog.New(slog.NewTextHandler(&log, nil))
			})
			f.applyHello(t)
			trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
			store.err = errors.New(faultDetail)
			status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, c.headers)
			requireFixedWords(t, status, http.StatusServiceUnavailable, body, c.words, &log)
		})
	}

	// The reason code of a limit's unread check.
	g := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Starter = &concStarter{fakeStarter: cfg.Starter.(*fakeStarter), indexed: true}
		cfg.Capabilities = &fakeCapabilities{err: errors.New(faultDetail)}
		cfg.ConcurrencyReader = &fakeConcReader{}
	})
	_, err := g.orkflowClient().ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: limitedYAML(templatedLimit + "\n  max_queued: 3")}))
	must.OK(t, err)
	limited, secret := g.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "migrate"})
	status, body := g.invoke(t, limited.GetId(), bearer(secret), "", nil)
	if status != http.StatusServiceUnavailable || !strings.Contains(string(body), engine.ConcurrencyCheckUnavailableCode+": this request started no run; send it again") {
		t.Fatalf("status %d, body %s; want 503 with its reason code", status, body)
	}
}

// A trigger whose Orkflow or namespace is gone says so and lists nothing of
// what is there.
func TestTriggerInvoke_WhatIsGoneNamesNothingElse(t *testing.T) {
	var store *faultyOrkflows
	f := newFixtureWith(t, func(cfg *api.Config) {
		store = &faultyOrkflows{OrkflowStore: cfg.Orkflows}
		cfg.Orkflows = store
	})
	f.applyHello(t)
	trigger, secret := f.createTrigger(t, &orkstackv1.CreateTriggerRequest{OrkflowName: "hello-ork"})
	store.err = fmt.Errorf("namespaces are payments, people: %w", artifacts.ErrNotFound)
	status, body := f.invoke(t, trigger.GetId(), bearer(secret), `{}`, nil)
	if status != http.StatusNotFound || errorCode(t, body) != "not_found" || strings.Contains(string(body), "payments") ||
		!strings.Contains(string(body), "no longer exists") {
		t.Fatalf("status %d, body %s", status, body)
	}
}
