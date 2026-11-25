package api_test

import (
	"context"
	"encoding/json/v2"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// TestWebhookNotifier_OrkflowScopeIsTheNamespacesOwnOrkflow: payments and
// platform each hold an Orkflow "build", and platform's is shared. A
// subscription to payments' "build" hears of payments' own runs of it, never
// of platform's running in payments as a child; and the payload of a delivery
// that does carry a shared child names the Orkflow's owner.
func TestWebhookNotifier_OrkflowScopeIsTheNamespacesOwnOrkflow(t *testing.T) {
	notify := func(f *notifierFixture, id, owner string) {
		f.adapter.NotifyRunEvent(context.Background(), &orkstackv1.Event{RunId: id, Sequence: 1,
			Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, Status: orkstackv1.Status_STATUS_SUCCEEDED},
			&orkstackv1.Run{Id: id, Namespace: "payments", OrkflowName: "build", OrkflowVersion: "1.0.0",
				OrkflowNamespace: owner, Status: orkstackv1.Status_STATUS_SUCCEEDED})
	}
	type payload struct {
		RunID            string `json:"runId"`
		OrkflowName      string `json:"orkflowName"`
		OrkflowNamespace string `json:"orkflowNamespace"`
	}
	received := func(t *testing.T, f *notifierFixture) payload {
		body, _ := f.awaitDelivery(t)
		var p payload
		must.OK(t, json.Unmarshal(body, &p))
		return p
	}

	t.Run("scoped to the namespace's own", func(t *testing.T) {
		f := newNotifierFixture(t)
		_, err := f.registry.Register(webhooks.Registration{URL: f.server.URL, Namespace: "payments", OrkflowName: "build",
			Events: []webhooks.Event{webhooks.EventRunSucceeded}})
		must.OK(t, err)
		notify(f, "ork-shared", "platform")
		notify(f, "ork-own", "")
		if got := received(t, f); got.RunID == "ork-shared" {
			t.Fatalf("delivered %+v; want only payments' own build", got)
		}
		must.OK(t, f.notifier.Wait(context.Background()))
		if n := f.count(); n != 1 {
			t.Fatalf("%d deliveries; want only payments' own build's", n)
		}
	})
	t.Run("an unscoped delivery names the owner", func(t *testing.T) {
		f := newNotifierFixture(t)
		_, err := f.registry.Register(webhooks.Registration{URL: f.server.URL, Events: []webhooks.Event{webhooks.EventRunSucceeded}})
		must.OK(t, err)
		notify(f, "ork-shared", "platform")
		if got := received(t, f); got.OrkflowNamespace != "platform" || got.OrkflowName != "build" {
			t.Fatalf("payload = %+v; want it to name platform", got)
		}
	})
}
