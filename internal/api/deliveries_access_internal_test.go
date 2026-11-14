package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// TestListDeliveries_APageEndsOnARowTheCallerMayRead: a deployment
// webhook's deliveries of a namespace's runs are not the namespace reader's,
// so a page neither shows one nor ends its cursor on one (security re-check
// LOW): the scan passes them by and fills the page with rows it may read.
func TestListDeliveries_APageEndsOnARowTheCallerMayRead(t *testing.T) {
	ctx := context.Background()
	registry := webhooks.NewRegistry()
	hidden, err := registry.Register(webhooks.Registration{URL: "https://ops.example/all"})
	if err != nil {
		t.Fatal(err)
	}
	own, err := registry.Register(webhooks.Registration{URL: "https://payments.example/hooks", Namespace: "payments"})
	if err != nil {
		t.Fatal(err)
	}
	store := artifacts.NewMemoryDeliveryStore()
	now := time.Now().UTC()
	put := func(at time.Time, hook string) string {
		id, err := webhooks.NewDeliveryID(at)
		if err != nil {
			t.Fatal(err)
		}
		rec := &orkstackv1.Delivery{Id: id, WebhookId: hook, Event: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
			RunId: "ork-x", Namespace: "payments", OrkflowName: "reconcile", Status: orkstackv1.Status_STATUS_FAILED,
			Url: "https://x.example", CreatedAt: timestamppb.New(at), EventAt: timestamppb.New(at), SettledAt: timestamppb.New(at)}
		if err := store.PutDelivery(ctx, rec); err != nil {
			t.Fatal(err)
		}
		return id
	}
	older := put(now.Add(-3*time.Minute), own.ID)
	newer := put(now.Add(-2*time.Minute), own.ID)
	hiddenID := put(now.Add(-time.Minute), hidden.ID)

	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Deliveries: store, Webhooks: registry})
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleNamespaceAdmin,
		Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
	s.access = a
	inPayments := []*orkstackv1.FieldFilter{{Field: "namespace", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{"payments"}}}
	resp, err := s.ListDeliveries(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.ListDeliveriesRequest{Filters: inPayments, PageSize: 1}))
	if err != nil {
		t.Fatal(err)
	}
	rows := resp.Msg.GetDeliveries()
	if len(rows) != 1 || rows[0].GetId() != newer {
		t.Fatalf("first page = %v; want the newest delivery the caller may read, %s", rows, newer)
	}
	set, err := compileFilters(&orkstackv1.ListDeliveriesRequest{Filters: inPayments})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, beforeKey, err := decodeDeliveryCursor(resp.Msg.GetNextPageToken(), set)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(beforeKey, hiddenID) || strings.Contains(beforeKey, hidden.ID) || !strings.Contains(beforeKey, newer) {
		t.Fatalf("the page's cursor %q does not end on the row it showed, %s", beforeKey, newer)
	}
	next, err := s.ListDeliveries(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.ListDeliveriesRequest{
		Filters: inPayments, PageSize: 1, PageToken: resp.Msg.GetNextPageToken()}))
	if err != nil {
		t.Fatal(err)
	}
	if got := next.Msg.GetDeliveries(); len(got) != 1 || got[0].GetId() != older {
		t.Fatalf("second page = %v; want %s", got, older)
	}
	// An admin, who may read the deployment's webhook, sees all three.
	all, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleAdmin,
		Scope: "deployment", Subjects: []string{"user:corp:alice"}})
	s.access = all
	whole, err := s.ListDeliveries(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.ListDeliveriesRequest{Filters: inPayments}))
	if err != nil {
		t.Fatal(err)
	}
	if len(whole.Msg.GetDeliveries()) != 3 {
		t.Fatalf("an admin read %d deliveries, want 3", len(whole.Msg.GetDeliveries()))
	}
}
