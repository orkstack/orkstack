package api

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
)

// TestAuthz_EventSourceRows holds the event-source rows the coordinator
// decided: an inspect is an authoring act (eventsource.write), a viewer is
// refused it; evaluating a candidate also needs orkflow.read at its Orkflow,
// so eventsource.read alone is refused, while a stored source's evaluation
// needs eventsource.read only.
func TestAuthz_EventSourceRows(t *testing.T) {
	candidate := &orkstackv1.EvaluateEventSourceMessageRequest{Source: &orkstackv1.EvaluateEventSourceMessageRequest_Candidate{
		Candidate: &orkstackv1.EventSource{Namespace: "payments", Name: "c", OrkflowName: "handle-order"}}}
	stored := &orkstackv1.EvaluateEventSourceMessageRequest{Namespace: "payments", Source: &orkstackv1.EvaluateEventSourceMessageRequest_Name{Name: "orders"}}
	inspect := &orkstackv1.InspectSourceImageRequest{Namespace: "payments", Reference: "ghcr.io/acme/s:1"}
	for _, tc := range []struct {
		name      string
		role      string
		perms     []string
		procedure string
		msg       proto.Message
		admitted  bool
	}{
		{"viewer inspects", authz.RoleViewer, nil, orkstackv1connect.EventSourceServiceInspectSourceImageProcedure, inspect, false},
		{"developer inspects", authz.RoleDeveloper, nil, orkstackv1connect.EventSourceServiceInspectSourceImageProcedure, inspect, true},
		{"eventsource.read alone evaluates a candidate", "r", []string{"eventsource.read"}, orkstackv1connect.EventSourceServiceEvaluateEventSourceMessageProcedure, candidate, false},
		{"eventsource.read and orkflow.read evaluate a candidate", "r", []string{"eventsource.read", "orkflow.read"}, orkstackv1connect.EventSourceServiceEvaluateEventSourceMessageProcedure, candidate, true},
		{"eventsource.read alone evaluates a stored source", "r", []string{"eventsource.read"}, orkstackv1connect.EventSourceServiceEvaluateEventSourceMessageProcedure, stored, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newAccessService(t)
			var roles []*orkstackv1.Role
			if tc.perms != nil {
				roles = []*orkstackv1.Role{{Name: tc.role, Permissions: tc.perms}}
			}
			a, _ := accessFor(t, authn.ModeEnforce, roles,
				&orkstackv1.Binding{Name: "b", Role: tc.role, Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
			s.access = a
			_, err := a.decide(asAlice(authn.ModeEnforce), s, tc.procedure, tc.msg)
			if tc.admitted && err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !tc.admitted && err == nil {
				t.Fatal("admitted")
			}
		})
	}
}

func TestNamespaceRate_BoundsAndForgets(t *testing.T) {
	r := newNamespaceRate(2)
	now := time.Unix(1_000_000, 0)
	first, second, third := r.allow("a", now), r.allow("a", now), r.allow("a", now)
	if !first || !second || third {
		t.Fatalf("two a minute, then refused: %v %v %v", first, second, third)
	}
	if !r.allow("a", now.Add(time.Minute)) {
		t.Fatal("the window moves on")
	}
	for i := 0; i < 1000; i++ {
		r.allow(fmt.Sprintf("ns-%d", i), now)
	}
	r.allow("b", now.Add(2*time.Minute))
	if len(r.window) > 2 {
		t.Fatalf("idle namespaces are forgotten: %d kept", len(r.window))
	}
}
