package api_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
)

// TestCancelRun_RecordsThePrincipalOfItsMode holds who a cancellation
// names to the mode: the header in asserted mode, the verified principal in
// observe and enforce mode, whatever header the caller sent, and nobody for
// a request in observe mode that presented no credential.
func TestCancelRun_RecordsThePrincipalOfItsMode(t *testing.T) {
	verified := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:ada", Method: authn.MethodOIDC, Issuer: "corp"}
	tests := []struct {
		name      string
		mode      string
		principal *authn.Principal
		header    string
		want      string
	}{
		{name: "asserted names the header", mode: authn.ModeAsserted, principal: verified, header: "dana@acme", want: "dana@acme"},
		{name: "observe names the verified principal", mode: authn.ModeObserve, principal: verified, header: "mallory", want: "user:corp:ada"},
		{name: "enforce names the verified principal", mode: authn.ModeEnforce, principal: verified, header: "mallory", want: "user:corp:ada"},
		{name: "observe without a credential names nobody", mode: authn.ModeObserve, header: "mallory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			if _, err := f.orkflowClient().ApplyOrkflow(ctx,
				connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: helloYAML(t)})); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if _, err := f.runClient().SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "hello-ork"})); err != nil {
				t.Fatalf("SubmitRun: %v", err)
			}
			callCtx := authn.WithMode(ctx, tt.mode)
			if tt.principal != nil {
				callCtx = authn.WithPrincipal(callCtx, tt.principal)
			}
			cancel := connect.NewRequest(&orkstackv1.CancelRunRequest{RunId: "run-001", Reason: "stop"})
			cancel.Header().Set(api.AuditPrincipalHeader, tt.header)
			if _, err := f.svc.CancelRun(callCtx, cancel); err != nil {
				t.Fatalf("CancelRun: %v", err)
			}
			f.starter.mu.Lock()
			defer f.starter.mu.Unlock()
			if len(f.starter.cancelRequests) != 1 {
				t.Fatalf("cancel requests = %v, want one", f.starter.cancelRequests)
			}
			if got := f.starter.cancelRequests[0].Principal; got != tt.want {
				t.Fatalf("the cancellation names %q, want %q", got, tt.want)
			}
		})
	}
}
