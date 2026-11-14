package api

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
)

// TestSubmitOrigin_RecordsWhoAndHow holds a submit's origin to the mode: in
// asserted mode it is the header, verbatim and unverified; in observe and
// enforce mode it is the verified principal with its method, and the header
// is read nowhere.
func TestSubmitOrigin_RecordsWhoAndHow(t *testing.T) {
	verified := &authn.Principal{Kind: authn.KindUser, ID: "user:corp:ada", Method: authn.MethodOIDC, Issuer: "corp"}
	tests := []struct {
		name         string
		mode         string
		principal    *authn.Principal
		header       string
		wantID       string
		wantVerified bool
		wantMethod   string
	}{
		{name: "asserted reads the header", mode: authn.ModeAsserted, header: "alice", wantID: "alice"},
		{name: "asserted ignores a context principal", mode: authn.ModeAsserted, principal: verified, header: "alice", wantID: "alice"},
		{name: "enforce records the verified principal", mode: authn.ModeEnforce, principal: verified, header: "mallory",
			wantID: "user:corp:ada", wantVerified: true, wantMethod: authn.MethodOIDC},
		{name: "observe records the verified principal", mode: authn.ModeObserve, principal: verified, header: "mallory",
			wantID: "user:corp:ada", wantVerified: true, wantMethod: authn.MethodOIDC},
		{name: "observe with no credential records nobody", mode: authn.ModeObserve, header: "mallory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := authn.WithMode(context.Background(), tt.mode)
			if tt.principal != nil {
				ctx = authn.WithPrincipal(ctx, tt.principal)
			}
			req := connect.NewRequest(&orkstackv1.SubmitRunRequest{})
			req.Header().Set(authn.PrincipalHeader, tt.header)
			for _, source := range []string{"", "ork-src"} {
				got := submitOrigin(ctx, req, source)
				if got.GetPrincipal() != tt.wantID || got.GetPrincipalVerified() != tt.wantVerified || got.GetAuthMethod() != tt.wantMethod {
					t.Fatalf("source %q: origin = (%q, verified %v, %q); want (%q, %v, %q)", source,
						got.GetPrincipal(), got.GetPrincipalVerified(), got.GetAuthMethod(), tt.wantID, tt.wantVerified, tt.wantMethod)
				}
			}
		})
	}
}
