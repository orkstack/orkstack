package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestRetireSigningKey_Compromised: a retire may mark the key compromised
// from an instant in the past, answered on the key; marking it from
// another instant later is FAILED_PRECONDITION, and an instant in the
// future is refused.
func TestRetireSigningKey_Compromised(t *testing.T) {
	s := newAccessService(t)
	s.issuer = testIssuer(t, "")
	ctx := context.Background()
	keys, _ := s.ListSigningKeys(ctx, connect.NewRequest(&orkstackv1.ListSigningKeysRequest{}))
	kid := keys.Msg.GetKeys()[0].GetKeyId()
	from := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	resp, err := s.RetireSigningKey(ctx, connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: kid, CompromisedFrom: timestamppb.New(from)}))
	if err != nil || !resp.Msg.GetKey().GetCompromisedFrom().AsTime().Equal(from) || resp.Msg.GetKey().GetRetiredAt() == nil {
		t.Fatalf("retire compromised = %v, %v", resp.Msg, err)
	}
	if _, err := s.RetireSigningKey(ctx, connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: kid, CompromisedFrom: timestamppb.New(from.Add(-time.Minute))})); err != nil {
		t.Fatalf("an earlier instant = %v", err)
	}
	if _, err := s.RetireSigningKey(ctx, connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: kid, CompromisedFrom: timestamppb.New(from)})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("a later instant = %v", err)
	}
	if _, err := s.RetireSigningKey(ctx, connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: kid, CompromisedFrom: timestamppb.New(time.Now().Add(time.Hour))})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("a future instant = %v", err)
	}
}

type enginesPolling []engine.EnginePresence

func (p enginesPolling) ObserveEngines(context.Context) ([]engine.EnginePresence, error) {
	return p, nil
}

// TestIdentityService_Off: with no signing key, GetIssuer says task
// identity is off and the others fail FAILED_PRECONDITION saying so.
func TestIdentityService_Off(t *testing.T) {
	s := newAccessService(t)
	resp, err := s.GetIssuer(context.Background(), connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
	if err != nil || resp.Msg.GetEnabled() || resp.Msg.GetIssuer() != "" {
		t.Fatalf("GetIssuer = %v, %v", resp.Msg, err)
	}
	_, err = s.ListSigningKeys(context.Background(), connect.NewRequest(&orkstackv1.ListSigningKeysRequest{}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "identity.signing.key_files") {
		t.Fatalf("ListSigningKeys = %v", err)
	}
	_, err = s.RetireSigningKey(context.Background(), connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: "k"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("RetireSigningKey = %v", err)
	}
}

// TestIdentityService_KeysWithNoIssuer: an engine that holds signing keys
// with identity.issuer unset says task identity is off and serves no
// document, and still lists its keys and retires one.
func TestIdentityService_KeysWithNoIssuer(t *testing.T) {
	s := newAccessService(t)
	s.issuer = testKeysOnly(t)
	ctx := context.Background()
	resp, err := s.GetIssuer(ctx, connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
	if err != nil || resp.Msg.GetEnabled() || resp.Msg.GetIssuer() != "" || resp.Msg.GetDiscovery() != "" || resp.Msg.GetJwks() != "" ||
		resp.Msg.GetKeysSequence() != 1 || resp.Msg.GetKeysReadAt() == nil {
		t.Fatalf("GetIssuer = %v, %v", resp.Msg, err)
	}
	keys, err := s.ListSigningKeys(ctx, connect.NewRequest(&orkstackv1.ListSigningKeysRequest{}))
	if err != nil || len(keys.Msg.GetKeys()) != 1 {
		t.Fatalf("ListSigningKeys = %v, %v", keys.Msg, err)
	}
	retired, err := s.RetireSigningKey(ctx, connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: keys.Msg.GetKeys()[0].GetKeyId()}))
	if err != nil || retired.Msg.GetKey().GetRetiredAt() == nil {
		t.Fatalf("RetireSigningKey = %v, %v", retired.Msg, err)
	}
}

// TestIdentityService_AnswersTheIssuer: GetIssuer answers the settings, the
// three documents exactly as served, and what the engine last read; keys
// are listed with their holders marked live from the engines' presence;
// a retire is recorded with the caller and the key is no longer served.
func TestIdentityService_AnswersTheIssuer(t *testing.T) {
	s := newAccessService(t)
	s.issuer = testIssuer(t, "ork.acme.dev")
	s.enginePresence = enginesPolling{{Identity: "e1 ork-caps=x", LastSeen: time.Now()}}
	ctx := context.Background()
	resp, err := s.GetIssuer(ctx, connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	m := resp.Msg
	disc, _ := s.issuer.Discovery()
	jwks, _ := s.issuer.JWKS()
	bundle, _ := s.issuer.Bundle()
	if !m.GetEnabled() || m.GetIssuer() != "https://ork-id.acme.dev" || m.GetSubjectFormat() != "ork" || m.GetTrustDomain() != "ork.acme.dev" ||
		m.GetDiscovery() != string(disc) || m.GetJwks() != string(jwks) || m.GetSpiffeBundle() != string(bundle) ||
		m.GetKeysSequence() != 1 || m.GetKeysReadAt() == nil || m.GetKeysReadError() != "" {
		t.Fatalf("GetIssuer = %v", m)
	}

	keys, err := s.ListSigningKeys(ctx, connect.NewRequest(&orkstackv1.ListSigningKeysRequest{}))
	if err != nil || len(keys.Msg.GetKeys()) != 1 {
		t.Fatalf("ListSigningKeys = %v, %v", keys.Msg, err)
	}
	k := keys.Msg.GetKeys()[0]
	if !k.GetPublished() || !k.GetHeldHere() || k.GetActive() || k.GetInactiveReason() == "" || k.GetAlgorithm() != string(jwt.ES256) ||
		len(k.GetHolders()) != 1 || !k.GetHolders()[0].GetLive() || k.GetHolders()[0].GetEngine() != "e1" {
		t.Fatalf("key = %v", k)
	}

	if _, err := s.RetireSigningKey(ctx, connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: "nope"})); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("retiring an unknown key = %v", err)
	}
	if _, err := s.RetireSigningKey(ctx, connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("retiring no key = %v", err)
	}
	req := connect.NewRequest(&orkstackv1.RetireSigningKeyRequest{KeyId: k.GetKeyId()})
	req.Header().Set("Ork-Principal", "alice")
	retired, err := s.RetireSigningKey(ctx, req)
	if err != nil || retired.Msg.GetKey().GetRetiredAt() == nil || retired.Msg.GetKey().GetRetiredBy() != "alice" || retired.Msg.GetKey().GetActive() {
		t.Fatalf("RetireSigningKey = %v, %v", retired.Msg, err)
	}
	after, _ := s.GetIssuer(ctx, connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
	if ks, _ := jwt.ParseKeySet([]byte(after.Msg.GetJwks())); ks.Len() != 0 {
		t.Fatalf("a retired key is served: %s", after.Msg.GetJwks())
	}

	s.enginePresence = enginesPolling{}
	keys, _ = s.ListSigningKeys(ctx, connect.NewRequest(&orkstackv1.ListSigningKeysRequest{}))
	if keys.Msg.GetKeys()[0].GetHolders()[0].GetLive() {
		t.Fatal("a holder not on the presence channel read as live")
	}
}

// TestGetIssuer_NamesNoStoreLocation: a failed read of the published keys
// is reported without the store's location.
func TestGetIssuer_NamesNoStoreLocation(t *testing.T) {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	signer, _ := jwt.NewSigner(k)
	store := unreachableKeys{}
	i, err := identity.New(identity.Config{URL: "https://ork-id.acme.dev", Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256,
		Engine: "e1"}, store, noLiveIssuer{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Refresh(context.Background()); err == nil {
		t.Fatal("a refresh against an unreachable store succeeded")
	}
	s := newAccessService(t)
	s.issuer = i
	resp, err := s.GetIssuer(context.Background(), connect.NewRequest(&orkstackv1.GetIssuerRequest{}))
	if err != nil || resp.Msg.GetKeysReadError() == "" || strings.Contains(resp.Msg.GetKeysReadError(), "secret-bucket") {
		t.Fatalf("GetIssuer = %q, %v", resp.Msg.GetKeysReadError(), err)
	}
}

// unreachableKeys fails every call naming the store's location, as the
// S3 store's errors do.
type unreachableKeys struct{}

func (unreachableKeys) Read(context.Context) (artifacts.PublishedKeys, error) {
	return artifacts.PublishedKeys{}, errors.New("artifacts: read s3://secret-bucket/metadata/identity/issuer/jwks.json: boom")
}

func (unreachableKeys) Update(context.Context, func(*artifacts.PublishedKeys) (bool, error)) (artifacts.PublishedKeys, error) {
	return artifacts.PublishedKeys{}, errors.New("artifacts: write s3://secret-bucket/metadata/identity/issuer/jwks.json: boom")
}
