package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// errIdentityOff answers a call that needs the signing keys on an engine
// that holds none.
var errIdentityOff = connect.NewError(connect.CodeFailedPrecondition,
	errors.New("this engine holds no signing key: identity.signing.key_files is not set (docs/TASK_IDENTITY.md)"))

// GetIssuer answers the issuer and its documents as this engine serves
// them (docs/TASK_IDENTITY.md). An engine whose signing keys have no
// issuer says task identity is off, with what it last read of its keys.
func (s *Service) GetIssuer(_ context.Context, _ *connect.Request[orkstackv1.GetIssuerRequest]) (*connect.Response[orkstackv1.GetIssuerResponse], error) {
	if s.issuer == nil {
		return connect.NewResponse(&orkstackv1.GetIssuerResponse{}), nil
	}
	if s.issuer.URL() == "" {
		out := &orkstackv1.GetIssuerResponse{}
		keysStatus(out, s.issuer.Status())
		return connect.NewResponse(out), nil
	}
	out := &orkstackv1.GetIssuerResponse{
		Enabled:        true,
		Issuer:         s.issuer.URL(),
		SubjectFormat:  string(s.issuer.SubjectFormat()),
		TrustDomain:    s.issuer.TrustDomain(),
		ServeDiscovery: s.serveDiscovery,
	}
	disc, err := s.issuer.Discovery()
	if err != nil {
		return nil, s.issuerError("encode the discovery document", err)
	}
	out.Discovery = string(disc)
	// Before the engine has read the published keys it answers no key set
	// and no bundle, and keys_read_at unset says why: never an empty set
	// that an export would upload.
	jwks, err := s.issuer.JWKS()
	if err != nil && !servedButNotNow(err) {
		return nil, s.issuerError("encode the key set", err)
	}
	out.Jwks = string(jwks)
	if out.TrustDomain != "" {
		bundle, err := s.issuer.Bundle()
		if err != nil && !servedButNotNow(err) {
			return nil, s.issuerError("encode the SPIFFE bundle", err)
		}
		out.SpiffeBundle = string(bundle)
	}
	st := s.issuer.Status()
	keysStatus(out, st)
	out.LiveCheckedAt = timestampOrNil(st.LiveAt)
	out.LiveKeyIds = st.LiveKids
	if st.LiveErr != nil {
		out.LiveError = st.LiveErr.Error()
	}
	return connect.NewResponse(out), nil
}

// keysStatus sets what the engine last read of the published keys.
func keysStatus(out *orkstackv1.GetIssuerResponse, st identity.Status) {
	out.KeysSequence = st.Document.Sequence
	out.KeysReadAt = timestampOrNil(st.ReadAt)
	if st.ReadErr != nil {
		// The cause names the store's location, which no caller is told;
		// the engine's log holds it.
		out.KeysReadError = "the published signing keys could not be read or written; the engine log names the cause"
	}
}

// ListSigningKeys answers every key this engine holds and every published
// key, each holder marked live when it polls the engines' presence channel.
func (s *Service) ListSigningKeys(ctx context.Context, _ *connect.Request[orkstackv1.ListSigningKeysRequest]) (*connect.Response[orkstackv1.ListSigningKeysResponse], error) {
	if s.issuer == nil {
		return nil, errIdentityOff
	}
	live := s.liveEngines(ctx)
	out := &orkstackv1.ListSigningKeysResponse{}
	for _, k := range s.issuer.Keys() {
		out.Keys = append(out.Keys, signingKeyMessage(k, live))
	}
	return connect.NewResponse(out), nil
}

// RetireSigningKey retires a key on behalf of the caller.
func (s *Service) RetireSigningKey(ctx context.Context, req *connect.Request[orkstackv1.RetireSigningKeyRequest]) (*connect.Response[orkstackv1.RetireSigningKeyResponse], error) {
	if s.issuer == nil {
		return nil, errIdentityOff
	}
	kid := strings.TrimSpace(req.Msg.GetKeyId())
	if kid == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("key_id is required"))
	}
	by := authn.Recorded(ctx, req.Header()).ID
	var from time.Time
	if ts := req.Msg.GetCompromisedFrom(); ts != nil {
		if err := ts.CheckValid(); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("compromised_from: %w", err))
		}
		from = ts.AsTime()
		if from.IsZero() || from.Before(time.Unix(0, 0)) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("compromised_from must be an instant after 1970; give the earliest moment the key may have leaked"))
		}
		// The earliest moment the key may have leaked lies in the past: an
		// instant to come would keep the key's signatures good until then.
		if from.After(s.now()) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("compromised_from is in the future; give the earliest moment the key may have leaked"))
		}
	}
	if _, err := s.issuer.Retire(ctx, kid, by, from); err != nil {
		if errors.Is(err, identity.ErrAlreadyCompromised) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		if errors.Is(err, identity.ErrKeyNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no published signing key has the id %q", kid))
		}
		if errors.Is(err, artifacts.ErrPublishedKeysContended) {
			return nil, connect.NewError(connect.CodeAborted, errors.New("the published signing keys are being changed by another engine; try again"))
		}
		return nil, s.issuerError("retire the signing key", err)
	}
	for _, k := range s.issuer.Keys() {
		if k.KeyID == kid {
			return connect.NewResponse(&orkstackv1.RetireSigningKeyResponse{Key: signingKeyMessage(k, s.liveEngines(ctx))}), nil
		}
	}
	return nil, s.issuerError("read the retired key back", fmt.Errorf("key %s is missing after its retirement", kid))
}

// issuerError logs what failed and answers the client without the store's
// location: the published keys could not be read or written.
func (s *Service) issuerError(what string, err error) error {
	s.logger.Error("identity: "+what, "err", err)
	if errors.Is(err, artifacts.ErrPublishedKeysUnreadable) {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("%s: the published signing keys document is unreadable; the engine log names the object", what))
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: the published signing keys could not be reached", what))
}

// liveEngines are the identities polling the engines' presence channel,
// or nil when it cannot be read: then no holder is marked live.
func (s *Service) liveEngines(ctx context.Context) []string {
	if s.enginePresence == nil {
		return nil
	}
	engines, err := s.enginePresence.ObserveEngines(ctx)
	if err != nil {
		s.logger.Warn("identity: the engines' presence could not be read; no key holder is shown live", "err", err)
		return nil
	}
	out := make([]string, 0, len(engines))
	for _, e := range engines {
		out = append(out, e.Identity)
	}
	return out
}

// isLive says holder, an engine's base identity, polls now: a presence
// identity is the base followed by what the process advertises.
func isLive(holder string, live []string) bool {
	for _, id := range live {
		if id == holder || strings.HasPrefix(id, holder+" ") {
			return true
		}
	}
	return false
}

func signingKeyMessage(k identity.KeyStatus, live []string) *orkstackv1.SigningKey {
	out := &orkstackv1.SigningKey{
		KeyId:           k.KeyID,
		Algorithm:       string(k.Algorithm),
		Published:       k.Published,
		PublishedAt:     timestampOrNil(k.PublishedAt),
		ActivatedAt:     timestampOrNil(k.ActivatedAt),
		RetiredAt:       timestampOrNil(k.RetiredAt),
		RetiredBy:       k.RetiredBy,
		CompromisedFrom: timestampOrNil(k.CompromisedFrom),
		HeldHere:        k.HeldHere,
		Active:          k.Active,
		InactiveReason:  k.Why,
		Unheld:          k.Unheld,
	}
	if k.Published {
		out.PublishedBy = k.PublishedBy
	}
	for _, h := range k.Holders {
		out.Holders = append(out.Holders, &orkstackv1.SigningKeyHolder{Engine: h.Engine, ConfirmedAt: timestamppb.New(h.ConfirmedAt), Live: isLive(h.Engine, live)})
	}
	return out
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// servedButNotNow says the documents are not served at this moment: not
// read yet, read too long ago, or holding no key. GetIssuer then answers
// them empty, which export refuses.
func servedButNotNow(err error) bool {
	return errors.Is(err, identity.ErrKeysNotRead) || errors.Is(err, identity.ErrKeysStale) || errors.Is(err, identity.ErrNoServedKeys)
}
