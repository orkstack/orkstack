package api

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Whose an idempotency key is (docs/AUTHENTICATION.md, "Idempotency keys").
// In enforce mode a key is its caller's: the claim's coordinate holds the
// verified principal, so nobody reads, takes or acts for another's key by
// using it. Elsewhere the principal may be a header nobody authenticated, and
// a key is shared by its namespace. A change of mode moves the coordinate, so
// for one claim's life after it a submission reads the other one too.

// claimCoords are the coordinates one submission's key has.
type claimCoords struct {
	// own is where the key is claimed; "" for a submission with no key.
	own string
	// other is where a submission made before the last change of mode may
	// have claimed it, "" when no such claim can still be alive.
	other string
	// otherShared says other is the namespace's own coordinate, shared by
	// everybody who could submit: a claim there may be another caller's.
	otherShared bool
}

// sharedClaimKey is the coordinate of a key its namespace shares: the
// caller's key, scoped by the namespace it was submitted into.
//
// The scope is load-bearing. engine.ClaimWorkflowID hashes this string into a
// Temporal workflow id, and one id is one binding for the whole deployment, so
// an unscoped key makes "nightly" in payments and "nightly" in staging the
// same claim: the second tenant is answered with the first tenant's run id,
// reported as success, and the run they asked for is never started.
//
// A namespace name is a slug (^[a-z][a-z0-9-]*$, pkg/artifacts), so it can
// never contain "/": the first separator splits the pair back apart, and no
// two (namespace, key) pairs can compose to one coordinate.
func sharedClaimKey(namespace, key string) string { return namespace + "/" + key }

// ownedClaim is the coordinate of a key that is its caller's, as canonical
// JSON, so no two compose alike; none is a shared coordinate either, which
// begins with a letter. The caller is a verified principal, or the trigger
// whose token was presented: a trigger's door verifies nobody else.
type ownedClaim struct {
	V         int    `json:"v"`
	Namespace string `json:"namespace"`
	Principal string `json:"principal,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
	Key       string `json:"key"`
}

// ownedClaimKey is the coordinate key has as the caller's own in namespace,
// or "" when the request has no caller anyone verified.
func ownedClaimKey(ctx context.Context, namespace, key string, origin *orkstackv1.RunOrigin) (string, error) {
	c := ownedClaim{V: 1, Namespace: namespace, Key: key}
	if origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER {
		c.Trigger = origin.GetId()
	} else if p := authn.FromContext(ctx); p != nil {
		c.Principal = p.ID
	}
	if c.Principal == "" && c.Trigger == "" {
		return "", nil
	}
	raw, err := stablejson.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("compose the idempotency key's coordinate: %w", err)
	}
	return string(raw), nil
}

// claimCoordsOf are the coordinates of key, submitted into namespace by the
// request's caller, or through the trigger its origin names.
func (s *Service) claimCoordsOf(ctx context.Context, namespace, key string, origin *orkstackv1.RunOrigin) (claimCoords, error) {
	if key == "" {
		return claimCoords{}, nil
	}
	// JSON has one spelling for every invalid byte, so two such keys would be
	// one coordinate.
	if !utf8.ValidString(key) {
		return claimCoords{}, connect.NewError(connect.CodeInvalidArgument, errors.New("idempotency_key is not valid UTF-8"))
	}
	shared := sharedClaimKey(namespace, key)
	if !s.access.enabled() {
		return claimCoords{own: shared}, nil
	}
	owned, err := ownedClaimKey(ctx, namespace, key, origin)
	if err != nil {
		return claimCoords{}, connect.NewError(connect.CodeInternal, err)
	}
	alive := s.access.ModeRecord.otherClaimsAlive(s.access.Mode)
	if !s.access.enforcing() {
		c := claimCoords{own: shared}
		if alive {
			c.other = owned
		}
		return c, nil
	}
	if owned == "" {
		return claimCoords{}, connect.NewError(connect.CodeInternal, errors.New("an idempotency key is its caller's, and this request has no verified caller"))
	}
	c := claimCoords{own: owned}
	if alive {
		c.other, c.otherShared = shared, true
	}
	return c, nil
}

// otherClaimsAlive reports whether a claim made under the other coordinate,
// before the deployment last entered enforce (for an engine that enforces) or
// last left it (for one that observes), can still be alive. A record this
// engine has not read, or that names another mode than its own, answers yes:
// the look costs one read, and not looking could start a second run.
func (g *ModeGuard) otherClaimsAlive(mode string) bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	var since time.Time
	switch {
	case !g.seen || g.record.Mode != mode:
		return true
	case mode == artifacts.AuthModeEnforce:
		since = g.record.RaisedAt
	default:
		since = g.record.LeftEnforceAt
		// A record written again (lost, or forced) holds no time the
		// deployment left enforce: it may have, until the record was written.
		if g.record.RaisedAt.After(since) {
			since = g.record.RaisedAt
		}
	}
	return g.now().Before(since.Add(g.claimLife(g.peerTTL)))
}

// errOtherClaimUnread answers a submission that could not learn whether its
// key was used before the mode changed.
func errOtherClaimUnread(key string, err error) error {
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
		"whether idempotency key %q was used before the authentication mode changed could not be read: %w; submit again under the same key", key, err))
}

// lookupBoth reads both coordinates of a key that a change of mode gave two,
// the caller's own claim first: where both stand it is the one the caller
// made. It answers the submission of the claim found, or bound false. No
// claim is asked at the second coordinate, so a lookup that is not answered
// ends the submission: going on could start a second run, or answer another
// caller's.
func (s *Service) lookupBoth(ctx context.Context, sub submission) (resp *connect.Response[orkstackv1.SubmitRunResponse], bound bool, err error) {
	order := []string{sub.claim.own, sub.claim.other}
	if !sub.claim.otherShared {
		order = []string{sub.claim.other, sub.claim.own}
	}
	for _, at := range order {
		binding, held, err := s.idempotency.Lookup(ctx, at)
		switch {
		case err != nil:
			return nil, false, errOtherClaimUnread(sub.key, err)
		case held && at == sub.claim.own:
			resp, err = s.answerBinding(ctx, sub, binding)
			return resp, true, err
		case held:
			resp, err = s.answerOther(ctx, sub, binding)
			return resp, true, err
		}
	}
	return nil, false, nil
}

// answerOther answers a submission whose key was claimed under its other
// coordinate, as a claim under its own is answered. A coordinate the
// namespace shares may hold another caller's claim: its run is answered, or
// started, only for a caller who may read it.
func (s *Service) answerOther(ctx context.Context, sub submission, binding engine.Claim) (*connect.Response[orkstackv1.SubmitRunResponse], error) {
	// Before the submissions are compared: a caller who may not read the
	// claim's run learns nothing of what was submitted under the key.
	if sub.claim.otherShared {
		if err := s.mayTakeSharedClaim(ctx, sub, binding); err != nil {
			return nil, err
		}
	}
	sub.claim = claimCoords{own: sub.claim.other}
	return s.answerBinding(ctx, sub, binding)
}

// mayTakeSharedClaim refuses a caller whose key names a run it may not read.
// A trigger holds no permission: it takes the claims it made itself, which
// the claim's own snapshot says.
func (s *Service) mayTakeSharedClaim(ctx context.Context, sub submission, binding engine.Claim) error {
	refused := func(why string) error {
		s.logger.Warn("a submission was refused an idempotency key claimed before callers' keys were their own",
			"namespace", sub.namespace, "orkflow", sub.wf.GetName(), "origin", sub.origin.GetKind().String(), "because", why)
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf(
			"idempotency key %q was used before each caller's keys were its own, %s; use another key", sub.key, why))
	}
	run, err := claimedSnapshot(binding)
	if err != nil || run == nil {
		return refused("and whose run it names cannot be told")
	}
	if sub.origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER {
		from := run.GetOrigin()
		if from.GetKind() == sub.origin.GetKind() && from.GetId() == sub.origin.GetId() {
			return nil
		}
		return refused("for a run this trigger did not start")
	}
	policy, unavailable := s.access.policy(ctx)
	_, err = s.authorizeWithIDs(ctx, authz.Request{Action: authz.Action(authz.PermRunRead), Subject: subjectOf(authn.FromContext(ctx)),
		Resource: authz.Resource{Namespace: sub.namespace, Orkflow: run.GetOrkflowName()}, Policy: policy})
	var denied *authz.DeniedError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &denied) && !unavailable:
		return refused("for a run this caller may not read (" + string(authz.PermRunRead) + ")")
	default:
		return errOtherClaimUnread(sub.key, fmt.Errorf("whether the caller may read its run could not be decided: %w", err))
	}
}
