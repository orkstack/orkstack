package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The decisions an audit record names.
const (
	DecisionAllowed   = "allowed"
	DecisionDenied    = "denied"
	DecisionWouldDeny = "would-deny"
)

// AuthCounter is the telemetry seam of authentication and authorization.
// Nil is a no-op.
type AuthCounter interface {
	// CountRequest counts one request that presented a credential.
	CountRequest(ctx context.Context, method, outcome string)
	// CountDecision counts one decision: decision is allowed, denied or
	// would-deny.
	CountDecision(ctx context.Context, permission, decision string)
	// Annotate sets a request's span attributes: the principal's kind, the
	// method and the decision; never the principal's id.
	Annotate(ctx context.Context, kind, method, decision string)
	// CountStreamClosed counts an open stream or socket closed by a
	// re-check: reason is denied, disabled, unavailable or expired.
	CountStreamClosed(ctx context.Context, reason string)
}

// AccessControl is authentication's and authorization's state on one
// replica. Nil, or a mode of asserted, is v0.3.0: nothing is installed.
type AccessControl struct {
	// Mode is observe or enforce.
	Mode string
	// Policies is the replica's copy of the access document.
	Policies *PolicyCache
	// Configured are the bindings configuration makes (auth.admins,
	// break-glass): what still authorizes once the document is past its
	// stale limit.
	Configured []authz.Binding
	// Authenticator verifies credentials (the HTTP middleware).
	Authenticator *authn.Authenticator
	// Signins is sign-in for the web UI; nil where no issuer offers it.
	Signins *authn.Signins
	// ModeRecord is this engine's side of the mode record.
	ModeRecord *ModeGuard
	// Fleet reports the engines seen and those that do not enforce.
	Fleet   *FleetReport
	Counter AuthCounter
	Logger  *slog.Logger
	Now     func() time.Time

	denials *denialLimiter
	once    sync.Once
	// rechecked, when set, is told after every re-check of an open stream:
	// a test's positive control that the watcher ran.
	rechecked func(err error)
	// signinCheck replaces authn.SigninCheckInterval in a test.
	signinCheck time.Duration
}

func (a *AccessControl) init() {
	a.once.Do(func() {
		if a.Logger == nil {
			a.Logger = slog.Default()
		}
		if a.Now == nil {
			a.Now = time.Now
		}
		a.denials = newDenialLimiter(a.Now)
	})
}

// enabled reports whether requests are authorized at all.
func (a *AccessControl) enabled() bool {
	return a != nil && (a.Mode == authn.ModeObserve || a.Mode == authn.ModeEnforce)
}

func (a *AccessControl) enforcing() bool { return a.Mode == authn.ModeEnforce }

// accessDecision is what admitted a request, on its context: the audit
// interceptor records it, and a handler that starts something longer than
// the request reads it.
type accessDecision struct {
	Permission authz.Permission
	Scope      string
	Rule       string
	Generation int64
	Decision   string
}

type decisionKey struct{}

func withDecision(ctx context.Context, d *accessDecision) context.Context {
	return context.WithValue(ctx, decisionKey{}, d)
}

func decisionFrom(ctx context.Context) *accessDecision {
	d, _ := ctx.Value(decisionKey{}).(*accessDecision)
	return d
}

// subjectOf is the verified caller as authz reads it.
func subjectOf(p *authn.Principal) *authz.Subject {
	if p == nil {
		return nil
	}
	return &authz.Subject{ID: p.ID, Kind: string(p.Kind), Issuer: p.Issuer, Groups: p.Groups, Claims: p.Claims}
}

// admission is one request's decision: each check's resource, kept so a
// stream is checked again against the same facts and nothing is read twice.
type admission struct {
	row        accessRow
	checks     []resolvedCheck
	filterRows bool // a list answered with the rows the caller may read
	decision   *accessDecision
	// refused is true when the request was refused by the policy, not by a
	// lookup that failed: only a refusal is a denial to record.
	refused bool
}

type resolvedCheck struct {
	perm     authz.Permission
	resource authz.Resource
}

// policy is the document a decision is made against, or configuration's
// bindings alone once the document is past its stale limit; unavailable
// says which.
func (a *AccessControl) policy(ctx context.Context) (p *authz.Policy, unavailable bool) {
	if a.Policies != nil {
		if p, err := a.Policies.Policy(ctx); err == nil {
			return p, false
		}
		if last := a.Policies.Last(); last != nil {
			return last.ConfigurationOnly(), true
		}
	}
	p, _ = authz.Compile(nil, a.Configured, a.Now())
	return p, true
}

const (
	orkstackNamespaceUpdate = "/orkstack.v1.NamespaceService/UpdateNamespace"
	accessServicePrefix     = "/orkstack.v1.AccessService/"
)

// decide asks every applicable check of a row and returns what admitted
// the request, or why not.
func (a *AccessControl) decide(ctx context.Context, s *Service, procedure string, msg proto.Message) (*admission, error) {
	a.init()
	row, ok := accessRows[procedure]
	subject := subjectOf(authn.FromContext(ctx))
	if !ok {
		// Deny by default: a procedure this build has no row for.
		return &admission{}, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s has no permission row; it is refused", procedure))
	}
	adm := &admission{row: row}
	if row.public {
		return adm, nil
	}
	if row.noPermission || row.decidedByHandler {
		if subject == nil {
			return adm, connect.NewError(connect.CodeUnauthenticated, errors.New("an authenticated caller is required"))
		}
		return adm, nil
	}
	policy, unavailable := a.policy(ctx)
	deep := map[*resolver]authz.Resource{}
	// resolve answers a resource with the ids of the incarnation it names
	// when a pinned binding could tell incarnations apart (withIDs).
	resolve := func(r *resolver, shallow bool) (authz.Resource, error) {
		if shallow || r.deep == nil {
			res, err := r.shallow(msg)
			if err != nil {
				return res, err
			}
			return s.withIDs(ctx, policy, res)
		}
		if res, ok := deep[r]; ok {
			return res, nil
		}
		res, err := r.deep(ctx, s, msg)
		if err == nil {
			res, err = s.withIDs(ctx, policy, res)
		}
		if err == nil {
			deep[r] = res
		}
		return res, err
	}
	var first *accessDecision
	for i, c := range row.checks {
		if c.when != nil {
			if procedure == orkstackNamespaceUpdate && c.perm == authz.PermNamespacePolicyUpdate {
				if !s.policyChange(ctx, msg) {
					continue
				}
			} else if !c.when(msg) {
				continue
			}
		}
		r := &adm.row.res
		if c.res != nil {
			r = c.res
		}
		res, err := resolve(r, true)
		if err != nil {
			return adm, err
		}
		decision, err := authz.Authorize(authz.Request{Action: authz.Action(c.perm), Subject: subject, Resource: res, Policy: policy})
		var shallowErr error
		if err != nil && r.deep != nil && subject != nil {
			shallowErr = err
			res, err = resolve(r, false)
			if err != nil {
				// A caller refused at the shallow resource learns nothing from
				// the lookup: a missing object reads as the refusal it would
				// have had. Only an outage is said as one.
				if connect.CodeOf(err) == connect.CodeUnavailable {
					return adm, err
				}
				adm.refused = true
				adm.decision = &accessDecision{Permission: c.perm, Scope: res.String(), Generation: policy.Revision, Decision: DecisionDenied}
				return adm, a.refusal(shallowErr, row, msg)
			}
			decision, err = authz.Authorize(authz.Request{Action: authz.Action(c.perm), Subject: subject, Resource: res, Policy: policy})
		}
		if err != nil && c.orElse != nil && subject != nil && !unavailable {
			// The row's own permission is refused: the one it admits instead
			// (access.bind for a binding of the caller's namespace) is asked
			// at its own resource. The handler then decides the rest.
			alt := *c.orElse
			altRes, rerr := resolve(alt.res, false)
			if rerr != nil && connect.CodeOf(rerr) == connect.CodeUnavailable {
				return adm, rerr
			}
			if rerr == nil {
				if d, aerr := authz.Authorize(authz.Request{Action: authz.Action(alt.perm), Subject: subject, Resource: altRes, Policy: policy}); aerr == nil {
					decision, err, res, c = d, nil, altRes, alt
				}
			}
		}
		if err != nil && i == 0 && row.list != nil && subject != nil {
			// A list a caller holds part of: its own rows, or its filter.
			ok, lerr := a.listAdmits(ctx, s, row.list, msg, subject, policy, c.perm)
			if ok {
				adm.filterRows = row.list.rows != ""
				adm.checks = append(adm.checks, resolvedCheck{perm: c.perm, resource: res})
				first = &accessDecision{Permission: c.perm, Scope: res.String(), Rule: "filtered",
					Generation: policy.Revision, Decision: DecisionAllowed}
				continue
			}
			if lerr != nil {
				err = lerr
			}
		}
		if err != nil {
			adm.refused = true
			adm.decision = &accessDecision{Permission: c.perm, Scope: res.String(), Generation: policy.Revision, Decision: DecisionDenied}
			if unavailable {
				return adm, connect.NewError(connect.CodeUnavailable, fmt.Errorf("%w; only configuration's principals are served", ErrPolicyUnavailable))
			}
			// A refusal after a lookup names what the caller asked about,
			// never what the lookup found; the audit record keeps the
			// decided scope.
			var denied *authz.DeniedError
			if shallowErr != nil && errors.As(err, &denied) && denied.Reason == authz.ReasonPermissionDenied {
				err = shallowErr
			}
			if c.named {
				row.notFound = false
			}
			return adm, a.refusal(err, row, msg)
		}
		adm.checks = append(adm.checks, resolvedCheck{perm: c.perm, resource: res})
		if first == nil {
			first = &accessDecision{Permission: c.perm, Scope: decision.Scope, Rule: decision.Rule,
				Generation: decision.Generation, Decision: DecisionAllowed}
		}
	}
	if len(adm.checks) == 0 {
		// Every check skipped: a row that asks nothing admits nothing.
		return adm, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s asked no permission of this request; it is refused", procedure))
	}
	adm.decision = first
	adm.filterRows = adm.filterRows || row.artifactKinds
	return adm, nil
}

// listAdmits decides a list for a caller who does not hold it on the whole
// scope: a list of rows that name their own scope is answered with the
// caller's rows; a list paged by a key walk or a cursor that names its last
// row must carry a filter whose every value the caller may read, so no page
// and no cursor ever names a row the caller may not see.
func (a *AccessControl) listAdmits(ctx context.Context, s *Service, l *listRule, msg proto.Message, subject *authz.Subject, policy *authz.Policy, perm authz.Permission) (bool, error) {
	if l.filterField == "" {
		// The caller's rows only, and only where it holds something: a
		// caller who holds nothing in the namespace learns nothing of it,
		// not even whether it exists.
		ns := ""
		if l.rows != "webhooks" {
			ns, _ = namespaceOf(strField(msg, "namespace"))
		}
		return l.rows != "" && policy.HoldsWithin(subject, perm, ns), nil
	}
	values, ok := filterValues(msg, l.filterField)
	if !ok {
		return false, connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("%s: %s of the whole scope is not yours; add a %s= filter naming what you may read",
				authz.ReasonPermissionDenied, perm, l.filterField))
	}
	for _, v := range values {
		res, err := s.withIDs(ctx, policy, l.filterResource(msg, v))
		if err != nil {
			return false, err
		}
		if _, err := authz.Authorize(authz.Request{Action: authz.Action(perm), Subject: subject, Resource: res, Policy: policy}); err != nil {
			return false, err
		}
	}
	return true, nil
}

// filterValues is the values of the one EQUALS or IN filter on name; a
// list with no such filter, two of them, or another operator keeps nothing
// inside.
func filterValues(msg proto.Message, name string) ([]string, bool) {
	fd, m, ok := field(msg, "filters")
	if !ok || !fd.IsList() {
		return nil, false
	}
	list := m.Get(fd).List()
	var out []string
	found := false
	for i := 0; i < list.Len(); i++ {
		f, ok := list.Get(i).Message().Interface().(*orkstackv1.FieldFilter)
		if !ok || f.GetField() != name {
			continue
		}
		switch f.GetOp() {
		case orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, orkstackv1.FilterOperator_FILTER_OPERATOR_IN:
		default:
			return nil, false
		}
		if found || len(f.GetValues()) == 0 {
			return nil, false
		}
		found, out = true, f.GetValues()
	}
	return out, found
}

// refusal is a denial as the caller sees it: a run read answers not_found,
// a caller nobody authenticated unauthenticated, anyone else
// permission_denied with the permission and the scope.
func (a *AccessControl) refusal(err error, row accessRow, msg proto.Message) error {
	var denied *authz.DeniedError
	if !errors.As(err, &denied) {
		if connect.CodeOf(err) != connect.CodeUnknown {
			return err
		}
		return connect.NewError(connect.CodePermissionDenied, err)
	}
	if denied.Reason == authz.ReasonUnauthenticated {
		return connect.NewError(connect.CodeUnauthenticated, denied)
	}
	if row.notFound {
		return errNotFoundRun(msg)
	}
	return connect.NewError(connect.CodePermissionDenied, denied)
}

// recheck decides an open stream again against the same resources, with
// nothing read but the policy.
func (a *AccessControl) recheck(ctx context.Context, adm *admission) error {
	if adm.row.noPermission || adm.row.public || adm.row.decidedByHandler {
		return nil
	}
	subject := subjectOf(authn.FromContext(ctx))
	policy, unavailable := a.policy(ctx)
	for _, c := range adm.checks {
		if _, err := authz.Authorize(authz.Request{Action: authz.Action(c.perm), Subject: subject, Resource: c.resource, Policy: policy}); err != nil {
			if unavailable {
				return connect.NewError(connect.CodeUnavailable, ErrPolicyUnavailable)
			}
			var denied *authz.DeniedError
			if errors.As(err, &denied) && (denied.Reason == authz.ReasonUnauthenticated || denied.Reason == authz.ReasonPrincipalDisabled) {
				return connect.NewError(connect.CodeUnauthenticated, err)
			}
			return connect.NewError(connect.CodePermissionDenied, err)
		}
	}
	return nil
}

// admitCall is decide with the mode applied: enforce returns the refusal;
// observe records it as would-deny and serves the request.
func (s *Service) admitCall(ctx context.Context, procedure string, msg proto.Message, peer string) (context.Context, *admission, error) {
	a := s.access
	// The access document and the mode record are enforced in every mode:
	// observe must not let anyone read who holds what, or lower the mode a
	// rollout to enforce has already raised.
	enforcing := a.enforcing() || strings.HasPrefix(procedure, accessServicePrefix)
	adm, err := a.decide(ctx, s, procedure, msg)
	decision := adm.decision
	if decision == nil {
		decision = &accessDecision{Decision: DecisionAllowed}
	}
	if err != nil {
		decision.Decision = DecisionDenied
		if !enforcing {
			decision.Decision = DecisionWouldDeny
			adm.filterRows = false
		}
		if a.Counter != nil {
			a.Counter.CountDecision(ctx, string(decision.Permission), decision.Decision)
		}
		// A denied write in enforce mode, and every denied read, write a
		// denial record; a write served in observe mode is recorded by the
		// audit interceptor with its would-deny.
		if _, audited := auditedProcedures[procedure]; adm.refused && (!audited || enforcing) {
			s.recordDenial(ctx, procedure, peer, decision, err)
		}
		if enforcing {
			return ctx, adm, err
		}
	} else if a.Counter != nil {
		a.Counter.CountDecision(ctx, string(decision.Permission), decision.Decision)
	}
	if !enforcing {
		adm.filterRows = false // observe changes no answer
	}
	if a.Counter != nil {
		kind, method := "", ""
		if p := authn.FromContext(ctx); p != nil {
			kind, method = string(p.Kind), p.Method
		}
		a.Counter.Annotate(ctx, kind, method, decision.Decision)
	}
	return withDecision(ctx, decision), adm, nil
}

// recordDenial writes a denial's audit record, of a read as well: who tried
// is evidence. It is coalesced (denialLimiter), and a caller nobody
// authenticated writes nothing: an anonymous caller must not be able to
// write objects.
func (s *Service) recordDenial(ctx context.Context, procedure, peer string, d *accessDecision, err error) {
	p := authn.FromContext(ctx)
	if p == nil || s.audit == nil {
		return
	}
	if !s.access.denials.allow(p.ID, procedure, d.Scope) {
		return
	}
	at := s.audit.now().UTC()
	// The namespace is the decided scope's, never the raw request's.
	ns := ""
	if scope, perr := authz.ParseScope(d.Scope); perr == nil {
		ns = scope.Namespace
	}
	rec := &orkstackv1.AuditRecord{
		Id: artifacts.NewAuditID(at), At: timestamppb.New(at),
		SourceAddress: peer, Action: "access.denied", Procedure: procedure,
		Namespace: ns, Target: auditNoTarget, Detail: err.Error(), Succeeded: false,
		ErrorCode: connect.CodeOf(err).String(),
	}
	stampDecision(ctx, rec, d)
	s.audit.Record(ctx, rec)
}

// stampDecision sets a record's principal and, outside asserted mode, its
// version-2 fields from the request's principal and decision. A record in
// asserted mode is version 1, byte for byte.
func stampDecision(ctx context.Context, rec *orkstackv1.AuditRecord, d *accessDecision) {
	if authn.ModeFrom(ctx) == authn.ModeAsserted {
		return
	}
	who := authn.Recorded(ctx, nil)
	rec.Principal = who.ID
	rec.PrincipalVerified = who.Verified
	rec.AuthMethod = who.Method
	rec.PrincipalIssuer = who.Issuer
	rec.Signin = who.Signin
	if d != nil {
		rec.Permission = string(d.Permission)
		rec.Scope = d.Scope
		rec.Decision = d.Decision
		rec.Rule = d.Rule
		rec.PolicyGeneration = d.Generation
	}
	rec.SchemaVersion = artifacts.AuditSchemaVersionVerified
}

// filterRowsOf removes from a list's response the rows the caller may not
// read (docs/ACCESS.md, "Lists"). Each row names its own scope, so this is
// exact and reads nothing but, for deliveries, their subscriptions.
func (s *Service) filterRowsOf(ctx context.Context, adm *admission, req, resp proto.Message) error {
	l := adm.row.list
	if adm.row.artifactKinds {
		l = &listRule{rows: "artifacts"}
	}
	if l == nil || l.rows == "" || resp == nil || len(adm.checks) == 0 {
		return nil
	}
	fd, m, ok := field(resp, l.rows)
	if !ok || !fd.IsList() {
		return nil
	}
	subject := subjectOf(authn.FromContext(ctx))
	policy, _ := s.access.policy(ctx)
	base := adm.checks[0]
	hooks := map[string]authz.Resource{}
	// A row whose incarnation cannot be read fails the list: a pinned
	// binding is never decided against an unknown id, and a list that
	// silently left rows out would mislead whoever reads it.
	var idErr error
	allowed := func(row proto.Message) bool {
		perm, res := base.perm, base.resource
		switch {
		case l.rowResource != nil:
			res = l.rowResource(req, row)
		case l.rows == "artifacts":
			if a, ok := row.(*orkstackv1.RunArtifact); ok {
				perm = artifactPermission(a.GetKind())
			}
		case l.rows == "deliveries":
			id := strField(row, "webhook_id")
			r, ok := hooks[id]
			if !ok {
				var err error
				if r, err = s.webhookResource(ctx, id); err != nil {
					return false
				}
				hooks[id] = r
			}
			res = r
		}
		res, err := s.withIDs(ctx, policy, res)
		if err != nil {
			if idErr == nil {
				idErr = err
			}
			return false
		}
		_, err = authz.Authorize(authz.Request{Action: authz.Action(perm), Subject: subject, Resource: res, Policy: policy})
		return err == nil
	}
	list := m.Mutable(fd).List()
	kept := 0
	for i := 0; i < list.Len(); i++ {
		v := list.Get(i)
		if allowed(v.Message().Interface()) {
			list.Set(kept, v)
			kept++
		}
	}
	if idErr != nil {
		return idErr
	}
	list.Truncate(kept)
	return nil
}

// accessInterceptor authorizes every RPC, unary and streaming, first in
// the chain.
func (s *Service) accessInterceptor() connect.Interceptor { return accessInterceptor{s: s} }

type accessInterceptor struct{ s *Service }

func (i accessInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		msg, _ := req.Any().(proto.Message)
		ctx, adm, err := i.s.admitCall(ctx, req.Spec().Procedure, msg, req.Peer().Addr)
		if err != nil {
			return nil, err
		}
		resp, err := next(ctx, req)
		if err == nil && resp != nil && adm.filterRows {
			if out, ok := resp.Any().(proto.Message); ok {
				if ferr := i.s.filterRowsOf(ctx, adm, msg, out); ferr != nil {
					return nil, ferr
				}
			}
		}
		return resp, err
	}
}

func (i accessInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler decides a server stream on its one request message,
// then checks it again at every change of the access document, closing it
// when the caller no longer may (docs/ACCESS.md, "Open streams").
func (i accessInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		first := &firstMessageConn{StreamingHandlerConn: conn}
		msg, err := first.peek()
		if err != nil {
			return err
		}
		ctx, adm, err := i.s.admitCall(ctx, conn.Spec().Procedure, msg, conn.Peer().Addr)
		if err != nil {
			return err
		}
		ctx, stop := i.s.watchAccess(ctx, adm)
		defer stop()
		err = next(ctx, first)
		if cause := context.Cause(ctx); cause != nil && errors.Is(cause, errAccessWithdrawn) {
			return cause
		}
		return err
	}
}

// errAccessWithdrawn marks a stream closed by a re-check.
var errAccessWithdrawn = errors.New("access withdrawn")

type withdrawn struct{ err error }

func (w withdrawn) Error() string   { return w.err.Error() }
func (w withdrawn) Unwrap() []error { return []error{w.err, errAccessWithdrawn} }

// watchAccess cancels ctx, with the refusal as its cause, at the first
// change of the access document under which the request is no longer
// allowed. In observe mode it never cancels.
func (s *Service) watchAccess(ctx context.Context, adm *admission) (context.Context, func()) {
	if s.access == nil || s.access.Policies == nil || !s.access.enforcing() {
		return ctx, func() {}
	}
	ctx, cancel := context.WithCancelCause(ctx)
	changes, unsubscribe := s.access.Policies.Subscribe()
	var expired <-chan time.Time
	var timer *time.Timer
	if p := authn.FromContext(ctx); p != nil && !p.Expires.IsZero() {
		timer = time.NewTimer(time.Until(p.Expires))
		expired = timer.C
	}
	// A stream opened under a sign-in closes once the sign-in ends (signed
	// out, revoked), read from the store every SigninCheckInterval, not only
	// at its revalidation.
	var signinTick <-chan time.Time
	stopTicker := func() {}
	signin := authn.SigninFrom(ctx)
	if signin != nil && s.access.Signins != nil {
		interval := authn.SigninCheckInterval
		if s.access.signinCheck > 0 {
			interval = s.access.signinCheck
		}
		ticker := time.NewTicker(interval)
		signinTick = ticker.C
		stopTicker = ticker.Stop
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer stopTicker()
		if timer != nil {
			defer timer.Stop()
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-signinTick:
				err := s.access.Signins.Alive(ctx, signin)
				if err != nil && !errors.Is(err, authn.ErrSigninEnded) && ctx.Err() == nil {
					s.logger.Warn("an open stream's sign-in could not be read; the stream stays open until its revalidation",
						"signin", signin.PublicID, "error", err)
				}
				if errors.Is(err, authn.ErrSigninEnded) {
					if s.access.Counter != nil {
						s.access.Counter.CountStreamClosed(ctx, "signed_out")
					}
					cancel(connect.NewError(connect.CodeUnauthenticated, withdrawn{err: authn.ErrSigninEnded}))
					return
				}
			case <-expired:
				if s.access.Counter != nil {
					s.access.Counter.CountStreamClosed(ctx, "expired")
				}
				err := errors.New("the credential this stream was opened with has expired")
				cancel(connect.NewError(connect.CodeUnauthenticated, withdrawn{err: err}))
				return
			case <-changes:
				err := s.access.recheck(ctx, adm)
				if s.access.rechecked != nil {
					s.access.rechecked(err)
				}
				if err != nil {
					reason := "denied"
					switch connect.CodeOf(err) {
					case connect.CodeUnavailable:
						reason = "unavailable"
					case connect.CodeUnauthenticated:
						reason = "disabled"
					}
					if s.access.Counter != nil {
						s.access.Counter.CountStreamClosed(ctx, reason)
					}
					cancel(connect.NewError(connect.CodeOf(err), withdrawn{err: err}))
					return
				}
			}
		}
	}()
	return ctx, func() {
		unsubscribe()
		cancel(nil)
		<-done
	}
}

// firstMessageConn reads a server stream's one request message for the
// decision and hands the same message to the handler.
type firstMessageConn struct {
	connect.StreamingHandlerConn
	msg      proto.Message
	received bool
}

func (c *firstMessageConn) peek() (proto.Message, error) {
	md, ok := c.StreamingHandlerConn.Spec().Schema.(protoreflect.MethodDescriptor)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, errors.New("a stream's schema is not a method descriptor"))
	}
	var msg proto.Message
	if mt, err := protoregistry.GlobalTypes.FindMessageByName(md.Input().FullName()); err == nil {
		msg = mt.New().Interface()
	} else {
		msg = dynamicpb.NewMessage(md.Input())
	}
	if err := c.StreamingHandlerConn.Receive(msg); err != nil {
		return nil, err
	}
	c.msg, c.received = msg, true
	return msg, nil
}

func (c *firstMessageConn) Receive(m any) error {
	if c.received {
		c.received = false
		dst, ok := m.(proto.Message)
		if !ok {
			return connect.NewError(connect.CodeInternal, errors.New("a stream's message is not a proto message"))
		}
		proto.Reset(dst)
		proto.Merge(dst, c.msg)
		return nil
	}
	return c.StreamingHandlerConn.Receive(m)
}

// denialLimiter coalesces denial records: at most one per principal,
// procedure and scope a minute on this replica, and at most ten per
// principal a minute across all scopes. It is in memory, and a restart
// forgets it: the count of records is a floor (docs/ACCESS.md).
type denialLimiter struct {
	mu    sync.Mutex
	now   func() time.Time
	keys  map[string]time.Time
	per   map[string][]time.Time
	swept time.Time
}

const (
	denialWindow       = time.Minute
	denialsPerMinute   = 10
	denialLimiterBound = 100_000
)

func newDenialLimiter(now func() time.Time) *denialLimiter {
	return &denialLimiter{now: now, keys: map[string]time.Time{}, per: map[string][]time.Time{}}
}

func (l *denialLimiter) allow(principal, procedure, scope string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if len(l.keys) >= denialLimiterBound && now.Sub(l.swept) >= denialWindow {
		// Swept at most once a window: at the bound, a sweep per call would
		// hold the lock for a full scan on every denial.
		l.swept = now
		for k, at := range l.keys {
			if now.Sub(at) >= denialWindow {
				delete(l.keys, k)
			}
		}
		for p, times := range l.per {
			if len(times) == 0 || now.Sub(times[len(times)-1]) >= denialWindow {
				delete(l.per, p)
			}
		}
	}
	if len(l.keys) >= denialLimiterBound {
		return false
	}
	key := principal + "\x00" + procedure + "\x00" + scope
	if at, ok := l.keys[key]; ok && now.Sub(at) < denialWindow {
		return false
	}
	recent := l.per[principal][:0]
	for _, at := range l.per[principal] {
		if now.Sub(at) < denialWindow {
			recent = append(recent, at)
		}
	}
	if len(recent) >= denialsPerMinute {
		l.per[principal] = recent
		return false
	}
	l.keys[key] = now
	l.per[principal] = append(recent, now)
	return true
}
