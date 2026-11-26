package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The reasons ExplainAccess gives a denial.
const (
	explainNoBinding = "NO_BINDING"
	explainDisabled  = authz.ReasonPrincipalDisabled
)

// The revision listing's bounds.
const (
	defaultRevisionPage = 50
	maxRevisionPage     = 500
	// maxRevisionWalk bounds how far back GetAccessRevision follows the
	// chain from the head to find a revision by number.
	maxRevisionWalk = 4096
)

var explainMatch = map[authz.Match]orkstackv1.BindingMatch{
	authz.MatchGranted:               orkstackv1.BindingMatch_BINDING_MATCH_GRANTED,
	authz.MatchSubjectDiffers:        orkstackv1.BindingMatch_BINDING_MATCH_SUBJECT_DIFFERS,
	authz.MatchScopeDiffers:          orkstackv1.BindingMatch_BINDING_MATCH_SCOPE_DIFFERS,
	authz.MatchRoleLacksPermission:   orkstackv1.BindingMatch_BINDING_MATCH_ROLE_LACKS_PERMISSION,
	authz.MatchPinnedElsewhere:       orkstackv1.BindingMatch_BINDING_MATCH_PINNED_ELSEWHERE,
	authz.MatchSharedOrkerCredential: orkstackv1.BindingMatch_BINDING_MATCH_SHARED_ORKER_CREDENTIAL,
}

func explainedProto(e authz.Explained) *orkstackv1.ExplainedBinding {
	return &orkstackv1.ExplainedBinding{Binding: e.Binding, Match: explainMatch[e.Match], Subject: e.Subject,
		Role: e.Role, Scope: e.Scope, ClaimsReliedOn: e.ClaimsReliedOn}
}

// ExplainAccess decides one permission for one principal, against this
// replica's copy of the access document, and says which bindings did and
// did not grant it. A principal is known here by its id alone, so a binding
// that selects by group or by a claim is shown as not matching it.
func (h accessHandler) ExplainAccess(ctx context.Context, req *connect.Request[orkstackv1.ExplainAccessRequest]) (*connect.Response[orkstackv1.ExplainAccessResponse], error) {
	m := req.Msg
	perm := authz.Permission(m.GetPermission())
	if !authz.Known(perm) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("permission %q is not in the catalogue", m.GetPermission()))
	}
	sub, err := authz.SubjectFromID(m.GetPrincipalId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	res, err := explainResource(m)
	if err != nil {
		return nil, err
	}
	policy, unavailable := h.svc.access.policy(ctx)
	if unavailable || policy == nil {
		return nil, connect.NewError(connect.CodeUnavailable, ErrPolicyUnavailable)
	}
	if res, err = h.svc.withIDs(ctx, policy, res); err != nil {
		return nil, err
	}
	ex := authz.Explain(policy, sub, perm, res)
	out := &orkstackv1.ExplainAccessResponse{AccessRevision: policy.Revision}
	var stale []string
	for _, c := range ex.Considered {
		out.Considered = append(out.Considered, explainedProto(c))
		if c.Match == authz.MatchPinnedElsewhere {
			stale = append(stale, c.Binding)
		}
	}
	var denied *authz.DeniedError
	switch {
	case ex.Err == nil:
		out.Decision = orkstackv1.AccessDecision_ACCESS_DECISION_ALLOWED
		if ex.GrantedBy != nil {
			out.GrantedBy = explainedProto(*ex.GrantedBy)
			out.Explanation = fmt.Sprintf("%s holds %s at %s by binding %q (%s at %s)",
				sub.ID, perm, res.String(), ex.GrantedBy.Binding, ex.GrantedBy.Role, ex.GrantedBy.Scope)
		} else {
			out.Explanation = fmt.Sprintf("every authenticated principal holds %s", perm)
		}
	case errors.As(ex.Err, &denied) && denied.Reason == authz.ReasonPrincipalDisabled:
		out.Decision, out.Reason = orkstackv1.AccessDecision_ACCESS_DECISION_DENIED, explainDisabled
		out.Explanation = fmt.Sprintf("%s is disabled: it holds nothing, whatever its bindings grant", sub.ID)
	case denied != nil:
		out.Decision, out.Reason = orkstackv1.AccessDecision_ACCESS_DECISION_DENIED, explainNoBinding
		out.Explanation = fmt.Sprintf("no binding grants %s %s at %s; groups and claims are not known from a principal id, "+
			"so a binding that selects by them is shown as not matching", sub.ID, perm, res.String())
		if len(stale) > 0 {
			out.Explanation += fmt.Sprintf("; %s would have matched by name but %s pinned to an earlier incarnation of %s "+
				"(orkctl admin binding update NAME --repin pins it again)",
				strings.Join(quoted(stale), ", "), isOrAre(len(stale)), res.String())
		}
	default:
		return nil, connect.NewError(connect.CodeInternal, ex.Err)
	}
	return connect.NewResponse(out), nil
}

// explainResource is the resource an ExplainAccess names, as narrow as
// given: an Orkflow of a namespace (the default's when none is named), a
// namespace, a pool, or the deployment.
func explainResource(m *orkstackv1.ExplainAccessRequest) (authz.Resource, error) {
	invalid := func(err error) (authz.Resource, error) {
		return authz.Resource{}, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if pool := m.GetPool(); pool != "" {
		if m.GetNamespace() != "" || m.GetOrkflow() != "" {
			return invalid(errors.New("a pool is named alone, with no namespace or Orkflow"))
		}
		if err := dsl.ValidSlug("pool", pool); err != nil {
			return invalid(err)
		}
		return authz.Resource{Pool: pool}, nil
	}
	if m.GetNamespace() == "" && m.GetOrkflow() == "" {
		return authz.Resource{}, nil
	}
	ns, err := namespaceOf(m.GetNamespace())
	if err != nil {
		return authz.Resource{}, err
	}
	res := authz.Resource{Namespace: ns}
	if o := m.GetOrkflow(); o != "" {
		if err := dsl.ValidSlug("orkflow name", o); err != nil {
			return invalid(err)
		}
		res.Orkflow = o
	}
	return res, nil
}

func revisionProto(meta artifacts.AccessRevisionMeta) *orkstackv1.AccessRevision {
	r := &orkstackv1.AccessRevision{Revision: meta.Revision, Sha256: meta.SHA256, PreviousSha256: meta.PreviousSHA256,
		WrittenBy: meta.WrittenBy, WrittenByVerified: meta.WrittenByVerified, Change: meta.Change}
	if !meta.WrittenAt.IsZero() {
		r.WrittenAt = timestamppb.New(meta.WrittenAt)
	}
	return r
}

var revisionToken = regexp.MustCompile(`^([1-9][0-9]{0,17}):([0-9a-f]{64})$`)

// head reads the document's head; nil when none was ever written.
func (h accessHandler) head(ctx context.Context) (*artifacts.AccessHead, error) {
	a := h.svc.access
	if a.Policies == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine keeps no access document"))
	}
	readCtx, cancel := context.WithTimeout(ctx, a.Policies.readTime)
	defer cancel()
	head, err := a.Policies.store.Get(readCtx)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read the access document: %w", err))
	}
	return head, nil
}

// revision reads the revision a link names. A revision the chain names
// that is gone ends the history there; any other failure is unavailable.
func (h accessHandler) revision(ctx context.Context, rev int64, sha string) (*artifacts.AccessHead, bool, error) {
	got, err := h.svc.access.Policies.store.GetRevision(ctx, rev, sha)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, false, nil
	case err != nil:
		return nil, false, connect.NewError(connect.CodeUnavailable, err)
	}
	return got, true, nil
}

// ListAccessRevisions lists the document's history newest first, followed
// from the head by each revision's link to the one before it: an orphan a
// lost swap left is never shown.
func (h accessHandler) ListAccessRevisions(ctx context.Context, req *connect.Request[orkstackv1.ListAccessRevisionsRequest]) (*connect.Response[orkstackv1.ListAccessRevisionsResponse], error) {
	size := int(req.Msg.GetPageSize())
	switch {
	case size == 0:
		size = defaultRevisionPage
	case size < 0 || size > maxRevisionPage:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size %d is outside 1..%d", size, maxRevisionPage))
	}
	var rev int64
	var sha string
	out := &orkstackv1.ListAccessRevisionsResponse{}
	if token := req.Msg.GetPageToken(); token != "" {
		parts := revisionToken.FindStringSubmatch(token)
		if parts == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page_token"))
		}
		rev, _ = strconv.ParseInt(parts[1], 10, 64)
		sha = parts[2]
	} else {
		head, err := h.head(ctx)
		if err != nil || head == nil {
			return connect.NewResponse(out), err
		}
		// The history begins at the head as it is: its bytes are its
		// revision's, and a head written around the API that has no revision
		// is still listed, with the digest an acknowledgement names.
		out.Revisions = append(out.Revisions, revisionProto(head.Meta))
		rev, sha = head.Meta.Revision-1, head.Meta.PreviousSHA256
	}
	for len(out.Revisions) < size && rev >= 1 && sha != "" {
		got, ok, err := h.revision(ctx, rev, sha)
		if err != nil {
			return nil, err
		}
		if !ok {
			return connect.NewResponse(out), nil
		}
		out.Revisions = append(out.Revisions, revisionProto(got.Meta))
		rev, sha = got.Meta.Revision-1, got.Meta.PreviousSHA256
	}
	if rev >= 1 && sha != "" {
		out.NextPageToken = strconv.FormatInt(rev, 10) + ":" + sha
	}
	return connect.NewResponse(out), nil
}

// GetAccessRevision reads one revision of the history by its number, found
// by following the chain from the head.
func (h accessHandler) GetAccessRevision(ctx context.Context, req *connect.Request[orkstackv1.GetAccessRevisionRequest]) (*connect.Response[orkstackv1.GetAccessRevisionResponse], error) {
	want := req.Msg.GetRevision()
	if want < 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("revision %d is not a revision; the first is 1", want))
	}
	head, err := h.head(ctx)
	if err != nil {
		return nil, err
	}
	missing := connect.NewError(connect.CodeNotFound, fmt.Errorf("revision %d is not in the access document's history", want))
	if head == nil || want > head.Meta.Revision {
		return nil, missing
	}
	if head.Meta.Revision-want > maxRevisionWalk {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"revision %d is more than %d revisions behind the head; list the history to read it", want, maxRevisionWalk))
	}
	if want == head.Meta.Revision {
		// The head itself, whether or not it has a revision of its own.
		return connect.NewResponse(&orkstackv1.GetAccessRevisionResponse{Revision: revisionProto(head.Meta), Document: revisionDocument(head)}), nil
	}
	rev, sha := head.Meta.Revision-1, head.Meta.PreviousSHA256
	for rev >= want && sha != "" {
		got, ok, err := h.revision(ctx, rev, sha)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, missing
		}
		if rev == want {
			return connect.NewResponse(&orkstackv1.GetAccessRevisionResponse{Revision: revisionProto(got.Meta), Document: got.Document}), nil
		}
		rev, sha = got.Meta.Revision-1, got.Meta.PreviousSHA256
	}
	return nil, missing
}

// revisionDocument is a head's document as a revision's reads: the etag is
// the head's compare-and-swap token, not part of any revision.
func revisionDocument(head *artifacts.AccessHead) *orkstackv1.AccessDocument {
	doc := proto.Clone(head.Document).(*orkstackv1.AccessDocument)
	doc.Etag = ""
	return doc
}

func quoted(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = strconv.Quote(n)
	}
	return out
}

func isOrAre(n int) string {
	if n == 1 {
		return "it is"
	}
	return "they are"
}
