package api

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"go.yaml.in/yaml/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The permission of every RPC (docs/ACCESS.md, "Every RPC and route"). Each
// procedure has exactly one row; TestAuthz_EveryProcedureIsClassified holds
// the generated descriptors to it, and the generated matrix test holds each
// row to its permissions and its scope.

// resolver derives the resource a request names. shallow reads nothing and
// may be wider than the truth (a namespace for a run); deep reads what it
// must (the run's Orkflow) and is exact. A wider resource allowed implies
// every narrower one is, so a caller allowed at the shallow resource is
// decided there and nothing is read.
type resolver struct {
	shallow func(msg proto.Message) (authz.Resource, error)
	deep    func(ctx context.Context, s *Service, msg proto.Message) (authz.Resource, error)
}

// check is one permission a row asks, at its own resource (the row's when
// res is zero), when it applies to the request.
type check struct {
	perm authz.Permission
	res  *resolver
	when func(msg proto.Message) bool
	// orElse is the check that admits a request this one refuses: its own
	// permission, at its own resource (which must be set).
	orElse *check
}

// listRule is how a list row answers a caller who does not hold the
// permission on the whole scope the request names (docs/ACCESS.md, "Lists").
type listRule struct {
	// rows is the response's repeated field, and rowResource each row's own
	// resource; rows the caller may not read are left out.
	rows        string
	rowResource func(req, row proto.Message) authz.Resource
	// filterField, when set, is the request filter that must keep the list
	// inside what the caller may read; filterResource is the resource each
	// of its values names.
	filterField    string
	filterResource func(req proto.Message, value string) authz.Resource
}

type accessRow struct {
	res    resolver
	checks []check
	// notFound answers a denial as a run that does not exist: a refusal
	// would tell a caller which run ids are real.
	notFound bool
	list     *listRule
	// noPermission is a row any authenticated principal may call on itself.
	noPermission bool
	// artifactKinds leaves out of an admitted answer the artifacts whose
	// kind the caller may not read: logs and artifact bytes have their own
	// permissions wherever they are listed.
	artifactKinds bool
}

func perms(ps ...authz.Permission) []check {
	out := make([]check, len(ps))
	for i, p := range ps {
		out[i] = check{perm: p}
	}
	return out
}

// --- field readers -------------------------------------------------------

func field(msg proto.Message, name string) (protoreflect.FieldDescriptor, protoreflect.Message, bool) {
	if msg == nil {
		return nil, nil, false
	}
	m := msg.ProtoReflect()
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
	return fd, m, fd != nil
}

func strField(msg proto.Message, name string) string {
	fd, m, ok := field(msg, name)
	if !ok || fd.Kind() != protoreflect.StringKind || fd.IsList() {
		return ""
	}
	return m.Get(fd).String()
}

func boolField(msg proto.Message, name string) bool {
	fd, m, ok := field(msg, name)
	return ok && fd.Kind() == protoreflect.BoolKind && m.Get(fd).Bool()
}

func msgField(msg proto.Message, name string) proto.Message {
	fd, m, ok := field(msg, name)
	if !ok || fd.Kind() != protoreflect.MessageKind || fd.IsList() || !m.Has(fd) {
		return nil
	}
	return m.Get(fd).Message().Interface()
}

func namespaceOf(name string) (string, error) {
	if name == "" {
		return artifacts.DefaultNamespace, nil
	}
	if err := dsl.ValidSlug("namespace", name); err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument, err)
	}
	return name, nil
}

// --- resolvers -----------------------------------------------------------

var deployment = resolver{shallow: func(proto.Message) (authz.Resource, error) { return authz.Resource{}, nil }}

// bindingRequested is the namespace of the binding a request carries: the
// deployment for a binding of any other scope, which no delegation makes.
var bindingRequested = resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
	return bindingNamespace(strField(msgField(msg, "binding"), "scope")), nil
}}

// bindingStored is the namespace of the stored binding a request names, read
// from this replica's copy of the access document; the deployment when there
// is none of that name.
var bindingStored = resolver{
	shallow: func(proto.Message) (authz.Resource, error) { return authz.Resource{}, nil },
	deep: func(_ context.Context, s *Service, msg proto.Message) (authz.Resource, error) {
		if s.access.Policies == nil {
			return authz.Resource{}, nil
		}
		name := strField(msg, "name")
		if p := s.access.Policies.Last(); p != nil {
			for _, b := range p.Bindings() {
				if b.Name == name && !b.FromConfiguration {
					return bindingNamespace(b.Scope.String()), nil
				}
			}
		}
		return authz.Resource{}, nil
	},
}

func bindingNamespace(scope string) authz.Resource {
	sc, err := authz.ParseScope(scope)
	if err != nil || (sc.Kind != authz.ScopeNamespace && sc.Kind != authz.ScopeOrkflow) {
		return authz.Resource{}
	}
	return authz.Resource{Namespace: sc.Namespace}
}

// inNamespace is the namespace a field names.
func inNamespace(nsField string) resolver {
	return resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
		ns, err := namespaceOf(strField(msg, nsField))
		return authz.Resource{Namespace: ns}, err
	}}
}

// namespaceDocument is the namespace a request's document names
// (CreateNamespace, UpdateNamespace).
var namespaceDocument = resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
	ns, err := namespaceOf(strField(msgField(msg, "namespace"), "name"))
	return authz.Resource{Namespace: ns}, err
}}

// orkflowField is the Orkflow a field names in the request's namespace; an
// empty name is the namespace.
func orkflowField(nameField string) resolver {
	return resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
		ns, err := namespaceOf(strField(msg, "namespace"))
		return authz.Resource{Namespace: ns, Orkflow: strField(msg, nameField)}, err
	}}
}

// documentHead reads an authored document's top-level `namespace:` and
// `name:` the way its handler will: ok is false for a document whose head
// cannot be read as plain strings, which is then decided at the
// deployment, closed, never at the request's default. Every top-level key
// must be a plain, untagged, unanchored string: a merge key (`<<`), an
// alias used as a key, an anchor or a tag could hide a `namespace:` that a
// struct decode would still take (FuzzDocumentHead holds the two to one
// answer).
func documentHead(source string) (namespace, name string, ok bool) {
	var root yaml.Node
	if yaml.Unmarshal([]byte(source), &root) != nil || len(root.Content) != 1 || root.Content[0].Kind != yaml.MappingNode {
		return "", "", false
	}
	m := root.Content[0]
	for i := 0; i+1 < len(m.Content); i += 2 {
		key, value := m.Content[i], m.Content[i+1]
		if !plainString(key) || key.Value == "<<" {
			return "", "", false
		}
		if key.Value != "namespace" && key.Value != "name" {
			continue
		}
		if !plainString(value) {
			return "", "", false
		}
		if key.Value == "namespace" {
			namespace = value.Value
		} else {
			name = value.Value
		}
	}
	return namespace, name, true
}

// plainString is an untagged, unanchored string scalar: what it reads as is
// what it says.
func plainString(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode && n.Style&yaml.TaggedStyle == 0 && n.Anchor == "" && n.ShortTag() == "!!str"
}

// documentNamespace is the namespace a document-carrying request acts in
// (docs/ACCESS.md, "A request that carries a document"): the document's
// when it names one, else the request's, else default. The head is always
// read, and one that cannot be read leaves ok false: the deployment
// decides. A request and a document that name different namespaces are
// refused here, before anything is compiled or read, as the handler would.
func documentNamespace(msg proto.Message) (ns string, ok bool, err error) {
	docNS, _, readable := documentHead(strField(msg, "yaml"))
	if !readable {
		return "", false, nil
	}
	selector := strField(msg, "namespace")
	if selector != "" && docNS != "" && artifacts.ResolveNamespace(selector) != artifacts.ResolveNamespace(docNS) {
		return "", false, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"the document names namespace %q but the request names %q; give one", docNS, selector))
	}
	if docNS == "" {
		docNS = selector
	}
	ns, err = namespaceOf(docNS)
	if err != nil {
		return "", false, err
	}
	return ns, true, nil
}

// inAutomationNamespace decides PlanAutomation in the namespace its
// document describes. A request that names another is answered by the
// handler with its namespace issue and no step, before it reads anything,
// so it is decided where the document points rather than refused here.
var inAutomationNamespace = resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
	docNS, _, readable := documentHead(strField(msg, "yaml"))
	if !readable {
		return authz.Resource{}, nil
	}
	if docNS == "" {
		docNS = strField(msg, "namespace")
	}
	ns, err := namespaceOf(docNS)
	if err != nil {
		return authz.Resource{}, nil
	}
	return authz.Resource{Namespace: ns}, nil
}}

// inDocumentNamespace decides ValidateOrkflow in the namespace its
// document acts in.
var inDocumentNamespace = resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
	ns, ok, err := documentNamespace(msg)
	if err != nil || !ok {
		return authz.Resource{}, err // unreadable: the deployment decides
	}
	return authz.Resource{Namespace: ns}, nil
}}

// orkflowDocument is the Orkflow an applied document names, in the
// namespace it acts in. Its name is read only when the caller is not
// allowed at that namespace; a name that cannot be read leaves the decision
// at the namespace, and the handler refuses the document for what it is.
var orkflowDocument = resolver{
	shallow: inDocumentNamespace.shallow,
	deep: func(_ context.Context, _ *Service, msg proto.Message) (authz.Resource, error) {
		ns, ok, err := documentNamespace(msg)
		if err != nil || !ok {
			return authz.Resource{}, err
		}
		_, name, ok := documentHead(strField(msg, "yaml"))
		if !ok || dsl.ValidSlug("orkflow name", name) != nil {
			return authz.Resource{Namespace: ns}, nil
		}
		return authz.Resource{Namespace: ns, Orkflow: name}, nil
	},
}

// planRunTarget is PlanRun's: a stored Orkflow by name, or a document.
var planRunTarget = resolver{
	shallow: func(msg proto.Message) (authz.Resource, error) {
		if strField(msg, "yaml") != "" {
			return orkflowDocument.shallow(msg)
		}
		return orkflowField("orkflow_name").shallow(msg)
	},
	deep: func(ctx context.Context, s *Service, msg proto.Message) (authz.Resource, error) {
		if strField(msg, "yaml") != "" {
			return orkflowDocument.deep(ctx, s, msg)
		}
		return orkflowField("orkflow_name").shallow(msg)
	},
}

// runIn is the run a field names: the namespace first, then the run's
// Orkflow from the same facts a run read returns. A shared child run's
// Orkflow is <owner>/<name>, which no Orkflow binding of this namespace
// names.
func runIn(idField string) resolver {
	return resolver{
		shallow: inNamespace("namespace").shallow,
		deep: func(ctx context.Context, s *Service, msg proto.Message) (authz.Resource, error) {
			ns, err := namespaceOf(strField(msg, "namespace"))
			if err != nil {
				return authz.Resource{}, err
			}
			id := strField(msg, idField)
			if id == "" {
				return authz.Resource{Namespace: ns}, nil
			}
			facts, err := s.runFactsIn(ctx, id, ns)
			if err != nil {
				return authz.Resource{}, err
			}
			orkflow := facts.Orkflow
			if facts.OrkflowNamespace != "" {
				orkflow = facts.OrkflowNamespace + "/" + facts.Orkflow
			}
			return authz.Resource{Namespace: ns, Orkflow: orkflow}, nil
		},
	}
}

// scheduleNamed is the schedule a request names: its Orkflow.
var scheduleNamed = resolver{
	shallow: inNamespace("namespace").shallow,
	deep: func(ctx context.Context, s *Service, msg proto.Message) (authz.Resource, error) {
		ns, err := namespaceOf(strField(msg, "namespace"))
		if err != nil || s.schedules == nil {
			return authz.Resource{Namespace: ns}, err
		}
		view, err := s.schedules.Describe(ctx, ns, strField(msg, "name"))
		if err != nil {
			return authz.Resource{}, err
		}
		return authz.Resource{Namespace: ns, Orkflow: view.OrkflowName}, nil
	},
}

// triggerByID is the trigger a request names: its Orkflow, when it is in
// the request's namespace.
var triggerByID = resolver{
	shallow: inNamespace("namespace").shallow,
	deep: func(ctx context.Context, s *Service, msg proto.Message) (authz.Resource, error) {
		ns, err := namespaceOf(strField(msg, "namespace"))
		if err != nil {
			return authz.Resource{}, err
		}
		t, ok, err := s.triggers.Get(ctx, strField(msg, "id"))
		if err != nil {
			return authz.Resource{}, connect.NewError(connect.CodeUnavailable, err)
		}
		if !ok || t.Namespace != ns {
			return authz.Resource{Namespace: ns}, nil
		}
		return authz.Resource{Namespace: ns, Orkflow: t.OrkflowName}, nil
	},
}

// webhookScope is a subscription's scope: its namespace, or the deployment
// when it names none.
func webhookScope(namespace string) authz.Resource { return authz.Resource{Namespace: namespace} }

// webhookByID is the stored subscription a request names.
var webhookByID = resolver{
	shallow: deployment.shallow,
	deep: func(ctx context.Context, s *Service, msg proto.Message) (authz.Resource, error) {
		hook, ok, err := s.webhooks.GetContext(ctx, strField(msg, "id"))
		if err != nil {
			return authz.Resource{}, connect.NewError(connect.CodeUnavailable, err)
		}
		if !ok {
			return authz.Resource{}, nil
		}
		return webhookScope(hook.Namespace), nil
	},
}

// webhookRequested is the scope a register or an update asks for.
var webhookRequested = resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
	ns := strField(msg, "namespace")
	if ns != "" {
		if err := dsl.ValidSlug("namespace", ns); err != nil {
			return authz.Resource{}, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}
	return webhookScope(ns), nil
}}

// deliveryByID is the subscription of the delivery a request names.
var deliveryByID = resolver{
	shallow: deployment.shallow,
	deep: func(ctx context.Context, s *Service, msg proto.Message) (authz.Resource, error) {
		rec, err := s.readDelivery(ctx, strField(msg, "id"))
		if err != nil {
			return authz.Resource{}, err
		}
		return s.webhookResource(ctx, rec.GetWebhookId())
	},
}

func (s *Service) webhookResource(ctx context.Context, id string) (authz.Resource, error) {
	hook, ok, err := s.webhooks.LookupForDelivery(ctx, id)
	if err != nil {
		return authz.Resource{}, connect.NewError(connect.CodeUnavailable, err)
	}
	if !ok {
		return authz.Resource{}, nil
	}
	return webhookScope(hook.Namespace), nil
}

// purgeResource is a purge's: every namespace's memos with `all`.
var purgeResource = resolver{shallow: func(msg proto.Message) (authz.Resource, error) {
	if boolField(msg, "all") {
		return authz.Resource{}, nil
	}
	return inNamespace("namespace").shallow(msg)
}}

// mayAnswer is, in enforce, whether the caller holds perm at res: what a
// handler asks before a row of an answer names something the request did
// not. Observe changes no answer, and asserted mode decides nothing.
func (s *Service) mayAnswer(ctx context.Context, perm authz.Permission, res authz.Resource) (bool, error) {
	if !s.access.enabled() || !s.access.enforcing() {
		return true, nil
	}
	policy, _ := s.access.policy(ctx)
	return s.mayWithIDs(ctx, authz.Request{Action: authz.Action(perm), Subject: subjectOf(authn.FromContext(ctx)), Resource: res, Policy: policy})
}

// --- request-dependent permissions ----------------------------------------

func artifactKind(msg proto.Message) orkstackv1.ArtifactKind {
	fd, m, ok := field(msg, "kind")
	if !ok || fd.Kind() != protoreflect.EnumKind {
		return orkstackv1.ArtifactKind_ARTIFACT_KIND_UNSPECIFIED
	}
	return orkstackv1.ArtifactKind(m.Get(fd).Enum())
}

// artifactPermission is what reading one kind of artifact asks: logs are
// one permission wherever they are read, artifact bytes another.
func artifactPermission(kind orkstackv1.ArtifactKind) authz.Permission {
	switch kind {
	case orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT, orkstackv1.ArtifactKind_ARTIFACT_KIND_STDERR:
		return authz.PermRunLogsRead
	case orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES:
		return authz.PermRunRead
	}
	return authz.PermRunArtifactsRead
}

func kindIs(perm authz.Permission) func(proto.Message) bool {
	return func(msg proto.Message) bool { return artifactPermission(artifactKind(msg)) == perm }
}

func unless(name string) func(proto.Message) bool {
	return func(msg proto.Message) bool { return !boolField(msg, name) }
}

func when(name string) func(proto.Message) bool {
	return func(msg proto.Message) bool { return strField(msg, name) != "" }
}

// namespacePolicyFields are UpdateNamespace's fields that change what a
// namespace may reach (docs/ACCESS.md, "Namespace fields").
var namespacePolicyFields = []string{"allowed_pools", "allowed_secrets", "require_signed_by", "max_running",
	"budget", "fair_share_weight", "identity"}

// policyChange reports whether an UpdateNamespace changes a policy field of
// the stored document; a namespace that cannot be read is treated as one
// that changes them, so the stricter permission is asked.
func (s *Service) policyChange(ctx context.Context, msg proto.Message) bool {
	next, _ := msgField(msg, "namespace").(*orkstackv1.Namespace)
	if next == nil {
		return false
	}
	stored, err := s.namespace(ctx, next.GetName())
	if err != nil {
		return true
	}
	return namespacePolicyDiffers(next, stored)
}

// namespacePolicyDiffers reports whether next changes a policy field of
// stored. The interceptor asks it of the namespace it reads; the handler,
// which decides, asks it of the exact document its write replaces.
func namespacePolicyDiffers(next, stored *orkstackv1.Namespace) bool {
	// The namespace's id is output only: a document read and sent back
	// carries it, and that is no change of policy.
	next, stored = withoutNamespaceID(next), withoutNamespaceID(stored)
	a, b := next.ProtoReflect(), stored.ProtoReflect()
	for _, name := range namespacePolicyFields {
		fd := a.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			continue
		}
		if !a.Get(fd).Equal(b.Get(fd)) {
			return true
		}
	}
	return false
}

// mayChangeNamespacePolicy is namespace.policy.update, decided by the
// UpdateNamespace handler against stored, the document its compare-and-swap
// replaces: a write whose etag is not stored's replaces nothing and is
// refused before anything is compared. Only enforce refuses; the
// interceptor has already recorded what observe would deny.
func (s *Service) mayChangeNamespacePolicy(ctx context.Context, next, stored *orkstackv1.Namespace) error {
	if !s.access.enabled() || authn.ModeFrom(ctx) != authn.ModeEnforce {
		return nil
	}
	if next.GetEtag() != stored.GetEtag() {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q was written by somebody else since it was read; read it again and retry", stored.GetName()))
	}
	if !namespacePolicyDiffers(next, stored) {
		return nil
	}
	policy, unavailable := s.access.policy(ctx)
	_, err := s.authorizeWithIDs(ctx, authz.Request{Action: authz.Action(authz.PermNamespacePolicyUpdate),
		Subject: subjectOf(authn.FromContext(ctx)), Resource: authz.Resource{Namespace: stored.GetName()}, Policy: policy})
	if connect.CodeOf(err) == connect.CodeUnavailable {
		return err
	}
	if err != nil && unavailable {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%w; only configuration's principals are served", ErrPolicyUnavailable))
	}
	return deniedAsConnect(err)
}

// --- the table -----------------------------------------------------------

func listOf(rows string, rowResource func(req, row proto.Message) authz.Resource) *listRule {
	return &listRule{rows: rows, rowResource: rowResource}
}

func rowInRequestNamespace(nameField string) func(req, row proto.Message) authz.Resource {
	return func(req, row proto.Message) authz.Resource {
		ns, _ := namespaceOf(strField(req, "namespace"))
		return authz.Resource{Namespace: ns, Orkflow: strField(row, nameField)}
	}
}

func rowOwnNamespace(nsField, nameField string) func(req, row proto.Message) authz.Resource {
	return func(_, row proto.Message) authz.Resource {
		ns, _ := namespaceOf(strField(row, nsField))
		r := authz.Resource{Namespace: ns}
		if nameField != "" {
			r.Orkflow = strField(row, nameField)
		}
		return r
	}
}

func filterOrkflow(req proto.Message, value string) authz.Resource {
	ns, _ := namespaceOf(strField(req, "namespace"))
	return authz.Resource{Namespace: ns, Orkflow: value}
}

func filterNamespace(_ proto.Message, value string) authz.Resource {
	return authz.Resource{Namespace: value}
}

// accessRows is every procedure's row.
var accessRows = map[string]accessRow{
	// OrkflowService
	orkstackv1connect.OrkflowServiceValidateOrkflowProcedure:          {res: inDocumentNamespace, checks: perms(authz.PermOrkflowValidate)},
	orkstackv1connect.OrkflowServiceApplyOrkflowProcedure:             {res: orkflowDocument, checks: perms(authz.PermOrkflowWrite)},
	orkstackv1connect.OrkflowServiceGetOrkflowProcedure:               {res: orkflowField("name"), checks: perms(authz.PermOrkflowRead)},
	orkstackv1connect.OrkflowServiceListOrkflowVersionsProcedure:      {res: orkflowField("name"), checks: perms(authz.PermOrkflowRead)},
	orkstackv1connect.OrkflowServiceDiffOrkflowVersionsProcedure:      {res: orkflowField("name"), checks: perms(authz.PermOrkflowRead)},
	orkstackv1connect.OrkflowServiceCompareInterfacesProcedure:        {res: orkflowField("name"), checks: perms(authz.PermOrkflowRead)},
	orkstackv1connect.OrkflowServiceListOrkflowCallersProcedure:       {res: orkflowField("name"), checks: perms(authz.PermOrkflowRead)},
	orkstackv1connect.OrkflowServicePlanRunProcedure:                  {res: planRunTarget, checks: perms(authz.PermOrkflowRead)},
	orkstackv1connect.OrkflowServiceListOrkflowsProcedure:             {res: inNamespace("namespace"), checks: perms(authz.PermOrkflowRead), list: &listRule{rows: "orkflows", rowResource: rowInRequestNamespace("name"), filterField: "name", filterResource: filterOrkflow}},
	orkstackv1connect.OrkflowServiceSetCurrentOrkflowVersionProcedure: {res: orkflowField("name"), checks: perms(authz.PermOrkflowPromote)},
	orkstackv1connect.OrkflowServiceDeleteOrkflowProcedure:            {res: orkflowField("name"), checks: perms(authz.PermOrkflowDelete)},

	// RunService
	orkstackv1connect.RunServiceSubmitRunProcedure: {res: orkflowField("orkflow_name"), checks: []check{
		{perm: authz.PermRunSubmit},
		{perm: authz.PermRunRead, res: ptr(runIn("source_run_id")), when: when("source_run_id")},
	}},
	orkstackv1connect.RunServiceGetRunProcedure:           {res: runIn("run_id"), checks: perms(authz.PermRunRead), notFound: true},
	orkstackv1connect.RunServiceStreamEventsProcedure:     {res: runIn("run_id"), checks: perms(authz.PermRunRead), notFound: true},
	orkstackv1connect.RunServiceListTaskAttemptsProcedure: {res: runIn("run_id"), checks: perms(authz.PermRunRead), notFound: true},
	orkstackv1connect.RunServiceInspectRunProcedure:       {res: runIn("run_id"), checks: perms(authz.PermRunRead), notFound: true},
	orkstackv1connect.RunServiceGetArtifactProvenanceProcedure: {res: runIn("run_id"), notFound: true, checks: []check{
		{perm: authz.PermRunRead},
		{perm: authz.PermRunLogsRead, when: kindIs(authz.PermRunLogsRead)},
		{perm: authz.PermRunArtifactsRead, when: kindIs(authz.PermRunArtifactsRead)},
	}},
	orkstackv1connect.RunServiceListRunArtifactsProcedure: {res: runIn("run_id"), checks: perms(authz.PermRunRead), notFound: true,
		artifactKinds: true},
	orkstackv1connect.RunServiceDiffRunsProcedure: {res: runIn("from_run_id"), notFound: true, checks: []check{
		{perm: authz.PermRunRead},
		{perm: authz.PermRunRead, res: ptr(runIn("to_run_id"))},
	}},
	orkstackv1connect.RunServiceListRunsProcedure: {res: inNamespace("namespace"), checks: perms(authz.PermRunRead),
		list: &listRule{filterField: "orkflow_name", filterResource: filterOrkflow}},
	orkstackv1connect.RunServiceStreamTaskLogsProcedure: {res: runIn("run_id"), checks: perms(authz.PermRunLogsRead), notFound: true},
	orkstackv1connect.RunServiceReadArtifactProcedure: {res: runIn("run_id"), notFound: true, checks: []check{
		{perm: authz.PermRunLogsRead, when: kindIs(authz.PermRunLogsRead)},
		{perm: authz.PermRunArtifactsRead, when: kindIs(authz.PermRunArtifactsRead)},
		{perm: authz.PermRunRead, when: kindIs(authz.PermRunRead)},
	}},
	orkstackv1connect.RunServiceExportRunEvidenceProcedure: {res: runIn("run_id"), notFound: true, checks: []check{
		{perm: authz.PermRunEvidenceExport},
		{perm: authz.PermRunLogsRead, when: unless("exclude_logs")},
		{perm: authz.PermRunArtifactsRead, when: unless("exclude_artifacts")},
	}},
	orkstackv1connect.RunServiceCancelRunProcedure:    {res: runIn("run_id"), checks: perms(authz.PermRunCancel), notFound: true},
	orkstackv1connect.RunServiceResolveAwaitProcedure: {res: runIn("run_id"), checks: perms(authz.PermRunAwaitResolve), notFound: true},

	// ScheduleService
	orkstackv1connect.ScheduleServiceListSchedulesProcedure: {res: inNamespace("namespace"), checks: perms(authz.PermScheduleRead),
		list: listOf("schedules", rowOwnNamespace("namespace", "orkflow_name"))},
	orkstackv1connect.ScheduleServiceGetScheduleProcedure:      {res: scheduleNamed, checks: perms(authz.PermScheduleRead)},
	orkstackv1connect.ScheduleServiceCreateScheduleProcedure:   {res: orkflowField("orkflow_name"), checks: perms(authz.PermScheduleWrite, authz.PermRunSubmit)},
	orkstackv1connect.ScheduleServiceUpdateScheduleProcedure:   {res: scheduleNamed, checks: perms(authz.PermScheduleWrite, authz.PermRunSubmit)},
	orkstackv1connect.ScheduleServiceDeleteScheduleProcedure:   {res: scheduleNamed, checks: perms(authz.PermScheduleWrite)},
	orkstackv1connect.ScheduleServicePauseScheduleProcedure:    {res: scheduleNamed, checks: perms(authz.PermScheduleOperate)},
	orkstackv1connect.ScheduleServiceResumeScheduleProcedure:   {res: scheduleNamed, checks: perms(authz.PermScheduleOperate)},
	orkstackv1connect.ScheduleServiceFireScheduleProcedure:     {res: scheduleNamed, checks: perms(authz.PermScheduleOperate, authz.PermRunSubmit)},
	orkstackv1connect.ScheduleServiceBackfillScheduleProcedure: {res: scheduleNamed, checks: perms(authz.PermScheduleOperate, authz.PermRunSubmit)},

	// TriggerService
	orkstackv1connect.TriggerServiceListTriggersProcedure: {res: inNamespace("namespace"), checks: perms(authz.PermTriggerRead),
		list: listOf("triggers", rowOwnNamespace("namespace", "orkflow_name"))},
	orkstackv1connect.TriggerServiceCreateTriggerProcedure: {res: orkflowField("orkflow_name"), checks: perms(authz.PermTriggerWrite, authz.PermRunSubmit)},
	orkstackv1connect.TriggerServiceUpdateTriggerProcedure: {res: triggerByID, checks: perms(authz.PermTriggerWrite, authz.PermRunSubmit)},
	orkstackv1connect.TriggerServiceDeleteTriggerProcedure: {res: triggerByID, checks: perms(authz.PermTriggerWrite)},

	// WebhookService: a subscription's scope is its namespace, or the
	// deployment when it names none.
	orkstackv1connect.WebhookServiceListWebhooksProcedure: {res: deployment, checks: perms(authz.PermWebhookRead),
		list: listOf("webhooks", rowOwnNamespaceOrDeployment)},
	orkstackv1connect.WebhookServiceListDeliveriesProcedure: {res: deployment, checks: perms(authz.PermWebhookRead),
		list: &listRule{rows: "deliveries", filterField: "namespace", filterResource: filterNamespace}},
	orkstackv1connect.WebhookServiceGetDeliveryProcedure:     {res: deliveryByID, checks: perms(authz.PermWebhookRead)},
	orkstackv1connect.WebhookServiceRegisterWebhookProcedure: {res: webhookRequested, checks: perms(authz.PermWebhookWrite, authz.PermRunRead)},
	orkstackv1connect.WebhookServiceUpdateWebhookProcedure: {res: webhookByID, checks: []check{
		{perm: authz.PermWebhookWrite}, {perm: authz.PermRunRead},
		{perm: authz.PermWebhookWrite, res: &webhookRequested}, {perm: authz.PermRunRead, res: &webhookRequested},
	}},
	orkstackv1connect.WebhookServiceDeleteWebhookProcedure:    {res: webhookByID, checks: perms(authz.PermWebhookWrite)},
	orkstackv1connect.WebhookServiceRedeliverWebhookProcedure: {res: deliveryByID, checks: perms(authz.PermWebhookRedeliver)},

	// StatsService and CacheService
	orkstackv1connect.StatsServiceGetStatsProcedure:            {res: inNamespace("namespace"), checks: perms(authz.PermStatsRead)},
	orkstackv1connect.StatsServiceGetFailureBreakdownProcedure: {res: inNamespace("namespace"), checks: perms(authz.PermStatsRead)},
	orkstackv1connect.StatsServiceGetFlakeIndexProcedure:       {res: inNamespace("namespace"), checks: perms(authz.PermStatsRead)},
	orkstackv1connect.StatsServiceGetUsageProcedure:            {res: inNamespace("namespace"), checks: perms(authz.PermStatsRead)},
	orkstackv1connect.CacheServiceGetCacheStatsProcedure:       {res: inNamespace("namespace"), checks: perms(authz.PermStatsRead)},
	orkstackv1connect.CacheServicePurgeCacheProcedure:          {res: purgeResource, checks: perms(authz.PermCachePurge)},

	// AutomationService
	orkstackv1connect.AutomationServicePlanAutomationProcedure:   {res: inAutomationNamespace, checks: perms(authz.PermAutomationPlan)},
	orkstackv1connect.AutomationServiceExportAutomationProcedure: {res: inNamespace("namespace"), checks: perms(authz.PermAutomationPlan)},

	// NamespaceService
	orkstackv1connect.NamespaceServiceGetNamespaceProcedure: {res: inNamespace("name"), checks: perms(authz.PermNamespaceRead)},
	orkstackv1connect.NamespaceServiceListPoolsProcedure:    {res: inNamespace("namespace"), checks: perms(authz.PermNamespaceRead)},
	orkstackv1connect.NamespaceServiceListNamespacesProcedure: {res: deployment, checks: perms(authz.PermNamespaceRead),
		list: &listRule{rows: "namespaces", rowResource: rowOwnNamespace("name", ""), filterField: "name", filterResource: filterNamespace}},
	orkstackv1connect.NamespaceServiceUpdateNamespaceProcedure: {res: namespaceDocument, checks: []check{
		{perm: authz.PermNamespaceUpdate},
		{perm: authz.PermNamespacePolicyUpdate, when: alwaysCheckedByHandler},
	}},
	orkstackv1connect.NamespaceServiceCreateNamespaceProcedure: {res: deployment, checks: perms(authz.PermNamespaceCreate)},
	orkstackv1connect.NamespaceServiceDeleteNamespaceProcedure: {res: deployment, checks: perms(authz.PermNamespaceDelete)},

	// ActivityService
	orkstackv1connect.ActivityServiceListActivitiesProcedure:  {res: deployment, checks: perms(authz.PermCatalogRead)},
	orkstackv1connect.ActivityServiceInspectActivityProcedure: {res: deployment, checks: perms(authz.PermCatalogRead)},
	orkstackv1connect.ActivityServiceListOrkersProcedure:      {res: deployment, checks: perms(authz.PermOrkerRead)},
	orkstackv1connect.ActivityServiceDescribeOrkerProcedure:   {res: deployment, checks: perms(authz.PermOrkerRead)},
	orkstackv1connect.ActivityServiceCordonOrkerProcedure:     {res: deployment, checks: perms(authz.PermOrkerCordon)},

	// SystemService
	orkstackv1connect.SystemServiceGetSystemInfoProcedure:         {res: deployment, checks: perms(authz.PermSystemRead)},
	orkstackv1connect.SystemServiceGetRetentionPolicyProcedure:    {res: deployment, checks: perms(authz.PermSystemRead)},
	orkstackv1connect.SystemServiceGetImagePolicyProcedure:        {res: deployment, checks: perms(authz.PermSystemRead)},
	orkstackv1connect.SystemServiceListReleasesProcedure:          {res: deployment, checks: perms(authz.PermSystemRead)},
	orkstackv1connect.SystemServiceListTelemetrySinksProcedure:    {res: deployment, checks: perms(authz.PermCatalogRead)},
	orkstackv1connect.SystemServiceUpdateRetentionPolicyProcedure: {res: deployment, checks: perms(authz.PermRetentionUpdate)},
	orkstackv1connect.SystemServiceUpdateImagePolicyProcedure:     {res: deployment, checks: perms(authz.PermImagePolicyUpdate)},

	// IdentityService: the task identity issuer, at the deployment
	// (docs/TASK_IDENTITY.md, "Administering the issuer").
	orkstackv1connect.IdentityServiceGetIssuerProcedure:        {res: deployment, checks: perms(authz.PermIdentityRead)},
	orkstackv1connect.IdentityServiceListSigningKeysProcedure:  {res: deployment, checks: perms(authz.PermIdentityRead)},
	orkstackv1connect.IdentityServiceRetireSigningKeyProcedure: {res: deployment, checks: perms(authz.PermIdentityKeysRetire)},
	orkstackv1connect.SystemServiceRampReleaseProcedure:        {res: deployment, checks: perms(authz.PermReleaseOperate)},
	orkstackv1connect.SystemServicePromoteReleaseProcedure:     {res: deployment, checks: perms(authz.PermReleaseOperate)},

	// AuditService: the deployment's trail, or one namespace's with a
	// namespace filter.
	orkstackv1connect.AuditServiceListAuditRecordsProcedure: {res: deployment, checks: perms(authz.PermAuditRead),
		list: &listRule{filterField: "namespace", filterResource: filterNamespace}},

	// AccessService: reads need access.read and writes access.write, at the
	// deployment. CreateBinding and DeleteBinding also admit access.bind at
	// the binding's own namespace; their handlers then hold it to the
	// delegation rule against the document they change (authz.MayDelegate).
	orkstackv1connect.AccessServiceGetAccessProcedure:           {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceListPermissionsProcedure:     {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceListPrincipalsProcedure:      {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceListSigninsProcedure:         {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceExplainAccessProcedure:       {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServicePlanAccessProcedure:          {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceExportAccessProcedure:        {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceListAccessRevisionsProcedure: {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceGetAccessRevisionProcedure:   {res: deployment, checks: perms(authz.PermAccessRead)},
	orkstackv1connect.AccessServiceCreateRoleProcedure:          {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceUpdateRoleProcedure:          {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceDeleteRoleProcedure:          {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceCreateBindingProcedure: {res: deployment, checks: []check{{perm: authz.PermAccessWrite,
		orElse: &check{perm: authz.PermAccessBind, res: &bindingRequested}}}},
	orkstackv1connect.AccessServiceUpdateBindingProcedure: {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceDeleteBindingProcedure: {res: deployment, checks: []check{{perm: authz.PermAccessWrite,
		orElse: &check{perm: authz.PermAccessBind, res: &bindingStored}}}},
	orkstackv1connect.AccessServiceCreateServiceAccountProcedure:      {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceUpdateServiceAccountProcedure:      {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceDeleteServiceAccountProcedure:      {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceSetPrincipalDisabledProcedure:      {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceRevokeSigninsProcedure:             {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceLowerAuthModeProcedure:             {res: deployment, checks: perms(authz.PermAccessWrite)},
	orkstackv1connect.AccessServiceAcknowledgeAccessRevisionProcedure: {res: deployment, checks: perms(authz.PermAccessWrite)},

	// AuthService: the caller itself. GetAuthConfig is public and is never
	// mounted behind the middleware's authentication (routes).
	orkstackv1connect.AuthServiceGetAuthConfigProcedure: {noPermission: true},
	orkstackv1connect.AuthServiceGetSigninProcedure:     {noPermission: true},
	orkstackv1connect.AuthServiceRefreshSigninProcedure: {noPermission: true},
	orkstackv1connect.AuthServiceSignOutProcedure:       {noPermission: true},
}

func ptr[T any](v T) *T { return &v }

// alwaysCheckedByHandler marks namespace.policy.update: whether it applies
// needs the stored document, so the interceptor asks it through
// policyChange (accessInterceptor.applies).
func alwaysCheckedByHandler(proto.Message) bool { return true }

func rowOwnNamespaceOrDeployment(_, row proto.Message) authz.Resource {
	return webhookScope(strField(row, "namespace"))
}

// errNotFoundRun is the denial of a run read, said as a run that does not
// exist.
func errNotFoundRun(msg proto.Message) error {
	id := strField(msg, "run_id")
	if id == "" {
		id = strField(msg, "from_run_id")
	}
	return connect.NewError(connect.CodeNotFound, fmt.Errorf("run %q not found", id))
}
