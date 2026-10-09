package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// registry returns the durable namespace registry, or the refusal a process
// without one owes its caller.
//
// There is no in-memory fallback, deliberately. A namespace this process
// invented would be gone at the next restart and absent from every other
// replica, so a create that "succeeded" would be followed by a submit saying
// the namespace does not exist. FailedPrecondition names the deployment as the
// thing to fix.
func (s *Service) registry() (engine.NamespaceRegistry, error) {
	if s.namespaces == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("this engine has no namespace registry configured, so namespaces "+
				"cannot be created, changed or removed"))
	}
	return s.namespaces, nil
}

// validNamespaceName holds a client-supplied name to the registry's own rule
// before anything is written.
//
// The rule is the store's (artifacts.NamespaceKey validates what it addresses),
// asked here so the refusal arrives as InvalidArgument naming the field rather
// than as whatever a backend makes of an unaddressable key.
func validNamespaceName(name string) error {
	if name == "" {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("namespace name is required"))
	}
	if _, err := artifacts.NamespaceKey(name); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

// validNamespaceSelector is the same rule for a request that SELECTS a
// namespace rather than naming one to write: empty is the default namespace,
// which is the contract of every `namespace` field on the wire.
//
// It is asked on every path, not only the writing ones. A read that handed
// the raw name to the registry and answered with whatever came back would make
// `Payments` NotFound against an in-memory registry — "does not exist; these
// do: default", which reads as an empty namespace and sends the caller to
// create it — and Unavailable against a store that validates its own keys,
// which blames the deployment for a typo. Neither is true: the name cannot be
// a namespace anywhere, and that is a bad request.
func validNamespaceSelector(name string) error {
	if name == "" {
		return nil
	}
	return validNamespaceName(name)
}

// validNamespaceRetention holds a client-supplied run_retention to the two
// rules the write path enforces: it is a usable positive duration, and it is
// within the deployment maximum. Unset is always fine — it means inherit.
//
// The ceiling check needs the retention controller, so unlike
// validNamespaceName this is not a pure function: an engine with no
// retention policy configured cannot tell whether a value exceeds one, and
// answers Unimplemented rather than silently skipping the check.
func (s *Service) validNamespaceRetention(ctx context.Context, spec *orkstackv1.Namespace) error {
	if spec.GetRunRetention() == nil {
		return nil
	}
	d, err := validateRetentionDuration(fmt.Sprintf("namespace %q run retention", spec.GetName()), spec.GetRunRetention())
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	ctrl, err := s.retentionController()
	if err != nil {
		return err
	}
	policy, err := ctrl.Policy(ctx)
	if err != nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"namespace %q run retention cannot be checked against the deployment maximum: %w", spec.GetName(), err))
	}
	if max := policy.GetMaxRunRetention().AsDuration(); d > max {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"namespace %q run retention %s exceeds the deployment maximum run retention %s",
			spec.GetName(), d, max))
	}
	return nil
}

// validNamespaceTelemetry holds a client-supplied telemetry policy to the
// full write-path rule set CreateNamespace enforces: the sink must be one
// this engine actually declared, and the attributes must fit dsl's one
// shared shape (internal/dsl.attributes.go) plus the reserved prefixes the
// platform's own instrumentation uses.
//
// It is validNamespaceTelemetryDocument plus validNamespaceTelemetrySink.
// UpdateNamespace does not call this directly — see validNamespaceTelemetrySink
// for why the sink-existence half is conditional there.
func (s *Service) validNamespaceTelemetry(spec *orkstackv1.Namespace) error {
	if err := validNamespaceTelemetryDocument(spec); err != nil {
		return err
	}
	return s.validNamespaceTelemetrySink(spec.GetTelemetry().GetSink())
}

// validNamespaceTelemetryDocument holds a client-supplied telemetry policy to
// the rules a document can be judged on by itself, with no engine state: the
// attributes must fit dsl's one shared shape (internal/dsl.attributes.go)
// plus the reserved prefixes the platform's own instrumentation uses. This is
// the half pkg/artifacts/namespace_store.go's validNamespaceTelemetry also
// enforces, and it is a hard error on every write path, create or update —
// unlike the sink-existence half, staleness is never a defense for a
// malformed attribute map.
func validNamespaceTelemetryDocument(spec *orkstackv1.Namespace) error {
	policy := spec.GetTelemetry()
	if policy == nil {
		return nil
	}
	if err := validTelemetryAttributes(policy.GetAttributes()); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

// validNamespaceTelemetrySink holds a client-supplied telemetry.sink to the
// one rule that needs engine state: the name must be one this deployment
// actually declared.
//
// It is engine-side rather than pure (like validNamespaceName) for the same
// reason validNamespaceRetention's ceiling check is: only the process that
// parsed ORK_TELEMETRY_SINKS knows what exists, so a namespace document can
// name a sink the store itself has no way to confirm.
//
// An empty sink is always fine — it means the default sink, which always
// exists. CreateNamespace calls this unconditionally; UpdateNamespace calls
// it only when the spec's sink differs from what is already stored, so a
// namespace whose stored sink an operator later removed from
// ORK_TELEMETRY_SINKS stays editable (docs/DEPLOY.md's fallback-to-default
// promise) instead of being locked out of every future edit.
func (s *Service) validNamespaceTelemetrySink(sink string) error {
	if sink == "" {
		return nil
	}
	known := s.knownTelemetrySinks()
	for _, name := range known {
		if name == sink {
			return nil
		}
	}
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
		"telemetry.sink %q is not a sink this deployment declared (known: %s)",
		sink, strings.Join(known, ", ")))
}

// validTelemetryAttributes is dsl.ValidateAttributeMap plus
// dsl.ValidateNoReservedPrefix, shared by the API's write path (this file)
// and the store's document-only validation (pkg/artifacts/namespace_store.go)
// so the two halves of the split enforce the identical attribute rule.
func validTelemetryAttributes(attributes map[string]string) error {
	if err := dsl.ValidateAttributeMap("telemetry.attributes", attributes, 16); err != nil {
		return err
	}
	return dsl.ValidateNoReservedPrefix("telemetry.attributes", attributes)
}

// CreateNamespace registers a namespace, idempotently for an identical spec.
//
// The write is conditional on the object being absent, so two replicas racing
// the same name produce one winner and one refusal rather than two writes
// where the second silently replaced the first. The refusal is then compared
// against what is stored: an identical spec is the same request arriving twice
// — a re-run provisioning script — and is answered with the existing document;
// a different spec is a collision and says which field differs.
func (s *Service) CreateNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.CreateNamespaceRequest],
) (*connect.Response[orkstackv1.CreateNamespaceResponse], error) {
	registry, err := s.registry()
	if err != nil {
		return nil, err
	}
	spec := req.Msg.GetNamespace()
	if err := validNamespaceName(spec.GetName()); err != nil {
		return nil, err
	}
	if err := s.validNamespaceRetention(ctx, spec); err != nil {
		return nil, err
	}
	if err := s.validNamespaceTelemetry(spec); err != nil {
		return nil, err
	}
	if err := s.validNamespaceSigner(ctx, spec); err != nil {
		return nil, err
	}
	if artifacts.ResolveNamespace(spec.GetName()) == artifacts.DefaultNamespace {
		// Wanting the default namespace is not an error; it is already there.
		// An identical spec is the no-op it looks like, and a different one is
		// refused as any other collision is.
		return s.createConflict(ctx, spec)
	}

	stored, err := registry.Put(ctx, proto.Clone(spec).(*orkstackv1.Namespace),
		artifacts.PutConditions{IfAbsent: true})
	switch {
	case err == nil:
		return connect.NewResponse(&orkstackv1.CreateNamespaceResponse{
			Namespace: stored,
			Created:   true,
		}), nil
	case errors.Is(err, artifacts.ErrPreconditionFailed):
		return s.createConflict(ctx, spec)
	default:
		return nil, namespaceStoreError(err, "create namespace %q", spec.GetName())
	}
}

// createConflict answers a create whose name is already taken: the same spec is
// an idempotent repeat, a different one is a collision.
func (s *Service) createConflict(
	ctx context.Context,
	spec *orkstackv1.Namespace,
) (*connect.Response[orkstackv1.CreateNamespaceResponse], error) {
	existing, err := s.namespaces.Get(ctx, spec.GetName())
	if err != nil {
		return nil, namespaceStoreError(err, "read namespace %q", spec.GetName())
	}
	if difference := specDifference(existing, spec); difference != "" {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
			"namespace %q already exists and differs: %s", spec.GetName(), difference))
	}
	return connect.NewResponse(&orkstackv1.CreateNamespaceResponse{
		Namespace: existing,
		Created:   false,
	}), nil
}

// specDifference names the first field in which two namespace specs disagree,
// or "" when they are the same namespace said twice.
//
// Only the fields a client sets are compared. created_at, updated_at and the
// etag are the registry's and always differ between a request and a stored
// document; comparing them would make every repeat a collision, which is the
// opposite of idempotent.
//
// Two fields are compared through the meaning of their value rather than its
// spelling, because a spec is identical when the system would act on it
// identically. allowed_pools is a set: a list reordered is the same
// allow-list, and refusing a reformatted script would be a collision that is
// not one. default_pool is compared through engine.DefaultPoolFor — the
// resolution rule itself — because an unset value MEANS main: `orkctl
// namespace list` prints main for a stored "", and `--default-pool`
// advertises main as its default, so a user who reads back what the CLI told
// them and re-runs their create is sending the same spec. Comparing the raw
// strings refused exactly that, with `default_pool is "main", not "main"` — a
// difference report whose two sides print identically.
func specDifference(stored, requested *orkstackv1.Namespace) string {
	if got, want := stored.GetDescription(), requested.GetDescription(); got != want {
		return fmt.Sprintf("description is %q, not %q", got, want)
	}
	if got, want := engine.DefaultPoolFor(stored), engine.DefaultPoolFor(requested); got != want {
		return fmt.Sprintf("default_pool is %q, not %q", got, want)
	}
	if got, want := sortedCopy(stored.GetAllowedPools()),
		sortedCopy(requested.GetAllowedPools()); !equalStrings(got, want) {
		return fmt.Sprintf("allowed_pools are [%s], not [%s]",
			strings.Join(got, " "), strings.Join(want, " "))
	}
	// allowed_secrets is a set for the same reason allowed_pools is: a
	// provisioning script that only reordered its --allow-secret flags is
	// re-sending the namespace it already created.
	if got, want := sortedCopy(stored.GetAllowedSecrets()),
		sortedCopy(requested.GetAllowedSecrets()); !equalStrings(got, want) {
		return fmt.Sprintf("allowed_secrets are [%s], not [%s]",
			strings.Join(got, " "), strings.Join(want, " "))
	}
	if got, want := stored.GetLabels(), requested.GetLabels(); !equalLabels(got, want) {
		return fmt.Sprintf("labels are %s, not %s", renderLabels(got), renderLabels(want))
	}
	if got, want := retentionLabel(stored.GetRunRetention()), retentionLabel(requested.GetRunRetention()); got != want {
		return fmt.Sprintf("run_retention is %s, not %s", got, want)
	}
	if got, want := signerLabel(stored.GetRequireSignedBy()), signerLabel(requested.GetRequireSignedBy()); got != want {
		return fmt.Sprintf("require_signed_by is %s, not %s", got, want)
	}
	// A hold is compared by its reason alone: since is the registry's, so two
	// requests that both ask for the same hold are the same spec whenever
	// they were sent.
	if got, want := holdLabel(stored.GetHold()), holdLabel(requested.GetHold()); got != want {
		return fmt.Sprintf("hold is %s, not %s", got, want)
	}
	if got, want := stored.GetMaxRunning(), requested.GetMaxRunning(); got != want {
		return fmt.Sprintf("max_running is %d, not %d", got, want)
	}
	if got, want := budgetLabel(stored.GetBudget()), budgetLabel(requested.GetBudget()); got != want {
		return fmt.Sprintf("budget is %s, not %s", got, want)
	}
	if diff := telemetryDifference(stored.GetTelemetry(), requested.GetTelemetry()); diff != "" {
		return diff
	}
	return ""
}

// telemetryDifference is specDifference's telemetry half, broken out because
// a policy has three independently-differing fields (sink, task_logs,
// attributes) and each gets its own message, the same granularity
// specDifference gives every other field.
func telemetryDifference(stored, requested *orkstackv1.TelemetryPolicy) string {
	if got, want := stored.GetSink(), requested.GetSink(); got != want {
		return fmt.Sprintf("telemetry.sink is %q, not %q", got, want)
	}
	if got, want := stored.GetTaskLogs(), requested.GetTaskLogs(); got != want {
		return fmt.Sprintf("telemetry.task_logs is %s, not %s", got, want)
	}
	if got, want := stored.GetAttributes(), requested.GetAttributes(); !equalLabels(got, want) {
		return fmt.Sprintf("telemetry.attributes are %s, not %s", renderLabels(got), renderLabels(want))
	}
	return ""
}

// signerLabel renders require_signed_by for a difference report: the quoted
// signer, or `"" (none)` for an unset one.
func signerLabel(signer string) string {
	if signer == "" {
		return `"" (none)`
	}
	return fmt.Sprintf("%q", signer)
}

// holdLabel renders a namespace's hold for a difference report: "none" for
// an accepting namespace, the quoted reason otherwise.
func holdLabel(hold *orkstackv1.NamespaceHold) string {
	if hold == nil {
		return "none"
	}
	return fmt.Sprintf("%q", hold.GetReason())
}

// retentionLabel renders a namespace's run_retention for a difference
// report: nil reads as "inherited (deployment maximum)", never as an empty
// string that could be mistaken for a stored zero.
// budgetLabel renders a namespace's budget for a difference report and the
// audit summary: "none" when unbounded, every field otherwise, so two
// budgets that differ in any field render differently.
func budgetLabel(b *orkstackv1.Budget) string {
	if b == nil {
		return "none"
	}
	return fmt.Sprintf("{cpu_seconds=%d gib_seconds=%d period=%s enforce=%t}",
		b.GetCpuSeconds(), b.GetGibSeconds(), b.GetPeriod().AsDuration(), b.GetEnforce())
}

func retentionLabel(d *durationpb.Duration) string {
	if d == nil {
		return "inherited (deployment maximum)"
	}
	return d.AsDuration().String()
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, key+"="+labels[key])
	}
	return "{" + strings.Join(pairs, " ") + "}"
}

// GetNamespace reads one namespace document. The default namespace always
// resolves, whether or not anyone created it.
func (s *Service) GetNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetNamespaceRequest],
) (*connect.Response[orkstackv1.GetNamespaceResponse], error) {
	ns, err := s.namespace(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.GetNamespaceResponse{Namespace: ns}), nil
}

// ListNamespaces returns the registered namespaces, ordered by name, with the
// built-in default always among them, filtered by name_contains.
//
// page_size 0 answers with every match, which is what the namespace switcher,
// Admin → Usage and orkctl ask for. A positive page_size walks the registry's
// NAME listing — keys only — and reads just the documents on the page: a page
// of 25 out of thousands of namespaces is one listing and 25 reads, where the
// full answer is one read per namespace.
func (s *Service) ListNamespaces(
	ctx context.Context,
	req *connect.Request[orkstackv1.ListNamespacesRequest],
) (*connect.Response[orkstackv1.ListNamespacesResponse], error) {
	size := int(req.Msg.GetPageSize())
	filter := req.Msg.GetNameContains()
	token := req.Msg.GetPageToken()
	switch {
	case size < 0:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("page_size cannot be negative"))
	case len(filter) > maxNamespaceNameContains:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"name_contains is %d bytes; a namespace name is at most %d, so nothing longer can match",
			len(filter), maxNamespaceNameContains))
	case token != "" && size == 0:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"page_token continues a paged listing; send the page_size it was issued for (page_size 0 lists everything)"))
	}
	after := ""
	if token != "" {
		var err error
		if after, err = decodeNamespaceCursor(token, filter); err != nil {
			return nil, err
		}
	}

	if s.namespaces == nil {
		// A process with no registry still has the namespace every orkflow
		// that names none is in. An empty list would say the deployment has no
		// namespaces, which is never true.
		return namespacePage(ctx, []string{artifacts.DefaultNamespace}, filter, after, size,
			func(context.Context, string) (*orkstackv1.Namespace, error) {
				return artifacts.DefaultNamespaceDocument(), nil
			})
	}
	if size == 0 {
		registered, err := s.namespaces.List(ctx)
		if err != nil {
			return nil, namespaceStoreError(err, "list namespaces")
		}
		matched := registered[:0:0]
		for _, ns := range registered {
			if strings.Contains(ns.GetName(), filter) {
				matched = append(matched, ns)
			}
		}
		return connect.NewResponse(&orkstackv1.ListNamespacesResponse{Namespaces: matched}), nil
	}
	names, err := s.namespaces.ListNames(ctx)
	if err != nil {
		return nil, namespaceStoreError(err, "list namespaces")
	}
	return namespacePage(ctx, names, filter, after, size, s.namespaces.Get)
}

// maxNamespaceNameContains is the longest name_contains accepted: the longest
// a namespace name can be (AGENTS.md §7), since a longer substring matches
// nothing and is almost certainly a mistake.
const maxNamespaceNameContains = 200

// namespacePageReads bounds how many documents one page reads at once.
const namespacePageReads = 8

// namespacePage answers one page of a name-ordered listing: the names after
// the cursor that contain filter, size of them, each document read by get.
// A name whose document is gone by the time it is read was deleted between
// the listing and the read and is left out, exactly as the full listing
// leaves it out; the cursor still moves past it.
func namespacePage(
	ctx context.Context,
	names []string,
	filter, after string,
	size int,
	get func(context.Context, string) (*orkstackv1.Namespace, error),
) (*connect.Response[orkstackv1.ListNamespacesResponse], error) {
	var matched []string
	for _, name := range names {
		if name > after && strings.Contains(name, filter) {
			matched = append(matched, name)
		}
	}
	response := &orkstackv1.ListNamespacesResponse{}
	if size > 0 && len(matched) > size {
		matched = matched[:size]
		token, err := encodeNamespaceCursor(filter, matched[len(matched)-1])
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		response.NextPageToken = token
	}

	docs := make([]*orkstackv1.Namespace, len(matched))
	errs := make([]error, len(matched))
	slots := make(chan struct{}, namespacePageReads)
	var wg sync.WaitGroup
	for i, name := range matched {
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			docs[i], errs[i] = get(ctx, name)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			response.Namespaces = append(response.Namespaces, docs[i])
			continue
		}
		if errors.Is(err, artifacts.ErrNotFound) {
			continue // deleted between the listing and the read
		}
		return nil, namespaceStoreError(err, "read namespace %q", matched[i])
	}
	return connect.NewResponse(response), nil
}

// namespaceCursor is a ListNamespaces page token: the last name a page held,
// bound to the filter it was issued under, in the shape orkflowCursor uses.
type namespaceCursor struct {
	Version int    `json:"v"`
	Filter  string `json:"f"`
	Name    string `json:"name"`
}

func encodeNamespaceCursor(filter, name string) (string, error) {
	raw, err := json.Marshal(namespaceCursor{Version: 1, Filter: filter, Name: name})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeNamespaceCursor(token, filter string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	var c namespaceCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.Name == "" {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid page_token"))
	}
	if c.Filter != filter {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"page_token was issued for name_contains %q, not %q; start again from the first page",
			c.Filter, filter))
	}
	return c.Name, nil
}

// UpdateNamespace replaces a namespace's mutable fields, compare-and-swapping
// on the etag the caller read.
//
// The etag travels inside the document rather than in a field of its own
// because it belongs to the document: a client reads a namespace, edits it and
// sends it back. Every update is conditional, and the etag says on WHAT: a
// token names the stored version it replaces, and no token says "the version I
// read had no stored bytes at all", which is written IfAbsent.
//
// There is deliberately no unconditional escape hatch, and an empty etag is
// not one; `default` is why that cannot stand: until somebody writes
// metadata/namespaces/default.json a read hands back a SYNTHESIZED document,
// whose etag is empty because there is nothing stored to name — not because
// the caller asked for anything. Read as an escape hatch, the first two edits
// of `default` would both be unconditional writes and one of them would vanish
// with no error, which is exactly what Namespace.etag promises cannot happen.
// A caller who genuinely means "whatever is there now" reads
// first and sends that token back; that round trip IS the compare-and-swap.
func (s *Service) UpdateNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.UpdateNamespaceRequest],
) (*connect.Response[orkstackv1.UpdateNamespaceResponse], error) {
	registry, err := s.registry()
	if err != nil {
		return nil, err
	}
	spec := req.Msg.GetNamespace()
	if err := validNamespaceName(spec.GetName()); err != nil {
		return nil, err
	}
	if err := s.validNamespaceRetention(ctx, spec); err != nil {
		return nil, err
	}
	if err := validNamespaceTelemetryDocument(spec); err != nil {
		return nil, err
	}
	// An update is not a create: a name nobody registered is NotFound, naming
	// the ones that do exist, rather than a namespace quietly brought into
	// being by a typo. The read is also what lets the sink check below tell a
	// change from a repeat: reusing it (rather than reading twice) makes that
	// comparison free.
	stored, err := s.namespace(ctx, spec.GetName())
	if err != nil {
		return nil, err
	}
	// Sink existence is enforced only when the caller is actually changing
	// it. A namespace whose stored sink an operator later removed from
	// ORK_TELEMETRY_SINKS would otherwise be uneditable in every other way —
	// description, pools, retention, labels — which contradicts
	// docs/DEPLOY.md's promise that such a namespace merely falls back to the
	// default sink. The caller round-tripping that stale name unchanged is
	// not asking this deployment to bless it, only to leave it alone; logged
	// so an operator watching this namespace can see it is still pointed at a
	// sink that no longer exists.
	newSink := spec.GetTelemetry().GetSink()
	if newSink != stored.GetTelemetry().GetSink() {
		if err := s.validNamespaceTelemetrySink(newSink); err != nil {
			return nil, err
		}
	} else if newSink != "" && s.validNamespaceTelemetrySink(newSink) != nil {
		s.logger.Warn("namespace update proceeding with a telemetry sink this deployment no longer declares",
			"namespace", spec.GetName(), "telemetry_sink", newSink)
	}
	// The signer's key is checked only when the caller is changing the
	// signer, on the sink check's terms and for its reason: a namespace whose
	// key an operator later removed (reachable only through a document
	// another release wrote — UpdateImagePolicy refuses the removal) must stay
	// editable in every other way. The spelling is the store's rule and is
	// always enforced there.
	newSigner := spec.GetRequireSignedBy()
	if newSigner != stored.GetRequireSignedBy() {
		if err := s.validNamespaceSigner(ctx, spec); err != nil {
			return nil, err
		}
	} else if newSigner != "" && s.validNamespaceSigner(ctx, spec) != nil {
		s.logger.Warn("namespace update proceeding with a require_signed_by key this deployment no longer holds",
			"namespace", spec.GetName(), "require_signed_by", newSigner)
	}
	cond := artifacts.PutConditions{IfMatch: spec.GetEtag()}
	if spec.GetEtag() == "" {
		cond = artifacts.PutConditions{IfAbsent: true}
	}
	updated, err := registry.Put(ctx, proto.Clone(spec).(*orkstackv1.Namespace), cond)
	switch {
	case err == nil:
		return connect.NewResponse(&orkstackv1.UpdateNamespaceResponse{Namespace: updated}), nil
	case errors.Is(err, artifacts.ErrPreconditionFailed) && cond.IfAbsent:
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q was written by somebody else since it was read "+
				"(it had no stored version then and has one now); "+
				"read it again and re-apply the change", spec.GetName()))
	case errors.Is(err, artifacts.ErrPreconditionFailed):
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q changed since it was read (etag %q is stale); "+
				"read it again and re-apply the change", spec.GetName(), spec.GetEtag()))
	default:
		return nil, namespaceStoreError(err, "update namespace %q", spec.GetName())
	}
}

// DeleteNamespace removes a namespace that holds no orkflows.
//
// The catalog is checked here and the names of what is in the way are
// reported: the registry's own refusal knows a count, and a count is not a next
// step. The store refuses too, and that refusal is the one no client can
// bypass — this one is the one that can say what to delete first.
func (s *Service) DeleteNamespace(
	ctx context.Context,
	req *connect.Request[orkstackv1.DeleteNamespaceRequest],
) (*connect.Response[orkstackv1.DeleteNamespaceResponse], error) {
	registry, err := s.registry()
	if err != nil {
		return nil, err
	}
	name := artifacts.ResolveNamespace(req.Msg.GetName())
	if name == artifacts.DefaultNamespace {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q cannot be deleted: every orkflow that names no namespace is in it",
			name))
	}
	// A name nobody registered is NotFound, and says which names exist.
	if _, err := s.namespace(ctx, name); err != nil {
		return nil, err
	}
	held, err := s.orkflows.List(ctx, name)
	if err != nil {
		return nil, orkflowStoreError(err, "list orkflows in namespace %q", name)
	}
	if len(held) > 0 {
		names := make([]string, 0, len(held))
		for _, wf := range held {
			names = append(names, wf.GetName())
		}
		sort.Strings(names)
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"namespace %q still holds %d orkflow(s) (%s); delete them first",
			name, len(names), strings.Join(names, ", ")))
	}
	if err := registry.Delete(ctx, name); err != nil {
		return nil, namespaceStoreError(err, "delete namespace %q", name)
	}
	return connect.NewResponse(&orkstackv1.DeleteNamespaceResponse{}), nil
}

// namespaceStoreError maps a registry failure onto a Connect code.
//
// A name the store cannot address is the caller's (InvalidArgument), a missing
// document is NotFound, a refused delete or a lost race is FailedPrecondition,
// and anything else means the registry could not be reached — Unavailable,
// never answered from memory.
func namespaceStoreError(err error, format string, args ...any) error {
	subject := fmt.Sprintf(format, args...)
	switch {
	case errors.Is(err, artifacts.ErrInvalidNamespace):
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s: %w", subject, err))
	case errors.Is(err, artifacts.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("%s: not found", subject))
	case errors.Is(err, artifacts.ErrNamespaceNotEmpty),
		errors.Is(err, artifacts.ErrPreconditionFailed):
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s: %w", subject, err))
	default:
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s: %w", subject, err))
	}
}
