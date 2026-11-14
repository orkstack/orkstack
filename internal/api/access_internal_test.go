package api

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.yaml.in/yaml/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// TestAuthz_EveryProcedureIsClassified holds every procedure the generated
// descriptors declare to exactly one row, and every row to a procedure that
// exists: a service added without its rows fails here.
func TestAuthz_EveryProcedureIsClassified(t *testing.T) {
	procedures := orkstackProcedures(t)
	for procedure := range procedures {
		if _, ok := accessRows[procedure]; !ok {
			t.Errorf("%s has no permission row (internal/api/access_rows.go); it would be refused", procedure)
		}
	}
	for procedure, row := range accessRows {
		if _, ok := procedures[procedure]; !ok {
			t.Errorf("row %s names no declared procedure", procedure)
		}
		if !row.noPermission && len(row.checks) == 0 {
			t.Errorf("%s asks no permission and is not marked noPermission", procedure)
		}
		for _, c := range row.checks {
			if !authz.Known(c.perm) {
				t.Errorf("%s asks %q, which is not in the catalogue", procedure, c.perm)
			}
		}
	}
}

// TestAccessCatalogue_Numbers generates the catalogue's two numbers, which
// docs/ACCESS.md quotes: its permissions and the procedures with a row.
func TestAccessCatalogue_Numbers(t *testing.T) {
	got := fmt.Sprintf("permissions: %d\nprocedures with a row: %d\n", len(authz.Permissions()), len(accessRows))
	path := filepath.Join("testdata", "access_numbers.golden")
	if os.Getenv("ORK_UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with ORK_UPDATE_GOLDEN=1 to write it)", err)
	}
	if got != string(want) {
		t.Fatalf("the catalogue's numbers changed:\n%s\nwant\n%s\nregenerate with ORK_UPDATE_GOLDEN=1 and update docs/ACCESS.md", got, want)
	}
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "ACCESS.md"))
	if err != nil {
		t.Fatalf("read docs/ACCESS.md: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		_, n, _ := strings.Cut(line, ": ")
		if !strings.Contains(string(doc), "**"+n+"**") {
			t.Errorf("docs/ACCESS.md does not quote %q as **%s**", line, n)
		}
	}
}

func newAccessService(t testing.TB) *Service {
	t.Helper()
	return NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		AuditStore: artifacts.NewMemoryAuditStore()})
}

// accessFor builds an AccessControl in mode over a document holding roles
// and bindings.
func accessFor(t testing.TB, mode string, roles []*orkstackv1.Role, bindings ...*orkstackv1.Binding) (*AccessControl, *artifacts.MemoryCAS) {
	t.Helper()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	if len(roles)+len(bindings) > 0 {
		if _, err := store.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Roles: roles, Bindings: bindings}, nil,
			artifacts.AccessWrite{At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	return &AccessControl{Mode: mode, Policies: NewPolicyCache(store, PolicyCacheOptions{})}, cas
}

var alice = &authn.Principal{Kind: authn.KindUser, ID: "user:corp:alice", Issuer: "corp", Method: authn.MethodJWT}

func asAlice(mode string) context.Context {
	return authn.WithPrincipal(authn.WithMode(context.Background(), mode), alice)
}

func requestFor(t testing.TB, procedure string) proto.Message {
	t.Helper()
	service, method, _ := strings.Cut(strings.TrimPrefix(procedure, "/"), "/")
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		t.Fatal(err)
	}
	md := d.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(method))
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.Input().FullName())
	if err != nil {
		t.Fatal(err)
	}
	msg := mt.New().Interface()
	set := func(name, value string) {
		fd := md.Input().Fields().ByName(protoreflect.Name(name))
		if fd != nil && fd.Kind() == protoreflect.StringKind && !fd.IsList() {
			msg.ProtoReflect().Set(fd, protoreflect.ValueOfString(value))
		}
	}
	set("namespace", "payments")
	set("name", "deploy")
	set("orkflow_name", "deploy")
	set("id", "w-1")
	if doc := md.Input().Fields().ByName("namespace"); doc != nil && doc.Kind() == protoreflect.MessageKind {
		msg.ProtoReflect().Set(doc, protoreflect.ValueOfMessage((&orkstackv1.Namespace{Name: "payments"}).ProtoReflect()))
	}
	return msg
}

// matrixVariants are the request values a row's permissions depend on: the
// generated matrix has one case for each (docs/ACCESS.md, "Where it is
// enforced").
var matrixVariants = map[string]map[string]func(proto.Message){
	orkstackv1connect.AccessServiceCreateBindingProcedure: {
		"no binding": func(proto.Message) {},
		"at a namespace": func(m proto.Message) {
			m.(*orkstackv1.CreateBindingRequest).Binding = &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:payments"}
		},
	},
	orkstackv1connect.RunServiceReadArtifactProcedure: {
		"stdout": func(m proto.Message) {
			m.(*orkstackv1.ReadArtifactRequest).Kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT
		},
		"stderr": func(m proto.Message) {
			m.(*orkstackv1.ReadArtifactRequest).Kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_STDERR
		},
		"output": func(m proto.Message) {
			m.(*orkstackv1.ReadArtifactRequest).Kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT
		},
		"values": func(m proto.Message) {
			m.(*orkstackv1.ReadArtifactRequest).Kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES
		},
	},
	orkstackv1connect.RunServiceExportRunEvidenceProcedure: {
		"whole":        func(proto.Message) {},
		"exclude logs": func(m proto.Message) { m.(*orkstackv1.ExportRunEvidenceRequest).ExcludeLogs = true },
		"exclude both": func(m proto.Message) {
			r := m.(*orkstackv1.ExportRunEvidenceRequest)
			r.ExcludeLogs, r.ExcludeArtifacts = true, true
		},
		"exclude output": func(m proto.Message) { m.(*orkstackv1.ExportRunEvidenceRequest).ExcludeArtifacts = true },
	},
	orkstackv1connect.RunServiceGetArtifactProvenanceProcedure: {
		"stdout": func(m proto.Message) {
			m.(*orkstackv1.GetArtifactProvenanceRequest).Kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT
		},
		"output": func(m proto.Message) {
			m.(*orkstackv1.GetArtifactProvenanceRequest).Kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT
		},
		"values": func(m proto.Message) {
			m.(*orkstackv1.GetArtifactProvenanceRequest).Kind = orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES
		},
	},
	orkstackv1connect.CacheServicePurgeCacheProcedure: {
		"namespace": func(proto.Message) {},
		"all":       func(m proto.Message) { m.(*orkstackv1.PurgeCacheRequest).All = true },
	},
	orkstackv1connect.RunServiceSubmitRunProcedure: {
		"plain": func(proto.Message) {},
	},
	orkstackv1connect.WebhookServiceRegisterWebhookProcedure: {
		"scoped":   func(proto.Message) {},
		"unscoped": func(m proto.Message) { m.(*orkstackv1.RegisterWebhookRequest).Namespace = "" },
	},
}

// applicable lists the checks a row asks of msg, with their resources.
func applicable(t testing.TB, s *Service, procedure string, msg proto.Message) []resolvedCheck {
	t.Helper()
	row := accessRows[procedure]
	var out []resolvedCheck
	for _, c := range row.checks {
		if c.when != nil {
			if procedure == orkstackNamespaceUpdate && c.perm == authz.PermNamespacePolicyUpdate {
				if !s.policyChange(context.Background(), msg) {
					continue
				}
			} else if !c.when(msg) {
				continue
			}
		}
		r := row.res
		if c.res != nil {
			r = *c.res
		}
		res, err := r.shallow(msg)
		if err != nil {
			t.Fatalf("%s: shallow resource: %v", procedure, err)
		}
		out = append(out, resolvedCheck{perm: c.perm, resource: res})
	}
	return out
}

func scopeString(r authz.Resource) string { return r.String() }

// TestAuthz_PermissionMatrix is the generated matrix of the permission
// table: for every row, and every value of a field that chooses a row's
// permission, a principal with exactly that row's permissions at exactly
// its scopes is admitted, and one holding every other permission at the
// deployment is refused (or, for a list of rows, answered only its own).
func TestAuthz_PermissionMatrix(t *testing.T) {
	procedures := make([]string, 0, len(accessRows))
	for p := range accessRows {
		procedures = append(procedures, p)
	}
	sort.Strings(procedures)
	everything := authz.Permissions()
	cases := 0
	for _, procedure := range procedures {
		row := accessRows[procedure]
		if row.noPermission {
			continue
		}
		variants := matrixVariants[procedure]
		if variants == nil {
			variants = map[string]func(proto.Message){"": func(proto.Message) {}}
		}
		for vname, mutate := range variants {
			t.Run(strings.TrimPrefix(procedure, "/orkstack.v1.")+"/"+vname, func(t *testing.T) {
				s := newAccessService(t)
				msg := requestFor(t, procedure)
				mutate(msg)
				checks := applicable(t, s, procedure, msg)
				if len(checks) == 0 {
					t.Fatal("no check applies")
				}
				// Exactly the row's permissions, each at its own scope.
				var bindings []*orkstackv1.Binding
				roles := []*orkstackv1.Role{}
				for i, c := range checks {
					role := fmt.Sprintf("only-%d", i)
					roles = append(roles, &orkstackv1.Role{Name: role, Permissions: []string{string(c.perm)}})
					bindings = append(bindings, &orkstackv1.Binding{Name: fmt.Sprintf("b-%d", i), Role: role,
						Scope: scopeString(c.resource), Subjects: []string{"user:corp:alice"}})
				}
				a, _ := accessFor(t, authn.ModeEnforce, roles, bindings...)
				s.access = a
				if _, err := a.decide(asAlice(authn.ModeEnforce), s, procedure, msg); err != nil {
					t.Fatalf("exactly the row's permissions at its scopes are refused: %v", err)
				}
				// Every other permission, at the deployment: a check's
				// alternative (orElse) is left out with it, and admits alone
				// at its own resource.
				for _, c := range checks {
					if c.perm == authz.PermCatalogRead {
						continue
					}
					var alt *check
					for _, rc := range row.checks {
						if rc.perm == c.perm && rc.orElse != nil {
							alt = rc.orElse
						}
					}
					others := []string{}
					for _, p := range everything {
						if p != c.perm && (alt == nil || p != alt.perm) {
							others = append(others, string(p))
						}
					}
					if alt != nil {
						res, err := alt.res.shallow(msg)
						if err != nil {
							t.Fatal(err)
						}
						if alt.res.deep != nil {
							res, err = alt.res.deep(context.Background(), s, msg)
							if err != nil {
								t.Fatal(err)
							}
						}
						// Held at a namespace: it admits a request of that
						// namespace, and one that names none is refused.
						a, _ := accessFor(t, authn.ModeEnforce, []*orkstackv1.Role{{Name: "only-alt", Permissions: []string{string(alt.perm)}}},
							&orkstackv1.Binding{Name: "b", Role: "only-alt", Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
						s.access = a
						_, err = a.decide(asAlice(authn.ModeEnforce), s, procedure, msg)
						switch {
						case res.Namespace == "payments" && err != nil:
							t.Errorf("%s alone at namespace:payments is refused: %v", alt.perm, err)
						case res.Namespace == "" && err == nil:
							t.Errorf("%s held at a namespace admitted a request that names none", alt.perm)
						}
					}
					a, _ := accessFor(t, authn.ModeEnforce, []*orkstackv1.Role{{Name: "all-but", Permissions: others}},
						&orkstackv1.Binding{Name: "b", Role: "all-but", Scope: "deployment", Subjects: []string{"user:corp:alice"}})
					s.access = a
					_, err := a.decide(asAlice(authn.ModeEnforce), s, procedure, msg)
					if err == nil {
						t.Errorf("every permission but %s at the deployment is admitted", c.perm)
					}
				}
				cases++
			})
		}
	}
	if cases == 0 {
		t.Fatal("the matrix ran no case")
	}
}

// TestAuthz_RunReadDeniedAsNotFound: a run the caller may not read is
// answered not_found, as one that does not exist.
func TestAuthz_RunReadDeniedAsNotFound(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s.access = a
	_, err := a.decide(asAlice(authn.ModeEnforce), s, orkstackv1connect.RunServiceGetRunProcedure,
		&orkstackv1.GetRunRequest{RunId: "ork-xyz"})
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("a run the caller may not read = %v; want not_found", err)
	}
}

// TestAuthz_ListRunsNeedsAFilterInsideTheCallersScope: a key-walked list is
// never answered from a subset; a caller bound at one Orkflow must filter
// by it.
func TestAuthz_ListRunsNeedsAFilterInsideTheCallersScope(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "orkflow:payments/deploy", Subjects: []string{"user:corp:alice"}})
	s.access = a
	ctx := asAlice(authn.ModeEnforce)
	list := func(filters ...*orkstackv1.FieldFilter) error {
		_, err := a.decide(ctx, s, orkstackv1connect.RunServiceListRunsProcedure, &orkstackv1.ListRunsRequest{Namespace: "payments", Filters: filters})
		return err
	}
	if err := list(); connect.CodeOf(err) != connect.CodePermissionDenied || !strings.Contains(err.Error(), "orkflow_name=") {
		t.Errorf("no filter = %v; want permission_denied naming the filter to add", err)
	}
	eq := func(vs ...string) *orkstackv1.FieldFilter {
		op := orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS
		if len(vs) > 1 {
			op = orkstackv1.FilterOperator_FILTER_OPERATOR_IN
		}
		return &orkstackv1.FieldFilter{Field: "orkflow_name", Op: op, Values: vs}
	}
	if err := list(eq("deploy")); err != nil {
		t.Errorf("orkflow_name=deploy: %v", err)
	}
	if err := list(eq("deploy", "nightly")); err == nil {
		t.Error("orkflow_name in (deploy, nightly) was admitted with nightly outside the caller's scope")
	}
	if err := list(&orkstackv1.FieldFilter{Field: "orkflow_name", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX, Values: []string{"dep"}}); err == nil {
		t.Error("a prefix filter was taken as keeping the list inside")
	}
}

// TestAuthz_TokenPagedListsNeedANameFilter: ListOrkflows and
// ListNamespaces page by a cursor that carries the last row's name, so a
// caller who may read only some rows must filter by name, and a cursor can
// never name a row it may not see (security review M3).
func TestAuthz_TokenPagedListsNeedANameFilter(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "orkflow:payments/deploy", Subjects: []string{"user:corp:alice"}})
	s.access = a
	ctx := asAlice(authn.ModeEnforce)
	name := func(vs ...string) []*orkstackv1.FieldFilter {
		return []*orkstackv1.FieldFilter{{Field: "name", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_IN, Values: vs}}
	}
	if _, _, err := s.admitCall(ctx, orkstackv1connect.OrkflowServiceListOrkflowsProcedure, &orkstackv1.ListOrkflowsRequest{Namespace: "payments"}, ""); err == nil {
		t.Error("a partial ListOrkflows with no name filter was admitted")
	}
	if _, _, err := s.admitCall(ctx, orkstackv1connect.OrkflowServiceListOrkflowsProcedure, &orkstackv1.ListOrkflowsRequest{Namespace: "payments", Filters: name("deploy", "nightly")}, ""); err == nil {
		t.Error("a name filter reaching a hidden Orkflow was admitted")
	}
	req := &orkstackv1.ListOrkflowsRequest{Namespace: "payments", Filters: name("deploy")}
	ctx2, adm, err := s.admitCall(ctx, orkstackv1connect.OrkflowServiceListOrkflowsProcedure, req, "")
	if err != nil {
		t.Fatalf("name=deploy: %v", err)
	}
	resp := &orkstackv1.ListOrkflowsResponse{Orkflows: []*orkstackv1.Orkflow{{Name: "deploy"}, {Name: "nightly"}}}
	s.filterRowsOf(ctx2, adm, req, resp)
	if len(resp.GetOrkflows()) != 1 || resp.GetOrkflows()[0].GetName() != "deploy" {
		t.Fatalf("rows = %v; want deploy alone", resp.GetOrkflows())
	}
	b, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
	s.access = b
	if _, _, err := s.admitCall(ctx, orkstackv1connect.NamespaceServiceListNamespacesProcedure, &orkstackv1.ListNamespacesRequest{}, ""); err == nil {
		t.Error("a partial ListNamespaces with no name filter was admitted")
	}
	if _, _, err := s.admitCall(ctx, orkstackv1connect.NamespaceServiceListNamespacesProcedure, &orkstackv1.ListNamespacesRequest{Filters: name("payments")}, ""); err != nil {
		t.Errorf("ListNamespaces name=payments: %v", err)
	}
}

// TestAuthz_ObserveRefusesNothingAndRecordsWouldDeny.
func TestAuthz_ObserveRefusesNothingAndRecordsWouldDeny(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeObserve, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s.access = a
	ctx, _, err := s.admitCall(asAlice(authn.ModeObserve), orkstackv1connect.OrkflowServiceGetOrkflowProcedure,
		&orkstackv1.GetOrkflowRequest{Namespace: "payments", Name: "deploy"}, "10.0.0.1:1")
	if err != nil {
		t.Fatalf("observe refused: %v", err)
	}
	if d := decisionFrom(ctx); d == nil || d.Decision != DecisionWouldDeny || d.Permission != authz.PermOrkflowRead {
		t.Fatalf("decision = %+v; want would-deny of orkflow.read", d)
	}
	recs := auditRecords(t, s)
	if len(recs) != 1 || recs[0].GetDecision() != DecisionWouldDeny || recs[0].GetSchemaVersion() != 2 || !recs[0].GetPrincipalVerified() ||
		recs[0].GetPermission() != "orkflow.read" || recs[0].GetScope() != "orkflow:payments/deploy" {
		t.Fatalf("records = %v; want one version-2 would-deny", recs)
	}
	// The same in enforce mode is refused, with a denied record.
	s2 := newAccessService(t)
	a2, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s2.access = a2
	_, _, err = s2.admitCall(asAlice(authn.ModeEnforce), orkstackv1connect.OrkflowServiceGetOrkflowProcedure,
		&orkstackv1.GetOrkflowRequest{Namespace: "payments", Name: "deploy"}, "10.0.0.1:1")
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("enforce = %v; want permission_denied", err)
	}
	if recs := auditRecords(t, s2); len(recs) != 1 || recs[0].GetDecision() != DecisionDenied {
		t.Fatalf("records = %v; want one denied", recs)
	}
}

func auditRecords(t testing.TB, s *Service) []*orkstackv1.AuditRecord {
	t.Helper()
	mem := s.audit.store.(*artifacts.MemoryAuditStore)
	var out []*orkstackv1.AuditRecord
	for _, body := range mem.Documents() {
		rec, err := artifacts.UnmarshalAuditRecord(body)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rec)
	}
	return out
}

func TestDenialLimiter(t *testing.T) {
	now := time.Unix(0, 0)
	l := newDenialLimiter(func() time.Time { return now })
	if !l.allow("p", "/x", "s") || l.allow("p", "/x", "s") {
		t.Fatal("one record a minute per principal, procedure and scope")
	}
	allowed := 1
	for i := 0; i < 20; i++ {
		if l.allow("p", "/x", fmt.Sprintf("scope-%d", i)) {
			allowed++
		}
	}
	if allowed != denialsPerMinute {
		t.Fatalf("a principal varying the scope wrote %d records; at most %d a minute", allowed, denialsPerMinute)
	}
	if !l.allow("q", "/x", "s") {
		t.Fatal("another principal is limited by p's records")
	}
	now = now.Add(denialWindow)
	if !l.allow("p", "/x", "s") {
		t.Fatal("the limit does not lift after a minute")
	}
}

// TestAuthz_StreamClosedWhenAccessWithdrawn: an open stream is checked
// again at a change of the access document and closed when the caller lost
// the permission; a change that keeps it leaves it open.
func TestAuthz_StreamClosedWhenAccessWithdrawn(t *testing.T) {
	s := newAccessService(t)
	a, cas := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
	s.access = a
	ctx, adm, err := s.admitCall(asAlice(authn.ModeEnforce), orkstackv1connect.RunServiceStreamEventsProcedure,
		&orkstackv1.StreamEventsRequest{RunId: "r", Namespace: "payments"}, "")
	if err != nil {
		t.Fatal(err)
	}
	rechecks := make(chan error, 4)
	a.rechecked = func(err error) { rechecks <- err }
	ctx, stop := s.watchAccess(ctx, adm)
	defer stop()
	store := artifacts.NewAccessStoreOn(cas)
	head, _ := store.Get(context.Background())
	// A change that keeps the binding: still open.
	head, err = store.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{
		{Name: "b", Role: authz.RoleViewer, Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}},
		{Name: "c", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:bob"}},
	}}, head, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	a.Policies.Refresh(context.Background())
	// Positive control: the watcher re-checked this change, and kept it.
	select {
	case err := <-rechecks:
		if err != nil {
			t.Fatalf("a change that kept the permission re-checked as %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the watcher never re-checked the change")
	}
	if ctx.Err() != nil {
		t.Fatal("a stream closed on a change that kept its permission")
	}
	// The binding removed: closed, permission_denied.
	if _, err := store.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{
		{Name: "c", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:bob"}},
	}}, head, artifacts.AccessWrite{At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	a.Policies.Refresh(context.Background())
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a stream stayed open after its permission was removed")
	}
	if !socketWithdrawn(ctx) || connect.CodeOf(context.Cause(ctx)) != connect.CodePermissionDenied {
		t.Fatalf("cause = %v; want permission_denied, withdrawn", context.Cause(ctx))
	}
}

// TestNewMux_AssertedInstallsNothing: in asserted mode the mux is v0.3.0's:
// no middleware, no Access route.
func TestNewMux_AssertedInstallsNothing(t *testing.T) {
	s := newAccessService(t)
	for _, access := range []*AccessControl{nil, {Mode: authn.ModeAsserted}} {
		mux := NewMux(s, MuxOptions{Access: access})
		req := httptest.NewRequest(http.MethodPost, orkstackv1connect.AccessServiceGetAccessProcedure, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("asserted: AccessService answers %d; want 404 (not mounted)", rec.Code)
		}
		if s.access != nil {
			t.Error("asserted mode installed access control")
		}
	}
}

// TestAuthn_EveryMuxPatternIsClassified walks the real mux: every pattern it
// mounts is exempt with a reason, a Connect service whose procedures all
// have rows, or a route decided by a procedure's row.
func TestAuthn_EveryMuxPatternIsClassified(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil)
	_, patterns := newMux(s, MuxOptions{Access: a, UI: http.NotFoundHandler()})
	procedures := orkstackProcedures(t)
	for _, pattern := range patterns {
		if _, ok := exemptPatterns[pattern]; ok {
			continue
		}
		if procedure, ok := routeRows[pattern]; ok {
			if _, has := accessRows[procedure]; !has && procedure != legacyWebhookRoute {
				t.Errorf("route %s is decided by %s, which has no row", pattern, procedure)
			}
			continue
		}
		if strings.HasPrefix(pattern, "/orkstack.v1.") {
			found := false
			for procedure := range procedures {
				if strings.HasPrefix(procedure, pattern) || procedure == pattern {
					found = true
					if _, ok := accessRows[procedure]; !ok {
						t.Errorf("%s is mounted with no row", procedure)
					}
				}
			}
			if !found {
				t.Errorf("Connect pattern %s matches no declared procedure", pattern)
			}
			continue
		}
		t.Errorf("pattern %s is neither exempt, a Connect service nor a classified route", pattern)
	}
	for pattern := range exemptPatterns {
		if pattern != "/" && !containsString(patterns, pattern) {
			t.Errorf("exempt pattern %s is not mounted", pattern)
		}
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// FuzzRouteClassification: in enforce mode no path reaches a handler
// outside the exempt patterns without a principal.
func FuzzRouteClassification(f *testing.F) {
	for _, seed := range []string{"/", "/orkstack.v1.RunService/GetRun", "/ws/runs/x", "/v1/webhooks", "/v1/webhooks/x",
		"/v1/triggers/x", "/v1/runs/r/awaits/t", "/orkstack.v1.RunService/../RunService/GetRun", "//v1/webhooks", "/V1/webhooks",
		"/v1/webhooks/%2e%2e", "/ws/runs/x/tasks/t/logs", "/orkstack.v1.AccessService/GetAccess", "/_app/x.js"} {
		f.Add("POST", seed)
		f.Add("GET", seed)
	}
	s := newAccessService(f)
	a, _ := accessFor(f, authn.ModeEnforce, nil)
	a.Authenticator = &authn.Authenticator{}
	served := map[string]bool{}
	ui := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { served["/"] = true })
	inner, _ := newMux(s, MuxOptions{Access: a, UI: ui})
	outer := NewMux(s, MuxOptions{Access: a, UI: ui})
	f.Fuzz(func(t *testing.T, method, path string) {
		if !strings.HasPrefix(path, "/") || strings.ContainsAny(path, " \r\n\x00?#") || method == "" || strings.ContainsAny(method, " \r\n\x00") {
			return
		}
		req, err := http.NewRequest(method, "http://ork.test"+path, strings.NewReader("{}"))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		_, pattern := inner.Handler(req)
		rec := httptest.NewRecorder()
		outer.ServeHTTP(rec, req)
		if _, exempt := exemptPatterns[pattern]; exempt {
			return
		}
		if rec.Code != http.StatusUnauthorized && rec.Code < 300 {
			t.Fatalf("%s %s (pattern %q) answered %d with no credential", method, path, pattern, rec.Code)
		}
		if rec.Code != http.StatusUnauthorized && pattern != "" {
			// A redirect or a 404 from the mux itself is no handler reached.
			if rec.Code != http.StatusMovedPermanently && rec.Code != http.StatusTemporaryRedirect &&
				rec.Code != http.StatusPermanentRedirect && rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s (pattern %q) answered %d with no credential; want 401", method, path, pattern, rec.Code)
			}
		}
	})
}

// TestPrincipal_HeaderIsReadOnlyInAssertedMode: the Ork-Principal header
// has one reader, inside authn; every site takes its principal from
// authn.Recorded. It reads the syntax tree, not the text: every identifier
// PrincipalHeader or AuditPrincipalHeader and every string literal
// "Ork-Principal" in non-test Go is counted by file, so a new use in any
// form (Get, Values, an index, an alias) changes a count and fails here.
func TestPrincipal_HeaderIsReadOnlyInAssertedMode(t *testing.T) {
	root := filepath.Join("..", "..")
	uses := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "gen", "ui", "site", ".data", "proof":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.Ident:
				if n.Name == "PrincipalHeader" || n.Name == "AuditPrincipalHeader" {
					uses[rel] = append(uses[rel], fmt.Sprintf("%d:%s", fset.Position(n.Pos()).Line, n.Name))
				}
			case *ast.BasicLit:
				if v, err := strconv.Unquote(n.Value); n.Kind == token.STRING && err == nil && strings.EqualFold(v, "Ork-Principal") {
					uses[rel] = append(uses[rel], fmt.Sprintf("%d:%s", fset.Position(n.Pos()).Line, n.Value))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The declarations, the one reader (authn.Recorded), the API's alias,
	// and orkctl's own constant, which only sends the header.
	want := map[string]int{
		filepath.Join("internal", "authn", "middleware.go"): 3,
		filepath.Join("internal", "api", "audit.go"):        2,
		filepath.Join("cmd", "orkctl", "root.go"):           1,
	}
	for file, at := range uses {
		if len(at) != want[file] {
			t.Errorf("%s names Ork-Principal %d times, want %d: %v", file, len(at), want[file], at)
		}
	}
	for file, n := range want {
		if _, ok := uses[file]; !ok && n > 0 {
			t.Errorf("%s no longer names Ork-Principal; update this test's table", file)
		}
	}
}

func TestPolicyCache_FreshStaleThenClosed(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	if _, err := store.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{
		{Name: "b", Role: authz.RoleViewer, Scope: "deployment", Subjects: []string{"user:corp:a"}}}}, nil, artifacts.AccessWrite{At: now}); err != nil {
		t.Fatal(err)
	}
	c := NewPolicyCache(store, PolicyCacheOptions{Refresh: 10 * time.Second, StaleLimit: 5 * time.Minute, Now: func() time.Time { return now }})
	ctx := context.Background()
	if p, err := c.Policy(ctx); err != nil || p.Revision != 1 {
		t.Fatalf("first read = %v, %v", p, err)
	}
	cas.SetFailure(errors.New("store down"))
	now = now.Add(4 * time.Minute)
	if p, err := c.Policy(ctx); err != nil || p.Revision != 1 {
		t.Fatalf("within the stale limit = %v, %v; want the last good copy", p, err)
	}
	if st := c.State(); !st.Stale || st.Unavailable {
		t.Fatalf("state = %+v; want stale", st)
	}
	now = now.Add(2 * time.Minute)
	if _, err := c.Policy(ctx); !errors.Is(err, ErrPolicyUnavailable) {
		t.Fatalf("past the stale limit = %v; want ErrPolicyUnavailable", err)
	}
	cas.SetFailure(nil)
	now = now.Add(2 * time.Second)
	if p, err := c.Policy(ctx); err != nil || p.Revision != 1 {
		t.Fatalf("after the store came back = %v, %v; want it recovered", p, err)
	}
}

func TestPolicyCache_StaleLimitZeroClosesAtOnce(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cas := artifacts.NewMemoryCAS()
	c := NewPolicyCache(artifacts.NewAccessStoreOn(cas), PolicyCacheOptions{StaleLimit: -1, Now: func() time.Time { return now }})
	if _, err := c.Policy(context.Background()); err != nil {
		t.Fatal(err)
	}
	cas.SetFailure(errors.New("down"))
	now = now.Add(DefaultPolicyRefresh)
	if _, err := c.Policy(context.Background()); !errors.Is(err, ErrPolicyUnavailable) {
		t.Fatalf("a zero stale limit served a stale copy: %v", err)
	}
}

func TestPolicyCache_WrittenAroundTheAPI(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	doc := &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{
		{Name: "b", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:a"}}}}
	head, err := store.Write(ctx, doc, nil, artifacts.AccessWrite{At: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	c := NewPolicyCache(store, PolicyCacheOptions{})
	c.Refresh(ctx)
	if c.State().WrittenAroundAPI {
		t.Fatal("a head written through the API is reported written around it")
	}
	// Two writes through the API while the replica was not looking: still
	// a chain it can follow.
	head, _ = store.Write(ctx, doc, head, artifacts.AccessWrite{At: time.Now()})
	_, _ = store.Write(ctx, doc, head, artifacts.AccessWrite{At: time.Now()})
	c.Refresh(ctx)
	if c.State().WrittenAroundAPI {
		t.Fatal("two writes the replica missed are reported written around the API")
	}
	// A head put straight into the bucket, with no revision.
	body, _ := artifacts.EncodeAccessObject(doc, artifacts.AccessRevisionMeta{Revision: 9, WrittenAt: time.Now()})
	cas.PutRaw(artifacts.AccessDocumentKey, body)
	c.Refresh(ctx)
	st := c.State()
	if !st.WrittenAroundAPI || st.Revision != 9 {
		t.Fatalf("a head written around the API = %+v; want it reported, and used", st)
	}
	c.Acknowledge(9)
	if c.State().WrittenAroundAPI {
		t.Fatal("acknowledged, still reported")
	}
}

func TestModeGuard_Boot(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	boot := func(cas *artifacts.MemoryCAS, mode string) error {
		g := &ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: mode, Now: func() time.Time { return now }}
		return g.Boot(ctx)
	}
	recorded := func(cas *artifacts.MemoryCAS) string {
		rec, _, err := artifacts.NewModeStoreOn(cas).Get(ctx)
		if err != nil {
			return "none"
		}
		return rec.Mode
	}
	// No record: asserted writes nothing; enforce writes it.
	cas := artifacts.NewMemoryCAS()
	if err := boot(cas, authn.ModeAsserted); err != nil || recorded(cas) != "none" {
		t.Fatalf("asserted with no record = %v, %s", err, recorded(cas))
	}
	if err := boot(cas, authn.ModeObserve); err != nil || recorded(cas) != authn.ModeObserve {
		t.Fatalf("observe with no record = %v, %s", err, recorded(cas))
	}
	// A lower record is raised; an equal one kept.
	if err := boot(cas, authn.ModeEnforce); err != nil || recorded(cas) != authn.ModeEnforce {
		t.Fatalf("enforce over observe = %v, %s", err, recorded(cas))
	}
	if err := boot(cas, authn.ModeEnforce); err != nil {
		t.Fatalf("enforce over enforce = %v", err)
	}
	// A higher record refuses, naming both.
	for _, lower := range []string{authn.ModeObserve, authn.ModeAsserted} {
		err := boot(cas, lower)
		if !errors.Is(err, ErrModeRecordHigher) || !strings.Contains(err.Error(), "enforce") || !strings.Contains(err.Error(), lower) {
			t.Fatalf("%s under an enforce record = %v; want a refusal naming both", lower, err)
		}
	}
	// A record that does not decode refuses every mode, naming the object.
	cas.PutRaw(artifacts.ModeRecordKey, []byte("garbage"))
	for _, mode := range []string{authn.ModeAsserted, authn.ModeObserve, authn.ModeEnforce} {
		if err := boot(cas, mode); err == nil || !strings.Contains(err.Error(), artifacts.ModeRecordKey) {
			t.Fatalf("%s over an unreadable record = %v; want a refusal naming it", mode, err)
		}
	}
	// A store that cannot be read: no start.
	cas.SetFailure(errors.New("down"))
	if err := boot(cas, authn.ModeEnforce); err == nil {
		t.Fatal("an engine started with the store unreadable")
	}
}

func TestModeGuard_Lower(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0).UTC()
	cas := artifacts.NewMemoryCAS()
	g := &ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: authn.ModeEnforce, ClaimTTL: 24 * time.Hour, Now: func() time.Time { return now }}
	if err := g.Boot(ctx); err != nil {
		t.Fatal(err)
	}
	etag := g.Status().ETag
	var refused *LowerRefusal
	if _, err := g.Lower(ctx, authn.ModeEnforce, etag, false, "user:corp:a", ""); !errors.As(err, &refused) {
		t.Fatalf("lowering to the same mode = %v; want refused", err)
	}
	if _, err := g.Lower(ctx, authn.ModeAsserted, etag, false, "user:corp:a", ""); !errors.As(err, &refused) || !refused.Precondition ||
		!strings.Contains(err.Error(), "observe first") {
		t.Fatalf("enforce to asserted = %v; want failed_precondition, observe first", err)
	}
	if _, err := g.Lower(ctx, authn.ModeObserve, `"stale"`, false, "user:corp:a", ""); !errors.As(err, &refused) {
		t.Fatalf("a stale etag = %v; want refused", err)
	}
	prev, err := g.Lower(ctx, authn.ModeObserve, etag, false, "user:corp:a", "")
	if err != nil || prev != authn.ModeEnforce {
		t.Fatalf("enforce to observe = %q, %v; never refused", prev, err)
	}
	etag = g.Status().ETag
	if _, err := g.Lower(ctx, authn.ModeAsserted, etag, false, "user:corp:a", ""); err == nil {
		t.Fatal("asserted was allowed inside a claim's life after leaving enforce")
	}
	if safe := g.Status().AssertedSafe; !safe.Equal(now.Add(24*time.Hour + claimGrace)) {
		t.Fatalf("asserted safe from %v; want one claim's life and its grace after leaving enforce", safe)
	}
	now = now.Add(24*time.Hour + claimGrace)
	if _, err := g.Lower(ctx, authn.ModeAsserted, etag, false, "user:corp:a", ""); err != nil {
		t.Fatalf("asserted after the wait: %v", err)
	}
	// A record lowered: an engine of the higher mode still boots, raising
	// it; one of the lower mode boots too.
	g2 := &ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: authn.ModeAsserted, Now: func() time.Time { return now }}
	if err := g2.Boot(ctx); err != nil {
		t.Fatalf("asserted after the lowering: %v", err)
	}
}

func TestModeGuard_CheckWritesAMissingRecordAgain(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	missing := 0
	g := &ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: authn.ModeEnforce, Missing: func(context.Context) { missing++ }}
	if err := g.Boot(ctx); err != nil {
		t.Fatal(err)
	}
	cas.Delete(artifacts.ModeRecordKey)
	g.Check(ctx)
	if missing != 1 {
		t.Fatalf("a record gone was counted %d times", missing)
	}
	if rec, _, err := g.Store.Get(ctx); err != nil || rec.Mode != authn.ModeEnforce {
		t.Fatalf("the record was not written again: %+v, %v", rec, err)
	}
	cas.PutRaw(artifacts.ModeRecordKey, []byte(`{"format_version":1,"mode":"enforce","raised_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}`))
	g2 := &ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: authn.ModeObserve}
	g2.Check(ctx)
	if !strings.Contains(g2.Status().Problem, "enforce") {
		t.Fatalf("a higher record while running = %q; want it reported", g2.Status().Problem)
	}
}

// TestGetSystemInfo_AuthMode: a client learns the mode from GetSystemInfo;
// an asserted engine leaves the field unset, as before.
func TestGetSystemInfo_AuthMode(t *testing.T) {
	s := newAccessService(t)
	resp, err := s.GetSystemInfo(context.Background(), connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
	if err != nil || resp.Msg.GetAuthMode() != "" {
		t.Fatalf("asserted = %q, %v; want unset", resp.Msg.GetAuthMode(), err)
	}
	s.access, _ = accessFor(t, authn.ModeEnforce, nil)
	resp, err = s.GetSystemInfo(context.Background(), connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
	if err != nil || resp.Msg.GetAuthMode() != authn.ModeEnforce {
		t.Fatalf("enforce = %q, %v", resp.Msg.GetAuthMode(), err)
	}
}

// TestAuthz_DocumentsAreDecidedWhereTheyAct: a request that carries a
// document is decided in the namespace its handler will act in, the
// document's own when the request names none (security review C1).
func TestAuthz_DocumentsAreDecidedWhereTheyAct(t *testing.T) {
	inPayments := "namespace: payments\nname: x\n"
	cases := []struct {
		procedure string
		msg       proto.Message
		perm      string
	}{
		{orkstackv1connect.OrkflowServiceApplyOrkflowProcedure, &orkstackv1.ApplyOrkflowRequest{Yaml: inPayments}, "orkflow.write"},
		{orkstackv1connect.OrkflowServiceValidateOrkflowProcedure, &orkstackv1.ValidateOrkflowRequest{Yaml: inPayments}, "orkflow.validate"},
		{orkstackv1connect.OrkflowServicePlanRunProcedure, &orkstackv1.PlanRunRequest{Yaml: inPayments}, "orkflow.read"},
		{orkstackv1connect.AutomationServicePlanAutomationProcedure, &orkstackv1.PlanAutomationRequest{Yaml: "namespace: payments\n"}, "automation.plan"},
	}
	for _, tc := range cases {
		t.Run(tc.procedure, func(t *testing.T) {
			s := newAccessService(t)
			a, _ := accessFor(t, authn.ModeEnforce, []*orkstackv1.Role{{Name: "r", Permissions: []string{tc.perm}}},
				&orkstackv1.Binding{Name: "d", Role: "r", Scope: "namespace:default", Subjects: []string{"user:corp:alice"}})
			s.access = a
			if _, err := a.decide(asAlice(authn.ModeEnforce), s, tc.procedure, tc.msg); err == nil {
				t.Fatal("a caller bound at default was admitted for a document that acts in payments")
			}
			b, _ := accessFor(t, authn.ModeEnforce, []*orkstackv1.Role{{Name: "r", Permissions: []string{tc.perm}}},
				&orkstackv1.Binding{Name: "p", Role: "r", Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
			s.access = b
			if _, err := b.decide(asAlice(authn.ModeEnforce), s, tc.procedure, tc.msg); err != nil {
				t.Fatalf("a caller bound at payments: %v", err)
			}
		})
	}
	// A merge key or an aliased key hides nothing: decided at the
	// deployment, where a caller bound at default is refused.
	for name, yaml := range map[string]string{
		"merge key":    "<<: {namespace: payments}\nname: x\n",
		"aliased key":  "description: &k namespace\n*k : payments\nname: x\n",
		"tagged key":   "!!str namespace: payments\nname: x\n",
		"anchored key": "&a namespace: payments\nname: x\n",
	} {
		s := newAccessService(t)
		a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "d", Role: authz.RoleAdmin, Scope: "namespace:default", Subjects: []string{"user:corp:alice"}})
		s.access = a
		if _, err := a.decide(asAlice(authn.ModeEnforce), s, orkstackv1connect.OrkflowServiceApplyOrkflowProcedure,
			&orkstackv1.ApplyOrkflowRequest{Yaml: yaml}); err == nil {
			t.Errorf("%s: decided at default", name)
		}
	}
	// Both named and different: refused before anything is read, whatever
	// the caller holds.
	{
		s := newAccessService(t)
		a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "d", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:alice"}})
		s.access = a
		_, err := a.decide(asAlice(authn.ModeEnforce), s, orkstackv1connect.OrkflowServiceApplyOrkflowProcedure,
			&orkstackv1.ApplyOrkflowRequest{Namespace: "default", Yaml: inPayments})
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("a request and a document that disagree = %v; want invalid_argument", err)
		}
	}
	// An unreadable head is decided at the deployment even with -n set.
	{
		s := newAccessService(t)
		a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "d", Role: authz.RoleAdmin, Scope: "namespace:default", Subjects: []string{"user:corp:alice"}})
		s.access = a
		if _, err := a.decide(asAlice(authn.ModeEnforce), s, orkstackv1connect.OrkflowServiceApplyOrkflowProcedure,
			&orkstackv1.ApplyOrkflowRequest{Namespace: "default", Yaml: "namespace: [payments]\n"}); err == nil {
			t.Error("an unreadable head with -n set was decided at the request's namespace")
		}
	}
	// A document whose namespace cannot be read is decided at the
	// deployment: closed, never the request's default.
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "d", Role: authz.RoleAdmin, Scope: "namespace:default", Subjects: []string{"user:corp:alice"}})
	s.access = a
	if _, err := a.decide(asAlice(authn.ModeEnforce), s, orkstackv1connect.OrkflowServiceApplyOrkflowProcedure,
		&orkstackv1.ApplyOrkflowRequest{Yaml: "namespace: [payments]\n"}); err == nil {
		t.Fatal("an unreadable document namespace was decided at default")
	}
}

// TestAuthz_AccessServiceIsEnforcedInEveryMode: observe refuses nothing
// else, but the access document and the mode record are never read or
// written by a caller who does not hold access.* (security review H1).
func TestAuthz_AccessServiceIsEnforcedInEveryMode(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeObserve, nil, &orkstackv1.Binding{Name: "v", Role: authz.RoleViewer, Scope: "deployment", Subjects: []string{"user:corp:alice"}})
	s.access = a
	anonymous := authn.WithMode(context.Background(), authn.ModeObserve)
	for _, procedure := range []string{orkstackv1connect.AccessServiceLowerAuthModeProcedure, orkstackv1connect.AccessServiceGetAccessProcedure} {
		for name, ctx := range map[string]context.Context{"anonymous": anonymous, "a viewer": asAlice(authn.ModeObserve)} {
			if _, _, err := s.admitCall(ctx, procedure, requestFor(t, procedure), ""); err == nil {
				t.Errorf("observe served %s to %s", procedure, name)
			}
		}
	}
	// Everything else is still only observed.
	if _, _, err := s.admitCall(anonymous, orkstackv1connect.OrkflowServiceGetOrkflowProcedure, requestFor(t, orkstackv1connect.OrkflowServiceGetOrkflowProcedure), ""); err != nil {
		t.Errorf("observe refused an ordinary read: %v", err)
	}
}

func TestModeGuard_RefusesToLowerFromBeneathTheRecord(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	enforcing := &ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: authn.ModeEnforce}
	if err := enforcing.Boot(ctx); err != nil {
		t.Fatal(err)
	}
	observing := &ModeGuard{Store: artifacts.NewModeStoreOn(cas), Mode: authn.ModeObserve}
	observing.Check(ctx)
	if _, err := observing.Lower(ctx, authn.ModeObserve, observing.Status().ETag, true, "x", ""); err == nil {
		t.Fatal("an engine in observe lowered an enforce record")
	}
	// A running engine reports a record lower than its own mode.
	if _, err := enforcing.Lower(ctx, authn.ModeObserve, enforcing.Status().ETag, false, "x", ""); err != nil {
		t.Fatal(err)
	}
	enforcing.Check(ctx)
	if !strings.Contains(enforcing.Status().Problem, "lower") {
		t.Fatalf("a record below this engine's mode = %q; want it reported", enforcing.Status().Problem)
	}
}

// TestPolicyCache_AHungStoreNeitherBlocksDecisionsNorStaysFresh: a store
// that accepts a read and never answers holds no decision: a read is
// bounded by its timeout, and a decision while one is in flight is answered
// from the copy, stale and then closed (security review M1).
func TestPolicyCache_AHungStoreNeitherBlocksDecisionsNorStaysFresh(t *testing.T) {
	cas := artifacts.NewMemoryCAS()
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	c := NewPolicyCache(artifacts.NewAccessStoreOn(cas), PolicyCacheOptions{Now: clock, ReadTimeout: 50 * time.Millisecond})
	if _, err := c.Policy(context.Background()); err != nil {
		t.Fatal(err)
	}
	hang, arrived := make(chan struct{}), make(chan struct{}, 1)
	defer close(hang)
	cas.SetHang(hang, arrived)
	mu.Lock()
	now = now.Add(time.Minute)
	mu.Unlock()

	// A refresh in flight: a decision is answered from the copy at once.
	refreshed := make(chan struct{})
	go func() { c.Refresh(context.Background()); close(refreshed) }()
	<-arrived // the refresh holds the read: the decision below must not wait on it
	answered := make(chan error, 1)
	go func() { _, err := c.Policy(context.Background()); answered <- err }()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatalf("a decision during a hung read = %v; want the stale copy", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a decision waited on a read the store never answered")
	}
	// And the read itself ends at its timeout.
	select {
	case <-refreshed:
	case <-time.After(5 * time.Second):
		t.Fatal("a read the store never answered was not bounded")
	}
}

// TestAuthz_ListRunArtifactsNeedsTheRunAndFiltersKinds: a caller who may
// not read the run learns nothing of it, not even that it exists; one who
// may gets only the kinds it may read (security review M4).
func TestAuthz_ListRunArtifactsNeedsTheRunAndFiltersKinds(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s.access = a
	_, _, err := s.admitCall(asAlice(authn.ModeEnforce), orkstackv1connect.RunServiceListRunArtifactsProcedure, &orkstackv1.ListRunArtifactsRequest{RunId: "ork-x"}, "")
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("a caller who may not read the run = %v; want not_found", err)
	}
	b, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:default", Subjects: []string{"user:corp:alice"}})
	s.access = b
	req := &orkstackv1.ListRunArtifactsRequest{RunId: "ork-x"}
	ctx, adm, err := s.admitCall(asAlice(authn.ModeEnforce), orkstackv1connect.RunServiceListRunArtifactsProcedure, req, "")
	if err != nil {
		t.Fatal(err)
	}
	resp := &orkstackv1.ListRunArtifactsResponse{Artifacts: []*orkstackv1.RunArtifact{
		{Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT}, {Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT}, {Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES}}}
	s.filterRowsOf(ctx, adm, req, resp)
	if len(resp.GetArtifacts()) != 1 || resp.GetArtifacts()[0].GetKind() != orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES {
		t.Fatalf("a viewer's artifacts = %v; want the values alone", resp.GetArtifacts())
	}
}

func TestAuthz_ObserveFiltersNoList(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeObserve, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "orkflow:payments/deploy", Subjects: []string{"user:corp:alice"}})
	s.access = a
	_, adm, err := s.admitCall(asAlice(authn.ModeObserve), orkstackv1connect.ScheduleServiceListSchedulesProcedure, &orkstackv1.ListSchedulesRequest{Namespace: "payments"}, "")
	if err != nil || adm.filterRows {
		t.Fatalf("observe = %v, filtered %v; it refuses and filters nothing", err, adm.filterRows)
	}
}

func TestAuthz_ADisabledAdminStaysDisabledWhenTheDocumentIsUnavailable(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	if _, err := store.Write(context.Background(), &orkstackv1.AccessDocument{SchemaVersion: 1, Disabled: []string{"user:corp:alice"}}, nil, artifacts.AccessWrite{At: now}); err != nil {
		t.Fatal(err)
	}
	admins, _ := authz.CompileConfiguration(authz.Configuration{Admins: []string{"user:corp:alice"}})
	s := newAccessService(t)
	s.access = &AccessControl{Mode: authn.ModeEnforce, Configured: admins, Now: func() time.Time { return now },
		Policies: NewPolicyCache(store, PolicyCacheOptions{Configured: admins, Now: func() time.Time { return now }})}
	if _, err := s.access.Policies.Policy(context.Background()); err != nil {
		t.Fatal(err)
	}
	cas.SetFailure(errors.New("down"))
	now = now.Add(time.Hour)
	if _, _, err := s.admitCall(asAlice(authn.ModeEnforce), orkstackv1connect.SystemServiceGetSystemInfoProcedure, &orkstackv1.GetSystemInfoRequest{}, ""); err == nil {
		t.Fatal("a disabled auth.admins subject was served once the document was unavailable")
	}
}

func TestAuthz_DenialRecordsNameTheDecidedNamespaceAndOnlyDenials(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s.access = a
	_, _, _ = s.admitCall(asAlice(authn.ModeEnforce), orkstackv1connect.OrkflowServiceApplyOrkflowProcedure,
		&orkstackv1.ApplyOrkflowRequest{Yaml: "namespace: payments\nname: x\n"}, "")
	recs := auditRecords(t, s)
	if len(recs) != 1 || recs[0].GetNamespace() != "payments" {
		t.Fatalf("records = %v; want one, in payments where the request would act", recs)
	}
	// A request refused for what it is, not by the policy, is no denial:
	// nothing recorded.
	s2 := newAccessService(t)
	a2, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s2.access = a2
	_, _, err := s2.admitCall(asAlice(authn.ModeEnforce), orkstackv1connect.OrkflowServiceGetOrkflowProcedure, &orkstackv1.GetOrkflowRequest{Namespace: "Not A Slug", Name: "x"}, "")
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("an invalid namespace = %v; want invalid_argument", err)
	}
	if recs := auditRecords(t, s2); len(recs) != 0 {
		t.Fatalf("a failed lookup was recorded as a denial: %v", recs)
	}
}

func TestAuthz_AStreamClosesWhenItsCredentialExpires(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "deployment", Subjects: []string{"user:corp:alice"}})
	s.access = a
	p := *alice
	p.Expires = time.Now().Add(100 * time.Millisecond)
	ctx := authn.WithPrincipal(authn.WithMode(context.Background(), authn.ModeEnforce), &p)
	ctx, adm, err := s.admitCall(ctx, orkstackv1connect.RunServiceStreamEventsProcedure, &orkstackv1.StreamEventsRequest{RunId: "r"}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := s.watchAccess(ctx, adm)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a stream outlived its credential")
	}
	if connect.CodeOf(context.Cause(ctx)) != connect.CodeUnauthenticated {
		t.Fatalf("cause = %v; want unauthenticated", context.Cause(ctx))
	}
}

// FuzzDocumentHead holds the interceptor's head reader to the DSL's: when
// documentHead reads a head, the namespace a struct decode of the same
// document takes (merge keys and aliases honoured, as the DSL's are) is
// the same one.
func FuzzDocumentHead(f *testing.F) {
	for _, seed := range []string{
		"namespace: payments\nname: x\n", "<<: {namespace: payments}\nname: x\n",
		"description: &k namespace\n*k : payments\n", "!!str namespace: payments\n", "&a namespace: p\n",
		"name: x\n", "namespace: [p]\n", "",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, doc string) {
		ns, _, ok := documentHead(doc)
		if !ok {
			return
		}
		var decoded struct {
			Namespace string `yaml:"namespace"`
		}
		if yaml.Unmarshal([]byte(doc), &decoded) != nil {
			return
		}
		if decoded.Namespace != ns {
			t.Fatalf("documentHead read namespace %q; a struct decode takes %q", ns, decoded.Namespace)
		}
	})
}

// stubRegistry knows a fixed set of namespaces.
type stubRegistry struct {
	names map[string]bool
	list  []string
}

func (r stubRegistry) Get(_ context.Context, name string) (*orkstackv1.Namespace, error) {
	name = artifacts.ResolveNamespace(name)
	if name == artifacts.DefaultNamespace || r.names[name] {
		return &orkstackv1.Namespace{Name: name}, nil
	}
	return nil, artifacts.ErrNotFound
}
func (r stubRegistry) List(context.Context) ([]*orkstackv1.Namespace, error) {
	out := make([]*orkstackv1.Namespace, 0, len(r.list))
	for _, n := range r.list {
		out = append(out, &orkstackv1.Namespace{Name: n})
	}
	return out, nil
}
func (r stubRegistry) ListNames(context.Context) ([]string, error) { return nil, nil }
func (r stubRegistry) Put(_ context.Context, ns *orkstackv1.Namespace, _ artifacts.PutConditions) (*orkstackv1.Namespace, error) {
	return ns, nil
}
func (r stubRegistry) Delete(context.Context, string) error { return nil }

// TestEnrichVersionRequired_ListsOnlyWhatTheCallerMayRead: a composed
// task's missing version names the versions of another namespace's
// Orkflow only to a caller who may read it (identity rev18, HIGH).
func TestEnrichVersionRequired_ListsOnlyWhatTheCallerMayRead(t *testing.T) {
	ctx := context.Background()
	orkflows := engine.NewMemoryOrkflowStore()
	if _, _, err := orkflows.Put(ctx, &orkstackv1.Orkflow{Namespace: "payments", Name: "child", Version: "1.0.0"}, artifacts.HeadExpectation{}); err != nil {
		t.Fatal(err)
	}
	s := NewService(Config{Orkflows: orkflows, Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Namespaces: stubRegistry{names: map[string]bool{"payments": true}}})
	missing := &dsl.ComposeVersionRequiredError{Name: "child", Namespace: "payments", Qualified: true}
	enrich := func(binding string) string {
		a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: binding, Subjects: []string{"user:corp:alice"}})
		s.access = a
		decided := withDecision(asAlice(authn.ModeEnforce), &accessDecision{Scope: "namespace:default"})
		return s.enrichVersionRequired(decided, "", missing).Error()
	}
	if got := enrich("namespace:default"); strings.Contains(got, "1.0.0") {
		t.Errorf("a caller bound at default was shown payments' versions: %s", got)
	}
	if got := enrich("namespace:payments"); !strings.Contains(got, "1.0.0") {
		t.Errorf("a caller who may read payments was not shown its versions: %s", got)
	}
	// Asserted mode lists them as before.
	s.access = nil
	if got := s.enrichVersionRequired(ctx, "", missing).Error(); !strings.Contains(got, "1.0.0") {
		t.Errorf("asserted mode: %s", got)
	}
}

// TestAuthz_ADeniedCallerLearnsNothingFromALookup: a caller refused at the
// namespace gets the refusal whatever the deep lookup found, so a missing
// schedule or delivery reads the same as one that exists (spec review
// MEDIUM-1).
func TestAuthz_ADeniedCallerLearnsNothingFromALookup(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s.access = a
	for procedure, msg := range map[string]proto.Message{
		orkstackv1connect.WebhookServiceGetDeliveryProcedure:      &orkstackv1.GetDeliveryRequest{Id: "dlv-missing"},
		orkstackv1connect.WebhookServiceRedeliverWebhookProcedure: &orkstackv1.RedeliverWebhookRequest{Id: "dlv-missing"},
		orkstackv1connect.TriggerServiceDeleteTriggerProcedure:    &orkstackv1.DeleteTriggerRequest{Id: "missing", Namespace: "payments"},
		orkstackv1connect.ScheduleServiceGetScheduleProcedure:     &orkstackv1.GetScheduleRequest{Name: "missing", Namespace: "payments"},
	} {
		_, err := a.decide(asAlice(authn.ModeEnforce), s, procedure, msg)
		if code := connect.CodeOf(err); code != connect.CodePermissionDenied {
			t.Errorf("%s: a denied caller got %v (%s); want permission_denied, whatever exists", procedure, code, err)
		}
	}
}

// TestPolicyCache_UnexpectedHeads: a head that went missing after this
// replica saw one, and a restore to an older revision, are reported; a
// revision that could not be read now is not taken for one written around
// the API (spec review LOW 2-4).
func TestPolicyCache_UnexpectedHeads(t *testing.T) {
	ctx := context.Background()
	cas := artifacts.NewMemoryCAS()
	store := artifacts.NewAccessStoreOn(cas)
	doc := &orkstackv1.AccessDocument{SchemaVersion: 1, Bindings: []*orkstackv1.Binding{
		{Name: "b", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:a"}}}}
	h1, _ := store.Write(ctx, doc, nil, artifacts.AccessWrite{At: time.Now()})
	h2, _ := store.Write(ctx, doc, h1, artifacts.AccessWrite{At: time.Now()})
	unexpected := 0
	c := NewPolicyCache(store, PolicyCacheOptions{OnUnexpected: func() { unexpected++ }})
	c.Refresh(ctx)

	// Gone.
	body, _ := cas.Raw(artifacts.AccessDocumentKey)
	cas.Delete(artifacts.AccessDocumentKey)
	c.Refresh(ctx)
	if !c.State().WrittenAroundAPI || unexpected != 1 {
		t.Fatalf("a head gone = %+v, %d reports; want it reported", c.State(), unexpected)
	}
	c.Acknowledge(c.State().Revision)
	cas.PutRaw(artifacts.AccessDocumentKey, body)
	c.Refresh(ctx)

	// Restored to revision 1 after revision 2 was acknowledged.
	c.Acknowledge(h2.Meta.Revision)
	old, _ := cas.Raw(artifacts.AccessRevisionKey(1, h1.Meta.SHA256))
	cas.PutRaw(artifacts.AccessDocumentKey, old)
	c.Refresh(ctx)
	if !c.State().WrittenAroundAPI {
		t.Fatal("a restore to an older revision raised no banner")
	}
}

// TestAuthz_ARefusalNamesOnlyWhatTheCallerAsked: a denial after a lookup
// names the scope the request asked about, never what the lookup found
// (final security re-check M2).
func TestAuthz_ARefusalNamesOnlyWhatTheCallerAsked(t *testing.T) {
	s := newAccessService(t)
	trig, _, err := s.triggers.Create(context.Background(), triggers.Registration{Namespace: "payments", OrkflowName: "settle-ledger"})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:search", Subjects: []string{"user:corp:alice"}})
	s.access = a
	_, err = a.decide(asAlice(authn.ModeEnforce), s, orkstackv1connect.TriggerServiceDeleteTriggerProcedure,
		&orkstackv1.DeleteTriggerRequest{Id: trig.ID, Namespace: "payments"})
	if err == nil || strings.Contains(err.Error(), "settle-ledger") {
		t.Fatalf("a refusal = %v; it must not name the trigger's Orkflow", err)
	}
}

// TestAuthz_ARowWhoseChecksAllSkipIsRefused (final security re-check L4).
func TestAuthz_ARowWhoseChecksAllSkipIsRefused(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleAdmin, Scope: "deployment", Subjects: []string{"user:corp:alice"}})
	s.access = a
	const probe = "/orkstack.v1.RunService/ProbeAllSkip"
	accessRows[probe] = accessRow{res: deployment, checks: []check{{perm: authz.PermRunRead, when: func(proto.Message) bool { return false }}}}
	defer delete(accessRows, probe)
	if _, err := a.decide(asAlice(authn.ModeEnforce), s, probe, &orkstackv1.GetRunRequest{}); err == nil {
		t.Fatal("a row that asked nothing admitted the request")
	}
}

// TestAuthz_NoListIsAWayToTheNamespaces: a caller who holds nothing in a
// namespace is refused its row-filtered lists, and a missing namespace's
// refusal names the others only to a caller who may read them all (final
// security re-check M1).
func TestAuthz_NoListIsAWayToTheNamespaces(t *testing.T) {
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Namespaces: stubRegistry{names: map[string]bool{"payments": true}, list: []string{"payments", "search"}}})
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "orkflow:search/x", Subjects: []string{"user:corp:alice"}})
	s.access = a
	ctx := asAlice(authn.ModeEnforce)
	for procedure, msg := range map[string]proto.Message{
		orkstackv1connect.TriggerServiceListTriggersProcedure:   &orkstackv1.ListTriggersRequest{Namespace: "zz-nope"},
		orkstackv1connect.ScheduleServiceListSchedulesProcedure: &orkstackv1.ListSchedulesRequest{Namespace: "payments"},
	} {
		if _, err := a.decide(ctx, s, procedure, msg); err == nil {
			t.Errorf("%s: a caller holding nothing in the namespace was admitted", procedure)
		}
	}
	if _, err := a.decide(ctx, s, orkstackv1connect.TriggerServiceListTriggersProcedure, &orkstackv1.ListTriggersRequest{Namespace: "search"}); err != nil {
		t.Errorf("a caller holding an Orkflow of search: %v", err)
	}
	if got := s.existingNamespaces(ctx); got != "" {
		t.Errorf("a caller without namespace.read at the deployment was shown %q", got)
	}
	s.access = nil
	if got := s.existingNamespaces(context.Background()); !strings.Contains(got, "search") {
		t.Errorf("asserted mode lists them as before: %q", got)
	}
}

// TestResolveComposition_NamesNoOtherNamespacesOrkflows: a composed task
// naming another namespace's Orkflow that is missing, has no such version or
// is not shared reads the same to a caller who may not read it, with no
// version listed (final security re-check M3).
func TestResolveComposition_NamesNoOtherNamespacesOrkflows(t *testing.T) {
	ctx := context.Background()
	orkflows := engine.NewMemoryOrkflowStore()
	if _, _, err := orkflows.Put(ctx, &orkstackv1.Orkflow{Namespace: "victim", Name: "payroll", Version: "3.1.0"}, artifacts.HeadExpectation{}); err != nil {
		t.Fatal(err)
	}
	s := NewService(Config{Orkflows: orkflows, Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Namespaces: stubRegistry{names: map[string]bool{"victim": true}}})
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleDeveloper, Scope: "namespace:default", Subjects: []string{"user:corp:alice"}})
	s.access = a
	actx := withDecision(asAlice(authn.ModeEnforce), &accessDecision{Scope: "namespace:default"})
	sentence := func(name, version string) string {
		wf := &orkstackv1.Orkflow{Namespace: "default", Name: "caller", Tasks: []*orkstackv1.Task{{Id: "t",
			Orkflow: &orkstackv1.OrkflowRef{Namespace: "victim", Name: name, Version: version}}}}
		issues, err := s.resolveComposition(actx, &orkstackv1.Namespace{Name: "default"}, wf)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, i := range issues {
			out = append(out, i.GetMessage())
		}
		return strings.Join(out, "; ")
	}
	missingVersion, missingOrkflow, notShared := sentence("payroll", "0.0.0-x"), sentence("nope", "1.0.0"), sentence("payroll", "3.1.0")
	if strings.Contains(missingVersion, "3.1.0") {
		t.Errorf("another namespace's versions were listed: %s", missingVersion)
	}
	strip := func(s, name string) string { return strings.ReplaceAll(s, name, "N") }
	if a, b, c := strip(strip(missingVersion, "payroll"), "0.0.0-x"), strip(strip(missingOrkflow, "nope"), "1.0.0"), strip(strip(notShared, "payroll"), "3.1.0"); a != b || b != c {
		t.Errorf("the three cases read differently:\n%s\n%s\n%s", missingVersion, missingOrkflow, notShared)
	}
}

// TestListDeliveries_ARunIdTellsNothingOfAnotherNamespace: a run_id filter
// starts the window at the run only when the caller may read that run
// (final security re-check L1).
func TestListDeliveries_ARunIdTellsNothingOfAnotherNamespace(t *testing.T) {
	runs := engine.NewRunStore()
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	runs.Save(&orkstackv1.Run{Id: "ork-other", Namespace: "victim", OrkflowName: "payroll", CreatedAt: timestamppb.New(created)})
	s := NewService(Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: runs, Bus: engine.NewEventBus(),
		Deliveries: artifacts.NewMemoryDeliveryStore()})
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleNamespaceAdmin, Scope: "namespace:payments", Subjects: []string{"user:corp:alice"}})
	s.access = a
	resp, err := s.ListDeliveries(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.ListDeliveriesRequest{Filters: []*orkstackv1.FieldFilter{
		{Field: "namespace", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{"payments"}},
		{Field: "run_id", Op: orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS, Values: []string{"ork-other"}},
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetSinceIsRunStart() {
		t.Fatal("a run of another namespace started the window: the answer said it exists")
	}
}

// TestAuthz_ProvenanceIsDecidedByKind: an artifact's provenance needs the
// permission its bytes need (final security re-check L3).
func TestAuthz_ProvenanceIsDecidedByKind(t *testing.T) {
	s := newAccessService(t)
	a, _ := accessFor(t, authn.ModeEnforce, nil, &orkstackv1.Binding{Name: "b", Role: authz.RoleViewer, Scope: "namespace:default", Subjects: []string{"user:corp:alice"}})
	s.access = a
	ask := func(kind orkstackv1.ArtifactKind) error {
		_, err := a.decide(asAlice(authn.ModeEnforce), s, orkstackv1connect.RunServiceGetArtifactProvenanceProcedure,
			&orkstackv1.GetArtifactProvenanceRequest{RunId: "ork-x", Kind: kind})
		return err
	}
	if ask(orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT) == nil || ask(orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT) == nil {
		t.Error("a viewer read a log's or an artifact's provenance")
	}
	if err := ask(orkstackv1.ArtifactKind_ARTIFACT_KIND_VALUES); err != nil {
		t.Errorf("a viewer's values provenance: %v", err)
	}
}
