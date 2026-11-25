package api

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// sharedBuild is a child run in payments of platform's shared Orkflow
// "build", while payments holds an Orkflow "build" of its own at the same
// version.
func sharedBuild(t *testing.T, s *Service) *orkstackv1.Run {
	t.Helper()
	store := func(namespace, image string) string {
		wf := &orkstackv1.Orkflow{Namespace: namespace, Name: "build", Version: "1.0.0", SourceYaml: "of " + namespace,
			Tasks: []*orkstackv1.Task{{Id: "compile", Uses: image,
				Image: &orkstackv1.ImageRef{Reference: image, Pinned: image + "@sha256:" + namespace}}}}
		digest, err := dsl.DefinitionDigest(wf)
		must.OK(t, err)
		wf.DefinitionDigest = digest
		_, _, err = s.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation())
		must.OK(t, err)
		return digest
	}
	store("payments", "ghcr.io/payments/build")
	return &orkstackv1.Run{Id: "ork-child", Namespace: "payments", OrkflowName: "build", OrkflowVersion: "1.0.0",
		OrkflowNamespace: "platform", OrkflowDefinitionDigest: store("platform", "ghcr.io/platform/build")}
}

// The permission to export a run's evidence, or to read the run, is not the
// permission to read another namespace's Orkflow: a caller without
// orkflow.read there gets no byte of it, and is told why.
func TestSharedDefinition_IsWithheldFromACallerWhoMayNotReadTheOwnersOrkflow(t *testing.T) {
	const withheld = "definition_not_readable"
	bind := func(namespace string) *orkstackv1.Binding {
		return &orkstackv1.Binding{Name: "in-" + namespace, Role: authz.RoleViewer, Scope: "namespace:" + namespace,
			Subjects: []string{alice.ID}}
	}
	for _, tc := range []struct {
		name     string
		mode     string
		bindings []*orkstackv1.Binding
		readable bool
	}{
		{"bound in the running namespace only", authn.ModeEnforce, []*orkstackv1.Binding{bind("payments")}, false},
		{"bound in the owner's too", authn.ModeEnforce, []*orkstackv1.Binding{bind("payments"), bind("platform")}, true},
		{"asserted decides nothing", authn.ModeAsserted, nil, true},
		{"observe changes no answer", authn.ModeObserve, []*orkstackv1.Binding{bind("payments")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newAccessService(t)
			s.access, _ = accessFor(t, tc.mode, nil, tc.bindings...)
			run := sharedBuild(t, s)
			ctx := asAlice(tc.mode)

			var m evidence.Manifest
			e := &evidenceExport{s: s, small: map[string][]byte{}}
			must.OK(t, e.definition(ctx, &m, run))
			definition, err := s.pinnedDefinition(ctx, run)
			must.OK(t, err)
			if len(m.Members) != 1 {
				t.Fatalf("members = %+v; the definition is always listed", m.Members)
			}
			if tc.readable {
				if m.Definition.State != evidence.StatePresent || string(e.small["orkflow/source.yaml"]) != "of platform" ||
					definition.images["compile"] != "ghcr.io/platform/build@sha256:platform" {
					t.Fatalf("definition = %+v, source = %q, diff = %+v; want the owner's document", m.Definition, e.small["orkflow/source.yaml"], definition)
				}
				return
			}
			if len(e.small) != 0 {
				t.Fatalf("bytes of a withheld definition are held: %q", e.small)
			}
			if got := m.Members[0]; got.State != evidence.StateWithheld || got.Reason != withheld || got.SHA256 != "" || got.SizeBytes != 0 ||
				m.Definition.State != evidence.StateWithheld || m.Definition.Reason != withheld {
				t.Fatalf("member = %+v, definition = %+v; want it withheld", got, m.Definition)
			}
			if definition.reason != withheld || len(definition.images) != 0 {
				t.Fatalf("diff = %+v; want %s and no image", definition, withheld)
			}
		})
	}
}

// An outage of what decides is not a refusal: a bundle that said withheld
// would say it for good.
func TestSharedDefinition_IsNeverWithheldForAnOutage(t *testing.T) {
	s := newAccessService(t)
	s.access = &AccessControl{Mode: authn.ModeEnforce, Now: time.Now}
	run := sharedBuild(t, s)
	ctx := asAlice(authn.ModeEnforce)
	var m evidence.Manifest
	if err := (&evidenceExport{s: s, small: map[string][]byte{}}).definition(ctx, &m, run); connect.CodeOf(err) != connect.CodeUnavailable || len(m.Members) != 0 {
		t.Fatalf("definition = %v, members %+v; want unavailable", err, m.Members)
	}
	if got, err := s.pinnedDefinition(ctx, run); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("pinnedDefinition = %+v, %v; want unavailable", got, err)
	}
}
