//go:build integration

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_AnIDBelongsToOneIncarnation: on a real engine and store, a
// namespace and an Orkflow each get an id that holds while they do (an
// update of the namespace, a second version of the Orkflow), and a delete
// and a create under the same name give each a new one.
func TestIntegration_AnIDBelongsToOneIncarnation(t *testing.T) {
	s := startScopeStack(t, nil, nil)
	fmt.Fprintf(&s.log, "# ids of namespaces and Orkflows (%s)\nengine %s\n\n", time.Now().UTC().Format(time.RFC3339), s.engine.addr)
	ns := "ids-" + fmt.Sprint(time.Now().UnixNano()%1_000_000_000)

	namespaceID := func(step string) string {
		t.Helper()
		out := s.cli("namespace", "get", ns, "--json")
		resp := &orkstackv1.GetNamespaceResponse{}
		must.OKf(t, api.UnmarshalJSON([]byte(out), resp), "namespace get --json: %s", out)
		id := resp.GetNamespace().GetIdentity().GetNamespaceId()
		if !strings.HasPrefix(id, artifacts.NamespaceIDPrefix) {
			t.Fatalf("after %s namespace get answered id %q", step, id)
		}
		fmt.Fprintf(&s.log, "after %s: namespace %s is %s\n", step, ns, id)
		return id
	}
	orkflowID := func(step string) string {
		t.Helper()
		out := s.cli("orkflow", "get", "report", "-n", ns, "--json")
		resp := &orkstackv1.GetOrkflowResponse{}
		must.OKf(t, api.UnmarshalJSON([]byte(out), resp), "orkflow get --json: %s", out)
		id := resp.GetOrkflowId()
		if !strings.HasPrefix(id, artifacts.OrkflowIDPrefix) {
			t.Fatalf("after %s orkflow get answered id %q", step, id)
		}
		fmt.Fprintf(&s.log, "after %s: orkflow %s/report is %s\n", step, ns, id)
		return id
	}
	report := func(version string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "report.ork.yaml")
		must.OK(t, os.WriteFile(path, []byte(fmt.Sprintf(`apiVersion: orkstack/v1
kind: Orkflow
name: report
version: "%s"
tasks:
  - id: echo
    uses: shell
    with:
      op: exec
      command:
        - "true"
`, version)), 0o644))
		s.cli("orkflow", "create", path, "-n", ns)
	}

	s.cli("namespace", "create", ns)
	first := namespaceID("the create")
	s.cli("namespace", "update", ns, "--description", "rewritten")
	if again := namespaceID("an update"); again != first {
		t.Fatalf("an update of the namespace changed its id from %s to %s", first, again)
	}

	report("1")
	of := orkflowID("the first version")
	report("2")
	if again := orkflowID("a second version"); again != of {
		t.Fatalf("a second version changed the Orkflow's id from %s to %s", of, again)
	}
	s.cli("orkflow", "delete", "report", "-n", ns, "--yes")
	report("1")
	if remade := orkflowID("a delete and a create"); remade == of {
		t.Fatalf("the Orkflow made again under its name kept the id %s", of)
	}
	s.cli("orkflow", "delete", "report", "-n", ns, "--yes")

	s.cli("namespace", "delete", ns, "--yes")
	s.cli("namespace", "create", ns)
	if remade := namespaceID("a delete and a create"); remade == first {
		t.Fatalf("the namespace made again under its name kept the id %s", first)
	}
	if out := s.cli("namespace", "get", "default", "--json"); !strings.Contains(out, artifacts.DefaultNamespaceID) {
		t.Fatalf("default's id is not %s: %s", artifacts.DefaultNamespaceID, out)
	}
	s.cli("namespace", "delete", ns, "--yes")
	t.Logf("EVIDENCE ids: a namespace and an Orkflow kept their ids across an update and a second version, and each got a new one when made again under its name")
	proof.Write(t, "orkctl", "ids-one-incarnation", []byte(s.log.String()))
}
