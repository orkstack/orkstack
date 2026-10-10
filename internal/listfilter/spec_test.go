package listfilter_test

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

var update = flag.Bool("update", false, "rewrite testdata goldens")

func findMessage(name string) (protoreflect.MessageDescriptor, error) {
	_ = orkstackv1.File_orkstack_v1_service_proto
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, err
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a message", name)
	}
	return md, nil
}

// listMethods is every service method of orkstack.v1 whose name starts
// with List, by method name.
func listMethods(t *testing.T) map[string]protoreflect.MethodDescriptor {
	t.Helper()
	_ = orkstackv1.File_orkstack_v1_service_proto
	out := map[string]protoreflect.MethodDescriptor{}
	protoregistry.GlobalFiles.RangeFilesByPackage("orkstack.v1", func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			methods := fd.Services().Get(i).Methods()
			for j := 0; j < methods.Len(); j++ {
				if m := methods.Get(j); strings.HasPrefix(string(m.Name()), "List") {
					out[string(m.Name())] = m
				}
			}
		}
		return true
	})
	return out
}

// exempt are the list RPCs that take no filters (docs/UI_CLI_PARITY.md
// "Filtering lists"): child listings of one parent, a computed report, and
// a handful of names from one setting.
var exempt = map[string]string{
	"ListOrkflowVersions": "a child listing of one Orkflow",
	"ListOrkflowCallers":  "a computed report about one Orkflow",
	"ListRunArtifacts":    "a child listing of one run",
	"ListTaskAttempts":    "a child listing of one run",
	"ListTelemetrySinks":  "a handful of names from one setting",
}

// notYetFiltered is TEMPORARY: the lists a later stage of the structured
// filters converts. The test fails if one of them already declares filters,
// so the table cannot outlive its stage; stage 3 deletes it.
var notYetFiltered = map[string]string{
	"ListRuns": "stage 3",
}

func TestSpec_EveryListRequestDeclaresItsFilters(t *testing.T) {
	methods := listMethods(t)
	if len(methods) < 17 {
		t.Fatalf("found %d List RPCs; want at least 17", len(methods))
	}
	for name, m := range methods {
		input := m.Input()
		declared := listfilter.Declared(input)
		switch {
		case exempt[name] != "":
			if declared {
				t.Errorf("%s is exempt (%s) but declares filters", name, exempt[name])
			}
		case notYetFiltered[name] != "":
			if declared {
				t.Errorf("%s declares filters; delete it from notYetFiltered (%s)", name, notYetFiltered[name])
			}
		default:
			if !declared {
				t.Errorf("%s (%s) declares no (orkstack.v1.filterable) option", name, input.FullName())
				continue
			}
			if _, err := listfilter.SpecForDescriptor(input); err != nil {
				t.Errorf("%s: %v", name, err)
			}
		}
	}
}

// nameValued is every (resource, path) that holds an OrkStack name and so
// must carry the NAME value rule — and nothing else may.
var nameValued = []string{
	"Namespace.name", "Namespace.default_pool", "Orkflow.name",
	"Webhook.name", "Webhook.namespace", "Webhook.orkflow_name", "Webhook.task_id",
	"Trigger.name", "Trigger.orkflow_name", "Schedule.name", "Schedule.orkflow_name",
	"Pool.name", "Orker.pool",
	"Delivery.namespace", "AuditRecord.namespace",
}

// opaqueIDs are matched whole, never by part.
var opaqueIDs = []string{
	"Webhook.id", "Trigger.id", "Schedule.id", "Orkflow.definition_digest", "ActivityInfo.index_digest",
	"Delivery.run_id", "Delivery.webhook_id", "Run.origin.id",
}

func declaredSpecs(t *testing.T) map[string]*listfilter.Spec {
	t.Helper()
	out := map[string]*listfilter.Spec{}
	for name, m := range listMethods(t) {
		if !listfilter.Declared(m.Input()) {
			continue
		}
		spec, err := listfilter.SpecForDescriptor(m.Input())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out[name] = spec
	}
	return out
}

func TestSpec_EveryDeclarationResolves(t *testing.T) {
	var names, orkerIDs []string
	for name, spec := range declaredSpecs(t) {
		for _, f := range spec.Fields {
			id := string(spec.Resource.Name()) + "." + f.Path
			switch f.ValueRule {
			case orkstackv1.FilterValueRule_FILTER_VALUE_RULE_NAME:
				names = append(names, id)
			case orkstackv1.FilterValueRule_FILTER_VALUE_RULE_ORKER_ID:
				orkerIDs = append(orkerIDs, id)
			}
			if slices.Contains(opaqueIDs, id) {
				for _, op := range f.Operators {
					if op == orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS || op == orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX {
						t.Errorf("%s: %s is an opaque id and takes %s; an id is matched whole", name, id, listfilter.OperatorLabel(op))
					}
				}
			}
			if f.KeptWhenUnavailable && spec.Unavailable == nil {
				t.Errorf("%s: %s is kept when unavailable but the list declares no unavailable_field", name, id)
			}
		}
	}
	sort.Strings(names)
	want := slices.Sorted(slices.Values(nameValued))
	if !slices.Equal(names, want) {
		t.Errorf("fields declaring the NAME rule:\n got %q\nwant %q", names, want)
	}
	if !slices.Equal(orkerIDs, []string{"Orker.orker_id"}) {
		t.Errorf("fields declaring the ORKER_ID rule = %q; want only Orker.orker_id", orkerIDs)
	}
}

func TestSpec_AllowlistsGolden(t *testing.T) {
	specs := declaredSpecs(t)
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "== %s (%s)\n%s\n", name, specs[name].Resource.FullName(), specs[name].Help())
	}
	const path = "testdata/allowlists.golden"
	if *update {
		if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if b.String() != string(want) {
		t.Errorf("allowlists changed; review the diff and rerun with -update:\n%s", b.String())
	}
}

func TestSpec_RefusesARequestWithoutTheOption(t *testing.T) {
	if _, err := listfilter.SpecFor(&orkstackv1.ListTelemetrySinksRequest{}); err == nil {
		t.Fatal("SpecFor(ListTelemetrySinksRequest) succeeded; it declares no filters")
	}
}

func TestSpec_HelpNamesEveryFieldAndItsRule(t *testing.T) {
	help := specOf(t, &orkstackv1.ListOrkersRequest{}).Help()
	for _, want := range []string{
		"Filters (--filter FIELD OP VALUE, repeatable; every filter must hold):",
		"orker_id", "an Orker id",
		"pool", "a name (a-z, 0-9, -; at most 200 bytes) or empty; an unreachable row is kept",
		"cordoned", "true false; an unreachable row is kept",
		"= != =a,b ~ ^",
	} {
		if !strings.Contains(help, want) {
			t.Errorf("help lacks %q:\n%s", want, help)
		}
	}
	runs := runsSpec(t).Help()
	for _, want := range []string{"origin.id", "text; needs origin.kind=", "PENDING RUNNING SUCCEEDED FAILED CANCELED SKIPPED",
		"RFC 3339 instant with a zone (2026-10-09T00:00:00Z)"} {
		if !strings.Contains(runs, want) {
			t.Errorf("runs help lacks %q:\n%s", want, runs)
		}
	}
	if ns := namespacesSpec(t).Help(); !strings.Contains(ns, "labels.KEY") {
		t.Errorf("namespaces help lacks labels.KEY:\n%s", ns)
	}
}
