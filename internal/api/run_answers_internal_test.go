package api

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/orkstack/orkstack/internal/authz"
)

// RunReaders answer a caller who may read each run they carry, so with its
// trace context; RunsWithoutContext answer a caller who need not.
// TestTraceparent_IsOnlyInAnAnswerToAReader drives every one of both. Exported
// for that test, in the package's external tests.
var (
	RunReaders         = []string{"RunService/DiffRuns", "RunService/GetRun", "RunService/ListRuns"}
	RunsWithoutContext = []string{"RunService/CancelRun", "RunService/SubmitRun", "StatsService/GetStats", "SystemService/ListReleases"}
)

// TestRunAnswers_EveryOneIsClassified: every RPC whose answer carries a Run is
// named above, and one that keeps the run's trace context asks for run.read.
func TestRunAnswers_EveryOneIsClassified(t *testing.T) {
	var carries func(d protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool
	carries = func(d protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
		if d.FullName() == "orkstack.v1.Run" {
			return true
		}
		if seen[d.FullName()] {
			return false
		}
		seen[d.FullName()] = true
		for i := range d.Fields().Len() {
			if m := d.Fields().Get(i).Message(); m != nil && carries(m, seen) {
				return true
			}
		}
		return false
	}
	var got []string
	protoregistry.GlobalFiles.RangeFilesByPackage("orkstack.v1", func(f protoreflect.FileDescriptor) bool {
		for i := range f.Services().Len() {
			service := f.Services().Get(i)
			for j := range service.Methods().Len() {
				if m := service.Methods().Get(j); carries(m.Output(), map[protoreflect.FullName]bool{}) {
					got = append(got, string(service.Name())+"/"+string(m.Name()))
				}
			}
		}
		return true
	})
	sort.Strings(got)
	want := slices.Sorted(slices.Values(append(slices.Clone(RunReaders), RunsWithoutContext...)))
	if !slices.Equal(got, want) {
		t.Fatalf("RPCs whose answer carries a Run:\n%s\nclassified:\n%s\n"+
			"classify a new one, and strip its trace context (withoutTraceparent) unless it asks for run.read",
			strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, name := range RunReaders {
		row := accessRows["/orkstack.v1."+name]
		if !slices.ContainsFunc(row.checks, func(c check) bool { return c.perm == authz.PermRunRead && c.when == nil }) {
			t.Errorf("%s keeps a run's trace context and does not ask for run.read", name)
		}
	}
}
