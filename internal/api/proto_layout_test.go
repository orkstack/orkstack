package api_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestProtoLayout_EveryServiceHasItsOwnFile holds the proto contract to one
// service per file (AGENTS.md §12). A generated client module is one file's
// worth of schemas, so a file that declares two services makes every caller
// of either load both — the UI's routes once loaded all seven of
// service.proto's services to call one (F-ui-perf-3).
//
// A file that declares a service is named for it: <name>_service.proto, or
// the domain file that already holds it (stats.proto declares StatsService).
func TestProtoLayout_EveryServiceHasItsOwnFile(t *testing.T) {
	_ = orkstackv1.File_orkstack_v1_run_service_proto // load the package's descriptors
	services := 0
	protoregistry.GlobalFiles.RangeFilesByPackage("orkstack.v1", func(fd protoreflect.FileDescriptor) bool {
		if fd.Services().Len() > 1 {
			names := make([]string, 0, fd.Services().Len())
			for i := range fd.Services().Len() {
				names = append(names, string(fd.Services().Get(i).Name()))
			}
			t.Errorf("%s declares %d services (%s); give each its own file", fd.Path(), len(names), strings.Join(names, ", "))
		}
		if fd.Services().Len() == 1 {
			services++
			service := string(fd.Services().Get(0).Name())
			stem := strings.ToLower(strings.TrimSuffix(service, "Service"))
			base := strings.TrimSuffix(fd.Path()[strings.LastIndex(fd.Path(), "/")+1:], ".proto")
			if base != stem+"_service" && base != stem {
				t.Errorf("%s declares %s; name it %s_service.proto", fd.Path(), service, stem)
			}
		}
		return true
	})
	if services < 12 {
		t.Fatalf("found %d files declaring a service; want at least the 12 orkstack.v1 services, one per file", services)
	}
}
