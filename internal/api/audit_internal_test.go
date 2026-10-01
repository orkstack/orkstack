package api

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// auditReadRPCs is every unary RPC that only reads. Each entry begins with one
// of the read verbs below, so a write cannot hide here under a write's name.
var auditReadRPCs = map[string]bool{
	orkstackv1connect.OrkflowServiceValidateOrkflowProcedure:     true,
	orkstackv1connect.OrkflowServiceGetOrkflowProcedure:          true,
	orkstackv1connect.OrkflowServiceListOrkflowsProcedure:        true,
	orkstackv1connect.OrkflowServiceListOrkflowVersionsProcedure: true,
	orkstackv1connect.OrkflowServiceDiffOrkflowVersionsProcedure: true,
	orkstackv1connect.OrkflowServicePlanRunProcedure:             true,
	orkstackv1connect.RunServiceGetRunProcedure:                  true,
	orkstackv1connect.RunServiceListRunsProcedure:                true,
	orkstackv1connect.RunServiceListRunArtifactsProcedure:        true,
	orkstackv1connect.RunServiceListTaskAttemptsProcedure:        true,
	orkstackv1connect.RunServiceGetArtifactProvenanceProcedure:   true,
	orkstackv1connect.RunServiceInspectRunProcedure:              true,
	orkstackv1connect.RunServiceDiffRunsProcedure:                true,
	orkstackv1connect.WebhookServiceListWebhooksProcedure:        true,
	orkstackv1connect.WebhookServiceListDeliveriesProcedure:      true,
	orkstackv1connect.WebhookServiceGetDeliveryProcedure:         true,
	orkstackv1connect.TriggerServiceListTriggersProcedure:        true,
	orkstackv1connect.ScheduleServiceListSchedulesProcedure:      true,
	orkstackv1connect.ScheduleServiceGetScheduleProcedure:        true,
	orkstackv1connect.NamespaceServiceGetNamespaceProcedure:      true,
	orkstackv1connect.NamespaceServiceListNamespacesProcedure:    true,
	orkstackv1connect.NamespaceServiceListPoolsProcedure:         true,
	orkstackv1connect.ActivityServiceListActivitiesProcedure:     true,
	orkstackv1connect.ActivityServiceDescribeOrkerProcedure:      true,
	orkstackv1connect.ActivityServiceListOrkersProcedure:         true,
	// InspectActivity resolves one reference against the registry and writes
	// nothing (activity-interface-manifests §5.4).
	orkstackv1connect.ActivityServiceInspectActivityProcedure:  true,
	orkstackv1connect.SystemServiceGetSystemInfoProcedure:      true,
	orkstackv1connect.SystemServiceGetRetentionPolicyProcedure: true,
	orkstackv1connect.SystemServiceListTelemetrySinksProcedure: true,
	orkstackv1connect.StatsServiceGetStatsProcedure:            true,
	orkstackv1connect.StatsServiceGetFailureBreakdownProcedure: true,
	orkstackv1connect.StatsServiceGetFlakeIndexProcedure:       true,
	orkstackv1connect.AuditServiceListAuditRecordsProcedure:    true,
}

// auditStreamingReads is every streaming RPC. The audit interceptor is unary
// only, so a streaming WRITE would go unrecorded; this list is what makes
// adding one a failing test rather than a silent gap.
var auditStreamingReads = map[string]bool{
	orkstackv1connect.RunServiceStreamEventsProcedure:   true,
	orkstackv1connect.RunServiceStreamTaskLogsProcedure: true,
	orkstackv1connect.RunServiceReadArtifactProcedure:   true,
	// ExportRunEvidence assembles a bundle from objects that already exist
	// and writes nothing (run-evidence-bundle §8).
	orkstackv1connect.RunServiceExportRunEvidenceProcedure: true,
}

// Plan is a read verb: PlanRun simulates a definition and writes nothing
// (docs/design/proposals/phase-3/run-plan, "Reads only"). Inspect is the verb
// of InspectRun (a live describe) and InspectActivity (an on-demand registry
// read, activity-interface-manifests §5.4); both write nothing.
var auditReadVerbs = []string{"Get", "List", "Validate", "Diff", "Describe", "Stream", "Plan", "Inspect"}

// orkstackProcedures walks every service in the generated orkstack.v1 file
// registry and returns each method's procedure and whether it streams.
func orkstackProcedures(t *testing.T) map[string]bool {
	t.Helper()
	_ = orkstackv1.File_orkstack_v1_audit_proto // ensure the package is registered
	procedures := map[string]bool{}
	protoregistry.GlobalFiles.RangeFilesByPackage("orkstack.v1", func(file protoreflect.FileDescriptor) bool {
		services := file.Services()
		for i := range services.Len() {
			service := services.Get(i)
			methods := service.Methods()
			for j := range methods.Len() {
				method := methods.Get(j)
				procedure := "/" + string(service.FullName()) + "/" + string(method.Name())
				procedures[procedure] = method.IsStreamingClient() || method.IsStreamingServer()
			}
		}
		return true
	})
	if len(procedures) == 0 {
		t.Fatal("no orkstack.v1 services are registered")
	}
	return procedures
}

func TestAudit_EveryUnaryRPCIsClassified(t *testing.T) {
	procedures := orkstackProcedures(t)
	names := make([]string, 0, len(procedures))
	for procedure := range procedures {
		names = append(names, procedure)
	}
	sort.Strings(names)
	for _, procedure := range names {
		streaming := procedures[procedure]
		_, audited := auditedProcedures[procedure]
		_, excluded := unauditedWrites[procedure]
		read := auditReadRPCs[procedure]
		if streaming {
			if !auditStreamingReads[procedure] || audited || excluded {
				t.Errorf("%s streams: the audit interceptor is unary only, so a streaming RPC must be a read "+
					"listed in auditStreamingReads — a streaming write would go unrecorded", procedure)
			}
			continue
		}
		classes := 0
		for _, in := range []bool{audited, excluded, read} {
			if in {
				classes++
			}
		}
		switch classes {
		case 0:
			t.Errorf("%s is not classified: add it to auditedProcedures (internal/api/audit.go), "+
				"to unauditedWrites with a reason, or — if it only reads — to auditReadRPCs", procedure)
		case 1:
		default:
			t.Errorf("%s is classified more than once", procedure)
		}
	}
	for procedure := range auditReadRPCs {
		method := procedure[strings.LastIndex(procedure, "/")+1:]
		verb := false
		for _, prefix := range auditReadVerbs {
			verb = verb || strings.HasPrefix(method, prefix)
		}
		if !verb {
			t.Errorf("%s is listed as a read but its name begins with none of %v", procedure, auditReadVerbs)
		}
		if _, ok := procedures[procedure]; !ok {
			t.Errorf("%s is listed as a read but no such RPC exists", procedure)
		}
	}
	for procedure := range auditStreamingReads {
		if _, ok := procedures[procedure]; !ok {
			t.Errorf("%s is listed as a streaming read but no such RPC exists", procedure)
		}
	}
	for procedure := range auditedProcedures {
		if streaming, ok := procedures[procedure]; !ok || streaming {
			t.Errorf("%s is audited but is not a unary RPC of the API", procedure)
		}
	}
	for procedure := range unauditedWrites {
		if _, ok := procedures[procedure]; !ok {
			t.Errorf("%s is excluded but no such RPC exists", procedure)
		}
	}
}

func TestAuditInterceptor_DetachedFromRequestCancellation(t *testing.T) {
	store := artifacts.NewMemoryAuditStore()
	now := time.Date(2026, 3, 14, 11, 2, 44, 0, time.UTC)
	svc := NewService(Config{AuditStore: store, Now: func() time.Time { return now }})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := connect.NewRequest(&orkstackv1.DeleteNamespaceRequest{Name: "gone"})
	next := func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		// The caller hangs up the instant the handler returns.
		cancel()
		return connect.NewResponse(&orkstackv1.DeleteNamespaceResponse{}), nil
	}
	if _, err := svc.auditUnary(ctx, orkstackv1connect.NamespaceServiceDeleteNamespaceProcedure, req, next); err != nil {
		t.Fatalf("auditUnary: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("request context was not cancelled")
	}
	page, err := store.ListAuditRecords(context.Background(), artifacts.AuditQuery{
		Since: now.Add(-time.Hour), Until: now.Add(time.Hour), Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListAuditRecords: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].GetAction() != "namespace.delete" || page.Records[0].GetTarget() != "gone" {
		t.Fatalf("records = %v, want the delete recorded despite the cancelled request", page.Records)
	}
}
