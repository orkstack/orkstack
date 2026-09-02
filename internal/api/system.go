package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// SystemInfo is what this replica was built and configured as — the values
// GetSystemInfo hands back.
//
// It is INJECTED, never read here. `ork` fills it once at boot from the
// configuration it already loaded (cmd/ork/main.go), which keeps the handler
// a pure function of its input: an admin view that called os.Getenv would
// describe the environment of whoever last exported a variable into this
// process, and would be untestable without one.
//
// Every field is configuration- or build-derived, and the proto says so field
// by field. The one observed value in the response — the answering replica's
// clock — is not in here, because it is not configuration: it comes from the
// Service's own now().
type SystemInfo struct {
	// EngineVersion is the release this binary was built as. cmd/ork resolves
	// it from -ldflags, then the module's build info, then "dev".
	EngineVersion string
	// GoVersion is the toolchain the binary was compiled with.
	GoVersion string

	// DeploymentName and BuildID are the Worker Deployment identity both
	// binaries of a release join (ORK_DEPLOYMENT_NAME, ORK_BUILD_ID). An
	// empty BuildID is the documented off switch for versioning, not a
	// missing value.
	DeploymentName string
	BuildID        string

	// ArtifactBackend is "local" or "s3"; ArtifactLocation is the directory
	// or bucket it addresses.
	//
	// A credential never enters this struct. The admin surface is a read for
	// operators, and the access key is neither useful to them nor safe in a
	// UI, a CLI transcript or a proof artifact.
	ArtifactBackend  string
	ArtifactLocation string

	// TemporalAddress is the cluster endpoint this replica dials. That it is
	// configured is not a claim that it answers: pool health is the read that
	// proves the cluster is reachable.
	TemporalAddress string
}

// VersioningActive reports whether Worker Deployment Versioning is on for this
// process.
//
// It is the same rule as task.Deployment.Versioned — both halves required —
// restated over the injected copy rather than imported, because this struct is
// a snapshot of configuration and not the live deployment object. Keeping the
// rule in one shape matters: an admin view that said "versioned" for a
// deployment whose runs are unversioned would be worse than no view.
func (s SystemInfo) VersioningActive() bool {
	return s.DeploymentName != "" && s.BuildID != ""
}

// GetSystemInfo describes the replica that answered.
//
// This is the one admin read that does not leave the process, and AGENTS.md
// §1.7 is satisfied rather than bent: the rule forbids answering a read about
// RUN, ORKFLOW or NAMESPACE state from process memory, because any replica
// must answer for any of them. This RPC reads none of those. It answers about
// the process itself, which is the one question only the process can answer —
// and replicas of one release answer identically by construction, since they
// carry the same build and the same configuration. Mid-rollout they differ in
// build_id, which is the field an operator is looking for.
func (s *Service) GetSystemInfo(
	_ context.Context,
	_ *connect.Request[orkstackv1.GetSystemInfoRequest],
) (*connect.Response[orkstackv1.GetSystemInfoResponse], error) {
	return connect.NewResponse(&orkstackv1.GetSystemInfoResponse{
		EngineVersion:    s.system.EngineVersion,
		GoVersion:        s.system.GoVersion,
		DeploymentName:   s.system.DeploymentName,
		BuildId:          s.system.BuildID,
		VersioningActive: s.system.VersioningActive(),
		ArtifactBackend:  s.system.ArtifactBackend,
		ArtifactLocation: s.system.ArtifactLocation,
		TemporalAddress:  s.system.TemporalAddress,
		ServerTime:       timestamppb.New(s.now().UTC()),
	}), nil
}

// retentionBinding resolves the deployment's sole router binding and the
// retention seam on it — internal/engine/router.go's SingleRouter, the SAME
// binding GetRun, ListRuns and CancelRun read through, so there is no second,
// parallel path to a Temporal client here.
//
// It deliberately does NOT go through s.binding(namespace), which asks
// router.For(namespace) whether the binding serves a given OrkStack
// namespace (defaulting "" to "default" — see SingleRouter.For). Execution
// retention has no OrkStack-namespace scope at all: a deployment whose one
// binding is scoped to `Namespaces: []string{"payments"}` (Binding.serves)
// still has exactly one Temporal namespace and exactly one retention
// setting, and asking "does this binding serve the OrkStack namespace
// `default`?" would wrongly refuse it. So this reads router.Bindings()
// directly and takes the one binding a single-cluster deployment has —
// the same binding s.binding("") would have resolved to, minus the
// accidental OrkStack-namespace gate.
func (s *Service) retentionBinding() (engine.RetentionAdmin, error) {
	bindings := s.router.Bindings()
	if len(bindings) != 1 {
		return nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("execution retention requires exactly one router binding, this engine has %d", len(bindings)))
	}
	binding := bindings[0]
	if binding.Retention == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("this engine has no Temporal retention admin configured"))
	}
	return binding.Retention, nil
}

// GetExecutionRetention reads Temporal's CURRENT workflow-execution retention
// for this deployment's one Temporal namespace, live from the cluster
// (DescribeNamespace) — UNLIKE GetSystemInfo above, whose fields are all
// configuration echoed back rather than observed. That guarantee is
// unaffected by this RPC: it still holds for every field of
// GetSystemInfoResponse, which gains none here.
//
// It is deployment-wide, never per-OrkStack-Namespace: see retentionBinding.
func (s *Service) GetExecutionRetention(
	ctx context.Context,
	_ *connect.Request[orkstackv1.GetExecutionRetentionRequest],
) (*connect.Response[orkstackv1.GetExecutionRetentionResponse], error) {
	retention, err := s.retentionBinding()
	if err != nil {
		return nil, err
	}
	current, err := retention.NamespaceRetention(ctx, s.temporalNamespace)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read temporal namespace %q retention: %w", s.temporalNamespace, err))
	}
	return connect.NewResponse(&orkstackv1.GetExecutionRetentionResponse{
		TemporalNamespace: s.temporalNamespace,
		Retention:         durationpb.New(current),
	}), nil
}

// UpdateExecutionRetention CHANGES that same cluster-wide setting through
// Temporal's real UpdateNamespace RPC — UNLIKE GetSystemInfo, a live write
// against the cluster, and again no claim about GetSystemInfoResponse's own
// fields is affected by this RPC existing.
//
// A non-positive duration is rejected with InvalidArgument before any call
// leaves this process. A Temporal refusal of a positive one — including its
// own server-side minimum-retention rejection — is returned to the caller
// untouched: never swallowed into a default, a zero value, or a fabricated
// success.
func (s *Service) UpdateExecutionRetention(
	ctx context.Context,
	req *connect.Request[orkstackv1.UpdateExecutionRetentionRequest],
) (*connect.Response[orkstackv1.UpdateExecutionRetentionResponse], error) {
	requested := req.Msg.GetRetention()
	if requested == nil || requested.AsDuration() <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("retention must be a positive duration"))
	}
	retention, err := s.retentionBinding()
	if err != nil {
		return nil, err
	}
	confirmed, err := retention.SetNamespaceRetention(ctx, s.temporalNamespace, requested.AsDuration())
	if err != nil {
		return nil, connect.NewError(temporalWriteErrorCode(err),
			fmt.Errorf("update temporal namespace %q retention: %w", s.temporalNamespace, err))
	}
	return connect.NewResponse(&orkstackv1.UpdateExecutionRetentionResponse{
		TemporalNamespace: s.temporalNamespace,
		Retention:         durationpb.New(confirmed),
	}), nil
}

// temporalWriteErrorCode classifies a Temporal write failure the way the
// caller can act on it. Temporal's own validation refusals — the
// server-side minimum-retention rejection is exactly this shape — surface as
// *serviceerror.InvalidArgument, so those come back as InvalidArgument rather
// than the Unavailable every other cluster-reachability failure here uses;
// the message itself is always Temporal's own, untouched either way.
func temporalWriteErrorCode(err error) connect.Code {
	var invalid *serviceerror.InvalidArgument
	if errors.As(err, &invalid) {
		return connect.CodeInvalidArgument
	}
	return connect.CodeUnavailable
}
