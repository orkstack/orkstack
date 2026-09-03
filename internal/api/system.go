package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/pkg/artifacts"
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

// retentionController resolves the deployment's retention controller — the
// same one every namespace-retention validation (internal/api/namespaces.go)
// and the cleanup worker (cmd/ork/main.go) share, so there is no second,
// parallel path to the policy store or the cluster here.
func (s *Service) retentionController() (*archive.RetentionController, error) {
	if s.retention == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("this engine has no retention policy configured"))
	}
	return s.retention, nil
}

// GetRetentionPolicy reads the deployment's maximum run retention: the
// ceiling on every namespace's own run_retention, and what a namespace that
// sets none inherits. It is read from the store, so any replica answers the
// same thing — unlike GetSystemInfo's fields, which describe the replica
// that answered, this describes the deployment.
func (s *Service) GetRetentionPolicy(
	ctx context.Context,
	_ *connect.Request[orkstackv1.GetRetentionPolicyRequest],
) (*connect.Response[orkstackv1.GetRetentionPolicyResponse], error) {
	ctrl, err := s.retentionController()
	if err != nil {
		return nil, err
	}
	policy, err := ctrl.Policy(ctx)
	if err != nil {
		if errors.Is(err, artifacts.ErrNotFound) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("no retention policy is stored; the engine writes one at boot"))
		}
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&orkstackv1.GetRetentionPolicyResponse{
		Policy:                     policy,
		NamespaceRetentionEnforced: s.retentionEnforced,
	}), nil
}

// UpdateRetentionPolicy changes the deployment's maximum run retention.
//
// A non-positive duration, a malformed protobuf Duration (CheckValid), or a
// magnitude that cannot round-trip losslessly through Go's time.Duration is
// rejected with InvalidArgument before any call leaves this process — see
// validateRetentionDuration. Lowering the maximum below a namespace's own
// explicit run_retention is refused with FailedPrecondition, naming that
// namespace: the namespace must be lowered first. Namespaces that set no
// run_retention of their own follow the new maximum immediately.
func (s *Service) UpdateRetentionPolicy(
	ctx context.Context,
	req *connect.Request[orkstackv1.UpdateRetentionPolicyRequest],
) (*connect.Response[orkstackv1.UpdateRetentionPolicyResponse], error) {
	requested, err := validateRetentionDuration("maximum run retention", req.Msg.GetMaxRunRetention())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	ctrl, err := s.retentionController()
	if err != nil {
		return nil, err
	}
	if s.namespaces != nil {
		docs, err := s.namespaces.List(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("maximum run retention cannot be checked against every namespace: %w", err))
		}
		var exceeding []string
		for _, ns := range docs {
			d := ns.GetRunRetention()
			if d != nil && d.CheckValid() == nil && d.AsDuration() > requested {
				exceeding = append(exceeding, fmt.Sprintf("%s (%s)", ns.GetName(), d.AsDuration()))
			}
		}
		if len(exceeding) > 0 {
			sort.Strings(exceeding)
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
				"maximum run retention %s is below the run retention of namespace(s) %s; lower them first",
				requested, exceeding))
		}
	}
	policy, err := ctrl.SetMax(ctx, requested)
	if err != nil {
		return nil, connect.NewError(retentionWriteErrorCode(err),
			fmt.Errorf("maximum run retention %s could not be applied: %w", requested, err))
	}
	return connect.NewResponse(&orkstackv1.UpdateRetentionPolicyResponse{
		Policy:                     policy,
		NamespaceRetentionEnforced: s.retentionEnforced,
	}), nil
}

// validateRetentionDuration turns a requested protobuf Duration into a Go
// time.Duration the same way the write path will actually use it, or rejects
// it — never partially: every rejection here happens before any store or
// cluster write. field names what was being validated, so the same three
// checks read correctly for the deployment maximum and for one namespace's
// own run_retention.
//
// Three distinct ways a Duration can be unusable, each caught explicitly:
//
//  1. nil — no duration was supplied at all.
//  2. Malformed per the protobuf Duration spec — CheckValid catches
//     out-of-range nanos and a seconds/nanos sign mismatch (e.g.
//     seconds=1,nanos=-1), which durationpb.AsDuration would otherwise
//     silently misinterpret rather than reject.
//  3. In-range for protobuf's Duration (±10,000 years) but too large to
//     represent losslessly as a Go time.Duration (±~292 years):
//     Duration.AsDuration performs SATURATING arithmetic on overflow rather
//     than erroring, so e.g. seconds=315576000000 (10,000 years) silently
//     clamps to math.MaxInt64 nanoseconds — a different value than what was
//     requested. Guarding against this requires round-tripping the
//     converted time.Duration back through durationpb.New and checking it
//     reproduces the exact seconds/nanos requested; CheckValid alone does
//     not catch it, since 10,000 years is a perfectly valid protobuf
//     Duration.
func validateRetentionDuration(field string, requested *durationpb.Duration) (time.Duration, error) {
	if requested == nil {
		return 0, fmt.Errorf("%s must be a positive duration", field)
	}
	if err := requested.CheckValid(); err != nil {
		return 0, fmt.Errorf("%s is not a valid duration: %w", field, err)
	}
	d := requested.AsDuration()
	if roundtrip := durationpb.New(d); roundtrip.GetSeconds() != requested.GetSeconds() ||
		roundtrip.GetNanos() != requested.GetNanos() {
		return 0, fmt.Errorf(
			"%s %s exceeds the range a Go duration can represent losslessly (max ~292 years)",
			field, requested)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", field)
	}
	return d, nil
}

// retentionWriteErrorCode classifies a retention write failure the way the
// caller can act on it. The underlying cluster's own validation refusal —
// its server-side minimum-retention rejection is exactly this shape —
// surfaces as *serviceerror.InvalidArgument, so those come back as
// InvalidArgument rather than the Unavailable every other reachability
// failure here uses. The message is passed through untouched either way:
// this classifies, it never rewrites what the underlying system said.
func retentionWriteErrorCode(err error) connect.Code {
	var invalid *serviceerror.InvalidArgument
	if errors.As(err, &invalid) {
		return connect.CodeInvalidArgument
	}
	return connect.CodeUnavailable
}
