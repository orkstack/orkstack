package api

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
