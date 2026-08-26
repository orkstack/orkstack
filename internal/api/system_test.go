package api_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
)

// TestGetSystemInfo_ReportsTheConfigurationItWasWiredWith is the whole
// contract of the admin surface's one new read: the handler echoes what the
// process was configured with, and invents nothing.
//
// The test wires values no environment could produce, so a handler that read
// os.Getenv, a build stamp or a cluster instead of its injected configuration
// answers with something else and fails here. That is the property the design
// asks for — "nothing is fabricated to look observed".
func TestGetSystemInfo_ReportsTheConfigurationItWasWiredWith(t *testing.T) {
	stamped := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	svc := api.NewService(api.Config{
		System: api.SystemInfo{
			EngineVersion:    "v9.9.9-test",
			GoVersion:        "go1.99",
			DeploymentName:   "orkstack-test",
			BuildID:          "build-42",
			ArtifactBackend:  "s3",
			ArtifactLocation: "s3://orkstack-artifacts",
			TemporalAddress:  "cluster.test:7233",
		},
		Now: func() time.Time { return stamped },
	})

	resp, err := svc.GetSystemInfo(context.Background(),
		connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
	if err != nil {
		t.Fatalf("GetSystemInfo: %v", err)
	}
	info := resp.Msg

	for _, field := range []struct {
		name string
		got  string
		want string
	}{
		{"engine_version", info.GetEngineVersion(), "v9.9.9-test"},
		{"go_version", info.GetGoVersion(), "go1.99"},
		{"deployment_name", info.GetDeploymentName(), "orkstack-test"},
		{"build_id", info.GetBuildId(), "build-42"},
		{"artifact_backend", info.GetArtifactBackend(), "s3"},
		{"artifact_location", info.GetArtifactLocation(), "s3://orkstack-artifacts"},
		{"temporal_address", info.GetTemporalAddress(), "cluster.test:7233"},
	} {
		if field.got != field.want {
			t.Errorf("%s = %q, want %q", field.name, field.got, field.want)
		}
	}
	// versioning_active is derived from the two fields above and from nothing
	// else: it says what this replica registered as, not what the cluster
	// routes today.
	if !info.GetVersioningActive() {
		t.Error("versioning_active is false while both a deployment name and a build id are configured")
	}
	// The one observed field, and it is the answering replica's own clock.
	if got := info.GetServerTime().AsTime(); !got.Equal(stamped) {
		t.Errorf("server_time = %s, want the replica's clock %s", got, stamped)
	}
}

// TestGetSystemInfo_ReportsVersioningOffWithoutABuildID pins the switch.
//
// ORK_BUILD_ID alone turns Worker Deployment Versioning on (task.Deployment),
// so the flag has to follow the same rule rather than a second one — an admin
// view reporting "versioned" for a deployment whose runs are unversioned is
// worse than no view at all.
func TestGetSystemInfo_ReportsVersioningOffWithoutABuildID(t *testing.T) {
	svc := api.NewService(api.Config{
		System: api.SystemInfo{
			DeploymentName: "orkstack",
			// No build id: the default, and the off switch.
		},
	})

	resp, err := svc.GetSystemInfo(context.Background(),
		connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
	if err != nil {
		t.Fatalf("GetSystemInfo: %v", err)
	}
	if resp.Msg.GetVersioningActive() {
		t.Error("versioning_active is true with no build id configured")
	}
	if got := resp.Msg.GetBuildId(); got != "" {
		t.Errorf("build_id = %q, want empty — an unversioned deployment has none", got)
	}
	// An unconfigured process still answers: the fields it has no value for
	// are empty, and empty is the honest answer rather than an invented one.
	if got := resp.Msg.GetServerTime(); got == nil {
		t.Error("server_time is unset; the one observed field must always be there")
	}
}

// TestGetSystemInfo_AnswersFromAServiceThatHoldsNoRunState is AGENTS.md §1.7
// read the only way it can be read for this RPC.
//
// Every other admin read leaves the process; this one describes the process,
// which is the one thing only it can answer. What must still hold is that it
// depends on NO run, orkflow or namespace state — so a replica that started
// nothing, holds no store and has no cluster wired answers exactly as the
// replica that started everything does. This Service has none of those seams.
func TestGetSystemInfo_AnswersFromAServiceThatHoldsNoRunState(t *testing.T) {
	wired := api.SystemInfo{
		EngineVersion:    "v0.0.4",
		DeploymentName:   "orkstack",
		ArtifactBackend:  "local",
		ArtifactLocation: "./.data/artifacts",
		TemporalAddress:  "localhost:7233",
	}
	// Two Services of one release: one with every seam a busy replica has,
	// one with nothing at all.
	busy := api.NewService(api.Config{
		System:     wired,
		Starter:    &fakeStarter{},
		Namespaces: registry{"default": {Name: "default"}},
	})
	idle := api.NewService(api.Config{System: wired})

	got := func(svc *api.Service) *orkstackv1.GetSystemInfoResponse {
		t.Helper()
		resp, err := svc.GetSystemInfo(context.Background(),
			connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
		if err != nil {
			t.Fatalf("GetSystemInfo: %v", err)
		}
		return resp.Msg
	}
	from, to := got(busy), got(idle)
	if from.GetEngineVersion() != to.GetEngineVersion() ||
		from.GetDeploymentName() != to.GetDeploymentName() ||
		from.GetBuildId() != to.GetBuildId() ||
		from.GetVersioningActive() != to.GetVersioningActive() ||
		from.GetArtifactBackend() != to.GetArtifactBackend() ||
		from.GetArtifactLocation() != to.GetArtifactLocation() ||
		from.GetTemporalAddress() != to.GetTemporalAddress() {
		t.Errorf("two replicas of one release describe the deployment differently:\n%v\n%v", from, to)
	}
}
