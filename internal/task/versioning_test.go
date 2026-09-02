package task_test

import (
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/task"
)

// lazyClient is a Temporal client that has not dialled anything. worker.New
// validates its options against it without touching the network, so these
// tests exercise the SDK's real construction path — including the panics it
// raises for an inconsistent versioning configuration — with no cluster.
func lazyClient(t *testing.T) client.Client {
	t.Helper()
	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewLazyClient: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestDeploymentOptions_UnsetEnvLeavesVersioningOff is the §6 guarantee: a
// developer running `make run-ork` with no configuration gets an unversioned
// worker, exactly as if the versioning options did not exist.
//
// The DefaultVersioningBehavior half is not cosmetic. The SDK panics when it
// is set while UseVersioning is false
// (sdk@v1.47.0/internal/internal_worker.go:2321-2323), so an implementation
// that hoisted it out of the guard would take every binary down at boot.
func TestDeploymentOptions_UnsetEnvLeavesVersioningOff(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "")
	t.Setenv(task.EnvBuildID, "")

	deployment := task.DeploymentFromEnv()
	if deployment.Versioned() {
		t.Fatalf("deployment %v reports versioned with no build id configured", deployment)
	}

	options := deployment.Apply(worker.Options{DisableRegistrationAliasing: true})
	if options.DeploymentOptions.UseVersioning {
		t.Error("UseVersioning = true with no configuration, want versioning off by default")
	}
	if options.DeploymentOptions.DefaultVersioningBehavior != workflow.VersioningBehaviorUnspecified {
		t.Errorf("DefaultVersioningBehavior = %v with versioning off, want Unspecified: the SDK panics on that pair",
			options.DeploymentOptions.DefaultVersioningBehavior)
	}
	if (options.DeploymentOptions.Version != worker.WorkerDeploymentVersion{}) {
		t.Errorf("Version = %+v with versioning off, want the zero version", options.DeploymentOptions.Version)
	}
	if !options.DisableRegistrationAliasing {
		t.Error("Apply dropped a caller's worker option")
	}

	// The panic is a real construction-time check, so build the real worker.
	w := worker.New(lazyClient(t), task.DefaultQueue, options)
	w.RegisterWorkflowWithOptions(func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: "Probe"})
}

// TestDeploymentOptions_BuildIDTurnsVersioningOn is the other half: one
// variable moves a worker onto the GA versioning path, carrying the deployment
// name every OrkStack binary of that release shares.
func TestDeploymentOptions_BuildIDTurnsVersioningOn(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "")
	t.Setenv(task.EnvBuildID, "v2")

	deployment := task.DeploymentFromEnv()
	if !deployment.Versioned() {
		t.Fatalf("deployment %v is not versioned with ORK_BUILD_ID set", deployment)
	}
	if deployment.Name != task.DefaultDeploymentName {
		t.Errorf("deployment name = %q, want the default %q", deployment.Name, task.DefaultDeploymentName)
	}
	if got, want := deployment.String(), task.DefaultDeploymentName+".v2"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	options := deployment.Apply(worker.Options{DisableWorkflowWorker: true})
	if !options.DeploymentOptions.UseVersioning {
		t.Error("UseVersioning = false with a build id configured")
	}
	want := worker.WorkerDeploymentVersion{DeploymentName: task.DefaultDeploymentName, BuildID: "v2"}
	if options.DeploymentOptions.Version != want {
		t.Errorf("Version = %+v, want %+v", options.DeploymentOptions.Version, want)
	}
	if options.DeploymentOptions.DefaultVersioningBehavior != workflow.VersioningBehaviorUnspecified {
		t.Errorf("DefaultVersioningBehavior = %v, want Unspecified so every workflow type declares its own",
			options.DeploymentOptions.DefaultVersioningBehavior)
	}
	if !options.DisableWorkflowWorker {
		t.Error("Apply dropped a caller's worker option")
	}

	// A versioned worker with no DefaultVersioningBehavior panics on any
	// workflow registered without one. That is the invariant behind the
	// Pinned/AutoUpgrade registrations in cmd/ork.
	w := worker.New(lazyClient(t), task.DefaultQueue, deployment.Apply(worker.Options{}))
	func() {
		defer func() {
			if recover() == nil {
				t.Error("registering a workflow with no VersioningBehavior on a versioned worker did not panic; " +
					"the compile-time guarantee that every workflow declares one is gone")
			}
		}()
		w.RegisterWorkflowWithOptions(func(workflow.Context) error { return nil },
			workflow.RegisterOptions{Name: "Undeclared"})
	}()
	w.RegisterWorkflowWithOptions(func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: "Declared", VersioningBehavior: workflow.VersioningBehaviorPinned})
}

// TestDeploymentFromEnv_ExplicitNameAndBuildID covers the deployment an
// operator names, which is what docs/DEPLOY.md tells them to set on both
// binaries of one release.
func TestDeploymentFromEnv_ExplicitNameAndBuildID(t *testing.T) {
	t.Setenv(task.EnvDeploymentName, "orkstack-prod")
	t.Setenv(task.EnvBuildID, "2026.08.16-a1b2c3d")

	deployment := task.DeploymentFromEnv()
	if deployment.Name != "orkstack-prod" || deployment.BuildID != "2026.08.16-a1b2c3d" {
		t.Fatalf("deployment = %+v, want the configured pair", deployment)
	}
	if err := deployment.Validate(); err != nil {
		t.Errorf("Validate() = %v, want a build id with dots to be accepted", err)
	}
}

// TestDeployment_ValidateRejectsWhatTheClusterWould turns a configuration
// mistake into a boot error that names the variable, instead of a confusing
// rejection from the server on the first poll.
func TestDeployment_ValidateRejectsWhatTheClusterWould(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deployment task.Deployment
		wantErr    bool
	}{
		{"unversioned is always valid", task.Deployment{Name: "orkstack"}, false},
		{"zero value is always valid", task.Deployment{}, false},
		{"ordinary pair", task.Deployment{Name: "orkstack", BuildID: "v1"}, false},
		{"dot in deployment name", task.Deployment{Name: "ork.stack", BuildID: "v1"}, true},
		{"padded name", task.Deployment{Name: " orkstack", BuildID: "v1"}, true},
		{"padded build id", task.Deployment{Name: "orkstack", BuildID: "v1 "}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.deployment.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("Validate(%+v) = nil, want an error", tc.deployment)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Validate(%+v) = %v, want nil", tc.deployment, err)
			}
		})
	}
}

// TestDeployment_StringNamesTheVersionOrSaysUnversioned keeps the boot log
// readable: an operator reading it must be able to tell which of the two
// states the process is in.
func TestDeployment_StringNamesTheVersionOrSaysUnversioned(t *testing.T) {
	if got := (task.Deployment{Name: "orkstack"}).String(); got != "unversioned" {
		t.Errorf("String() with no build id = %q, want %q", got, "unversioned")
	}
	if got := (task.Deployment{Name: "orkstack", BuildID: "v7"}).String(); got != "orkstack.v7" {
		t.Errorf("String() = %q, want %q", got, "orkstack.v7")
	}
}
