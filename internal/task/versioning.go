package task

import (
	"fmt"
	"os"
	"strings"

	"go.temporal.io/sdk/worker"
)

// Worker Deployment Versioning: the identity a released OrkStack binary claims
// on the cluster, and the guard that keeps it off until an operator asks for it.
//
// One Worker Deployment spans BOTH binaries. Temporal routes the activities of
// a PINNED workflow to the workflow's own Deployment Version, with one
// exception (api/enums/v1/workflow.pb.go, VERSIONING_BEHAVIOR_PINNED):
//
//	Activities of PINNED workflows are sent to the same Deployment Version.
//	Exception … when the activity Task Queue workers are not present in the
//	workflow's Deployment Version, in which case the activity will be sent to
//	the Current Deployment Version of its own task queue.
//
// So an in-flight run's tasks stay on the Orker build that started them only if
// the Orker polling the task queue carries the SAME (DeploymentName, BuildID)
// as the engine hosting RunWorkflow. `ork` and `orker` are therefore released
// in lockstep as one version. See docs/DEPLOY.md.
const (
	// EnvDeploymentName names the Worker Deployment both binaries join.
	EnvDeploymentName = "ORK_DEPLOYMENT_NAME"

	// EnvBuildID names this release inside that deployment. It is the switch:
	// unset means unversioned (see Deployment.Versioned).
	EnvBuildID = "ORK_BUILD_ID"

	// DefaultDeploymentName is the deployment an OrkStack release joins when
	// ORK_DEPLOYMENT_NAME is unset.
	//
	// It is deliberately non-empty while DefaultBuildID is empty, so exactly
	// one variable — ORK_BUILD_ID — turns versioning on. Defaulting both to ""
	// would make `ORK_BUILD_ID=v2` alone a silent no-op, which is the worse
	// failure: an operator would believe a rollout was versioned when it was
	// not. Versioning still stays off by default, because a build id has no
	// sane default to invent: it is the release's identity.
	DefaultDeploymentName = "orkstack"

	// DefaultBuildID is empty: no build id, no versioning.
	DefaultBuildID = ""
)

// Deployment is the (deployment name, build id) pair a worker registers with.
// The zero value, and any value missing a build id, means unversioned.
type Deployment struct {
	Name    string
	BuildID string
}

// DeploymentFromEnv reads the deployment identity from ORK_ configuration.
// With no configuration it returns the unversioned default, which is what
// keeps a single-process `make run-ork` working with no setup at all.
func DeploymentFromEnv() Deployment {
	return Deployment{
		Name:    envOr(EnvDeploymentName, DefaultDeploymentName),
		BuildID: envOr(EnvBuildID, DefaultBuildID),
	}
}

func envOr(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

// Versioned reports whether this identity turns Worker Deployment Versioning
// on. Both halves are required: a build id without a deployment to hold it
// names nothing the server can route to.
func (d Deployment) Versioned() bool {
	return d.Name != "" && d.BuildID != ""
}

// Validate rejects an identity the cluster would refuse later, at a point where
// the error can still name the variable that caused it. An unversioned
// identity is always valid — that is the off switch, not a mistake.
func (d Deployment) Validate() error {
	if !d.Versioned() {
		return nil
	}
	// "." is reserved: the server builds a version's canonical string from
	// the deployment name and the build id, so a dot in the name is
	// ambiguous (see client.WorkerDeploymentInfo.Name).
	if strings.Contains(d.Name, ".") {
		return fmt.Errorf("%s=%q: a worker deployment name cannot contain \".\"", EnvDeploymentName, d.Name)
	}
	if strings.TrimSpace(d.Name) != d.Name || strings.TrimSpace(d.BuildID) != d.BuildID {
		return fmt.Errorf("%s=%q %s=%q: no surrounding whitespace", EnvDeploymentName, d.Name, EnvBuildID, d.BuildID)
	}
	return nil
}

// String renders the identity for a boot log line.
func (d Deployment) String() string {
	if !d.Versioned() {
		return "unversioned"
	}
	return d.Name + "." + d.BuildID
}

// Apply returns opts with this deployment's versioning configuration attached.
// An unversioned identity returns opts unchanged, so a worker built this way
// behaves exactly as it did before versioning existed.
//
// DefaultVersioningBehavior is deliberately never set: every workflow type
// declares its own behaviour at registration (RunWorkflow is Pinned, the
// idempotency claim is AutoUpgrade). It also MUST stay unset while
// UseVersioning is false, because the SDK panics on that combination
// (sdk@v1.47.0/internal/internal_worker.go:2321-2323) — which is why it lives
// behind the guard rather than beside it.
func (d Deployment) Apply(opts worker.Options) worker.Options {
	if !d.Versioned() {
		return opts
	}
	opts.DeploymentOptions = worker.DeploymentOptions{
		UseVersioning: true,
		Version: worker.WorkerDeploymentVersion{
			DeploymentName: d.Name,
			BuildID:        d.BuildID,
		},
	}
	return opts
}
