// Package dashboards builds the observability kit's two Grafana dashboards,
// OrkStack (uid orkstack) and OrkStack / Run (uid orkstack-run), with
// Grafana's own typed builder, grafana-foundation-sdk, pinned in go.mod. The
// provisioned JSON under deploy/observability/grafana is its output, checked
// for drift by the package's tests (deploy/observability/README.md, "Dashboards").
package dashboards

// The dashboards' uids: the only two the kit provisions.
const (
	UIDOrkStack = "orkstack"
	UIDRun      = "orkstack-run"
)
