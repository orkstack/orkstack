// This file, and removed_test.go beside it, are the ONLY two in internal/dsl
// that may spell a removed key. They exist to reject it, and a rule forbidding
// the compiler to name what it rejects would forbid the rejection.
// proof/M10.1/naming-sweep.sh exempts exactly this pair and holds every other
// file in the package to the ban.
package dsl

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// removedTaskFields are the keys a task document may no longer carry. A key
// here is decoded on purpose: yaml.v3 ignores what it cannot map, so a field
// that simply disappeared from the struct would be accepted in silence and the
// author would find out when the run behaved differently.
//
// The key is decoded into a yaml.Node rather than a *string, because a pointer
// only records a key that carried a VALUE: yaml.v3 leaves it nil for `queue:`
// and for `queue: null`, and a document written against the old contract would
// have compiled without a word. A Node records the key itself — it is zero
// only when the key is absent — so presence, not value, is what the check
// reads.
//
// Removal is never an alias. Copying `queue:` into `pool:` would leave a
// Temporal queue name inside a field that means a set of Orkers — the document
// would still be wrong, and every naming gate in the tree would pass on it.
type removedTaskFields struct {
	Queue  yaml.Node `yaml:"queue"`
	Plugin yaml.Node `yaml:"plugin"`
}

// check reports the first removed key the task document carries.
func (r removedTaskFields) check(taskID string) error {
	if !r.Queue.IsZero() {
		return fmt.Errorf(
			"task %q: `queue:` was removed in v0.0.3; use `pool:` instead "+
				"(a pool is a set of Orkers, and it resolves to a Temporal task "+
				"queue inside the engine — the DSL no longer names one)", taskID)
	}
	if !r.Plugin.IsZero() {
		return fmt.Errorf(
			"task %q: `plugin:` was removed in v0.0.5; use `uses:` instead "+
				"(an activity is an OCI image, not a Go type compiled into an Orker: "+
				"`uses: shell` for a first-party activity, or "+
				"`uses: ghcr.io/acme/migrate:v3` for any image at all)", taskID)
	}
	return nil
}

// checkUses refuses the one `uses:` value that names a retired capability.
//
// `container` was a builtin plugin, and there is deliberately NO
// activity-container image to replace it: the plugin reached a daemon through
// an ambient CLI plus DOCKER_HOST, and §9.1 forbids the socket as source or
// target, credential mounts, --privileged, --device and host namespaces. The
// capability did not disappear — every task is already a container, so what
// `uses: container` did is now what naming that image directly does.
//
// It is an ERROR rather than a one-release alias because the launch contract
// excludes compatibility shims outright, and because an alias could not have
// preserved the example that motivated it anyway:
// examples/container-example.ork.yaml selected its image from
// `{{ .inputs.image }}`, which no alias can pin (§17 C6, C7).
func checkUses(taskID, uses string) error {
	if uses != "container" {
		return nil
	}
	return fmt.Errorf(
		"task %q: `uses: container` was removed in v0.0.5 and has no replacement alias. "+
			"Every task is already a container, so name the image you wanted to run "+
			"directly — `uses: ghcr.io/acme/tool:v1` — instead of asking a container "+
			"activity to start one. A container that starts containers would need the "+
			"docker socket, which the security profile forbids without exception", taskID)
}
