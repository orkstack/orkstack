// This file, and removed_test.go beside it, are the ONLY two in internal/dsl
// that may spell a removed key. They exist to reject it, and a rule forbidding
// the compiler to name what it rejects would forbid the rejection.
// proof/M10.1/naming-sweep.sh exempts exactly this pair and holds every other
// file in the package to the ban.
package dsl

import "fmt"

// removedTaskFields are the keys a task document may no longer carry. A key
// here is decoded on purpose: yaml.v3 ignores what it cannot map, so a field
// that simply disappeared from the struct would be accepted in silence and the
// author would find out when the run behaved differently.
//
// Removal is never an alias. Copying `queue:` into `pool:` would leave a
// Temporal queue name inside a field that means a set of Orkers — the document
// would still be wrong, and every naming gate in the tree would pass on it.
type removedTaskFields struct {
	Queue *string `yaml:"queue"`
}

// check reports the first removed key the task document carries.
func (r removedTaskFields) check(taskID string) error {
	if r.Queue != nil {
		return fmt.Errorf(
			"task %q: `queue:` was removed in v0.0.3; use `pool:` instead "+
				"(a pool is a set of Orkers, and it resolves to a Temporal task "+
				"queue inside the engine — the DSL no longer names one)", taskID)
	}
	return nil
}
