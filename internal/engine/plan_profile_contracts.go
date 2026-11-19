package engine

import "github.com/orkstack/orkstack/internal/stablejson"

// ProfileContracts is what one run pinned of one profile on one pool at
// submit (RunPlan.ProfileContracts; docs/DEPLOY.md, "Changing a profile").
type ProfileContracts struct {
	// Set is every contract the pool's serving Orkers served, sorted by
	// label: a task whose image declares no contract for the profile runs
	// under any of them. An empty set encodes as [], never null.
	Set []ContractPin `json:"set"`
	// Declared maps each label a task's image declared to the definitions
	// its Orkers gave it at submit; such a task runs under that contract
	// alone. omitempty: a run with no declaring image carries none.
	Declared map[string][]string `json:"declared,omitempty"`
}

// ContractPin is one contract label and the definitions (structural
// identity digests, sorted) the pool's Orkers gave it at submit. No digest
// pins the label alone, which only an Orker that accepts it serves.
type ContractPin struct {
	Label   string   `json:"label"`
	Digests []string `json:"digests,omitempty"`
}

// MarshalJSON encodes p with an empty Set as [], never null: stablejson keeps
// v1's nil-as-null, and a set is a list in every encoding a reader sees. A
// value method, so the plan's map values reach it under json v1 and v2.
func (p ProfileContracts) MarshalJSON() ([]byte, error) {
	type plain ProfileContracts
	if p.Set == nil {
		p.Set = []ContractPin{}
	}
	return stablejson.Marshal(plain(p))
}
