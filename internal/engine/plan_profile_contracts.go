package engine

// ProfileContracts is what one run pinned of one profile on one pool at
// submit (RunPlan.ProfileContracts; docs/DEPLOY.md, "Changing a profile").
type ProfileContracts struct {
	// Set is every contract the pool's serving Orkers served, sorted by
	// label: a task whose image declares no contract for the profile runs
	// under any of them.
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
