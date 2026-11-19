package engine

import (
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

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

// pinProfiles records what one pool's profiled tasks pinned, and when the
// census they were judged on was read. Nothing for a pool none of whose
// tasks names a profile.
func (p *RunPlan) pinProfiles(pool string, pins profilePins, censusReadAt time.Time) {
	if pins.identities == nil {
		return
	}
	if p.ProfileIdentities == nil {
		p.ProfileIdentities = map[string]map[string]string{}
	}
	p.ProfileIdentities[pool] = pins.identities
	if p.ProfileContracts == nil {
		p.ProfileContracts = map[string]map[string]ProfileContracts{}
	}
	p.ProfileContracts[pool] = pins.contracts
	if !censusReadAt.IsZero() && (p.profileCensusAt.IsZero() || censusReadAt.Before(p.profileCensusAt)) {
		p.profileCensusAt = censusReadAt
	}
}

// ProfileCensusReadAt is when the oldest census this plan, or a composed
// child's plan in it, pinned profile contracts from was read, on the
// submitting replica's clock; zero when none pinned any. Never encoded: it
// is the submit's own freshness guard (profileCensusMaxAge), not a fact a
// run carries.
func (p RunPlan) ProfileCensusReadAt() time.Time {
	oldest := p.profileCensusAt
	for _, child := range p.Children {
		if child == nil {
			continue
		}
		if t := child.Plan.ProfileCensusReadAt(); !t.IsZero() && (oldest.IsZero() || t.Before(oldest)) {
			oldest = t
		}
	}
	return oldest
}

// ProfileCensusMaxAge is how old the census a submit pinned profile
// contracts from may be when its run is minted. Older, the submit is refused
// Unavailable and asked again, so a run's created_at is never far past the
// Orkers its plan was judged on (docs/DEPLOY.md, "Changing a profile").
const ProfileCensusMaxAge = 60 * time.Second
