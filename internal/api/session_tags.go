package api

import (
	"slices"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// sessionTagsOf is what a's admission pins: the setting of each audience that
// its sets and grants use, nil when none is tagged. It is the admission's own
// copy in the record's one form, the transitive tags sorted, each once.
func (s *Service) sessionTagsOf(a artifacts.Admission) map[string]artifacts.SessionTagging {
	var out map[string]artifacts.SessionTagging
	take := func(audience string) {
		if tagging, ok := s.sessionTags[audience]; ok {
			if out == nil {
				out = map[string]artifacts.SessionTagging{}
			}
			var keys []string
			if len(tagging.Transitive) > 0 {
				keys = slices.Compact(slices.Sorted(slices.Values(tagging.Transitive)))
			}
			out[audience] = artifacts.SessionTagging{Transitive: keys}
		}
	}
	for _, set := range a.Sets {
		for _, id := range set.Identities {
			take(id.Audience)
		}
	}
	for _, g := range a.Grants {
		take(g.Audience)
	}
	return out
}
