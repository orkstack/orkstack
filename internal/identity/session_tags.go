package identity

import (
	"fmt"
	"slices"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ClaimAWSTags is the claim AWS STS reads a web identity's session tags from
// (providers.md §7.3): AssumeRoleWithWebIdentity takes none as a parameter.
//
// Nothing here is bounded or escaped. Every value is, as the engine writes it,
// a name, an Orkflow version, an id or a closed word, each within AWS's
// grammar and length for a tag's value, and there are twelve at most of AWS's
// fifty (TestSessionTags_EveryGrammarIsWithinAWSs,
// TestSessionTags_TheLargestToken, TestOrkflowVersions_AreValuesAWSTakes).
//
// AWS's bound on the packed size of a session's tags and policies is not
// proven: AWS states it only as a percentage of what it takes, and nothing
// here can measure it. The tags of the largest token are 1,547 bytes before
// packing. A session past the bound is refused by AWS, PackedPolicyTooLarge.
const ClaimAWSTags = "https://aws.amazon.com/tags"

// sessionTags is the claim of a token whose other claims are claims, tagged
// as the admission pinned. A tag's value is the claim's of the same name, so
// the two never differ; the grant's, which is no claim, is the subject's.
func sessionTags(claims map[string]any, grant string, tagging artifacts.SessionTagging) map[string]any {
	tags := map[string][]string{}
	for _, key := range artifacts.SessionTagKeys {
		value, _ := claims[key].(string)
		if key == artifacts.SessionTagGrant {
			value = grant
		}
		// A value that is empty is no tag: a condition on it then fails.
		if value != "" {
			tags[key] = []string{value}
		}
	}
	// AWS refuses a transitive key that is no tag of the token.
	transitive := []string{}
	for _, key := range tagging.Transitive {
		if tags[key] != nil {
			transitive = append(transitive, key)
		}
	}
	return map[string]any{"principal_tags": tags, "transitive_tag_keys": transitive}
}

// SessionTagSettings are the audiences whose tokens carry session tags, as
// the deployment's declared providers ask. A token names an audience and no
// provider, so two providers that share one must ask for the same; an engine
// whose do not refuses to boot.
func SessionTagSettings(set provider.Set) (map[string]artifacts.SessionTagging, error) {
	type setting struct {
		provider   string
		on         bool
		transitive []string
	}
	asked := map[string]setting{}
	for _, name := range set.Names() {
		p, _ := set.ByName(name)
		tagger, ok := p.(provider.SessionTagger)
		if !ok {
			continue
		}
		on, transitive := tagger.SessionTags()
		now := setting{provider: name, on: on, transitive: slices.Compact(slices.Sorted(slices.Values(transitive)))}
		first, seen := asked[p.Audience()]
		if !seen {
			asked[p.Audience()] = now
			continue
		}
		if first.on != now.on || !slices.Equal(first.transitive, now.transitive) {
			return nil, fmt.Errorf("providers %s and %s share the audience %q and ask for different session tags; a token is of an audience, so they must ask for the same",
				first.provider, name, p.Audience())
		}
	}
	out := map[string]artifacts.SessionTagging{}
	for audience, s := range asked {
		if s.on {
			out[audience] = artifacts.SessionTagging{Transitive: s.transitive}
		}
	}
	return out, nil
}
