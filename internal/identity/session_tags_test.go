package identity

import (
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/types"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const stsAudience = "sts.amazonaws.com"

func taggedInput(tagging artifacts.SessionTagging) ClaimsInput {
	adm := claimsAdmission()
	adm.SessionTags = map[string]artifacts.SessionTagging{stsAudience: tagging}
	return ClaimsInput{Issuer: "https://ork.example", Deployment: "prod", Admission: adm, TaskID: "apply", Attempt: 2, Identity: "aws",
		Audience: stsAudience, Pool: "prod", Orker: "orker-1", Now: time.Unix(1755302400, 0), Lifetime: time.Hour, JTI: "j1"}
}

func tagsOf(t *testing.T, in ClaimsInput) (map[string][]string, []string) {
	t.Helper()
	c, err := BuildClaims(in)
	if err != nil {
		t.Fatal(err)
	}
	claim, ok := c[ClaimAWSTags].(map[string]any)
	if !ok {
		t.Fatalf("the token carries no tags: %v", c)
	}
	if len(claim) != 2 {
		t.Errorf("the claim holds %v, want principal_tags and transitive_tag_keys alone", slices.Sorted(maps.Keys(claim)))
	}
	return claim["principal_tags"].(map[string][]string), claim["transitive_tag_keys"].([]string)
}

// The claim is AWS's own shape, and holds the run's facts, each under the
// name of the claim it comes from.
func TestSessionTags_ASubmittedRunsToken(t *testing.T) {
	tags, transitive := tagsOf(t, taggedInput(artifacts.SessionTagging{}))
	want := map[string][]string{
		"ork_namespace_id": {"ns_1"}, "ork_orkflow_id": {"of_1"}, "ork_namespace": {"payments"}, "ork_orkflow": {"deploy"},
		"ork_task": {"apply"}, "ork_origin": {"submit"}, "ork_run_id": {"ork-run"}, "ork_orkflow_version": {"3"}, "ork_pool": {"prod"},
	}
	if !reflect.DeepEqual(tags, want) {
		t.Errorf("tags = %v, want %v", tags, want)
	}
	if transitive == nil || len(transitive) != 0 {
		t.Errorf("transitive_tag_keys = %#v, want an empty list, never null", transitive)
	}
}

// A grant's login is tagged with the grant, a shared Orkflow with its owner
// and a child run with its root; no other key is ever a tag, and the
// submitter never is.
func TestSessionTags_EveryKeyComesFromItsClaim(t *testing.T) {
	in := taggedInput(artifacts.SessionTagging{Transitive: []string{"ork_grant", "ork_namespace", "ork_task"}})
	in.Grant, in.Identity = "deployer", "grant:deployer"
	in.Admission.OrkflowNamespace = "platform"
	in.Admission.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-parent"}
	in.Admission.RootRunID = "ork-root"
	tags, transitive := tagsOf(t, in)
	claims, err := BuildClaims(in)
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range tags {
		if !slices.Contains(artifacts.SessionTagKeys, key) {
			t.Errorf("%s is not of the closed set", key)
		}
		if len(values) != 1 {
			t.Errorf("%s has %d values; AWS takes one", key, len(values))
		}
		if key != "ork_grant" && claims[key] != values[0] {
			t.Errorf("the tag %s is %q and its claim %v", key, values[0], claims[key])
		}
	}
	if len(tags) != len(artifacts.SessionTagKeys) {
		t.Errorf("tags = %v, want all of %v", slices.Sorted(maps.Keys(tags)), artifacts.SessionTagKeys)
	}
	if tags["ork_grant"][0] != "deployer" || tags["ork_root_run_id"][0] != "ork-root" || tags["ork_orkflow_namespace"][0] != "platform" {
		t.Errorf("tags = %v", tags)
	}
	if !slices.Equal(transitive, []string{"ork_grant", "ork_namespace", "ork_task"}) {
		t.Errorf("transitive = %v", transitive)
	}
}

// AWS refuses a transitive key the token has no tag for, so one is left out
// where its tag is.
func TestSessionTags_ATransitiveKeyWithNoTagIsLeftOut(t *testing.T) {
	_, transitive := tagsOf(t, taggedInput(artifacts.SessionTagging{Transitive: []string{"ork_grant", "ork_task"}}))
	if !slices.Equal(transitive, []string{"ork_task"}) {
		t.Errorf("transitive = %v, want ork_task alone", transitive)
	}
	in := taggedInput(artifacts.SessionTagging{})
	in.Admission.OrkflowID = ""
	if tags, _ := tagsOf(t, in); tags["ork_orkflow_id"] != nil {
		t.Error("a value that is empty is a tag: a condition on it would pass for every run without one")
	}
}

// Only an audience the admission pinned is tagged, whatever the setting is
// now: the mint reads nothing else.
func TestSessionTags_OnlyAPinnedAudience(t *testing.T) {
	in := taggedInput(artifacts.SessionTagging{})
	in.Audience = "https://vault"
	c, err := BuildClaims(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c[ClaimAWSTags]; ok {
		t.Error("an audience that is not pinned is tagged")
	}
}

// The largest token any run could hold: a child of a shared Orkflow, through
// a grant, every name as long as it may be. Its tags are fewer than AWS's 50,
// each key and value within its 128 and 256 characters. AWS also bounds their
// packed size, which it states only as a percentage and nothing here can ask:
// the sum is pinned so that a change which makes it larger is seen.
func TestSessionTags_TheLargestToken(t *testing.T) {
	long := strings.Repeat("a", maxNameBytes)
	id := strings.Repeat("0", 26)
	in := taggedInput(artifacts.SessionTagging{})
	in.Grant, in.Identity = strings.Repeat("g", 63), "grant:g"
	in.TaskID, in.Pool = long, long
	in.Admission.Namespace, in.Admission.Orkflow, in.Admission.OrkflowVersion, in.Admission.OrkflowNamespace = long, long, long, "b"+long[1:]
	in.Admission.NamespaceID, in.Admission.OrkflowID = artifacts.NamespaceIDPrefix+id, artifacts.OrkflowIDPrefix+id
	in.Admission.RunID, in.Admission.RootRunID = "ork-"+id, "ork-"+id
	in.Admission.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-parent"}
	in.Admission.Origin.Kind = "event-source"
	tags, _ := tagsOf(t, in)
	total := 0
	for key, values := range tags {
		if len(key) > 128 || len(values[0]) > 256 {
			t.Errorf("the tag %s is %d bytes of %d", key, len(values[0]), len(key))
		}
		total += len(key) + len(values[0])
	}
	if len(tags) != len(artifacts.SessionTagKeys) || len(tags) > 50 || total != 1547 {
		t.Errorf("the largest token carries %d tags of %d bytes, want %d of 1547", len(tags), total, len(artifacts.SessionTagKeys))
	}
}

// Every value is of a grammar AWS takes as a tag's value (letters, digits,
// spaces and _ . : / = + - @, at most 256 characters, not beginning "aws:"),
// so none is checked: this holds the grammars it comes from to that. The
// values: namespace, Orkflow, owner, task, grant and pool are slugs; a version
// is a clean component of the store; the rest are ids and closed words.
func TestSessionTags_EveryGrammarIsWithinAWSs(t *testing.T) {
	aws := regexp.MustCompile(`^[A-Za-z0-9 _.:/=+@-]{0,256}$`)
	for _, v := range []string{strings.Repeat("a", 200), "a-0", "1.0.0-rc.1_x", "ns_default", "ork-0", "parent-run", "event-source"} {
		if !aws.MatchString(v) {
			t.Errorf("%q is not a value AWS takes", v)
		}
	}
	for c := range 256 {
		s := string([]byte{'a', byte(c)})
		if slugPattern.MatchString(s) && !aws.MatchString(s) {
			t.Errorf("a name may hold %q, which AWS does not take", s)
		}
	}
}

// TestSessionTagSettings: what the declared providers ask for, by audience;
// two that share one and disagree are refused, naming both.
func TestSessionTagSettings(t *testing.T) {
	const a, b = "providers:\n  aws:\n    type: aws\n    region: eu-west-1\n", "  other:\n    type: aws\n    region: us-east-1\n"
	for name, tc := range map[string]struct {
		doc  string
		want map[string]artifacts.SessionTagging
		err  string
	}{
		"none declared":     {"", map[string]artifacts.SessionTagging{}, ""},
		"tagged by default": {a, map[string]artifacts.SessionTagging{stsAudience: {}}, ""},
		"off":               {a + "    session_tags: false\n", map[string]artifacts.SessionTagging{}, ""},
		"transitive":        {a + "    transitive_tags: [ork_task, ork_pool]\n", map[string]artifacts.SessionTagging{stsAudience: {Transitive: []string{"ork_pool", "ork_task"}}}, ""},
		"two that agree":    {a + b, map[string]artifacts.SessionTagging{stsAudience: {}}, ""},
		"two audiences":     {a + b + "    audience: other\n    session_tags: false\n", map[string]artifacts.SessionTagging{stsAudience: {}}, ""},
		"on and off":        {a + b + "    session_tags: false\n", nil, "providers aws and other share the audience"},
		"another list":      {a + b + "    transitive_tags: [ork_task]\n", nil, "providers aws and other share the audience"},
	} {
		t.Run(name, func(t *testing.T) {
			set, err := provider.ParseWith([]byte(tc.doc), types.All())
			if err != nil {
				t.Fatal(err)
			}
			got, err := SessionTagSettings(set)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("SessionTagSettings = %v, %v; want %q", got, err, tc.err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("SessionTagSettings = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}
