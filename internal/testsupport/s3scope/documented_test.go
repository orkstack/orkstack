package s3scope

import (
	"encoding/json/v2"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// documentedSection is docs/DEPLOY.md's "An Orker's own storage credential",
// to the heading or the paragraph that begins the scope check.
func documentedSection(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "DEPLOY.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(raw), "\n### An Orker's own storage credential\n")
	if !ok {
		t.Fatal("docs/DEPLOY.md has no section for an Orker's own storage credential")
	}
	section, _, ok := strings.Cut(rest, "\n**The scope check.**")
	if !ok {
		t.Fatal("the section does not reach the scope check")
	}
	return section
}

// granted is the verbs a statement of the policy grants, by prefix; a HEAD is
// a GET to a store's policy, so it is not named.
type granted map[string]map[Verb]bool

func (g granted) add(prefix string, v Verb) {
	if g[prefix] == nil {
		g[prefix] = map[Verb]bool{}
	}
	g[prefix][v] = true
}

// tableGrants reads the section's prefix table: a cell's verbs are its
// comma-separated words, up to a semicolon.
func tableGrants(t *testing.T, section string) granted {
	t.Helper()
	g := granted{}
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		cells := strings.Split(line, "|")
		if len(cells) != 4 {
			t.Fatalf("not a two-column row: %s", line)
		}
		verbs, _, _ := strings.Cut(cells[2], ";")
		for i, p := range strings.Split(cells[1], "`") {
			if i%2 == 0 {
				continue
			}
			for _, v := range strings.Split(verbs, ",") {
				verb := Verb(strings.TrimSpace(v))
				if !slices.Contains([]Verb{Put, Get, List, Delete}, verb) {
					t.Fatalf("%q is no verb, in %s", verb, line)
				}
				g.add(p, verb)
			}
		}
	}
	return g
}

// stringOrList is an IAM policy's string or list of strings.
type stringOrList []string

func (s *stringOrList) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var one string
		if err := json.Unmarshal(b, &one); err != nil {
			return err
		}
		*s = stringOrList{one}
		return nil
	}
	return json.Unmarshal(b, (*[]string)(s))
}

// jsonGrants reads the section's AWS policy document.
func jsonGrants(t *testing.T, section string) granted {
	t.Helper()
	_, rest, ok := strings.Cut(section, "\n```json\n")
	if !ok {
		t.Fatal("the section has no JSON policy")
	}
	body, _, _ := strings.Cut(rest, "\n```")
	var doc struct {
		Version   string
		Statement []struct {
			Effect    string
			Action    stringOrList
			Resource  stringOrList
			Condition struct {
				StringLike map[string][]string
			} `json:",omitzero"`
		}
	}
	if err := json.Unmarshal([]byte(body), &doc, json.RejectUnknownMembers(true)); err != nil {
		t.Fatalf("the documented policy: %v", err)
	}
	const objects = "arn:aws:s3:::BUCKET/"
	verbOf := map[string]Verb{"s3:PutObject": Put, "s3:GetObject": Get, "s3:DeleteObject": Delete}
	g := granted{}
	for _, st := range doc.Statement {
		if st.Effect != "Allow" {
			t.Fatalf("a statement's effect is %q", st.Effect)
		}
		for _, action := range st.Action {
			switch verb, ok := verbOf[action]; {
			case ok:
				for _, r := range st.Resource {
					prefix, under := strings.CutPrefix(r, objects)
					if prefix, ok = strings.CutSuffix(prefix, "*"); !under || !ok || !strings.HasSuffix(prefix, "/") {
						t.Fatalf("%s on %q, which is no prefix of the bucket", action, r)
					}
					g.add(prefix, verb)
				}
			case action == "s3:ListBucket":
				for _, p := range st.Condition.StringLike["s3:prefix"] {
					prefix, ok := strings.CutSuffix(p, "*")
					if !ok || !strings.HasSuffix(prefix, "/") {
						t.Fatalf("a listing of %q", p)
					}
					g.add(prefix, List)
				}
			case action == "s3:AbortMultipartUpload":
				// An artifact's upload aborts the parts it leaves; it is
				// part of a put and grants no other.
				if !slices.Equal(st.Resource, stringOrList{objects + "orkflows/*"}) {
					t.Fatalf("%s on %q", action, st.Resource)
				}
			default:
				t.Fatalf("the documented policy grants %s, which no table names", action)
			}
		}
	}
	return g
}

// TestOrkerPolicy_IsTheDocumentedOne holds the policy the tests run an Orker
// under to both of its statements in docs/DEPLOY.md, the table and the AWS
// document an operator copies, verb by verb, so none of the three can change
// alone.
func TestOrkerPolicy_IsTheDocumentedOne(t *testing.T) {
	want := granted{}
	for _, rule := range OrkerPolicy() {
		for _, v := range rule.Verbs {
			if v != Head {
				want.add(rule.Prefix, v)
			}
		}
		if slices.Contains(rule.Verbs, Head) != slices.Contains(rule.Verbs, Get) {
			t.Errorf("%s: a HEAD is a GET to a store's policy", rule.Prefix)
		}
	}
	section := documentedSection(t)
	for name, got := range map[string]granted{"table": tableGrants(t, section), "JSON policy": jsonGrants(t, section)} {
		for prefix := range maps.Keys(want) {
			if !maps.Equal(got[prefix], want[prefix]) {
				t.Errorf("docs/DEPLOY.md's %s grants %v on %s; the tests' policy grants %v", name, slices.Sorted(maps.Keys(got[prefix])), prefix, slices.Sorted(maps.Keys(want[prefix])))
			}
		}
		for prefix := range got {
			if want[prefix] == nil {
				t.Errorf("docs/DEPLOY.md's %s grants %s, which the tests' policy does not name", name, prefix)
			}
		}
	}
}
