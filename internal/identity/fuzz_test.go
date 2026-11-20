package identity

import (
	"encoding/json/v2"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// FuzzSPIFFESubject: names that build a SPIFFE ID parse back to the same
// names, a SPIFFE ID ParseSPIFFEID accepts is the one its subject builds,
// and the colon form of a subject that builds holds no name that could be
// read another way.
func FuzzSPIFFESubject(f *testing.F) {
	f.Add("payments", "", "deploy", "apply", "", "ork.acme.dev", "spiffe://ork.acme.dev/ork/ns/payments/orkflow/deploy/task/apply")
	f.Add("payments", "platform", "deploy-lib", "apply", "deployer", "a", "spiffe://a/ork/ns/x/shared/y/z/task/t/grant/g")
	f.Add("pay:ments", "", "de/ploy", "apply", "", "ORK", "spiffe://ork.acme.dev/ork/ns/a/orkflow/b/task/c/")
	f.Fuzz(func(t *testing.T, ns, owner, orkflow, task, grant, domain, id string) {
		s := Subject{Namespace: ns, Owner: owner, Orkflow: orkflow, Task: task, Grant: grant}
		if built, err := s.SPIFFEID(domain); err == nil {
			back, d, err := ParseSPIFFEID(built)
			if err != nil || back != s || d != domain {
				t.Fatalf("%q parsed back as %+v in %q (%v), built from %+v in %q", built, back, d, err, s, domain)
			}
			if _, err := s.Ork(); err != nil {
				t.Fatalf("a subject with a SPIFFE ID has no colon form: %v", err)
			}
		}
		if parsed, d, err := ParseSPIFFEID(id); err == nil {
			again, err := parsed.SPIFFEID(d)
			if err != nil || again != id {
				t.Fatalf("%q parsed as %+v, which builds %q (%v)", id, parsed, again, err)
			}
		}
	})
}

// FuzzKeysCopy: whatever bytes the copy file holds, parsing never panics,
// and a copy it accepts holds a document that keeps the store's rules.
func FuzzKeysCopy(f *testing.F) {
	f.Add([]byte(`{"v":1,"fetchedAt":"2026-11-15T12:00:00Z","keys":{"v":1,"sequence":1,"keys":[]}}`))
	f.Add([]byte(`{"v":1,"fetchedAt":"2026-11-15T12:00:00Z","keys":{"v":1,"sequence":1,"keys":[{"kid":"a","alg":"RS256","jwk":{},"publishedAt":"2026-11-15T12:00:00Z"}]}}`))
	f.Add([]byte(`{"v":2}`))
	f.Add([]byte(`{`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		c, err := parseKeysCopy(raw)
		if err != nil {
			return
		}
		if c.FetchedAt.IsZero() {
			t.Fatal("an accepted copy has no fetchedAt")
		}
		if _, err := artifacts.ParsePublishedKeys(mustJSON(t, c.Keys)); err != nil {
			t.Fatalf("an accepted copy's document breaks the store's rules: %v", err)
		}
	})
}

// FuzzCommitResultRequest: a commit key is accepted only for the
// namespace, run, task and item the request names, and the key it maps to
// is always under metadata/attested/, the commit key's own segments.
func FuzzCommitResultRequest(f *testing.F) {
	f.Add("ns", "ork-1", "build", int32(-1), "metadata/commits/ns/ork-1/build/commit.json", "ns", "ork-1", "deploy")
	f.Add("ns", "ork-1", "convert", int32(3), "metadata/commits/ns/ork-1/convert/00003/commit.json", "ns", "ork-2", "convert")
	f.Add("", "ork-1", "build", int32(-1), "metadata/commits/default/ork-1/build/commit.json", "other", "ork-1", "build")
	f.Add("ns", "../x", "build", int32(0), "metadata/commits/../x/build/commit.json", "ns", "x", "build")
	f.Add("ns", "ork/1", "build", int32(-1), "metadata/attested/ns/ork-1/build/commit.json", "ns", "ork-1", "build")
	f.Fuzz(func(t *testing.T, ns, run, taskID string, item int32, key, otherNS, otherRun, otherTask string) {
		req := CommitRequest{Namespace: artifacts.ResolveNamespace(ns), RunID: run, TaskID: taskID, CommitKey: key}
		if item >= 0 {
			req.Item = artifacts.Item{Index: item, Set: true}
		}
		want, _, err := commitCoordinates(req)
		if err != nil || key != want {
			return
		}
		attested, err := artifacts.AttestedCommitKey(key)
		if err != nil {
			t.Fatalf("the attempt's own commit key %q maps to nothing: %v", key, err)
		}
		if attested != artifacts.AttestedPrefix+"/"+key[len(artifacts.CommitPrefix)+1:] {
			t.Fatalf("%q maps to %q, not its own segments under the attested prefix", key, attested)
		}
		other := req
		other.Namespace, other.RunID, other.TaskID = artifacts.ResolveNamespace(otherNS), otherRun, otherTask
		if other.Namespace == req.Namespace && other.RunID == req.RunID && other.TaskID == req.TaskID {
			return
		}
		if theirs, _, err := commitCoordinates(other); err == nil && theirs == key {
			t.Fatalf("%q is the commit key of %s/%s/%s and of %s/%s/%s", key, req.Namespace, req.RunID, req.TaskID, other.Namespace, other.RunID, other.TaskID)
		}
	})
}
