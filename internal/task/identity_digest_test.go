package task

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func digestFixture(t *testing.T) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	with, err := structpb.NewStruct(map[string]any{"region": "eu-west-1", "count": 3})
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.ExecuteTaskInput{
		Image:   &orkstackv1.ImageRef{Pinned: "ghcr.io/acme/deploy@sha256:" + strings.Repeat("a", 64)},
		Command: []string{"deploy", "--now"},
		Env:     map[string]string{"B": "2", "A": "1"},
		With:    with,
		Secrets: []*orkstackv1.SecretRef{{Name: "db", Source: "env:DB_PASSWORD"}},
		Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE,
		Identity: &orkstackv1.IdentityContext{DigestVersion: 1, Identities: []*orkstackv1.IdentityRef{
			{Name: "aws", Audience: "sts.amazonaws.com", TtlSeconds: 900, Refresh: orkstackv1.RefreshPolicy_REFRESH_POLICY_REQUIRED}},
			Credentials: []*orkstackv1.CredentialRef{{Name: "cert", Grant: "pki", Path: "pki/issue/payments",
				Params: map[string]string{"ttl": "12h", "common_name": "payments.internal"}, Refresh: orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT}}},
		Devices: []*orkstackv1.ProfileRef{{Name: "gpu-a", IdentityDigest: strings.Repeat("e", 64)}},
		// Not in version 1's list: never hashed.
		Pool: "main", RunId: "ork-1", TaskId: "deploy",
	}
}

// TestIdentitySpecDigest_Golden: version 1's digest of the fixture is fixed
// for good; every later release must reproduce it.
func TestIdentitySpecDigest_Golden(t *testing.T) {
	digest, members, err := IdentitySpecDigest(1, IdentitySpecOfInput(digestFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 11 {
		t.Fatalf("members %v", members)
	}
	const golden = "d79e4b2ffbc1434d7fdc6e1453155c07f4159459aa37a0d9537fd906d49eb4ef"
	if digest != golden {
		t.Fatalf("version 1's digest of the fixture is %s, want %s", digest, golden)
	}
}

// TestIdentitySpecDigest_Members: changing one listed field changes its own
// member's digest and no other; a field not listed changes nothing; an
// unknown version is refused.
func TestIdentitySpecDigest_Members(t *testing.T) {
	_, base, err := IdentitySpecDigest(1, IdentitySpecOfInput(digestFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	for member, edit := range map[string]func(*orkstackv1.ExecuteTaskInput){
		"image":             func(in *orkstackv1.ExecuteTaskInput) { in.Image.Pinned += "x" },
		"command":           func(in *orkstackv1.ExecuteTaskInput) { in.Command = append(in.Command, "-v") },
		"env":               func(in *orkstackv1.ExecuteTaskInput) { in.Env["A"] = "9" },
		"with":              func(in *orkstackv1.ExecuteTaskInput) { in.With.Fields["region"] = structpb.NewStringValue("us-east-1") },
		"secrets":           func(in *orkstackv1.ExecuteTaskInput) { in.Secrets[0].Unmasked = true },
		"network":           func(in *orkstackv1.ExecuteTaskInput) { in.Network = orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED },
		"run_as_orker_user": func(in *orkstackv1.ExecuteTaskInput) { in.RunAsOrkerUser = true },
		"identities":        func(in *orkstackv1.ExecuteTaskInput) { in.Identity.Identities[0].TtlSeconds = 600 },
		"credentials":       func(in *orkstackv1.ExecuteTaskInput) { in.Identity.Credentials[0].Path = "pki/issue/other" },
		"devices":           func(in *orkstackv1.ExecuteTaskInput) { in.Devices[0].Name = "gpu-b" },
	} {
		in := digestFixture(t)
		edit(in)
		_, got, err := IdentitySpecDigest(1, IdentitySpecOfInput(in))
		if err != nil {
			t.Fatal(err)
		}
		for name, d := range got {
			if changed := d != base[name]; changed != (name == member) {
				t.Errorf("editing %s: member %s changed = %v", member, name, changed)
			}
		}
	}
	in := digestFixture(t)
	in.Pool, in.RunId = "gpu", "ork-2"
	if _, got, _ := IdentitySpecDigest(1, IdentitySpecOfInput(in)); !equalMembers(got, base) {
		t.Error("a field not in version 1's list changed the digest")
	}
	if _, _, err := IdentitySpecDigest(2, IdentitySpecOfInput(in)); err == nil {
		t.Error("an unknown version was digested")
	}
}

func equalMembers(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestIdentitySpecDigest_EveryCredentialFieldIsBound: each field of a
// credential that version 1 lists moves the credentials member, and the
// order of its params' keys does not.
func TestIdentitySpecDigest_EveryCredentialFieldIsBound(t *testing.T) {
	_, base, err := IdentitySpecDigest(1, IdentitySpecOfInput(digestFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*orkstackv1.CredentialRef){
		"name":    func(c *orkstackv1.CredentialRef) { c.Name = "other" },
		"grant":   func(c *orkstackv1.CredentialRef) { c.Grant = "other" },
		"path":    func(c *orkstackv1.CredentialRef) { c.Path = "" },
		"params":  func(c *orkstackv1.CredentialRef) { c.Params["ttl"] = "1h" },
		"refresh": func(c *orkstackv1.CredentialRef) { c.Refresh = orkstackv1.RefreshPolicy_REFRESH_POLICY_REQUIRED },
	} {
		in := digestFixture(t)
		edit(in.Identity.Credentials[0])
		_, got, err := IdentitySpecDigest(1, IdentitySpecOfInput(in))
		if err != nil {
			t.Fatal(err)
		}
		if got["credentials"] == base["credentials"] {
			t.Errorf("a credential's %s did not move the credentials member", name)
		}
	}
	in := digestFixture(t)
	in.Identity.Credentials[0].Params = map[string]string{"common_name": "payments.internal", "ttl": "12h"}
	if _, got, _ := IdentitySpecDigest(1, IdentitySpecOfInput(in)); !equalMembers(got, base) {
		t.Error("the order a credential's params were built in changed the digest")
	}
}

// TestIdentitySpecOfTask_IsTheInputsProjection: a task's projection from
// its definition is the one from an input carrying the same fields.
func TestIdentitySpecOfTask_IsTheInputsProjection(t *testing.T) {
	in := digestFixture(t)
	task := &orkstackv1.Task{Id: "deploy", Image: in.GetImage(), Command: in.GetCommand(), Env: in.GetEnv(), With: in.GetWith(),
		Secrets: in.GetSecrets(), Network: in.GetNetwork(), RunAsOrkerUser: in.GetRunAsOrkerUser(),
		Identities: in.GetIdentity().GetIdentities(), Credentials: in.GetIdentity().GetCredentials(), TimeoutSeconds: 99,
		Devices: []string{"gpu-a"}}
	a, _, err := IdentitySpecDigest(1, IdentitySpecOfTask(task))
	if err != nil {
		t.Fatal(err)
	}
	if b, _, _ := IdentitySpecDigest(1, IdentitySpecOfInput(in)); a != b {
		t.Fatalf("the definition's digest %s is not the input's %s", a, b)
	}
}

// TestIdentitySpecDigest_DevicesBindNamesNotPins: the devices member is the
// profiles' names in order; what the run pinned of each (its identity
// digest, its contracts) is the profile check's, not the digest's.
func TestIdentitySpecDigest_DevicesBindNamesNotPins(t *testing.T) {
	_, base, err := IdentitySpecDigest(1, IdentitySpecOfInput(digestFixture(t)))
	if err != nil {
		t.Fatal(err)
	}
	pinned := digestFixture(t)
	pinned.Devices[0].IdentityDigest = strings.Repeat("f", 64)
	pinned.Devices[0].Contracts = []*orkstackv1.ProfileContractPin{{Label: "v2"}}
	if _, got, _ := IdentitySpecDigest(1, IdentitySpecOfInput(pinned)); !equalMembers(got, base) {
		t.Error("a profile's pinned identity changed the digest")
	}
	more := digestFixture(t)
	more.Devices = append(more.Devices, &orkstackv1.ProfileRef{Name: "fpga"})
	if _, got, _ := IdentitySpecDigest(1, IdentitySpecOfInput(more)); got["devices"] == base["devices"] {
		t.Error("another profile left the devices member unchanged")
	}
}
