package consent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// roleDef is the fixture of the golden values: an approval, a sleep, a role
// task that needs both, and a task that is upstream of nothing.
func roleDef() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:    "deploy",
		Version: "3",
		Tasks: []*orkstackv1.Task{
			{Id: "approve", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 3600}},
			{Id: "settle", Needs: []string{"approve"}, Sleep: &orkstackv1.SleepSpec{Seconds: 60}},
			{
				Id: "apply", Needs: []string{"settle"}, Pool: "prod",
				Image:   &orkstackv1.ImageRef{Reference: "ghcr.io/acme/apply:1"},
				Command: []string{"apply"},
				Identities: []*orkstackv1.IdentityRef{
					{Name: "vault", Audience: "https://vault.example"},
					{Name: "aws", Audience: "sts.amazonaws.com", TtlSeconds: 900, Refresh: orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT},
				},
				Secrets: []*orkstackv1.SecretRef{
					{Name: "db", Source: "env:DB"},
					{Name: "deploy-key", Grant: "deployer"},
				},
			},
			{Id: "notify", Image: &orkstackv1.ImageRef{Reference: "ghcr.io/acme/notify:1"}},
		},
	}
}

func noChildren(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
	return nil, errors.New("no child expected")
}

func compute(t *testing.T, def *orkstackv1.Orkflow, r Resolver) Asks {
	t.Helper()
	asks, err := Compute(context.Background(), "team", def, r)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	return asks
}

// TestCompute_AskingForNothingIsTheEmptyTable: a definition with no role
// task, awaits and composed tasks included, consents to the frozen empty
// table, so every consent recorded before an Orkflow could ask for an
// identity stays valid.
func TestCompute_AskingForNothingIsTheEmptyTable(t *testing.T) {
	def := roleDef()
	def.Tasks = def.Tasks[:2]
	def.Tasks = append(def.Tasks, &orkstackv1.Task{Id: "child", Uses: "orkflow:lib@1",
		Orkflow: &orkstackv1.OrkflowRef{Name: "lib", Version: "1"}})
	child := &orkstackv1.Orkflow{Name: "lib", Version: "1", Tasks: []*orkstackv1.Task{{Id: "x"}}}
	asks := compute(t, def, func(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
		return child, nil
	})
	if asks.Digest != Empty() || len(asks.Audiences) != 0 || len(asks.Grants) != 0 || !asks.Empty() {
		t.Fatalf("asks = %+v, want the empty table", asks)
	}
	if got := compute(t, &orkstackv1.Orkflow{}, noChildren); got.Digest != Empty() {
		t.Fatalf("an empty definition digests to %s", got.Digest)
	}
}

// TestCompute_Golden pins projection 1's bytes and digest for the fixture.
// These values are frozen by the release that first ships them: a later
// release reproduces them or adds c2:.
func TestCompute_Golden(t *testing.T) {
	asks := compute(t, roleDef(), noChildren)
	const wantBytes = `{"tasks":[{"credentials":[],"grants":[{"ciphertext_sha256":"","grant":"deployer","name":"deploy-key","source":"","unmasked":false}],"id":"apply",` +
		`"identities":[{"audience":"sts.amazonaws.com","name":"aws","refresh":2,"ttl_seconds":900},` +
		`{"audience":"https://vault.example","name":"vault","refresh":0,"ttl_seconds":0}],"join":0,"kind":"container",` +
		`"path":[],"pool":"prod","upstream":[` +
		`{"await":{"callback":false,"timeout_seconds":3600},"id":"approve","join":0,"kind":"awaited","needs":[],"path":[],"uses":null},` +
		`{"await":null,"id":"settle","join":0,"kind":"sleeping","needs":["approve"],"path":[],"uses":null}]}],"v":1}`
	if got := string(asks.canonical); got != wantBytes {
		t.Fatalf("canonical bytes:\n got %s\nwant %s", got, wantBytes)
	}
	const golden = "c1:11776702449828520ead2f647c346d8a468dcef5e303af03131f03d14d685d83"
	if asks.Digest != golden {
		t.Fatalf("digest = %s, want the frozen %s", asks.Digest, golden)
	}
	if !Valid(asks.Digest) {
		t.Fatalf("digest %q is not a c1: digest", asks.Digest)
	}
	if got := strings.Join(asks.Audiences, ","); got != "https://vault.example,sts.amazonaws.com" {
		t.Errorf("audiences = %s", got)
	}
	if got := strings.Join(asks.Grants, ","); got != "deployer" {
		t.Errorf("grants = %s", got)
	}
}

// TestCompute_WhatChangesTheConsent: every member of projection 1 is
// another digest when it changes (workload-identity §3.2), and what it does
// not cover is the same digest.
func TestCompute_WhatChangesTheConsent(t *testing.T) {
	base := compute(t, roleDef(), noChildren).Digest
	task := func(def *orkstackv1.Orkflow, id string) *orkstackv1.Task {
		for _, tk := range def.GetTasks() {
			if tk.GetId() == id {
				return tk
			}
		}
		t.Fatalf("no task %q", id)
		return nil
	}
	for _, tc := range []struct {
		name   string
		edit   func(*orkstackv1.Orkflow)
		differ bool
	}{
		{"the role task's join", func(d *orkstackv1.Orkflow) {
			task(d, "apply").Join = orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE
		}, true},
		{"the role task's pool", func(d *orkstackv1.Orkflow) { task(d, "apply").Pool = "other" }, true},
		{"the role task's id", func(d *orkstackv1.Orkflow) { task(d, "apply").Id = "apply2" }, true},
		{"an audience", func(d *orkstackv1.Orkflow) { task(d, "apply").Identities[0].Audience = "x" }, true},
		{"a token's life", func(d *orkstackv1.Orkflow) { task(d, "apply").Identities[0].TtlSeconds = 60 }, true},
		{"a secret's grant", func(d *orkstackv1.Orkflow) { task(d, "apply").Secrets[1].Grant = "admin" }, true},
		{"a granted secret's source", func(d *orkstackv1.Orkflow) { task(d, "apply").Secrets[1].Source = "vault:secret/data/other" }, true},
		{"a granted secret left unmasked", func(d *orkstackv1.Orkflow) { task(d, "apply").Secrets[1].Unmasked = true }, true},
		{"a granted secret's ciphertext", func(d *orkstackv1.Orkflow) { task(d, "apply").Secrets[1].Ciphertext = "QUJD" }, true},
		{"a credential added", func(d *orkstackv1.Orkflow) { task(d, "apply").Credentials = []*orkstackv1.CredentialRef{cred()} }, true},
		{"a credential's grant", credEdit(func(c *orkstackv1.CredentialRef) { c.Grant = "admin" }), true},
		{"a credential's path", credEdit(func(c *orkstackv1.CredentialRef) { c.Path = "pki/issue/other" }), true},
		{"a credential's params", credEdit(func(c *orkstackv1.CredentialRef) { c.Params["ttl"] = "24h" }), true},
		{"a credential's name", credEdit(func(c *orkstackv1.CredentialRef) { c.Name = "other" }), true},
		{"a credential's refresh", credEdit(func(c *orkstackv1.CredentialRef) { c.Refresh = orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT }), true},
		{"a fan-out", func(d *orkstackv1.Orkflow) {
			task(d, "apply").Each = &orkstackv1.EachSpec{Over: "x"}
		}, true},
		{"the approval turned into a sleep under the same id", func(d *orkstackv1.Orkflow) {
			a := task(d, "approve")
			a.Await, a.Sleep = nil, &orkstackv1.SleepSpec{Seconds: 1}
		}, true},
		{"a shorter approval", func(d *orkstackv1.Orkflow) { task(d, "approve").Await.TimeoutSeconds = 60 }, true},
		{"an approval a callback token gives", func(d *orkstackv1.Orkflow) { task(d, "approve").Await.Callback = true }, true},
		{"a join in between", func(d *orkstackv1.Orkflow) {
			task(d, "settle").Join = orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE
		}, true},
		{"the approval unhooked from the path", func(d *orkstackv1.Orkflow) { task(d, "settle").Needs = nil }, true},
		{"the image", func(d *orkstackv1.Orkflow) { task(d, "apply").Image.Reference = "ghcr.io/acme/apply:2" }, false},
		{"the command", func(d *orkstackv1.Orkflow) { task(d, "apply").Command = []string{"plan"} }, false},
		{"a plain secret", func(d *orkstackv1.Orkflow) { task(d, "apply").Secrets[0].Source = "env:OTHER" }, false},
		{"a task upstream of nothing", func(d *orkstackv1.Orkflow) {
			task(d, "notify").Join = orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE
		}, false},
		{"the order of identities", func(d *orkstackv1.Orkflow) {
			ids := task(d, "apply").Identities
			ids[0], ids[1] = ids[1], ids[0]
		}, false},
		{"the order of tasks", func(d *orkstackv1.Orkflow) {
			d.Tasks[0], d.Tasks[3] = d.Tasks[3], d.Tasks[0]
		}, false},
		{"the version", func(d *orkstackv1.Orkflow) { d.Version = "4" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def, was := roleDef(), base
			if strings.HasPrefix(tc.name, "a credential's") {
				task(def, "apply").Credentials = []*orkstackv1.CredentialRef{cred()}
				was = compute(t, def, noChildren).Digest
			}
			tc.edit(def)
			got := compute(t, def, noChildren).Digest
			if (got != was) != tc.differ {
				t.Fatalf("digest changed = %v, want %v", got != base, tc.differ)
			}
		})
	}
}

// cred is a credential of the role task.
func cred() *orkstackv1.CredentialRef {
	return &orkstackv1.CredentialRef{Name: "pki", Grant: "issuer", Path: "pki/issue/role", Params: map[string]string{"ttl": "1h"}}
}

// credEdit edits the role task's one credential, which the case adds first.
func credEdit(edit func(*orkstackv1.CredentialRef)) func(*orkstackv1.Orkflow) {
	return func(d *orkstackv1.Orkflow) {
		for _, tk := range d.GetTasks() {
			for _, c := range tk.GetCredentials() {
				edit(c)
			}
		}
	}
}

// composedDef composes shared/lib@1 after the approval; lib's task holds an
// identity.
func composedDef() (*orkstackv1.Orkflow, map[string]*orkstackv1.Orkflow) {
	parent := &orkstackv1.Orkflow{Name: "release", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "approve", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 600}},
		{Id: "run-lib", Needs: []string{"approve"}, Uses: "orkflow:shared/lib@1",
			Orkflow: &orkstackv1.OrkflowRef{Namespace: "shared", Name: "lib", Version: "1"}},
	}}
	lib := &orkstackv1.Orkflow{Name: "lib", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "push", Identities: []*orkstackv1.IdentityRef{{Name: "reg", Audience: "registry"}}},
	}}
	lib2 := proto.Clone(lib).(*orkstackv1.Orkflow)
	lib2.Version = "2"
	return parent, map[string]*orkstackv1.Orkflow{"shared/lib@1": lib, "shared/lib@2": lib2}
}

func mapResolver(m map[string]*orkstackv1.Orkflow) Resolver {
	return func(_ context.Context, owner, name, version string) (*orkstackv1.Orkflow, error) {
		return m[owner+"/"+name+"@"+version], nil
	}
}

// TestCompute_ComposedChild: a child's role task is covered with the
// composing task and what it reaches in its parent upstream, and a child
// swapped for another version is another consent.
func TestCompute_ComposedChild(t *testing.T) {
	parent, children := composedDef()
	asks := compute(t, parent, mapResolver(children))
	if asks.Empty() || strings.Join(asks.Audiences, ",") != "registry" {
		t.Fatalf("asks = %+v, want the child's identity", asks)
	}
	b := string(asks.canonical)
	for _, want := range []string{
		`"id":"push"`, `"path":["run-lib"]`,
		`"id":"run-lib","join":0,"kind":"composed","needs":["approve"],"path":[],"uses":{"name":"lib","namespace":"shared","version":"1"}`,
		`"id":"approve"`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("canonical bytes lack %s:\n%s", want, b)
		}
	}
	swapped := proto.Clone(parent).(*orkstackv1.Orkflow)
	swapped.Tasks[1].Orkflow.Version, swapped.Tasks[1].Uses = "2", "orkflow:shared/lib@2"
	if compute(t, swapped, mapResolver(children)).Digest == asks.Digest {
		t.Fatal("a child swapped for another version keeps the digest")
	}
}

// TestCompute_OwnNamespaceChild: a reference with no namespace resolves in
// the namespace of the definition that wrote it.
func TestCompute_OwnNamespaceChild(t *testing.T) {
	parent, children := composedDef()
	parent.Tasks[1].Orkflow.Namespace, parent.Tasks[1].Uses = "", "orkflow:lib@1"
	children["team/lib@1"] = children["shared/lib@1"]
	asks := compute(t, parent, mapResolver(children))
	if !strings.Contains(string(asks.canonical), `"uses":{"name":"lib","namespace":"team","version":"1"}`) {
		t.Fatalf("own-namespace child not recorded under its owner:\n%s", asks.canonical)
	}
}

// TestCompute_Refusals: a child that cannot be read is an error, never a
// smaller table; so is a missing child and a chain past the depth bound.
func TestCompute_Refusals(t *testing.T) {
	parent, _ := composedDef()
	outage := errors.New("store down")
	if _, err := Compute(context.Background(), "team", parent, func(context.Context, string, string, string) (*orkstackv1.Orkflow, error) {
		return nil, outage
	}); !errors.Is(err, outage) {
		t.Fatalf("an unreadable child: err = %v, want the outage", err)
	}
	if _, err := Compute(context.Background(), "team", parent, mapResolver(nil)); !errors.Is(err, ErrChildMissing) {
		t.Fatalf("a missing child: err = %v, want ErrChildMissing", err)
	}
	self := &orkstackv1.Orkflow{Name: "loop", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "again", Uses: "orkflow:loop@1", Orkflow: &orkstackv1.OrkflowRef{Name: "loop", Version: "1"}},
	}}
	if _, err := Compute(context.Background(), "team", self, mapResolver(map[string]*orkstackv1.Orkflow{"team/loop@1": self})); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("a cycle: err = %v, want ErrTooDeep", err)
	}
}

// TestDifference names what changed between a consent's short list and
// what the Orkflow asks for now.
func TestDifference(t *testing.T) {
	now := Asks{Digest: "c1:" + strings.Repeat("b", 64), Audiences: []string{"a", "b"}, Grants: []string{"deployer"}}
	for _, tc := range []struct {
		name  string
		given *orkstackv1.ConsentView
		want  []string
	}{
		{"none given", nil, []string{"no consent was given", "audiences a, b", "grants deployer"}},
		{"more asked", &orkstackv1.ConsentView{Digest: "c1:" + strings.Repeat("a", 64), Audiences: []string{"a"}},
			[]string{"now also asks for audience b", "now also asks for grant deployer"}},
		{"fewer asked", &orkstackv1.ConsentView{Digest: "c1:" + strings.Repeat("a", 64), Audiences: []string{"a", "b", "c"}, Grants: []string{"deployer"}},
			[]string{"no longer asks for audience c"}},
		{"same names, other tasks", &orkstackv1.ConsentView{Digest: "c1:" + strings.Repeat("a", 64), Audiences: []string{"a", "b"}, Grants: []string{"deployer"}},
			[]string{"the tasks that hold them, or what runs before those tasks, changed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Difference(tc.given, now)
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("Difference = %q, want it to contain %q", got, w)
				}
			}
		})
	}
}
