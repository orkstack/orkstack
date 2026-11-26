package main

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func writeFileAt(t *testing.T, path, body string) string {
	t.Helper()
	must.OK(t, os.MkdirAll(filepath.Dir(path), 0o755))
	must.OK(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// unsignedJWT is a token of exp, whose signature nothing here reads.
func unsignedJWT(exp int64) string {
	part := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	return part(`{"alg":"none"}`) + "." + part(fmt.Sprintf(`{"exp":%d}`, exp)) + "." + part("sig")
}

// identityFixture is the task-run fixture with `consume` declaring two
// identities and a credential, as a definition an engine that admits them
// stores.
func identityFixture(t *testing.T) (*taskRunFixture, string) {
	t.Helper()
	fx := newTaskRunFixture(t)
	wf := proto.Clone(fx.wf).(*orkstackv1.Orkflow)
	wf.Version = "9"
	for _, task := range wf.GetTasks() {
		if task.GetId() == "consume" {
			task.Identities = []*orkstackv1.IdentityRef{{Name: "aws", Audience: "sts.amazonaws.com"}, {Name: "gcp", Audience: "//iam"}}
			task.Credentials = []*orkstackv1.CredentialRef{{Name: "db", Grant: "vault-app"}}
		}
	}
	if _, _, err := fx.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	const run = "ork-1790000000000000000-4294967299"
	fx.seedRun(t, run, func(r *orkstackv1.Run) { r.OrkflowVersion = "9" })
	return fx, run
}

// A local run's identity and credential reach the container as an attempt's
// do: files through `current`, named in task.json, masked in the log; one left
// out is in none of them.
func TestTaskRun_SuppliedIdentitiesReachTheContainer(t *testing.T) {
	fx, run := identityFixture(t)
	dir := t.TempDir()
	exp := time.Unix(time.Now().Add(2*time.Hour).Unix(), 0)
	token := unsignedJWT(exp.Unix())
	tokenFile := writeFileAt(t, filepath.Join(dir, "aws.token"), token+"\n")
	writeFileAt(t, filepath.Join(dir, "db", "password"), "hunter2-not-a-real-one\n")
	writeFileAt(t, filepath.Join(dir, "db", "user"), "app-user-name")
	writeFileAt(t, filepath.Join(dir, "db", "config.json"), `{"nested":{"key":"inner-value-of-a-file"}}`)

	type taskJSON struct {
		Paths       map[string]string `json:"paths"`
		Identities  []map[string]any  `json:"identities"`
		Credentials []map[string]any  `json:"credentials"`
	}
	var seen taskJSON
	rt := &fakeTaskRuntime{script: func(spec orker.ContainerSpec, stdout, stderr io.Writer) int32 {
		raw, err := os.ReadFile(filepath.Join(mountSource(spec, orker.ContainerInput), "task.json"))
		must.OK(t, err)
		must.OK(t, json.Unmarshal(raw, &seen))
		identity, credentials := mountSource(spec, "/orkstack/identity"), mountSource(spec, "/orkstack/credentials")
		requireFileBytes(t, filepath.Join(identity, "aws", "current", "token"), token)
		requireFileBytes(t, filepath.Join(identity, "aws", "token"), token)
		expiration, err := os.ReadFile(filepath.Join(identity, "aws", "expiration"))
		must.OK(t, err)
		if at, err := time.Parse(time.RFC3339, strings.TrimSpace(string(expiration))); err != nil || at.Sub(exp).Abs() > 2*time.Second {
			t.Errorf("expiration = %q, want about %s", expiration, exp)
		}
		if _, err := os.Stat(filepath.Join(identity, "gcp")); !os.IsNotExist(err) {
			t.Errorf("the omitted identity's directory: %v", err)
		}
		requireFileBytes(t, filepath.Join(credentials, "db", "current", "password"), "hunter2-not-a-real-one\n")
		requireFileBytes(t, filepath.Join(credentials, "db", "user"), "app-user-name")
		fmt.Fprintf(stdout, "token %s password hunter2-not-a-real-one inner inner-value-of-a-file\n", token)
		return succeedWith(`{"ok":true}`, "")(spec, stdout, stderr)
	}}
	got := fx.runTaskRun(rt, "--from-run", run, "--task", "consume", "--identity", "aws=file:"+tokenFile, "--omit-identity", "gcp",
		"--credential", "db=dir:"+filepath.Join(dir, "db")).requireExit(t, exitOK)
	if strings.Contains(got.stdout+got.stderr, token) || strings.Contains(got.stdout+got.stderr, "hunter2") ||
		strings.Contains(got.stdout+got.stderr, "inner-value") {
		t.Fatalf("a supplied value reached the output:\n%s\n%s", got.stdout, got.stderr)
	}
	if !strings.Contains(got.stdout, "token *** password *** inner ***") {
		t.Fatalf("the container's line is not masked:\n%s", got.stdout)
	}
	if len(seen.Identities) != 1 || seen.Identities[0]["name"] != "aws" || seen.Identities[0]["path"] != "/orkstack/identity/aws/token" ||
		len(seen.Credentials) != 1 || seen.Credentials[0]["name"] != "db" || seen.Credentials[0]["provider"] != orker.LocalCredentialProvider ||
		seen.Paths["identity"] != "/orkstack/identity" || seen.Paths["credentials"] != "/orkstack/credentials" {
		t.Fatalf("task.json = %+v", seen)
	}
	requireNoSession(t, fx.workdir)
}

// Omitted, they leave no trace: no mount, and no key of task.json.
func TestTaskRun_OmittedIdentitiesLeaveNoTrace(t *testing.T) {
	fx, run := identityFixture(t)
	rt := &fakeTaskRuntime{script: func(spec orker.ContainerSpec, stdout, stderr io.Writer) int32 {
		raw, err := os.ReadFile(filepath.Join(mountSource(spec, orker.ContainerInput), "task.json"))
		must.OK(t, err)
		if strings.Contains(string(raw), "identit") || strings.Contains(string(raw), "credentials") ||
			mountSource(spec, "/orkstack/identity") != "" || mountSource(spec, "/orkstack/credentials") != "" {
			t.Errorf("task.json or the mounts name what was omitted:\n%s", raw)
		}
		return succeedWith(`{"ok":true}`, "")(spec, stdout, stderr)
	}}
	fx.runTaskRun(rt, "--from-run", run, "--task", "consume", "--omit-identity", "aws", "--omit-identity", "gcp", "--omit-credential", "db").
		requireExit(t, exitOK)
}

// Each declared one is supplied or omitted, or the run is refused by its name
// before anything is created.
func TestTaskRun_AnUnaccountedIdentityIsRefusedByName(t *testing.T) {
	fx, run := identityFixture(t)
	token := writeFileAt(t, filepath.Join(t.TempDir(), "t"), "opaque")
	for name, tc := range map[string]struct {
		args []string
		want []string
	}{
		"none":       {nil, []string{`declares identity "aws"`, "--identity aws=file:PATH", "--omit-identity aws", `declares identity "gcp"`, `declares credential "db"`, "--credential db=dir:PATH"}},
		"undeclared": {[]string{"--identity", "azure=file:" + token, "--omit-credential", "cache"}, []string{`no identity named "azure" (declared: aws, gcp)`, `no credential named "cache" (declared: db)`}},
	} {
		t.Run(name, func(t *testing.T) {
			rt := &fakeTaskRuntime{}
			got := fx.runTaskRun(rt, append([]string{"--from-run", run, "--task", "consume"}, tc.args...)...).requireExit(t, exitError)
			for _, want := range tc.want {
				if !strings.Contains(got.stderr, want) {
					t.Errorf("stderr does not say %q:\n%s", want, got.stderr)
				}
			}
			if rt.created() != 0 {
				t.Error("a container was created")
			}
		})
	}
	t.Run("--render-only starts nothing", func(t *testing.T) {
		fx.runTaskRun(&fakeTaskRuntime{}, "--from-run", run, "--task", "consume", "--render-only").requireExit(t, exitOK)
	})
}

func TestTaskRun_RefusesMalformedIdentityFlags(t *testing.T) {
	dir := t.TempDir()
	token := writeFileAt(t, filepath.Join(dir, "token"), "opaque\n")
	twoLines := writeFileAt(t, filepath.Join(dir, "two"), "a\nb\n")
	expired := writeFileAt(t, filepath.Join(dir, "expired"), unsignedJWT(1))
	empty := writeFileAt(t, filepath.Join(dir, "empty"), "\n")
	must.OK(t, os.Mkdir(filepath.Join(dir, "none"), 0o755))
	nested := filepath.Join(dir, "nested")
	writeFileAt(t, filepath.Join(nested, "sub", "file"), "x")
	part := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	badExp := writeFileAt(t, filepath.Join(dir, "badexp"), part("{}")+"."+part(`{"exp":"soon"}`)+"."+part("s"))
	fraction := writeFileAt(t, filepath.Join(dir, "fraction"), part("{}")+"."+part(`{"exp":1.5}`)+"."+part("s"))
	many := filepath.Join(dir, "many")
	for i := range 17 {
		writeFileAt(t, filepath.Join(many, fmt.Sprintf("f%d", i)), "x")
	}
	at := func(source string) []string { return []string{source} }
	for _, tc := range []struct {
		opts taskRunOptions
		want string
	}{
		{taskRunOptions{identities: at("aws")}, "want NAME=file:PATH"},
		{taskRunOptions{identities: at("aws=env:TOKEN")}, "want NAME=file:PATH"},
		{taskRunOptions{identities: at("aws=file:" + filepath.Join(dir, "gone"))}, "no such file"},
		{taskRunOptions{identities: at("aws=file:" + dir)}, "not a regular file"},
		{taskRunOptions{identities: at("aws=file:" + twoLines)}, "one token on one line"},
		{taskRunOptions{identities: at("aws=file:" + empty)}, "one token on one line"},
		{taskRunOptions{identities: at("aws=file:" + expired)}, "expired at 1970-01-01"},
		{taskRunOptions{identities: at("aws=file:" + badExp)}, "exp is no time"},
		{taskRunOptions{identities: at("aws=file:" + fraction)}, "expired at 1970-01-01"},
		{taskRunOptions{credentials: at("db=dir:" + many)}, "more than 16 files"},
		{taskRunOptions{identities: []string{"aws=file:" + token, "aws=file:" + token}}, "given twice"},
		{taskRunOptions{identities: at("aws=file:" + token), omitIdentities: at("aws")}, "both supplied and omitted"},
		{taskRunOptions{identities: at("aws=file:" + token), renderOnly: true}, "--identity has no effect with --render-only"},
		{taskRunOptions{credentials: at("db=file:" + token)}, "want NAME=dir:PATH"},
		{taskRunOptions{credentials: at("db=dir:" + filepath.Join(dir, "gone"))}, "no such file"},
		{taskRunOptions{credentials: at("db=dir:" + filepath.Join(dir, "none"))}, "holds no file"},
		{taskRunOptions{credentials: at("db=dir:" + nested)}, "sub is not a regular file"},
	} {
		tc.opts.task, tc.opts.fromRun = "consume", "ork-x"
		if _, err := parseTaskRunFlags(&tc.opts); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: %v, want %q", tc.opts, err, tc.want)
		}
	}
}

// A file this machine's own credentials are in is refused, whichever way it is
// reached: named itself, or one of a directory's, as --secret refuses it; and at
// each refresh, not at the first read alone.
func TestTaskRun_RefusesTheMachinesOwnCredentials(t *testing.T) {
	dir := t.TempDir()
	own := writeFileAt(t, filepath.Join(dir, "creds", "credentials"), "own")
	writeFileAt(t, filepath.Join(dir, "creds", "other"), "other")
	link := filepath.Join(dir, "link")
	plain := writeFileAt(t, filepath.Join(dir, "plain"), "plain")
	must.OK(t, os.Symlink(plain, link))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", own)
	for _, opts := range []taskRunOptions{
		{identities: []string{"aws=file:" + own}},
		{credentials: []string{"db=dir:" + filepath.Dir(own)}},
	} {
		opts.task, opts.fromRun = "consume", "ork-x"
		if _, err := parseTaskRunFlags(&opts); err == nil {
			t.Errorf("%+v is accepted", opts)
		}
	}
	opts := taskRunOptions{task: "consume", fromRun: "ork-x", identities: []string{"aws=file:" + link}}
	parsed, err := parseTaskRunFlags(&opts)
	must.OK(t, err)
	local, _ := parsed.identity.local(&orkstackv1.Task{Identities: []*orkstackv1.IdentityRef{{Name: "aws"}}})
	ask := &orkstackv1.MintAttemptTokensRequest{Tokens: []*orkstackv1.TokenRequest{{Name: "aws"}}}
	if _, err := local.Mint(context.Background(), ask); err != nil {
		t.Fatalf("the positive control: %v", err)
	}
	must.OK(t, os.Symlink(own, link+".own"))
	must.OK(t, os.Rename(link+".own", link))
	if resp, err := local.Mint(context.Background(), ask); err == nil {
		t.Fatalf("a path that came to lead to the machine's own credentials is read: %v", resp)
	}
}

// A token's file is read again at each refresh, so what replaces it there
// reaches the container; one that names no expiry lives an hour at a time.
func TestSuppliedIdentities_ReadTheFileAtEachRefresh(t *testing.T) {
	path := writeFileAt(t, filepath.Join(t.TempDir(), "token"), "first")
	s := suppliedIdentities{tokens: map[string]string{"aws": path}, policy: orker.SecurityPolicyFromEnv()}
	local, any := s.local(&orkstackv1.Task{Identities: []*orkstackv1.IdentityRef{{Name: "aws", Audience: "a"}}})
	if !any || len(local.Identities) != 1 {
		t.Fatalf("local = %+v, %v", local, any)
	}
	ask := &orkstackv1.MintAttemptTokensRequest{Tokens: []*orkstackv1.TokenRequest{{Name: "aws", Audience: "a"}}}
	for _, want := range []string{"first", "second"} {
		writeFileAt(t, path, want)
		resp, err := local.Mint(context.Background(), ask)
		must.OK(t, err)
		if got := resp.GetTokens()[0]; got.GetToken() != want || got.GetExpiresInSeconds() != 3600 {
			t.Fatalf("token = %v, want %q for an hour", got, want)
		}
	}
	if notes := s.notes(time.Now()); len(notes) != 1 || !strings.Contains(notes[0], "--identity aws: its token names no expiry") {
		t.Fatalf("notes = %q", notes)
	}
	writeFileAt(t, path, unsignedJWT(time.Now().Add(time.Hour).Unix()))
	if notes := s.notes(time.Now()); len(notes) != 0 {
		t.Fatalf("notes of a token that says when it expires = %q", notes)
	}
	must.OK(t, os.Rename(path, path+".gone"))
	if _, err := local.Mint(context.Background(), ask); err == nil || !strings.Contains(err.Error(), "--identity aws") {
		t.Fatalf("Mint of a file that is gone = %v", err)
	}
}

// The flags are for a build that runs task identity; until this one does they
// are in no help and no reference.
func TestTaskRun_IdentityFlagsAreHiddenBehindTheGate(t *testing.T) {
	cmd := newTaskRunCommand(&globalFlags{})
	for _, name := range []string{"identity", "omit-identity", "credential", "omit-credential"} {
		if f := cmd.Flags().Lookup(name); f == nil || f.Hidden == dsl.TaskIdentityAvailable() {
			t.Errorf("--%s: %+v, with task identity available %v", name, f, dsl.TaskIdentityAvailable())
		}
	}
}
