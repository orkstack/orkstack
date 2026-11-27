//go:build integration

package main

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/listenaddr"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// tokenShape is a JWT: what must be in no log, and is kept out of this
// test's own evidence whatever it finds.
var tokenShape = regexp.MustCompile(`ey[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`)

const (
	identityE2EIssuer   = "https://ork-id.example.test"
	identityE2EAudience = "sts.example.test"
)

// identityStack is a deployment that issues task identity, as an operator
// runs one: an engine that enforces authentication and an Orker with its own
// credential and a scoped storage credential, the proof's own binaries
// (testbin.Path), and orkctl as an admin. Nothing in it is in-process.
type identityStack struct {
	t      *testing.T
	ctx    context.Context
	root   string
	orkctl string
	token  string
	addr   string
	proxy  *s3scope.Proxy
	log    strings.Builder
	// engine is a replica's environment, and the store its replicas and no
	// other process reach it through. temporal is a client of the
	// deployment's namespace, as whoever may write to it holds.
	env       []string
	store     *s3scope.Proxy
	temporal  client.Client
	namespace string
	first     launched
	// parent is the whole test's, for a process that outlives a subtest.
	parent *testing.T
}

func newIdentityStack(t *testing.T, more ...string) *identityStack {
	t.Helper()
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireObjectStoreForIdempotency(t, cfg)
	address := temporalAddress()
	requireTemporal(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	s := &identityStack{t: t, parent: t, ctx: ctx, root: repoRoot(t), orkctl: testbin.Path(t, "./cmd/orkctl")}
	t.Cleanup(func() { proof.Write(t, "orkctl", "identity-submit", []byte(s.log.String())) })

	// The issuer of the admin's and the Orker's credentials, and the
	// engine's signing key.
	dir := t.TempDir()
	devtoken := testbin.Path(t, "./test/tools/devtoken")
	run := func(args ...string) {
		t.Helper()
		if _, stderr, code := runBinary(ctx, t, devtoken, s.root, args...); code != 0 {
			t.Fatalf("devtoken %v: exit %d: %s", args, code, stderr)
		}
	}
	orkerID := proof.OrkerID("orker-identity-submit")
	s.token = filepath.Join(dir, "admin.token")
	orkerToken := filepath.Join(dir, "orker.token")
	run("init", "-dir", dir)
	run("sign", "-dir", dir, "-iss", "orkstack-dev", "-aud", "orkstack", "-sub", "admin", "-ttl", "1h", "-out", s.token)
	run("sign", "-dir", dir, "-iss", "orkstack-dev", "-aud", "orkstack", "-sub", "orker", "-claim", "orker_id="+orkerID, "-ttl", "1h", "-out", orkerToken)

	queue, namespace := "ork-identity-"+stamp, isolatedNamespace()
	temporal := dialIsolatedNamespace(ctx, t, address, namespace)
	s.temporal, s.namespace = temporal, namespace
	s.store = s3scope.Open(t, cfg.S3Endpoint, cfg.S3Bucket)
	s.env = []string{
		"ORK_HTTP_ADDR=" + listenaddr.Loopback,
		"ORK_TEMPORAL_ADDRESS=" + address,
		"ORK_TEMPORAL_NAMESPACE=" + namespace,
		"ORK_ORKER_QUEUE=" + queue,
		"ORK_ARTIFACT_BACKEND=s3",
		"ORK_S3_ENDPOINT=" + s.store.URL,
		"ORK_AUTH_MODE=enforce",
		"ORK_AUTH_ADMINS=claims:dev:sub=admin",
		"ORK_AUTH_ORKERS=orkers:\n  - subject: claims:dev:sub=orker\n    pool: main\n",
		"ORK_AUTH_ISSUERS=issuers:\n  dev:\n    issuer: orkstack-dev\n    kind: service\n    dedicated: true\n    audiences:\n      - orkstack\n" +
			"    algorithms:\n      - ES256\n    max_lifetime: 2h\n    orker_id_claim: orker_id\n    jwks_file: " + filepath.Join(dir, "dev-issuer.jwks.json") + "\n",
		"ORK_IDENTITY_ISSUER=" + identityE2EIssuer,
		"ORK_IDENTITY_SIGNING_KEY_FILES=" + filepath.Join(dir, "engine-signing.key"),
		"ORK_IDENTITY_SIGNING_ALGORITHM=ES256",
		"ORK_IDENTITY_SIGNING_VERIFY_PUBLISHED=false",
	}
	s.env = append(s.env, more...)
	s.first = s.replica("ork")
	s.proxy = s3scope.New(t, cfg.S3Endpoint, cfg.S3Bucket, s3scope.OrkerPolicy())
	startOrkerWithEnv(ctx, t, temporal, testbin.Path(t, "./cmd/orker"), address, namespace, queue,
		"ORK_ORKER_ID="+orkerID,
		"ORK_S3_ENDPOINT="+s.proxy.URL,
		"ORK_ORKER_ENGINE_URL=http://"+s.addr,
		"ORK_ORKER_ENGINE_AUTH_METHOD=token_file",
		"ORK_ORKER_ENGINE_AUTH_TOKEN_FILE="+orkerToken,
	)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporal, queue, queue)
	s.await("the Orker to serve task identity", func() (bool, string) {
		d, err := starter.DescribeOrkerByID(ctx, orkerID)
		if err != nil {
			return false, err.Error()
		}
		return d.GetIdentitySupported(), fmt.Sprintf("%q, store scope %v %q", d.GetIdentityUnsupportedReason(), d.GetStoreScope(), d.GetStoreScopeDetail())
	})
	return s
}

// replica starts another engine, which the stack's orkctl then asks.
func (s *identityStack) replica(name string) launched {
	s.t.Helper()
	p := launch(s.ctx, s.parent, name, testbin.Path(s.t, "./cmd/ork"), s.env...)
	s.addr = p.api(s.ctx, s.t)
	s.await(name+" to answer its admin", func() (bool, string) {
		out, stderr, code := s.try("whoami")
		return code == 0, out + stderr
	})
	return p
}

// await polls ok every second for at most three minutes.
func (s *identityStack) await(what string, ok func() (bool, string)) {
	s.t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		done, saw := ok()
		if done {
			return
		}
		if time.Now().After(deadline) || s.ctx.Err() != nil {
			s.t.Fatalf("waited for %s; last saw %s", what, saw)
		}
		time.Sleep(time.Second)
	}
}

func (s *identityStack) try(args ...string) (string, string, int) {
	s.t.Helper()
	full := append(slices.Clone(args), "--server", "http://"+s.addr)
	stdout, stderr, code := runBinaryWithEnv(s.ctx, s.t, s.orkctl, s.root, []string{"ORK_TOKEN_FILE=" + s.token}, full...)
	fmt.Fprintf(&s.log, "$ orkctl %s\n%s(exit %d)\n\n", strings.Join(args, " "), tokenShape.ReplaceAllString(stdout+stderr, "<token>"), code)
	return stdout, stderr, code
}

func (s *identityStack) cli(args ...string) string {
	s.t.Helper()
	stdout, stderr, code := s.try(args...)
	if code != 0 {
		s.t.Fatalf("orkctl %s exit %d: %s", strings.Join(args, " "), code, stderr)
	}
	return stdout
}

func (s *identityStack) create(namespace, name, source string) {
	s.t.Helper()
	path := filepath.Join(s.t.TempDir(), name+".ork.yaml")
	must.OK(s.t, os.WriteFile(path, []byte(source), 0o644))
	s.cli("-n", namespace, "orkflow", "create", path)
}

// run submits name and waits for it to end, answering the finished run.
func (s *identityStack) run(namespace, name string) *orkstackv1.Run {
	s.t.Helper()
	id := parseRunID(s.t, s.cli("-n", namespace, "run", "submit", name))
	s.try("-n", namespace, "run", "watch", id)
	return s.get(namespace, id)
}

func (s *identityStack) get(namespace, id string) *orkstackv1.Run {
	s.t.Helper()
	return decodeRun(s.t, s.cli("-n", namespace, "run", "get", id, "--json"))
}

// artifact is what task wrote as its artifact name.
func (s *identityStack) artifact(namespace, runID, task, name string) string {
	s.t.Helper()
	path := filepath.Join(s.t.TempDir(), "artifact")
	s.cli("-n", namespace, "run", "artifact", "download", runID, "--task", task, "--kind", "output", "--name", name, "--output", path)
	raw, err := os.ReadFile(path)
	must.OK(s.t, err)
	return string(raw)
}

// claims is the payload of the token task wrote as its artifact name.
func (s *identityStack) claims(namespace, runID, task, name string) map[string]any {
	s.t.Helper()
	raw := s.artifact(namespace, runID, task, name)
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	must.OKf(s.t, err, "the payload of %s's token: %s", task, raw)
	out := map[string]any{}
	must.OKf(s.t, json.Unmarshal(payload, &out), "the claims of %s's token: %s", task, payload)
	return out
}

// issued is what the engine recorded it issued a run.
func (s *identityStack) issued(namespace, runID string) []*orkstackv1.IssuedTokenRecord {
	s.t.Helper()
	resp := &orkstackv1.ListIssuedTokensResponse{}
	must.OK(s.t, api.UnmarshalJSON([]byte(s.cli("-n", namespace, "run", "tokens", runID, "--json")), resp))
	return resp.GetRecords()
}

// recorded is the keys a run holds under prefix, below its own.
func (s *identityStack) recorded(prefix, namespace, runID string) []string {
	s.t.Helper()
	cfg := artifacts.ConfigFromEnv()
	raw, err := s3bucket.Client(s.ctx, cfg)
	must.OK(s.t, err)
	under := prefix + "/" + namespace + "/" + runID + "/"
	page, err := raw.ListObjectsV2(s.ctx, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.S3Bucket), Prefix: aws.String(under)})
	must.OK(s.t, err)
	var out []string
	for _, o := range page.Contents {
		out = append(out, strings.TrimPrefix(aws.ToString(o.Key), under))
	}
	slices.Sort(out)
	return out
}

func (s *identityStack) succeeded(run *orkstackv1.Run) {
	s.t.Helper()
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		s.t.Fatalf("run %s of %s ended %v: %v; tasks %v", run.GetId(), run.GetOrkflowName(), run.GetStatus(), run.GetError(), run.GetTaskRuns())
	}
}

// holdsToken is a container task that holds identity aws and keeps its
// token's payload, which is no credential, as its artifact claims.txt; then
// runs the lines of script.
func holdsToken(id, more string, script ...string) string {
	tail := ""
	for _, line := range script {
		tail += "        " + line + "\n"
	}
	return `  - id: ` + id + `
    uses: shell
` + more + `    timeout_seconds: 180
    identities:
      - name: aws
        audience: ` + identityE2EAudience + `
        ttl_seconds: 900
    with:
      op: script
      interpreter: sh
      script: |
        token="$(cat /orkstack/identity/aws/token)"
        echo "holding $token"
        payload="${token#*.}"
        printf '%s' "${payload%.*}" > /orkstack/output/artifacts/claims.txt
` + tail
}

func orkflowHeader(name, more string) string {
	return "apiVersion: orkstack/v1\nkind: Orkflow\nname: " + name + "\nversion: \"1\"\n" + more + "tasks:\n"
}

// TestIntegration_IdentitySubmit: a run that holds identities, from the
// create of its Orkflow and SubmitRun to its recorded results, through real
// ork, orker and orkctl processes with authentication enforced. The
// container uses the token the engine minted for its attempt, the engine
// records what it issued and each task's result, and nothing of the run is
// where an Orker writes its commits.
func TestIntegration_IdentitySubmit(t *testing.T) {
	s := newIdentityStack(t)
	s.cli("namespace", "create", "platform")
	// An Orkflow may be created only where its audience is allowed; a run's
	// own namespace is the one asked at its submit and at each mint.
	for _, ns := range []string{"default", "platform"} {
		s.cli("namespace", "update", ns, "--audience", identityE2EAudience)
	}

	t.Run("a task, and one that follows it", func(t *testing.T) {
		s.t = t
		s.create("default", "deploy", orkflowHeader("deploy", "")+holdsToken("apply", "")+
			"  - id: report\n    uses: shell\n    needs:\n      - apply\n    with:\n      op: exec\n      command:\n        - \"true\"\n")
		run := s.run("default", "deploy")
		s.succeeded(run)
		c := s.claims("default", run.GetId(), "apply", "claims.txt")
		if c["iss"] != identityE2EIssuer || c["ork_run_id"] != run.GetId() || c["aud"] != identityE2EAudience ||
			c["sub"] != "ork:namespace:default:orkflow:deploy:task:apply" {
			t.Fatalf("the token the task used claims %v", c)
		}
		// One attempt, one token, and the record names the token used.
		recs := s.issued("default", run.GetId())
		if len(recs) != 1 || recs[0].GetTaskId() != "apply" || len(recs[0].GetTokens()) != 1 ||
			recs[0].GetTokens()[0].GetJti() != c["jti"] || recs[0].GetTokens()[0].GetSubject() != c["sub"] {
			t.Fatalf("the engine recorded %v as issued; the task used %v", recs, c)
		}
		// Both tasks' results are the engine's, the plain one's too.
		if got := s.recorded(artifacts.AttestedPrefix, "default", run.GetId()); !slices.Equal(got, []string{"apply/commit.json", "report/commit.json"}) {
			t.Fatalf("the engine recorded %v", got)
		}
		if got := s.recorded(artifacts.CommitPrefix, "default", run.GetId()); len(got) != 0 {
			t.Fatalf("an Orker wrote %v of a run whose results the engine records", got)
		}
		// The token itself is nowhere a reader of the run sees it.
		logs := s.cli("-n", "default", "run", "logs", run.GetId(), "--task", "apply")
		if !strings.Contains(logs, "holding ***") || tokenShape.MatchString(logs) {
			t.Fatalf("the task's logs: %s", tokenShape.ReplaceAllString(logs, "<token>"))
		}
	})

	t.Run("a fan-out", func(t *testing.T) {
		s.t = t
		s.create("default", "spread", orkflowHeader("spread", "")+`  - id: list
    uses: json
    network: none
    with:
      op: query
      nullInput: true
      vars:
        parts:
          - a
          - b
          - c
      filter: '$vars.parts'
`+holdsToken("each-of", "    needs:\n      - list\n    each:\n      over: '{{ .tasks.list.output.result }}'\n      as: part\n      max_items: 3\n"))
		run := s.run("default", "spread")
		s.succeeded(run)
		var items []int32
		for _, rec := range s.issued("default", run.GetId()) {
			if rec.Item == nil || len(rec.GetTokens()) != 1 {
				t.Fatalf("issued %v, want one token of one item", rec)
			}
			items = append(items, rec.GetItem())
		}
		if slices.Sort(items); !slices.Equal(items, []int32{0, 1, 2}) {
			t.Fatalf("tokens were issued for items %v, want one each for 0, 1 and 2", items)
		}
		if got := s.recorded(artifacts.AttestedPrefix, "default", run.GetId()); !slices.Equal(got,
			[]string{"each-of/00000/commit.json", "each-of/00001/commit.json", "each-of/00002/commit.json", "list/commit.json"}) {
			t.Fatalf("the engine recorded %v", got)
		}
	})

	t.Run("a shared child, beside this namespace's own Orkflow of its name", func(t *testing.T) {
		s.t = t
		s.create("platform", "ship", orkflowHeader("ship", "shared_with:\n  - namespace: default\n")+holdsToken("push", ""))
		s.create("default", "ship", orkflowHeader("ship", "")+holdsToken("push", ""))
		s.create("default", "release", orkflowHeader("release", "")+
			"  - id: build\n    uses: shell\n    with:\n      op: exec\n      command:\n        - \"true\"\n"+
			"  - id: theirs\n    uses: orkflow:platform/ship@1\n    needs:\n      - build\n"+
			"  - id: ours\n    uses: orkflow:ship@1\n    needs:\n      - build\n")
		run := s.run("default", "release")
		s.succeeded(run)
		subject := func(task string) string {
			child := taskOf(t, run, task).GetChild().GetRunId()
			c := s.claims("default", child, "push", "claims.txt")
			if c["ork_run_id"] != child {
				t.Fatalf("%s's child %s used a token of %v", task, child, c["ork_run_id"])
			}
			if got := s.recorded(artifacts.AttestedPrefix, "default", child); !slices.Contains(got, "push/commit.json") {
				t.Fatalf("the engine recorded %v of %s's child", got, task)
			}
			return fmt.Sprint(c["sub"])
		}
		theirs, ours := subject("theirs"), subject("ours")
		if theirs == ours || !strings.Contains(theirs, "platform") || strings.Contains(ours, "platform") {
			t.Fatalf("the shared child's subject is %q and this namespace's own %q", theirs, ours)
		}
	})
	t.Run("an approval, and a task that uses what it answered", func(t *testing.T) {
		s.t = t
		s.create("default", "guarded", orkflowHeader("guarded", "")+
			"  - id: approval\n    await:\n      timeout_seconds: 600\n"+
			holdsToken("apply", "    needs:\n      - approval\n",
				`printf '%s' {{ sh .tasks.approval.output.approver }} > /orkstack/output/artifacts/approver.txt`))
		id := parseRunID(t, s.cli("-n", "default", "run", "submit", "guarded"))
		s.await("the approval to be awaited", func() (bool, string) {
			got := rowIn(s.get("default", id), "approval").GetStatus()
			return got == orkstackv1.Status_STATUS_AWAITING, got.String()
		})
		// Nothing of the approval is recorded, nor any token issued, before
		// the answer.
		if recs, got := s.issued("default", id), s.recorded(artifacts.AttestedPrefix, "default", id); len(recs) != 0 || len(got) != 0 {
			t.Fatalf("before the answer: issued %v, recorded %v", recs, got)
		}
		s.cli("-n", "default", "run", "resolve", id, "--task", "approval", "--values", `{"approver":"dana"}`)
		s.try("-n", "default", "run", "watch", id)
		run := s.get("default", id)
		s.succeeded(run)
		if got := s.artifact("default", id, "apply", "approver.txt"); got != "dana" {
			t.Fatalf("the task was given approver %q", got)
		}
		if a := taskOf(t, run, "approval").GetAwait(); !a.GetResolvedByVerified() || a.GetResolvedBy() == "" {
			t.Fatalf("the approval reads as given by %q, verified %v", a.GetResolvedBy(), a.GetResolvedByVerified())
		}
		if got := s.recorded(artifacts.AttestedPrefix, "default", id); !slices.Equal(got, []string{"apply/commit.json", "approval/resolution.json"}) {
			t.Fatalf("the engine recorded %v", got)
		}
		if recs := s.issued("default", id); len(recs) != 1 || recs[0].GetTaskId() != "apply" {
			t.Fatalf("issued %v", recs)
		}
		// The Orker's own credential writes none of them, and writes a probe.
		cfg := artifacts.ConfigFromEnv()
		through := cfg
		through.S3Endpoint = s.proxy.URL
		orkers, err := s3bucket.Client(s.ctx, through)
		must.OK(t, err)
		write := func(key string) error {
			_, err := orkers.PutObject(s.ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key), Body: strings.NewReader("{}")})
			return err
		}
		for _, name := range []string{"approval/resolution.json", "apply/composed.json", "outputs.json", "apply/commit.json"} {
			if err := write(artifacts.AttestedPrefix + "/default/" + id + "/" + name); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
				t.Errorf("the Orker's credential wrote %s: %v", name, err)
			}
		}
		must.OK(t, write("metadata/probes/identity-e2e"))
	})

	t.Run("a rejected approval", func(t *testing.T) {
		s.t = t
		id := parseRunID(t, s.cli("-n", "default", "run", "submit", "guarded"))
		s.await("the approval to be awaited", func() (bool, string) {
			got := rowIn(s.get("default", id), "approval").GetStatus()
			return got == orkstackv1.Status_STATUS_AWAITING, got.String()
		})
		s.cli("-n", "default", "run", "resolve", id, "--task", "approval", "--reject", "--reason", "not during the freeze")
		s.try("-n", "default", "run", "watch", id)
		run := s.get("default", id)
		if got := taskOf(t, run, "approval").GetError(); run.GetStatus() != orkstackv1.Status_STATUS_FAILED || got.GetCode() != "AWAIT_REJECTED" ||
			!strings.Contains(got.GetMessage(), "not during the freeze") {
			t.Fatalf("a run whose approval was rejected ended %v, the approval %v", run.GetStatus(), got)
		}
		if recs, got := s.issued("default", id), s.recorded(artifacts.AttestedPrefix, "default", id); len(recs) != 0 || !slices.Equal(got, []string{"approval/resolution.json"}) {
			t.Fatalf("a rejected approval: issued %v, recorded %v", recs, got)
		}
	})

	t.Run("a composed task that passes a file, returns one and is used by a task", func(t *testing.T) {
		s.t = t
		s.create("default", "sign", orkflowHeader("sign", "inputs:\n  bundle:\n    type: artifact\n    required: true\n"+
			"outputs:\n  who: '{{ .tasks.sign.output.exitCode }}'\n  signed:\n    artifact: sign/signed.txt\n")+
			holdsToken("sign", "", `sed 's/$/ signed/' {{ sh (artifact "_inputs/bundle") }} > /orkstack/output/artifacts/signed.txt`))
		s.create("default", "pipeline", orkflowHeader("pipeline", "")+
			"  - id: build\n    uses: shell\n    with:\n      op: script\n      interpreter: sh\n      script: printf bundle > /orkstack/output/artifacts/bundle.txt\n"+
			"  - id: signer\n    uses: orkflow:sign@1\n    needs:\n      - build\n    artifacts:\n      bundle: build/bundle.txt\n"+
			holdsToken("publish", "    needs:\n      - signer\n",
				`cat {{ sh (artifact "signer/signed") }} > /orkstack/output/artifacts/published.txt`,
				`printf ' %s' {{ sh .tasks.signer.output.who }} >> /orkstack/output/artifacts/published.txt`))
		run := s.run("default", "pipeline")
		s.succeeded(run)
		child := taskOf(t, run, "signer").GetChild().GetRunId()
		if got := s.artifact("default", run.GetId(), "publish", "published.txt"); got != "bundle signed 0" {
			t.Fatalf("the task published %q", got)
		}
		for task, at := range map[string]string{"sign": child, "publish": run.GetId()} {
			if c := s.claims("default", at, task, "claims.txt"); c["ork_run_id"] != at {
				t.Fatalf("%s used a token of %v, want %s's", task, c["ork_run_id"], at)
			}
		}
		if got := s.recorded(artifacts.AttestedPrefix, "default", run.GetId()); !slices.Equal(got,
			[]string{"build/commit.json", "publish/commit.json", "signer/composed.json"}) {
			t.Fatalf("the engine recorded %v of the parent", got)
		}
		if got := s.recorded(artifacts.AttestedPrefix, "default", child); !slices.Equal(got, []string{"outputs.json", "sign/commit.json"}) {
			t.Fatalf("the engine recorded %v of the child", got)
		}
	})
	t.Logf("EVIDENCE identity through SubmitRun: a task, a fan-out, a shared child, an approval given and rejected, and a composed task that passes and returns a file, on real ork, orker and orkctl")
}
