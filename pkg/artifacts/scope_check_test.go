package artifacts

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/testsupport/s3scope"
)

// scopeFixture is one store behind the policy proxy, and a client of the
// proxy, as an Orker holding that policy's credential would have.
type scopeFixture struct {
	double *s3Double
	proxy  *s3scope.Proxy
	client *s3.Client
}

func newScopeFixture(t *testing.T, policy s3scope.Policy) scopeFixture {
	t.Helper()
	d := newS3Double(t)
	p := s3scope.New(t, d.server.URL, d.bucket, policy)
	client := d.fastClient(t, func(o *s3.Options) { o.BaseEndpoint = aws.String(p.URL) })
	return scopeFixture{double: d, proxy: p, client: client}
}

func (f scopeFixture) check(t *testing.T, timeout time.Duration) ScopeRound {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return checkStoreScope(ctx, f.client, f.double.bucket, "orker-a")
}

// answerOn scripts one answer for the requests match picks.
func answerOn(match func(s3scope.Request) bool, a s3scope.Answer) func(s3scope.Request) *s3scope.Answer {
	return func(r s3scope.Request) *s3scope.Answer {
		if match(r) {
			return &a
		}
		return nil
	}
}

func under(prefix string, verbs ...s3scope.Verb) func(s3scope.Request) bool {
	return func(r s3scope.Request) bool {
		return strings.HasPrefix(r.Key, prefix) && (len(verbs) == 0 || slices.Contains(verbs, r.Verb))
	}
}

func every(s3scope.Request) bool { return true }

// TestCheckStoreScope_ReadsEachAnswer is the scope check's table test
// (docs/DEPLOY.md, "An Orker's own storage credential"): the store gives
// each answer it can, and the round reads it as scoped, unscoped or unclear.
// After every case the bucket holds only what the case seeded: the check
// created nothing.
func TestCheckStoreScope_ReadsEachAnswer(t *testing.T) {
	orker := s3scope.OrkerPolicy()
	denied := s3scope.Answer{Status: 403, Code: "AccessDenied"}
	cases := []struct {
		name    string
		policy  s3scope.Policy
		script  func(s3scope.Request) *s3scope.Answer
		seed    bool // the engine's sentinel exists
		timeout time.Duration

		outcome ScopeOutcome
		detail  string
		results ResultsReach
		// ignored: the store answered 2xx to a conditional write, and the
		// check removed the key it may have stored.
		ignored bool
	}{
		{name: "the documented policy", policy: orker, seed: true,
			outcome: ScopeScoped, results: ResultsEveryNamespace},
		// A credential that may not read is refused whether or not the
		// sentinel is there.
		{name: "the documented policy, no sentinel yet", policy: orker,
			outcome: ScopeScoped, results: ResultsEveryNamespace},
		// One that may read and list is told the key is missing, which
		// cannot tell it from a non-reader: unclear, never scoped.
		{name: "a reader of a missing sentinel", policy: orker.With(IdentityPrefix+"/sentinel", s3scope.Get, s3scope.List).With(IdentityPrefix+"/", s3scope.List),
			script:  answerOn(func(r s3scope.Request) bool { return r.Verb == s3scope.List && r.Key == IdentityPrefix+"/" }, denied),
			outcome: ScopeUnclear, detail: "the sentinel is absent"},
		{name: "results limited by namespace", policy: orker, seed: true,
			script:  answerOn(under(CommitPrefix+"/"+ScopeResultsSegment+"/"), denied),
			outcome: ScopeScoped, results: ResultsByNamespace},
		{name: "a shared credential", policy: s3scope.Everything(), seed: true,
			outcome: ScopeUnscoped, detail: "put " + IdentityPrefix + "/"},
		{name: "412 on a write under metadata/identity", policy: orker, seed: true,
			script:  answerOn(under(IdentityPrefix+"/", s3scope.Put), s3scope.Answer{Status: 412, Code: "PreconditionFailed"}),
			outcome: ScopeUnscoped, detail: "put " + IdentityPrefix + "/"},
		{name: "404 on a write under metadata/identity", policy: orker, seed: true,
			script:  answerOn(under(IdentityPrefix+"/", s3scope.Put), s3scope.Answer{Status: 404, Code: "NoSuchKey"}),
			outcome: ScopeUnscoped, detail: "put " + IdentityPrefix + "/"},
		{name: "a delete reaches", policy: orker, seed: true,
			script:  answerOn(under(NamespacePrefix+"/", s3scope.Delete), s3scope.Answer{Status: 412, Code: "PreconditionFailed"}),
			outcome: ScopeUnscoped, detail: "delete " + NamespacePrefix + "/"},
		{name: "a 2xx to the conditional delete", policy: orker, seed: true,
			script:  answerOn(under(AuditPrefix+"/", s3scope.Delete), s3scope.Answer{Status: 204}),
			outcome: ScopeUnscoped, detail: "delete " + AuditPrefix + "/"},
		{name: "a listing reaches", policy: orker, seed: true,
			script:  answerOn(under(IdentityPrefix+"/", s3scope.List), s3scope.Answer{Status: 200}),
			outcome: ScopeUnscoped, detail: "list " + IdentityPrefix + "/"},
		{name: "the sentinel is readable", policy: orker.With(IdentityPrefix+"/", s3scope.Get), seed: true,
			outcome: ScopeUnscoped, detail: "get " + ScopeSentinelKey},
		{name: "a 2xx to the conditional write", policy: orker, seed: true,
			script:  answerOn(under(IdentityPrefix+"/", s3scope.Put), s3scope.Answer{Status: 200}),
			outcome: ScopeUnscoped, detail: "put " + IdentityPrefix + "/", ignored: true},
		{name: "AccessDenied on everything, the control included", policy: orker, seed: true,
			script:  answerOn(every, denied),
			outcome: ScopeUnclear, detail: "AccessDenied"},
		{name: "the control answered 2xx", policy: orker, seed: true,
			script:  answerOn(under(ScopeProbePrefix+"/", s3scope.Put), s3scope.Answer{Status: 200}),
			outcome: ScopeUnclear, detail: "If-Match", ignored: true},
		{name: "NoSuchBucket", policy: orker, seed: true,
			script:  answerOn(every, s3scope.Answer{Status: 404, Code: "NoSuchBucket"}),
			outcome: ScopeUnclear, detail: "NoSuchBucket"},
		{name: "SignatureDoesNotMatch", policy: orker, seed: true,
			script:  answerOn(every, s3scope.Answer{Status: 403, Code: "SignatureDoesNotMatch"}),
			outcome: ScopeUnclear, detail: "SignatureDoesNotMatch"},
		{name: "RequestTimeTooSkewed", policy: orker, seed: true,
			script:  answerOn(every, s3scope.Answer{Status: 403, Code: "RequestTimeTooSkewed"}),
			outcome: ScopeUnclear, detail: "RequestTimeTooSkewed"},
		{name: "NotImplemented on one prefix", policy: orker, seed: true,
			script:  answerOn(under(WebhookPrefix+"/", s3scope.Put), s3scope.Answer{Status: 501, Code: "NotImplemented"}),
			outcome: ScopeUnclear, detail: "NotImplemented"},
		{name: "a 403 with another code on one listing", policy: orker, seed: true,
			script:  answerOn(under(TriggerPrefix+"/", s3scope.List), s3scope.Answer{Status: 403, Code: "InvalidAccessKeyId"}),
			outcome: ScopeUnclear, detail: "InvalidAccessKeyId"},
		{name: "500 on one write", policy: orker, seed: true,
			script:  answerOn(under(SettingsPrefix+"/", s3scope.Put), s3scope.Answer{Status: 500, Code: "InternalError"}),
			outcome: ScopeUnclear, detail: "InternalError"},
		{name: "429 on the sentinel", policy: orker, seed: true,
			script:  answerOn(under(ScopeSentinelKey, s3scope.Get), s3scope.Answer{Status: 429, Code: "SlowDown"}),
			outcome: ScopeUnclear, detail: "SlowDown"},
		{name: "a stalled store", policy: orker, seed: true, timeout: 300 * time.Millisecond,
			script:  answerOn(every, s3scope.Answer{Stall: true}),
			outcome: ScopeUnclear, detail: "timeout"},
		{name: "one reach beside unclear answers", policy: orker, seed: true,
			script: func(r s3scope.Request) *s3scope.Answer {
				switch {
				case strings.HasPrefix(r.Key, WebhookPrefix+"/") && r.Verb == s3scope.List:
					return &s3scope.Answer{Status: 200}
				case strings.HasPrefix(r.Key, SettingsPrefix+"/"):
					return &s3scope.Answer{Status: 503, Code: "ServiceUnavailable"}
				}
				return nil
			},
			outcome: ScopeUnscoped, detail: "list " + WebhookPrefix + "/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newScopeFixture(t, tc.policy)
			if tc.seed {
				f.double.objects[ScopeSentinelKey] = []byte(scopeSentinelBody)
			}
			f.proxy.Script(tc.script)
			timeout := tc.timeout
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			round := f.check(t, timeout)
			if round.Outcome != tc.outcome {
				t.Fatalf("outcome = %v (%q), want %v", round.Outcome, round.Detail, tc.outcome)
			}
			if tc.detail != "" && !strings.Contains(round.Detail, tc.detail) {
				t.Errorf("detail = %q, want it to name %q", round.Detail, tc.detail)
			}
			if tc.outcome == ScopeScoped && round.Detail != "" {
				t.Errorf("a scoped round says %q", round.Detail)
			}
			if tc.results != ResultsUnknown && round.Results != tc.results {
				t.Errorf("results = %v, want %v", round.Results, tc.results)
			}
			if round.IgnoredCondition != tc.ignored {
				t.Errorf("IgnoredCondition = %v, want %v", round.IgnoredCondition, tc.ignored)
			}
			if tc.ignored {
				// Every key a 2xx answered is deleted again, unconditionally.
				requests := f.proxy.Requests()
				for _, r := range requests {
					if r.Verb == s3scope.Put && r.IfMatch != "" && tc.script(r) != nil && tc.script(r).Status < 300 &&
						!slices.ContainsFunc(requests, func(d s3scope.Request) bool {
							return d.Verb == s3scope.Delete && d.Key == r.Key && d.IfMatch == ""
						}) {
						t.Errorf("the check did not remove %s, which the store may have stored", r.Key)
					}
				}
			}
			// The check created nothing: only what the case seeded is left.
			f.double.mu.Lock()
			defer f.double.mu.Unlock()
			for key := range f.double.objects {
				if key != ScopeSentinelKey || !tc.seed {
					t.Errorf("the check left %s behind", key)
				}
			}
		})
	}
}

// TestCheckStoreScope_AsksEveryCheckedPrefix: each prefix of the table is
// written and deleted under a condition and listed, and the control comes
// first.
func TestCheckStoreScope_AsksEveryCheckedPrefix(t *testing.T) {
	f := newScopeFixture(t, s3scope.OrkerPolicy())
	if round := f.check(t, 10*time.Second); round.Outcome != ScopeScoped {
		t.Fatalf("round = %+v", round)
	}
	requests := f.proxy.Requests()
	if len(requests) == 0 || requests[0].Verb != s3scope.Put || !strings.HasPrefix(requests[0].Key, ScopeProbePrefix+"/") || requests[0].IfMatch == "" {
		t.Fatalf("the first request is not the conditional control under %s: %+v", ScopeProbePrefix, requests)
	}
	for _, prefix := range ScopeCheckedPrefixes {
		for _, verb := range []s3scope.Verb{s3scope.Put, s3scope.Delete, s3scope.List} {
			if !slices.ContainsFunc(requests, func(r s3scope.Request) bool {
				return r.Verb == verb && strings.HasPrefix(r.Key, prefix+"/") && (verb == s3scope.List || r.IfMatch != "")
			}) {
				t.Errorf("no %s of %s/", verb, prefix)
			}
		}
	}
}

// TestScopeTables_CoverEveryPrefix ties the check's two tables to every
// prefix constant of this package: each lies under exactly one entry of the
// Orker's allow-list or of the checked prefixes, so a prefix a later release
// adds cannot be forgotten.
func TestScopeTables_CoverEveryPrefix(t *testing.T) {
	prefixes := storePrefixConstants(t)
	if len(prefixes) < 15 {
		t.Fatalf("found only %d prefix constants: %q", len(prefixes), prefixes)
	}
	entries := append(slices.Clone(OrkerAllowedPrefixes), ScopeCheckedPrefixes...)
	for _, p := range prefixes {
		var in []string
		for _, entry := range entries {
			if p == entry || strings.HasPrefix(p, entry+"/") {
				in = append(in, entry)
			}
		}
		if len(in) != 1 {
			t.Errorf("prefix constant %q lies under %q; want exactly one entry of OrkerAllowedPrefixes or ScopeCheckedPrefixes", p, in)
		}
	}
	for _, p := range ScopeCheckedPrefixes {
		if slices.ContainsFunc(OrkerAllowedPrefixes, func(a string) bool {
			return p == a || strings.HasPrefix(p, a+"/") || strings.HasPrefix(a, p+"/")
		}) {
			t.Errorf("%q is both checked and allowed", p)
		}
	}
	// The documented policy, as s3scope reads it, allows exactly the
	// allow-list's prefixes.
	policy := s3scope.OrkerPolicy()
	if len(policy) != len(OrkerAllowedPrefixes) {
		t.Errorf("s3scope's policy has %d rules, OrkerAllowedPrefixes %d entries", len(policy), len(OrkerAllowedPrefixes))
	}
	for _, rule := range policy {
		if !slices.Contains(OrkerAllowedPrefixes, strings.TrimSuffix(rule.Prefix, "/")) {
			t.Errorf("s3scope's policy allows %q, which OrkerAllowedPrefixes does not name", rule.Prefix)
		}
	}
}

// storePrefixConstants is every string constant of this package's own
// source that names a key prefix: a "metadata/…" value, and the two
// top-level namespaces an artifact's keys lie under.
func storePrefixConstants(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var out []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				for _, value := range spec.(*ast.ValueSpec).Values {
					lit, ok := value.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					s, err := strconv.Unquote(lit.Value)
					if err != nil {
						continue
					}
					if strings.HasPrefix(s, "metadata/") || s == s3OrkflowNamespace || s == s3ManifestNamespace {
						out = append(out, strings.TrimSuffix(s, "/"))
					}
				}
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// TestEnsureScopeSentinel writes the sentinel once and leaves an existing
// one alone.
func TestEnsureScopeSentinel(t *testing.T) {
	d := newS3Double(t)
	client := d.fastClient(t)
	for range 2 {
		if err := ensureScopeSentinel(context.Background(), client, d.bucket); err != nil {
			t.Fatalf("ensureScopeSentinel: %v", err)
		}
	}
	if got := string(d.objects[ScopeSentinelKey]); got != scopeSentinelBody {
		t.Fatalf("sentinel = %q", got)
	}
	d.objects[ScopeSentinelKey] = []byte("kept")
	if err := ensureScopeSentinel(context.Background(), client, d.bucket); err != nil {
		t.Fatal(err)
	}
	if string(d.objects[ScopeSentinelKey]) != "kept" {
		t.Fatal("an existing sentinel was rewritten")
	}
}

// TestCheckStoreScopeOf_TheLocalBackend: the local backend has no
// credential to hold to a prefix, so its round is unscoped and says why.
func TestCheckStoreScopeOf_TheLocalBackend(t *testing.T) {
	store, err := NewLocalCommitStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	round := CheckStoreScopeOf(context.Background(), store, "orker-a")
	if round.Outcome != ScopeUnscoped || !strings.Contains(round.Detail, "local backend") {
		t.Fatalf("round = %+v", round)
	}
}

// TestBoundText keeps a detail valid UTF-8 and within its bound whatever a
// store answers, so DescribeOrker always encodes it.
func TestBoundText(t *testing.T) {
	for _, tc := range []struct {
		in   string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{strings.Repeat("a", 63) + "é", 64, strings.Repeat("a", 63)},
		{"\xff\xfeok", 10, "\uFFFD" + "ok"},
		{"日本語", 4, "日"},
	} {
		got := boundText(tc.in, tc.n)
		if got != tc.want || !utf8.ValidString(got) || len(got) > tc.n {
			t.Errorf("boundText(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
		}
	}
}

// TestCheckStoreScope_AHostileCodeIsBounded: a store's code too long, or not
// UTF-8, still leaves a detail a proto string can carry.
func TestCheckStoreScope_AHostileCodeIsBounded(t *testing.T) {
	f := newScopeFixture(t, s3scope.OrkerPolicy())
	f.double.objects[ScopeSentinelKey] = []byte(scopeSentinelBody)
	f.proxy.Script(answerOn(under(WebhookPrefix+"/", s3scope.Put), s3scope.Answer{Status: 400, Code: strings.Repeat("é", 100)}))
	round := f.check(t, 10*time.Second)
	if round.Outcome != ScopeUnclear || !utf8.ValidString(round.Detail) || len(round.Detail) > MaxScopeDetail {
		t.Fatalf("round = %v, detail %q (%d bytes)", round.Outcome, round.Detail, len(round.Detail))
	}
}
