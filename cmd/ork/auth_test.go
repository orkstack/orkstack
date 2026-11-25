package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func lookupFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

const testIssuers = `issuers:
  corp:
    issuer: https://login.example.test
    kind: user
    audiences:
      - orkstack
    algorithms:
      - ES256
    max_lifetime: 1h
`

func TestAuthFromEnv(t *testing.T) {
	c, err := authFromEnv(lookupFrom(nil))
	if err != nil || c.mode != "asserted" || c.policyRefresh != api.DefaultPolicyRefresh || c.policyStaleLimit != api.DefaultPolicyStaleLimit || c.clockSkew != time.Minute {
		t.Fatalf("defaults = %+v, %v", c, err)
	}
	c, err = authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_MODE": "enforce", "ORK_AUTH_ISSUERS": testIssuers,
		"ORK_AUTH_ADMINS": "group:corp:sre, user:corp:root", "ORK_AUTH_POLICY_STALE_LIMIT": "0s"}))
	if err != nil || c.issuers.Len() != 1 || len(c.admins) != 2 || c.policyStaleLimit != 0 {
		t.Fatalf("set = %+v, %v", c, err)
	}
	for name, env := range map[string]map[string]string{
		"an unknown mode":                {"ORK_AUTH_MODE": "paranoid"},
		"a bad duration":                 {"ORK_AUTH_POLICY_REFRESH": "soon"},
		"a zero refresh":                 {"ORK_AUTH_POLICY_REFRESH": "0s"},
		"a bad issuers document":         {"ORK_AUTH_ISSUERS": "issuers: [1]"},
		"an admin that is not a subject": {"ORK_AUTH_ADMINS": "everyone"},
		"a wildcard admin":               {"ORK_AUTH_ADMINS": "group:corp:*"},
		"a public URL with a path":       {"ORK_AUTH_PUBLIC_URL": "https://ork.example.test/x"},
	} {
		if _, err := authFromEnv(lookupFrom(env)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAuthConfig_CheckBoot(t *testing.T) {
	enforce, _ := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_MODE": "enforce"}))
	if err := enforce.checkBoot("", false); err == nil || !strings.Contains(err.Error(), "nobody could ever authenticate") {
		t.Errorf("enforce with no way in = %v", err)
	}
	glass, _ := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_MODE": "enforce", "ORK_AUTH_BREAK_GLASS_KEY_FILE": "/k.pem"}))
	if err := glass.checkBoot("", false); err == nil || !strings.Contains(err.Error(), "auth.public_url") {
		t.Errorf("break-glass with no public URL = %v", err)
	}
	ok, _ := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_MODE": "observe", "ORK_AUTH_ISSUERS": testIssuers}))
	if err := ok.checkBoot("127.0.0.1:9090", true); err != nil {
		t.Errorf("pprof on loopback: %v", err)
	}
	if err := ok.checkBoot("0.0.0.0:9090", true); err == nil {
		t.Error("pprof on every interface with authentication on was accepted")
	}
	asserted, _ := authFromEnv(lookupFrom(nil))
	if err := asserted.checkBoot("0.0.0.0:9090", true); err != nil {
		t.Errorf("asserted mode is unchanged: %v", err)
	}
}

// TestEngineDescription_FromConfiguration: what an engine says of itself on
// the presence channel is its configuration: its mode, its idempotency key's
// life, its deployment and build, and, only when it authenticates, the
// digests of auth.admins and auth.orkers, which are the same whatever order
// they were written in.
func TestEngineDescription_FromConfiguration(t *testing.T) {
	asserted, _ := authFromEnv(lookupFrom(nil))
	cfg := config{auth: asserted, idempotencyTTL: 36 * time.Hour, deployment: task.Deployment{Name: "orkstack", BuildID: "b7"}}
	if d := engineDescription(cfg); d != (engine.EngineDescription{Mode: "asserted", ClaimTTL: 36 * time.Hour, Deployment: "orkstack", BuildID: "b7"}) {
		t.Fatalf("asserted = %+v; want no digests", d)
	}
	orkers := "orkers:\n- subject: serviceaccount:orker-main\n  pool: main\n- subject: serviceaccount:orker-gpu\n  pool: gpu\n  orker_ids: any\n"
	enforce, err := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_MODE": "enforce", "ORK_AUTH_ISSUERS": testIssuers,
		"ORK_AUTH_ADMINS": "group:corp:sre, user:corp:root", "ORK_AUTH_ORKERS": orkers}))
	if err != nil {
		t.Fatal(err)
	}
	cfg.auth = enforce
	d := engineDescription(cfg)
	if d.Mode != "enforce" || len(d.AdminsDigest) != engine.ConfigDigestLen || len(d.OrkersDigest) != engine.ConfigDigestLen {
		t.Fatalf("enforce = %+v; want both digests", d)
	}
	reordered, err := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_MODE": "enforce", "ORK_AUTH_ISSUERS": testIssuers,
		"ORK_AUTH_ADMINS": "user:corp:root,group:corp:sre",
		"ORK_AUTH_ORKERS": "orkers:\n- subject: serviceaccount:orker-gpu\n  pool: gpu\n  orker_ids: any\n- subject: serviceaccount:orker-main\n  pool: main\n"}))
	if err != nil {
		t.Fatal(err)
	}
	cfg.auth = reordered
	if again := engineDescription(cfg); again != d {
		t.Fatalf("the same lists in another order describe %+v, want %+v", again, d)
	}
	other, err := authFromEnv(lookupFrom(map[string]string{"ORK_AUTH_MODE": "enforce", "ORK_AUTH_ISSUERS": testIssuers,
		"ORK_AUTH_ADMINS": "group:corp:sre, user:corp:root",
		"ORK_AUTH_ORKERS": "orkers:\n- subject: serviceaccount:orker-main\n  pool: main\n  orker_ids: any\n- subject: serviceaccount:orker-gpu\n  pool: gpu\n  orker_ids: any\n"}))
	if err != nil {
		t.Fatal(err)
	}
	cfg.auth = other
	if differs := engineDescription(cfg); differs.OrkersDigest == d.OrkersDigest || differs.AdminsDigest != d.AdminsDigest {
		t.Fatalf("auth.orkers differing only in orker_ids digests alike: %+v and %+v", differs, d)
	}
}

func TestSplitSetModeRecord(t *testing.T) {
	a, rest, err := splitSetModeRecord([]string{"--config", "x.yaml", "--set-mode-record", "observe", "--reason=bad record", "--force"})
	if err != nil || !a.requested || a.mode != "observe" || a.reason != "bad record" || !a.force || strings.Join(rest, " ") != "--config x.yaml" {
		t.Fatalf("= %+v, %v, %v", a, rest, err)
	}
	for _, args := range [][]string{
		{"--set-mode-record", "loud", "--reason", "x"},
		{"--set-mode-record", "observe"},
		{"--force"},
		{"--set-mode-record"},
	} {
		if _, _, err := splitSetModeRecord(args); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
	if a, rest, err := splitSetModeRecord([]string{"--check"}); err != nil || a.requested || len(rest) != 1 {
		t.Errorf("no command = %+v, %v, %v", a, rest, err)
	}
}

func TestSetModeRecord(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cas := artifacts.NewMemoryCAS()
	modes := artifacts.NewModeStoreOn(cas)
	audit := artifacts.NewMemoryAuditStore()
	recorder := api.NewAuditRecorder(audit, nil, logger, nil)
	var out bytes.Buffer
	// A readable record is refused without --force, and nothing is written.
	if _, err := modes.Put(ctx, artifacts.ModeRecord{Mode: "enforce", UpdatedAt: time.Now()}, artifacts.PutConditions{IfAbsent: true}); err != nil {
		t.Fatal(err)
	}
	err := setModeRecordIn(ctx, setModeRecordArgs{requested: true, mode: "asserted", reason: "r"}, modes, "e", recorder, &out, logger)
	if err == nil || len(audit.Documents()) != 0 {
		t.Fatalf("a readable record without --force = %v, %d records", err, len(audit.Documents()))
	}
	if !strings.Contains(out.String(), artifacts.ModeRecordKey) {
		t.Errorf("the store was not printed first: %q", out.String())
	}
	// An unreadable record is overwritten, after its audit record.
	cas.PutRaw(artifacts.ModeRecordKey, []byte("garbage"))
	if err := setModeRecordIn(ctx, setModeRecordArgs{requested: true, mode: "observe", reason: "bad record"}, modes, "e", recorder, &out, logger); err != nil {
		t.Fatal(err)
	}
	if rec, _, err := modes.Get(ctx); err != nil || rec.Mode != "observe" {
		t.Fatalf("record = %+v, %v", rec, err)
	}
	if n := len(audit.Documents()); n != 1 {
		t.Fatalf("audit records = %d; want auth.mode.record.set", n)
	}
	// No audit record can be written: nothing changes.
	cas.PutRaw(artifacts.ModeRecordKey, []byte("garbage"))
	broken := api.NewAuditRecorder(failingAudit{audit}, nil, logger, nil)
	if err := setModeRecordIn(ctx, setModeRecordArgs{requested: true, mode: "observe", reason: "x"}, modes, "e", broken, &out, logger); err == nil {
		t.Fatal("the record was set with its audit record unwritten")
	}
	if raw, _ := cas.Raw(artifacts.ModeRecordKey); string(raw) != "garbage" {
		t.Fatal("the record changed although its audit record could not be written")
	}
}

// TestSetModeRecord_KeepsWhenEnforceWasLeft: a record set below enforce says
// the deployment left it now where the record it replaces was enforcing,
// unreadable or missing, and keeps what a lower one says: the wait for
// asserted, and the look for a caller's own idempotency keys, rest on it.
func TestSetModeRecord_KeepsWhenEnforceWasLeft(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	left := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		was  *artifacts.ModeRecord // nil is none
		raw  string
		to   string
		want func(time.Time) bool
	}{
		"enforce to observe":            {was: &artifacts.ModeRecord{Mode: "enforce", UpdatedAt: left}, to: "observe", want: recent},
		"enforce to asserted":           {was: &artifacts.ModeRecord{Mode: "enforce", UpdatedAt: left}, to: "asserted", want: recent},
		"unreadable to observe":         {raw: "garbage", to: "observe", want: recent},
		"none to observe":               {to: "observe", want: recent},
		"observe to asserted":           {was: &artifacts.ModeRecord{Mode: "observe", LeftEnforceAt: left}, to: "asserted", want: left.Equal},
		"observe, which never enforced": {was: &artifacts.ModeRecord{Mode: "observe"}, to: "asserted", want: time.Time.IsZero},
		"anything to enforce":           {was: &artifacts.ModeRecord{Mode: "observe", LeftEnforceAt: left}, to: "enforce", want: time.Time.IsZero},
	} {
		cas := artifacts.NewMemoryCAS()
		modes := artifacts.NewModeStoreOn(cas)
		if tc.was != nil {
			if _, err := modes.Put(ctx, *tc.was, artifacts.PutConditions{IfAbsent: true}); err != nil {
				t.Fatal(err)
			}
		} else if tc.raw != "" {
			cas.PutRaw(artifacts.ModeRecordKey, []byte(tc.raw))
		}
		recorder := api.NewAuditRecorder(artifacts.NewMemoryAuditStore(), nil, logger, nil)
		if err := setModeRecordIn(ctx, setModeRecordArgs{requested: true, mode: tc.to, reason: "r", force: true}, modes, "e", recorder, io.Discard, logger); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if rec, _, err := modes.Get(ctx); err != nil || rec.Mode != tc.to || !tc.want(rec.LeftEnforceAt) {
			t.Errorf("%s: %+v, %v", name, rec, err)
		}
	}
}

func recent(at time.Time) bool { return time.Since(at).Abs() < time.Minute }

type failingAudit struct{ *artifacts.MemoryAuditStore }

func (failingAudit) PutAuditRecord(context.Context, *orkstackv1.AuditRecord) error {
	return errors.New("store down")
}

const testLoginIssuers = testIssuers + `    login:
      client_id: orkstack-web
      scopes:
        - openid
`

// TestAuthConfig_CheckBootSignin: an issuer that offers sign-in needs a
// public URL, https off loopback, a sign-in key, and limits that fit
// inside max_age.
func TestAuthConfig_CheckBootSignin(t *testing.T) {
	base := map[string]string{"ORK_AUTH_MODE": "enforce", "ORK_AUTH_ISSUERS": testLoginIssuers,
		"ORK_AUTH_PUBLIC_URL": "https://ork.example.test", "ORK_AUTH_SIGNIN_KEY_FILES": "/k1, /k2"}
	with := func(over map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range over {
			m[k] = v
		}
		return m
	}
	c, err := authFromEnv(lookupFrom(base))
	if err != nil || !c.signinOffered() || len(c.signinKeyFiles) != 2 || c.signin.MaxAge != 12*time.Hour {
		t.Fatalf("authFromEnv = %+v, %v", c, err)
	}
	if err := c.checkBoot("", false); err != nil {
		t.Fatalf("a complete sign-in: %v", err)
	}
	local, _ := authFromEnv(lookupFrom(with(map[string]string{"ORK_AUTH_PUBLIC_URL": "http://localhost:8080"})))
	if err := local.checkBoot("", false); err != nil {
		t.Errorf("plain http on loopback: %v", err)
	}
	for name, over := range map[string]map[string]string{
		"no public URL":             {"ORK_AUTH_PUBLIC_URL": ""},
		"plain http off loopback":   {"ORK_AUTH_PUBLIC_URL": "http://ork.example.test"},
		"no sign-in key":            {"ORK_AUTH_SIGNIN_KEY_FILES": ""},
		"an idle timeout past life": {"ORK_AUTH_SIGNIN_IDLE_TIMEOUT": "13h"},
	} {
		c, err := authFromEnv(lookupFrom(with(over)))
		if err == nil {
			err = c.checkBoot("", false)
		}
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := authFromEnv(lookupFrom(with(map[string]string{"ORK_AUTH_SIGNIN_MAX_AGE": "forever"}))); err == nil {
		t.Error("a bad max_age was accepted")
	}
}

func TestSigninSecret(t *testing.T) {
	t.Setenv("ORK_TEST_LOGIN_SECRET", " s3cret\n")
	if v, err := signinSecret("env:ORK_TEST_LOGIN_SECRET"); err != nil || v != "s3cret" {
		t.Fatalf("env = %q %v", v, err)
	}
	path := t.TempDir() + "/secret"
	if err := os.WriteFile(path, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if v, err := signinSecret("file:" + path); err != nil || v != "from-file" {
		t.Fatalf("file = %q %v", v, err)
	}
	for _, src := range []string{"env:ORK_TEST_UNSET_SECRET", "file:/nonexistent", "vault:x"} {
		if _, err := signinSecret(src); err == nil {
			t.Errorf("%s: read", src)
		}
	}
}
