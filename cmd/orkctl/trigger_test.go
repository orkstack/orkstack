package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestTriggerCommand_IsDiscoverableFromHelp(t *testing.T) {
	// Help needs no engine, so it runs Execute directly rather than through
	// the fixture, which appends --server to every invocation.
	for _, tc := range []struct {
		args     []string
		fragment []string
	}{
		{[]string{"--help"}, []string{"trigger", "Create and inspect inbound run starters"}},
		{[]string{"trigger", "--help"}, []string{"create", "list", "delete", "POST /v1/triggers/<id>"}},
		{[]string{"trigger", "create", "--help"}, []string{"--orkflow", "--version", "--description"}},
		{[]string{"trigger", "list", "--help"}, []string{"--orkflow"}},
		{[]string{"trigger", "delete", "--help"}, []string{"delete <id>"}},
	} {
		var stdout, stderr bytes.Buffer
		if code := Execute(tc.args, &stdout, &stderr); code != exitOK {
			t.Fatalf("orkctl %v exited %d: %s", tc.args, code, stderr.String())
		}
		for _, fragment := range tc.fragment {
			if !strings.Contains(stdout.String(), fragment) {
				t.Errorf("orkctl %v help does not mention %q:\n%s", tc.args, fragment, stdout.String())
			}
		}
	}
}

func TestTriggerCreate_RefusesWithoutAnOrkflowBeforeAnyRequest(t *testing.T) {
	// A trigger is bound to exactly one orkflow, so a missing --orkflow is
	// refused locally: the server address below is never dialed.
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))

	var stdout, stderr bytes.Buffer
	code := Execute([]string{"trigger", "create", "--server", "http://127.0.0.1:1"}, &stdout, &stderr)
	if code == exitOK {
		t.Fatalf("creating a trigger without --orkflow succeeded:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "--orkflow is required") {
		t.Errorf("stderr does not explain the refusal:\n%s", stderr.String())
	}
}

func TestRenderTriggerCreated_ShowsTheSecretOnceWithARunnableCurl(t *testing.T) {
	var out bytes.Buffer
	trigger := &orkstackv1.Trigger{
		Id:          "trg-0123456789abcdef",
		Namespace:   "team-a",
		OrkflowName: "hello",
		Description: "nightly kickoff",
		CreatedAt:   timestamppb.New(fixedNow),
	}
	if err := renderTriggerCreated(&out, "https://ork.example.test/", trigger, "s3cr3t"); err != nil {
		t.Fatalf("renderTriggerCreated: %v", err)
	}

	for _, fragment := range []string{
		"Created trigger trg-0123456789abcdef",
		"team-a",
		"hello@current",
		"nightly kickoff",
		"s3cr3t",
		"will not be shown again",
		"curl -X POST 'https://ork.example.test/v1/triggers/trg-0123456789abcdef'",
		"-H 'Authorization: Bearer s3cr3t'",
		"-H 'Content-Type: application/json'",
		"-d '{}'",
		"# optional: -H 'Ork-Origin-Reference: <your CI job URL>'",
	} {
		if !strings.Contains(out.String(), fragment) {
			t.Errorf("created output does not contain %q:\n%s", fragment, out.String())
		}
	}
}

func TestRenderTriggerCreated_ShowsAPinnedVersion(t *testing.T) {
	var out bytes.Buffer
	trigger := &orkstackv1.Trigger{
		Id: "trg-1", Namespace: "default", OrkflowName: "hello", OrkflowVersion: "3",
	}
	if err := renderTriggerCreated(&out, "http://localhost:8080", trigger, "token"); err != nil {
		t.Fatalf("renderTriggerCreated: %v", err)
	}
	if !strings.Contains(out.String(), "hello@3") {
		t.Errorf("pinned trigger does not name its version:\n%s", out.String())
	}
	if strings.Contains(out.String(), currentVersionLabel) {
		t.Errorf("pinned trigger reads as unpinned:\n%s", out.String())
	}
}

func TestRenderTriggerList_RendersAnUnpinnedVersionAsCurrent(t *testing.T) {
	var out bytes.Buffer
	err := renderTriggerList(&out, []*orkstackv1.Trigger{
		{
			Id: "trg-aaa", Namespace: "default", OrkflowName: "hello",
			CreatedAt: timestamppb.New(fixedNow),
		},
		{
			Id: "trg-bbb", Namespace: "default", OrkflowName: "http-post", OrkflowVersion: "2",
			Description: "from CI", CreatedAt: timestamppb.New(fixedNow),
		},
	})
	if err != nil {
		t.Fatalf("renderTriggerList: %v", err)
	}
	for _, fragment := range []string{
		"ID", "NAMESPACE", "ORKFLOW", "VERSION", "DESCRIPTION", "CREATED",
		"trg-aaa", "current", "trg-bbb", "2", "from CI", "2026-01-02T03:04:05Z",
	} {
		if !strings.Contains(out.String(), fragment) {
			t.Errorf("list output does not contain %q:\n%s", fragment, out.String())
		}
	}
}

func TestRenderTriggerList_SaysSoWhenNothingExists(t *testing.T) {
	var out bytes.Buffer
	if err := renderTriggerList(&out, nil); err != nil {
		t.Fatalf("renderTriggerList: %v", err)
	}
	if got := out.String(); got != "No triggers.\n" {
		t.Errorf("empty list = %q, want %q", got, "No triggers.\n")
	}
}

func TestShellQuote_KeepsAQuotedValueOneArgument(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"plain", "'plain'"},
		{"has space", "'has space'"},
		{"it's", `'it'\''s'`},
		{"", "''"},
	} {
		if got := shellQuote(tc.in); got != tc.want {
			t.Errorf("shellQuote(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}
