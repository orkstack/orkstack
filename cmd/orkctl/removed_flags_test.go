package main

import (
	"bytes"
	"strings"
	"testing"
)

// This is the one file that must spell the flags the structured list
// filters removed: it proves each is refused. scripts/check-list-filter-
// leftovers.sh scans everything else for them.

// TestRemovedFlags_AreUnknown: a removed filter flag is an unknown flag —
// cobra refuses it and the command exits non-zero — never an alias of
// --filter. OrkStack keeps no compatibility layer.
func TestRemovedFlags_AreUnknown(t *testing.T) {
	for _, args := range [][]string{
		{"trigger", "list", "--orkflow", "hello-ork"},
		{"schedule", "list", "--orkflow", "hello-ork"},
		{"orker", "list", "--pool", "gpu"},
		{"namespace", "list", "--name-contains", "pay"},
		{"webhook", "deliveries", "--webhook", "wh-4a1c0e2b7d3f8159"},
		{"webhook", "deliveries", "--run", "ork-2026-01-01-8f31"},
		{"webhook", "deliveries", "--event", "RUN_FAILED"},
		{"webhook", "deliveries", "--delivered", "false"},
		{"webhook", "deliveries", "--since", "24h"},
		{"webhook", "deliveries", "--until", "2026-01-01T00:00:00Z"},
		{"admin", "audit", "--since", "2026-01-01"},
		{"admin", "audit", "--until", "2026-01-02"},
		{"admin", "audit", "--action", "namespace."},
		{"admin", "audit", "--principal", "dsantos"},
	} {
		var stdout, stderr bytes.Buffer
		code := Execute(append(args, "--server", "http://127.0.0.1:1"), &stdout, &stderr)
		if code == exitOK {
			t.Errorf("orkctl %v exited 0; a removed flag must be refused", args)
		}
		if !strings.Contains(stderr.String(), "unknown flag: "+args[2]) {
			t.Errorf("orkctl %v stderr = %q; want cobra's unknown flag error", args, stderr.String())
		}
	}
}

// TestDeliveriesAndAudit_RefuseAnExplicitNamespaceFlag: webhook deliveries
// and the audit trail are deployment-wide listings, narrowed by a namespace
// FILTER. An explicit -n/--namespace on either command line is refused before
// any request, so the flag that used to narrow them cannot silently widen
// them; ORK_NAMESPACE is ignored, as it always was.
func TestDeliveriesAndAudit_RefuseAnExplicitNamespaceFlag(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		message string
	}{
		{[]string{"-n", "payments", "webhook", "deliveries"}, "webhook deliveries lists every namespace; narrow it with --filter namespace=<ns>"},
		{[]string{"webhook", "deliveries", "--namespace", "payments"}, "webhook deliveries lists every namespace; narrow it with --filter namespace=<ns>"},
		{[]string{"-n", "payments", "admin", "audit"}, "admin audit lists every namespace; narrow it with --filter namespace=<ns>"},
		{[]string{"admin", "audit", "-n", "default"}, "admin audit lists every namespace; narrow it with --filter namespace=<ns>"},
	} {
		f := newFixture(t)
		res := f.run(tc.args...)
		if res.code == exitOK || !strings.Contains(res.stderr, tc.message) {
			t.Errorf("orkctl %v = %d %q; want a refusal %q", tc.args, res.code, res.stderr, tc.message)
		}
		if got := f.requests(); len(got) != 0 {
			t.Errorf("orkctl %v sent %v before refusing", tc.args, got)
		}
	}
	// ORK_NAMESPACE is not an explicit flag: the request is sent, unnarrowed
	// (this fixture wires no audit store; reaching the engine is the point).
	f := newFixture(t)
	t.Setenv(namespaceEnvVar, "payments")
	f.run("admin", "audit")
	if len(f.requests()) == 0 {
		t.Errorf("admin audit with only ORK_NAMESPACE set never reached the engine")
	}
}
