package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	cfgfile "github.com/orkstack/orkstack/internal/config"
	"github.com/orkstack/orkstack/internal/healthcheck"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// adminStub is a stand-in admin listener that answers /readyz with status.
func adminStub(t *testing.T, status int) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(telemetry.ReadyzPath, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}

// TestHealthcheck_AsksThisBinarysAdminAddressFromTheConfigFile: --healthcheck
// reads its configuration the way a boot does, so an admin address written in
// the file, per binary as {ork: X, orker: Y}, is the one this binary asks, and
// the other binary's side of the pair is not.
func TestHealthcheck_AsksThisBinarysAdminAddressFromTheConfigFile(t *testing.T) {
	unsetenvT(t, telemetry.AdminAddrVar)
	// A is the ork side of the pair, B the orker side. Only A is ready, so
	// the check passes only if it asked the address that is this binary's own.
	a := adminStub(t, http.StatusOK)
	b := adminStub(t, http.StatusServiceUnavailable)
	path := writeTempConfig(t, "cfg.yaml", fmt.Sprintf(
		"telemetry:\n  admin_addr: {ork: %q, orker: %q}\n", a, b))

	// The same argv the container's health check runs: the file, and the flag.
	_, _, check, _, err := applyConfigFile(
		[]string{"--healthcheck", "--config", path}, cfgfile.BinaryOrk, os.LookupEnv, setenvT(t))
	if err != nil || check {
		t.Fatalf("applyConfigFile: check=%v, err=%v", check, err)
	}
	var stdout, stderr bytes.Buffer
	if code := healthcheck.Run(context.Background(), "ork", os.Getenv, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d, stderr %q; the file's ork address is ready, so the check should pass", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "ork: ready") {
		t.Errorf("stdout %q, want a line starting \"ork: ready\"", stdout.String())
	}
}

// TestHealthcheck_EnvironmentWinsOverTheFile: the same precedence a boot has
// (defaults < file < environment). A container that sets ORK_ADMIN_ADDR is
// checked at that address whatever the file says.
func TestHealthcheck_EnvironmentWinsOverTheFile(t *testing.T) {
	ready := adminStub(t, http.StatusOK)
	notReady := adminStub(t, http.StatusServiceUnavailable)
	t.Setenv(telemetry.AdminAddrVar, ready)
	path := writeTempConfig(t, "cfg.yaml", fmt.Sprintf("telemetry:\n  admin_addr: %q\n", notReady))

	if _, _, _, _, err := applyConfigFile(
		[]string{"--healthcheck", "--config", path}, cfgfile.BinaryOrk, os.LookupEnv, setenvT(t)); err != nil {
		t.Fatalf("applyConfigFile: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if code := healthcheck.Run(context.Background(), "ork", os.Getenv, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d, stderr %q; the environment's address is ready, so the check should pass", code, stderr.String())
	}
}

// TestHealthcheck_NoAdminAddressAnywhereFailsNamingIt: neither a file nor the
// environment gives the process an admin listener, so there is nothing to ask.
func TestHealthcheck_NoAdminAddressAnywhereFailsNamingIt(t *testing.T) {
	unsetenvT(t, telemetry.AdminAddrVar)
	path := writeTempConfig(t, "cfg.yaml", "pool: {}\n")
	if _, _, _, _, err := applyConfigFile(
		[]string{"--healthcheck", "--config", path}, cfgfile.BinaryOrk, os.LookupEnv, setenvT(t)); err != nil {
		t.Fatalf("applyConfigFile: %v", err)
	}
	var stdout, stderr bytes.Buffer
	code := healthcheck.Run(context.Background(), "ork", os.Getenv, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "ORK_ADMIN_ADDR") {
		t.Fatalf("exit %d, stdout %q, stderr %q; want exit 1 and a reason naming ORK_ADMIN_ADDR", code, stdout.String(), stderr.String())
	}
}

// TestApplyConfigFile_HealthcheckWithCheckIsAUsageError: --check validates a
// file and starts nothing; --healthcheck asks a running process. Together they
// are a usage error (exit 2), not a bad file.
func TestApplyConfigFile_HealthcheckWithCheckIsAUsageError(t *testing.T) {
	path := writeTempConfig(t, "cfg.yaml", "pool: {}\n")
	doc, gotPath, _, _, err := applyConfigFile(
		[]string{"--config", path, "--healthcheck", "--check"}, cfgfile.BinaryOrk, os.LookupEnv, setenvT(t))
	if err == nil {
		t.Fatal("applyConfigFile accepted --healthcheck with --check")
	}
	if doc != nil || gotPath != "" {
		t.Errorf("applyConfigFile = (doc=%v, path=%q), want the usage-error shape (nil, \"\") so main exits 2", doc, gotPath)
	}
	for _, want := range []string{"--healthcheck", "--check"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}
