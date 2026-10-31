package main

import (
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
)

func TestListenerFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name, idle, maxBytes string
		want                 listenerConfig
		wantErr              string
	}{
		{name: "unset: the defaults",
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: api.DefaultRequestMaxBytes}},
		{name: "both set", idle: "11m", maxBytes: "33554432",
			want: listenerConfig{idleTimeout: 11 * time.Minute, requestMaxBytes: 32 << 20}},
		{name: "the smallest cap", maxBytes: "1048576",
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: api.MinRequestMaxBytes}},
		{name: "the largest cap", maxBytes: "1073741824",
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: maxRequestMaxBytes}},
		{name: "an idle timeout that is not a duration", idle: "soon", wantErr: "ORK_HTTP_IDLE_TIMEOUT"},
		{name: "an idle timeout under a second", idle: "500ms", wantErr: "ORK_HTTP_IDLE_TIMEOUT"},
		{name: "a cap that is not a number", maxBytes: "16MiB", wantErr: "ORK_REQUEST_MAX_BYTES"},
		{name: "a cap under 1 MiB", maxBytes: "1048575", wantErr: "ORK_REQUEST_MAX_BYTES"},
		{name: "a cap over 1 GiB", maxBytes: "1073741825", wantErr: "ORK_REQUEST_MAX_BYTES"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORK_HTTP_IDLE_TIMEOUT", tc.idle)
			t.Setenv("ORK_REQUEST_MAX_BYTES", tc.maxBytes)
			got, err := listenerFromEnv()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.idleTimeout != tc.want.idleTimeout || got.requestMaxBytes != tc.want.requestMaxBytes {
				t.Errorf("listenerFromEnv() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// ORK_ALLOWED_HOSTS is a comma-separated list read into the Host policy with
// ORK_HTTP_ADDR; space around an entry and empty entries are ignored, and an
// entry that is not a name or a pattern stops the boot naming it.
func TestListenerFromEnv_AllowedHosts(t *testing.T) {
	t.Setenv("ORK_HTTP_ADDR", "ork.internal:8080")
	t.Setenv("ORK_ALLOWED_HOSTS", " orkstack.example.com, ,*.corp.example ")
	got, err := listenerFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.allowedHosts, ",") != "orkstack.example.com,*.corp.example" {
		t.Errorf("allowedHosts = %q", got.allowedHosts)
	}
	for host, want := range map[string]bool{
		"orkstack.example.com": true, "a.corp.example:443": true, "ork.internal:8080": true, "app.localhost": true,
		"127.0.0.1:8080": true, "ork:8080": true, "evil.example": false, "ork.orkstack.svc": false,
	} {
		if got.hosts.Allows(host) != want {
			t.Errorf("Host %q: admitted = %t, want %t", host, !want, want)
		}
	}

	t.Setenv("ORK_ALLOWED_HOSTS", "https://orkstack.example.com")
	if _, err := listenerFromEnv(); err == nil || !strings.Contains(err.Error(), "ORK_ALLOWED_HOSTS") {
		t.Errorf("a URL for a host: err = %v, want a refusal naming ORK_ALLOWED_HOSTS", err)
	}
	t.Setenv("ORK_ALLOWED_HOSTS", "")
	t.Setenv("ORK_HTTP_ADDR", "")
	got, err = listenerFromEnv()
	if err != nil || !got.hosts.Allows("localhost:8080") || got.hosts.Allows("evil.example") {
		t.Errorf("unset: %v; want the default policy", err)
	}
}
