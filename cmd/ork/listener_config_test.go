package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestListenerFromEnv(t *testing.T) {
	ceiling := engine.DefaultOrkflowMaxTasks
	derived := api.RequestMaxBytesFor(ceiling)
	for _, tc := range []struct {
		name, idle, maxBytes string
		maxTasks             int32
		want                 listenerConfig
		wantErr              []string
	}{
		{name: "unset: the defaults, the cap derived from the task ceiling", maxTasks: ceiling,
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: derived}},
		{name: "unset under a raised task ceiling: the cap rises with it", maxTasks: 20_000,
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: api.RequestMaxBytesFor(20_000)}},
		{name: "unset under a lowered task ceiling: the cap falls with it", maxTasks: 10,
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: api.RequestMaxBytesFor(10)}},
		{name: "both set", idle: "11m", maxBytes: "33554432", maxTasks: ceiling,
			want: listenerConfig{idleTimeout: 11 * time.Minute, requestMaxBytes: 32 << 20}},
		{name: "a cap exactly what the ceiling needs", maxBytes: strconv.FormatInt(derived, 10), maxTasks: ceiling,
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: derived}},
		{name: "the largest cap", maxBytes: "1073741824", maxTasks: ceiling,
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: maxRequestMaxBytes}},
		{name: "a ceiling that needs more than 1 GiB gets what it needs, warned", maxTasks: 300_000,
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: api.RequestMaxBytesFor(300_000),
				warnings: []string{"ORK_ORKFLOW_MAX_TASKS=300000"}}},
		{name: "a ceiling that needs more than 1 GiB may be given twice that", maxTasks: 300_000,
			maxBytes: strconv.FormatInt(2*api.RequestMaxBytesFor(300_000), 10),
			want: listenerConfig{idleTimeout: api.DefaultIdleTimeout, requestMaxBytes: 2 * api.RequestMaxBytesFor(300_000),
				warnings: []string{"over 1 GiB"}}},
		{name: "a ceiling that needs more than 1 GiB, given more than twice that", maxTasks: 300_000,
			maxBytes: strconv.FormatInt(2*api.RequestMaxBytesFor(300_000)+1, 10),
			wantErr:  []string{"ORK_REQUEST_MAX_BYTES", "twice"}},
		{name: "an idle timeout that is not a duration", idle: "soon", maxTasks: ceiling,
			wantErr: []string{"ORK_HTTP_IDLE_TIMEOUT"}},
		{name: "an idle timeout under a second", idle: "500ms", maxTasks: ceiling, wantErr: []string{"ORK_HTTP_IDLE_TIMEOUT"}},
		{name: "a cap that is not a number", maxBytes: "16MiB", maxTasks: ceiling, wantErr: []string{"ORK_REQUEST_MAX_BYTES"}},
		{name: "a cap one byte under what the ceiling needs", maxBytes: strconv.FormatInt(derived-1, 10), maxTasks: ceiling,
			wantErr: []string{"ORK_REQUEST_MAX_BYTES", "engine.request_max_bytes", "ORK_ORKFLOW_MAX_TASKS=4000",
				"engine.orkflow_max_tasks", strconv.FormatInt(derived, 10)}},
		{name: "the old 16 MiB default under the default ceiling", maxBytes: "16777216", maxTasks: ceiling,
			wantErr: []string{"ORK_REQUEST_MAX_BYTES", "ORK_ORKFLOW_MAX_TASKS"}},
		{name: "a 32 MiB cap under a ceiling that needs more", maxBytes: "33554432", maxTasks: 20_000,
			wantErr: []string{"ORK_REQUEST_MAX_BYTES", "ORK_ORKFLOW_MAX_TASKS=20000"}},
		{name: "a cap over 1 GiB", maxBytes: "1073741825", maxTasks: ceiling, wantErr: []string{"ORK_REQUEST_MAX_BYTES"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORK_HTTP_IDLE_TIMEOUT", tc.idle)
			t.Setenv("ORK_REQUEST_MAX_BYTES", tc.maxBytes)
			got, err := listenerFromEnv(tc.maxTasks)
			if len(tc.wantErr) > 0 {
				if err == nil {
					t.Fatalf("err = nil, want one naming %q", tc.wantErr)
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("err = %v, want one naming %s", err, want)
					}
				}
				return
			}
			must.OK(t, err)
			if got.idleTimeout != tc.want.idleTimeout || got.requestMaxBytes != tc.want.requestMaxBytes {
				t.Errorf("listenerFromEnv(%d) = %+v, want %+v", tc.maxTasks, got, tc.want)
			}
			if len(got.warnings) != len(tc.want.warnings) {
				t.Fatalf("warnings %q, want %d naming %q", got.warnings, len(tc.want.warnings), tc.want.warnings)
			}
			for i, want := range tc.want.warnings {
				if !strings.Contains(got.warnings[i], want) {
					t.Errorf("warning %q, want one naming %q", got.warnings[i], want)
				}
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
	got, err := listenerFromEnv(engine.DefaultOrkflowMaxTasks)
	must.OK(t, err)
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
	if _, err := listenerFromEnv(engine.DefaultOrkflowMaxTasks); err == nil || !strings.Contains(err.Error(), "ORK_ALLOWED_HOSTS") {
		t.Errorf("a URL for a host: err = %v, want a refusal naming ORK_ALLOWED_HOSTS", err)
	}
	t.Setenv("ORK_ALLOWED_HOSTS", "")
	t.Setenv("ORK_HTTP_ADDR", "")
	got, err = listenerFromEnv(engine.DefaultOrkflowMaxTasks)
	if err != nil || !got.hosts.Allows("localhost:8080") || got.hosts.Allows("evil.example") {
		t.Errorf("unset: %v; want the default policy", err)
	}
}
