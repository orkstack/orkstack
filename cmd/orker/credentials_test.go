package main

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestCheckStoreCredentials: what an Orker's boot refuses, by backend. A local
// backend signs nothing, so it needs no credential, but half a key is a mistake
// whatever the backend. What is logged is where a credential came from.
func TestCheckStoreCredentials(t *testing.T) {
	const access, secret = "VISIBLE-ACCESS", "VISIBLE-SECRET"
	cases := []struct {
		name       string
		store      artifacts.Config
		wantError  string
		wantSource string
	}{
		{name: "local needs no credential", store: artifacts.Config{Backend: artifacts.BackendLocal}},
		{name: "unset backend is local", store: artifacts.Config{}},
		{
			name:      "local still refuses half a key",
			store:     artifacts.Config{Backend: artifacts.BackendLocal, S3AccessKey: access},
			wantError: "ORK_S3_SECRET_KEY",
		},
		{
			name:      "s3 with nothing to sign with",
			store:     artifacts.Config{Backend: artifacts.BackendS3},
			wantError: "default credential chain",
		},
		{
			name:       "s3 with a key",
			store:      artifacts.Config{Backend: artifacts.BackendS3, S3AccessKey: access, S3SecretKey: secret},
			wantSource: "StaticCredentials",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			for _, key := range []string{
				"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
				"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
			} {
				t.Setenv(key, "")
			}
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(home, "no-credentials"))
			t.Setenv("AWS_CONFIG_FILE", filepath.Join(home, "no-config"))
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

			var logged bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logged, nil))
			err := checkStoreCredentials(context.Background(), tc.store, logger)
			switch {
			case tc.wantError == "" && err != nil:
				t.Fatalf("checkStoreCredentials() = %v, want nil", err)
			case tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)):
				t.Fatalf("checkStoreCredentials() = %v, want a refusal naming %q", err, tc.wantError)
			}
			if tc.wantSource != "" && !strings.Contains(logged.String(), "source="+tc.wantSource) {
				t.Errorf("log %q does not name the source %s", logged.String(), tc.wantSource)
			}
			text := logged.String()
			if err != nil {
				text += err.Error()
			}
			for _, value := range []string{access, secret} {
				if strings.Contains(text, value) {
					t.Errorf("a credential was printed: %q", text)
				}
			}
		})
	}
}
