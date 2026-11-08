//go:build integration

package main

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// TestIntegration_BootRefusesCredentialsItCouldNotSignWith runs the real binary
// with an environment holding only what is named here, so nothing of the host's
// can answer for the default credential chain. It stops at the check: no store,
// no cluster and no container engine is reached, and the refusal names what the
// operator can set and prints no value.
func TestIntegration_BootRefusesCredentialsItCouldNotSignWith(t *testing.T) {
	const value = "VISIBLE-VALUE"
	cases := []struct {
		name string
		env  []string
		want []string
	}{
		{
			name: "no key and nothing in the chain",
			want: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY", "default credential chain"},
		},
		{
			name: "an access key alone",
			env:  []string{"ORK_S3_ACCESS_KEY=" + value},
			want: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY", "set both"},
		},
		{
			name: "a secret key alone",
			env:  []string{"ORK_S3_SECRET_KEY=" + value},
			want: []string{"ORK_S3_ACCESS_KEY", "ORK_S3_SECRET_KEY", "set both"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, testbin.Path(t, "./cmd/ork"))
			cmd.Dir = home
			cmd.Env = append([]string{
				"HOME=" + home,
				"ORK_ARTIFACT_BACKEND=s3",
				"ORK_ORKER_WORKDIR=" + filepath.Join(home, "work"),
				"AWS_SHARED_CREDENTIALS_FILE=" + filepath.Join(home, "no-credentials"),
				"AWS_CONFIG_FILE=" + filepath.Join(home, "no-config"),
				"AWS_EC2_METADATA_DISABLED=true",
			}, tc.env...)
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || ctx.Err() != nil {
				t.Fatalf("the boot did not refuse: err %v, ctx %v\n%s", err, ctx.Err(), output.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(output.String(), want) {
					t.Errorf("output does not say %q:\n%s", want, output.String())
				}
			}
			if strings.Contains(output.String(), value) {
				t.Errorf("output prints the key's value:\n%s", output.String())
			}
		})
	}
}
